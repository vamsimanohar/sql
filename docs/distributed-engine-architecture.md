# Distributed PPL Query Engine — Architecture

## High-Level Execution Flow

```
                          PPL Query: "search source=accounts | stats avg(age) by gender"
                                                    |
                                                    v
                                         +--------------------+
                                         |   PPL Parser /     |
                                         |   Calcite Planner  |
                                         +--------------------+
                                                    |
                                              RelNode tree
                                                    |
                                                    v
                                +---------------------------------+
                                |   DistributedExecutionEngine    |
                                |   (query router)                |
                                +---------------------------------+
                                    |                       |
                        distributed=true           distributed=false
                                    |                       |
                                    v                       v
                        +-------------------+    +------------------------+
                        | DistributedQuery  |    | OpenSearchExecution    |
                        | Planner           |    | Engine (legacy)        |
                        +-------------------+    +------------------------+
                                    |
                                    v
                        +-------------------+
                        | DistributedTask   |
                        | Scheduler         |
                        +-------------------+
                           /        |        \
                          v         v         v
                      +------+  +------+  +------+
                      |Node-1|  |Node-2|  |Node-3|    (Transport: OPERATOR_PIPELINE)
                      +------+  +------+  +------+
                          |         |         |
                    LuceneScan  LuceneScan  LuceneScan  (direct _source reads)
                          \         |         /
                           \        |        /
                            v       v       v
                        +-------------------+
                        | Coordinator:      |
                        | Calcite RelRunner |
                        | (merge + compute) |
                        +-------------------+
                                    |
                                    v
                             QueryResponse
```

---

## Module Layout

```
sql/
 ├── core/src/main/java/org/opensearch/sql/planner/distributed/
 │    ├── DistributedQueryPlanner.java      Planning: RelNode → DistributedPhysicalPlan
 │    ├── DistributedPhysicalPlan.java       Plan container (stages, status, transient RelNode)
 │    ├── DistributedPlanAnalyzer.java       Walks RelNode, produces RelNodeAnalysis
 │    ├── RelNodeAnalysis.java               Analysis data class (table, filters, aggs, joins)
 │    ├── ExecutionStage.java                Stage: SCAN / PROCESS / FINALIZE
 │    ├── WorkUnit.java                      Parallelizable unit (partition + node assignment)
 │    ├── DataPartition.java                 Shard/file abstraction (Lucene, Parquet, etc.)
 │    ├── PartitionDiscovery.java            Interface: tableName → List<DataPartition>
 │    │
 │    ├── operator/                          ── Phase 5A Core Operator Framework ──
 │    │    ├── Operator.java                 Push/pull interface (Page batches)
 │    │    ├── SourceOperator.java           Reads from storage (extends Operator)
 │    │    ├── SinkOperator.java             Terminal consumer (extends Operator)
 │    │    ├── OperatorFactory.java           Creates Operator instances
 │    │    ├── SourceOperatorFactory.java     Creates SourceOperator instances
 │    │    └── OperatorContext.java           Runtime context (memory, cancellation)
 │    │
 │    ├── page/                              ── Data Batching ──
 │    │    ├── Page.java                     Columnar-ready batch interface
 │    │    ├── RowPage.java                  Row-based Page implementation
 │    │    └── PageBuilder.java              Row-by-row Page builder
 │    │
 │    ├── pipeline/                          ── Pipeline Execution ──
 │    │    ├── Pipeline.java                 Ordered chain of OperatorFactories
 │    │    ├── PipelineDriver.java           Drives data through operator chain
 │    │    └── PipelineContext.java           Runtime state (status, cancellation)
 │    │
 │    ├── stage/                             ── Staged Planning ──
 │    │    ├── StagedPlan.java               Tree of ComputeStages (dependency order)
 │    │    ├── ComputeStage.java             Stage with pipeline + partitioning
 │    │    ├── PartitioningScheme.java        Output partitioning (gather, hash, broadcast)
 │    │    └── ExchangeType.java             Enum: GATHER / HASH_REPARTITION / BROADCAST / NONE
 │    │
 │    ├── exchange/                          ── Inter-Stage Data Transfer ──
 │    │    ├── ExchangeManager.java          Creates sink/source operators
 │    │    ├── ExchangeSinkOperator.java      Sends pages downstream
 │    │    └── ExchangeSourceOperator.java    Receives pages from upstream
 │    │
 │    ├── split/                             ── Data Assignment ──
 │    │    ├── Split.java                    Unit of work (index + shard + preferred nodes)
 │    │    ├── SplitSource.java              Generates splits for source operators
 │    │    └── SplitAssignment.java          Assigns splits to nodes
 │    │
 │    └── planner/                           ── Physical Planning Interfaces ──
 │         ├── PhysicalPlanner.java          RelNode → StagedPlan
 │         └── CostEstimator.java            Row count / size / selectivity estimation
 │
 └── opensearch/src/main/java/org/opensearch/sql/opensearch/executor/
      ├── DistributedExecutionEngine.java    Entry point: routes legacy vs distributed
      │
      └── distributed/
           ├── DistributedTaskScheduler.java        Coordinates execution across cluster
           ├── OpenSearchPartitionDiscovery.java     Discovers shards from routing table
           │
           ├── TransportExecuteDistributedTaskAction.java  Transport handler (data node)
           ├── ExecuteDistributedTaskAction.java           ActionType for routing
           ├── ExecuteDistributedTaskRequest.java           OPERATOR_PIPELINE request
           ├── ExecuteDistributedTaskResponse.java          Rows / SearchResponse back
           │
           ├── RelNodeAnalyzer.java            Extracts filters/sorts/fields/joins from RelNode
           ├── HashJoinExecutor.java            Coordinator-side hash join (all join types)
           ├── QueryResponseBuilder.java        JDBC ResultSet → QueryResponse
           ├── TemporalValueNormalizer.java     Date/time normalization for Calcite
           ├── InMemoryScannableTable.java      In-memory Calcite table for coordinator exec
           ├── JoinInfo.java                    Join metadata record
           ├── SortKey.java                     Sort field record
           ├── FieldMapping.java                Column mapping record
           │
           ├── operator/                       ── OpenSearch Operators ──
           │    ├── LuceneScanOperator.java     Direct Lucene _source reads (Weight/Scorer)
           │    ├── LimitOperator.java           Row limit enforcement
           │    ├── ResultCollector.java          Collects pages into row lists
           │    └── FilterToLuceneConverter.java  Filter conditions → Lucene Query
           │
           └── pipeline/
                └── OperatorPipelineExecutor.java  Orchestrates pipeline on data node
```

---

## Class Hierarchy

### Planning Layer

```
DistributedQueryPlanner
 ├── uses PartitionDiscovery (interface)
 │         └── OpenSearchPartitionDiscovery (impl: ClusterService → shard routing)
 ├── uses DistributedPlanAnalyzer
 │         └── produces RelNodeAnalysis
 └── produces DistributedPhysicalPlan
              ├── List<ExecutionStage>
              │    ├── StageType: SCAN | PROCESS | FINALIZE
              │    ├── List<WorkUnit>
              │    │    ├── WorkUnitType: SCAN | PROCESS | FINALIZE
              │    │    └── DataPartition
              │    │         └── StorageType: LUCENE | PARQUET | ORC | ...
              │    ├── DataExchangeType: NONE | GATHER | HASH_REDISTRIBUTE | BROADCAST
              │    └── dependencies: List<stageId>
              ├── PlanStatus: CREATED → EXECUTING → COMPLETED | FAILED
              └── transient: RelNode + CalcitePlanContext (for coordinator execution)
```

### Operator Framework (H2 — MPP Architecture)

```
                   Operator (interface)
                  /         \
     SourceOperator       SinkOperator
     (adds splits)        (terminal)
          |                    |
  ExchangeSourceOperator  ExchangeSinkOperator
          |
  LuceneScanOperator (OpenSearch impl)

  Other Operators:
  ├── LimitOperator (implements Operator)
  └── (future: FilterOperator, ProjectOperator, AggOperator, etc.)

  Factories:
  ├── OperatorFactory      → creates Operator
  └── SourceOperatorFactory → creates SourceOperator

  Data Flow:
  Split → SourceOperator → Page → Operator → Page → ... → SinkOperator
                                    ↑
                               OperatorContext (memory, cancellation)
```

### Pipeline Execution

```
  Pipeline
   ├── SourceOperatorFactory (creates source)
   └── List<OperatorFactory> (creates intermediates)
            |
            v
  PipelineDriver
   ├── SourceOperator ──→ Operator ──→ ... ──→ Operator
   │         ↑                                      ↓
   │       Split                                  Page (output)
   └── PipelineContext (status, cancellation)
```

### Staged Plan (H2)

```
  StagedPlan
   └── List<ComputeStage> (dependency order: leaves → root)
        ├── stageId
        ├── SourceOperatorFactory
        ├── List<OperatorFactory>
        ├── PartitioningScheme
        │    ├── ExchangeType: GATHER | HASH_REPARTITION | BROADCAST | NONE
        │    └── hashChannels: List<Integer>
        ├── sourceStageIds (upstream dependencies)
        ├── List<Split> (data assignments)
        └── estimatedRows / estimatedBytes
```

---

## Typical Execution Plans

### Simple Scan: `search source=accounts | fields firstname, age | head 10`

```
  DistributedPhysicalPlan: distributed-plan-abc12345
  Status: EXECUTING → COMPLETED

  [1] SCAN  (exchange: NONE, parallelism: 5)
      ├── SCAN  [accounts/0]  ~100.0MB  → node-abc
      ├── SCAN  [accounts/1]  ~100.0MB  → node-abc
      ├── SCAN  [accounts/2]  ~100.0MB  → node-def
      ├── SCAN  [accounts/3]  ~100.0MB  → node-def
      └── SCAN  [accounts/4]  ~100.0MB  → node-ghi
  │
  ▼
  [2] FINALIZE  (exchange: GATHER, parallelism: 1)
      └── FINALIZE  → coordinator

  Execution:
  1. Coordinator groups shards by node: {abc: [0,1], def: [2,3], ghi: [4]}
  2. Sends OPERATOR_PIPELINE transport to each node
  3. Each node: LuceneScanOperator(fields=[firstname,age]) → LimitOperator(10)
  4. Coordinator merges rows, applies final limit(10)
  5. Returns QueryResponse
```

### Aggregation: `search source=accounts | stats avg(age) by gender`

```
  DistributedPhysicalPlan: distributed-plan-def45678
  Status: EXECUTING → COMPLETED

  [1] SCAN  (exchange: NONE, parallelism: 5)
      ├── SCAN  [accounts/0]  ~100.0MB  → node-abc
      ├── SCAN  [accounts/1]  ~100.0MB  → node-abc
      ├── SCAN  [accounts/2]  ~100.0MB  → node-def
      ├── SCAN  [accounts/3]  ~100.0MB  → node-def
      └── SCAN  [accounts/4]  ~100.0MB  → node-ghi
  │
  ▼
  [2] PROCESS (partial aggregation)  (exchange: NONE, parallelism: 3)
      ├── PROCESS  → (scheduler-assigned)
      ├── PROCESS  → (scheduler-assigned)
      └── PROCESS  → (scheduler-assigned)
  │
  ▼
  [3] FINALIZE (merge aggregation via InternalAggregations.reduce)  (exchange: GATHER, parallelism: 1)
      └── FINALIZE  → coordinator

  Execution (coordinator-side Calcite):
  1. Coordinator scans ALL rows from data nodes (no filter pushdown for correctness)
  2. Replaces TableScan with InMemoryScannableTable (in-memory rows)
  3. Runs full Calcite plan: BindableTableScan → Aggregate(avg(age), group=gender)
  4. Returns QueryResponse from JDBC ResultSet
```

### Join: `search source=employees | join left=e right=d ON e.dept_id = d.id source=departments`

```
  DistributedPhysicalPlan: distributed-plan-ghi78901

  [1] SCAN (left)  (exchange: NONE, parallelism: 3)
      ├── SCAN  [employees/0]  → node-abc
      ├── SCAN  [employees/1]  → node-def
      └── SCAN  [employees/2]  → node-ghi
  │
  ▼
  [2] SCAN (right)  (exchange: NONE, parallelism: 2)
      ├── SCAN  [departments/0]  → node-abc
      └── SCAN  [departments/1]  → node-def
  │
  ▼
  [3] FINALIZE  (exchange: GATHER, parallelism: 1)
      └── FINALIZE  → coordinator

  Execution (coordinator-side Calcite):
  1. Scan employees from all nodes in parallel
  2. Scan departments from all nodes in parallel
  3. Replace both TableScans with InMemoryScannableTable
  4. Calcite executes: BindableTableScan(employees) ⋈ BindableTableScan(departments)
  5. Full RelNode tree handles filter/sort/limit/projection above join
  6. Returns QueryResponse
```

### Filter Pushdown: `search source=accounts | where age > 30 | fields firstname, age`

```
  Execution (operator pipeline with filter):
  1. Coordinator extracts filter: {field: "age", op: "GT", value: 30}
  2. Sends to each node with filterConditions in transport request
  3. Data node: FilterToLuceneConverter → NumericRangeQuery(age > 30)
  4. LuceneScanOperator uses Lucene Weight/Scorer with filter query
  5. Only matching documents returned → reduces network transfer
```

---

## Data Node Operator Pipeline (per request)

```
  ExecuteDistributedTaskRequest
   │  indexName: "accounts"
   │  shardIds: [0, 1]
   │  fieldNames: ["firstname", "age"]
   │  queryLimit: 200
   │  filterConditions: [{field: "age", op: "GT", value: 30}]
   │
   v
  OperatorPipelineExecutor.execute()
   │
   ├── resolveIndexService("accounts")
   ├── For each shardId:
   │    ├── resolveIndexShard(shardId)
   │    ├── FilterToLuceneConverter.convert(filters) → Lucene Query
   │    ├── LuceneScanOperator
   │    │    ├── acquireSearcher() → Engine.Searcher
   │    │    ├── IndexSearcher.createWeight(query)
   │    │    ├── For each LeafReaderContext:
   │    │    │    ├── Weight.scorer(leaf)
   │    │    │    ├── DocIdSetIterator.nextDoc()
   │    │    │    ├── Read _source from StoredFields
   │    │    │    ├── Extract requested fields
   │    │    │    └── Build Page(batchSize rows)
   │    │    └── Returns Page batches
   │    ├── LimitOperator(queryLimit)
   │    │    └── Passes through until limit reached
   │    └── ResultCollector
   │         └── Accumulates rows from Pages
   │
   └── Return OperatorPipelineResult(fieldNames, rows)
```

---

## Transport Wire Protocol

```
  Coordinator                              Data Node
      │                                        │
      │  ExecuteDistributedTaskRequest         │
      │  ┌──────────────────────────┐          │
      │  │ stageId: "op-pipeline"  │          │
      │  │ indexName: "accounts"   │          │
      │  │ shardIds: [0, 1, 2]    │──────────►│
      │  │ executionMode: "OP.."  │          │
      │  │ fieldNames: [...]      │          │
      │  │ queryLimit: 200        │          │
      │  │ filterConditions: [...] │          │
      │  └──────────────────────────┘          │
      │                                        │
      │                                        │  LuceneScanOperator
      │                                        │  → reads shards 0,1,2
      │                                        │  → applies Lucene query
      │                                        │  → extracts _source fields
      │                                        │
      │  ExecuteDistributedTaskResponse        │
      │  ┌──────────────────────────┐          │
      │  │ success: true           │          │
      │  │ nodeId: "node-abc"     │◄──────────│
      │  │ pipelineFieldNames: .. │          │
      │  │ pipelineRows: [[...]]  │          │
      │  └──────────────────────────┘          │
      │                                        │
```

---

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `plugins.ppl.distributed.enabled` | `true` | Single toggle: legacy engine (off) or distributed operator pipeline (on) |

**No sub-settings.** When distributed is on, the operator pipeline is the only execution path. If a query pattern fails, we fix the pipeline — no fallback.

---

## Two Execution Paths (No Fallback)

```
  plugins.ppl.distributed.enabled = false        plugins.ppl.distributed.enabled = true
  ─────────────────────────────────────          ─────────────────────────────────────
  PPL → Calcite → OpenSearchExecutionEngine       PPL → Calcite → DistributedExecutionEngine
                   │                                                │
                   v                                                v
          client.search() (SSB pushdown)              DistributedQueryPlanner.plan()
          Single-node coordinator                     DistributedTaskScheduler.executeQuery()
                                                       │
                                                       v
                                                  OPERATOR_PIPELINE transport
                                                  to all data nodes
                                                       │
                                                       v
                                                  Coordinator merges + Calcite exec
```
