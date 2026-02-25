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
                                |   (routing shell)               |
                                +---------------------------------+
                                    |                       |
                        distributed=true           distributed=false (default)
                                    |                       |
                                    v                       v
                    UnsupportedOperationException   +------------------------+
                    (execution not yet implemented) | OpenSearchExecution    |
                                                    | Engine (legacy)        |
                                                    +------------------------+
```

When `plugins.ppl.distributed.enabled=true`, the engine throws `UnsupportedOperationException`.
Distributed execution will be implemented in the next phase against the clean H2 interfaces.

---

## Module Layout

```
sql/
 ├── core/src/main/java/org/opensearch/sql/planner/distributed/
 │    │
 │    ├── operator/                          ── Core Operator Framework ──
 │    │    ├── Operator.java                 Push/pull interface (Page batches)
 │    │    ├── SourceOperator.java           Reads from storage (extends Operator)
 │    │    ├── SinkOperator.java             Terminal consumer (extends Operator)
 │    │    ├── OperatorFactory.java           Creates Operator instances
 │    │    ├── SourceOperatorFactory.java     Creates SourceOperator instances
 │    │    └── OperatorContext.java           Runtime context (memory, cancellation)
 │    │
 │    ├── page/                              ── Data Batching (Columnar-Ready) ──
 │    │    ├── Page.java                     Columnar-ready batch interface
 │    │    ├── Block.java                    Single-column data (Arrow-aligned)
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
 │    │    ├── ComputeStage.java             Stage with pipeline + partitioning + planFragment
 │    │    ├── PartitioningScheme.java        Output partitioning (gather, hash, broadcast)
 │    │    └── ExchangeType.java             Enum: GATHER / HASH_REPARTITION / BROADCAST / NONE
 │    │
 │    ├── exchange/                          ── Inter-Stage Data Transfer ──
 │    │    ├── ExchangeManager.java          Creates sink/source operators
 │    │    ├── ExchangeSinkOperator.java      Sends pages downstream
 │    │    ├── ExchangeSourceOperator.java    Receives pages from upstream
 │    │    └── OutputBuffer.java             Back-pressure buffering for pages
 │    │
 │    ├── split/                             ── Data Assignment ──
 │    │    ├── DataUnit.java                 Abstract: unit of data (shard, file, etc.)
 │    │    ├── DataUnitSource.java           Generates DataUnits (shard discovery)
 │    │    └── DataUnitAssignment.java       Assigns DataUnits to nodes
 │    │
 │    ├── planner/                           ── Physical Planning Interfaces ──
 │    │    ├── PhysicalPlanner.java          RelNode → StagedPlan
 │    │    ├── PlanFragmenter.java           Auto stage creation from RelNode tree
 │    │    ├── FragmentationContext.java     Context for fragmentation (nodes, costs)
 │    │    ├── SubPlan.java                  RelNode fragment for data node pushdown
 │    │    └── CostEstimator.java            Row count / size / selectivity estimation
 │    │
 │    └── execution/                         ── Execution Lifecycle ──
 │         ├── QueryExecution.java           Full query lifecycle management
 │         ├── StageExecution.java           Per-stage execution tracking
 │         └── TaskExecution.java            Per-task execution tracking
 │
 └── opensearch/src/main/java/org/opensearch/sql/opensearch/executor/
      ├── DistributedExecutionEngine.java    Routing shell: legacy vs distributed
      │
      └── distributed/
           ├── TransportExecuteDistributedTaskAction.java  Transport handler (data node)
           ├── ExecuteDistributedTaskAction.java           ActionType for routing
           ├── ExecuteDistributedTaskRequest.java           Request wire format
           ├── ExecuteDistributedTaskResponse.java          Response wire format
           │
           ├── split/
           │    └── OpenSearchDataUnit.java   DataUnit impl (index + shard + locality)
           │
           ├── operator/                     ── OpenSearch Operators ──
           │    ├── LuceneScanOperator.java   Direct Lucene _source reads (Weight/Scorer)
           │    ├── LimitOperator.java         Row limit enforcement
           │    ├── ResultCollector.java        Collects pages into row lists
           │    └── FilterToLuceneConverter.java  Filter conditions → Lucene Query
           │
           └── pipeline/
                └── OperatorPipelineExecutor.java  Orchestrates pipeline on data node
```

---

## Class Hierarchy

### DataUnit Model

```
  DataUnit (abstract class)
   ├── getDataUnitId()          Unique identifier
   ├── getPreferredNodes()      Nodes where data is local
   ├── getEstimatedRows()       Row count estimate
   ├── getEstimatedSizeBytes()  Size estimate
   ├── getProperties()          Storage-specific metadata
   └── isRemotelyAccessible()   Default: true
        │
        └── OpenSearchDataUnit (concrete)
             ├── indexName, shardId
             └── isRemotelyAccessible() → false (Lucene requires locality)

  DataUnitSource (interface, AutoCloseable)
   └── getNextBatch(maxBatchSize) → List<DataUnit>

  DataUnitAssignment (interface)
   └── assign(dataUnits, availableNodes) → Map<nodeId, List<DataUnit>>
```

### Block / Page Columnar Model

```
  Page (interface)
   ├── getPositionCount()       Row count
   ├── getChannelCount()        Column count
   ├── getValue(pos, channel)   Cell access
   ├── getBlock(channel)        Columnar access (default: throws)
   ├── getRetainedSizeBytes()   Memory estimate
   └── getRegion(offset, len)   Sub-page slice
        │
        └── RowPage (row-based impl)

  Block (interface)
   ├── getPositionCount()       Row count in this column
   ├── getValue(position)       Value at row
   ├── isNull(position)         Null check
   ├── getRetainedSizeBytes()   Memory estimate
   ├── getRegion(offset, len)   Sub-block slice
   └── getType() → BlockType    BOOLEAN, INT, LONG, FLOAT, DOUBLE, STRING, ...

  Future: ArrowBlock wraps Arrow FieldVector for zero-copy exchange
```

### PlanFragmenter → StagedPlan → ComputeStage

```
  PlanFragmenter (interface)
   └── fragment(RelNode, FragmentationContext) → StagedPlan
        │
        │  FragmentationContext (interface)
        │   ├── getAvailableNodes()
        │   ├── getCostEstimator()
        │   ├── getDataUnitSource(tableName)
        │   ├── getMaxTasksPerStage()
        │   └── getCoordinatorNodeId()
        │
        │  SubPlan (class)
        │   ├── fragmentId
        │   ├── root: RelNode        ← sub-plan for data node execution (pushdown)
        │   ├── outputPartitioning
        │   └── children: List<SubPlan>
        │
        └── StagedPlan
             └── List<ComputeStage> (dependency order: leaves → root)
                  ├── stageId
                  ├── SourceOperatorFactory
                  ├── List<OperatorFactory>
                  ├── PartitioningScheme
                  │    ├── ExchangeType: GATHER | HASH_REPARTITION | BROADCAST | NONE
                  │    └── hashChannels: List<Integer>
                  ├── sourceStageIds (upstream dependencies)
                  ├── List<DataUnit> (data assignments)
                  ├── planFragment: RelNode (nullable — sub-plan for pushdown)
                  └── estimatedRows / estimatedBytes
```

### Exchange Interfaces

```
  ExchangeManager (interface)
   ├── createSink(context, targetStageId, partitioning) → ExchangeSinkOperator
   └── createSource(context, sourceStageId) → ExchangeSourceOperator

  OutputBuffer (interface, AutoCloseable)
   ├── enqueue(Page)           Add page to buffer
   ├── setNoMorePages()        Signal completion
   ├── isFull()                Back-pressure check
   ├── getBufferedBytes()      Buffer size
   ├── abort()                 Discard buffered pages
   ├── isFinished()            All pages consumed
   └── getPartitioningScheme() Output partitioning

  Exchange protocol:
   Current: OpenSearch transport (Netty TCP, StreamOutput/StreamInput)
   Future:  Arrow IPC (ArrowRecordBatch for zero-copy columnar exchange)
```

### Execution Lifecycle

```
  QueryExecution (interface)
   ├── State: PLANNING → STARTING → RUNNING → FINISHING → FINISHED | FAILED
   ├── getQueryId()
   ├── getPlan() → StagedPlan
   ├── getStageExecutions() → List<StageExecution>
   ├── getStats() → QueryStats (totalRows, elapsedTime, planningTime)
   └── cancel()

  StageExecution (interface)
   ├── State: PLANNED → SCHEDULING → RUNNING → FINISHED | FAILED | CANCELLED
   ├── getStage() → ComputeStage
   ├── addDataUnits(List<DataUnit>)
   ├── noMoreDataUnits()
   ├── getTaskExecutions() → Map<nodeId, List<TaskExecution>>
   ├── getStats() → StageStats (totalRows, totalBytes, completedTasks, totalTasks)
   ├── addStateChangeListener(listener)
   └── cancel()

  TaskExecution (interface)
   ├── State: PLANNED → RUNNING → FLUSHING → FINISHED | FAILED | CANCELLED
   ├── getTaskId(), getNodeId()
   ├── getAssignedDataUnits() → List<DataUnit>
   ├── getStats() → TaskStats (processedRows, processedBytes, outputRows, elapsedTime)
   └── cancel()
```

### Operator Framework

```
                   Operator (interface)
                  /         \
     SourceOperator       SinkOperator
     (adds DataUnits)     (terminal)
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
  DataUnit → SourceOperator → Page → Operator → Page → ... → SinkOperator
                                       ↑
                                  OperatorContext (memory, cancellation)
```

---

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `plugins.ppl.distributed.enabled` | `false` | Single toggle: legacy engine (off/default) or distributed (on, not yet implemented) |

**No sub-settings.** When distributed is enabled in the future, the operator pipeline will be the only execution path.

---

## Two Execution Paths (No Fallback)

```
  plugins.ppl.distributed.enabled = false (default)     plugins.ppl.distributed.enabled = true
  ──────────────────────────────────────────────        ──────────────────────────────────────
  PPL → Calcite → DistributedExecutionEngine             PPL → Calcite → DistributedExecutionEngine
                   │                                                      │
                   v                                                      v
          OpenSearchExecutionEngine (legacy)             UnsupportedOperationException
          client.search() (SSB pushdown)                 (execution not yet implemented)
          Single-node coordinator
```
