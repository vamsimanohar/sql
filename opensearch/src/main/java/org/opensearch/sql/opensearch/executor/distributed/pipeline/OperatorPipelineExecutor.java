/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.pipeline;

import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.sql.opensearch.executor.distributed.ExecuteDistributedTaskRequest;
import org.opensearch.sql.opensearch.executor.distributed.operator.LimitOperator;
import org.opensearch.sql.opensearch.executor.distributed.operator.LuceneScanOperator;
import org.opensearch.sql.opensearch.executor.distributed.operator.ResultCollector;
import org.opensearch.sql.planner.distributed.operator.OperatorContext;
import org.opensearch.sql.planner.distributed.page.Page;

/**
 * Orchestrates operator pipeline execution on a data node. Creates a LuceneScanOperator for each
 * assigned shard, pipes output through a LimitOperator, and collects results.
 */
@Log4j2
public class OperatorPipelineExecutor {

  private OperatorPipelineExecutor() {}

  /**
   * Executes the operator pipeline for the given request.
   *
   * @param indicesService used to resolve IndexShard instances
   * @param request contains index name, shard IDs, field names, and limit
   * @return the collected field names and rows
   */
  public static OperatorPipelineResult execute(
      IndicesService indicesService, ExecuteDistributedTaskRequest request) {

    String indexName = request.getIndexName();
    List<Integer> shardIds = request.getShardIds();
    List<String> fieldNames = request.getFieldNames();
    int queryLimit = request.getQueryLimit();

    log.info(
        "[Operator Pipeline] Executing on shards {} for index: {}, fields: {}, limit: {}",
        shardIds,
        indexName,
        fieldNames,
        queryLimit);

    ResultCollector collector = new ResultCollector(fieldNames);
    int remainingLimit = queryLimit;

    for (int shardId : shardIds) {
      if (remainingLimit <= 0) {
        break;
      }

      IndexShard indexShard = resolveIndexShard(indicesService, indexName, shardId);
      if (indexShard == null) {
        log.warn("[Operator Pipeline] Could not resolve shard {}/{}", indexName, shardId);
        continue;
      }

      OperatorContext ctx = OperatorContext.createDefault("lucene-scan-" + shardId);

      try (LuceneScanOperator source = new LuceneScanOperator(indexShard, fieldNames, 1024, ctx)) {

        LimitOperator limit = new LimitOperator(remainingLimit, ctx);

        // Pull loop: source → limit → collector
        while (!source.isFinished() && !limit.isFinished()) {
          Page page = source.getOutput();
          if (page != null) {
            limit.addInput(page);
            Page limited = limit.getOutput();
            if (limited != null) {
              collector.addPage(limited);
            }
          }
        }

        // Flush any remaining output from limit
        limit.finish();
        Page remaining = limit.getOutput();
        if (remaining != null) {
          collector.addPage(remaining);
        }

        limit.close();
      } catch (Exception e) {
        log.error("[Operator Pipeline] Error processing shard {}/{}", indexName, shardId, e);
        throw new RuntimeException(
            "Operator pipeline failed on shard " + indexName + "/" + shardId, e);
      }

      remainingLimit = queryLimit - collector.getRows().size();
    }

    log.info(
        "[Operator Pipeline] Completed - collected {} rows from {} shards",
        collector.getRows().size(),
        shardIds.size());

    return new OperatorPipelineResult(collector.getFieldNames(), collector.getRows());
  }

  private static IndexShard resolveIndexShard(
      IndicesService indicesService, String indexName, int shardId) {
    for (IndexService indexService : indicesService) {
      if (indexService.index().getName().equals(indexName)) {
        try {
          return indexService.getShard(shardId);
        } catch (Exception e) {
          log.warn(
              "[Operator Pipeline] Shard {} not found on this node for index: {}",
              shardId,
              indexName);
          return null;
        }
      }
    }
    log.warn("[Operator Pipeline] Index {} not found on this node", indexName);
    return null;
  }

  /** Result of operator pipeline execution containing field names and row data. */
  public static class OperatorPipelineResult {
    private final List<String> fieldNames;
    private final List<List<Object>> rows;

    public OperatorPipelineResult(List<String> fieldNames, List<List<Object>> rows) {
      this.fieldNames = fieldNames;
      this.rows = rows;
    }

    public List<String> getFieldNames() {
      return fieldNames;
    }

    public List<List<Object>> getRows() {
      return rows;
    }
  }
}
