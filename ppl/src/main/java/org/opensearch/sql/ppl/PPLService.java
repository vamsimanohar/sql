/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.SourceNativeQuery;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.utils.LogUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstExpressionBuilder;
import org.opensearch.sql.ppl.utils.PPLQueryDataAnonymizer;
import org.opensearch.sql.ppl.utils.UnresolvedPlanHelper;
import org.opensearch.sql.storage.StorageEngine;

@RequiredArgsConstructor
public class PPLService {
  private final PPLSyntaxParser parser;

  private final StorageEngine openSearchStorageEngine;

  private final ExecutionEngine openSearchExecutionEngine;

  private final BuiltinFunctionRepository repository;

  private final CatalogService catalogService;

  private final PPLQueryDataAnonymizer anonymizer = new PPLQueryDataAnonymizer();

  private String connector = "opensearch";

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Execute the {@link PPLQueryRequest}, using {@link ResponseListener} to get response.
   *
   * @param request  {@link PPLQueryRequest}
   * @param listener {@link ResponseListener}
   */
  public void execute(PPLQueryRequest request, ResponseListener<QueryResponse> listener) {
    try {
      ExecutionEngine executionEngine =
          catalogService.getExecutionEngine(connector).orElse(openSearchExecutionEngine);
      executionEngine.execute(plan(request), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Explain the query in {@link PPLQueryRequest} using {@link ResponseListener} to
   * get and format explain response.
   *
   * @param request  {@link PPLQueryRequest}
   * @param listener {@link ResponseListener} for explain response
   */
  public void explain(PPLQueryRequest request, ResponseListener<ExplainResponse> listener) {
    try {
      ExecutionEngine executionEngine =
          catalogService.getExecutionEngine(connector).orElse(openSearchExecutionEngine);
      executionEngine.explain(plan(request), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private PhysicalPlan plan(PPLQueryRequest request) {
    // 1.Parse query and convert parse tree (CST) to abstract syntax tree (AST)
    ParseTree cst = parser.analyzeSyntax(request.getRequest());
    UnresolvedPlan ast = cst.accept(
        new AstBuilder(new AstExpressionBuilder(), catalogService, request.getRequest()));
    LOG.info("[{}] Incoming request {}", LogUtils.getRequestId(), anonymizer.anonymizeData(ast));

    setConnector(ast);
    StorageEngine storageEngine =
        catalogService.getStorageEngine(connector).orElse(openSearchStorageEngine);

    // 2.Analyze abstract syntax to generate logical plan
    LogicalPlan logicalPlan =
        new Analyzer(new ExpressionAnalyzer(repository), storageEngine).analyze(
            UnresolvedPlanHelper.addSelectAll(ast),
            new AnalysisContext());

    // 3.Generate optimal physical plan from logical plan
    return new Planner(storageEngine, LogicalPlanOptimizer.create(new DSL(repository)))
        .plan(logicalPlan);
  }

  private void setConnector(UnresolvedPlan unresolvedPlan) {
    while (unresolvedPlan != null) {
      if (unresolvedPlan instanceof Relation) {
        connector = ((Relation) unresolvedPlan).getCatalogName();
        break;
      } else if (unresolvedPlan instanceof SourceNativeQuery) {
        connector = ((SourceNativeQuery) unresolvedPlan).getCatalogName();
        break;
      }
      unresolvedPlan = (UnresolvedPlan) unresolvedPlan.getChild().get(0);
    }
  }

}
