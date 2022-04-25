/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.request;

import com.google.common.annotations.VisibleForTesting;

import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.prometheus.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.prometheus.response.OpenSearchResponse;

/**
 * OpenSearch search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation. 2)
 * Indicate the search already done.
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchQueryRequest implements OpenSearchRequest {

  /**
   * Default query timeout in minutes.
   */
  public static final TimeValue DEFAULT_QUERY_TIMEOUT = TimeValue.timeValueMinutes(1L);

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private final IndexName indexName;

  /**
   * Search request source builder.
   */
  private final SearchSourceBuilder sourceBuilder;

  /**
   * Prometheus Query
   */
  private final StringBuilder prometheusQueryBuilder;

  private Long startTime;

  private Long endTime;

  private String step;


  /**
   * ElasticsearchExprValueFactory.
   */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Indicate the search already done.
   */
  private boolean searchDone = false;

  /**
   * Constructor of ElasticsearchQueryRequest.
   */
  public OpenSearchQueryRequest(String indexName, int size,
                                OpenSearchExprValueFactory factory) {
    this(new IndexName(indexName), size, factory);
  }

  /**
   * Constructor of ElasticsearchQueryRequest.
   */
  public OpenSearchQueryRequest(IndexName indexName, int size,
      OpenSearchExprValueFactory factory) {
    this.indexName = indexName;
    this.sourceBuilder = new SearchSourceBuilder();
    this.prometheusQueryBuilder = new StringBuilder();
    this.startTime = new Date().getTime()/1000-3600;
    this.endTime = new Date().getTime()/1000;
    this.step="14";
    sourceBuilder.from(0);
    sourceBuilder.size(size);
    sourceBuilder.timeout(DEFAULT_QUERY_TIMEOUT);
    this.exprValueFactory = factory;
  }

  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    if (searchDone) {
      return new OpenSearchResponse(SearchHits.empty(), exprValueFactory);
    } else {
      searchDone = true;
      return new OpenSearchResponse(searchAction.apply(searchRequest()), exprValueFactory);
    }
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    //do nothing.
  }

  @Override
  public void setStartTime(Long startTime) {
    this.startTime =startTime;
  }

  @Override
  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  @Override
  public void setStep(String step) {
    this.step = step;
  }

  @Override
  public String getStep() {
    if(this.step!=null) {
      return this.step;
    }
    else {
     return  String.valueOf((this.endTime.intValue() - this.startTime.intValue())/11000 + 1);
    }
  }

  /**
   * Generate OpenSearch search request.
   *
   * @return search request
   */
  @VisibleForTesting
  protected SearchRequest searchRequest() {
    return new SearchRequest()
        .indices(indexName.getIndexNames())
        .source(sourceBuilder);
  }
}
