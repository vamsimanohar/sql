/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.sql.planner.distributed.operator.Operator;
import org.opensearch.sql.planner.distributed.operator.OperatorContext;
import org.opensearch.sql.planner.distributed.page.Page;
import org.opensearch.sql.planner.distributed.page.PageBuilder;

/**
 * Operator that projects (selects) specific fields from input pages.
 *
 * <p>Implements field selection and nested field extraction following the standard operator
 * lifecycle pattern used by LimitOperator and other existing operators.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Field extraction from Page objects
 *   <li>Nested field access using dotted notation (e.g., "user.name")
 *   <li>Memory-efficient page building
 *   <li>Proper operator lifecycle implementation
 * </ul>
 */
@Log4j2
public class ProjectionOperator implements Operator {

  private final List<String> projectedFields;
  private final List<Integer> fieldIndices;
  private final OperatorContext context;

  private Page pendingOutput;
  private boolean inputFinished;

  /**
   * Creates a ProjectionOperator with pre-computed field indices.
   *
   * @param projectedFields the field names to project
   * @param inputFieldNames the field names from the input pages (for index computation)
   * @param context operator context
   */
  public ProjectionOperator(
      List<String> projectedFields, List<String> inputFieldNames, OperatorContext context) {

    this.projectedFields = projectedFields;
    this.context = context;
    this.fieldIndices = computeFieldIndices(projectedFields, inputFieldNames);

    log.debug(
        "Created ProjectionOperator: projectedFields={}, fieldIndices={}",
        projectedFields,
        fieldIndices);
  }

  @Override
  public boolean needsInput() {
    return pendingOutput == null && !inputFinished && !context.isCancelled();
  }

  @Override
  public void addInput(Page page) {
    if (pendingOutput != null) {
      throw new IllegalStateException("Cannot add input when output is pending");
    }

    if (context.isCancelled()) {
      return;
    }

    log.debug(
        "Processing page with {} rows, {} channels",
        page.getPositionCount(),
        page.getChannelCount());

    // Project the page to selected fields
    Page projectedPage = projectPage(page);
    pendingOutput = projectedPage;

    log.debug("Projected to {} channels", projectedPage.getChannelCount());
  }

  @Override
  public Page getOutput() {
    Page output = pendingOutput;
    pendingOutput = null;
    return output;
  }

  @Override
  public boolean isFinished() {
    return inputFinished && pendingOutput == null;
  }

  @Override
  public void finish() {
    inputFinished = true;
    log.debug("ProjectionOperator finished");
  }

  @Override
  public OperatorContext getContext() {
    return context;
  }

  @Override
  public void close() {
    // No resources to clean up
    log.debug("ProjectionOperator closed");
  }

  /** Projects a page to contain only the selected fields. */
  private Page projectPage(Page inputPage) {
    int positionCount = inputPage.getPositionCount();
    int projectedChannelCount = fieldIndices.size();

    // Build new page with selected fields only
    PageBuilder builder = new PageBuilder(projectedChannelCount);

    for (int position = 0; position < positionCount; position++) {
      builder.beginRow();

      for (int projectedChannel = 0; projectedChannel < projectedChannelCount; projectedChannel++) {
        int sourceChannel = fieldIndices.get(projectedChannel);

        Object value;
        if (sourceChannel >= 0 && sourceChannel < inputPage.getChannelCount()) {
          value = inputPage.getValue(position, sourceChannel);

          // Handle nested field extraction if the value is a JSON-like structure
          String projectedFieldName = projectedFields.get(projectedChannel);
          if (projectedFieldName.contains(".") && value != null) {
            value = extractNestedField(value, projectedFieldName);
          }
        } else {
          // Field not found in input - return null
          value = null;
        }

        builder.setValue(projectedChannel, value);
      }

      builder.endRow();
    }

    return builder.build();
  }

  /** Computes field indices for projected fields in the input schema. */
  private List<Integer> computeFieldIndices(
      List<String> projectedFields, List<String> inputFields) {
    List<Integer> indices = new ArrayList<>();

    for (String projectedField : projectedFields) {
      // For nested fields (e.g., "user.name"), look for the base field ("user")
      String baseField = extractBaseField(projectedField);

      int index = inputFields.indexOf(baseField);
      indices.add(index); // -1 if not found, handled in projectPage
    }

    return indices;
  }

  /**
   * Extracts the base field name from a potentially nested field path. Example: "user.name" →
   * "user", "age" → "age"
   */
  private String extractBaseField(String fieldPath) {
    int dotIndex = fieldPath.indexOf('.');
    return (dotIndex > 0) ? fieldPath.substring(0, dotIndex) : fieldPath;
  }

  /**
   * Extracts a nested field value from a JSON-like object structure. Handles dotted field paths
   * like "user.name" or "machine.os".
   */
  private Object extractNestedField(Object value, String fieldPath) {
    if (value == null) {
      return null;
    }

    String[] pathParts = fieldPath.split("\\.");
    Object current = value;

    // Navigate through the nested structure
    for (String part : pathParts) {
      if (current == null) {
        return null;
      }

      if (current instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) current;
        current = map.get(part);
      } else if (current instanceof String) {
        // Try to parse as JSON if it's a string (from _source field)
        try {
          String jsonString = (String) current;
          Map<String, Object> parsed =
              XContentHelper.convertToMap(new BytesArray(jsonString), false, XContentType.JSON)
                  .v2();
          current = parsed.get(part);
        } catch (Exception e) {
          log.debug("Failed to parse JSON for nested field extraction: {}", e.getMessage());
          return null;
        }
      } else {
        // Cannot navigate further
        log.debug(
            "Cannot extract nested field '{}' from non-map object: {}",
            fieldPath,
            current.getClass().getSimpleName());
        return null;
      }
    }

    return current;
  }
}
