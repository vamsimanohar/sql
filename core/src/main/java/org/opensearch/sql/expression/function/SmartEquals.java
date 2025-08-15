/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.PPLFuncImpTable.FunctionImp2;

/**
 * Smart equals operator that automatically handles:
 * 1. Standard equality for same-type comparisons
 * 2. Wildcard pattern matching when * or ? are detected in strings
 * 3. Automatic type conversion for numeric/date/IP fields with string patterns
 * 4. Both field=value and value=field syntax
 * 
 * Wildcard support:
 * - * matches zero or more characters
 * - ? matches exactly one character
 * 
 * Examples:
 * - name='John' -> exact match
 * - name='J*' -> wildcard match (names starting with J)
 * - age='3*' -> convert age to string, then wildcard match
 * - '192.168.*'=ip_field -> reverse wildcard match on IP field
 */
public class SmartEquals implements FunctionImp2 {

  @Override
  public RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2) {
    // Check if either argument is a string literal with wildcards
    String wildcardPattern = extractWildcardPattern(arg1, arg2);
    
    if (wildcardPattern != null) {
      // Determine which argument is the field and which is the pattern
      boolean patternOnRight = isStringLiteral(arg2);
      RexNode field = patternOnRight ? arg1 : arg2;
      
      // Convert wildcards to SQL LIKE pattern
      String sqlPattern = convertToSqlPattern(wildcardPattern);
      RexNode patternNode = builder.makeLiteral(sqlPattern);
      
      // If field is not a string, cast it to string first
      if (!SqlTypeFamily.CHARACTER.contains(field.getType())) {
        field = builder.makeCast(
            builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 
            field);
      }
      
      // Use ILIKE for case-insensitive wildcard matching
      return builder.makeCall(SqlLibraryOperators.ILIKE, field, patternNode);
    }
    
    // Handle mixed-type comparisons without wildcards
    if (isMixedTypeComparison(arg1, arg2)) {
      // For mixed types without wildcards, attempt type coercion
      // This allows comparisons like age='32' (exact match)
      return handleMixedTypeEquals(builder, arg1, arg2);
    }
    
    // Standard same-type equality
    return builder.makeCall(SqlStdOperatorTable.EQUALS, arg1, arg2);
  }

  /**
   * Get type checker that accepts both same-type and mixed-type comparisons
   */
  public PPLTypeChecker getTypeChecker() {
    return PPLTypeChecker.wrapUDT(
        List.of(
            // Same-type comparisons
            List.of(ExprCoreType.BOOLEAN, ExprCoreType.BOOLEAN),
            List.of(ExprCoreType.INTEGER, ExprCoreType.INTEGER),
            List.of(ExprCoreType.LONG, ExprCoreType.LONG),
            List.of(ExprCoreType.FLOAT, ExprCoreType.FLOAT),
            List.of(ExprCoreType.DOUBLE, ExprCoreType.DOUBLE),
            List.of(ExprCoreType.STRING, ExprCoreType.STRING),
            List.of(ExprCoreType.DATE, ExprCoreType.DATE),
            List.of(ExprCoreType.TIME, ExprCoreType.TIME),
            List.of(ExprCoreType.TIMESTAMP, ExprCoreType.TIMESTAMP),
            List.of(ExprCoreType.IP, ExprCoreType.IP),
            
            // Mixed-type comparisons for smart handling
            // Numeric types with strings
            List.of(ExprCoreType.INTEGER, ExprCoreType.STRING),
            List.of(ExprCoreType.LONG, ExprCoreType.STRING),
            List.of(ExprCoreType.FLOAT, ExprCoreType.STRING),
            List.of(ExprCoreType.DOUBLE, ExprCoreType.STRING),
            List.of(ExprCoreType.STRING, ExprCoreType.INTEGER),
            List.of(ExprCoreType.STRING, ExprCoreType.LONG),
            List.of(ExprCoreType.STRING, ExprCoreType.FLOAT),
            List.of(ExprCoreType.STRING, ExprCoreType.DOUBLE),
            
            // Date/time types with strings
            List.of(ExprCoreType.DATE, ExprCoreType.STRING),
            List.of(ExprCoreType.STRING, ExprCoreType.DATE),
            List.of(ExprCoreType.TIME, ExprCoreType.STRING),
            List.of(ExprCoreType.STRING, ExprCoreType.TIME),
            List.of(ExprCoreType.TIMESTAMP, ExprCoreType.STRING),
            List.of(ExprCoreType.STRING, ExprCoreType.TIMESTAMP),
            
            // IP type with strings
            List.of(ExprCoreType.IP, ExprCoreType.STRING),
            List.of(ExprCoreType.STRING, ExprCoreType.IP)
        ));
  }

  /**
   * Extract wildcard pattern from either argument if it's a string literal with wildcards
   */
  private String extractWildcardPattern(RexNode arg1, RexNode arg2) {
    if (isStringLiteral(arg1)) {
      String value = ((RexLiteral) arg1).getValueAs(String.class);
      if (value != null && containsWildcards(value)) {
        return value;
      }
    }
    if (isStringLiteral(arg2)) {
      String value = ((RexLiteral) arg2).getValueAs(String.class);
      if (value != null && containsWildcards(value)) {
        return value;
      }
    }
    return null;
  }

  /**
   * Check if a RexNode is a string literal
   */
  private boolean isStringLiteral(RexNode node) {
    return node.isA(SqlKind.LITERAL) && SqlTypeFamily.CHARACTER.contains(node.getType());
  }

  /**
   * Check if string contains OpenSearch wildcards
   */
  private boolean containsWildcards(String value) {
    return value.contains("*") || value.contains("?");
  }

  /**
   * Convert OpenSearch wildcards to SQL LIKE wildcards
   * * -> % (zero or more chars)
   * ? -> _ (exactly one char)
   */
  private String convertToSqlPattern(String value) {
    return value.replace("*", "%").replace("?", "_");
  }

  /**
   * Check if this is a mixed-type comparison
   */
  private boolean isMixedTypeComparison(RexNode arg1, RexNode arg2) {
    SqlTypeFamily family1 = arg1.getType().getSqlTypeName().getFamily();
    SqlTypeFamily family2 = arg2.getType().getSqlTypeName().getFamily();
    
    // Check if one is string and the other is not
    return (SqlTypeFamily.CHARACTER.equals(family1) && !SqlTypeFamily.CHARACTER.equals(family2))
        || (!SqlTypeFamily.CHARACTER.equals(family1) && SqlTypeFamily.CHARACTER.equals(family2));
  }

  /**
   * Handle mixed-type equals without wildcards (e.g., age='32')
   */
  private RexNode handleMixedTypeEquals(RexBuilder builder, RexNode arg1, RexNode arg2) {
    // Determine which is string and which is not
    boolean arg1IsString = SqlTypeFamily.CHARACTER.contains(arg1.getType());
    
    if (arg1IsString) {
      // Try to cast string to the type of arg2
      if (SqlTypeFamily.NUMERIC.contains(arg2.getType())) {
        // For numeric comparison, try to parse the string as a number
        try {
          if (arg1.isA(SqlKind.LITERAL)) {
            String strValue = ((RexLiteral) arg1).getValueAs(String.class);
            // If it's a valid number, cast it to the numeric type
            if (strValue != null && isNumeric(strValue)) {
              RexNode castedArg1 = builder.makeCast(arg2.getType(), arg1);
              return builder.makeCall(SqlStdOperatorTable.EQUALS, castedArg1, arg2);
            }
          }
        } catch (Exception e) {
          // Fall through to string comparison
        }
      }
      // Otherwise, cast arg2 to string
      RexNode castedArg2 = builder.makeCast(
          builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), arg2);
      return builder.makeCall(SqlStdOperatorTable.EQUALS, arg1, castedArg2);
    } else {
      // arg2 is string, arg1 is not
      if (SqlTypeFamily.NUMERIC.contains(arg1.getType())) {
        // For numeric comparison, try to parse the string as a number
        try {
          if (arg2.isA(SqlKind.LITERAL)) {
            String strValue = ((RexLiteral) arg2).getValueAs(String.class);
            // If it's a valid number, cast it to the numeric type
            if (strValue != null && isNumeric(strValue)) {
              RexNode castedArg2 = builder.makeCast(arg1.getType(), arg2);
              return builder.makeCall(SqlStdOperatorTable.EQUALS, arg1, castedArg2);
            }
          }
        } catch (Exception e) {
          // Fall through to string comparison
        }
      }
      // Otherwise, cast arg1 to string
      RexNode castedArg1 = builder.makeCast(
          builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), arg1);
      return builder.makeCall(SqlStdOperatorTable.EQUALS, castedArg1, arg2);
    }
  }

  /**
   * Check if a string represents a valid number
   */
  private boolean isNumeric(String str) {
    try {
      Double.parseDouble(str);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}