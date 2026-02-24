/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * All date/time/timestamp normalization and type coercion for the distributed engine. Methods are
 * pure functions with no state — all static.
 *
 * <p>Handles all OpenSearch built-in date formats (basic_date, basic_date_time, ordinal_date,
 * week_date, t_time, etc.) plus common custom formats. All OpenSearch "date" type fields map to
 * TIMESTAMP in Calcite, and raw _source values are in the original indexed format.
 */
@Log4j2
public final class TemporalValueNormalizer {

  private TemporalValueNormalizer() {}

  /**
   * Normalizes a row's values to match the declared Calcite row type. OpenSearch data nodes may
   * return Integer for fields declared as BIGINT (Long), or Float for DOUBLE fields. Calcite's
   * execution engine expects exact type matches, so we convert here.
   */
  public static Object[] normalizeRowForCalcite(List<Object> row, RelDataType rowType) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    Object[] result = new Object[row.size()];
    for (int i = 0; i < row.size(); i++) {
      Object val = row.get(i);
      if (val != null && i < fields.size()) {
        RelDataType fieldType = fields.get(i).getType();
        if (OpenSearchTypeFactory.isTimeBasedType(fieldType)) {
          val = normalizeTimeBasedValue(val, fieldType);
        } else {
          SqlTypeName sqlType = fieldType.getSqlTypeName();
          val = coerceToCalciteType(val, sqlType);
        }
      }
      result[i] = val;
    }
    return result;
  }

  /**
   * Normalizes a raw _source value for a time-based UDT field. Detects whether the field is DATE,
   * TIMESTAMP, or TIME and converts to the format expected by Calcite UDFs: - TIMESTAMP:
   * "yyyy-MM-dd HH:mm:ss" - DATE: "yyyy-MM-dd" - TIME: "HH:mm:ss"
   */
  public static Object normalizeTimeBasedValue(Object val, RelDataType fieldType) {
    if (val == null) {
      return null;
    }

    ExprType exprType;
    try {
      exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(fieldType);
    } catch (Exception e) {
      exprType = ExprCoreType.TIMESTAMP; // default fallback
    }

    if (exprType == ExprCoreType.TIMESTAMP) {
      return normalizeTimestamp(val);
    } else if (exprType == ExprCoreType.DATE) {
      return normalizeDate(val);
    } else if (exprType == ExprCoreType.TIME) {
      return normalizeTime(val);
    }
    return val;
  }

  /**
   * Normalizes a raw value to "yyyy-MM-dd HH:mm:ss" format for TIMESTAMP fields.
   *
   * <p>Handles ALL OpenSearch built-in date formats including: epoch_millis, date_optional_time,
   * basic_date_time, basic_ordinal_date_time, basic_week_date_time, t_time, basic_t_time,
   * basic_time, date_hour, date_hour_minute, date_hour_minute_second, week_date_time,
   * ordinal_date_time, compact dates (yyyyMMdd), ordinal dates (yyyyDDD, yyyy-DDD), week dates
   * (yyyyWwwd, yyyy-Www-d), partial times (HH, HH:mm), AM/PM times, and more.
   */
  public static String normalizeTimestamp(Object val) {
    String s = val.toString().trim();

    try {
      return normalizeTimestampInternal(s, val);
    } catch (Exception e) {
      log.warn(
          "[Distributed Engine] Failed to normalize timestamp value '{}': {}", s, e.getMessage());
      return s;
    }
  }

  private static String normalizeTimestampInternal(String s, Object val) {
    // 1. Epoch millis as number
    if (val instanceof Number) {
      return formatEpochMillis(((Number) val).longValue());
    }

    // 2. Strip leading T prefix (T-prefixed time formats: basic_t_time, t_time)
    if (s.startsWith("T")) {
      String timeStr = parseTimeComponent(s.substring(1));
      return "1970-01-01 " + timeStr;
    }

    // 3. Handle values containing T separator (datetime formats)
    int tIdx = s.indexOf('T');
    if (tIdx > 0) {
      String datePart = s.substring(0, tIdx);
      String timePart = s.substring(tIdx + 1);
      String normalizedDate = parseDateComponent(datePart);
      String normalizedTime = parseTimeComponent(timePart);
      return normalizedDate + " " + normalizedTime;
    }

    // 4. Handle simple AM/PM time formats (e.g., "09:07:42 AM", "09:07:42 PM")
    // Only match if the part before AM/PM looks like a pure time value (no dashes, no custom text)
    String upper = s.toUpperCase(Locale.ROOT);
    if (upper.endsWith(" AM") || upper.endsWith(" PM")) {
      String timePart = s.substring(0, s.length() - 3).trim();
      if (timePart.matches("[\\d:]+")) {
        boolean isPM = upper.endsWith(" PM");
        return "1970-01-01 " + convertAmPmTime(timePart, isPM);
      }
      // Complex custom format with AM/PM — try to extract date and time
      int spaceIdx = timePart.indexOf(' ');
      if (spaceIdx > 0) {
        String possibleDate = timePart.substring(0, spaceIdx);
        String parsedDate = tryParseDateOnly(possibleDate);
        if (parsedDate != null) {
          // Extract time portion: find HH:mm:ss pattern in the rest
          String rest = timePart.substring(spaceIdx + 1).trim();
          java.util.regex.Matcher m =
              java.util.regex.Pattern.compile("(\\d{2}:\\d{2}:\\d{2})").matcher(rest);
          if (m.find()) {
            boolean isPM = upper.endsWith(" PM");
            String normalizedTime = convertAmPmTime(m.group(1), isPM);
            return parsedDate + " " + normalizedTime;
          }
        }
      }
    }

    // 5. Handle "yyyy-MM-dd HH:mm:ss[.fractional][Z]" space-separated datetime
    if (s.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*")) {
      String result = s;
      if (result.endsWith("Z")) {
        result = result.substring(0, result.length() - 1);
      }
      // Strip non-digit/non-dot suffixes after time (e.g., " ---- AM" in custom formats)
      java.util.regex.Matcher m =
          java.util.regex.Pattern.compile("^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?)")
              .matcher(result);
      if (m.find()) {
        return m.group(1);
      }
      return result.substring(0, Math.min(result.length(), 19));
    }

    // 6. Combined compact: "yyyyMMddHHmmss" (14 digits)
    if (s.length() == 14 && s.matches("\\d{14}")) {
      return formatCompactDate(s.substring(0, 8)) + " " + formatCompactTime(s.substring(8, 14));
    }

    // 7. Combined compact with space: "yyyyMMdd HHmmss" (15 chars)
    if (s.length() == 15 && s.matches("\\d{8} \\d{6}")) {
      return formatCompactDate(s.substring(0, 8)) + " " + formatCompactTime(s.substring(9, 15));
    }

    // 8. Date-only formats (no time component)
    String dateResult = tryParseDateOnly(s);
    if (dateResult != null) {
      return dateResult + " 00:00:00";
    }

    // 9. Time-only formats (no date component) stored in TIMESTAMP field
    String timeResult = tryParseTimeOnly(s);
    if (timeResult != null) {
      return "1970-01-01 " + timeResult;
    }

    // 10. Fallback: try parsing as epoch millis (string, possibly with decimal)
    try {
      long epochMillis = (long) Double.parseDouble(s);
      return formatEpochMillis(epochMillis);
    } catch (NumberFormatException e) {
      // Not numeric
    }

    log.warn("[Distributed Engine] Unrecognized timestamp format, returning as-is: {}", s);
    return s;
  }

  /**
   * Normalizes a raw value to "yyyy-MM-dd" format for DATE fields. Handles: epoch millis, compact
   * date (yyyyMMdd), ordinal dates, week dates, datetime strings.
   */
  public static String normalizeDate(Object val) {
    String s = val.toString().trim();

    try {
      // Epoch millis as number
      if (val instanceof Number) {
        java.time.Instant inst = java.time.Instant.ofEpochMilli(((Number) val).longValue());
        return inst.atOffset(ZoneOffset.UTC).toLocalDate().toString();
      }

      // Try date-only patterns first
      String dateResult = tryParseDateOnly(s);
      if (dateResult != null) {
        return dateResult;
      }

      // Strip time part from datetime with T
      if (s.contains("T")) {
        String datePart = s.substring(0, s.indexOf('T'));
        return parseDateComponent(datePart);
      }

      // Strip time part from datetime with space
      if (s.contains(" ")) {
        String datePart = s.substring(0, s.indexOf(' '));
        String parsed = tryParseDateOnly(datePart);
        if (parsed != null) {
          return parsed;
        }
      }

      // Fallback: try parsing as epoch millis (string, possibly with decimal)
      try {
        long epochMillis = (long) Double.parseDouble(s);
        java.time.Instant inst = java.time.Instant.ofEpochMilli(epochMillis);
        return inst.atOffset(ZoneOffset.UTC).toLocalDate().toString();
      } catch (NumberFormatException e) {
        // Not numeric
      }

      log.warn("[Distributed Engine] Unrecognized date format, returning as-is: {}", s);
      return s;
    } catch (Exception e) {
      log.warn("[Distributed Engine] Failed to normalize date value '{}': {}", s, e.getMessage());
      return s;
    }
  }

  /**
   * Normalizes a raw value to "HH:mm:ss" format for TIME fields. Handles: epoch/time millis,
   * compressed time ("090742.000Z", "090742Z", "090742"), T-prefixed times, HH:mm:ss variants,
   * partial times (HH, HH:mm), AM/PM.
   */
  public static String normalizeTime(Object val) {
    String s = val.toString().trim();

    try {
      // Numeric: treat as time-of-day milliseconds
      if (val instanceof Number) {
        long millis = ((Number) val).longValue();
        int totalSeconds = (int) ((millis / 1000) % 86400);
        return String.format(
            "%02d:%02d:%02d", totalSeconds / 3600, (totalSeconds % 3600) / 60, totalSeconds % 60);
      }

      // Strip leading T (T-prefixed time formats)
      if (s.startsWith("T")) {
        s = s.substring(1);
      }

      return parseTimeComponent(s);
    } catch (Exception e) {
      log.warn("[Distributed Engine] Failed to normalize time value '{}': {}", s, e.getMessage());
      return s;
    }
  }

  // ---- Helper methods for date component parsing ----

  /**
   * Parses a date component string (the portion before T in a datetime, or a standalone date) to
   * "yyyy-MM-dd" format.
   *
   * <p>Handles: "19840412" (compact), "1984-04-12" (ISO), "1984103" (basic ordinal), "1984-103"
   * (ordinal), "1984W154" (basic week), "1984-W15-4" (week), "1984-04" (year-month).
   */
  static String parseDateComponent(String date) {
    String result = tryParseDateOnly(date);
    return result != null ? result : date;
  }

  /**
   * Attempts to parse a date-only string to "yyyy-MM-dd". Returns null if the format is not
   * recognized.
   */
  private static String tryParseDateOnly(String s) {
    // Compact date: "19840412" (8 digits, yyyyMMdd)
    if (s.length() == 8 && s.matches("\\d{8}")) {
      return s.substring(0, 4) + "-" + s.substring(4, 6) + "-" + s.substring(6, 8);
    }

    // ISO date: "1984-04-12" (yyyy-MM-dd)
    if (s.length() == 10 && s.matches("\\d{4}-\\d{2}-\\d{2}")) {
      return s;
    }

    // Basic ordinal date: "1984103" (7 digits, yyyyDDD)
    if (s.length() == 7 && s.matches("\\d{7}")) {
      try {
        int year = Integer.parseInt(s.substring(0, 4));
        int dayOfYear = Integer.parseInt(s.substring(4));
        LocalDate date = LocalDate.ofYearDay(year, dayOfYear);
        return date.toString();
      } catch (Exception e) {
        // Fall through
      }
    }

    // Ordinal date with dash: "1984-103" (yyyy-DDD)
    if (s.matches("\\d{4}-\\d{1,3}") && !s.matches("\\d{4}-\\d{2}-.*")) {
      try {
        String[] parts = s.split("-");
        int year = Integer.parseInt(parts[0]);
        int dayOfYear = Integer.parseInt(parts[1]);
        LocalDate date = LocalDate.ofYearDay(year, dayOfYear);
        return date.toString();
      } catch (Exception e) {
        // Fall through
      }
    }

    // Basic week date: "1984W154" (yyyyWwwd — year + W + 2-digit week + 1-digit day)
    // Convert to ISO format "1984-W15-4" and parse with ISO_WEEK_DATE
    if (s.matches("\\d{4}W\\d{2,3}")) {
      try {
        String isoWeek;
        if (s.length() == 8) { // "1984W154" → "1984-W15-4"
          isoWeek = s.substring(0, 4) + "-W" + s.substring(5, 7) + "-" + s.substring(7);
        } else { // "1984W15" → "1984-W15-1" (default to Monday)
          isoWeek = s.substring(0, 4) + "-W" + s.substring(5) + "-1";
        }
        LocalDate date = LocalDate.parse(isoWeek, DateTimeFormatter.ISO_WEEK_DATE);
        return date.toString();
      } catch (Exception e) {
        // Fall through
      }
    }

    // ISO week date: "1984-W15-4" (yyyy-Www-d)
    if (s.matches("\\d{4}-W\\d{2}-\\d")) {
      try {
        LocalDate date = LocalDate.parse(s, DateTimeFormatter.ISO_WEEK_DATE);
        return date.toString();
      } catch (DateTimeParseException e) {
        // Fall through
      }
    }

    // ISO week date without day: "1984-W15"
    if (s.matches("\\d{4}-W\\d{2}")) {
      try {
        LocalDate date = LocalDate.parse(s + "-1", DateTimeFormatter.ISO_WEEK_DATE);
        return date.toString();
      } catch (DateTimeParseException e) {
        // Fall through
      }
    }

    return null;
  }

  // ---- Helper methods for time component parsing ----

  /**
   * Parses a time component string to "HH:mm:ss[.fractional]" format, preserving sub-second
   * precision.
   *
   * <p>Handles: "090742.000Z" (compact with millis and Z), "090742Z" (compact with Z), "090742"
   * (compact), "09:07:42.000Z" (colon-separated with millis/Z), "09:07:42Z", "09:07:42", "09:07:42
   * AM", "09:07" (HH:mm), "09" (HH only).
   */
  static String parseTimeComponent(String time) {
    String s = time.trim();

    // Strip trailing Z
    if (s.endsWith("Z")) {
      s = s.substring(0, s.length() - 1);
    }

    // Extract and preserve fractional seconds
    String fractional = "";
    int dotIdx = s.indexOf('.');
    if (dotIdx > 0) {
      fractional = s.substring(dotIdx);
      s = s.substring(0, dotIdx);
    }

    // Handle AM/PM
    String upper = s.toUpperCase(Locale.ROOT);
    if (upper.endsWith(" AM") || upper.endsWith(" PM")) {
      String timePart = s.substring(0, s.length() - 3).trim();
      boolean isPM = upper.endsWith(" PM");
      return convertAmPmTime(timePart, isPM);
    }

    // Compact time: "090742" (HHmmss, 6 digits)
    if (s.length() == 6 && s.matches("\\d{6}")) {
      return s.substring(0, 2) + ":" + s.substring(2, 4) + ":" + s.substring(4, 6) + fractional;
    }

    // Compact time without seconds: "0907" (HHmm, 4 digits)
    if (s.length() == 4 && s.matches("\\d{4}")) {
      return s.substring(0, 2) + ":" + s.substring(2, 4) + ":00";
    }

    // Full colon time: "09:07:42"
    if (s.matches("\\d{2}:\\d{2}:\\d{2}")) {
      return s + fractional;
    }

    // Partial colon time: "09:07" (HH:mm)
    if (s.matches("\\d{2}:\\d{2}")) {
      return s + ":00";
    }

    // Hour only: "09" (2 digits)
    if (s.length() == 2 && s.matches("\\d{2}")) {
      return s + ":00:00";
    }

    // Single digit hour: "9"
    if (s.length() == 1 && s.matches("\\d")) {
      return "0" + s + ":00:00";
    }

    log.warn("[Distributed Engine] Unrecognized time format: {}", time);
    return s + fractional;
  }

  /** Tries to parse a time-only string. Returns "HH:mm:ss" format or null if not a time pattern. */
  private static String tryParseTimeOnly(String s) {
    // Compressed time patterns (no T prefix)
    if (s.matches("\\d{6}(\\.\\d+)?Z?")) {
      return s.substring(0, 2) + ":" + s.substring(2, 4) + ":" + s.substring(4, 6);
    }

    // Colon-separated time with optional millis/Z: "09:07:42.000Z", "09:07:42Z", "09:07:42"
    if (s.matches("\\d{2}:\\d{2}:\\d{2}.*")) {
      return s.length() > 8 ? s.substring(0, 8) : s;
    }

    // Partial time: "09:07" (HH:mm)
    if (s.matches("\\d{2}:\\d{2}")) {
      return s + ":00";
    }

    // Hour only: "09" (2 digits, must be <= 23 to be a valid hour)
    if (s.length() == 2 && s.matches("\\d{2}")) {
      int hour = Integer.parseInt(s);
      if (hour <= 23) {
        return s + ":00:00";
      }
    }

    return null;
  }

  /** Converts a 12-hour AM/PM time to 24-hour "HH:mm:ss" format. */
  private static String convertAmPmTime(String timePart, boolean isPM) {
    // Parse the time component (may have colons or not)
    String normalized = parseTimeComponent(timePart);
    String[] parts = normalized.split(":");
    if (parts.length >= 1) {
      int hour = Integer.parseInt(parts[0]);
      if (isPM && hour < 12) hour += 12;
      if (!isPM && hour == 12) hour = 0;
      return String.format(
          "%02d:%s:%s",
          hour, parts.length >= 2 ? parts[1] : "00", parts.length >= 3 ? parts[2] : "00");
    }
    return normalized;
  }

  /** Formats a compact date string "yyyyMMdd" to "yyyy-MM-dd". */
  private static String formatCompactDate(String compact) {
    return compact.substring(0, 4) + "-" + compact.substring(4, 6) + "-" + compact.substring(6, 8);
  }

  /** Formats a compact time string "HHmmss" to "HH:mm:ss". */
  private static String formatCompactTime(String compact) {
    return compact.substring(0, 2) + ":" + compact.substring(2, 4) + ":" + compact.substring(4, 6);
  }

  /**
   * Converts a value to the proper ExprValue for date/timestamp/time fields. Handles: - Java
   * temporal types (java.sql.Date, Time, Timestamp, java.time.*) - String dates in various formats
   * - Long epoch millis - String epoch millis
   */
  public static ExprValue convertToTimestampExprValue(Object val, ExprType resolvedType) {
    // Handle Java temporal types directly (Calcite may return these)
    if (val instanceof java.sql.Date
        || val instanceof java.sql.Time
        || val instanceof java.sql.Timestamp
        || val instanceof java.time.LocalDate
        || val instanceof java.time.LocalTime
        || val instanceof java.time.LocalDateTime
        || val instanceof java.time.Instant) {
      return ExprValueUtils.fromObjectValue(val);
    }

    ExprType type = resolvedType != null ? resolvedType : ExprCoreType.TIMESTAMP;

    if (val instanceof String s) {
      if (type == ExprCoreType.TIME) {
        return ExprValueUtils.fromObjectValue(normalizeTime(s), ExprCoreType.TIME);
      }
      if (type == ExprCoreType.DATE) {
        return ExprValueUtils.fromObjectValue(normalizeDate(s), ExprCoreType.DATE);
      }
      // TIMESTAMP
      return ExprValueUtils.fromObjectValue(normalizeTimestamp(s), ExprCoreType.TIMESTAMP);
    } else if (val instanceof Number n) {
      if (type == ExprCoreType.TIME) {
        return ExprValueUtils.fromObjectValue(normalizeTime(val), ExprCoreType.TIME);
      }
      if (type == ExprCoreType.DATE) {
        return ExprValueUtils.fromObjectValue(normalizeDate(val), ExprCoreType.DATE);
      }
      String formatted = formatEpochMillis(n.longValue());
      return ExprValueUtils.fromObjectValue(formatted, ExprCoreType.TIMESTAMP);
    }
    return ExprValueUtils.fromObjectValue(val);
  }

  /** Formats epoch millis as "yyyy-MM-dd HH:mm:ss" timestamp string. */
  public static String formatEpochMillis(long epochMillis) {
    java.time.Instant instant = java.time.Instant.ofEpochMilli(epochMillis);
    LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    return ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }

  /**
   * Coerces a value to the expected Calcite Java type for the given SQL type. Handles: BIGINT →
   * Long, DOUBLE → Double, INTEGER → Integer, FLOAT → Float, SMALLINT → Short.
   */
  public static Object coerceToCalciteType(Object val, SqlTypeName sqlType) {
    if (val == null) {
      return null;
    }
    return switch (sqlType) {
      case BIGINT -> {
        if (val instanceof Number n) {
          yield n.longValue();
        }
        yield val;
      }
      case INTEGER -> {
        if (val instanceof Number n) {
          yield n.intValue();
        }
        yield val;
      }
      case DOUBLE -> {
        if (val instanceof Number n) {
          yield n.doubleValue();
        }
        yield val;
      }
      case FLOAT, REAL -> {
        if (val instanceof Number n) {
          yield n.floatValue();
        }
        yield val;
      }
      case SMALLINT -> {
        if (val instanceof Number n) {
          yield n.shortValue();
        }
        yield val;
      }
      case TINYINT -> {
        if (val instanceof Number n) {
          yield n.byteValue();
        }
        yield val;
      }
      case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
        // Calcite internal representation for TIMESTAMP: Long (millis since epoch)
        if (val instanceof String s) {
          yield parseTimestampToEpochMillis(s);
        }
        if (val instanceof Number n) {
          yield n.longValue();
        }
        yield val;
      }
      case DATE -> {
        // Calcite internal representation for DATE: Integer (days since epoch)
        if (val instanceof String s) {
          yield parseDateToEpochDays(s);
        }
        if (val instanceof Number n) {
          yield n.intValue();
        }
        yield val;
      }
      default -> val;
    };
  }

  /**
   * Parses a date/timestamp string to epoch milliseconds. Handles formats: "2018-06-23",
   * "2018-06-23 12:30:00", "2018-06-23T12:30:00", and epoch millis as strings.
   */
  public static long parseTimestampToEpochMillis(String s) {
    try {
      // Try ISO datetime with time (e.g., "2018-06-23T12:30:00")
      LocalDateTime ldt = LocalDateTime.parse(s, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
      return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    } catch (DateTimeParseException e1) {
      try {
        // Try datetime with space separator (e.g., "2018-06-23 12:30:00")
        LocalDateTime ldt =
            LocalDateTime.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
      } catch (DateTimeParseException e2) {
        try {
          // Try date only (e.g., "2018-06-23")
          LocalDate ld = LocalDate.parse(s, DateTimeFormatter.ISO_LOCAL_DATE);
          return ld.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (DateTimeParseException e3) {
          try {
            // Try epoch millis as string
            return Long.parseLong(s);
          } catch (NumberFormatException e4) {
            log.warn("[Distributed Engine] Could not parse timestamp string: {}", s);
            return 0L;
          }
        }
      }
    }
  }

  /** Parses a date string to epoch days (days since 1970-01-01). Handles format: "2018-06-23". */
  public static int parseDateToEpochDays(String s) {
    try {
      LocalDate ld = LocalDate.parse(s, DateTimeFormatter.ISO_LOCAL_DATE);
      return (int) ld.toEpochDay();
    } catch (DateTimeParseException e) {
      log.warn("[Distributed Engine] Could not parse date string: {}", s);
      return 0;
    }
  }
}
