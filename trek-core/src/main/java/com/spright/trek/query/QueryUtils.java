package com.spright.trek.query;

import com.spright.trek.DConstants;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class QueryUtils {

  public static int getLimit(final Map<String, String> rawQuery) {
    return QueryUtils.parsePositiveValue(
            rawQuery.get(DConstants.URI_DATA_LIMIT),
            Integer.MAX_VALUE,
            DConstants.DEFAULT_URI_DATA_LIMIT);
  }

  public static int getOffset(final Map<String, String> rawQuery) {
    return QueryUtils.parsePositiveValue(
            rawQuery.get(DConstants.URI_DATA_OFFSET),
            DConstants.DEFAULT_URI_DATA_OFFSET);
  }

  public static Set<OrderKey<String>> getOrderKeys(final Map<String, String> rawQuery) {
    Set<OrderKey<String>> orderKeys = new LinkedHashSet<>();
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_DATA_ORDER_BY))
            .map(v -> new OrderKey<>(v, QueryUtils.parseBoolean(rawQuery.get(
                    DConstants.URI_DATA_ORDER_ASC),
                    DConstants.DEFAULT_URI_DATA_ORDER_ASC)))
            .map(v -> orderKeys.add(v));
    return orderKeys;
  }

  public static Optional<String> parseEmptyString(final String v) {
    return Optional.ofNullable(checkEmptyString(v));
  }

  public static String checkEmptyString(final String v) {
    if (v == null || v.length() == 0) {
      return null;
    } else {
      return v;
    }
  }

  public static boolean parseBoolean(final String value, final boolean defaultValue) {
    if (checkEmptyString(value) == null) {
      return defaultValue;
    }
    if (value.equalsIgnoreCase("false")) {
      return false;
    } else if (value.equalsIgnoreCase("true")) {
      return true;
    } else {
      return defaultValue;
    }
  }

  public static Optional<Long> parsePositiveTime(final DateFormat timeSdf,
          final String value) {
    if (checkEmptyString(value) == null) {
      return Optional.empty();
    }
    try {
      long time = timeSdf.parse(value).getTime();
      if (time >= 0) {
        return Optional.of(time);
      } else {
        return Optional.empty();
      }
    } catch (ParseException e) {
      return Optional.empty();
    }
  }

  public static long parsePositiveTime(final DateFormat timeSdf,
          final String value, final long defaultValue) {
    return parsePositiveTime(timeSdf, value, defaultValue, defaultValue);
  }

  public static long parsePositiveTime(DateFormat timeSdf,
          final String value, final long valueIfNegative, final long defaultValue) {
    if (checkEmptyString(value) == null) {
      return defaultValue;
    }
    try {
      long time = timeSdf.parse(value).getTime();
      if (time >= 0) {
        return time;
      } else {
        return valueIfNegative;
      }
    } catch (ParseException e) {
      return defaultValue;
    }
  }

  private static boolean isValidRangeFormat(final String value) {
    return (checkEmptyString(value) != null)
            && !(value.startsWith("(") ^ value.endsWith(")"));
  }

  public static RangeInteger parsePositiveRange(
          final String specifiedValue, final int defaultValue) {
    return parsePositiveRangeInteger(specifiedValue)
            .orElse(new RangeInteger(defaultValue));
  }

  public static RangeLong parsePositiveRange(final DateFormat timeSdf,
          final String specifiedValue, final long defaultValue) {
    return parsePositiveRangeTime(timeSdf, specifiedValue)
            .orElse(new RangeLong(defaultValue));
  }

  public static RangeLong parsePositiveRange(
          final String specifiedValue, final long defaultValue) {
    return parsePositiveRangeLong(specifiedValue)
            .orElse(new RangeLong(defaultValue));
  }

  public static RangeDouble parsePositiveRange(
          final String specifiedValue, final double defaultValue) {
    return parsePositiveRangeDouble(specifiedValue)
            .orElse(new RangeDouble(defaultValue));
  }

  public static Optional<RangeDouble> parsePositiveRangeDouble(final String value) {
    if (!isValidRangeFormat(value)) {
      return Optional.empty();
    }
    List<Double> values = Stream.of(value.replaceAll(" ", "")
            .replaceAll("\\(", "").replaceAll("\\)", "").split(","))
            .map(arg -> parsePositiveDouble(arg).orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    switch (values.size()) {
      case 1:
        return Optional.of(new RangeDouble(values.get(0), values.get(0)));
      case 2:
        return Optional.of(new RangeDouble(values.get(0), values.get(1)));
      default:
        return Optional.empty();
    }
  }

  public static Optional<RangeInteger> parsePositiveRangeInteger(final String value) {
    if (!isValidRangeFormat(value)) {
      return Optional.empty();
    }
    List<Integer> values = Stream.of(value.replaceAll(" ", "")
            .replaceAll("\\(", "").replaceAll("\\)", "").split(","))
            .map(arg -> parsePositiveInteger(arg).orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    switch (values.size()) {
      case 1:
        return Optional.of(new RangeInteger(values.get(0), values.get(0)));
      case 2:
        return Optional.of(new RangeInteger(values.get(0), values.get(1)));
      default:
        return Optional.empty();
    }
  }

  public static Optional<RangeLong> parsePositiveRangeTime(
          final DateFormat timeSdf, final String value) {
    if (!isValidRangeFormat(value)) {
      return Optional.empty();
    }
    List<Long> values = Stream.of(value.replaceAll(" ", "")
            .replaceAll("\\(", "").replaceAll("\\)", "").split(","))
            .map(arg -> parsePositiveTime(timeSdf, arg).orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    switch (values.size()) {
      case 1:
        return Optional.of(new RangeLong(values.get(0), values.get(0)));
      case 2:
        return Optional.of(new RangeLong(values.get(0), values.get(1)));
      default:
        return Optional.empty();
    }
  }

  public static Optional<RangeLong> parsePositiveRangeLong(
          final String value) {
    if (!isValidRangeFormat(value)) {
      return Optional.empty();
    }
    List<Long> values = Stream.of(value.replaceAll(" ", "")
            .replaceAll("\\(", "").replaceAll("\\)", "").split(","))
            .map(arg -> parsePositiveLong(arg).orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    switch (values.size()) {
      case 1:
        return Optional.of(new RangeLong(values.get(0), values.get(0)));
      case 2:
        return Optional.of(new RangeLong(values.get(0), values.get(1)));
      default:
        return Optional.empty();
    }
  }

  public static Optional<Double> parsePositiveDouble(final String value) {
    if (checkEmptyString(value) == null) {
      return Optional.empty();
    }
    try {
      double vInt = Double.valueOf(value);
      if (vInt >= 0) {
        return Optional.of(vInt);
      } else {
        return Optional.empty();
      }
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  public static Optional<Integer> parsePositiveInteger(final String value) {
    if (checkEmptyString(value) == null) {
      return Optional.empty();
    }
    try {
      int vInt = Integer.valueOf(value);
      if (vInt >= 0) {
        return Optional.of(vInt);
      } else {
        return Optional.empty();
      }
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  public static Optional<Long> parsePositiveLong(final String value) {
    if (checkEmptyString(value) == null) {
      return Optional.empty();
    }
    try {
      long vInt = Long.valueOf(value);
      if (vInt >= 0) {
        return Optional.of(vInt);
      } else {
        return Optional.empty();
      }
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  public static double parsePositiveValue(final String value, final double defaultValue) {
    return parsePositiveValue(value, defaultValue, defaultValue);
  }

  public static double parsePositiveValue(final String value,
          final double valueIfNegative, final double defaultValue) {
    if (checkEmptyString(value) == null) {
      return defaultValue;
    }
    try {
      double vInt = Double.valueOf(value);
      if (vInt >= 0) {
        return vInt;
      } else {
        return valueIfNegative;
      }
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public static long parsePositiveValue(final String value, final long defaultValue) {
    return parsePositiveValue(value, defaultValue, defaultValue);
  }

  public static long parsePositiveValue(final String value,
          final long valueIfNegative, final long defaultValue) {
    if (checkEmptyString(value) == null) {
      return defaultValue;
    }
    try {
      long vInt = Long.valueOf(value);
      if (vInt >= 0) {
        return vInt;
      } else {
        return valueIfNegative;
      }
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public static int parsePositiveValue(final String value, final int defaultValue) {
    return parsePositiveValue(value, defaultValue, defaultValue);
  }

  public static int parsePositiveValue(final String value,
          final int valueIfNegative, final int defaultValue) {
    if (checkEmptyString(value) == null) {
      return defaultValue;
    }
    try {
      int vInt = Integer.valueOf(value);
      if (vInt >= 0) {
        return vInt;
      } else {
        return valueIfNegative;
      }
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private QueryUtils() {
  }
}
