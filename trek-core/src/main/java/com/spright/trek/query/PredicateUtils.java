package com.spright.trek.query;

import java.util.function.Function;
import java.util.regex.Pattern;
import com.spright.trek.utils.TrekUtils;
import java.util.List;
import java.util.function.Predicate;

public final class PredicateUtils {

  @SuppressWarnings("rawtypes")
  private static final Predicate EMPTY_PREDICATE = (t) -> true;

  @SuppressWarnings("unchecked")
  public static <T> Predicate<T> empty() {
    return (Predicate<T>) EMPTY_PREDICATE;
  }

  public static <T> Predicate<T> newPredicate(final List<Predicate<T>> filters) {
    if (filters.isEmpty()) {
      return empty();
    }
    return (T t) -> filters.stream().allMatch(v -> v.test(t));
  }

  public static <T> Predicate<T> newPredicate(final T v) {
    return new ExcludeNullFilter<>((value) -> v == value);
  }

  public static <T, U> Predicate<T> newPredicate(
          final Function<T, U> f, final Predicate<U> filter) {
    return (T t) -> {
      if (t == null) {
        return false;
      }
      return filter.test(f.apply(t));
    };
  }

  public static Predicate<String> newPredicate(final Pattern ptn) {
    return new ExcludeNullFilter<>((value) -> ptn.matcher(value).find());
  }

  public static Predicate<String> newPredicate(final String ptn) {
    if (TrekUtils.hasWildcardToRegular(ptn)) {
      return newPredicate(Pattern.compile(TrekUtils.wildcardToRegular(ptn)));
    }
    return new ExcludeNullFilter<>(value -> ptn.equals(value));
  }

  public static Predicate<Long> newPredicate(final RangeLong range) {
    return new ExcludeNullFilter<>(value
            -> (value >= range.getMinValue() && value <= range.getMaxValue()));
  }

  public static Predicate<Integer> newPredicate(final RangeInteger range) {
    return new ExcludeNullFilter<>(value
            -> (value >= range.getMinValue() && value <= range.getMaxValue()));
  }

  public static Predicate<Double> newPredicate(final RangeDouble range) {
    return new ExcludeNullFilter<>(value
            -> (value >= range.getMinValue() && value <= range.getMaxValue()));
  }

  private static class ExcludeNullFilter<T> implements Predicate<T> {

    private final Predicate<T> filter;

    ExcludeNullFilter(final Predicate<T> filter) {
      this.filter = filter;
    }

    @Override
    public boolean test(T t) {
      if (t == null) {
        return false;
      }
      return filter.test(t);
    }
  }

  private PredicateUtils() {
  }
}
