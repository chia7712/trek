package com.spright.trek.query;

import java.util.Collection;
import java.util.Comparator;

public final class ComparatorUtils {

  /**
   * Checks if the comparator is a empty comparator. True is the comparator's
   * instance type is the {@link EmptyComparator}.
   *
   * @param <T>
   * @param comparator
   * @return
   */
  public static <T> boolean isEmptyComparator(final Comparator<T> comparator) {
    return comparator.getClass().equals(EmptyComparator.class);
  }

  public static <T> Comparator<T> newComparator(final Collection<Comparator<T>> comparators) {
    if (comparators.isEmpty()) {
      return new EmptyComparator<>();
    }
    return (T o1, T o2) -> {
      int rval = 0;
      for (Comparator<T> cmp : comparators) {
        rval = cmp.compare(o1, o2);
        if (rval != 0) {
          return rval;
        }
      }
      return rval;
    };
  }

  /**
   * Use the private class to check the outside class is empty or not.
   *
   * @see ComparatorUtils#isEmptyComparator(java.util.Comparator)
   * @param <T>
   */
  private static class EmptyComparator<T> implements Comparator<T> {

    @Override
    public int compare(T o1, T o2) {
      //Sort by the check order
      return 1;
    }
  }

  private ComparatorUtils() {
  }
}
