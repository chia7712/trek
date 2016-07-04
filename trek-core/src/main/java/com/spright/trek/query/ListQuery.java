package com.spright.trek.query;

import java.util.Comparator;
import java.util.Set;
import java.util.function.Predicate;

public interface ListQuery<T extends Comparable<T>, U> {

  int getLimit();

  int getOffset();

  boolean getKeep();

  Set<OrderKey<T>> getOrderKey();

  Predicate<U> getPredicate();

  Comparator<U> getComparator();
}
