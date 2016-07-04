package com.spright.trek.query;

import java.util.Objects;

public class OrderKey<T extends Comparable<T>> implements Comparable<OrderKey<T>> {

  private final T t;
  private final boolean asc;

  public OrderKey(final T t, final boolean asc) {
    this.t = t;
    this.asc = asc;
  }

  public T getKey() {
    return t;
  }

  public boolean getAsc() {
    return asc;
  }

  @Override
  public int hashCode() {
    return t.hashCode();
  }

  @Override
  public String toString() {
    return t + ":" + asc;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final OrderKey<?> other = (OrderKey<?>) obj;
    return Objects.equals(this.t, other.t);
  }

  @Override
  public int compareTo(OrderKey<T> o) {
    return t.compareTo(o.getKey());
  }
}
