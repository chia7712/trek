package com.spright.trek.query;

import java.util.Objects;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class RangeNumber<T extends Number> {

  private static final Log LOG = LogFactory.getLog(RangeNumber.class);
  private final T minValue;
  private final T maxValue;

  public RangeNumber(final T v) {
    this(v, v);
  }

  public RangeNumber(final T minValue, final T maxValue) {
    this.minValue = minValue;
    this.maxValue = maxValue;
    checkValue();
  }

  private void checkValue() {
    if (minValue == null || maxValue == null) {
      String msg = "No valid value!!";
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
  }

  public T getMinValue() {
    return minValue;
  }

  public T getMaxValue() {
    return maxValue;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 37 * hash + Objects.hashCode(this.minValue);
    hash = 37 * hash + Objects.hashCode(this.maxValue);
    return hash;
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
    final RangeNumber<?> other = (RangeNumber<?>) obj;
    if (!Objects.equals(this.minValue, other.minValue)) {
      return false;
    }
    return Objects.equals(this.maxValue, other.maxValue);
  }

  @Override
  public String toString() {
    return "(" + minValue + "," + maxValue + ")";
  }
}
