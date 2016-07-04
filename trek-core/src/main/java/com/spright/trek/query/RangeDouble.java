package com.spright.trek.query;

public class RangeDouble extends RangeNumber<Double> {

  public RangeDouble(final double v) {
    super(v);
  }

  public RangeDouble(final double min, final double max) {
    super(Math.min(min, max), Math.max(min, max));
  }
}
