package com.spright.trek.query;

public class RangeLong extends RangeNumber<Long> {

  public RangeLong(final long v) {
    super(v);
  }

  public RangeLong(final long min, final long max) {
    super(Math.min(min, max), Math.max(min, max));
  }
}
