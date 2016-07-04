package com.spright.trek.query;

public class RangeInteger extends RangeNumber<Integer> {

  public RangeInteger(final int v) {
    super(v);
  }

  public RangeInteger(final int min, final int max) {
    super(Math.min(min, max), Math.max(min, max));
  }
}
