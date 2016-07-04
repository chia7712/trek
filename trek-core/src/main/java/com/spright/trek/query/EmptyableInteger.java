package com.spright.trek.query;

import com.spright.trek.exception.UriParseIOException;

public class EmptyableInteger extends EmptyableValue<Integer> {

  private final Integer value;

  public EmptyableInteger(final int v) {
    super(EmptyableState.HAS_VALUE);
    value = v;
  }

  public EmptyableInteger(final String string) throws UriParseIOException {
    super(string == null ? EmptyableState.NULL
            : string.length() == 0 ? EmptyableState.EMPTY : EmptyableState.HAS_VALUE);
    if (string == null || string.length() == 0) {
      value = null;
      return;
    }
    try {
      value = Integer.valueOf(string);
    } catch (NumberFormatException e) {
      throw new UriParseIOException("Invalid number format:" + string);
    }
  }

  @Override
  protected Integer get() {
    return value;
  }

}
