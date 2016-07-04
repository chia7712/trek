package com.spright.trek.query;

public class EmptyableString extends EmptyableValue<String> {

  private final String value;

  public EmptyableString(final String string) {
    super(string == null ? EmptyableState.NULL
            : string.length() == 0 ? EmptyableState.EMPTY : EmptyableState.HAS_VALUE);
    value = string;
  }

  @Override
  protected String get() {
    return value;
  }
}
