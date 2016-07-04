package com.spright.trek.datasystem.request;

import java.util.Optional;

public enum DataType {
  FILE, DIRECTORY, OTHERS;

  public static Optional<DataType> find(final String name) {
    for (DataType type : DataType.values()) {
      if (type.name().equalsIgnoreCase(name)) {
        return Optional.of(type);
      }
    }
    return Optional.empty();
  }
}
