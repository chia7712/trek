package com.spright.trek.loadbalancer;

import java.util.Optional;

public enum Operation {
  READ((byte) 0),
  WRITE((byte) 1),
  DELETE((byte) 2),
  LIST((byte) 3);
  private final byte code;

  Operation(final byte code) {
    this.code = code;
  }

  public byte getCode() {
    return code;
  }

  public static Optional<Operation> find(final byte code) {
    for (Operation op : Operation.values()) {
      if (op.getCode() == code) {
        return Optional.of(op);
      }
    }
    return Optional.empty();
  }
}
