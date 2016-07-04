package com.spright.trek.task;

import java.util.Optional;
import org.apache.hadoop.hbase.util.Bytes;

public enum TaskState {
  PENDING((short) 0, "pending"),
  RUNNING((short) 1, "running"),
  FAILED((short) 2, "failed"),
  SUCCEED((short) 3, "succeed"),
  ABORT((short) 4, "aborted");
  private final byte[] code;
  private final String description;

  TaskState(final short code, final String description) {
    this.code = Bytes.toBytes(code);
    this.description = description;
  }

  public byte[] getCode() {
    return code;
  }

  public String getDescription() {
    return description;
  }

  public static TaskState check(final String name) {
    if (name == null || name.length() == 0) {
      return null;
    }
    for (TaskState state : TaskState.values()) {
      if (state.getDescription().equalsIgnoreCase(name)) {
        return state;
      }
    }
    return null;
  }

  public static TaskState check(final byte[] code) {
    if (code == null || code.length == 0) {
      return null;
    }
    for (TaskState state : TaskState.values()) {
      if (Bytes.compareTo(code, state.getCode()) == 0) {
        return state;
      }
    }
    return null;
  }

  public static Optional<TaskState> find(byte[] code) {
    return Optional.ofNullable(check(code));
  }

  public static Optional<TaskState> find(final String name) {
    return Optional.ofNullable(check(name));
  }
}
