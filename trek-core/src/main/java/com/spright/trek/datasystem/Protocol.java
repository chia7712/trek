package com.spright.trek.datasystem;

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.util.Bytes;

public enum Protocol {
  HBASE((short) 0),
  HDFS((short) 1),
  LOCAL((short) 2),
  FTP((short) 3),
  SMB((short) 4),
  FILE((short) 5),
  HTTP((short) 6);

  public static Optional<Protocol> find(String name) {
    return Stream.of(Protocol.values())
            .filter(v -> v.name().toLowerCase().equals(name))
            .findFirst();
  }

  public static Optional<Protocol> find(byte[] code) {
    if (code == null || code.length == 0) {
      return Optional.empty();
    }
    for (Protocol p : Protocol.values()) {
      if (Bytes.compareTo(p.getCode(), code) == 0) {
        return Optional.of(p);
      }
    }
    return Optional.empty();
  }
  private final byte[] code;

  /**
   * @param c The short value to saved in HDS
   */
  Protocol(final short c) {
    code = Bytes.toBytes(c);
  }

  public byte[] getCode() {
    return code;
  }
}
