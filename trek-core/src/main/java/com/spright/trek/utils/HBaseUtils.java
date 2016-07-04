package com.spright.trek.utils;

import java.util.Optional;
import org.apache.hadoop.hbase.util.Bytes;

public final class HBaseUtils {

  public static Optional<byte[]> getAndCheckInt(final byte[] buf) {
    return getAndCheck(buf, Bytes.SIZEOF_INT);
  }

  public static Optional<byte[]> getAndCheckLong(final byte[] buf) {
    return getAndCheck(buf, Bytes.SIZEOF_LONG);
  }

  public static Optional<byte[]> getAndCheckString(final byte[] buf) {
    return getAndCheck(buf, -1);
  }

  public static Optional<byte[]> getAndCheckDouble(final byte[] buf) {
    return getAndCheck(buf, Bytes.SIZEOF_DOUBLE);
  }

  public static Optional<byte[]> getAndCheck(final byte[] buf,
          int expectedLen) {
    if (buf == null || buf.length == 0
            || (expectedLen != -1 && expectedLen != buf.length)) {
      return Optional.empty();
    }
    return Optional.of(buf);
  }

  private HBaseUtils() {
  }
}
