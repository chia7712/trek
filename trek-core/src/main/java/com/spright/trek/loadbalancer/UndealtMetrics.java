package com.spright.trek.loadbalancer;

import java.io.Closeable;

public interface UndealtMetrics extends Closeable {

  void addBytes(final long v);
}
