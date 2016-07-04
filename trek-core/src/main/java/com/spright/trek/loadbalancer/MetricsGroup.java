package com.spright.trek.loadbalancer;

public interface MetricsGroup {

  UndealtMetrics newMetrics(final Operation op);

  UndealtMetrics newMetrics(final Operation op, final long expectedSize);
}
