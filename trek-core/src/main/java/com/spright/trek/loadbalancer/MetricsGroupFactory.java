package com.spright.trek.loadbalancer;

import com.spright.trek.cluster.NodeManager;
import com.spright.trek.cluster.NodeManagerFactory;
import com.spright.trek.utils.TrekUtils;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class MetricsGroupFactory {

  private static MetricsGroup SINGLETON;

  public static MetricsGroup getInstance(final Configuration config) throws IOException {
    synchronized (InMemoryMetricsGroup.class) {
      if (SINGLETON == null) {
        SINGLETON = new InMemoryMetricsGroup(config);
      }
    }
    return SINGLETON;
  }

  private static class InMemoryMetricsGroup implements MetricsGroup {

    private static final Log LOG = LogFactory.getLog(InMemoryMetricsGroup.class);
    private final ConcurrentMap<Operation, Counter> loads = new ConcurrentSkipListMap<>();
    private final NodeManager nm;
    private final AtomicBoolean working = new AtomicBoolean(false);

    InMemoryMetricsGroup(final Configuration config) throws IOException {
      nm = NodeManagerFactory.newInstance(config);
    }

    private void update() throws IOException {
      if (working.compareAndSet(false, true)) {
        try {
          nm.update(list());
        } finally {
          working.set(false);
        }
      }
    }

    @Override
    public UndealtMetrics newMetrics(final Operation key, long expectedSize) {
      return loads.computeIfAbsent(key,
              k -> new Counter(this, k))
              .newUndealtMetrics(expectedSize);
    }

    public Optional<OperationLoad> findMetrics(final Operation key) {
      return Optional.ofNullable(loads.get(key).createOperationLoad());
    }

    public List<OperationLoad> list() {
      return loads.values().stream().map(v -> v.createOperationLoad())
              .collect(Collectors.toList());
    }

    @Override
    public UndealtMetrics newMetrics(Operation op) {
      return loads.computeIfAbsent(op,
              k -> new Counter(this, k)).createUndealtMetrics();
    }

    private static class Counter {

      private final InMemoryMetricsGroup mg;
      private final long firstCall = System.currentTimeMillis();
      private final AtomicLong lastCall = new AtomicLong(firstCall);
      private final Operation type;
      private final AtomicLong totalCount = new AtomicLong();
      private final AtomicLong totalBytes = new AtomicLong();
      private final AtomicLong undealtCount = new AtomicLong();
      private final AtomicLong undealtBytes = new AtomicLong();

      Counter(final InMemoryMetricsGroup mg, final Operation type) {
        this.mg = mg;
        this.type = type;
      }

      void updateLastCall() {
        lastCall.set(System.currentTimeMillis());
      }

      UndealtMetrics createUndealtMetrics() {
        undealtCount.incrementAndGet();
        return new UndealtMetrics() {
          @Override
          public void addBytes(long v) {
            if (v < 0) {
              String msg = "The increment size should be bigger than or "
                      + "equal zero, current value is " + v;
              LOG.error(msg);
              throw new RuntimeException(msg);
            }
            totalBytes.addAndGet(v);
          }

          @Override
          public void close() throws IOException {
            undealtCount.decrementAndGet();
            totalCount.incrementAndGet();
            mg.update();
          }
        };
      }

      OperationLoad createOperationLoad() {
        return OperationLoad.newBuilder()
                .setHost(TrekUtils.getHostname())
                .setType(type)
                .setUndealtCount(undealtCount.get())
                .setUndealtBytes(undealtBytes.get())
                .setTotalCount(totalCount.get())
                .setTotalBytes(totalBytes.get())
                .setFirstCall(firstCall)
                .setLastCall(lastCall.get())
                .build();
      }

      UndealtMetrics newUndealtMetrics(final long expectedSize) {
        updateLastCall();
        if (expectedSize <= 0) {
          String msg = "The expected size should be bigger than zero, current value is " + expectedSize;
          LOG.error(msg);
          throw new RuntimeException(msg);
        }
        undealtBytes.addAndGet(expectedSize);
        undealtCount.incrementAndGet();
        return new UndealtMetrics() {
          private long remainer = expectedSize;

          @Override
          public void addBytes(long v) {
            if (v < 0) {
              String msg = "The increment size should be bigger than or "
                      + "equal zero, current value is " + v;
              LOG.error(msg);
              throw new RuntimeException(msg);
            }
            if (v > remainer) {
              String msg = "The " + type + "'s increment size should be bigger than or "
                      + "equal remainer: " + remainer + ", increment:" + v
                      + ", expected: " + expectedSize;
              LOG.error(msg);
              undealtBytes.addAndGet(-remainer);
              totalBytes.addAndGet(remainer);
              remainer = 0;
            } else {
              undealtBytes.addAndGet(-v);
              totalBytes.addAndGet(v);
              remainer -= v;
            }
          }

          @Override
          public void close() throws IOException {
            undealtBytes.addAndGet(-remainer);
            undealtCount.decrementAndGet();
            totalCount.incrementAndGet();
            mg.update();
          }
        };
      }
    }
  }
}
