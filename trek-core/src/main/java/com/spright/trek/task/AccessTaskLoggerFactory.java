package com.spright.trek.task;

import com.spright.trek.query.CloseableIterator;
import com.spright.trek.query.IteratorUtils;
import com.spright.trek.DConstants;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

public final class AccessTaskLoggerFactory {

  public static AccessTaskLogger instance(final Configuration conf) throws Exception {
    return instance(conf, TableName.valueOf(
            conf.get(DConstants.TREK_NAMESPACE, DConstants.DEFAULT_TREK_NAMESPACE),
            DConstants.TASK_LOG_NAME));
  }

  public static AccessTaskLogger instance(final Configuration conf,
          final TableName name) throws Exception {
    if (conf.getBoolean(DConstants.ENABLE_SINGLE_MODE,
            DConstants.DEFAULT_ENABLE_SINGLETON)) {
      return new InMemoryLogger(conf);
    }
    return new AccessTaskLogTable(conf, name);
  }

  private static class InMemoryLogger implements AccessTaskLogger {

    private final Map<String, AccessStatusWrap> statusMap = new TreeMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final int ttl;
    private boolean closed = false;

    InMemoryLogger(final Configuration config) {
      ttl = config.getInt(
              DConstants.ACCESS_LOG_TTL_IN_SECOND,
              DConstants.DEFAULT_ACCESS_LOG_TTL_IN_SECOND);
    }

    @Override
    public boolean supportOrder(AccessStatusQuery query) {
      return query.getOrderKey()
              .stream()
              .allMatch(v -> v.getKey() == AccessStatus.Field.ID && v.getAsc());
    }

    @Override
    public boolean supportFilter(AccessStatusQuery query) {
      return false;
    }

    @Override
    public void add(AccessStatus status) throws IOException {
      lock.writeLock().lock();
      try {
        Map<String, AccessStatusWrap> validMap = new TreeMap<>();
        statusMap.put(status.getId(), new AccessStatusWrap(status, ttl));
        statusMap.forEach((key, value) -> {
          if (!value.shouldDelete()) {
            validMap.put(key, value);
          }
        });
        statusMap.clear();
        statusMap.putAll(validMap);
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public CloseableIterator<AccessStatus> list(AccessStatusQuery query) throws IOException {
      lock.readLock().lock();
      try {
        return Optional.of(IteratorUtils.wrap(statusMap.values()
                .stream()
                .filter(v -> !v.shouldDelete())
                .map(v -> (AccessStatus) v)
                .collect(Collectors.toList())
                .iterator()))
                .map(v -> IteratorUtils.wrap(v, query.getPredicate()))
                .map(v -> IteratorUtils.wrap(v, query.getComparator()))
                .map(v -> IteratorUtils.wrapOffset(v, query.getOffset()))
                .map(v -> IteratorUtils.wrapLimit(v, query.getLimit()))
                .get();
      } finally {
        lock.readLock().unlock();
      }
    }

    @Override
    public void close() throws Exception {
      lock.writeLock().lock();
      if (closed) {
        return;
      }
      closed = true;
      try {
        statusMap.clear();
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public Optional<AccessStatus> find(String id) throws IOException {
      lock.readLock().lock();
      try {
        return Optional.ofNullable(statusMap.get(id));
      } finally {
        lock.readLock().unlock();
      }
    }

    @Override
    public CloseableIterator<AccessStatus> list() throws IOException {
      return list(AccessStatusQuery.QUERY_ALL);
    }
  }

  private static class AccessStatusWrap extends AccessStatus {

    private final long ts = System.currentTimeMillis();
    private final long ttl;

    AccessStatusWrap(final AccessStatus ref, final long ttl) {
      super(ref);
      this.ttl = ttl;
    }

    boolean shouldDelete() {
      return System.currentTimeMillis() - ts > (ttl * 1000);
    }

    long getConstructTime() {
      return ts;
    }
  }

  private AccessTaskLoggerFactory() {
  }
}
