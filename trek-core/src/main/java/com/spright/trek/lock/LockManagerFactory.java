package com.spright.trek.lock;

import com.spright.trek.DConstants;
import com.spright.trek.exception.LockIOException;
import java.io.IOException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import net.spright.trek.zookeeper.ZkClient;
import net.spright.trek.zookeeper.ZkClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

public final class LockManagerFactory {

  public static LockManager newInstance(final Configuration conf) throws Exception {
    return newInstance(conf, TableName.valueOf(conf.get(DConstants.TREK_NAMESPACE,
            DConstants.DEFAULT_TREK_NAMESPACE), DConstants.LOCK_NAME));
  }

  public static LockManager newInstance(final Configuration conf,
          final TableName name) throws Exception {
    if (conf.getBoolean(DConstants.LOCK_DISABLE,
            DConstants.DEFAULT_LOCK_DISABLE)) {
      return new EmptyReadWriteLock();
    }
    if (conf.getBoolean(DConstants.ENABLE_SINGLE_MODE,
            DConstants.DEFAULT_ENABLE_SINGLETON)) {
      return new MemoryLockManager();
    }
    return new ZkLockManager(conf);
  }

  private static class EmptyReadWriteLock implements LockManager {

    private static final Lock EMPTY_LOCK = () -> {
    };

    @Override
    public Optional<Lock> tryReadLock(String key) throws IOException {
      return Optional.of(EMPTY_LOCK);
    }

    @Override
    public Optional<Lock> tryWriteLock(String key) throws IOException {
      return Optional.of(EMPTY_LOCK);
    }

    @Override
    public Lock getReadLock(String key) throws IOException {
      return EMPTY_LOCK;
    }

    @Override
    public Lock getWriteLock(String key) throws IOException {
      return EMPTY_LOCK;
    }
  }

  private static class ZkLockManager implements LockManager {

    private static final String LOCK_NODE = "lock";
    private final String root;
    private final ZkClient zk;
    private final List<ACL> ACLS = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    ZkLockManager(final Configuration config) throws IOException, KeeperException, InterruptedException {
      zk = ZkClientFactory.getInstance(config);
      root = zk.getRootPath() + "/" + LOCK_NODE;
      if (zk.exists(root, false) == null) {
        zk.create(root, null, ACLS, CreateMode.PERSISTENT);
      }
    }

    @Override
    public Lock getReadLock(String key) throws IOException {
      return tryReadLock(key).orElseThrow(() -> new LockIOException("Write-Read Conflict"));
    }

    @Override
    public Lock getWriteLock(String key) throws IOException {
      return tryWriteLock(key).orElseThrow(() -> new LockIOException("Write-Write Conflict"));
    }

    @Override
    public Optional<Lock> tryReadLock(String key) throws IOException {
      String format = bytesToHexString(key.getBytes());
      final String writeStaring = "write-" + format + "-";
      final String readStaring = "read-" + format + "-";
      String realPath = zk.create(root + "/" + readStaring, null, ACLS, CreateMode.EPHEMERAL_SEQUENTIAL);
      if (zk.getChildren(root, false)
              .stream()
              .noneMatch(v -> v.startsWith(writeStaring))) {
        return Optional.of(() -> zk.delete(realPath, -1));
      } else {
        zk.delete(realPath, -1);
        return Optional.empty();
      }
    }

    @Override
    public Optional<Lock> tryWriteLock(String key) throws IOException {
      String format = bytesToHexString(key.getBytes());
      final String writeStaring = "write-" + format + "-";
      final String readStaring = "read-" + format + "-";
      String realPath = zk.create(root + "/" + writeStaring, null, ACLS, CreateMode.EPHEMERAL_SEQUENTIAL);
      final long value = getNumber(realPath);
      final long min = zk.getChildren(root, false)
              .stream()
              .filter(v -> v.startsWith(writeStaring))
              .mapToLong(v -> getNumber(v))
              .min()
              .orElse(Long.MAX_VALUE);
      if (value <= min) {
        return Optional.of(() -> zk.delete(realPath, -1));
      } else {
        zk.delete(realPath, -1);
        return Optional.empty();
      }
    }

    private static long getNumber(final String string) {
      final int index = string.lastIndexOf("-");
      if (index == -1) {
        throw new RuntimeException("No found of delimiter:-");
      }
      return Long.valueOf(string.substring(index + 1));
    }

    public static String bytesToHexString(byte[] bytes) {
      StringBuilder sb = new StringBuilder(bytes.length * 2);
      Formatter formatter = new Formatter(sb);
      for (byte b : bytes) {
        formatter.format("%02x", b);
      }
      return sb.toString();
    }
  }

  private static class MemoryLockManager implements LockManager {

    private final Map<String, AtomicInteger> locks = new HashMap<>();

    private AtomicInteger getReadCount(final String key) {
      AtomicInteger value;
      synchronized (locks) {
        value = locks.get(key);
        if (value == null) {
          value = new AtomicInteger(0);
          locks.put(key, value);
        }
      }
      return value;
    }

    private AtomicInteger getWriteCount(final String key) {
      AtomicInteger value;
      synchronized (locks) {
        value = locks.get(key);
        if (value == null) {
          value = new AtomicInteger(Integer.MAX_VALUE);
          locks.put(key, value);
          return value;
        } else {
          return null;
        }
      }
    }

    private static boolean isWrite(final AtomicInteger count) {
      return count.get() == Integer.MAX_VALUE;
    }

    private void removeKey(final String key) {
      synchronized (locks) {
        locks.remove(key);
      }
    }

    @Override
    public Lock getReadLock(String key) throws IOException {
      return tryReadLock(key).orElseThrow(() -> new LockIOException("Write-Read Conflict"));
    }

    @Override
    public Lock getWriteLock(String key) throws IOException {
      return tryWriteLock(key).orElseThrow(() -> new LockIOException("Write-Write Conflict"));
    }

    @Override
    public Optional<Lock> tryReadLock(final String key) throws IOException {
      AtomicInteger value = getReadCount(key);
      if (isWrite(value)) {
        return Optional.empty();
      } else {
        value.incrementAndGet();
        Lock lock = () -> {
          if (value.decrementAndGet() == 0) {
            removeKey(key);
          }
        };
        return Optional.of(lock);
      }
    }

    @Override
    public Optional<Lock> tryWriteLock(String key) throws IOException {
      AtomicInteger value = getWriteCount(key);
      if (value == null) {
        return Optional.empty();
      } else {
        return Optional.of(() -> removeKey(key));
      }
    }
  }

  private LockManagerFactory() {
  }
}
