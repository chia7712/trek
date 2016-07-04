package net.spright.trek.zookeeper;

import com.spright.trek.DConstants;
import com.spright.trek.utils.TrekUtils;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public final class ZkClientFactory {

  private static ShareableZkClient zkClient = null;

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        close();
      } catch (Exception ex) {
      }
    }));
  }

  private static void close() throws Exception {
    synchronized (ShareableZkClient.class) {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  public static ZkClient getInstance(final Configuration config) throws IOException {
    synchronized (ShareableZkClient.class) {
      if (zkClient == null) {
        if (config.getBoolean(DConstants.ENABLE_SINGLE_MODE, DConstants.DEFAULT_ENABLE_SINGLETON)) {
          throw new RuntimeException("Unsupport single mode when using zk client");
        }
        zkClient = new ShareableZkClient(config);
      }
    }
    return zkClient;
  }

  private static class EventHandler {

    private final ZkClient zk;
    private final EventType acceptedEvent;
    private final Consumer<String> consumer;

    EventHandler(final ZkClient zk, final EventType acceptedEvent,
            Consumer<String> consumer) {
      this.zk = zk;
      this.acceptedEvent = acceptedEvent;
      this.consumer = consumer;
    }

    void apply(final EventType event, final String path) {
      if (event == acceptedEvent) {
        consumer.accept(path);
      }
    }
  }

  private static class ShareableZkClient implements ZkClient, AutoCloseable {

    private final AtomicLong id = new AtomicLong(0);
    private final ConcurrentMap<Long, EventHandler> handlers = new ConcurrentHashMap<>();
    private final ZooKeeper zk;
    private final String zkRoot;

    ShareableZkClient(final Configuration config) throws IOException {
      zkRoot = config.get(DConstants.ZOOKEEPER_ROOT, DConstants.DEFAULT_ZOOKEEPER_ROOT);
      String quorum = config.get(DConstants.ZOOKEEPER_QUORUM);
      if (quorum == null) {
        throw new RuntimeException("Something's missing, zkRoot:" + zkRoot
                + ", quorum:" + quorum);
      }
      int sessionTimeout = config.getInt(DConstants.ZOOKEEPER_SESSION_TIMEOUT,
              DConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
      zk = new ZooKeeper(quorum, sessionTimeout, (WatchedEvent e) -> {
        handlers.values().forEach(v -> v.apply(e.getType(), e.getPath()));
      });
      try {
        if (zk.exists(zkRoot, false) == null) {
          zk.create(zkRoot, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
      } catch (KeeperException | InterruptedException e) {
        TrekUtils.closeWithLog(() -> zk.close(), null);
        throw new IOException(e);
      }
    }

    @Override
    public long register(EventType type, Consumer<String> consumer) {
      long currentId = id.getAndIncrement();
      handlers.put(currentId, new EventHandler(this, type, consumer));
      return currentId;
    }

    @Override
    public void unregister(long id) {
      handlers.remove(id);
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws IOException {
      try {
        return zk.create(path, data, acl, createMode);
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public Stat exists(String path, boolean watch) throws IOException {
      try {
        return zk.exists(path, watch);
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void delete(String path, int version) throws IOException {
      try {
        zk.delete(path, version);
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws IOException {
      try {
        return zk.getChildren(path, watch);
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public byte[] getData(String path, boolean watch, Stat stat) throws IOException {
      try {
        return zk.getData(path, watch, stat);
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws Exception {
      handlers.clear();
      zk.close();
    }

    @Override
    public String getRootPath() {
      return zkRoot;
    }

    @Override
    public Stat setData(String path, byte[] data, int version) throws IOException {
      try {
        return zk.setData(path, data, version);
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  private ZkClientFactory() {
  }
}
