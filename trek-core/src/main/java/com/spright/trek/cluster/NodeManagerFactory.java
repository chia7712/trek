package com.spright.trek.cluster;

import com.spright.trek.DConstants;
import com.spright.trek.loadbalancer.OperationLoad;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import net.spright.trek.zookeeper.ZkClient;
import net.spright.trek.zookeeper.ZkClientFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;

public final class NodeManagerFactory {

  private static NodeManager SINGLETON = null;

  public static NodeManager newInstance(final Configuration config) throws IOException {
    synchronized (ZkNodeManager.class) {
      if (SINGLETON == null) {
        if (config.getBoolean(DConstants.ENABLE_SINGLE_MODE, DConstants.DEFAULT_ENABLE_SINGLETON)) {
          SINGLETON = new InMemoryNodeManager(config);
        } else {
          SINGLETON = new ZkNodeManager(config);
        }
      }
    }
    return SINGLETON;
  }

  private static class InMemoryNodeManager implements NodeManager {

    private final ConcurrentMap<NodeInfo, Collection<OperationLoad>> nodeLoads = new ConcurrentHashMap<>();
    private final NodeInfo localNode;

    InMemoryNodeManager(final Configuration config) {
      localNode = new NodeInfo(
              config.get(DConstants.RESTFUL_SERVER_BINDING_ARRRESS,
                      DConstants.DEFAULT_RESTFUL_SERVER_BINDING_ARRRESS),
              config.getInt(DConstants.RESTFUL_SERVER_BINDING_PORT,
                      DConstants.DEFAULT_RESTFUL_SERVER_BINDING_PORT));
    }

    @Override
    public void update(Collection<OperationLoad> loads) {
      nodeLoads.put(localNode, loads);
    }

    @Override
    public Collection<NodeInfo> getNodes() throws IOException {
      return nodeLoads.keySet();
    }

    @Override
    public Collection<OperationLoad> getLoad(NodeInfo node) throws IOException {
      return nodeLoads.getOrDefault(node, new ArrayList<>(0));
    }

    @Override
    public Map<NodeInfo, Collection<OperationLoad>> list() throws IOException {
      return new TreeMap<>(nodeLoads);
    }

  }

  private static class ZkNodeManager implements NodeManager {

    private static final Log LOG = LogFactory.getLog(ZkNodeManager.class);
    private static final String MA_NODE = "node";
    private final ConcurrentMap<NodeInfo, Collection<OperationLoad>> nodeLoads = new ConcurrentHashMap<>();
    private final List<Long> ids = new LinkedList<>();
    private final ZkClient zk;
    private final String root;
    private final NodeInfo localNode;

    ZkNodeManager(final Configuration config) throws IOException {
      zk = ZkClientFactory.getInstance(config);
      root = zk.getRootPath() + "/" + MA_NODE;
      if (zk.exists(root, false) == null) {
        zk.create(root, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      localNode = new NodeInfo(
              config.get(DConstants.RESTFUL_SERVER_BINDING_ARRRESS,
                      DConstants.DEFAULT_RESTFUL_SERVER_BINDING_ARRRESS),
              config.getInt(DConstants.RESTFUL_SERVER_BINDING_PORT,
                      DConstants.DEFAULT_RESTFUL_SERVER_BINDING_PORT));

      ids.add(zk.register(EventType.NodeChildrenChanged, (String path) -> {
        updateAllChildrens(path);
      }));
      ids.add(zk.register(EventType.NodeDataChanged, (String childPath) -> {
        if (childPath.startsWith(root) && childPath.compareTo(root) != 0) {
          try {
            NodeInfo nodeInfo = toNodeInfo(childPath);
            //Only update the node is present in the map
            if (nodeInfo != null && nodeLoads.containsKey(nodeInfo)) {
              nodeLoads.put(nodeInfo, toLoad(zk.getData(childPath, true, null)));
            }
          } catch (IOException ex) {
            LOG.error(ex);
          }
        }
      }));
      ids.add(zk.register(EventType.NodeDeleted, (String childPath) -> {
        if (childPath.startsWith(root) && childPath.compareTo(root) != 0) {
          NodeInfo nodeInfo = toNodeInfo(childPath);
          if (nodeInfo != null) {
            nodeLoads.remove(nodeInfo);
          }
        }
      }));
      updateAllChildrens(root);
    }

    private void updateAllChildrens(final String path) {
      if (path.compareTo(root) == 0) {
        try {
          for (String childPath : zk.getChildren(path, true)) {
            NodeInfo nodeInfo = toNodeInfo(childPath);
            if (nodeInfo != null && !nodeLoads.containsKey(nodeInfo)) {
              nodeLoads.put(nodeInfo, toLoad(zk.getData(childPath, true, null)));
            }
          }
        } catch (IOException ex) {
          LOG.error(ex);
        }
      }
    }

    private static Collection<OperationLoad> toLoad(final byte[] data) {
      List<OperationLoad> rval = new LinkedList<>();
      try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(data))) {
        final int count = input.readInt();
        for (int i = 0; i <= count; ++i) {
          rval.add(OperationLoad.read(input));
        }
      } catch (IOException e) {
        LOG.error("Some error happened on parse zk data", e);
      }
      return rval;
    }

    private static NodeInfo toNodeInfo(final String path) {
      if (path == null) {
        return null;
      }
      String name = FilenameUtils.getName(path);
      final int lastIndex = name.lastIndexOf(":");
      if (lastIndex == -1 || lastIndex == 0) {
        LOG.error("Invalid node name:" + path);
        return null;
      }
      try {
        return new NodeInfo(name.substring(0, lastIndex),
                Integer.valueOf(name.substring(lastIndex)));
      } catch (NumberFormatException e) {
        LOG.error("Invalid node name:" + path);
        return null;
      }
    }

    @Override
    public Collection<NodeInfo> getNodes() throws IOException {
      return new ArrayList<>(nodeLoads.keySet());
    }

    @Override
    public Collection<OperationLoad> getLoad(NodeInfo node) throws IOException {
      return nodeLoads.getOrDefault(node, new ArrayList<>(0));
    }

    @Override
    public Map<NodeInfo, Collection<OperationLoad>> list() throws IOException {
      return new TreeMap<>(nodeLoads);
    }

    private static byte[] toByteArray(final Collection<OperationLoad> loads) throws IOException {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      try (DataOutputStream output = new DataOutputStream(buf)) {
        output.writeInt(loads.size());
        for (OperationLoad load : loads) {
          OperationLoad.write(output, load);
        }
      }
      return buf.toByteArray();
    }

    @Override
    public void update(Collection<OperationLoad> loads) throws IOException {
      String path = root + "/" + localNode.toString();
      byte[] data = toByteArray(loads);
      if (zk.exists(path, true) == null) {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      } else {
        zk.setData(path, data, -1);
      }
    }
  }

  private NodeManagerFactory() {
  }
}
