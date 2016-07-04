package net.spright.trek.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public interface ZkClient {

  String getRootPath();

  long register(final EventType type, final Consumer<String> consumer);

  void unregister(final long id);

  Stat setData(final String path, byte data[], int version) throws IOException;

  String create(final String path, final byte data[], final List<ACL> acl, final CreateMode createMode) throws IOException;

  Stat exists(final String path, final boolean watch) throws IOException;

  void delete(final String path, final int version) throws IOException;

  List<String> getChildren(final String path, final boolean watch) throws IOException;

  byte[] getData(final String path, final boolean watch, final Stat stat) throws IOException;
}
