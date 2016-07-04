package com.spright.trek.cluster;

import com.spright.trek.loadbalancer.OperationLoad;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface NodeManager {

  void update(final Collection<OperationLoad> loads) throws IOException;

  Collection<NodeInfo> getNodes() throws IOException;

  Collection<OperationLoad> getLoad(final NodeInfo node) throws IOException;

  Map<NodeInfo, Collection<OperationLoad>> list() throws IOException;
}
