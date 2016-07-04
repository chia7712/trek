package com.spright.trek.cluster;

import com.spright.trek.DConstants;
import com.spright.trek.loadbalancer.Operation;
import com.spright.trek.loadbalancer.OperationLoad;
import com.spright.trek.utils.TrekUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class NodeManagerTest {

  public NodeManagerTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /**
   * Test of update method, of class NodeManager.
   */
  @Test
  public void testUpdate() throws Exception {
    System.out.println("update");
    Configuration config = new Configuration();
    config.set(DConstants.RESTFUL_SERVER_BINDING_ARRRESS, "127.0.0.1");
    config.setInt(DConstants.RESTFUL_SERVER_BINDING_PORT, 12345);
    config.setBoolean(DConstants.ENABLE_SINGLE_MODE, true);
    NodeManager instance = NodeManagerFactory.newInstance(config);
    OperationLoad load = OperationLoad.newBuilder()
            .setType(Operation.READ)
            .setFirstCall(123)
            .setHost(TrekUtils.getAddress())
            .setLastCall(132)
            .setTotalBytes(123)
            .setTotalCount(333)
            .setUndealtBytes(12333)
            .setUndealtCount(999)
            .build();
    instance.update(Arrays.asList(load));
    Map<NodeInfo, Collection<OperationLoad>> infos = instance.list();
    NodeInfo localNode = new NodeInfo(
            config.get(DConstants.RESTFUL_SERVER_BINDING_ARRRESS,
                    DConstants.DEFAULT_RESTFUL_SERVER_BINDING_ARRRESS),
            config.getInt(DConstants.RESTFUL_SERVER_BINDING_PORT,
                    DConstants.DEFAULT_RESTFUL_SERVER_BINDING_PORT));
    assertEquals(1, infos.size());
    assertEquals(1, infos.get(localNode).size());
    assertEquals(true, load.equals(infos.get(localNode).iterator().next()));
  }

}
