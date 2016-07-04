package com.spright.trek.cluster;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class NodeInfoTest {

  public NodeInfoTest() {
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
   * Test of getAddress method, of class NodeInfo.
   */
  @Test
  public void testGetAddress() {
    System.out.println("getAddress");
    NodeInfo instance_v0 = new NodeInfo("abc", 123);
    NodeInfo instance_v1 = new NodeInfo("abc", 123);
    assertEquals(instance_v0.getAddress(), instance_v1.getAddress());
    assertEquals(instance_v0.getPort(), instance_v1.getPort());
    assertEquals(true, instance_v0.equals(instance_v1));
  }

}
