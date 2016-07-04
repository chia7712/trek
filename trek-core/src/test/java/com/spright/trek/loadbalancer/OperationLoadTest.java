package com.spright.trek.loadbalancer;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.spright.trek.io.json.JsonIO;
import com.spright.trek.utils.TrekUtils;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.avro.data.Json;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class OperationLoadTest {

  public OperationLoadTest() {
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
   * Test of write method, of class OperationLoad.
   */
  @Test
  public void testWrite_JsonWriter_OperationLoad() throws Exception {
    System.out.println("write");
    long ts = System.currentTimeMillis();
    OperationLoad load = OperationLoad.newBuilder()
            .setHost(TrekUtils.getHostname())
            .setType(Operation.READ)
            .setFirstCall(ts)
            .setLastCall(ts)
            .setTotalBytes(1)
            .setTotalCount(2)
            .setUndealtBytes(3)
            .setUndealtCount(4)
            .build();
    assertEquals(ts, load.getFirstCall());
    assertEquals(ts, load.getLastCall());
    assertEquals(1, load.getTotalBytes());
    assertEquals(2, load.getTotalCount());
    assertEquals(3, load.getUndealtBytes());
    assertEquals(4, load.getUndealtCount());
    assertEquals(Operation.READ, load.getType());
    assertEquals(TrekUtils.getHostname(), load.getHost());
    JsonIO json = new JsonIO(1024, false);
    try (JsonWriter writer = json.getWriter()) {
      OperationLoad.write(writer, load);
    }
    try (JsonReader reader = json.getReader()) {
      OperationLoad copy = OperationLoad.read(reader);
      assertEquals(true, copy.equals(load));
    }
  }

}
