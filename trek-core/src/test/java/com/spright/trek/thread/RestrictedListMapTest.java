package com.spright.trek.thread;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class RestrictedListMapTest {

  public RestrictedListMapTest() {
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
   * Test of get method, of class RestrictedListMap.
   */
  @Test
  public void testGet() throws Exception {
    System.out.println("get");
    RestrictedListMap<String, ValueImpl> instance = new RestrictedListMap(10, 3);
    ValueImpl result_0 = instance.get("aaa", () -> new ValueImpl(2 * 1000, "aaa"));
    assertEquals("aaa", result_0.getMessage());
    assertEquals(1, instance.size());
    result_0.free();
    assertEquals(false, result_0.isClean());
    assertEquals(1, instance.size());
    ValueImpl result_1 = instance.get("aaa", () -> new ValueImpl(2 * 1000, "bbb"));
    assertEquals("aaa", result_1.getMessage());
    result_1.close();
    assertEquals(true, result_1.isClean());
    assertEquals(0, instance.size());
    ValueImpl result_2 = instance.get("aaa", () -> new ValueImpl(2 * 1000, "ccc"));
    assertEquals("ccc", result_2.getMessage());
    assertEquals(1, instance.size());
    result_2.free();
    TimeUnit.SECONDS.sleep(5);
    assertEquals(true, result_2.isClean());
    assertEquals(0, instance.size());
    assertEquals(true, result_2.isClean());
    ValueImpl result_3 = instance.get("aaa", () -> new ValueImpl(2 * 1000, "ddd"));
    assertEquals("ddd", result_3.getMessage());
    assertEquals(1, instance.size());
    result_3.close();
    assertEquals(0, instance.size());
  }

  private static class ValueImpl extends RestrictedListMap.Value {

    private boolean isClean = false;
    private final String msg;

    public ValueImpl(long idleTime, final String m) {
      super(idleTime);
      msg = m;
    }

    public String getMessage() {
      return msg;
    }

    public boolean isClean() {
      return isClean;
    }

    @Override
    protected void clean() throws IOException {
      isClean = true;
    }
  }
}
