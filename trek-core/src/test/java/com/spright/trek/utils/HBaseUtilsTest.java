package com.spright.trek.utils;

import java.util.Optional;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class HBaseUtilsTest {

  public HBaseUtilsTest() {
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
   * Test of getAndCheckInt method, of class HBaseUtils.
   */
  @Test
  public void testGetAndCheckInt() {
    System.out.println("getAndCheckInt");
    byte[] buf = Bytes.toBytes((int) 123);
    Optional<byte[]> expResult = Optional.of(buf);
    Optional<byte[]> result = HBaseUtils.getAndCheckInt(buf);
    assertEquals(true, result.isPresent());
    assertEquals(0, Bytes.compareTo(expResult.get(), result.get()));
    assertEquals(false, HBaseUtils.getAndCheckInt(Bytes.toBytes((long) 123)).isPresent());
  }

  /**
   * Test of getAndCheckLong method, of class HBaseUtils.
   */
  @Test
  public void testGetAndCheckLong() {
    System.out.println("getAndCheckLong");
    byte[] buf = Bytes.toBytes((long) 123);
    Optional<byte[]> expResult = Optional.of(buf);
    Optional<byte[]> result = HBaseUtils.getAndCheckLong(buf);
    assertEquals(true, result.isPresent());
    assertEquals(0, Bytes.compareTo(expResult.get(), result.get()));
    assertEquals(false, HBaseUtils.getAndCheckInt(buf).isPresent());
  }

  /**
   * Test of getAndCheckString method, of class HBaseUtils.
   */
  @Test
  public void testGetAndCheckString() {
    System.out.println("getAndCheckString");
    byte[] buf = Bytes.toBytes("Xxxx");
    Optional<byte[]> expResult = Optional.of(buf);
    Optional<byte[]> result = HBaseUtils.getAndCheckString(buf);
    assertEquals(true, result.isPresent());
    assertEquals(0, Bytes.compareTo(expResult.get(), result.get()));
  }

  /**
   * Test of getAndCheckDouble method, of class HBaseUtils.
   */
  @Test
  public void testGetAndCheckDouble() {
    System.out.println("getAndCheckDouble");
    byte[] buf = Bytes.toBytes((double) 0.123);
    Optional<byte[]> expResult = Optional.of(buf);
    Optional<byte[]> result = HBaseUtils.getAndCheckDouble(buf);
    assertEquals(true, result.isPresent());
    assertEquals(0, Bytes.compareTo(expResult.get(), result.get()));
    assertEquals(false, HBaseUtils.getAndCheckInt(buf).isPresent());
  }

}
