package com.spright.trek.datasystem.request;

import com.spright.trek.DConstants;
import com.spright.trek.exception.MappingIOException;
import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.query.OrderKey;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class DataInfoQueryTest {

  public DataInfoQueryTest() {
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
   * Test of newBuilder method, of class DataInfoQuery.
   *
   * @throws com.spright.trek.exception.UriParseIOException
   * @throws com.spright.trek.exception.MappingIOException
   */
  @Test
  public void testNewBuilder() throws UriParseIOException, MappingIOException {
    System.out.println("newBuilder");
    Map<String, String> rawQuery = new TreeMap<>();
    rawQuery.put(DConstants.URI_DATA_SIZE, "123");
    rawQuery.put(DConstants.URI_DATA_FROM, "ftp://host:123/dir/chia7712");
    DataInfoQuery query = DataInfoQuery.parse(rawQuery, null);
    assertEquals("chia7712", query.getPath().getName());
    assertEquals(123, (long) query.getSize().get().getMaxValue());
    assertEquals(123, (long) query.getSize().get().getMinValue());
  }

  /**
   * Test of getPredicate method, of class DataInfoQuery.
   *
   * @throws com.spright.trek.exception.UriParseIOException
   * @throws com.spright.trek.exception.MappingIOException
   */
  @Test
  public void testGetPredicate() throws UriParseIOException, MappingIOException {
    System.out.println("getPredicate");
    Map<String, String> rawQuery = new TreeMap<>();
    rawQuery.put(DConstants.URI_DATA_SIZE, "123");
    rawQuery.put(DConstants.URI_DATA_FROM, "ftp://host:123/dir/chia7712");
    DataInfoQuery query = DataInfoQuery.parse(rawQuery, null);
    DataInfo info = DataInfo.newBuilder()
            .setSize(123)
            .setOwner(DataOwner.newSingleOwner("host"))
            .setRequest(UriRequest.parse("ftp://host:123/dir/chia7712", null, null))
            .setType(DataType.FILE)
            .setUploadTime(System.currentTimeMillis())
            .build();
    Predicate<DataInfo> predicate = query.getPredicate();
    assertEquals(true, predicate.test(info));
  }

  /**
   * Test of getComparator method, of class DataInfoQuery.
   *
   * @throws com.spright.trek.exception.UriParseIOException
   * @throws com.spright.trek.exception.MappingIOException
   */
  @Test
  public void testGetComparator() throws UriParseIOException, MappingIOException {
    System.out.println("getComparator");
    Set<OrderKey<DataInfo.Field>> order = new LinkedHashSet<>();
    order.add(new OrderKey<>(DataInfo.Field.SIZE, true));
    order.add(new OrderKey<>(DataInfo.Field.NAME, false));
    DataInfoQuery query = DataInfoQuery.newBuilder()
            .setOrderKeys(order)
            .setUriRequest(UriRequest.parse("ftp://host:132/dir/aa.zip", null, null))
            .build();
    Comparator<DataInfo> cmp = query.getComparator();
    DataInfo info_0 = DataInfo.newBuilder()
            .setSize(123)
            .setOwner(DataOwner.newSingleOwner("host"))
            .setRequest(UriRequest.parse("ftp://host:123/dir/chia7712", null, null))
            .setType(DataType.FILE)
            .setUploadTime(System.currentTimeMillis())
            .build();
    DataInfo info_1 = DataInfo.newBuilder()
            .setSize(1230)
            .setOwner(DataOwner.newSingleOwner("host"))
            .setRequest(UriRequest.parse("ftp://host:123/dir/chia7712", null, null))
            .setType(DataType.FILE)
            .setUploadTime(System.currentTimeMillis())
            .build();
    DataInfo info_2 = DataInfo.newBuilder()
            .setSize(1230)
            .setOwner(DataOwner.newSingleOwner("host"))
            .setRequest(UriRequest.parse("ftp://host:123/dir/chia7713", null, null))
            .setType(DataType.FILE)
            .setUploadTime(System.currentTimeMillis())
            .build();
    assertEquals(-1, cmp.compare(info_0, info_1));
    assertEquals(1, cmp.compare(info_1, info_2));
  }
}
