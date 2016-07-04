package com.spright.trek.mapping;

import com.spright.trek.DConstants;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.query.EmptyableInteger;
import com.spright.trek.query.EmptyableString;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class InMemoryMappingTest {

  public InMemoryMappingTest() {
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
   * Test of delete method, of class Mapping.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testAdd_String() throws Exception {
    System.out.println("add");
    Configuration conf = new Configuration();
    conf.setBoolean(DConstants.ENABLE_SINGLE_MODE, true);
    Map<String, String> baseQuery = new TreeMap<>();
    baseQuery.put(DConstants.URI_MAPPING_ID, "id1");
    baseQuery.put(DConstants.URI_MAPPING_HOST, "host");
    AccountInfoUpdate query = AccountInfoUpdate.parse(baseQuery);
    Mapping result = MappingFactory.newInstance(conf);
    result.add(query);
    try (CloseableIterator<AccountInfo> infos = result.list()) {
      int count = 0;
      while (infos.hasNext()) {
        AccountInfo info = infos.next();
        assertEquals("id1", info.getId().get());
        assertEquals("host", info.getHost().get());
        ++count;
      }
      assertEquals(1, count);
    }
  }

  /**
   * Test of list method, of class Mapping.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testList() throws Exception {
    System.out.println("list");
    Configuration conf = new Configuration();
    conf.setBoolean(DConstants.ENABLE_SINGLE_MODE, true);
    Map<String, String> baseQuery = new TreeMap<>();
    baseQuery.put(DConstants.URI_MAPPING_ID, "id1");
    baseQuery.put(DConstants.URI_MAPPING_HOST, "host");
    AccountInfoUpdate update = AccountInfoUpdate.parse(baseQuery);
    Mapping result = MappingFactory.newInstance(conf);
    result.add(update);
    result.add(AccountInfoUpdate.newBuilder()
            .setId("aaa")
            .setDomain(new EmptyableString("domain"))
            .setHost(new EmptyableString("host9999"))
            .setPassword(new EmptyableString("password"))
            .setPort(new EmptyableInteger(123))
            .setUser(new EmptyableString("user"))
            .build());
    AccountInfoQuery query = AccountInfoQuery.newBuilder()
            .setHost("host9999")
            .build();
    try (CloseableIterator<AccountInfo> iter = result.list(query)) {
      int count = 0;
      while (iter.hasNext()) {
        AccountInfo info = iter.next();
        assertEquals("aaa", info.getId().get());
        assertEquals("domain", info.getDomain().get());
        assertEquals("host9999", info.getHost().get());
        assertEquals("password", info.getPassword().get());
        assertEquals(123, (int) info.getPort().get());
        assertEquals("user", info.getUser().get());
        ++count;
      }
      assertEquals(1, count);
    }

  }

  /**
   * Test of find method, of class Mapping.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testDelete() throws Exception {
    System.out.println("delete");
    Configuration conf = new Configuration();
    conf.setBoolean(DConstants.ENABLE_SINGLE_MODE, true);
    Map<String, String> baseQuery = new TreeMap<>();
    baseQuery.put(DConstants.URI_MAPPING_ID, "id1");
    baseQuery.put(DConstants.URI_MAPPING_HOST, "host");
    AccountInfoUpdate update = AccountInfoUpdate.parse(baseQuery);
    Mapping result = MappingFactory.newInstance(conf);
    result.add(update);
    result.add(AccountInfoUpdate.newBuilder()
            .setId("aaa")
            .setDomain(new EmptyableString("domain"))
            .setHost(new EmptyableString("host9999"))
            .setPassword(new EmptyableString("password"))
            .setPort(new EmptyableInteger(123))
            .setUser(new EmptyableString("user"))
            .build());
    AccountInfoQuery query = AccountInfoQuery.newBuilder()
            .setHost("host9999")
            .setKeep(false)
            .build();
    try (CloseableIterator<AccountInfo> iter = result.list(query)) {
      int count = 0;
      while (iter.hasNext()) {
        AccountInfo info = iter.next();
        assertEquals("aaa", info.getId().get());
        assertEquals("domain", info.getDomain().get());
        assertEquals("host9999", info.getHost().get());
        assertEquals("password", info.getPassword().get());
        assertEquals(123, (int) info.getPort().get());
        assertEquals("user", info.getUser().get());
        ++count;
      }
      assertEquals(0, count);
    }
    try (CloseableIterator<AccountInfo> iter = result.list()) {
      int count = 0;
      while (iter.hasNext()) {
        AccountInfo info = iter.next();
        assertEquals("id1", info.getId().get());
        assertEquals("host", info.getHost().get());
        ++count;
      }
      assertEquals(1, count);
    }
  }

}
