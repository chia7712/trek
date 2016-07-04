package com.spright.trek.datasystem.request;

import com.spright.trek.mapping.AccountInfo;
import com.spright.trek.mapping.Mapping;
import com.spright.trek.DConstants;
import com.spright.trek.datasystem.Protocol;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.Mockito;

public class UriRequestTest {

  public UriRequestTest() {
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
   * Test of valueOfTo method, of class UriRequest.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testValueOfTo() throws Exception {
    System.out.println("valueOfTo");
    UriRequest result = UriRequest.parse("smb://domain;user:%40%3F.%2F%7E@host:123/dir/aa.zip", null, null);
    assertEquals(Protocol.SMB, result.getScheme());
    assertEquals("domain", result.getAccountInfo().getDomain().get());
    assertEquals("user", result.getAccountInfo().getUser().get());
    assertEquals("@?./~", result.getAccountInfo().getPassword().get());
    assertEquals("host", result.getAccountInfo().getHost().get());
    assertEquals(123, (int) result.getAccountInfo().getPort().get());
    assertEquals("/dir/aa.zip", result.getPath().toString());
  }

  /**
   * Test of valueOfTo method, of class UriRequest.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testValueOfTo_noDomain() throws Exception {
    System.out.println("valueOfTo_noDomain");
    UriRequest result = UriRequest.parse("smb://user:%40%3F.%2F%7E@host:123/dir/aa.zip", null, null);
    assertEquals(Protocol.SMB, result.getScheme());
    assertEquals(false, result.getAccountInfo().getDomain().isPresent());
    assertEquals("user", result.getAccountInfo().getUser().get());
    assertEquals("@?./~", result.getAccountInfo().getPassword().get());
    assertEquals("host", result.getAccountInfo().getHost().get());
    assertEquals(123, (int) result.getAccountInfo().getPort().get());
    assertEquals("/dir/aa.zip", result.getPath().toString());
  }

  /**
   * Test of valueOfTo method, of class UriRequest.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testValueOfTo_noUser() throws Exception {
    System.out.println("valueOfTo_noUser");
    UriRequest result = UriRequest.parse("smb://:%40%3F.%2F%7E@host:123/dir/aa.zip", null, null);
    assertEquals(Protocol.SMB, result.getScheme());
    assertEquals(false, result.getAccountInfo().getDomain().isPresent());
    assertEquals(false, result.getAccountInfo().getUser().isPresent());
    assertEquals("@?./~", result.getAccountInfo().getPassword().get());
    assertEquals("host", result.getAccountInfo().getHost().get());
    assertEquals(123, (int) result.getAccountInfo().getPort().get());
    assertEquals("/dir/aa.zip", result.getPath().toString());
  }

  /**
   * Test of valueOfTo method, of class UriRequest.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testValueOfTo_noPassword() throws Exception {
    System.out.println("valueOfTo_noPassword");
    UriRequest result = UriRequest.parse("smb://:@host:123/dir/aa.zip", null, null);
    assertEquals(Protocol.SMB, result.getScheme());
    assertEquals(false, result.getAccountInfo().getDomain().isPresent());
    assertEquals(false, result.getAccountInfo().getUser().isPresent());
    assertEquals(false, result.getAccountInfo().getPassword().isPresent());
    assertEquals("host", result.getAccountInfo().getHost().get());
    assertEquals(123, (int) result.getAccountInfo().getPort().get());
    assertEquals("/dir/aa.zip", result.getPath().toString());
  }

  /**
   * Test of valueOfTo method, of class UriRequest.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testValueOfTo_noHost() throws Exception {
    System.out.println("valueOfTo_noHost");
    Map<String, String> rawQuery = new TreeMap<>();
    rawQuery.put(DConstants.URI_DATA_TO, "smb://:@:123/dir/aa.zip");
    Mapping mapping = null;
    UriRequest result = UriRequest.parse("smb://:@:123/dir/aa.zip", null, null);
    assertEquals(Protocol.SMB, result.getScheme());
    assertEquals(false, result.getAccountInfo().getDomain().isPresent());
    assertEquals(false, result.getAccountInfo().getUser().isPresent());
    assertEquals(false, result.getAccountInfo().getPassword().isPresent());
    assertEquals(false, result.getAccountInfo().getHost().isPresent());
    assertEquals(123, (int) result.getAccountInfo().getPort().get());
    assertEquals("/dir/aa.zip", result.getPath().toString());
  }

  /**
   * Test of valueOfTo method, of class UriRequest.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testValueOfTo_noPort() throws Exception {
    System.out.println("valueOfTo_noHost");
    UriRequest result = UriRequest.parse("smb://:@:/dir/aa.zip", null, null);
    assertEquals(Protocol.SMB, result.getScheme());
    assertEquals(false, result.getAccountInfo().getDomain().isPresent());
    assertEquals(false, result.getAccountInfo().getUser().isPresent());
    assertEquals(false, result.getAccountInfo().getPassword().isPresent());
    assertEquals(false, result.getAccountInfo().getHost().isPresent());
    assertEquals(false, result.getAccountInfo().getPort().isPresent());
    assertEquals("/dir/aa.zip", result.getPath().toString());
  }

  /**
   * Test of valueOfTo method, of class UriRequest.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testValueOfTo_onlyUserAndHost() throws Exception {
    System.out.println("valueOfTo_onlyUserAndHost");
    UriRequest result = UriRequest.parse("smb://%40%3F.%2F%7E@host/dir/aa.zip", null, null);
    assertEquals(Protocol.SMB, result.getScheme());
    assertEquals(false, result.getAccountInfo().getDomain().isPresent());
    assertEquals(true, result.getAccountInfo().getUser().isPresent());
    assertEquals(false, result.getAccountInfo().getPassword().isPresent());
    assertEquals("host", result.getAccountInfo().getHost().get());
    assertEquals(false, result.getAccountInfo().getPort().isPresent());
    assertEquals("/dir/aa.zip", result.getPath().toString());
  }

  /**
   * Test of valueOfTo method, of class UriRequest.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testValueOfTo_onlyPortAndHost() throws Exception {
    System.out.println("valueOfTo_onlyPasswordAndHost");
    UriRequest result = UriRequest.parse("smb://host:555/dir/aa.zip", null, null);
    assertEquals(Protocol.SMB, result.getScheme());
    assertEquals(false, result.getAccountInfo().getDomain().isPresent());
    assertEquals(false, result.getAccountInfo().getUser().isPresent());
    assertEquals(false, result.getAccountInfo().getPassword().isPresent());
    assertEquals("host", result.getAccountInfo().getHost().get());
    assertEquals(555, (int) result.getAccountInfo().getPort().get());
    assertEquals("/dir/aa.zip", result.getPath().toString());
  }

  private static void printAccountInfo(final AccountInfo info) {
    System.out.println("domain:" + info.getDomain());
    System.out.println("user:" + info.getUser());
    System.out.println("password:" + info.getPassword());
    System.out.println("host:" + info.getHost());
    System.out.println("port:" + info.getPort());

  }

  /**
   * Test of valueOf method, of class UriRequest.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testValueOf_4args_0() throws Exception {
    System.out.println("valueOf_0");
    final long ts = System.currentTimeMillis();
    SimpleDateFormat timeSdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    Map<String, String> rawQuery = new TreeMap<>();
    rawQuery.put(DConstants.URI_DATA_TO, "smb://host:555/dir");
    rawQuery.put(DConstants.URI_DATA_UPLOAD_TIME, timeSdf.format(new Date(ts)));
    Mapping mapping = null;
    UriRequest req = Mockito.mock(UriRequest.class);
    Mockito.when(req.getPath()).thenReturn(new DataPath("/dir", "aaa"));
    DataInfo info = Mockito.mock(DataInfo.class);
    Mockito.when(info.getSize()).thenReturn((long) 555);
    Mockito.when(info.getUriRequest()).thenReturn(req);
    WriteDataRequest result = WriteDataRequest.parse(rawQuery, mapping, info);
    assertEquals(Protocol.SMB, result.getScheme());
    assertEquals(false, result.getAccountInfo().getDomain().isPresent());
    assertEquals(false, result.getAccountInfo().getUser().isPresent());
    assertEquals(false, result.getAccountInfo().getPassword().isPresent());
    assertEquals("host", result.getAccountInfo().getHost().get());
    assertEquals(555, (int) result.getAccountInfo().getPort().get());
    assertEquals("/dir", result.getPath().toString());
    assertEquals("dir", result.getPath().getName());
    assertEquals("/", result.getPath().getCatalog());
    assertEquals(ts, (long) result.getExpectedTime().get());
    assertEquals(555, (long) result.getExpectedSize().get());
  }

}
