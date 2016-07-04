package com.spright.trek.datasystem.request;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.spright.trek.io.json.JsonIO;
import com.spright.trek.mapping.Mapping;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class DataInfoTest {

  public DataInfoTest() {
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
   * Test of write method, of class DataInfo.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testWrite() throws Exception {
    System.out.println("write");
    DataInfo info = DataInfo.newBuilder()
            .setSize(134)
            .setType(DataType.FILE)
            .setUploadTime(System.currentTimeMillis())
            .setRequest(UriRequest.parse("ftp://host:123/dir/a", null, null))
            .setOwner(DataOwner.newSingleOwner("local"))
            .build();
    JsonIO json = new JsonIO(1024, false);
    try (JsonWriter writer = json.getWriter()) {
      DataInfo.write(writer, info);
    }
    try (JsonReader reader = json.getReader()) {
      DataInfo copy = DataInfo.read(reader, null);
      assertEquals(copy.getDataOwners().size(), info.getDataOwners().size());
      assertEquals(true, copy.getDataOwners().get(0).equals(info.getDataOwners().get(0)));
      assertEquals(copy.getSize(), info.getSize());
      assertEquals(copy.getType(), info.getType());
      assertEquals(copy.getUploadTime(), info.getUploadTime());
      assertEquals(copy.getUriRequest(), info.getUriRequest());
    }
  }

}
