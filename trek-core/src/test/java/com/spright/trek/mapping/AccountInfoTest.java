package com.spright.trek.mapping;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.spright.trek.io.json.JsonIO;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class AccountInfoTest {

  public AccountInfoTest() {
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
   * Test of write method, of class AccountInfo.
   */
  @Test
  public void testWrite() throws Exception {
    System.out.println("write");
    AccountInfo info = AccountInfo.newBuilder()
            .setId("id")
            .setHost("host")
            .setPassword("pwd")
            .setPort(123)
            .setUser("user")
            .setDomain("domain")
            .build();
    JsonIO json = new JsonIO(1024, false);
    try (JsonWriter writer = json.getWriter()) {
      AccountInfo.write(writer, info);
    }
    try (JsonReader reader = json.getReader()) {
      AccountInfo copy = AccountInfo.read(reader);
      assertEquals(copy.getId().get(), info.getId().get());
      assertEquals(copy.getUser().get(), info.getUser().get());
      assertEquals(copy.getDomain().get(), info.getDomain().get());
      assertEquals(copy.getUser().get(), info.getUser().get());
      assertEquals(copy.getPassword().get(), info.getPassword().get());
      assertEquals(copy.getPort().get(), info.getPort().get());
      assertEquals(true, info.equals(copy));
    }
  }

  /**
   * Test of isEmpty method, of class AccountInfo.
   */
  @Test
  public void testIsEmpty() {
    System.out.println("isEmpty");
    AccountInfo instance = AccountInfo.EMPTY;
    assertEquals(true, instance.isEmpty());
  }
}
