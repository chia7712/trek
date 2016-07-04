package com.spright.trek.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class InfiniteAccesserTest {

  public InfiniteAccesserTest() {
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
   * Test of getSize method, of class InfiniteAccesser.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testGetSize() throws IOException {
    System.out.println("getSize");
    InfiniteAccesser instance = new InfiniteAccesser(120, 100, new InfiniteAccesser.FileIOFactory());
    try (OutputStream output = instance.getOutputStream()) {
      byte[] data = new byte[90];
      output.write(data);
      assertEquals(90, instance.getSize());
      assertEquals(true, instance.isInMemory());
      output.write(data);
      assertEquals(180, instance.getSize());
      assertEquals(false, instance.isInMemory());
    }
    assertEquals(true, instance.isOutputClosed());
    try (InputStream input = instance.getInputStream()) {
      int rval;
      int count = 0;
      byte[] buf = new byte[1024];
      while ((rval = input.read(buf)) != -1) {
        count += rval;
      }
      assertEquals(180, count);
    }
  }
}
