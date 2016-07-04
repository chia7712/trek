package com.spright.trek.utils;

import com.spright.trek.query.RangeNumber;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class TrekUtilsTest {

  public TrekUtilsTest() {
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
   * Test of closeWithLog method, of class TrekUtils.
   */
  @Test
  public void testCloseWithLog_AutoCloseable_Log() {
    System.out.println("closeWithLog");
    AutoCloseable closable = new AutoCloseable() {
      @Override
      public void close() throws Exception {
        throw new IOException("xxx");
      }
    };
    Optional<Exception> result = TrekUtils.closeWithLog(closable, null);
    assertEquals(result.isPresent(), true);
  }

  /**
   * Test of closeWithLog method, of class TrekUtils.
   */
  @Test
  public void testCloseWithLog_Closeable_Log() {
    System.out.println("closeWithLog");
    Closeable closable = new Closeable() {
      @Override
      public void close() throws IOException {
        throw new IOException("xxx");
      }
    };
    Optional<Exception> result = TrekUtils.closeWithLog(closable, null);
    assertEquals(result.isPresent(), true);
  }

}
