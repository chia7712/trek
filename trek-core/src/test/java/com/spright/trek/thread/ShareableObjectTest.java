package com.spright.trek.thread;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class ShareableObjectTest {

  public ShareableObjectTest() {
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
   * Test of create method, of class ShareableObject.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testCreate_1() throws Exception {
    System.out.println("create");
    String value = "kkk";
    ShareableObject.Supplier<CloseableString> supplier = () -> new CloseableString(value);
    ShareableObject<CloseableString> expResult_0 = ShareableObject.create(CloseableString.class, supplier);
    assertEquals(1, expResult_0.getReferenceCount());
    assertEquals(false, expResult_0.isClosed());
    assertEquals(true, expResult_0.hasReference());
    assertEquals(value, expResult_0.get().toString());
    assertEquals(false, expResult_0.get().isClosed());
    assertEquals(1, ShareableObject.size());

    ShareableObject<CloseableString> expResult_1 = ShareableObject.create(CloseableString.class, supplier);
    assertEquals(2, expResult_1.getReferenceCount());
    assertEquals(false, expResult_1.isClosed());
    assertEquals(true, expResult_1.hasReference());
    assertEquals(value, expResult_1.get().toString());
    assertEquals(false, expResult_0.get().isClosed());
    assertEquals(1, ShareableObject.size());

    expResult_0.close();
    expResult_0.close();
    expResult_0.close();
    assertEquals(1, ShareableObject.size());
    assertEquals(1, expResult_0.getReferenceCount());
    assertEquals(true, expResult_0.isClosed());
    assertEquals(true, expResult_0.hasReference());
    assertEquals(1, expResult_1.getReferenceCount());
    assertEquals(false, expResult_1.isClosed());
    assertEquals(true, expResult_1.hasReference());

    expResult_1.close();
    assertEquals(0, ShareableObject.size());
    assertEquals(0, expResult_1.getReferenceCount());
    assertEquals(0, expResult_1.getReferenceCount());
  }

  /**
   * Test of create method, of class ShareableObject.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testCreate_2() throws Exception {
    System.out.println("create");
    String value = "kkk";
    ShareableObject.Supplier<CloseableString> supplier = () -> new CloseableString(value);
    ShareableObject<CloseableString> expResult_0 = ShareableObject.create(null, supplier);
    assertEquals(1, expResult_0.getReferenceCount());
    assertEquals(false, expResult_0.isClosed());
    assertEquals(true, expResult_0.hasReference());
    assertEquals(value, expResult_0.get().toString());
    assertEquals(false, expResult_0.get().isClosed());
    assertEquals(1, ShareableObject.size());

    ShareableObject<CloseableString> expResult_1 = ShareableObject.create(null, supplier);
    assertEquals(1, expResult_1.getReferenceCount());
    assertEquals(false, expResult_1.isClosed());
    assertEquals(true, expResult_1.hasReference());
    assertEquals(value, expResult_1.get().toString());
    assertEquals(false, expResult_0.get().isClosed());
    assertEquals(2, ShareableObject.size());

    expResult_0.close();
    expResult_0.close();
    expResult_0.close();
    assertEquals(0, expResult_0.getReferenceCount());
    assertEquals(true, expResult_0.isClosed());
    assertEquals(false, expResult_0.hasReference());
    assertEquals(1, expResult_1.getReferenceCount());
    assertEquals(false, expResult_1.isClosed());
    assertEquals(true, expResult_1.hasReference());

    expResult_1.close();
    assertEquals(0, ShareableObject.size());
    assertEquals(0, expResult_1.getReferenceCount());
    assertEquals(0, expResult_1.getReferenceCount());
  }

  /**
   * Test of create method, of class ShareableObject.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testCreate_3() throws Exception {
    System.out.println("create");
    String value = "kkk";
    ShareableObject.Supplier<CloseableString> supplier = () -> new CloseableString(value);
    ShareableObject<CloseableString> expResult_0 = ShareableObject.create(null, supplier);
    assertEquals(1, expResult_0.getReferenceCount());
    assertEquals(false, expResult_0.isClosed());
    assertEquals(true, expResult_0.hasReference());
    assertEquals(value, expResult_0.get().toString());
    assertEquals(false, expResult_0.get().isClosed());
    assertEquals(1, ShareableObject.size());

    ExecutorService service = Executors.newFixedThreadPool(100);
    List<ShareableObject<CloseableString>> results = new LinkedList<>();
    IntStream.range(0, 100).forEach(i -> {
      service.execute(() -> {
        try {
          results.add(ShareableObject.create(CloseableString.class, supplier));
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
    });
    service.shutdown();
    service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    assertEquals(101, expResult_0.getReferenceCount());
    assertEquals(1, ShareableObject.size());

    IntStream.range(0, 50).forEach(i -> {
      ShareableObject<CloseableString> obj = results.remove(0);
      try {
        obj.close();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    });
    assertEquals(51, expResult_0.getReferenceCount());
    assertEquals(1, ShareableObject.size());

    IntStream.range(0, 50).forEach(i -> {
      ShareableObject<CloseableString> obj = results.remove(0);
      try {
        obj.close();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    });
    assertEquals(1, expResult_0.getReferenceCount());
    assertEquals(1, ShareableObject.size());

    expResult_0.close();
    assertEquals(0, expResult_0.getReferenceCount());
    assertEquals(0, ShareableObject.size());
  }

  /**
   * Test of create method, of class ShareableObject.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testCreate_4() throws Exception {
    System.out.println("create");
    String value = "kkk";
    ShareableObject.Supplier<CloseableString> supplier = () -> new CloseableString(value);
    ShareableObject<CloseableString> expResult_0 = ShareableObject.create(null, supplier);
    assertEquals(1, expResult_0.getReferenceCount());
    assertEquals(false, expResult_0.isClosed());
    assertEquals(true, expResult_0.hasReference());
    assertEquals(value, expResult_0.get().toString());
    assertEquals(false, expResult_0.get().isClosed());
    assertEquals(1, ShareableObject.size());

    ExecutorService service = Executors.newFixedThreadPool(100);
    List<ShareableObject<CloseableString>> results = new LinkedList();
    IntStream.range(0, 100).forEach(i -> {
      service.execute(() -> {
        try {
          results.add(ShareableObject.create(null, supplier));
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
    });
    service.shutdown();
    service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    assertEquals(1, expResult_0.getReferenceCount());
    assertEquals(101, ShareableObject.size());
  }

  private static class CloseableString implements Closeable {

    private final String value;
    private boolean isClosed = false;

    private CloseableString(final String v) {
      value = v;
    }

    public boolean isClosed() {
      return isClosed;
    }

    @Override
    public String toString() {
      return value;
    }

    @Override
    public void close() throws IOException {
      isClosed = true;
    }
  }
}
