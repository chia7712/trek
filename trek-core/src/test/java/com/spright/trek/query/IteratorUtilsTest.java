package com.spright.trek.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class IteratorUtilsTest {

  public IteratorUtilsTest() {
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

  private static int count(CloseableIterator<?> iter) {
    int count = 0;
    while (iter.hasNext()) {
      iter.next();
      ++count;
    }
    return count;
  }

  /**
   * Test of empty method, of class IteratorUtils.
   */
  @Test
  public void testEmpty() {
    System.out.println("empty");
    CloseableIterator<String> result = IteratorUtils.empty();
    assertEquals(0, count(result));
  }

  /**
   * Test of wrap method, of class IteratorUtils.
   */
  @Test
  public void testWrap_Iterator() {
    System.out.println("wrap_single");
    CloseableIterator<String> result = IteratorUtils.wrap("aaa");
    assertEquals(1, count(result));
  }

  /**
   * Test of wrap method, of class IteratorUtils.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testWrap_3args() throws IOException {
    System.out.println("wrap_closeable");
    List<String> data = Arrays.asList("xx");
    AtomicBoolean closed = new AtomicBoolean(false);
    try (CloseableIterator<String> result = IteratorUtils.wrap(data.iterator(), () -> {
      closed.set(true);
    })) {
      System.out.println("waitting count");
      assertEquals(1, count(result));
      System.out.println("waitting count...done");
      assertEquals(false, closed.get());
    }
    assertEquals(true, closed.get());
  }

  /**
   * Test of wrap method, of class IteratorUtils.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testWrap_CloseableIterator_Function() throws IOException {
    System.out.println("wrap_function");
    List<String> data = new LinkedList<>();
    IntStream.range(0, 10).forEach(v -> data.add(String.valueOf(v)));
    assertEquals(10, data.size());
    try (CloseableIterator<Integer> iter = IteratorUtils.wrap(data.iterator(), (String v) -> Integer.valueOf(v))) {
      assertEquals(10, count(iter));
    }
    try (CloseableIterator<Integer> iter = IteratorUtils.wrap(data.iterator(), (String v) -> Integer.valueOf(v) > 5 ? null : Integer.valueOf(v))) {
      assertEquals(6, count(iter));
    }
  }

  /**
   * Test of wrap method, of class IteratorUtils.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testWrap_CloseableIterator_Comparator() throws IOException {
    System.out.println("wrap_cmp");
    Comparator<Integer> cmp = (v1, v2) -> Integer.compare(v1, v2);
    List<Integer> data = new LinkedList<>();
    IntStream.range(0, 3).forEach(data::add);
    Collections.shuffle(data);
    try (CloseableIterator<Integer> iter = IteratorUtils.wrap(data.iterator(), cmp)) {
      assertEquals(0, (int) iter.next());
      assertEquals(1, (int) iter.next());
      assertEquals(2, (int) iter.next());
    }
  }

  /**
   * Test of wrap method, of class IteratorUtils.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testWrap_CloseableIterator_Predicate() throws IOException {
    System.out.println("wrap_predicate");
    Predicate<Integer> predicate = (Integer v) -> v <= 2;
    List<Integer> data = new LinkedList<>();
    IntStream.range(0, 100).forEach(data::add);
    Collections.shuffle(data);
    try (CloseableIterator<Integer> iter = IteratorUtils.wrap(data.iterator(), predicate)) {
      assertEquals(3, count(iter));
    }
  }

  /**
   * Test of wrapLimit method, of class IteratorUtils.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testWrapLimit() throws IOException {
    System.out.println("wrapLimit");
    List<Integer> data = new LinkedList<>();
    IntStream.range(0, 100).forEach(data::add);
    Collections.shuffle(data);
    try (CloseableIterator<Integer> iter = IteratorUtils.wrapLimit(data.iterator(), 10)) {
      assertEquals(10, count(iter));
    }
  }

  /**
   * Test of wrapOffset method, of class IteratorUtils.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testWrapOffset() throws IOException {
    System.out.println("wrapOffset");
    List<Integer> data = new LinkedList<>();
    IntStream.range(0, 10).forEach(data::add);
    try (CloseableIterator<Integer> iter = IteratorUtils.wrapOffset(data.iterator(), 9)) {
      assertEquals(9, (int) iter.next());
    }
  }

  /**
   * Test of wrap method, of class IteratorUtils.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testWrapArrayIterator() throws IOException {
    System.out.println("wrapArrayIterator");
    List<Integer> data_v1 = new LinkedList<>();
    List<Integer> data_v2 = new LinkedList<>();
    IntStream.range(0, 10).forEach(data_v1::add);
    IntStream.range(0, 10).forEach(data_v2::add);
    int count = 0;
    try (CloseableIterator<Integer> iter = IteratorUtils.wrap(
            Arrays.asList(IteratorUtils.wrap(data_v1.iterator()),
                    IteratorUtils.wrap(data_v2.iterator())))) {
      while (iter.hasNext()) {
        iter.next();
        ++count;
      }
    }
    assertEquals(20, count);
  }
}
