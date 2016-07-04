package com.spright.trek.query;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class ComparatorUtilsTest {

  public ComparatorUtilsTest() {
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
   * Test of newComparator method, of class ComparatorUtils.
   */
  @Test
  public void testNewComparator() {
    System.out.println("newComparator");
    List<Comparator<String>> expResult = new LinkedList<>();
    Comparator<String> result = ComparatorUtils.newComparator(expResult);
    assertEquals(true, ComparatorUtils.isEmptyComparator(result));
  }

}
