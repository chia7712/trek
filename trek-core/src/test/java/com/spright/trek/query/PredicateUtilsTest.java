package com.spright.trek.query;

import java.util.Arrays;
import java.util.function.Predicate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class PredicateUtilsTest {

  public PredicateUtilsTest() {
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
   * Test of empty method, of class PredicateUtils.
   */
  @Test
  public void testEmpty() {
    System.out.println("empty");
    Predicate<String> result = PredicateUtils.empty();
    assertEquals(true, result.test("xx"));
    assertEquals(true, result.test("asdsad"));
  }

  /**
   * Test of newPredicate method, of class PredicateUtils.
   */
  @Test
  public void testNewPredicate_List() {
    System.out.println("newPredicate");
    Predicate<Integer> result = PredicateUtils.newPredicate(Arrays.asList(
            v -> v <= 5,
            v -> v >= 0
    ));
    assertEquals(true, result.test(3));
    assertEquals(false, result.test(8));
    assertEquals(false, result.test(-1));
  }

  /**
   * Test of newPredicate method, of class PredicateUtils.
   */
  @Test
  public void testNewPredicate_GenericType() {
    System.out.println("newPredicate");
    Integer v = 123;
    Predicate<Integer> result = PredicateUtils.newPredicate(v);
    assertEquals(true, result.test(123));
    assertEquals(false, result.test(111));
  }

  /**
   * Test of newPredicate method, of class PredicateUtils.
   */
  @Test
  public void testNewPredicate_Function_Predicate() {
    System.out.println("newPredicate");
    Predicate<String> result = PredicateUtils.newPredicate(v -> Integer.valueOf(v), v -> v <= 5);
    assertEquals(true, result.test("1"));
    assertEquals(false, result.test("8"));
  }

  /**
   * Test of newPredicate method, of class PredicateUtils.
   */
  @Test
  public void testNewPredicate_String() {
    System.out.println("newPredicate");
    Predicate<String> result = PredicateUtils.newPredicate("*abc");
    assertEquals(true, result.test("aabc"));
    assertEquals(true, result.test("aaaaaabc"));
    assertEquals(false, result.test("aaaaa"));
  }

  /**
   * Test of newPredicate method, of class PredicateUtils.
   */
  @Test
  public void testNewPredicate_RangeLong() {
    System.out.println("newPredicate");
    RangeLong range = new RangeLong(123, 456);
    Predicate<Long> result = PredicateUtils.newPredicate(range);
    assertEquals(true, result.test(133L));
    assertEquals(false, result.test(1L));
  }

  /**
   * Test of newPredicate method, of class PredicateUtils.
   */
  @Test
  public void testNewPredicate_RangeInteger() {
    System.out.println("newPredicate");
    RangeInteger range = new RangeInteger(1, 10);
    Predicate<Integer> result = PredicateUtils.newPredicate(range);
    assertEquals(true, result.test(4));
    assertEquals(false, result.test(0));
  }

  /**
   * Test of newPredicate method, of class PredicateUtils.
   */
  @Test
  public void testNewPredicate_RangeNumber() {
    System.out.println("newPredicate");
    RangeDouble range = new RangeDouble(0.3, 0.7);
    Predicate<Double> result = PredicateUtils.newPredicate(range);
    assertEquals(true, result.test(0.5));
    assertEquals(false, result.test(0.1));
  }

}
