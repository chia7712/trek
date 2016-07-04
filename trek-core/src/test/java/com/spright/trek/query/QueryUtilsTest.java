package com.spright.trek.query;

import com.spright.trek.DConstants;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class QueryUtilsTest {

  public QueryUtilsTest() {
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
   * Test of parseEmptyString method, of class BaseQuery.
   */
  @Test
  public void testParseEmptyString() {
    System.out.println("parseEmptyString");
    assertEquals(QueryUtils.parseEmptyString("").isPresent(), false);
    assertEquals(QueryUtils.parseEmptyString(null).isPresent(), false);
    assertEquals(QueryUtils.parseEmptyString("asd").isPresent(), true);
  }

  /**
   * Test of parseBoolean method, of class BaseQuery.
   */
  @Test
  public void testParseBoolean() {
    System.out.println("parseBoolean");
    assertEquals(QueryUtils.parseBoolean("false", true), false);
    assertEquals(QueryUtils.parseBoolean("true", true), true);
    assertEquals(QueryUtils.parseBoolean("asdsadsad", true), true);
    assertEquals(QueryUtils.parseBoolean("asdasd", false), false);
  }

  /**
   * Test of parsePositiveTime method, of class BaseQuery.
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testParsePositiveTime_SimpleDateFormat_String() throws ParseException {
    System.out.println("parsePositiveTime");
    SimpleDateFormat timeSdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    String value = "1999-01-02T11:11:11.111";
    long expResult = timeSdf.parse(value).getTime();
    Optional<Long> result = QueryUtils.parsePositiveTime(timeSdf, value);
    assertEquals(true, result.isPresent());
    assertEquals(expResult, (long) result.get());
    Optional<Long> result2 = QueryUtils.parsePositiveTime(timeSdf, "asdsad");
    assertEquals(false, result2.isPresent());
  }

  /**
   * Test of parsePositiveTime method, of class BaseQuery.
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testParsePositiveTime_3args() throws ParseException {
    System.out.println("parsePositiveTime");
    SimpleDateFormat timeSdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    String value = "1999-01-02T11:11:11.111";
    long expResult = timeSdf.parse(value).getTime();
    long defaultValue = 1234556;
    long result = QueryUtils.parsePositiveTime(timeSdf, value, defaultValue);
    assertEquals(expResult, result);
    long result2 = QueryUtils.parsePositiveTime(timeSdf, "asdasdsad", defaultValue);
    assertEquals(defaultValue, result2);
  }

  /**
   * Test of parsePositiveRange method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveRange_5args_1() {
    System.out.println("parsePositiveRange");
    RangeInteger result_0 = QueryUtils.parsePositiveRange(String.valueOf(123), 12);
    assertEquals(123, (int) result_0.getMinValue());
    assertEquals(123, (int) result_0.getMaxValue());

    RangeInteger result_1 = QueryUtils.parsePositiveRange("(1, 5)", 12);
    assertEquals(1, (int) result_1.getMinValue());
    assertEquals(5, (int) result_1.getMaxValue());

  }

  /**
   * Test of parsePositiveRange method, of class BaseQuery.
   *
   * @throws java.text.ParseException
   */
  @Test
  public void testParsePositiveRange_6args() throws ParseException {
    System.out.println("parsePositiveRange");
    SimpleDateFormat timeSdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    long value = timeSdf.parse("1999-01-02T11:11:11.111").getTime();
    long defaultMin = timeSdf.parse("1999-01-02T11:11:11.111").getTime();
    long defaultMax = timeSdf.parse("2099-01-02T11:11:11.111").getTime();
    RangeLong result = QueryUtils.parsePositiveRange(timeSdf, "(1999-01-02T11:11:11.111, 2099-01-02T11:11:11.111)", value);
    assertEquals("2099-01-02T11:11:11.111", timeSdf.format(new Date(result.getMaxValue())));
    assertEquals(defaultMin, (long) result.getMinValue());
    assertEquals(defaultMax, (long) result.getMaxValue());

  }

  /**
   * Test of parsePositiveRange method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveRange_5args_3() {
    System.out.println("parsePositiveRange");
    RangeDouble result_0 = QueryUtils.parsePositiveRange(String.valueOf("0.4"), 0.3);
    assertEquals(0.4, (double) result_0.getMinValue(), 0);
    assertEquals(0.4, (double) result_0.getMaxValue(), 0);

    RangeDouble result_1 = QueryUtils.parsePositiveRange("(0.3, 0.7)", 12.1);
    assertEquals(0.3, (double) result_1.getMinValue(), 0);
    assertEquals(0.7, (double) result_1.getMaxValue(), 0);
  }

  /**
   * Test of parsePositiveIntegerRange method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveIntegerRange() {
    System.out.println("parsePositiveRange");
    RangeLong result_0 = QueryUtils.parsePositiveRange(String.valueOf("12345"), (long) 12345);
    assertEquals(12345, (long) result_0.getMinValue());
    assertEquals(12345, (long) result_0.getMaxValue());

    RangeLong result_1 = QueryUtils.parsePositiveRange("(12345, 123444)", (long) 12345);
    assertEquals(12345, (long) result_1.getMinValue());
    assertEquals(123444, (long) result_1.getMaxValue());
  }

  /**
   * Test of parsePositiveDouble method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveDouble() {
    System.out.println("parsePositiveDouble");
    assertEquals(true, QueryUtils.parsePositiveDouble("0.3").isPresent());
    assertEquals(false, QueryUtils.parsePositiveDouble("asdasd").isPresent());
    assertEquals(false, QueryUtils.parsePositiveDouble("-0.1").isPresent());
    assertEquals(true, QueryUtils.parsePositiveDouble("0").isPresent());
  }

  /**
   * Test of parsePositiveInteger method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveInteger() {
    System.out.println("parsePositiveInteger");
    assertEquals(true, QueryUtils.parsePositiveInteger("123").isPresent());
    assertEquals(true, QueryUtils.parsePositiveInteger("0").isPresent());
    assertEquals(false, QueryUtils.parsePositiveInteger("-1").isPresent());
    assertEquals(false, QueryUtils.parsePositiveInteger("ASDSAD").isPresent());
  }

  /**
   * Test of parsePositiveLong method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveLong() {
    System.out.println("parsePositiveLong");
    assertEquals(true, QueryUtils.parsePositiveLong("123").isPresent());
    assertEquals(true, QueryUtils.parsePositiveLong("0").isPresent());
    assertEquals(false, QueryUtils.parsePositiveLong("-1").isPresent());
    assertEquals(false, QueryUtils.parsePositiveLong("ASDSAD").isPresent());
  }

  /**
   * Test of parsePositiveValue method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveValue_String_double() {
    System.out.println("parsePositiveValue");
    assertEquals(0.1, QueryUtils.parsePositiveValue("0.1", 0.3), 0);
    assertEquals(0, QueryUtils.parsePositiveValue("0", 0.3), 0);
    assertEquals(0.3, QueryUtils.parsePositiveValue("-0.1", 0.3), 0);
    assertEquals(0.3, QueryUtils.parsePositiveValue("asdsad", 0.3), 0);
  }

  /**
   * Test of parsePositiveValue method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveValue_3args_1() {
    System.out.println("parsePositiveValue");
    assertEquals(0.1, QueryUtils.parsePositiveValue("0.1", 0.9, 0.3), 0);
    assertEquals(0, QueryUtils.parsePositiveValue("0", 0.9, 0.3), 0);
    assertEquals(0.9, QueryUtils.parsePositiveValue("-0.1", 0.9, 0.3), 0);
    assertEquals(0.3, QueryUtils.parsePositiveValue("asdsad", 0.9, 0.3), 0);
  }

  /**
   * Test of parsePositiveValue method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveValue_String_long() {
    System.out.println("parsePositiveValue");
    assertEquals(1, QueryUtils.parsePositiveValue("1", 123L));
    assertEquals(0, QueryUtils.parsePositiveValue("0", 123L));
    assertEquals(123, QueryUtils.parsePositiveValue("-1", 123L));
    assertEquals(123, QueryUtils.parsePositiveValue("ASDASD", 123L));

  }

  /**
   * Test of parsePositiveValue method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveValue_3args_2() {
    System.out.println("parsePositiveValue");
    assertEquals(1, QueryUtils.parsePositiveValue("1", 234L, 123L));
    assertEquals(0, QueryUtils.parsePositiveValue("0", 234L, 123L));
    assertEquals(234, QueryUtils.parsePositiveValue("-1", 234L, 123L));
    assertEquals(123, QueryUtils.parsePositiveValue("ASDASD", 234L, 123L));
  }

  /**
   * Test of parsePositiveValue method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveValue_String_int() {
    System.out.println("parsePositiveValue");
    assertEquals(1, QueryUtils.parsePositiveValue("1", 123));
    assertEquals(0, QueryUtils.parsePositiveValue("0", 123));
    assertEquals(123, QueryUtils.parsePositiveValue("-1", 123));
    assertEquals(123, QueryUtils.parsePositiveValue("ASDASD", 123));
  }

  /**
   * Test of parsePositiveValue method, of class BaseQuery.
   */
  @Test
  public void testParsePositiveValue_3args_3() {
    System.out.println("parsePositiveValue");
    assertEquals(1, QueryUtils.parsePositiveValue("1", 234, 123));
    assertEquals(0, QueryUtils.parsePositiveValue("0", 234, 123));
    assertEquals(234, QueryUtils.parsePositiveValue("-1", 234, 123));
    assertEquals(123, QueryUtils.parsePositiveValue("ASDASD", 234, 123));
  }

}
