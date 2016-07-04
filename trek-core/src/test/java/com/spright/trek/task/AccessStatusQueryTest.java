package com.spright.trek.task;

import com.spright.trek.DConstants;
import com.spright.trek.query.OrderKey;
import com.spright.trek.query.RangeDouble;
import com.spright.trek.query.RangeLong;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author chia7712
 */
public class AccessStatusQueryTest {

  public AccessStatusQueryTest() {
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
   * Test of newBuilder method, of class AccessStatusQuery.
   */
  @Test
  public void testNewBuilder() {
    System.out.println("newBuilder");
    AccessStatusQuery query = AccessStatusQuery.newBuilder()
            .setClientName("client_name")
            .setElapsed(new RangeLong(0, 100))
            .setExpectedSize(new RangeLong(0, 100))
            .setFrom("from")
            .setId("id")
            .setKeep(false)
            .setLimit(100)
            .setOffset(11)
            .setProgress(new RangeDouble(0.1, 0.9))
            .setRedirectFrom("redirect_from")
            .setServerName("server_name")
            .setStartTime(new RangeLong(0, 100))
            .setTaskState(TaskState.FAILED)
            .setTo("to")
            .setTransferredSize(new RangeLong(0, 100))
            .build();
    assertEquals("client_name", query.getClientName().get());
    assertEquals(new RangeLong(0, 100), query.getElapsed().get());
    assertEquals(new RangeLong(0, 100), query.getExpectedSize().get());
    assertEquals("from", query.getFrom().get());
    assertEquals("id", query.getId().get());
    assertEquals(false, query.getKeep());
    assertEquals(100, query.getLimit());
    assertEquals(11, query.getOffset());
    assertEquals(new RangeDouble(0.1, 0.9), query.getProgress().get());
    assertEquals("redirect_from", query.getRedirectFrom().get());
    assertEquals("server_name", query.getServerName().get());
    assertEquals(new RangeLong(0, 100), query.getStartTime().get());
    assertEquals(TaskState.FAILED, query.getState().get());
    assertEquals("to", query.getTo().get());
    assertEquals(new RangeLong(0, 100), query.getTransferredSize().get());
  }

  /**
   * Test of getRedirectFrom method, of class AccessStatusQuery.
   */
  @Test
  public void testGetRedirectFrom() {
    System.out.println("getRedirectFrom");
    Map<String, String> rawQuery = new TreeMap<>();
    String expResult = "abc";
    rawQuery.put(DConstants.URI_TASK_REDIRECT, expResult);
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<String> result = instance.getRedirectFrom();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, result.get());
  }

  /**
   * Test of getServerName method, of class AccessStatusQuery.
   */
  @Test
  public void testGetServerName() {
    System.out.println("getServerName");
    Map<String, String> rawQuery = new TreeMap<>();
    String expResult = "abc";
    rawQuery.put(DConstants.URI_TASK_SERVER_NAME, expResult);
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<String> result = instance.getServerName();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, result.get());
  }

  /**
   * Test of getClientName method, of class AccessStatusQuery.
   */
  @Test
  public void testGetClientName() {
    System.out.println("getClientName");
    Map<String, String> rawQuery = new TreeMap<>();
    String expResult = "abc";
    rawQuery.put(DConstants.URI_TASK_CLIENT_NAME, expResult);
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<String> result = instance.getClientName();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, result.get());
  }

  /**
   * Test of getId method, of class AccessStatusQuery.
   */
  @Test
  public void testGetId() {
    System.out.println("getId");
    Map<String, String> rawQuery = new TreeMap<>();
    String expResult = "abc";
    rawQuery.put(DConstants.URI_TASK_ID, expResult);
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<String> result = instance.getId();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, result.get());
  }

  /**
   * Test of getFrom method, of class AccessStatusQuery.
   */
  @Test
  public void testGetFrom() {
    System.out.println("getFrom");
    Map<String, String> rawQuery = new TreeMap<>();
    String expResult = "abc";
    rawQuery.put(DConstants.URI_TASK_FROM, expResult);
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<String> result = instance.getFrom();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, result.get());
  }

  /**
   * Test of getTo method, of class AccessStatusQuery.
   */
  @Test
  public void testGetTo() {
    System.out.println("getTo");
    Map<String, String> rawQuery = new TreeMap<>();
    String expResult = "abc";
    rawQuery.put(DConstants.URI_TASK_TO, expResult);
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<String> result = instance.getTo();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, result.get());
  }

  /**
   * Test of getState method, of class AccessStatusQuery.
   */
  @Test
  public void testGetState() {
    System.out.println("getState");
    Map<String, String> rawQuery = new TreeMap<>();
    TaskState expResult = TaskState.ABORT;
    rawQuery.put(DConstants.URI_TASK_STATE, TaskState.ABORT.getDescription());
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<TaskState> result = instance.getState();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, result.get());
  }

  /**
   * Test of getProgress method, of class AccessStatusQuery.
   */
  @Test
  public void testGetProgress() {
    System.out.println("getProgress");
    Map<String, String> rawQuery = new TreeMap<>();
    double expResult = 0.7;
    rawQuery.put(DConstants.URI_TASK_PROGRESS, String.valueOf(expResult));
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<RangeDouble> result = instance.getProgress();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, result.get().getMinValue(), 0);
    assertEquals(expResult, result.get().getMaxValue(), 0);
  }

  /**
   * Test of getStartTime method, of class AccessStatusQuery.
   */
  @Test
  public void testGetStartTime() {
    System.out.println("getStartTime");
    SimpleDateFormat sdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    Map<String, String> rawQuery = new TreeMap<>();
    long expResult = System.currentTimeMillis();
    rawQuery.put(DConstants.URI_TASK_START_TIME, String.valueOf(sdf.format(new Date(expResult))));
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<RangeLong> result = instance.getStartTime();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, (long) result.get().getMinValue());
    assertEquals(expResult, (long) result.get().getMaxValue());
  }

  /**
   * Test of getElapsed method, of class AccessStatusQuery.
   */
  @Test
  public void testGetElapsed() {
    System.out.println("getElapsed");
    Map<String, String> rawQuery = new TreeMap<>();
    long expResult = System.currentTimeMillis();
    rawQuery.put(DConstants.URI_TASK_ELAPSED, String.valueOf(expResult));
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<RangeLong> result = instance.getElapsed();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, (long) result.get().getMinValue());
    assertEquals(expResult, (long) result.get().getMaxValue());
  }

  /**
   * Test of getExpectedSize method, of class AccessStatusQuery.
   */
  @Test
  public void testGetExpectedSize() {
    System.out.println("getExpectedSize");
    Map<String, String> rawQuery = new TreeMap<>();
    long expResult = System.currentTimeMillis();
    rawQuery.put(DConstants.URI_TASK_EXPECTED_SIZE, String.valueOf(expResult));
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<RangeLong> result = instance.getExpectedSize();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, (long) result.get().getMinValue());
    assertEquals(expResult, (long) result.get().getMaxValue());
  }

  /**
   * Test of getTransferredSize method, of class AccessStatusQuery.
   */
  @Test
  public void testGetTransferredSize() {
    System.out.println("getTransferredSize");
    Map<String, String> rawQuery = new TreeMap<>();
    long expResult = System.currentTimeMillis();
    rawQuery.put(DConstants.URI_TASK_TRANSFERRED_SIZE, String.valueOf(expResult));
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Optional<RangeLong> result = instance.getTransferredSize();
    assertEquals(true, result.isPresent());
    assertEquals(expResult, (long) result.get().getMinValue());
    assertEquals(expResult, (long) result.get().getMaxValue());
  }

  /**
   * Test of getOrderKey method, of class AccessStatusQuery.
   */
  @Test
  public void testGetOrderKey() {
    System.out.println("getOrderKey");
    Map<String, String> rawQuery = new TreeMap<>();
    rawQuery.put(DConstants.URI_DATA_ORDER_BY, AccessStatus.Field.CLIENT_NAME.getDescription());
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    Set<OrderKey<AccessStatus.Field>> result = instance.getOrderKey();
    assertEquals(1, result.size());
    assertEquals(AccessStatus.Field.CLIENT_NAME, result.iterator().next().getKey());
  }

  /**
   * Test of getLimit method, of class AccessStatusQuery.
   */
  @Test
  public void testGetLimit() {
    System.out.println("getLimit");
    Map<String, String> rawQuery = new TreeMap<>();
    long expResult = 1020;
    rawQuery.put(DConstants.URI_DATA_LIMIT, String.valueOf(expResult));
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    int result = instance.getLimit();
    assertEquals(expResult, result);
  }

  /**
   * Test of getOffset method, of class AccessStatusQuery.
   */
  @Test
  public void testGetOffset() {
    System.out.println("getOffset");
    Map<String, String> rawQuery = new TreeMap<>();
    long expResult = 1020;
    rawQuery.put(DConstants.URI_DATA_OFFSET, String.valueOf(expResult));
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    int result = instance.getOffset();
    assertEquals(expResult, result);
  }

  /**
   * Test of getPredicate method, of class AccessStatusQuery.
   */
  @Test
  public void testGetPredicate() {
    System.out.println("getPredicate");
    AccessStatusQuery query = AccessStatusQuery.newBuilder()
            .setClientName("client_name")
            .setElapsed(new RangeLong(0, 100))
            .setExpectedSize(new RangeLong(0, 100))
            .setFrom("from")
            .setId("id")
            .setKeep(false)
            .setLimit(100)
            .setOffset(11)
            .setProgress(new RangeDouble(0.1, 0.9))
            .setRedirectFrom("redirect_from")
            .setServerName("server_name")
            .setStartTime(new RangeLong(0, 100))
            .setTaskState(TaskState.FAILED)
            .setTo("to")
            .setTransferredSize(new RangeLong(0, 100))
            .build();
    AccessStatus status_0 = AccessStatus.newBuilder()
            .setClientName("client_name")
            .setElapsed(12)
            .setExpectedSize(12)
            .setFrom("from")
            .setId("id")
            .setProgress(0.2)
            .setRedirectFrom("redirect_from")
            .setServerName("server_name")
            .setStartTime(13)
            .setTaskState(TaskState.FAILED)
            .setTo("to")
            .setTransferredSize(12)
            .build();
    AccessStatus status_1 = AccessStatus.newBuilder()
            .setClientName("client_name")
            .setElapsed(999)
            .setExpectedSize(12)
            .setFrom("from")
            .setId("id")
            .setProgress(0.2)
            .setRedirectFrom("redirect_from")
            .setServerName("server_name")
            .setStartTime(13)
            .setTaskState(TaskState.FAILED)
            .setTo("to")
            .setTransferredSize(12)
            .build();
    AccessStatus status_2 = AccessStatus.newBuilder()
            .setClientName("client_name")
            .setElapsed(13)
            .setExpectedSize(12)
            .setFrom("fromm")
            .setId("id")
            .setProgress(0.2)
            .setRedirectFrom("redirect_from")
            .setServerName("server_name")
            .setStartTime(13)
            .setTaskState(TaskState.FAILED)
            .setTo("to")
            .setTransferredSize(12)
            .build();
    Predicate<AccessStatus> predicate = query.getPredicate();
    assertEquals(true, predicate.test(status_0));
    assertEquals(false, predicate.test(status_1));
    assertEquals(false, predicate.test(status_2));
  }

  /**
   * Test of getComparator method, of class AccessStatusQuery.
   */
  @Test
  public void testGetComparator() {
    System.out.println("getComparator");
    Set<OrderKey<AccessStatus.Field>> order = new LinkedHashSet<>();
    order.add(new OrderKey<>(AccessStatus.Field.PROGRESS, false));
    order.add(new OrderKey<>(AccessStatus.Field.ID, true));
    AccessStatusQuery instance = AccessStatusQuery.newBuilder()
            .setOrderKeys(order)
            .build();
    assertEquals(2, order.size());
    Comparator<AccessStatus> expResult = instance.getComparator();
    AccessStatus status_0 = AccessStatus.newBuilder()
            .setClientName("client_name")
            .setElapsed(12)
            .setExpectedSize(12)
            .setFrom("from")
            .setId("id")
            .setProgress(0.2)
            .setRedirectFrom("redirect_from")
            .setServerName("server_name")
            .setStartTime(13)
            .setTaskState(TaskState.FAILED)
            .setTo("to")
            .setTransferredSize(12)
            .build();
    AccessStatus status_1 = AccessStatus.newBuilder()
            .setClientName("client_name")
            .setElapsed(999)
            .setExpectedSize(12)
            .setFrom("from")
            .setId("id1")
            .setProgress(0.3)
            .setRedirectFrom("redirect_from")
            .setServerName("server_name")
            .setStartTime(13)
            .setTaskState(TaskState.FAILED)
            .setTo("to")
            .setTransferredSize(12)
            .build();
    AccessStatus status_2 = AccessStatus.newBuilder()
            .setClientName("client_name")
            .setElapsed(999)
            .setExpectedSize(12)
            .setFrom("from")
            .setId("id0")
            .setProgress(0.3)
            .setRedirectFrom("redirect_from")
            .setServerName("server_name")
            .setStartTime(13)
            .setTaskState(TaskState.FAILED)
            .setTo("to")
            .setTransferredSize(12)
            .build();
    assertEquals(1, expResult.compare(status_0, status_1));
    assertEquals(1, expResult.compare(status_1, status_2));
  }

  /**
   * Test of getKeep method, of class AccessStatusQuery.
   */
  @Test
  public void testGetKeep() {
    System.out.println("getKeep");
    Map<String, String> rawQuery = new TreeMap<>();
    boolean expResult = false;
    rawQuery.put(DConstants.URI_DATA_KEEP, String.valueOf(expResult));
    AccessStatusQuery instance = AccessStatusQuery.parse(rawQuery);
    boolean result = instance.getKeep();
    assertEquals(expResult, result);
  }

}
