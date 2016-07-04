package com.spright.trek.lock;

import com.spright.trek.DConstants;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class MemoryGlobalReadWriteLockTest {

  public MemoryGlobalReadWriteLockTest() {
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
   * Test of get method, of class GlobalReadWriteLock.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testGet() throws Exception {
    System.out.println("get");
    Configuration conf = new Configuration();
    conf.setBoolean(DConstants.ENABLE_SINGLE_MODE, true);
    LockManager lm = LockManagerFactory.newInstance(conf);
    Optional<Lock> readLock = lm.tryReadLock("AAA");
    assertEquals(true, readLock.isPresent());
    Optional<Lock> writeLock = lm.tryWriteLock("AAA");
    assertEquals(false, writeLock.isPresent());
    Optional<Lock> readLock_V2 = lm.tryReadLock("AAA");
    assertEquals(true, readLock_V2.isPresent());
    readLock_V2.get().close();
    readLock.get().close();
    Optional<Lock> writeLock_v2 = lm.tryWriteLock("AAA");
    assertEquals(true, writeLock_v2.isPresent());
  }

}
