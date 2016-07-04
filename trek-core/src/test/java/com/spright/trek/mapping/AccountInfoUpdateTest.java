/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spright.trek.mapping;

import com.spright.trek.query.EmptyableInteger;
import com.spright.trek.query.EmptyableString;
import java.util.concurrent.atomic.AtomicBoolean;
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
public class AccountInfoUpdateTest {

  public AccountInfoUpdateTest() {
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
   * Test of newBuilder method, of class AccountInfoUpdate.
   */
  @Test
  public void testNewBuilder() {
    System.out.println("newBuilder");
    AccountInfoUpdate expResult = AccountInfoUpdate.newBuilder()
            .setId("id")
            .setDomain(new EmptyableString("domain"))
            .setHost(new EmptyableString("host"))
            .setPassword(new EmptyableString("password"))
            .setUser(new EmptyableString("user"))
            .setPort(new EmptyableInteger(123))
            .build();

    AtomicBoolean domain = new AtomicBoolean(false);
    AtomicBoolean user = new AtomicBoolean(false);
    AtomicBoolean password = new AtomicBoolean(false);
    AtomicBoolean host = new AtomicBoolean(false);
    AtomicBoolean port = new AtomicBoolean(false);
    expResult.getDomain().ifHasValue(v -> domain.set(true));
    expResult.getUser().ifHasValue(v -> user.set(true));
    expResult.getPassword().ifHasValue(v -> password.set(true));
    expResult.getHost().ifHasValue(v -> host.set(true));
    expResult.getPort().ifHasValue(v -> port.set(true));
    assertEquals("id", expResult.getId());
    assertEquals(true, domain.get());
    assertEquals(true, user.get());
    assertEquals(true, password.get());
    assertEquals(true, host.get());
    assertEquals(true, port.get());
  }

}
