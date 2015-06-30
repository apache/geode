/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package dunit.tests;

import dunit.*;

/**
 * The tests in this class always fail.  It is used when developing
 * DUnit to give us an idea of how test failure are logged, etc.
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public class TestFailure extends DistributedTestCase {

  public TestFailure(String name) {
    super(name);
  }

  ////////  Test Methods

  public void testFailure() {
    assertTrue("Test Failure", false);
  }

  public void testError() {
    String s = "Test Error";
    throw new Error(s);
  }

  public void testHang() throws InterruptedException {
    Thread.sleep(100000 * 1000);
  }

}
