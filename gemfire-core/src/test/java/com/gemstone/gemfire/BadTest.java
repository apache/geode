/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

import junit.framework.*;

/**
 * This test provides examples of a test failing and a test getting an
 * error.  We use it to test JUnit failure reporting.
 */
public class BadTest extends TestCase {

  public BadTest(String name) {
    super(name);
  }

  ////////  Test Methods

  public void testFailure() {
    fail("I'm failing away...");
  }

  public void testError() {
    String s = "I've failed";
    throw new RuntimeException(s);
  }

}
