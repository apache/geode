/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

/**
 * Allows tests to expect certain exceptions without the SystemFailure watchdog getting upset.
 * See bug 46988.
 * 
 * @author darrel
 * @since 7.0.1
 */
public class SystemFailureTestHook {

  private static Class<?> expectedClass;
  
  /**
   * If a test sets this to a non-null value then it should also
   * set it to "null" with a finally block.
   */
  public static void setExpectedFailureClass(Class<?> expected) {
    expectedClass = expected;
  }

  /**
   * Returns true if the given failure is expected.
   */
  public static boolean errorIsExpected(Error failure) {
    return expectedClass != null && expectedClass.isInstance(failure);
  }

  public static void loadEmergencyClasses() {
    // nothing more needed
  }
}
