/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.util.spring;

/**
 * Replaces org.springframework.shell.support.util.Assert which is
 * now removed from SPring Shell & the same class is referred from Spring Core
 * to avoid GemFire code dependency on Spring Core.
 * Internally uses ({@link com.gemstone.gemfire.internal.Assert}
 */
public class Assert {

  public static void isTrue(boolean b, String message) {
    com.gemstone.gemfire.internal.Assert.assertTrue(b, message);
  }

  public static void notNull(Object object, String message) {
    com.gemstone.gemfire.internal.Assert.assertTrue(object != null, message);
  }

}
