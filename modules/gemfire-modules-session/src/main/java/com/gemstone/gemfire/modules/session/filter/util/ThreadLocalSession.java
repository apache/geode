/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.filter.util;

import javax.servlet.http.HttpSession;

/**
 */
public class ThreadLocalSession {
  private static ThreadLocal<HttpSession> threadLocal =
      new ThreadLocal<HttpSession>();

  public static HttpSession get() {
    return threadLocal.get();
  }

  public static void set(HttpSession session) {
    threadLocal.set(session);
  }

  public static void remove() {
    threadLocal.remove();
  }
}
