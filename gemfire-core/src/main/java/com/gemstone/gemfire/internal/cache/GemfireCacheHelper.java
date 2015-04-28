/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
/*
 * Purpose of this class to create threadfactory and other helper classes for GemfireCache.
 * If we keep these classes as inner class of GemFireCache then some time it holds reference of static cache
 * Which can cause leak if app is just  starting and closing cache 
 */
public class GemfireCacheHelper {

  public static ThreadFactory CreateThreadFactory(final ThreadGroup tg, final String threadName) {
    final ThreadFactory threadFactory = new ThreadFactory() {
      public Thread newThread(Runnable command) {
        Thread thread = new Thread(tg, command, threadName);
        thread.setDaemon(true);
        return thread;
      }
    };
    return threadFactory;
  }
}
