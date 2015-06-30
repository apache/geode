/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.modules.session.filter.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory to create named threads for easy identification at runtime.
 */
public class NamedThreadFactory implements ThreadFactory {

  private static final Logger LOG = Logger.getLogger(
      NamedThreadFactory.class.getName());

  private final String id;

  private final AtomicLong serial = new AtomicLong();

  /**
   * Create a new thread factory, using the specified pool ID as a basis for
   * naming each thread.
   *
   * @param poolID pool name/ID
   */
  public NamedThreadFactory(final String poolID) {
    id = poolID;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * This implementation sets the name of the thread, sets the thread to be a
   * daemon thread, and adds an uncaught exception handler.
   */
  @Override
  public Thread newThread(Runnable r) {
    Thread thr = new Thread(r);
    thr.setDaemon(true);
    thr.setName(id + " - " + serial.incrementAndGet());
    thr.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        LOG.log(Level.WARNING,
            "Uncaught Exception in thread: " + t.getName(), e);
      }
    });
    return thr;
  }
}
