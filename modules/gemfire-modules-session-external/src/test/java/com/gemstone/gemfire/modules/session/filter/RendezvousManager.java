/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.modules.session.filter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RendezvousManager {

  private static AbstractListener listener = null;

  private static CountDownLatch latch = new CountDownLatch(1);

  public static void registerListener(AbstractListener listener) {
    RendezvousManager.listener = listener;
    latch.countDown();
  }

  public static AbstractListener getListener() {
    try {
      latch.await(2, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
    }

    return listener;
  }

}
