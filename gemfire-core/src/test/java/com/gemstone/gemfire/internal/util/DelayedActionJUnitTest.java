/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

@Category(UnitTest.class)
public class DelayedActionJUnitTest extends TestCase {
  public void testDelay() throws InterruptedException {
    final AtomicBoolean hit = new AtomicBoolean(false);
    final CountDownLatch complete = new CountDownLatch(1);
    
    Runnable r = new Runnable() {
      @Override
      public void run() {
        hit.set(true);
        complete.countDown();
      }
    };
    
    DelayedAction delay = new DelayedAction(r);
    
    ExecutorService exec = Executors.newSingleThreadExecutor();
    exec.execute(delay);
    
    delay.waitForArrival();
    assertFalse(hit.get());
    
    delay.allowToProceed();
    complete.await();
    assertTrue(hit.get());
  }
}
