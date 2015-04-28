/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.util.concurrent.CountDownLatch;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * @author dsmith
 * 
 */
@Category(UnitTest.class)
public class UnsafeThreadLocalJUnitTest extends TestCase {

  /**
   * Test that we can get the value of a thread local from another thread.
   * 
   * @throws InterruptedException
   */
  public void test() throws InterruptedException {
    final UnsafeThreadLocal<String> utl = new UnsafeThreadLocal<String>();
    final CountDownLatch localSet = new CountDownLatch(1);

    Thread test = new Thread() {
      public void run() {
        utl.set("hello");
        localSet.countDown();
        try {
          Thread.sleep(100 * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    test.setDaemon(true);
    test.start();
    localSet.await();
    assertEquals("hello", utl.get(test));
    assertEquals(null, utl.get(Thread.currentThread()));
  }

}
