/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;
import com.gemstone.org.jgroups.oswego.concurrent.CountDown;

import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class ReentrantSemaphoreJUnitTest extends TestCase {
  
  public void test() throws Throwable {
    final ReentrantSemaphore sem = new ReentrantSemaphore(2);
    
    sem.acquire();
    sem.acquire();
    assertEquals(1, sem.availablePermits());
    sem.release();
    sem.release();
    assertEquals(2, sem.availablePermits());
    
    final CountDownLatch testDone = new CountDownLatch(1);
    final CountDownLatch semsAquired = new CountDownLatch(2);
    
    final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
    
    Thread t1 = new Thread() {
      public void run() {
        try { 
          sem.acquire();
          sem.acquire();
          sem.acquire();
          semsAquired.countDown();
          testDone.await();
          sem.release();
          sem.release();
          sem.release();
        } catch(Exception e) {
          failure.compareAndSet(null, e);
        }
      }
    };
    t1.start();
    
    Thread t2 = new Thread() {
      public void run() {
        try {
          sem.acquire();
          sem.acquire();
          sem.acquire();
          semsAquired.countDown();
          testDone.await();
          sem.release();
          sem.release();
          sem.release();
        } catch(Exception e) {
          failure.compareAndSet(null, e);
        }
      }
    };
    t2.start();
    
    Thread t3 = new Thread() {
      public void run() {
        try {
          semsAquired.await();
          assertEquals(0, sem.availablePermits());
          assertFalse(sem.tryAcquire(1, TimeUnit.SECONDS));
        } catch(Exception e) {
          failure.compareAndSet(null, e);
        }
      }
    };
    t3.start();
    
    t3.join();
    testDone.countDown();
    t2.join();
    t1.join();
    
    if(failure.get() != null) {
      throw failure.get();
    }
    
    assertEquals(2, sem.availablePermits());
  }

}
