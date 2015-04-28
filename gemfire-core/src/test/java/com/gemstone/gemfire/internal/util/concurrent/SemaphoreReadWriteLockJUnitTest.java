/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * 
 * @author sbawaska
 */
@Category(UnitTest.class)
public class SemaphoreReadWriteLockJUnitTest extends TestCase {

  public void testReaderWaitsForWriter() throws InterruptedException {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    wl.lock();
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch waitToLock = new CountDownLatch(1);
    Thread writer = new Thread(new Runnable() {
      @Override
      public void run() {
        waitToLock.countDown();
        rl.lock();
        latch.countDown();
        rl.unlock();
      }
    });
    writer.start();
    waitToLock.await();
    assertEquals(1, latch.getCount());
    wl.unlock();
    assertTrue(latch.await(10, TimeUnit.SECONDS));
  }

  public void testWriterWaitsForReader() throws InterruptedException {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    rl.lock();
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch waitToLock = new CountDownLatch(1);
    Thread writer = new Thread(new Runnable() {
      @Override
      public void run() {
        waitToLock.countDown();
        wl.lock();
        latch.countDown();
        wl.unlock();
      }
    });
    writer.start();
    waitToLock.await();
    assertEquals(1, latch.getCount());
    rl.unlock();
    assertTrue(latch.await(10, TimeUnit.SECONDS));
  }

  public void testReadersNotBlockedByReaders() throws InterruptedException {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    rl.lock();
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch waitToLock = new CountDownLatch(1);
    Thread reader = new Thread(new Runnable() {
      @Override
      public void run() {
        waitToLock.countDown();
        rl.lock();
        latch.countDown();
      }
    });
    reader.start();
    waitToLock.await();
    assertTrue(latch.await(10, TimeUnit.SECONDS));
  }

  public void testWritersBlockedByWriters() throws InterruptedException {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch waitToLock = new CountDownLatch(1);
    wl.lock();
    Thread writer = new Thread(new Runnable() {
      @Override
      public void run() {
        waitToLock.countDown();
        wl.lock();
        latch.countDown();
        wl.unlock();
      }
    });
    writer.start();
    waitToLock.await();
    assertEquals(1, latch.getCount());
    wl.unlock();
    assertTrue(latch.await(10, TimeUnit.SECONDS));
  }

  public void testTrylock() throws InterruptedException {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch waitToLock = new CountDownLatch(1);
    assertTrue(wl.tryLock());
    final AtomicBoolean failed = new AtomicBoolean(false);
    Thread reader = new Thread(new Runnable() {
      @Override
      public void run() {
        waitToLock.countDown();
        if (rl.tryLock()) {
          failed.set(true);
        }
        latch.countDown();
      }
    });
    reader.start();
    waitToLock.await();
    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertFalse(failed.get());
  }

  public void testLockAndReleasebyDifferentThreads() throws InterruptedException {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch waitToLock = new CountDownLatch(1);
    rl.lock();
    Thread writer = new Thread(new Runnable() {
      @Override
      public void run() {
        waitToLock.countDown();
        try {
          assertTrue(wl.tryLock(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          fail(e.getMessage());
        }
        latch.countDown();
      }
    });
    writer.start();
    
    Thread reader2 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          waitToLock.await();
        } catch (InterruptedException e) {
          fail(e.getMessage());
        }
        rl.unlock();
      }
    });
    reader2.start();
    assertTrue(latch.await(10, TimeUnit.SECONDS));
  }
}
