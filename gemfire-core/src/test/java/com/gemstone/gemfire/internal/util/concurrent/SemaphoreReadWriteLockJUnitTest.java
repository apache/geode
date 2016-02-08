/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

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
