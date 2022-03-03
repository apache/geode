/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.util.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;


public class SemaphoreReadWriteLockJUnitTest {

  private static final long OPERATION_TIMEOUT_MILLIS = 10 * 1000;

  private CountDownLatch latch;
  private CountDownLatch waitToLock;

  @Rule
  public Timeout timeout = new Timeout(30, TimeUnit.SECONDS);

  @Before
  public void setUp() throws Exception {
    latch = new CountDownLatch(1);
    waitToLock = new CountDownLatch(1);
  }

  @After
  public void tearDown() throws Exception {
    waitToLock.countDown();
    latch.countDown();
  }

  @Test
  public void testReaderWaitsForWriter() throws Exception {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    wl.lock();

    Thread writer = new Thread(() -> {
      waitToLock.countDown();
      rl.lock();
      latch.countDown();
      rl.unlock();
    });
    writer.start();

    assertTrue(waitToLock.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    assertEquals(1, latch.getCount());

    wl.unlock();
    assertTrue(latch.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWriterWaitsForReader() throws Exception {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    rl.lock();

    Thread writer = new Thread(() -> {
      waitToLock.countDown();
      wl.lock();
      latch.countDown();
      wl.unlock();
    });
    writer.start();

    assertTrue(waitToLock.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    assertEquals(1, latch.getCount());

    rl.unlock();
    assertTrue(latch.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReadersNotBlockedByReaders() throws Exception {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    rl.lock();

    Thread reader = new Thread(() -> {
      waitToLock.countDown();
      rl.lock();
      latch.countDown();
    });
    reader.start();

    assertTrue(waitToLock.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    assertTrue(latch.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWritersBlockedByWriters() throws Exception {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    wl.lock();

    Thread writer = new Thread(() -> {
      waitToLock.countDown();
      wl.lock();
      latch.countDown();
      wl.unlock();
    });
    writer.start();

    assertTrue(waitToLock.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    assertEquals(1, latch.getCount());

    wl.unlock();
    assertTrue(latch.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testTryLock() throws Exception {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    assertTrue(wl.tryLock());

    final AtomicBoolean failed = new AtomicBoolean(false);

    Thread reader = new Thread(() -> {
      waitToLock.countDown();
      if (rl.tryLock()) {
        failed.set(true);
      }
      latch.countDown();
    });
    reader.start();

    assertTrue(waitToLock.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    assertTrue(latch.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    assertFalse(failed.get());
  }

  @Test
  public void testLockAndReleaseByDifferentThreads() throws Exception {
    SemaphoreReadWriteLock rwl = new SemaphoreReadWriteLock();
    final Lock rl = rwl.readLock();
    final Lock wl = rwl.writeLock();
    rl.lock();

    Thread writer = new Thread(() -> {
      waitToLock.countDown();
      try {
        assertTrue(wl.tryLock(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
      latch.countDown();
    });
    writer.start();

    Thread reader2 = new Thread(() -> {
      try {
        assertTrue(waitToLock.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
      rl.unlock();
    });
    reader2.start();

    assertTrue(latch.await(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
  }
}
