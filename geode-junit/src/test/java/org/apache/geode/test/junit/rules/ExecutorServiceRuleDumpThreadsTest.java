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
package org.apache.geode.test.junit.rules;

import static java.lang.Integer.toHexString;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link ExecutorServiceRule#dumpThreads()}. If these tests become too brittle, then
 * just delete the tests with names starting with "shows".
 */
public class ExecutorServiceRuleDumpThreadsTest {

  private static final long TIMEOUT_MILLIS = getTimeout().getValueInMS();

  private final CountDownLatch terminateLatch = new CountDownLatch(1);

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @After
  public void tearDown() {
    terminateLatch.countDown();
  }

  @Test
  public void includesThreadName() throws InterruptedException {
    CountDownLatch threadRunning = new CountDownLatch(1);

    executorServiceRule.submit(() -> {
      threadRunning.countDown();
      terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
      return null;
    });

    threadRunning.await(TIMEOUT_MILLIS, MILLISECONDS);

    Set<Thread> threads = executorServiceRule.getThreads();
    assertThat(threads).hasSize(1);

    Iterator<Thread> threadIterator = threads.iterator();
    String threadName = threadIterator.next().getName();
    String dump = executorServiceRule.dumpThreads();

    assertThat(dump)
        .contains(threadName);
  }

  @Test
  public void includesThreadNamesForMultipleThreads() throws InterruptedException {
    CountDownLatch threadRunning = new CountDownLatch(2);

    for (int i = 0; i < 2; i++) {
      executorServiceRule.submit(() -> {
        threadRunning.countDown();
        terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
        return null;
      });
    }

    threadRunning.await(TIMEOUT_MILLIS, MILLISECONDS);

    Set<Thread> threads = executorServiceRule.getThreads();
    assertThat(threads).hasSize(2);

    Iterator<Thread> threadIterator = threads.iterator();
    String threadName1 = threadIterator.next().getName();
    String threadName2 = threadIterator.next().getName();
    String dump = executorServiceRule.dumpThreads();

    assertThat(dump)
        .contains(threadName1)
        .contains(threadName2);
  }

  @Test
  public void includesThreadId() throws InterruptedException {
    CountDownLatch threadRunning = new CountDownLatch(1);

    executorServiceRule.submit(() -> {
      threadRunning.countDown();
      terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
      return null;
    });

    threadRunning.await(TIMEOUT_MILLIS, MILLISECONDS);

    Set<Thread> threads = executorServiceRule.getThreads();
    assertThat(threads).hasSize(1);

    Iterator<Thread> threadIterator = threads.iterator();
    String threadId = String.valueOf(threadIterator.next().getId());
    String dump = executorServiceRule.dumpThreads();

    assertThat(dump)
        .contains(threadId);
  }

  @Test
  public void includesThreadIdsForMultipleThreads() throws InterruptedException {
    CountDownLatch threadRunning = new CountDownLatch(2);

    for (int i = 0; i < 2; i++) {
      executorServiceRule.submit(() -> {
        threadRunning.countDown();
        terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
        return null;
      });
    }

    threadRunning.await(TIMEOUT_MILLIS, MILLISECONDS);

    Set<Thread> threads = executorServiceRule.getThreads();
    assertThat(threads).hasSize(2);

    Iterator<Thread> threadIterator = threads.iterator();
    String threadId1 = String.valueOf(threadIterator.next().getId());
    String threadId2 = String.valueOf(threadIterator.next().getId());
    String dump = executorServiceRule.dumpThreads();

    assertThat(dump)
        .contains(threadId1)
        .contains(threadId2);
  }

  @Test
  public void includesLockedMonitors() throws InterruptedException {
    Object lock1 = new Object();
    Object lock2 = new Object();
    CountDownLatch lockedMonitors = new CountDownLatch(1);

    executorServiceRule.submit(() -> {
      synchronized (lock1) {
        synchronized (lock2) {
          lockedMonitors.countDown();
          terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
        }
      }
      return null;
    });

    lockedMonitors.await(TIMEOUT_MILLIS, MILLISECONDS);

    String dump = executorServiceRule.dumpThreads();

    assertThat(dump)
        .contains("-  locked " + lock1.getClass().getName() + "@" + toHexString(lock1.hashCode()))
        .contains("-  locked " + lock2.getClass().getName() + "@" + toHexString(lock2.hashCode()));
  }

  @Test
  public void includesLockedSynchronizers() throws InterruptedException {
    Lock sync = new ReentrantLock();
    CountDownLatch lockedSynchronizer = new CountDownLatch(1);

    executorServiceRule.submit(() -> {
      sync.lockInterruptibly();
      try {
        lockedSynchronizer.countDown();
        terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
      } finally {
        sync.unlock();
      }
      return null;
    });

    lockedSynchronizer.await(TIMEOUT_MILLIS, MILLISECONDS);

    String dump = executorServiceRule.dumpThreads();

    assertThat(dump)
        .contains("Number of locked synchronizers = 2")
        .contains(sync.getClass().getName() + "$NonfairSync@");
  }

  @Test
  public void showsThreadAwaitingLatch() {
    executorServiceRule.submit(() -> terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS));

    await().untilAsserted(() -> {
      String dump = executorServiceRule.dumpThreads();

      assertThat(dump)
          .contains("TIMED_WAITING on " + terminateLatch.getClass().getName() + "$Sync@")
          .contains("waiting on " + terminateLatch.getClass().getName() + "$Sync@");
    });
  }

  @Test
  public void showsThreadsInMonitorDeadlock() throws InterruptedException {
    Object lock1 = new Object();
    Object lock2 = new Object();
    CountDownLatch deadlockLatch = new CountDownLatch(1);
    CountDownLatch acquiredLock1Latch = new CountDownLatch(1);
    CountDownLatch acquiredLock2Latch = new CountDownLatch(1);

    executorServiceRule.submit(() -> {
      synchronized (lock1) {
        acquiredLock1Latch.countDown();
        deadlockLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
        synchronized (lock2) {
          System.out.println(Thread.currentThread().getName() + " acquired lock1 and lock2");
        }
      }
      return null;
    });

    executorServiceRule.submit(() -> {
      synchronized (lock2) {
        acquiredLock2Latch.countDown();
        deadlockLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
        synchronized (lock1) {
          System.out.println(Thread.currentThread().getName() + " acquired lock2 and lock1");
        }
      }
      return null;
    });

    acquiredLock1Latch.await(TIMEOUT_MILLIS, MILLISECONDS);
    acquiredLock2Latch.await(TIMEOUT_MILLIS, MILLISECONDS);

    deadlockLatch.countDown();

    await().untilAsserted(() -> {
      String dump = executorServiceRule.dumpThreads();

      String lock1Hash = toHexString(lock1.hashCode());
      String lock2Hash = toHexString(lock2.hashCode());

      assertThat(dump)
          .contains("BLOCKED on " + lock1.getClass().getName() + "@" + lock1Hash + " owned by ")
          .contains("BLOCKED on " + lock2.getClass().getName() + "@" + lock2Hash + " owned by ")

          .contains("-  blocked on " + lock1.getClass().getName() + "@" + lock1Hash)
          .contains("-  blocked on " + lock2.getClass().getName() + "@" + lock2Hash)

          .contains("-  locked " + lock1.getClass().getName() + "@" + lock1Hash)
          .contains("-  locked " + lock2.getClass().getName() + "@" + lock2Hash);
    });
  }

  @Test
  public void showsThreadsInSynchronizerDeadlock() throws InterruptedException {
    Lock sync1 = new ReentrantLock();
    Lock sync2 = new ReentrantLock();
    CountDownLatch deadlockLatch = new CountDownLatch(1);
    CountDownLatch acquiredSync1Latch = new CountDownLatch(1);
    CountDownLatch acquiredSync2Latch = new CountDownLatch(1);

    executorServiceRule.submit(() -> {
      sync1.lockInterruptibly();
      try {
        acquiredSync1Latch.countDown();
        deadlockLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
        sync2.lockInterruptibly();
        sync2.unlock();
      } finally {
        sync1.unlock();
      }
      return null;
    });

    executorServiceRule.submit(() -> {
      sync2.lockInterruptibly();
      try {
        acquiredSync2Latch.countDown();
        deadlockLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
        sync1.lockInterruptibly();
        sync1.unlock();
      } finally {
        sync2.unlock();
      }
      return null;
    });

    acquiredSync1Latch.await(TIMEOUT_MILLIS, MILLISECONDS);
    acquiredSync2Latch.await(TIMEOUT_MILLIS, MILLISECONDS);

    deadlockLatch.countDown();

    await().untilAsserted(() -> {
      String dump = executorServiceRule.dumpThreads();

      Set<Thread> threads = executorServiceRule.getThreads();
      assertThat(threads).hasSize(2);

      Iterator<Thread> threadIterator = threads.iterator();
      String[] threadNames = new String[2];
      for (int i = 0; i < threadNames.length; i++) {
        threadNames[i] = threadIterator.next().getName();
      }

      String syncType = ReentrantLock.class.getName() + "$NonfairSync@";

      assertThat(dump)
          .contains("WAITING on " + syncType)
          .contains("owned by \"" + threadNames[0] + "\"")
          .contains("owned by \"" + threadNames[1] + "\"")
          .contains("waiting on " + syncType)
          .contains("Number of locked synchronizers = 2")
          .contains("- " + syncType);
    });
  }

  @Test
  public void showsThreadBlockedInWait() {
    Object object = new Object();
    AtomicBoolean waiting = new AtomicBoolean(true);
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    executorServiceRule.submit(() -> {
      threadRef.set(Thread.currentThread());
      while (waiting.get()) {
        synchronized (object) {
          object.wait();
        }
      }
      return null;
    });

    await().untilAsserted(() -> {
      String dump = executorServiceRule.dumpThreads();

      waiting.set(false);
      synchronized (object) {
        object.notifyAll();
      }

      String objectHashCode = toHexString(object.hashCode());

      assertThat(dump)
          .contains("-  waiting on " + Object.class.getName() + "@" + objectHashCode);
    });
  }

  @Test
  public void showsThreadBlockedByOtherThread() {
    Object object = new Object();
    CountDownLatch pausingLatch = new CountDownLatch(1);
    AtomicBoolean waiting = new AtomicBoolean(true);

    executorServiceRule.submit(() -> {
      synchronized (object) {
        pausingLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
      }
      return null;
    });

    executorServiceRule.submit(() -> {
      while (waiting.get()) {
        synchronized (object) {
          object.wait();
        }
      }
      return null;
    });

    await().untilAsserted(() -> {
      String dump = executorServiceRule.dumpThreads();

      pausingLatch.countDown();
      synchronized (object) {
        object.notifyAll();
      }
      waiting.set(false);

      String objectHashCode = toHexString(object.hashCode());

      assertThat(dump)
          .contains("-  locked " + Object.class.getName() + "@" + objectHashCode)
          .contains("-  blocked on " + Object.class.getName() + "@" + objectHashCode);
    });
  }
}
