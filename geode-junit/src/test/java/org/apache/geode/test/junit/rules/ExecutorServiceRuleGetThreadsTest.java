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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.StringUtils.countMatches;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link ExecutorServiceRule#getThreads()}.
 */
public class ExecutorServiceRuleGetThreadsTest {

  private static final long TIMEOUT_MILLIS = getTimeout().getValueInMS();

  private final CountDownLatch terminateLatch = new CountDownLatch(1);
  private final AtomicBoolean submittedChildren = new AtomicBoolean();

  private volatile ExecutorService executorService;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @After
  public void tearDown() {
    terminateLatch.countDown();
  }

  @Test
  public void noThreads() {
    assertThat(executorServiceRule.getThreads()).isEmpty();
  }

  @Test
  public void oneThread() {
    executorServiceRule.submit(() -> terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS));

    assertThat(executorServiceRule.getThreads()).hasSize(1);
  }

  @Test
  public void twoThreads() {
    executorServiceRule.submit(() -> terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS));
    executorServiceRule.submit(() -> terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS));

    assertThat(executorServiceRule.getThreads()).hasSize(2);
  }

  @Test
  public void childExecutorService() {
    executorServiceRule.submit(() -> {
      executorService = Executors.newCachedThreadPool();
      executorService.submit(() -> terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS));
      executorService.submit(() -> terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS));
      submittedChildren.set(true);
    });

    await().untilTrue(submittedChildren);

    assertThat(executorServiceRule.getThreads().size()).isLessThanOrEqualTo(1);
  }

  @Test
  public void getThreadIds() {
    executorServiceRule.submit(() -> terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS));
    executorServiceRule.submit(() -> terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS));

    long[] threadIds = executorServiceRule.getThreadIds();
    assertThat(threadIds).hasSize(2);

    Set<Thread> threads = executorServiceRule.getThreads();
    for (Thread thread : threads) {
      assertThat(threadIds).contains(thread.getId());
    }
  }

  @Test
  public void dumpThreadsAwaitingLatch() {
    executorServiceRule.submit(() -> terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS));
    executorServiceRule.submit(() -> terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS));

    String dump = executorServiceRule.dumpThreads();
    System.out.println(dump);

    Set<Thread> threads = executorServiceRule.getThreads();
    for (Thread thread : threads) {
      assertThat(dump).contains(thread.getName());
    }
  }

  @Test
  public void dumpThreadsContainsExecutorServiceThread() throws InterruptedException {
    CountDownLatch threadRunning = new CountDownLatch(1);
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    executorServiceRule.submit(() -> {
      threadRef.set(Thread.currentThread());
      threadRunning.countDown();
      terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
      return null;
    });

    threadRunning.await(TIMEOUT_MILLIS, MILLISECONDS);

    String dump = executorServiceRule.dumpThreads();
    System.out.println(dump);

    Thread thread = threadRef.get();
    assertThat(dump).contains(thread.getName()).contains(String.format("0x%1$x", thread.getId()));
  }

  @Test
  public void dumpThreadsIncludesLockedMonitors() throws InterruptedException {
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
    System.out.println(dump);

    assertThat(dump)
        .contains("- locked <" + formatToHexString(lock1.hashCode()) + "> (a " +
            Object.class.getName() + ")")
        .contains("- locked <" + formatToHexString(lock2.hashCode()) + "> (a " +
            Object.class.getName() + ")");
  }

  @Test
  public void dumpThreadsIncludesLockedSynchronizers() throws InterruptedException {
    Lock sync = new ReentrantLock();
    CountDownLatch lockedSynchronizer = new CountDownLatch(1);

    executorServiceRule.submit(() -> {
      sync.lockInterruptibly();
      lockedSynchronizer.countDown();
      terminateLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
      return null;
    });

    lockedSynchronizer.await(TIMEOUT_MILLIS, MILLISECONDS);

    String dump = executorServiceRule.dumpThreads();
    System.out.println(dump);

    assertThat(dump)
        .contains("- locked synchronizer <")
        .contains("> (a " + ReentrantLock.class.getName() + "$NonfairSync)");
  }

  @Test
  public void dumpThreadsShowsDeadlockedOnMonitor() throws InterruptedException {
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

    String dump = executorServiceRule.dumpThreads();
    System.out.println(dump);

    Set<Thread> threads = executorServiceRule.getThreads();
    assertThat(threads).hasSize(2);

    for (Thread thread : threads) {
      assertThat(dump).contains(thread.getName());
    }

    assertThat(dump)
        .contains("- locked <" + formatToHexString(lock1.hashCode()) + "> (a " +
            Object.class.getName() + ")")
        .contains("- locked <" + formatToHexString(lock2.hashCode()) + "> (a " +
            Object.class.getName() + ")")
        .contains("- waiting to lock <" + formatToHexString(lock1.hashCode()) + "> (a " +
            Object.class.getName() + ")")
        .contains("- waiting to lock <" + formatToHexString(lock2.hashCode()) + "> (a " +
            Object.class.getName() + ")");
  }

  @Test
  public void dumpThreadsShowsDeadlockedOnSynchronizer() throws InterruptedException {
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
        try {
          // nothing
        } finally {
          sync2.unlock();
        }
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
        try {
          // nothing
        } finally {
          sync1.unlock();
        }
      } finally {
        sync2.unlock();
      }
      return null;
    });

    acquiredSync1Latch.await(TIMEOUT_MILLIS, MILLISECONDS);
    acquiredSync2Latch.await(TIMEOUT_MILLIS, MILLISECONDS);

    deadlockLatch.countDown();

    String dump = executorServiceRule.dumpThreads();
    System.out.println(dump);

    Set<Thread> threads = executorServiceRule.getThreads();
    assertThat(threads).hasSize(2);

    for (Thread thread : threads) {
      assertThat(dump).contains(thread.getName());
    }

    String syncType = "> (a " + ReentrantLock.class.getName() + "$NonfairSync)";

    assertThat(dump)
        .contains("- locked synchronizer <")
        .contains(syncType);

    assertThat(countMatches(dump, syncType))
        .as("Count matches of " + syncType + " in " + dump)
        .isEqualTo(4);
  }

  private String formatToHexString(int value) {
    return String.format("0x%1$x", value);
  }
}
