/*
 * Copyright (C) 2000-2012 Heinz Max Kabutz
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. Heinz Max Kabutz licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.redis.internal.services;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;


/**
 * @author Heinz Kabutz
 */
/*
 * Changes from the original:
 * - Switch to AssertJ for all assertions
 *
 */
public class StripedExecutorServiceJUnitTest {
  @Before
  public void initialize() {
    TestRunnable.outOfSequence = false;
    TestUnstripedRunnable.outOfSequence = false;
    TestFastRunnable.outOfSequence = false;
  }

  @Test
  public void testSingleStripeRunnable() throws InterruptedException {
    ExecutorService pool = new StripedExecutorService();
    Object stripe = new Object();
    AtomicInteger actual = new AtomicInteger(0);
    for (int i = 0; i < 100; i++) {
      pool.submit(new TestRunnable(stripe, actual, i));
    }
    assertThat(pool.isTerminated()).isFalse();
    assertThat(pool.isShutdown()).isFalse();

    pool.shutdown();

    assertThat(pool.awaitTermination(1, TimeUnit.HOURS)).isTrue();
    assertThat(TestRunnable.outOfSequence)
        .as("Expected no out-of-sequence runnables to execute")
        .isFalse();
    assertThat(pool.isTerminated()).isTrue();
  }

  @Test
  public void testShutdown() throws InterruptedException {
    ThreadGroup group = new ThreadGroup("stripetestgroup");
    Thread starter = new Thread(group, "starter") {
      public void run() {
        ExecutorService pool = new StripedExecutorService();
        Object stripe = new Object();
        AtomicInteger actual = new AtomicInteger(0);
        for (int i = 0; i < 100; i++) {
          pool.submit(new TestRunnable(stripe, actual, i));
        }
        pool.shutdown();
      }
    };
    starter.start();
    starter.join();

    await().untilAsserted(() -> assertThat(group.activeCount()).isEqualTo(0));
  }

  @Test
  public void testShutdownNow() throws InterruptedException {
    int totalRunnables = 100;
    ExecutorService pool = new StripedExecutorService();
    Object stripe = new Object();
    AtomicInteger actual = new AtomicInteger(0);
    for (int i = 0; i < totalRunnables; i++) {
      pool.submit(new TestRunnable(stripe, actual, i));
    }

    await().during(Duration.ofMillis(500)).until(() -> !pool.isTerminated());
    Collection<Runnable> unfinishedJobs = pool.shutdownNow();

    assertThat(pool.isShutdown()).isTrue();
    assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
    assertThat(pool.isTerminated()).isTrue();

    assertThat(unfinishedJobs.size() > 0).isTrue();

    assertThat(unfinishedJobs.size() + actual.intValue()).isEqualTo(totalRunnables);
  }

  @Test
  public void testSingleStripeCallableWithCompletionService()
      throws InterruptedException, ExecutionException {
    ExecutorService pool = new StripedExecutorService();
    final CompletionService<Integer> cs = new ExecutorCompletionService<>(pool);

    Thread testSubmitter = new Thread("TestSubmitter") {
      public void run() {
        Object stripe = new Object();
        for (int i = 0; i < 50; i++) {
          cs.submit(new TestCallable(stripe, i));
        }
        for (int i = 50; i < 100; i++) {
          cs.submit(new TestCallable(stripe, i));
        }
      }
    };
    testSubmitter.start();

    for (int i = 0; i < 100; i++) {
      int actual = cs.take().get();
      assertThat(actual)
          .as("unexpected thread completion order")
          .isEqualTo(i);
    }
    pool.shutdown();

    assertThat(pool.awaitTermination(1, TimeUnit.HOURS)).isTrue();

    testSubmitter.join();
  }

  @Test
  public void testUnstripedRunnable() throws InterruptedException {
    ExecutorService pool = new StripedExecutorService();
    AtomicInteger actual = new AtomicInteger(0);
    for (int i = 0; i < 100; i++) {
      pool.submit(new TestUnstripedRunnable(actual, i));
    }
    pool.shutdown();
    assertThat(pool.awaitTermination(1, TimeUnit.HOURS)).isTrue();

    assertThat(TestUnstripedRunnable.outOfSequence)
        .as("Expected at least some out-of-sequence runnables to execute")
        .isTrue();
  }

  @Test
  public void testMultipleStripes() throws InterruptedException {
    final ExecutorService pool = new StripedExecutorService();
    ExecutorService producerPool = Executors.newCachedThreadPool();
    for (int i = 0; i < 20; i++) {
      producerPool.submit(() -> {
        Object stripe = new Object();
        AtomicInteger actual = new AtomicInteger(0);
        for (int i1 = 0; i1 < 100; i1++) {
          pool.submit(new TestRunnable(stripe, actual, i1));
        }
      });
    }
    producerPool.shutdown();

    await().until(() -> producerPool.awaitTermination(100, TimeUnit.MILLISECONDS));

    pool.shutdown();

    assertThat(pool.awaitTermination(1, TimeUnit.DAYS)).isTrue();
    assertThat(TestRunnable.outOfSequence)
        .as("Expected no out-of-sequence runnables to execute")
        .isFalse();
  }

  @Test
  public void testMultipleFastStripes() throws InterruptedException {
    final ExecutorService pool = new StripedExecutorService();
    ExecutorService producerPool = Executors.newCachedThreadPool();
    for (int i = 0; i < 20; i++) {
      producerPool.submit(() -> {
        Object stripe = new Object();
        AtomicInteger actual = new AtomicInteger(0);
        for (int i1 = 0; i1 < 100; i1++) {
          pool.submit(new TestFastRunnable(stripe, actual, i1));
        }
      });
    }
    producerPool.shutdown();

    await().until(() -> producerPool.awaitTermination(100, TimeUnit.MILLISECONDS));

    pool.shutdown();
    assertThat(pool.awaitTermination(1, TimeUnit.DAYS)).isTrue();
    assertThat(TestFastRunnable.outOfSequence)
        .as("Expected no out-of-sequence runnables to execute")
        .isFalse();
  }


  public static class TestRunnable implements StripedRunnable {
    private final Object stripe;
    private final AtomicInteger stripeSequence;
    private final int expected;
    private static volatile boolean outOfSequence = false;

    public TestRunnable(Object stripe, AtomicInteger stripeSequence, int expected) {
      this.stripe = stripe;
      this.stripeSequence = stripeSequence;
      this.expected = expected;
    }

    public Object getStripe() {
      return stripe;
    }

    public void run() {
      try {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        Thread.sleep(rand.nextInt(10) + 10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      int actual = stripeSequence.getAndIncrement();
      if (actual != expected) {
        outOfSequence = true;
      }
      System.out.printf("Execute stripe %h %d %d%n", stripe, actual, expected);
      assertThat(actual)
          .as("out of sequence")
          .isEqualTo(expected);
    }
  }

  public static class TestFastRunnable implements StripedRunnable {
    private final Object stripe;
    private final AtomicInteger stripeSequence;
    private final int expected;
    private static volatile boolean outOfSequence = false;

    public TestFastRunnable(Object stripe, AtomicInteger stripeSequence, int expected) {
      this.stripe = stripe;
      this.stripeSequence = stripeSequence;
      this.expected = expected;
    }

    public Object getStripe() {
      return stripe;
    }

    public void run() {
      int actual = stripeSequence.getAndIncrement();
      if (actual != expected) {
        outOfSequence = true;
      }
      System.out.printf("Execute stripe %h %d %d%n", stripe, actual, expected);
      assertThat(actual)
          .as("out of sequence")
          .isEqualTo(expected);
    }
  }

  public static class TestCallable implements StripedCallable<Integer> {
    private final Object stripe;
    private final int expected;

    public TestCallable(Object stripe, int expected) {
      this.stripe = stripe;
      this.expected = expected;
    }

    public Object getStripe() {
      return stripe;
    }

    public Integer call() throws Exception {
      try {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        Thread.sleep(rand.nextInt(10) + 10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return expected;
    }
  }

  public static class TestUnstripedRunnable implements Runnable {
    private final AtomicInteger stripeSequence;
    private final int expected;
    private static volatile boolean outOfSequence = false;

    public TestUnstripedRunnable(AtomicInteger stripeSequence, int expected) {
      this.stripeSequence = stripeSequence;
      this.expected = expected;
    }

    public void run() {
      try {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        Thread.sleep(rand.nextInt(10) + 10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      int actual = stripeSequence.getAndIncrement();
      if (actual != expected) {
        outOfSequence = true;
      }
      System.out.println("Execute unstriped " + actual + ", " + expected);
    }
  }

}
