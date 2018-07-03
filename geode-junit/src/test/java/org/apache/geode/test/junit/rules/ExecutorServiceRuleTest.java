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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.runners.TestRunner;

public class ExecutorServiceRuleTest {

  static volatile AtomicIntegerWithMaxValueSeen concurrentTasks;
  static volatile CountDownLatch hangLatch;
  static volatile CountDownLatch terminateLatch;
  static volatile ExecutorService executorService;

  @Before
  public void setUp() throws Exception {
    concurrentTasks = new AtomicIntegerWithMaxValueSeen(0);
    hangLatch = new CountDownLatch(1);
    terminateLatch = new CountDownLatch(1);
  }

  @After
  public void tearDown() throws Exception {
    concurrentTasks = null;

    while (hangLatch != null && hangLatch.getCount() > 0) {
      hangLatch.countDown();;
    }

    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }

    hangLatch = null;
    terminateLatch = null;
  }

  @Test
  public void providesExecutorService() throws Exception {
    Result result = TestRunner.runTest(HasExecutorService.class);
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(executorService).isInstanceOf(ExecutorService.class);
  }

  @Test
  public void shutsDownAfterTest() throws Exception {
    Result result = TestRunner.runTest(HasExecutorService.class);
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(executorService.isShutdown()).isTrue();
  }

  @Test
  public void terminatesAfterTest() throws Exception {
    Result result = TestRunner.runTest(HasExecutorService.class);
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(executorService.isTerminated()).isTrue();
  }

  @Test
  public void shutsDownHungThread() throws Exception {
    Result result = TestRunner.runTest(Hangs.class);
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(isTestHung()).isTrue();
    assertThat(executorService.isShutdown()).isTrue();
    terminateLatch.await(10, SECONDS);
  }

  @Test
  public void terminatesHungThread() throws Exception {
    Result result = TestRunner.runTest(Hangs.class);
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(isTestHung()).isTrue();
    await().atMost(10, SECONDS).until(() -> assertThat(executorService.isTerminated()).isTrue());
    terminateLatch.await(1, SECONDS);
  }

  @Test
  public void futureTimesOut() throws Exception {
    Result result = TestRunner.runTest(TimesOut.class);
    assertThat(result.wasSuccessful()).isFalse();
    assertThat(result.getFailures()).hasSize(1);
    Failure failure = result.getFailures().get(0);
    assertThat(failure.getException()).isInstanceOf(TimeoutException.class);
  }

  @Test
  public void singleThreadedByDefault() throws Exception {
    terminateLatch = new CountDownLatch(2);
    Result result = TestRunner.runTest(SingleThreaded.class);
    assertThat(result.wasSuccessful()).as(result.toString()).isTrue();
    assertThat(concurrentTasks.getMaxValueSeen()).isEqualTo(1);
  }

  @Test
  public void threadCountTwoHasTwoThreads() throws Exception {
    terminateLatch = new CountDownLatch(3);
    Result result = TestRunner.runTest(ThreadCountTwo.class);
    assertThat(result.wasSuccessful()).as(result.toString()).isTrue();
    assertThat(concurrentTasks.getMaxValueSeen()).isEqualTo(2);
  }

  private static boolean isTestHung() {
    return hangLatch.getCount() > 0;
  }

  public abstract static class HasAsynchronousRule {

    @Rule
    public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

    @Before
    public void setUpHasAsynchronousRule() throws Exception {
      executorService = executorServiceRule.getExecutorService();
    }
  }

  public static class HasExecutorService extends HasAsynchronousRule {

    @Test
    public void doTest() throws Exception {
      // nothing
    }
  }

  public static class Hangs extends HasAsynchronousRule {

    @Test
    public void doTest() throws Exception {
      executorServiceRule.runAsync(() -> {
        try {
          hangLatch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          terminateLatch.countDown();
        }
      });
    }
  }

  public static class TimesOut extends HasAsynchronousRule {

    @Test
    public void doTest() throws Exception {
      Future<?> future = executorServiceRule.runAsync(() -> {
        try {
          hangLatch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          terminateLatch.countDown();
        }
      });

      future.get(1, MILLISECONDS);
    }
  }

  public static class SingleThreaded extends HasAsynchronousRule {

    private volatile CountDownLatch task1Latch;
    private volatile CountDownLatch task2Latch;

    private volatile CountDownLatch hang1Latch;
    private volatile CountDownLatch hang2Latch;

    @Before
    public void setUp() throws Exception {
      task1Latch = new CountDownLatch(1);
      task2Latch = new CountDownLatch(1);

      hang1Latch = new CountDownLatch(1);
      hang2Latch = new CountDownLatch(1);

      assertThat(terminateLatch.getCount()).isEqualTo(2);
    }

    @Test
    public void doTest() throws Exception {
      Future<Void> task1 = executorServiceRule.runAsync(() -> {
        try {
          task1Latch.countDown();
          concurrentTasks.increment();
          hang1Latch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          concurrentTasks.decrement();
          terminateLatch.countDown();
        }
      });

      // assert that task1 begins
      task1Latch.await(30, SECONDS);

      Future<Void> task2 = executorServiceRule.runAsync(() -> {
        try {
          task2Latch.countDown();
          concurrentTasks.increment();
          hang2Latch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          concurrentTasks.decrement();
          terminateLatch.countDown();
        }
      });

      // assert that there is only 1 thread in the default rule's ExecutorService
      assertThat(task1Latch.getCount()).isEqualTo(0);
      assertThat(task2Latch.getCount()).isEqualTo(1);

      // assert that task1 completes
      hang1Latch.countDown();
      task1.get(30, SECONDS);
      assertThat(terminateLatch.getCount()).isEqualTo(1);

      // assert that task2 begins
      task2Latch.await(30, SECONDS);

      // assert that task2 completes
      hang2Latch.countDown();
      task2.get(30, SECONDS);
      assertThat(terminateLatch.getCount()).isEqualTo(0);
    }
  }

  public static class ThreadCountTwo {

    private volatile CountDownLatch task1Latch;
    private volatile CountDownLatch task2Latch;
    private volatile CountDownLatch task3Latch;

    private volatile CountDownLatch hang1Latch;
    private volatile CountDownLatch hang2Latch;
    private volatile CountDownLatch hang3Latch;

    @Rule
    public ExecutorServiceRule executorServiceRule =
        ExecutorServiceRule.builder().threadCount(2).build();

    @Before
    public void setUp() throws Exception {
      task1Latch = new CountDownLatch(1);
      task2Latch = new CountDownLatch(1);
      task3Latch = new CountDownLatch(1);

      hang1Latch = new CountDownLatch(1);
      hang2Latch = new CountDownLatch(1);
      hang3Latch = new CountDownLatch(1);

      assertThat(terminateLatch.getCount()).isEqualTo(3);
    }

    @Test
    public void doTest() throws Exception {
      Future<Void> task1 = executorServiceRule.runAsync(() -> {
        try {
          task1Latch.countDown();
          concurrentTasks.increment();
          hang1Latch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          concurrentTasks.decrement();
          terminateLatch.countDown();
        }
      });

      // assert that task1 begins
      task1Latch.await(30, SECONDS);

      Future<Void> task2 = executorServiceRule.runAsync(() -> {
        try {
          task2Latch.countDown();
          concurrentTasks.increment();
          hang2Latch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          concurrentTasks.decrement();
          terminateLatch.countDown();
        }
      });

      // assert that task2 begins
      task2Latch.await(30, SECONDS);

      Future<Void> task3 = executorServiceRule.runAsync(() -> {
        try {
          task3Latch.countDown();
          concurrentTasks.increment();
          hang3Latch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          concurrentTasks.decrement();
          terminateLatch.countDown();
        }
      });

      // assert that there are 2 threads in the rule's ExecutorService
      assertThat(task1Latch.getCount()).isEqualTo(0);
      assertThat(task2Latch.getCount()).isEqualTo(0);
      assertThat(task3Latch.getCount()).isEqualTo(1);

      // assert that task1 completes
      hang1Latch.countDown();
      task1.get(30, SECONDS);
      assertThat(terminateLatch.getCount()).isEqualTo(2);

      // assert that task3 begins
      task3Latch.await(30, SECONDS);

      // assert that task2 completes
      hang2Latch.countDown();
      task2.get(30, SECONDS);
      assertThat(terminateLatch.getCount()).isEqualTo(1);

      // assert that task3 completes
      hang3Latch.countDown();
      task3.get(30, SECONDS);
      assertThat(terminateLatch.getCount()).isEqualTo(0);
    }
  }

  static class AtomicIntegerWithMaxValueSeen extends AtomicInteger {

    private int maxValueSeen = 0;

    AtomicIntegerWithMaxValueSeen(int initialValue) {
      super(initialValue);
    }

    void increment() {
      maxValueSeen = Integer.max(maxValueSeen, super.incrementAndGet());
    }

    void decrement() {
      super.decrementAndGet();
    }

    int getMaxValueSeen() {
      return maxValueSeen;
    }
  }
}
