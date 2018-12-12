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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import org.apache.geode.test.junit.runners.TestRunner;

/**
 * Unit tests for {@link ExecutorServiceRule}.
 */
public class ExecutorServiceRuleTest {

  private static volatile CountDownLatch hangLatch;
  private static volatile CountDownLatch terminateLatch;
  private static volatile ExecutorService executorService;

  @Before
  public void setUp() throws Exception {
    hangLatch = new CountDownLatch(1);
    terminateLatch = new CountDownLatch(1);
  }

  @After
  public void tearDown() throws Exception {
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
  public void providesExecutorService() {
    Result result = TestRunner.runTest(HasExecutorService.class);
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(executorService).isInstanceOf(ExecutorService.class);
  }

  @Test
  public void shutsDownAfterTest() {
    Result result = TestRunner.runTest(HasExecutorService.class);
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(executorService.isShutdown()).isTrue();
  }

  @Test
  public void terminatesAfterTest() {
    Result result = TestRunner.runTest(HasExecutorService.class);
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(executorService.isTerminated()).isTrue();
  }

  @Test
  public void shutsDownHungThread() {
    Result result = TestRunner.runTest(Hangs.class);
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(isTestHung()).isTrue();
    assertThat(executorService.isShutdown()).isTrue();
    awaitLatch(terminateLatch);
  }

  @Test
  public void terminatesHungThread() {
    Result result = TestRunner.runTest(Hangs.class);
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(isTestHung()).isTrue();
    await().untilAsserted(() -> assertThat(executorService.isTerminated()).isTrue());
    awaitLatch(terminateLatch);
  }

  @Test
  public void futureTimesOut() {
    Result result = TestRunner.runTest(TimesOut.class);
    assertThat(result.wasSuccessful()).isFalse();
    assertThat(result.getFailures()).hasSize(1);
    Failure failure = result.getFailures().get(0);
    assertThat(failure.getException()).isInstanceOf(TimeoutException.class);
  }

  private static void awaitLatch(CountDownLatch latch) {
    await().untilAsserted(() -> assertThat(latch.getCount())
        .as("Latch failed to countDown within timeout").isZero());
  }

  private static boolean isTestHung() {
    return hangLatch.getCount() > 0;
  }

  public abstract static class HasExecutorServiceRule {

    @Rule
    public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

    @Before
    public void setUpHasAsynchronousRule() {
      executorService = executorServiceRule.getExecutorService();
    }
  }

  public static class HasExecutorService extends HasExecutorServiceRule {

    @Test
    public void doTest() throws Exception {
      // nothing
    }
  }

  public static class Hangs extends HasExecutorServiceRule {

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

  public static class TimesOut extends HasExecutorServiceRule {

    @Test
    public void doTest() throws Exception {
      Future<Void> future = executorServiceRule.runAsync(() -> {
        try {
          hangLatch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          terminateLatch.countDown();
        }
      });

      // this is expected to timeout
      future.get(1, MILLISECONDS);
    }
  }
}
