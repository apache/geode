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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Result;
import org.mockito.InOrder;

import org.apache.geode.test.junit.runners.TestRunner;

public class ExecutorServiceRuleIntegrationTest {

  private static volatile CountDownLatch hangLatch;
  private static volatile CountDownLatch terminateLatch;
  private static volatile ExecutorService executorService;
  private static Awaits.Invocations invocations;

  @Before
  public void setUp() throws Exception {
    hangLatch = new CountDownLatch(1);
    terminateLatch = new CountDownLatch(1);
    invocations = mock(Awaits.Invocations.class);
  }

  @After
  public void tearDown() throws Exception {
    invocations = null;

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
  public void awaitTermination() {
    Result result = TestRunner.runTest(Awaits.class);
    assertThat(result.wasSuccessful()).isTrue();

    assertThat(isTestHung()).isTrue();
    await()
        .untilAsserted(() -> assertThat(executorService.isTerminated()).isTrue());
    invocations.afterRule();

    InOrder invocationOrder = inOrder(invocations);
    invocationOrder.verify(invocations).afterTest();
    invocationOrder.verify(invocations).afterHangLatch();
    invocationOrder.verify(invocations).afterTerminateLatch();
    invocationOrder.verify(invocations).afterRule();
    invocationOrder.verifyNoMoreInteractions();
  }

  private static boolean isTestHung() {
    return hangLatch.getCount() > 0;
  }

  public static class Awaits {

    @Rule
    public ExecutorServiceRule executorServiceRule =
        ExecutorServiceRule.builder().awaitTermination(2, SECONDS).build();

    @Before
    public void setUp() throws Exception {
      executorService = executorServiceRule.getExecutorService();
    }

    @After
    public void after() throws Exception {
      invocations.afterTest();
    }

    @Test
    public void doTest() throws Exception {
      executorServiceRule.runAsync(() -> {
        try {
          hangLatch.await(1, SECONDS);
          invocations.afterHangLatch();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          terminateLatch.countDown();
          invocations.afterTerminateLatch();
        }
      });
    }

    interface Invocations {
      void afterHangLatch();

      void afterTerminateLatch();

      void afterRule();

      void afterTest();
    }
  }
}
