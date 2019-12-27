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
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link ExecutorServiceRule#getThreads()}.
 */
public class ExecutorServiceRuleGetThreadsTest {

  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();

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
}
