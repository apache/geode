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
package org.apache.geode.test.dunit.rules.tests;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.rules.DistributedExecutorServiceRule.builder;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.DistributedExecutorServiceRule;

@SuppressWarnings("serial")
public class DistributedExecutorServiceRuleLimitedThreadCountTest implements Serializable {

  private static final int THREAD_COUNT = 2;
  private static final long TIMEOUT = getTimeout().toMinutes();
  private static final TimeUnit UNIT = TimeUnit.MINUTES;
  private static final AtomicInteger STARTED_TASKS = new AtomicInteger();
  private static final AtomicInteger COMPLETED_TASKS = new AtomicInteger();
  private static final AtomicReference<CountDownLatch> LATCH = new AtomicReference<>();

  @Rule
  public DistributedExecutorServiceRule executorServiceRule = builder()
      .threadCount(THREAD_COUNT).vmCount(1).build();

  @Before
  public void setUp() {
    Stream.of(getController(), getVM(0)).forEach(vm -> vm.invoke(() -> {
      STARTED_TASKS.set(0);
      COMPLETED_TASKS.set(0);
      LATCH.set(new CountDownLatch(1));
    }));
  }

  @Test
  public void limitsRunningTasksToThreadCount() {
    // start THREAD_COUNT threads to use up the executor's thread pool
    Stream.of(getController(), getVM(0)).forEach(vm -> vm.invoke(() -> {
      for (int i = 1; i <= THREAD_COUNT; i++) {
        executorServiceRule.submit(() -> {
          // increment count of started tasks and use a LATCH to keep it running
          STARTED_TASKS.incrementAndGet();
          assertThat(LATCH.get().await(TIMEOUT, UNIT)).isTrue();
          COMPLETED_TASKS.incrementAndGet();
        });
      }

      // count of started tasks should be the same as THREAD_COUNT
      await().untilAsserted(() -> {
        assertThat(STARTED_TASKS.get()).isEqualTo(THREAD_COUNT);
        assertThat(COMPLETED_TASKS.get()).isZero();
      });

      // try to start one more task, but it should end up queued instead of started
      executorServiceRule.submit(() -> {
        STARTED_TASKS.incrementAndGet();
        assertThat(LATCH.get().await(TIMEOUT, UNIT)).isTrue();
        COMPLETED_TASKS.incrementAndGet();
      });

      // started tasks should still be the same as THREAD_COUNT
      assertThat(STARTED_TASKS.get()).isEqualTo(THREAD_COUNT);
      assertThat(COMPLETED_TASKS.get()).isZero();

      // number of threads running in executor should also be the same as THREAD_COUNT
      assertThat(executorServiceRule.getThreads()).hasSize(THREAD_COUNT);

      // open latch to let started tasks complete, and queued task should also start and finish
      LATCH.get().countDown();

      // all tasks should eventually complete as the executor threads finish tasks
      await().untilAsserted(() -> {
        assertThat(STARTED_TASKS.get()).isEqualTo(THREAD_COUNT + 1);
        assertThat(COMPLETED_TASKS.get()).isEqualTo(THREAD_COUNT + 1);
      });
    }));
  }
}
