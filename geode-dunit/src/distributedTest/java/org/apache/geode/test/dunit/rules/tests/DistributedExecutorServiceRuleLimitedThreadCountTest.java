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
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.DistributedExecutorServiceRule;

@SuppressWarnings("serial")
public class DistributedExecutorServiceRuleLimitedThreadCountTest implements Serializable {

  private static final int PARALLEL_TASK_COUNT = 2;
  private static final int PARTY_COUNT = 3;
  private static final long TIMEOUT = getTimeout().toMinutes();
  private static final TimeUnit UNIT = TimeUnit.MINUTES;
  private static final AtomicBoolean CANCELED = new AtomicBoolean();
  private static final AtomicBoolean COMPLETED = new AtomicBoolean();
  private static final AtomicReference<CyclicBarrier> BARRIER = new AtomicReference<>();

  private static final AtomicReference<Collection<Future<Void>>> TASKS =
      new AtomicReference<>(new ArrayList<>());

  @Rule
  public DistributedExecutorServiceRule executorServiceRule = builder()
      .threadCount(PARALLEL_TASK_COUNT).vmCount(1).build();

  @Before
  public void setUp() {
    Stream.of(getController(), getVM(0)).forEach(vm -> vm.invoke(() -> {
      CANCELED.set(false);
      COMPLETED.set(false);
      BARRIER.set(new CyclicBarrier(PARTY_COUNT, () -> COMPLETED.set(true)));
      TASKS.get().clear();
    }));
  }

  @Test
  public void limitsThreadCount() {
    Stream.of(getController(), getVM(0)).forEach(vm -> vm.invoke(() -> {
      for (int i = 1; i <= PARALLEL_TASK_COUNT; i++) {
        TASKS.get().add(executorServiceRule.submit(() -> {
          try {
            BARRIER.get().await(TIMEOUT, UNIT);
          } catch (BrokenBarrierException e) {
            if (!CANCELED.get()) {
              throw e;
            }
          }
        }));
      }

      await().untilAsserted(
          () -> assertThat(BARRIER.get().getNumberWaiting()).isEqualTo(PARALLEL_TASK_COUNT));

      for (Future<Void> task : TASKS.get()) {
        assertThat(task).isNotDone();
      }

      assertThat(COMPLETED).isFalse();

      for (Future<Void> task : TASKS.get()) {
        CANCELED.set(true);
        task.cancel(true);
      }

      await().untilAsserted(() -> {
        for (Future<Void> task : TASKS.get()) {
          assertThat(task).isCancelled();
          assertThat(task).isDone();
        }
      });
    }));
  }
}
