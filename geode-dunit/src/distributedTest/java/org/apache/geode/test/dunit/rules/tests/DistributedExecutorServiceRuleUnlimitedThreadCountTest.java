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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
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
public class DistributedExecutorServiceRuleUnlimitedThreadCountTest implements Serializable {

  private static final int PARALLEL_TASK_COUNT = 4;
  private static final long TIMEOUT = getTimeout().toMinutes();
  private static final TimeUnit UNIT = TimeUnit.MINUTES;
  private static final AtomicBoolean COMPLETED = new AtomicBoolean();
  private static final AtomicReference<CyclicBarrier> BARRIER = new AtomicReference<>();

  @Rule
  public DistributedExecutorServiceRule executorServiceRule =
      new DistributedExecutorServiceRule(0, 1);

  @Before
  public void setUp() {
    Stream.of(getController(), getVM(0)).forEach(vm -> vm.invoke(() -> {
      COMPLETED.set(false);
      BARRIER.set(new CyclicBarrier(PARALLEL_TASK_COUNT, () -> COMPLETED.set(true)));
    }));
  }

  @Test
  public void doesNotLimitThreadCount() {
    Stream.of(getController(), getVM(0)).forEach(vm -> vm.invoke(() -> {
      Collection<Future<Void>> tasks = new ArrayList<>();
      for (int i = 1; i <= PARALLEL_TASK_COUNT; i++) {
        tasks.add(executorServiceRule.submit(() -> {
          BARRIER.get().await(TIMEOUT, UNIT);
        }));
      }
      await().untilAsserted(() -> assertThat(COMPLETED.get()).isTrue());
      for (Future<Void> task : tasks) {
        assertThat(task).isDone();
      }
    }));
  }
}
