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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.DistributedExecutorServiceRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class DistributedExecutorServiceRuleTest implements Serializable {

  private static final long TIMEOUT_MILLIS = GeodeAwaitility.getTimeout().toMillis();

  private static final AtomicReference<ExecutorService> executorService = new AtomicReference<>();
  private static final AtomicReference<CountDownLatch> latch = new AtomicReference<>();
  private static final AtomicReference<Future<Void>> voidFuture = new AtomicReference<>();
  private static final AtomicReference<Future<Boolean>> booleanFuture = new AtomicReference<>();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public DistributedExecutorServiceRule executorServiceRule = new DistributedExecutorServiceRule();

  @Test
  public void eachVmHasAnExecutorService() {
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        executorService.set(executorServiceRule.getExecutorService());
      });
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        assertThat(executorService.get()).isNotNull();
      });
    }
  }

  @Test
  public void eachVmAwaitsItsOwnVoidFuture() {
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        latch.set(new CountDownLatch(1));
      });
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> voidFuture.set(executorServiceRule.submit(() -> {
        latch.get().await(TIMEOUT_MILLIS, MILLISECONDS);
      })));
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        assertThat(voidFuture.get().isDone()).isFalse();
      });
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> voidFuture.set(executorServiceRule.submit(() -> {
        latch.get().countDown();
      })));
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> voidFuture.set(executorServiceRule.submit(() -> {
        assertThat(voidFuture.get().isDone()).isTrue();
      })));
    }
  }

  @Test
  public void eachVmAwaitsItsOwnBooleanFuture() {
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        latch.set(new CountDownLatch(1));
      });
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> booleanFuture.set(executorServiceRule.submit(() -> {
        return latch.get().await(TIMEOUT_MILLIS, MILLISECONDS);
      })));
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        assertThat(booleanFuture.get().isDone()).isFalse();
      });
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> voidFuture.set(executorServiceRule.submit(() -> {
        latch.get().countDown();
      })));
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> voidFuture.set(executorServiceRule.submit(() -> {
        assertThat(latch.get().getCount()).isZero();
      })));
    }
  }

  @Test
  public void eachVmCompletesIndependently() {
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        latch.set(new CountDownLatch(1));
      });
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> voidFuture.set(executorServiceRule.submit(() -> {
        latch.get().await(TIMEOUT_MILLIS, MILLISECONDS);
      })));
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        assertThat(voidFuture.get().isDone()).isFalse();
      });
    }

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> voidFuture.set(executorServiceRule.submit(() -> {
        latch.get().countDown();

        assertThat(voidFuture.get().isDone()).isTrue();
      })));

      for (int j = 0; j < getVMCount(); j++) {
        if (j == i) {
          continue;
        }

        getVM(i).invoke(() -> voidFuture.set(executorServiceRule.submit(() -> {
          assertThat(voidFuture.get().isDone()).isFalse();
        })));
      }
    }
  }
}
