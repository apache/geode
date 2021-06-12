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

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Stopwatch;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedCounters;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * Distributed tests for {@link DistributedCounters}.
 */
@SuppressWarnings("serial")
public class DistributedCountersDistributedTest implements Serializable {

  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();
  private static final String ID = "ID";

  private static ExecutorService executor;
  private static CompletableFuture<Void> combined;
  private static List<CompletableFuture<Boolean>> futures;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public DistributedCounters distributedCounters = new DistributedCounters();

  @Test
  public void incrementingCounter_beforeInitializingIt_throwsNullPointerException() {
    assertThatThrownBy(() -> distributedCounters.increment(ID))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void referencingCounter_beforeInitializingIt_returnsNull() {
    assertThat(distributedCounters.reference(ID)).isNull();
  }

  @Test
  public void gettingCounter_afterInitializingIt_returnsZero() {
    assertThat(distributedCounters.initialize(ID).reference(ID).get()).isEqualTo(0);
  }

  @Test
  public void initializingCounterMoreThanOnce_initializesItJustOnce() {
    assertThat(distributedCounters.initialize(ID).initialize(ID).reference(ID).get()).isEqualTo(0);
  }

  @Test
  public void gettingCounter_afterIncrementingIt_returnsOne() {
    assertThat(distributedCounters.initialize(ID).increment(ID).reference(ID).get()).isEqualTo(1);
  }

  @Test
  public void gettingCounter_afterIncrementingWithDeltaTwo_returnsTwo() {
    assertThat(distributedCounters.initialize(ID).increment(ID, 2).reference(ID).get())
        .isEqualTo(2);
  }

  @Test
  public void gettingCounter_afterIncrementingTwice_returnsTwo() {
    assertThat(
        distributedCounters.initialize(ID).increment(ID).increment(ID).reference(ID).get())
            .isEqualTo(2);
  }

  @Test
  public void getTotal_afterIncrementingOnce_returnsOne() {
    distributedCounters.initialize(ID).increment(ID);

    int total = distributedCounters.getTotal(ID);

    assertThat(total).isEqualTo(1);
  }

  @Test
  public void getTotal_afterIncrementingInEveryVm_returnsSameValueAsVmCount() {
    distributedCounters.initialize(ID);
    for (VM vm : getAllVMs()) {
      vm.invoke(() -> {
        distributedCounters.increment(ID);
      });
    }

    int total = distributedCounters.getTotal(ID);

    assertThat(total).isEqualTo(getVMCount());
  }

  @Test
  public void getTotal_afterIncrementingInEveryVmAndController_returnsSameValueAsVmCountPlusOne() {
    distributedCounters.initialize(ID).increment(ID);
    for (VM vm : getAllVMs()) {
      vm.invoke(() -> {
        distributedCounters.increment(ID);
      });
    }
    assertThat(distributedCounters.getTotal(ID)).isEqualTo(getVMCount() + 1);
  }

  @Test
  public void getTotal_afterIncrementingByLotsOfThreadsInEveryVm_returnsExpectedTotal()
      throws Exception {
    int numThreads = 10;
    givenExecutorInEveryVM(numThreads);
    givenSharedCounterFor(ID);

    // inc ID in numThreads in every VM (4 DUnit VMs + Controller VM)
    submitIncrementTasks(numThreads, ID);
    for (VM vm : getAllVMs()) {
      vm.invoke(() -> submitIncrementTasks(numThreads, ID));
    }

    // await CompletableFuture in every VM
    Stopwatch stopwatch = Stopwatch.createStarted();
    combined.get(calculateTimeoutMillis(stopwatch), MILLISECONDS);
    for (VM vm : getAllVMs()) {
      long timeoutMillis = calculateTimeoutMillis(stopwatch);
      vm.invoke(() -> combined.get(timeoutMillis, MILLISECONDS));
    }

    int dunitVMCount = getVMCount();
    int controllerPlusDUnitVMCount = dunitVMCount + 1;
    int expectedIncrements = controllerPlusDUnitVMCount * numThreads;

    assertThat(distributedCounters.getTotal(ID)).isEqualTo(expectedIncrements);
  }

  @Test
  public void newVmInitializesExistingCounterToZero() {
    distributedCounters.initialize(ID);

    VM newVM = getVM(getVMCount());

    assertThat(newVM.invoke(() -> distributedCounters.getLocal(ID))).isEqualTo(0);
  }

  @Test
  public void whenNewVmIncrementsCounter_totalIncludesThatValue() {
    distributedCounters.initialize(ID);
    VM newVM = getVM(getVMCount());

    newVM.invoke(() -> distributedCounters.increment(ID));

    assertThat(distributedCounters.getTotal(ID)).isEqualTo(1);
  }

  @Test
  public void whenBouncedVmIncrementsCounterBeforeBounce_totalIncludesThatValue() {
    distributedCounters.initialize(ID);

    getVM(0).invoke(() -> distributedCounters.increment(ID));
    getVM(0).bounce();

    assertThat(distributedCounters.getTotal(ID)).isEqualTo(1);
  }

  @Test
  public void whenBouncedVmIncrementsCounterAfterBounce_totalIncludesThatValue() {
    distributedCounters.initialize(ID);

    getVM(0).bounce();
    getVM(0).invoke(() -> distributedCounters.increment(ID));

    assertThat(distributedCounters.getTotal(ID)).isEqualTo(1);
  }

  @Test
  public void whenBouncedVmIncrementsCounterBeforeAndAfterBounce_totalIncludesThatValue() {
    distributedCounters.initialize(ID);

    getVM(0).invoke(() -> distributedCounters.increment(ID));
    getVM(0).bounce();
    getVM(0).invoke(() -> distributedCounters.increment(ID));

    assertThat(distributedCounters.getTotal(ID)).isEqualTo(2);
  }

  @Test
  public void createdByBuilderWithId_initializesCounterInEveryVm() {
    runTestWithValidation(CreatedByBuilderWithId.class);
  }

  private void givenSharedCounterFor(final Serializable id) {
    distributedCounters.initialize(id);
  }

  private void givenExecutorInEveryVM(final int numThreads) {
    executor = Executors.newFixedThreadPool(numThreads);
    futures = new ArrayList<>();

    for (VM vm : getAllVMs()) {
      vm.invoke(() -> {
        executor = Executors.newFixedThreadPool(numThreads);
        futures = new ArrayList<>();
      });
    }
  }

  private void submitIncrementTasks(final int numThreads, final Serializable id) {
    for (int i = 0; i < numThreads; i++) {
      futures.add(supplyAsync(() -> {
        distributedCounters.increment(id);
        return true;
      }, executor));
    }
    combined = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
  }

  private static long calculateTimeoutMillis(final Stopwatch stopwatch) {
    return TIMEOUT_MILLIS - stopwatch.elapsed(MILLISECONDS);
  }

  /**
   * Used by test {@link #createdByBuilderWithId_initializesCounterInEveryVm()}
   */
  public static class CreatedByBuilderWithId implements Serializable {

    @Rule
    public DistributedCounters distCounters = new DistributedCounters.Builder().withId(ID).build();

    @Test
    public void initializesCounterInEveryVm() {
      for (VM vm : VM.toArray(getAllVMs(), getController())) {
        vm.invoke(() -> {
          assertThat(distCounters.getLocal(ID)).isEqualTo(0);
        });
      }
    }
  }
}
