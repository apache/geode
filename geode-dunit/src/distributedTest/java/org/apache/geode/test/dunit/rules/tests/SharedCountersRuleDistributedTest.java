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
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getVMCount;
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
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedCountersRule;

@SuppressWarnings("serial")
public class SharedCountersRuleDistributedTest implements Serializable {

  private static final int TWO_MINUTES_MILLIS = 2 * 60 * 1000;
  private static final String ID = "ID";

  private static ExecutorService executor;
  private static CompletableFuture<Void> combined;
  private static List<CompletableFuture<Boolean>> futures;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SharedCountersRule sharedCountersRule = new SharedCountersRule();

  @Test
  public void inc_withoutInit_throwsNullPointerException() {
    assertThatThrownBy(() -> sharedCountersRule.increment(ID))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void ref_withoutInit_returnsNull() {
    assertThat(sharedCountersRule.reference(ID)).isNull();
  }

  @Test
  public void init_get_returnsZero() {
    assertThat(sharedCountersRule.initialize(ID).reference(ID).get()).isEqualTo(0);
  }

  @Test
  public void initTwice_noop() {
    assertThat(sharedCountersRule.initialize(ID).reference(ID).get()).isEqualTo(0);
  }

  @Test
  public void inc_get_returnsOne() {
    assertThat(sharedCountersRule.initialize(ID).increment(ID).reference(ID).get()).isEqualTo(1);
  }

  @Test
  public void incDeltaTwo_get_returnsTwo() {
    assertThat(sharedCountersRule.initialize(ID).increment(ID, 2).reference(ID).get())
        .isEqualTo(2);
  }

  @Test
  public void incTwice_returnsTwo() {
    assertThat(
        sharedCountersRule.initialize(ID).increment(ID).increment(ID).reference(ID).get())
            .isEqualTo(2);
  }

  @Test
  public void inc_getTotal_returnsOne() {
    sharedCountersRule.initialize(ID).increment(ID);
    int total = sharedCountersRule.getTotal(ID);
    assertThat(total).isEqualTo(1);
  }

  @Test
  public void inc_fromDUnitVMs_getTotal_returnsFour() {
    sharedCountersRule.initialize(ID);
    for (VM vm : getAllVMs()) {
      vm.invoke(() -> {
        sharedCountersRule.increment(ID);
      });
    }
    assertThat(sharedCountersRule.getTotal(ID)).isEqualTo(getVMCount());
  }

  @Test
  public void inc_fromEveryVM_getTotal_returnsFive() {
    sharedCountersRule.initialize(ID).increment(ID);
    for (VM vm : getAllVMs()) {
      vm.invoke(() -> {
        sharedCountersRule.increment(ID);
      });
    }
    assertThat(sharedCountersRule.getTotal(ID)).isEqualTo(getVMCount() + 1);
  }

  @Test
  public void inc_multipleThreads_fromEveryVM_getTotal_returnsExpectedTotal() throws Exception {
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

    assertThat(sharedCountersRule.getTotal(ID)).isEqualTo(expectedIncrements);
  }

  private void givenSharedCounterFor(final Serializable id) {
    sharedCountersRule.initialize(id);
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

  private void submitIncrementTasks(int numThreads, final Serializable id) {
    for (int i = 0; i < numThreads; i++) {
      futures.add(supplyAsync(() -> {
        sharedCountersRule.increment(id);
        return true;
      }, executor));
    }
    combined = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  private static long calculateTimeoutMillis(final Stopwatch stopwatch) {
    return TWO_MINUTES_MILLIS - stopwatch.elapsed(MILLISECONDS);
  }
}
