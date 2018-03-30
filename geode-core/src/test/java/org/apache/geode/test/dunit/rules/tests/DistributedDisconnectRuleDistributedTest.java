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

import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.rules.DistributedDisconnectRule.disconnect;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.Result;
import org.junit.runners.MethodSorters;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedDisconnectRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.TestRunner;

@Category(DistributedTest.class)
public class DistributedDisconnectRuleDistributedTest {

  private static InternalCache cache;

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @After
  public void tearDown() throws Exception {
    disconnect();
    cache = null;

    for (VM vm : getAllVMs()) {
      vm.invoke(() -> {
        disconnect();
        cache = null;
      });
    }
  }

  @Test
  public void disconnectShouldDisconnect() throws Exception {
    cache = (InternalCache) new CacheFactory().create();
    assertThat(cache.isClosed()).isFalse();
    assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();

    disconnect();

    assertThat(cache.isClosed()).isTrue();
    assertThat(cache.getInternalDistributedSystem().isConnected()).isFalse();
  }

  @Test
  public void disconnectBeforeShouldDisconnectBeforeEachTest() throws Exception {
    createCacheInEveryVM();
    assertThatConnectedInEveryVM();

    Result result = TestRunner.runTest(DisconnectBefore.class);

    assertThat(result.wasSuccessful()).isTrue();
    assertThatConnectedInEveryVM();
  }

  @Test
  public void disconnectAfterShouldDisconnectAfterEachTest() throws Exception {
    createCacheInEveryVM();
    assertThatConnectedInEveryVM();

    Result result = TestRunner.runTest(DisconnectAfter.class);

    assertThat(result.wasSuccessful()).isTrue();
    assertThatDisconnectedInEveryVM();
  }

  static void createCacheInEveryVM() throws Exception {
    cache = (InternalCache) new CacheFactory().create();

    for (VM vm : getAllVMs()) {
      vm.invoke(() -> {
        cache = (InternalCache) new CacheFactory().create();
      });
    }
  }

  static void assertThatConnectedInEveryVM() throws Exception {
    assertThat(cache.isClosed()).isFalse();
    assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();

    for (VM vm : getAllVMs()) {
      vm.invoke(() -> {
        assertThat(cache.isClosed()).isFalse();
        assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();
      });
    }
  }

  static void assertThatDisconnectedInEveryVM() throws Exception {
    assertThat(cache.getInternalDistributedSystem().isConnected()).isFalse();
    assertThat(cache.isClosed()).isTrue();

    for (VM vm : getAllVMs()) {
      vm.invoke(() -> {
        assertThat(cache.getInternalDistributedSystem().isConnected()).isFalse();
        assertThat(cache.isClosed()).isTrue();
      });
    }
  }

  /**
   * Used by test {@link #disconnectBeforeShouldDisconnectBeforeEachTest()}
   */
  @FixMethodOrder(MethodSorters.NAME_ASCENDING)
  public static class DisconnectBefore {

    @ClassRule
    public static DistributedTestRule distributedTestRule = new DistributedTestRule();

    @Rule
    public DistributedDisconnectRule distributedDisconnectRule =
        new DistributedDisconnectRule.Builder().disconnectBefore(true).build();

    @Test
    public void test001CreateCacheInEveryVM() throws Exception {
      assertThatDisconnectedInEveryVM();
      createCacheInEveryVM();
      assertThatConnectedInEveryVM();
    }

    @Test
    public void test002EveryVMShouldHaveDisconnected() throws Exception {
      assertThatDisconnectedInEveryVM();
      createCacheInEveryVM();
      assertThatConnectedInEveryVM();
    }
  }

  /**
   * Used by test {@link #disconnectAfterShouldDisconnectAfterEachTest()}
   */
  @FixMethodOrder(MethodSorters.NAME_ASCENDING)
  public static class DisconnectAfter {

    @ClassRule
    public static DistributedTestRule distributedTestRule = new DistributedTestRule();

    @Rule
    public DistributedDisconnectRule distributedDisconnectRule =
        new DistributedDisconnectRule.Builder().disconnectAfter(true).build();

    @Test
    public void test001AlreadyHasCacheInEveryVM() throws Exception {
      assertThatConnectedInEveryVM();
    }

    @Test
    public void test002CreateCacheInEveryVM() throws Exception {
      assertThatDisconnectedInEveryVM();
      createCacheInEveryVM();
      assertThatConnectedInEveryVM();
    }

    @Test
    public void test003EveryVMShouldHaveDisconnected() throws Exception {
      assertThatDisconnectedInEveryVM();
      createCacheInEveryVM();
      assertThatConnectedInEveryVM();
    }
  }
}
