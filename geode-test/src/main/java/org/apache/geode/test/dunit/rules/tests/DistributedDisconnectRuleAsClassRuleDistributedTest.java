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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.Result;
import org.junit.runners.MethodSorters;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedDisconnectRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.RuleList;
import org.apache.geode.test.junit.runners.TestRunner;

@Category(DistributedTest.class)
public class DistributedDisconnectRuleAsClassRuleDistributedTest {

  @After
  public void tearDown() throws Exception {
    disconnect();
    DisconnectBefore.cache = null;
    DisconnectAfter.cache = null;

    for (VM vm : Host.getHost(0).getAllVMs()) {
      vm.invoke(() -> {
        disconnect();
        DisconnectBefore.cache = null;
        DisconnectAfter.cache = null;
      });
    }
  }

  @Test
  public void disconnectBeforeShouldDisconnectAllBeforeTests() throws Exception {
    Result result = TestRunner.runTest(DisconnectBefore.class);

    assertThat(result.wasSuccessful()).isTrue();

    assertThat(DisconnectBefore.cache.getInternalDistributedSystem().isConnected()).isTrue();
    assertThat(DisconnectBefore.cache.isClosed()).isFalse();

    for (VM vm : Host.getHost(0).getAllVMs()) {
      vm.invoke(() -> {
        assertThat(DisconnectBefore.cache.getInternalDistributedSystem().isConnected()).isTrue();
        assertThat(DisconnectBefore.cache.isClosed()).isFalse();
      });
    }
  }

  @Test
  public void disconnectAfterShouldDisconnectAllAfterTests() throws Exception {
    Result result = TestRunner.runTest(DisconnectAfter.class);

    assertThat(result.wasSuccessful()).isTrue();

    assertThat(DisconnectAfter.cache.getInternalDistributedSystem().isConnected()).isFalse();
    assertThat(DisconnectAfter.cache.isClosed()).isTrue();

    for (VM vm : Host.getHost(0).getAllVMs()) {
      vm.invoke(() -> {
        assertThat(DisconnectAfter.cache.getInternalDistributedSystem().isConnected()).isFalse();
        assertThat(DisconnectAfter.cache.isClosed()).isTrue();
      });
    }
  }

  /**
   * Used by test {@link #disconnectBeforeShouldDisconnectAllBeforeTests()}
   */
  public static class DisconnectBefore {

    @ClassRule
    public static RuleList rules = new RuleList().add(new DistributedTestRule())
        .add(new DistributedDisconnectRule.Builder().disconnectBefore(true).build());

    static InternalCache cache;

    @BeforeClass
    public static void setUpClass() throws Exception {
      cache = (InternalCache) new CacheFactory().create();
      assertThat(cache.isClosed()).isFalse();
      assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();

      for (VM vm : getAllVMs()) {
        vm.invoke(() -> {
          cache = (InternalCache) new CacheFactory().create();
          assertThat(cache.isClosed()).isFalse();
          assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();
        });
      }
    }

    @Test
    public void everyDUnitVMShouldStillBeConnected() throws Exception {
      assertThat(cache.isClosed()).isFalse();
      assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();

      for (VM vm : getAllVMs()) {
        vm.invoke(() -> {
          assertThat(cache.isClosed()).isFalse();
          assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();
        });
      }
    }
  }

  /**
   * Used by test {@link #disconnectAfterShouldDisconnectAllAfterTests()}
   */
  @FixMethodOrder(MethodSorters.NAME_ASCENDING)
  public static class DisconnectAfter {

    @ClassRule
    public static RuleList rules = new RuleList().add(new DistributedTestRule())
        .add(new DistributedDisconnectRule.Builder().disconnectAfter(true).build());

    static InternalCache cache;

    @Test
    public void createCacheInEveryVM() throws Exception {
      cache = (InternalCache) new CacheFactory().create();
      assertThat(cache.isClosed()).isFalse();
      assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();

      for (VM vm : getAllVMs()) {
        vm.invoke(() -> {
          cache = (InternalCache) new CacheFactory().create();
          assertThat(cache.isClosed()).isFalse();
          assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();
        });
      }
    }

    @Test
    public void everyVMShouldStillBeConnected() throws Exception {
      assertThat(cache.isClosed()).isFalse();
      assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();

      for (VM vm : getAllVMs()) {
        vm.invoke(() -> {
          assertThat(cache.isClosed()).isFalse();
          assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();
        });
      }
    }
  }
}
