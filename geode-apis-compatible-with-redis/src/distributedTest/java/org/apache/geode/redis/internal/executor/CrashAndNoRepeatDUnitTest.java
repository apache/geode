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
 *
 */

package org.apache.geode.redis.internal.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.cluster.RedisMemberInfo;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class CrashAndNoRepeatDUnitTest {

  private static final Logger logger = LogService.getLogger();

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;
  private static RedisClusterClient clusterClient;
  private static RedisAdvancedClusterCommands<String, String> lettuce;
  private static int redisServerPort1;
  private static int redisServerPort2;
  private static int redisServerPort3;
  private static Retry retry;

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void classSetup() throws Exception {
    locator = clusterStartUp.startLocatorVM(0);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    redisServerPort1 = clusterStartUp.getRedisPort(1);
    redisServerPort2 = clusterStartUp.getRedisPort(2);
    redisServerPort3 = clusterStartUp.getRedisPort(3);
    clusterClient = RedisClusterClient.create("redis://localhost:" + redisServerPort1);

    ClusterTopologyRefreshOptions refreshOptions =
        ClusterTopologyRefreshOptions.builder()
            .enableAllAdaptiveRefreshTriggers()
            .enablePeriodicRefresh(Duration.ofSeconds(5))
            .build();

    clusterClient.setOptions(ClusterClientOptions.builder()
        .topologyRefreshOptions(refreshOptions)
        .validateClusterNodeMembership(false)
        .build());

    lettuce = clusterClient.connect().sync();

    RetryConfig config = RetryConfig.custom()
        .maxAttempts(30)
        .retryOnException(e -> anyCauseContains(e, "Connection reset by peer")
            || anyCauseContains(e, "Unable to connect"))
        .build();
    RetryRegistry registry = RetryRegistry.of(config);
    retry = registry.retry("retries");
  }

  @AfterClass
  public static void cleanup() {
    try {
      clusterClient.shutdown();
    } catch (Exception ignored) {
      // See lettuce bug https://github.com/lettuce-io/lettuce-core/issues/1800
    }
  }

  private static boolean anyCauseContains(Throwable cause, String message) {
    Throwable error = cause;
    do {
      String thisMessage = error.getMessage();
      if (thisMessage != null && thisMessage.contains(message)) {
        return true;
      }
      error = error.getCause();
    } while (error != null);

    return false;
  }

  @Test
  public void givenServerCrashesDuringAppend_thenDataIsNotLost() throws Exception {
    AtomicBoolean running1 = new AtomicBoolean(true);
    AtomicBoolean running2 = new AtomicBoolean(false);

    String hashtag1 = getKeyOnServer("append", 1);
    String hashtag2 = getKeyOnServer("append", 2);
    String hashtag3 = getKeyOnServer("append", 3);

    AtomicReference<String> phase = new AtomicReference<>("STARTUP");
    Runnable task1 = () -> appendPerformAndVerify(1, 20000, hashtag1, running1, phase);
    Runnable task2 = () -> appendPerformAndVerify(2, 20000, hashtag2, running1, phase);
    Runnable task3 = () -> appendPerformAndVerify(3, 1000, hashtag3, running2, phase);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);

    future3.get();
    phase.set("CRASH 1 SERVER2");
    clusterStartUp.crashVM(2);
    phase.set("RESTART 1 SERVER2");
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    phase.set("REBALANCE 1 SERVER2");
    rebalanceAllRegions(server2);

    phase.set("CRASH 2 SERVER3");
    clusterStartUp.crashVM(3);
    phase.set("RESTART 2 SERVER3");
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());
    phase.set("REBALANCE 2 SERVER3");
    rebalanceAllRegions(server3);

    phase.set("CRASH 3 SERVER2");
    clusterStartUp.crashVM(2);
    phase.set("RESTART 3 SERVER2");
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    phase.set("REBALANCE 3 SERVER2");
    rebalanceAllRegions(server2);

    phase.set("CRASH 4 SERVER3");
    clusterStartUp.crashVM(3);
    phase.set("RESTART 4 SERVER3");
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());
    phase.set("REBALANCE 4 SERVER3");
    rebalanceAllRegions(server3);

    running1.set(false);

    future1.get();
    future2.get();
  }

  @Test
  public void givenServerCrashesDuringRename_thenDataIsNotLost() throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicBoolean runningFalse = new AtomicBoolean(false);

    String hashtag1 = getKeyOnServer("rename", 1);
    String hashtag2 = getKeyOnServer("rename", 2);
    String hashtag3 = getKeyOnServer("rename", 3);

    AtomicReference<String> phase = new AtomicReference<>("STARTUP");
    Runnable task1 = () -> renamePerformAndVerify(1, 20000, hashtag1, running, phase);
    Runnable task2 = () -> renamePerformAndVerify(2, 20000, hashtag2, running, phase);
    Runnable task3 = () -> renamePerformAndVerify(3, 1000, hashtag3, runningFalse, phase);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);

    future3.get();
    phase.set("CRASH 1 SERVER2");
    clusterStartUp.crashVM(2);
    phase.set("RESTART 1 SERVER2");
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    phase.set("REBALANCE 1 SERVER2");
    rebalanceAllRegions(server2);

    phase.set("CRASH 2 SERVER3");
    clusterStartUp.crashVM(3);
    phase.set("RESTART 2 SERVER3");
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());
    phase.set("REBALANCE 2 SERVER3");
    rebalanceAllRegions(server3);

    phase.set("CRASH 3 SERVER2");
    clusterStartUp.crashVM(2);
    phase.set("RESTART 3 SERVER2");
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    phase.set("REBALANCE 3 SERVER2");
    rebalanceAllRegions(server2);

    phase.set("CRASH 4 SERVER3");
    clusterStartUp.crashVM(3);
    phase.set("RESTART 4 SERVER3");
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());
    phase.set("REBALANCE 4 SERVER3");
    rebalanceAllRegions(server3);

    running.set(false);

    future1.get();
    future2.get();
  }

  private String getKeyOnServer(String keyPrefix, int vmId) {
    int port = clusterStartUp.getRedisPort(vmId);
    int i = 0;
    while (true) {
      String key = keyPrefix + i;
      RedisMemberInfo memberInfo = clusterStartUp.getMemberInfo(key);
      if (memberInfo.getRedisPort() == port) {
        return key;
      }
      i++;
    }
  }

  private void renamePerformAndVerify(int index, int minimumIterations, String hashtag,
      AtomicBoolean isRunning,
      AtomicReference<String> phase) {
    String baseKey = "{" + hashtag + "}-key-" + index;
    lettuce.set(baseKey + "-0", "value");
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      String oldKey = baseKey + "-" + iterationCount;
      String newKey = baseKey + "-" + (iterationCount + 1);
      try {
        Retry.decorateCallable(retry, () -> lettuce.rename(oldKey, newKey)).call();
      } catch (RedisCommandExecutionException rex) {
        // The command was retried after a failure where the Geode part was completed but the
        // response never made it back. So the next time round, the key doesn't exist. As long as
        // the 'exists' assertion below passes, this is OK.
        if (!rex.getMessage().contains(RedisConstants.ERROR_NO_SUCH_KEY)) {
          throw rex;
        }
      } catch (Exception ex) {
        isRunning.set(false);
        throw new RuntimeException("Exception performing RENAME " + oldKey + " " + newKey
            + " during phase: " + phase.get(), ex);
      }

      long exists;
      try {
        exists = Retry.decorateCallable(retry, () -> lettuce.exists(newKey)).call();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }

      assertThat(exists).as("key " + newKey + " should exist").isEqualTo(1);
      iterationCount += 1;
    }

    logger.info("--->>> RENAME test ran {} iterations", iterationCount);
  }

  private void appendPerformAndVerify(int index, int minimumIterations, String hashtag,
      AtomicBoolean isRunning, AtomicReference<String> phase) {
    String key = "{" + hashtag + "}-key-" + index;
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      String appendString = "-" + key + "-" + iterationCount + "-";
      try {
        Retry.decorateCallable(retry, () -> lettuce.append(key, appendString)).call();
      } catch (Exception ex) {
        isRunning.set(false);
        throw new RuntimeException("Exception performing APPEND " + appendString
            + " during phase: " + phase.get(), ex);
      }
      iterationCount += 1;
    }

    String storedString;
    try {
      storedString = Retry.decorateCallable(retry, () -> lettuce.get(key)).call();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    int idx = 0;
    int i = 0;
    while (i < iterationCount) {
      String previousValue = "-" + key + "-" + (i - 1) + "-";
      String expectedValue = "-" + key + "-" + i + "-";
      String foundValue = storedString.substring(idx, idx + expectedValue.length());
      if (!foundValue.equals(expectedValue)) {
        if (foundValue.equals(previousValue)) {
          // This means there was a duplicate which would be as a result of the APPEND command
          // being retried.
          idx += previousValue.length();
          continue;
        }
        Assert.fail("unexpected " + foundValue + " at index " + i + " iterationCount="
            + iterationCount + " in string "
            + storedString);
        break;
      }
      idx += expectedValue.length();
      i++;
    }

    logger.info("--->>> APPEND test ran {} iterations", iterationCount);
  }

  private static void rebalanceAllRegions(MemberVM vm) {
    vm.invoke(() -> {
      ResourceManager manager = ClusterStartupRule.getCache().getResourceManager();

      RebalanceFactory factory = manager.createRebalanceFactory();

      try {
        RebalanceResults result = factory.start().getResults();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
