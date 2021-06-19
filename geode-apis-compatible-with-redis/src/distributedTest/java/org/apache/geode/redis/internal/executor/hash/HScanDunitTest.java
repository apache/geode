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

package org.apache.geode.redis.internal.executor.hash;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.lettuce.core.MapScanCursor;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Ignore("GEODE-9368")
public class HScanDunitTest {

  @ClassRule
  public static RedisClusterStartupRule redisClusterStartupRule = new RedisClusterStartupRule(4);

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static RedisAdvancedClusterCommands<String, String> commands;
  private static RedisClusterClient clusterClient;

  private static MemberVM locator;

  static final String HASH_KEY = "key";
  static final String BASE_FIELD = "baseField_";
  static final Map<String, String> INITIAL_DATA_SET = makeEntrySet(1000);

  @BeforeClass
  public static void classSetup() {
    locator = redisClusterStartupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();

    // note: due to rules around member weighting in split-brain scenarios,
    // vm1 (server1) should not be crashed or it will cause additional (unrelated) failures
    redisClusterStartupRule.startRedisVM(1, locatorPort);
    redisClusterStartupRule.startRedisVM(2, locatorPort);
    redisClusterStartupRule.startRedisVM(3, locatorPort);

    int redisServerPort1 = redisClusterStartupRule.getRedisPort(1);
    clusterClient = RedisClusterClient.create("redis://localhost:" + redisServerPort1);

    ClusterTopologyRefreshOptions refreshOptions =
        ClusterTopologyRefreshOptions.builder()
            .enableAllAdaptiveRefreshTriggers()
            .build();

    clusterClient.setOptions(ClusterClientOptions.builder()
        .topologyRefreshOptions(refreshOptions)
        .validateClusterNodeMembership(false)
        .build());

    commands = clusterClient.connect().sync();
    commands.hset(HASH_KEY, INITIAL_DATA_SET);
  }

  @AfterClass
  public static void tearDown() {
    clusterClient.shutdown();
  }

  @Test
  public void should_allowHscanIterationToCompleteSuccessfullyGivenServerCrashesDuringIteration()
      throws ExecutionException, InterruptedException {

    AtomicBoolean keepCrashingVMs = new AtomicBoolean(true);
    AtomicInteger numberOfTimesVMCrashed = new AtomicInteger(0);

    Future<Void> hScanFuture = executor.runAsync(
        () -> doHScanContinuallyAndAssertOnResults(keepCrashingVMs, numberOfTimesVMCrashed));

    Future<Void> crashingVmFuture = executor.runAsync(
        () -> crashAlternatingVMS(keepCrashingVMs, numberOfTimesVMCrashed));

    hScanFuture.get();
    crashingVmFuture.get();
  }

  private static void doHScanContinuallyAndAssertOnResults(AtomicBoolean keepCrashingVMs,
      AtomicInteger numberOfTimesServersCrashed) {
    int numberOfAssertionsCompleted = 0;

    ScanCursor scanCursor = new ScanCursor("0", false);
    List<String> allEntries = new ArrayList<>();
    MapScanCursor<String, String> result;

    while (numberOfAssertionsCompleted < 3 || numberOfTimesServersCrashed.get() < 3) {

      allEntries.clear();
      scanCursor.setCursor("0");
      scanCursor.setFinished(false);

      try {
        do {
          result = commands.hscan(HASH_KEY, scanCursor);
          scanCursor.setCursor(result.getCursor());
          Map<String, String> resultEntries = result.getMap();

          resultEntries.forEach((key, value) -> allEntries.add(key));

        } while (!result.isFinished());

        assertThat(allEntries).containsAll(INITIAL_DATA_SET.keySet());
        numberOfAssertionsCompleted++;

      } catch (RedisCommandExecutionException ignore) {
      } catch (RedisException ex) {
        if (!ex.getMessage().contains("Connection reset by peer")) {
          throw ex;
        } // ignore error

      }
    }
    keepCrashingVMs.set(false);
  }

  private void crashAlternatingVMS(AtomicBoolean keepCrashingVMs,
      AtomicInteger numberOfTimesServersCrashed) {

    int vmToCrashToggle = 3;
    MemberVM vm;

    do {
      redisClusterStartupRule.crashVM(vmToCrashToggle);
      vm = redisClusterStartupRule.startRedisVM(vmToCrashToggle, locator.getPort());
      rebalanceAllRegions(vm);
      numberOfTimesServersCrashed.incrementAndGet();
      vmToCrashToggle = (vmToCrashToggle == 2) ? 3 : 2;

    } while (keepCrashingVMs.get());
  }

  private static Map<String, String> makeEntrySet(int sizeOfDataSet) {
    Map<String, String> dataSet = new HashMap<>();
    for (int i = 0; i < sizeOfDataSet; i++) {
      dataSet.put(BASE_FIELD + i, "value_" + i);
    }
    return dataSet;
  }

  private static void rebalanceAllRegions(MemberVM vm) {
    vm.invoke(() -> {

      ResourceManager manager = ClusterStartupRule.getCache().getResourceManager();
      RebalanceFactory factory = manager.createRebalanceFactory();

      try {
        factory.start().getResults();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
