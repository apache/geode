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

package org.apache.geode.redis.internal.commands.executor.hash;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lettuce.core.MapScanCursor;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class HScanDunitTest {

  @ClassRule
  public static RedisClusterStartupRule redisClusterStartupRule = new RedisClusterStartupRule(4);

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static RedisAdvancedClusterCommands<String, String> commands;
  private static RedisClusterClient clusterClient;

  private static MemberVM locator;

  private static final String HASH_KEY = "key";
  private static final String BASE_FIELD = "baseField_";
  private static final Map<String, String> INITIAL_DATA_SET = makeEntrySet(1000);

  @BeforeClass
  public static void classSetup() {
    locator = redisClusterStartupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();

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
    try {
      clusterClient.shutdown();
    } catch (Exception ignored) {
      // https://github.com/lettuce-io/lettuce-core/issues/1800
    }
  }

  @Test
  public void should_allowHscanIterationToCompleteSuccessfullyGivenBucketIsMoving()
      throws ExecutionException, InterruptedException {

    AtomicBoolean running = new AtomicBoolean(true);

    Future<Void> hScanFuture = executor.runAsync(
        () -> doHScanContinuallyAndAssertOnResults(running));

    for (int i = 0; i < 100 && running.get(); i++) {
      redisClusterStartupRule.moveBucketForKey(HASH_KEY);
      GeodeAwaitility.await().during(200, TimeUnit.MILLISECONDS).until(() -> true);
    }

    running.set(false);
    hScanFuture.get();
  }

  private static void doHScanContinuallyAndAssertOnResults(AtomicBoolean running) {
    ScanCursor scanCursor = new ScanCursor("0", false);
    List<String> allEntries = new ArrayList<>();
    MapScanCursor<String, String> result;
    int i = 0;

    while (running.get()) {
      allEntries.clear();
      scanCursor.setCursor("0");
      scanCursor.setFinished(false);

      do {
        result = commands.hscan(HASH_KEY, scanCursor);
        scanCursor.setCursor(result.getCursor());
        Map<String, String> resultEntries = result.getMap();

        resultEntries.forEach((key, value) -> allEntries.add(key));
      } while (!result.isFinished());

      try {
        assertThat(allEntries).containsAll(INITIAL_DATA_SET.keySet());
      } catch (AssertionError ex) {
        running.set(false);
        throw ex;
      }

      i++;
    }

    LogService.getLogger().info("--->>> Completed {} iterations of HSCAN", i);
  }

  private static Map<String, String> makeEntrySet(int sizeOfDataSet) {
    Map<String, String> dataSet = new HashMap<>();
    for (int i = 0; i < sizeOfDataSet; i++) {
      dataSet.put(BASE_FIELD + i, "value_" + i);
    }
    return dataSet;
  }

}
