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

package org.apache.geode.redis.internal.executor.hash;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Ignore("GEODE-9378")
public class HashesAndCrashesDUnitTest {

  private static final Logger logger = LogService.getLogger();

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  private static RedisAdvancedClusterCommands<String, String> commands;
  private static RedisClusterClient clusterClient;

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void classSetup() throws Exception {
    locator = clusterStartUp.startLocatorVM(0);

    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    int redisPort1 = clusterStartUp.getRedisPort(1);
    clusterClient = RedisClusterClient.create("redis://localhost:" + redisPort1);

    ClusterTopologyRefreshOptions refreshOptions =
        ClusterTopologyRefreshOptions.builder()
            .enableAllAdaptiveRefreshTriggers()
            .build();

    clusterClient.setOptions(ClusterClientOptions.builder()
        .topologyRefreshOptions(refreshOptions)
        .validateClusterNodeMembership(false)
        .build());

    commands = clusterClient.connect().sync();
  }

  @AfterClass
  public static void cleanup() {
    try {
      clusterClient.shutdown();
    } catch (Exception ignored) {
      // https://github.com/lettuce-io/lettuce-core/issues/1800
    }
  }

  @Test
  public void givenServerCrashesDuringHSET_thenDataIsNotLost_andNoExceptionsAreLogged()
      throws Exception {
    modifyDataWhileCrashingVMs(DataType.HSET);
  }

  @Test
  public void givenServerCrashesDuringSADD_thenDataIsNotLost() throws Exception {
    modifyDataWhileCrashingVMs(DataType.SADD);
  }

  @Test
  public void givenServerCrashesDuringSET_thenDataIsNotLost() throws Exception {
    modifyDataWhileCrashingVMs(DataType.SET);
  }

  enum DataType {
    HSET, SADD, SET
  }

  private void modifyDataWhileCrashingVMs(DataType dataType) throws Exception {
    AtomicBoolean running1 = new AtomicBoolean(true);
    AtomicBoolean running2 = new AtomicBoolean(true);
    AtomicBoolean running3 = new AtomicBoolean(true);
    AtomicBoolean running4 = new AtomicBoolean(false);

    Runnable task1 = null;
    Runnable task2 = null;
    Runnable task3 = null;
    Runnable task4 = null;
    switch (dataType) {
      case HSET:
        task1 = () -> hsetPerformAndVerify(0, 20000, running1);
        task2 = () -> hsetPerformAndVerify(1, 20000, running2);
        task3 = () -> hsetPerformAndVerify(3, 20000, running3);
        task4 = () -> hsetPerformAndVerify(4, 1000, running4);
        break;
      case SADD:
        task1 = () -> saddPerformAndVerify(0, 20000, running1);
        task2 = () -> saddPerformAndVerify(1, 20000, running2);
        task3 = () -> saddPerformAndVerify(3, 20000, running3);
        task4 = () -> saddPerformAndVerify(4, 1000, running4);
        break;
      case SET:
        task1 = () -> setPerformAndVerify(0, 20000, running1);
        task2 = () -> setPerformAndVerify(1, 20000, running2);
        task3 = () -> setPerformAndVerify(3, 20000, running3);
        task4 = () -> setPerformAndVerify(4, 1000, running4);
        break;
    }

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);
    Future<Void> future4 = executor.runAsync(task4);

    future4.get();
    clusterStartUp.crashVM(2);
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    rebalanceAllRegions(server2);

    clusterStartUp.crashVM(3);
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());
    rebalanceAllRegions(server3);

    clusterStartUp.crashVM(2);
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    rebalanceAllRegions(server2);

    clusterStartUp.crashVM(3);
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());
    rebalanceAllRegions(server3);

    running1.set(false);
    running2.set(false);
    running3.set(false);

    future1.get();
    future2.get();
    future3.get();
  }

  private void hsetPerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    String key = "hset-key-" + index;
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      String fieldName = "field-" + iterationCount;
      try {
        commands.hset(key, fieldName, "value-" + iterationCount);
        iterationCount += 1;
      } catch (RedisCommandExecutionException ignore) {
      } catch (RedisException ex) {
        if (!ex.getMessage().contains("Connection reset by peer")) {
          throw ex;
        }
      }
    }

    for (int i = 0; i < iterationCount; i++) {
      String field = "field-" + i;
      String value = "value-" + i;
      assertThat(commands.hget(key, field)).isEqualTo(value);
    }

    logger.info("--->>> HSET test ran {} iterations", iterationCount);
  }

  private void saddPerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    String key = "sadd-key-" + index;
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      String member = "member-" + index + "-" + iterationCount;
      try {
        commands.sadd(key, member);
        iterationCount += 1;
      } catch (RedisCommandExecutionException ignore) {
      } catch (RedisException ex) {
        if (!ex.getMessage().contains("Connection reset by peer")) {
          throw ex;
        }
      }
    }

    List<String> missingMembers = new ArrayList<>();
    for (int i = 0; i < iterationCount; i++) {
      String member = "member-" + index + "-" + i;
      if (!commands.sismember(key, member)) {
        missingMembers.add(member);
      }
    }
    assertThat(missingMembers).isEmpty();

    logger.info("--->>> SADD test ran {} iterations, retrying {} times", iterationCount);
  }

  private void setPerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      String key = "set-key-" + index + "-" + iterationCount;
      try {
        commands.set(key, key);
        iterationCount += 1;
      } catch (RedisCommandExecutionException ignore) {
      } catch (RedisException ex) {
        if (!ex.getMessage().contains("Connection reset by peer")) {
          throw ex;
        }
      }
    }

    for (int i = 0; i < iterationCount; i++) {
      String key = "set-key-" + index + "-" + i;
      String value = commands.get(key);
      assertThat(value).isEqualTo(key);
    }

    logger.info("--->>> SET test ran {} iterations", iterationCount);
  }

  private static void rebalanceAllRegions(MemberVM vm) {
    vm.invoke(() -> {
      ResourceManager manager =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getResourceManager();

      RebalanceFactory factory = manager.createRebalanceFactory();

      try {
        RebalanceResults result = factory.start().getResults();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
