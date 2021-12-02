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

package org.apache.geode.redis.internal.commands.executor.hash;

import static org.apache.geode.distributed.ConfigurationProperties.GEODE_FOR_REDIS_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.dunit.rules.SerializableFunction;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class HashesAndCrashesDUnitTest {

  private static final Logger logger = LogService.getLogger();

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  private static SerializableFunction<ServerStarterRule> serverOperator2;
  private static SerializableFunction<ServerStarterRule> serverOperator3;

  private RedisAdvancedClusterCommands<String, String> commands;
  private RedisClusterClient clusterClient;

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void classSetup() throws Exception {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    int locatorPort = locator.getPort();
    clusterStartUp.startRedisVM(1, locatorPort);

    int redisPort2 = AvailablePortHelper.getRandomAvailableTCPPort();
    serverOperator2 = s -> s
        .withProperty(GEODE_FOR_REDIS_PORT, redisPort2 + "")
        .withConnectionToLocator(locatorPort);
    clusterStartUp.startRedisVM(2, serverOperator2);

    int redisPort3 = AvailablePortHelper.getRandomAvailableTCPPort();
    serverOperator3 = s -> s
        .withProperty(GEODE_FOR_REDIS_PORT, redisPort3 + "")
        .withConnectionToLocator(locatorPort);
    clusterStartUp.startRedisVM(3, serverOperator3);

    clusterStartUp.enableDebugLogging(1);
    clusterStartUp.enableDebugLogging(2);
    clusterStartUp.enableDebugLogging(3);
  }

  @Before
  public void before() {
    int redisPort1 = clusterStartUp.getRedisPort(1);
    clusterClient = RedisClusterClient.create("redis://localhost:" + redisPort1);

    ClusterTopologyRefreshOptions refreshOptions =
        ClusterTopologyRefreshOptions.builder()
            .enableAllAdaptiveRefreshTriggers()
            .refreshTriggersReconnectAttempts(1)
            .refreshPeriod(Duration.ofSeconds(30))
            .build();

    clusterClient.setOptions(ClusterClientOptions.builder()
        .socketOptions(SocketOptions.builder()
            .connectTimeout(Duration.ofSeconds(30)).build())
        .topologyRefreshOptions(refreshOptions)
        .validateClusterNodeMembership(false)
        .build());

    commands = clusterClient.connect().sync();
  }

  @After
  public void cleanup() {
    clusterClient.shutdown();
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
    AtomicBoolean running = new AtomicBoolean(true);

    Runnable task1 = null;
    Runnable task2 = null;
    Runnable task3 = null;
    Runnable task4 = null;
    switch (dataType) {
      case HSET:
        task1 = () -> hsetPerformAndVerify(0, 10000, running);
        task2 = () -> hsetPerformAndVerify(1, 10000, running);
        task3 = () -> hsetPerformAndVerify(2, 10000, running);
        task4 = () -> hsetPerformAndVerify(3, 10000, running);
        break;
      case SADD:
        task1 = () -> saddPerformAndVerify(0, 10000, running);
        task2 = () -> saddPerformAndVerify(1, 10000, running);
        task3 = () -> saddPerformAndVerify(2, 10000, running);
        task4 = () -> saddPerformAndVerify(3, 10000, running);
        break;
      case SET:
        task1 = () -> setPerformAndVerify(0, 10000, running);
        task2 = () -> setPerformAndVerify(1, 10000, running);
        task3 = () -> setPerformAndVerify(2, 10000, running);
        task4 = () -> setPerformAndVerify(3, 10000, running);
        break;
    }

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);
    Future<Void> future4 = executor.runAsync(task4);

    // Wait for commands to start executing
    Thread.sleep(2000);

    cycleVM(2, serverOperator2);
    cycleVM(3, serverOperator3);
    cycleVM(2, serverOperator2);
    cycleVM(3, serverOperator3);

    running.set(false);

    future1.get();
    future2.get();
    future3.get();
    future4.get();
  }

  private void cycleVM(int vmId, SerializableFunction<ServerStarterRule> operator)
      throws Exception {
    clusterStartUp.crashVM(vmId);
    clusterStartUp.startRedisVM(vmId, operator);
    clusterStartUp.enableDebugLogging(vmId);
    clusterStartUp.rebalanceAllRegions();
    Thread.sleep(5000);
  }

  private void hsetPerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    String baseKey = "hset-key-" + index + "-";
    int iterationCount = 0;
    int totalKeys = 20;

    try {
      while (iterationCount < minimumIterations || isRunning.get()) {
        String key = baseKey + (iterationCount % totalKeys);
        String field = "field-" + iterationCount;
        String value = "value-" + iterationCount;
        executeUntilSuccess(() -> commands.hset(key, field, value),
            String.format("HSET %s %s %s", key, field, value));
        iterationCount += 1;
      }

      for (int i = 0; i < iterationCount; i++) {
        String key = baseKey + (i % totalKeys);
        String field = "field-" + i;
        String value = "value-" + i;
        assertThat(commands.hget(key, field)).isEqualTo(value);
      }
    } finally {
      isRunning.set(false);
      logger.info("--->>> HSET test ran {} iterations", iterationCount);
    }
  }

  private void saddPerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    String baseKey = "sadd-key-" + index + "-";
    int iterationCount = 0;
    int totalKeys = 20;

    try {
      while (iterationCount < minimumIterations || isRunning.get()) {
        String key = baseKey + (iterationCount % totalKeys);
        String member = "member-" + index + "-" + iterationCount;
        executeUntilSuccess(() -> commands.sadd(key, member),
            String.format("SADD %s %s", key, member));
        iterationCount += 1;
      }

      List<String> missingMembers = new ArrayList<>();
      for (int i = 0; i < iterationCount; i++) {
        String key = baseKey + (i % totalKeys);
        String member = "member-" + index + "-" + i;
        if (!commands.sismember(key, member)) {
          missingMembers.add(member);
        }
      }
      assertThat(missingMembers).isEmpty();
    } finally {
      isRunning.set(false);
      logger.info("--->>> SADD test ran {} iterations", iterationCount);
    }
  }

  private void setPerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    int iterationCount = 0;

    try {
      while (iterationCount < minimumIterations || isRunning.get()) {
        String key = "set-key-" + index + "-" + iterationCount;
        executeUntilSuccess(() -> commands.set(key, key), String.format("SET %s %s", key, key));
        iterationCount += 1;
      }

      for (int i = 0; i < iterationCount; i++) {
        String key = "set-key-" + index + "-" + i;
        assertThat(commands.get(key)).isEqualTo(key);
      }
    } finally {
      isRunning.set(false);
      logger.info("--->>> SET test ran {} iterations", iterationCount);
    }
  }

  private void executeUntilSuccess(Runnable command, String text) {
    while (true) {
      try {
        command.run();
        return;
      } catch (RedisCommandExecutionException | RedisCommandTimeoutException ex) {
        logger.info("DEBUG = will retry {} - {}", text, ex.getMessage());
      } catch (Exception ex) {
        if (ex.getMessage().contains("Connection reset by peer")
            || ex.getMessage().contains("Connection closed")) {
          logger.info("DEBUG = will retry {} - {}", text, ex.getMessage());
        } else {
          logger.info("DEBUG - exception", ex);
          throw ex;
        }
      }
    }
  }

}
