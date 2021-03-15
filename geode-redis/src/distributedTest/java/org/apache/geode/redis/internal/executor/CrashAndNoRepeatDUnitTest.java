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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.apache.geode.redis.internal.GeodeRedisServer.ENABLE_UNSUPPORTED_COMMANDS_SYSTEM_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.GeodeRedisService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class CrashAndNoRepeatDUnitTest {

  private static final Logger logger = LogService.getLogger();

  @ClassRule
  public static ClusterStartupRule clusterStartUp = new ClusterStartupRule(4);

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static Properties locatorProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  private static int[] redisPorts = new int[3];
  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void classSetup() throws Exception {
    String log_level = "info";
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    int locatorPort = locator.getPort();

    server1 = clusterStartUp.startServerVM(1,
        x -> x.withProperty(REDIS_PORT, "0")
            .withProperty(REDIS_ENABLED, "true")
            .withProperty(LOG_LEVEL, log_level)
            .withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withSystemProperty(ENABLE_UNSUPPORTED_COMMANDS_SYSTEM_PROPERTY, "true")
            .withConnectionToLocator(locatorPort));
    redisPorts[0] = getPort(server1);

    server2 = clusterStartUp.startServerVM(2,
        x -> x.withProperty(REDIS_PORT, "0")
            .withProperty(REDIS_ENABLED, "true")
            .withProperty(LOG_LEVEL, log_level)
            .withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withSystemProperty(ENABLE_UNSUPPORTED_COMMANDS_SYSTEM_PROPERTY, "true")
            .withConnectionToLocator(locatorPort));
    redisPorts[1] = getPort(server2);

    server3 = clusterStartUp.startServerVM(3,
        x -> x.withProperty(REDIS_PORT, "0")
            .withProperty(REDIS_ENABLED, "true")
            .withProperty(LOG_LEVEL, log_level)
            .withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withSystemProperty(ENABLE_UNSUPPORTED_COMMANDS_SYSTEM_PROPERTY, "true")
            .withConnectionToLocator(locatorPort));
    redisPorts[2] = getPort(server3);

    gfsh.connectAndVerify(locator);
  }

  private static int getPort(MemberVM vm) {
    return vm.invoke(() -> ClusterStartupRule.getCache()
        .getService(GeodeRedisService.class)
        .getPort());
  }

  private synchronized Jedis connect(AtomicReference<Jedis> jedisRef) {
    jedisRef.set(new Jedis(LOCAL_HOST, redisPorts[0], JEDIS_TIMEOUT));
    return jedisRef.get();
  }

  private MemberVM startRedisVM(int vmID, int redisPort) {
    int locatorPort = locator.getPort();
    return clusterStartUp.startServerVM(vmID,
        x -> x.withProperty(REDIS_PORT, redisPort + "")
            .withProperty(REDIS_ENABLED, "true")
            .withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withSystemProperty(ENABLE_UNSUPPORTED_COMMANDS_SYSTEM_PROPERTY, "true")
            .withConnectionToLocator(locatorPort));
  }

  @Test
  public void givenServerCrashesDuringAPPEND_thenDataIsNotLost() throws Exception {
    AtomicBoolean running1 = new AtomicBoolean(true);
    AtomicBoolean running2 = new AtomicBoolean(false);

    Runnable task1 = () -> appendPerformAndVerify(1, 20000, running1);
    Runnable task2 = () -> appendPerformAndVerify(2, 20000, running1);
    Runnable task3 = () -> appendPerformAndVerify(3, 20000, running1);
    Runnable task4 = () -> appendPerformAndVerify(4, 1000, running2);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);
    Future<Void> future4 = executor.runAsync(task4);

    future4.get();
    clusterStartUp.crashVM(2);
    server2 = startRedisVM(2, redisPorts[1]);
    rebalanceAllRegions(server2);

    clusterStartUp.crashVM(3);
    server3 = startRedisVM(3, redisPorts[2]);
    rebalanceAllRegions(server3);

    clusterStartUp.crashVM(2);
    server2 = startRedisVM(2, redisPorts[1]);
    rebalanceAllRegions(server2);

    clusterStartUp.crashVM(3);
    server3 = startRedisVM(3, redisPorts[2]);
    rebalanceAllRegions(server3);

    running1.set(false);

    future1.get();
    future2.get();
    future3.get();
  }

  @Test
  public void givenServerCrashesDuringRename_thenDataIsNotLost() throws Exception {
    AtomicBoolean running1 = new AtomicBoolean(true);
    AtomicBoolean running2 = new AtomicBoolean(true);
    AtomicBoolean running3 = new AtomicBoolean(true);
    AtomicBoolean running4 = new AtomicBoolean(false);

    Runnable task1 = () -> renamePerformAndVerify(1, 20000, running1);
    Runnable task2 = () -> renamePerformAndVerify(2, 20000, running2);
    Runnable task3 = () -> renamePerformAndVerify(3, 20000, running3);
    Runnable task4 = () -> renamePerformAndVerify(4, 1000, running4);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);
    Future<Void> future4 = executor.runAsync(task4);

    future4.get();
    clusterStartUp.crashVM(2);
    server2 = startRedisVM(2, redisPorts[1]);
    rebalanceAllRegions(server2);

    clusterStartUp.crashVM(3);
    server3 = startRedisVM(3, redisPorts[2]);
    rebalanceAllRegions(server3);

    clusterStartUp.crashVM(2);
    server2 = startRedisVM(2, redisPorts[1]);
    rebalanceAllRegions(server2);

    clusterStartUp.crashVM(3);
    server3 = startRedisVM(3, redisPorts[2]);
    rebalanceAllRegions(server3);

    running1.set(false);
    running2.set(false);
    running3.set(false);

    future1.get();
    future2.get();
    future3.get();
  }

  private <T> T doWithRetry(Supplier<T> supplier) {
    while (true) {
      try {
        return supplier.get();
      } catch (JedisConnectionException ex) {
        if (!ex.getMessage().contains("Unexpected end of stream.")) {
          throw ex;
        }
      }
    }
  }

  private void renamePerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    final AtomicReference<Jedis> jedisRef = new AtomicReference<>();
    connect(jedisRef);
    String newKey = null;
    String baseKey = "rename-key-" + index;
    jedisRef.get().set(baseKey + "-0", "value");
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      String oldKey = baseKey + "-" + iterationCount;
      newKey = baseKey + "-" + (iterationCount + 1);
      try {
        jedisRef.get().rename(oldKey, newKey);
        iterationCount += 1;
      } catch (JedisConnectionException | JedisDataException ex) {
        if (ex.getMessage().contains("Unexpected end of stream.")) {
          if (!doWithRetry(() -> connect(jedisRef).exists(oldKey))) {
            iterationCount += 1;
          }
        } else if (ex.getMessage().contains("no such key")) {
          if (!doWithRetry(() -> connect(jedisRef).exists(oldKey))) {
            iterationCount += 1;
          }
        } else {
          throw ex;
        }
      }
    }

    assertThat(jedisRef.get().keys(baseKey + "-*").size()).isEqualTo(1);
    assertThat(jedisRef.get().exists(newKey)).isTrue();

    logger.info("--->>> RENAME test ran {} iterations", iterationCount);
    jedisRef.get().disconnect();
  }

  private void appendPerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    String key = "append-key-" + index;
    int iterationCount = 0;
    final AtomicReference<Jedis> jedisRef = new AtomicReference<>();
    connect(jedisRef);

    while (iterationCount < minimumIterations || isRunning.get()) {
      String appendString = "-" + key + "-" + iterationCount + "-";
      try {
        jedisRef.get().append(key, appendString);
        iterationCount += 1;
      } catch (JedisConnectionException ex) {
        if (ex.getMessage().contains("Unexpected end of stream.")) {
          if (!doWithRetry(() -> connect(jedisRef).get(key)).endsWith(appendString)) {
            // give some time for the in-flight op to be done
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.interrupted();
            }
          }
          if (doWithRetry(() -> connect(jedisRef).get(key)).endsWith(appendString)) {
            iterationCount += 1;
          } else {
            LogService.getLogger().info("--->>> iterationCount not updated - will retry: "
                + appendString);
          }
        } else {
          throw ex;
        }
      }
    }

    String storedString = jedisRef.get().get(key);
    int idx = 0;
    for (int i = 0; i < iterationCount; i++) {
      String expectedValue = "-" + key + "-" + i + "-";
      String foundValue = storedString.substring(idx, idx + expectedValue.length());
      if (!expectedValue.equals(foundValue)) {
        Assert.fail("unexpected " + foundValue + " at index " + i + " iterationCount="
            + iterationCount + " in string "
            + storedString);
        break;
      }
      idx += expectedValue.length();
    }

    logger.info("--->>> APPEND test ran {} iterations", iterationCount);
    jedisRef.get().disconnect();
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
