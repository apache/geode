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

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.apache.geode.redis.internal.GeodeRedisServer.ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.ClientResources;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.session.springRedisTestApplication.config.DUnitSocketAddressResolver;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class HashesAndCrashesDUnitTest {

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

  private static int[] redisPorts;

  private RedisClient redisClient;
  private StatefulRedisConnection<String, String> connection;
  private RedisCommands<String, String> commands;

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void classSetup() throws Exception {
    redisPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);

    int locatorPort = locator.getPort();

    String redisPort1 = redisPorts[0] + "";
    server1 = clusterStartUp.startServerVM(1,
        x -> x.withProperty(REDIS_PORT, redisPort1)
            .withProperty(REDIS_ENABLED, "true")
            .withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withSystemProperty(ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM, "true")
            .withConnectionToLocator(locatorPort));

    String redisPort2 = redisPorts[1] + "";
    server2 = clusterStartUp.startServerVM(2,
        x -> x.withProperty(REDIS_PORT, redisPort2)
            .withProperty(REDIS_ENABLED, "true")
            .withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withSystemProperty(ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM, "true")
            .withConnectionToLocator(locatorPort));

    String redisPort3 = redisPorts[2] + "";
    server3 = clusterStartUp.startServerVM(3,
        x -> x.withProperty(REDIS_PORT, redisPort3)
            .withProperty(REDIS_ENABLED, "true")
            .withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withSystemProperty(ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM, "true")
            .withConnectionToLocator(locatorPort));

    gfsh.connectAndVerify(locator);

  }

  @Before
  public void before() {
    addIgnoredException(FunctionException.class);
    String redisPort1 = "" + redisPorts[0];
    String redisPort2 = "" + redisPorts[1];
    String redisPort3 = "" + redisPorts[2];
    // For now only tell the client about redisPort1.
    // That server is never restarted so clients should
    // never fail due to the server they are connected to failing.
    DUnitSocketAddressResolver dnsResolver =
        new DUnitSocketAddressResolver(new String[] {redisPort1});

    ClientResources resources = ClientResources.builder()
        .socketAddressResolver(dnsResolver)
        .build();

    redisClient = RedisClient.create(resources, "redis://localhost");
    redisClient.setOptions(ClientOptions.builder()
        .autoReconnect(true)
        .build());
    connection = redisClient.connect();
    commands = connection.sync();
  }

  @After
  public void after() {
    connection.close();
    redisClient.shutdown();
  }


  private MemberVM startRedisVM(int vmID, int redisPort) {
    int locatorPort = locator.getPort();
    return clusterStartUp.startServerVM(vmID,
        x -> x.withProperty(REDIS_PORT, redisPort + "")
            .withProperty(REDIS_ENABLED, "true")
            .withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withSystemProperty(ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM, "true")
            .withConnectionToLocator(locatorPort));
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
        if (ex.getMessage().contains("Connection reset by peer")) {
          // ignore it
        } else {
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
        if (ex.getMessage().contains("Connection reset by peer")) {
          // ignore it
        } else {
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
        if (ex.getMessage().contains("Connection reset by peer")) {
          // ignore it
        } else {
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
