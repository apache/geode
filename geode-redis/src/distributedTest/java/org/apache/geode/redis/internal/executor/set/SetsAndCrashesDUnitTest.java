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

package org.apache.geode.redis.internal.executor.set;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.apache.geode.redis.internal.GeodeRedisServer.ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.ClientResources;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.session.springRedisTestApplication.config.DUnitSocketAddressResolver;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class SetsAndCrashesDUnitTest {

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

  private RedisCommands<String, String> commands;
  private static RedisClient redisClient;

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

    DUnitSocketAddressResolver dnsResolver =
        new DUnitSocketAddressResolver(new String[] {redisPort2, redisPort3});

    ClientResources resources = ClientResources.builder()
        .socketAddressResolver(dnsResolver)
        .build();

    redisClient = RedisClient.create(resources, "redis://localhost");
    redisClient.setOptions(ClientOptions.builder()
        .autoReconnect(true)
        .build());

    gfsh.connectAndVerify(locator);
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
  public void givenServerCrashesDuringSadd_thenDataIsNotLost() throws Exception {
    commands = redisClient.connect().sync();

    AtomicBoolean running1 = new AtomicBoolean(true);
    AtomicBoolean running2 = new AtomicBoolean(true);
    AtomicBoolean running3 = new AtomicBoolean(true);
    AtomicBoolean running4 = new AtomicBoolean(false);

    Future<List<String>> future1 = executor.submit(() -> performAndVerify(0, 20000, running1));
    Future<List<String>> future2 = executor.submit(() -> performAndVerify(1, 20000, running2));
    Future<List<String>> future3 = executor.submit(() -> performAndVerify(3, 20000, running3));
    Future<List<String>> future4 = executor.submit(() -> performAndVerify(4, 1000, running4));

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

    assertThat(future1.get()).isEmpty();
    assertThat(future2.get()).isEmpty();
    assertThat(future3.get()).isEmpty();
  }

  private List<String> performAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    String key = "key-" + index;
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      int localI = iterationCount;
      retryableCommand(c -> c.sadd(key, "value-" + localI));
      iterationCount += 1;
    }

    List<String> missingValues = new ArrayList<>();
    int retries = 0;
    for (int i = 0; i < iterationCount; i++) {
      String value = "value-" + i;
      retries += retryableCommand(c -> {
        if (!c.sismember(key, value)) {
          missingValues.add(value);
        }
      });
    }

    logger.info("--->>> SADD test ran {} iterations, retrying {} times", iterationCount, retries);

    return missingValues;
  }

  private int retryableCommand(Consumer<RedisCommands<String, String>> exe) {
    int retries = 0;
    do {
      try {
        exe.accept(commands);
        return retries;
      } catch (Exception e) {
        logger.info("--->>> Handling retryable error {}", e.getMessage());
        commands = redisClient.connect().sync();
        retries += 1;
      }
    } while (true);
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
