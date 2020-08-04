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

package org.apache.geode.redis;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.apache.geode.redis.internal.GeodeRedisServer.ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.ClientResources;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.redis.session.springRedisTestApplication.config.DUnitSocketAddressResolver;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class NonPrimaryMemberCrashDUnit {
  private static final String REDIS_DATA_REGION = "__REDIS_DATA";

  private static final Logger logger = LogService.getLogger();

  @ClassRule
  public static ClusterStartupRule clusterStartUp = new ClusterStartupRule(4);

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static Properties locatorProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;

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

//    String redisPort3 = redisPorts[2] + "";
//    server3 = clusterStartUp.startServerVM(3,
//        x -> x.withProperty(REDIS_PORT, redisPort3)
//            .withProperty(REDIS_ENABLED, "true")
//            .withProperty(REDIS_BIND_ADDRESS, "localhost")
//            .withSystemProperty(ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM, "true")
//            .withConnectionToLocator(locatorPort));

    gfsh.connectAndVerify(locator);

  }

  @Before
  public void before() {
    String redisPort1 = "" + redisPorts[0];
    String redisPort2 = "" + redisPorts[1];
    String redisPort3 = "" + redisPorts[2];
    // For now only tell the client about redisPort1.
    // That server is never restarted so clients should
    // never fail due to the server they are connected to failing.

    connection = connectClientRedisServerOnPort(redisPort1);
    commands = connection.sync();
  }

  private StatefulRedisConnection connectClientRedisServerOnPort(String redisPort1) {
    DUnitSocketAddressResolver dnsResolver =
        new DUnitSocketAddressResolver(new String[]{redisPort1});

    ClientResources resources = ClientResources.builder()
        .socketAddressResolver(dnsResolver)
        .build();

    redisClient = RedisClient.create(resources, "redis://localhost");

    redisClient.setOptions(ClientOptions.builder()
        .autoReconnect(true)
        .build());

    return redisClient.connect();
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
  public void given_SecondaryServerCrashes_then_ClientConnectionToPrimaryShouldBeMaintained() {

    String key = "key";
    String value = "value";

    String primaryMemberForKey = getNameOfPrimaryServerForKey(key, value);

    int secondaryServerindex = primaryMemberForKey.equals("server-1") ? 2 : 1;

    String
        redisPortForPrimary =
        primaryMemberForKey.equals("server-1") ? "" + redisPorts[0] : "" + redisPorts[1];

    StatefulRedisConnection<String, String> connectionToClientOnPrimary =
        connectClientRedisServerOnPort(redisPortForPrimary);

    assert (connectionToClientOnPrimary.isOpen() == true);

    try {
      clusterStartUp.crashVM(secondaryServerindex);
    } catch (Exception e) {
    }

    assert (connectionToClientOnPrimary.isOpen() == true);
  }


  @Test
  public void given_SecondaryServerCrashesDuringOperation_then_ClientConnectionToPrimaryShouldBeLost() {

    String key = "key";
    String value = "value";

    String primaryMemberForKey = getNameOfPrimaryServerForKey(key, value);

    int secondaryServerindex = primaryMemberForKey.equals("server-1") ? 2 : 1;

    String
        redisPortForPrimary =
        primaryMemberForKey.equals("server-1") ? "" + redisPorts[0] : "" + redisPorts[1];

    int redisPortForSecondary =
        primaryMemberForKey.equals("server-1") ? redisPorts[1] : redisPorts[0];

    StatefulRedisConnection<String, String> connectionToClientOnPrimary =
        connectClientRedisServerOnPort(redisPortForPrimary);

    assert (connectionToClientOnPrimary.isOpen() == true);

    AtomicBoolean running1 = new AtomicBoolean(true);
    AtomicBoolean running2 = new AtomicBoolean(true);

    Runnable task1 = () -> renamePerformAndVerify(0, 20, running1);
    Runnable task2 = () -> renamePerformAndVerify(1, 20, running2);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);

    try {
      future1.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }

    try {
      future2.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }

    try {
      clusterStartUp.crashVM(secondaryServerindex);
      MemberVM newlyStartedServer = startRedisVM(secondaryServerindex, redisPortForSecondary);
      rebalanceAllRegions(newlyStartedServer);

    } catch (Exception e) {
    }

    running1.set(false);
    running2.set(false);

    try {
      future1.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    try {
      future2.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }

    assert (connectionToClientOnPrimary.isOpen() == false);
  }

  private String getNameOfPrimaryServerForKey(String key, String value) {

    return server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();

      final Region<ByteArrayWrapper, RedisData> region = cache.getRegion(REDIS_DATA_REGION);

      RedisString redisString = new RedisString();
      redisString.set(new ByteArrayWrapper(value.getBytes()));

      region.put(new ByteArrayWrapper(key.getBytes()), redisString);

      GeodeAwaitility.await()
          .until(() -> PartitionRegionHelper
              .getRedundantMembersForKey(
                  region,
                  new ByteArrayWrapper(key.getBytes()))
              .size() == 1);

//      rebalanceRegions(cache, region);

      return getPrimaryWhenAvailable(region, new ByteArrayWrapper(key.getBytes()));
    });

//    Region<String, String> region = cache.getRegion(REDIS_DATA_REGION);
//    region.put(key, value);
//
//    GeodeAwaitility.await()
//        .until(() -> PartitionRegionHelper.getRedundantMembersForKey(region, key).size() == 1);
//
////      rebalanceRegions(cache, region);
//
//    //TODO: does it matter that we're putting a non-redis-type into the redis region?
//    return getPrimaryWhenAvailable(region, key);
  }

  private static String getPrimaryWhenAvailable(Region<ByteArrayWrapper, RedisData> region,
                                                ByteArrayWrapper key) {

    AtomicReference<String> lastPrimary =
        new AtomicReference<>(
            PartitionRegionHelper.getPrimaryMemberForKey(region, key).getName());

    GeodeAwaitility.await()
        .during(10, TimeUnit.SECONDS)
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> {

          String currentPrimary =
              PartitionRegionHelper.getPrimaryMemberForKey(region, key).getName();

          return lastPrimary.getAndSet(currentPrimary).equals(currentPrimary);

        });

    return lastPrimary.get();
  }


  private void renamePerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    String newKey = null;
    String baseKey = "rename-key-" + index;
    commands.set(baseKey + "-0", "value");
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      String oldKey = baseKey + "-" + iterationCount;
      newKey = baseKey + "-" + (iterationCount + 1);
      try {
        commands.rename(oldKey, newKey);
        iterationCount += 1;
      } catch (RedisCommandExecutionException e) {
        if (e.getMessage().contains("memberDeparted")) {
          if (doWithRetry(() -> commands.exists(oldKey)) == 0) {
            iterationCount += 1;
          }
        } else if (e.getMessage().contains("no such key")) {
          iterationCount += 1;
        } else {
          throw e;
        }
      }
    }

    assertThat(commands.keys(baseKey + "-*").size()).isEqualTo(1);
    assertThat(commands.exists(newKey)).isEqualTo(1);

    logger.info("--->>> RENAME test ran {} iterations", iterationCount);
  }

  private void appendPerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    String key = "append-key-" + index;
    int iterationCount = 0;

    while (iterationCount < minimumIterations && isRunning.get()) {
      String appendString = "" + iterationCount % 2;
      try {
        commands.append(key, appendString);
        iterationCount += 1;
      } catch (RedisCommandExecutionException e) {

        //TODO member departed?  hmmm....
        if (e.getMessage().contains("memberDeparted")) {
          LogService.getLogger().info("loggin'");
          if (doWithRetry(() -> commands.get(key)).endsWith(appendString)) {
            iterationCount += 1;
          }
        } else {
          throw e;
        }
      }
    }

    String storedString = commands.get(key);
    for (int i = 0; i < iterationCount; i++) {
      String expectedValue = "" + i % 2;
      if (!expectedValue.equals("" + storedString.charAt(i))) {
        Assert.fail("unexpected " + storedString.charAt(i) + " at index " + i + " in string "
            + storedString);
        break;
      }
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

  private <T> T doWithRetry(Supplier<T> supplier) {
    while (true) {
      try {
        return supplier.get();
      } catch (RedisCommandExecutionException ex) {
        if (!ex.getMessage().contains("memberDeparted")) {
          throw ex;
        }
      }
    }
  }
}
