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

import java.util.Properties;
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
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.session.springRedisTestApplication.config.DUnitSocketAddressResolver;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class NonPrimaryMemberCrashDUnit {


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

//    String redisPort2 = redisPorts[1] + "";
//    server2 = clusterStartUp.startServerVM(2,
//        x -> x.withProperty(REDIS_PORT, redisPort2)
//            .withProperty(REDIS_ENABLED, "true")
//            .withProperty(REDIS_BIND_ADDRESS, "localhost")
//            .withSystemProperty(ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM, "true")
//            .withConnectionToLocator(locatorPort));
//
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
  public void given_SecondaryServerCrashesDuringOperation_then_ClientConnectionToPrimaryShouldBeLost()
      throws Exception {

    //primary server
    //secdonary server
    //clientconcected to primary
//    crash secondary dueing operation (how did they do that? )  X
      //expect client to be disocnnected

    AtomicBoolean running1 = new AtomicBoolean(true);
    AtomicBoolean running2 = new AtomicBoolean(true);
    AtomicBoolean running3 = new AtomicBoolean(true);
    AtomicBoolean running4 = new AtomicBoolean(false);

    Runnable task1 = null;
    Runnable task2 = null;
    Runnable task3 = null;
    Runnable task4 = null;


    task1 = () -> appendPerformAndVerify(0, 20000, running1);
    task2 = () -> appendPerformAndVerify(1, 20000, running2);
    task3 = () -> appendPerformAndVerify(3, 20000, running3);
    task4 = () -> appendPerformAndVerify(4, 1000, running4);

//    clusterStartUp.crashVM(1);

    assert(connection.isOpen()==true);

    try {
      clusterStartUp.crashVM(1);
    }
    catch (Exception e){}
    //    server2 = startRedisVM(2, redisPorts[1]);
//    rebalanceAllRegions(server2);

//    clusterStartUp.crashVM(3);
//    server3 = startRedisVM(3, redisPorts[2]);
//    rebalanceAllRegions(server3);

//    clusterStartUp.crashVM(2);
//    server2 = startRedisVM(2, redisPorts[1]);
//    rebalanceAllRegions(server2);

//    clusterStartUp.crashVM(3);
//    server3 = startRedisVM(3, redisPorts[2]);
    assert(connection.isOpen()==false);


  }


  private static String awaitForPrimary(Region<String, String> region) {

    AtomicReference<String> lastPrimary =
        new AtomicReference<>(PartitionRegionHelper.getPrimaryMemberForKey(region, KEY).getName());

    GeodeAwaitility.await()
        .during(10, TimeUnit.SECONDS)
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> {

          String currentPrimary =
              PartitionRegionHelper.getPrimaryMemberForKey(region, KEY).getName();

          return lastPrimary.getAndSet(currentPrimary).equals(currentPrimary);

        });

    return lastPrimary.get();
  }


  private void appendPerformAndVerify(int index, int minimumIterations, AtomicBoolean isRunning) {
    String key = "append-key-" + index;
    int iterationCount = 0;

    while (iterationCount < minimumIterations || isRunning.get()) {
      String appendString = "" + iterationCount % 2;
      try {
        commands.append(key, appendString);
        iterationCount += 1;
      } catch (RedisCommandExecutionException e) {

        //TODO member departed?  hmmm....
        if (e.getMessage().contains("memberDeparted")) {
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
