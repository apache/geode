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

import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.ClientResources;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.session.springRedisTestApplication.config.DUnitSocketAddressResolver;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class HdelDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final int HASH_SIZE = 50000;
  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static int[] redisPorts;
  private static RedisCommands<String, String> lettuce;
  private static StatefulRedisConnection<String, String> connection;
  private static ClientResources resources;

  @BeforeClass
  public static void classSetup() {
    redisPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);

    String redisPort1 = "" + redisPorts[0];
    String redisPort2 = "" + redisPorts[1];

    locator = cluster.startLocatorVM(0);

    server1 = startRedisVM(1, redisPorts[0]);
    server2 = startRedisVM(2, redisPorts[1]);

    DUnitSocketAddressResolver dnsResolver =
        new DUnitSocketAddressResolver(new String[] {redisPort2, redisPort1});

    resources = ClientResources.builder()
        .socketAddressResolver(dnsResolver)
        .build();

    RedisClient redisClient = RedisClient.create(resources, "redis://localhost");
    redisClient.setOptions(ClientOptions.builder()
        .autoReconnect(true)
        .build());
    connection = redisClient.connect();
    lettuce = connection.sync();
  }

  private static MemberVM startRedisVM(int vmID, int redisPort) {
    int locatorPort = locator.getPort();

    return cluster.startRedisVM(vmID, x -> x
        .withConnectionToLocator(locatorPort)
        .withProperty(REDIS_PORT, "" + redisPort));
  }

  @Before
  public void testSetup() {
    cluster.flushAll();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    resources.shutdown().get();
    connection.close();
    server1.stop();
    server2.stop();
  }

  @Test
  public void testConcurrentHDel_returnsExpectedNumberOfDeletions() {
    AtomicLong client1Deletes = new AtomicLong();
    AtomicLong client2Deletes = new AtomicLong();

    String key = "HSET";

    Map<String, String> setUpData =
        makeHashMap(HASH_SIZE, "field", "value");

    lettuce.hset(key, setUpData);

    new ConcurrentLoopingThreads(HASH_SIZE,
        i -> {
          long deleted = lettuce.hdel(key, "field" + i, "value" + i);
          client1Deletes.addAndGet(deleted);
        },
        i -> {
          long deleted = lettuce.hdel(key, "field" + i, "value" + i);
          client2Deletes.addAndGet(deleted);
        })
            .run();

    assertThat(client1Deletes.get() + client2Deletes.get()).isEqualTo(HASH_SIZE);
    assertThat(lettuce.hgetall(key).size())
        .as("Not all keys were deleted")
        .isEqualTo(0);
  }

  @Test
  public void testConcurrentHdel_whenServerCrashesAndRestarts() {
    String key = "HSET";

    Map<String, String> setUpData =
        makeHashMap(HASH_SIZE, "field", "value");

    lettuce.hset(key, setUpData);

    ConcurrentLoopingThreads loopingThreads = new ConcurrentLoopingThreads(HASH_SIZE,
        i -> retryableCommand(() -> lettuce.hdel(key, "field" + i, "value" + i)),
        i -> retryableCommand(() -> lettuce.hdel(key, "field" + i, "value" + i)))
            .start();

    cluster.crashVM(2);
    server2 = startRedisVM(2, redisPorts[1]);

    loopingThreads.await();
    assertThat(lettuce.hgetall(key).size())
        .as("Not all keys were deleted")
        .isEqualTo(0);
  }

  private void retryableCommand(Runnable command) {
    while (true) {
      try {
        command.run();
        return;
      } catch (RedisException rex) {
        if (!rex.getMessage().contains("Connection reset by peer")) {
          throw rex;
        }
      }
    }
  }

  private Map<String, String> makeHashMap(int hashSize, String baseFieldName,
      String baseValueName) {
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < hashSize; i++) {
      map.put(baseFieldName + i, baseValueName + i);
    }
    return map;
  }
}
