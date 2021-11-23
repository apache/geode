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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.redis.ConcurrentLoopingThreads;
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
  private static RedisAdvancedClusterCommands<String, String> lettuce;
  private static RedisClusterClient clusterClient;

  @BeforeClass
  public static void classSetup() {
    locator = cluster.startLocatorVM(0);

    cluster.startRedisVM(1, locator.getPort());
    cluster.startRedisVM(2, locator.getPort());

    int redisServerPort1 = cluster.getRedisPort(1);
    clusterClient = RedisClusterClient.create("redis://localhost:" + redisServerPort1);

    ClusterTopologyRefreshOptions refreshOptions =
        ClusterTopologyRefreshOptions.builder()
            .enableAllAdaptiveRefreshTriggers()
            .refreshTriggersReconnectAttempts(1)
            .build();

    clusterClient.setOptions(ClusterClientOptions.builder()
        .topologyRefreshOptions(refreshOptions)
        .validateClusterNodeMembership(false)
        .build());

    lettuce = clusterClient.connect().sync();
  }

  @AfterClass
  public static void cleanup() {
    try {
      clusterClient.shutdown();
    } catch (Exception ignored) {
      // https://github.com/lettuce-io/lettuce-core/issues/1800
    }
  }

  @Before
  public void testSetup() {
    cluster.flushAll();
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
    cluster.startRedisVM(2, locator.getPort());

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
