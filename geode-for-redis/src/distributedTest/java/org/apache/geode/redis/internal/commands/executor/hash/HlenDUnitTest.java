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

public class HlenDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final int HASH_SIZE = 5000;
  private static final int BIG_HASH_SIZE = 50000;
  private static final int NUM_ITERATIONS = 50000;
  private static RedisAdvancedClusterCommands<String, String> lettuce;
  private static RedisClusterClient clusterClient;

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = cluster.startLocatorVM(0);

    cluster.startRedisVM(1, locator.getPort());
    cluster.startRedisVM(2, locator.getPort());

    int redisServerPort1 = cluster.getRedisPort(1);
    clusterClient = RedisClusterClient.create("redis://localhost:" + redisServerPort1);

    lettuce = clusterClient.connect().sync();
  }

  @AfterClass
  public static void cleanup() {
    clusterClient.shutdown();
  }

  @Before
  public void testSetup() {
    cluster.flushAll();
  }

  @Test
  public void testConcurrentHLens_returnExpectedLength() {
    AtomicLong client1Len = new AtomicLong();
    AtomicLong client2Len = new AtomicLong();

    String key = "HLEN";

    Map<String, String> setUpData = makeInitialHashMap(HASH_SIZE, "field", "value");

    lettuce.hset(key, setUpData);

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        i -> {
          long len = lettuce.hlen(key);
          client1Len.addAndGet(len);
        },
        i -> {
          long len = lettuce.hlen(key);
          client2Len.addAndGet(len);
        })
            .run();

    assertThat(client1Len.get() + client2Len.get()).isEqualTo(NUM_ITERATIONS * HASH_SIZE * 2);
  }

  @Test
  public void testConcurrentHLen_whileAddingFields() {
    String key = "HLEN";
    String storeKey = "storedLength";

    Map<String, String> setUpData =
        makeInitialHashMap(HASH_SIZE, "filler-", String.valueOf(HASH_SIZE));
    lettuce.hset(key, setUpData);
    lettuce.set(storeKey, String.valueOf(HASH_SIZE));

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        (i) -> {
          int newElementCount = i + 1; // convert index to a length
          int currentLength = HASH_SIZE + newElementCount;

          lettuce.hset(key, "field-" + currentLength, String.valueOf(currentLength));
          lettuce.set(storeKey, String.valueOf(currentLength));
        },
        (i) -> {
          long expectedLength = Long.parseLong(lettuce.get(storeKey));
          long actualLength = lettuce.hlen(key);

          assertThat(actualLength).isGreaterThanOrEqualTo(expectedLength);
        }).run();

    long finalActualLength = lettuce.hlen(key);
    long finalExpectedLength = Long.parseLong(lettuce.get(storeKey));
    assertThat(finalActualLength).isEqualTo(finalExpectedLength);
  }

  @Test
  public void testConcurrentHLen_whileDeletingFields() {
    String key = "HLEN";
    String storeKey = "storedLength";

    Map<String, String> setUpData =
        makeInitialHashMap(BIG_HASH_SIZE, "field-", String.valueOf(BIG_HASH_SIZE));
    lettuce.hset(key, setUpData);
    lettuce.set(storeKey, String.valueOf(BIG_HASH_SIZE));

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        (i) -> {
          int newLength = BIG_HASH_SIZE - i - 1;

          lettuce.hdel(key, "field-" + newLength);
          lettuce.set(storeKey, String.valueOf(newLength));
        },
        (i) -> {
          long expectedLength = Long.parseLong(lettuce.get(storeKey));
          long actualLength = lettuce.hlen(key);

          assertThat(actualLength).isLessThanOrEqualTo(expectedLength);
        }).run();

    assertThat(lettuce.hlen(key)).isEqualTo(0L);
  }

  private Map<String, String> makeInitialHashMap(int hashSize, String baseFieldName,
      String baseValueName) {
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < hashSize; i++) {
      map.put(baseFieldName + i, baseValueName + i);
    }
    return map;
  }
}
