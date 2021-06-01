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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class HKeysDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int HASH_SIZE = 10;
  private static final int NUM_ITERATIONS = 1000;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;


  @BeforeClass
  public static void classSetup() {
    locator = clusterStartUp.startLocatorVM(0);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());

    int redisServerPort = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort), JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();

    server1.stop();
    server2.stop();
    locator.stop();
  }

  @Test
  public void testConcurrentHKeys_whileAddingValues() {
    String key = "key";

    Map<String, String> testMap = makeHashMap(HASH_SIZE, "field-", "value-");
    Set<String> expectedFields = makeSet(HASH_SIZE, "field-");
    BlockingQueue<String> queue = new LinkedBlockingQueue<>();

    jedis.hset(key, testMap);

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        (i) -> {
          int newIndex = HASH_SIZE + i + 1;
          jedis.hset(key, "field-" + newIndex, "value-" + newIndex);
          queue.add("field-" + newIndex);
        },
        (i) -> {
          try {
            expectedFields.add(queue.take());
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          assertThat(jedis.hkeys(key)).containsAll(expectedFields);
        }).run();

    assertThat(jedis.hkeys(key)).containsExactlyInAnyOrderElementsOf(expectedFields);
  }

  @Test
  public void testConcurrentHKeys_whileDeletingValues() {
    String key = "key";
    Map<String, String> testMap = makeHashMap(NUM_ITERATIONS, "field-", "value-");

    jedis.hset(key, testMap);

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        (i) -> jedis.hdel(key, "field-" + i),
        (i) -> jedis.hkeys(key)).run();

    assertThat(jedis.hkeys(key).size()).isEqualTo(0);
  }

  @Test
  public void testConcurrentHKeys_whileUpdatingValues() {
    String key = "key";
    Map<String, String> testMap = makeHashMap(NUM_ITERATIONS, "field-", "value-");

    jedis.hset(key, testMap);

    new ConcurrentLoopingThreads(NUM_ITERATIONS,
        (i) -> {
          jedis.hset(key, "field-" + i, "changedValue-" + i);
          testMap.put("field-" + i, "changedValue-" + i);
        },
        (i) -> assertThat(jedis.hkeys(key)).containsExactlyInAnyOrderElementsOf(testMap.keySet()))
            .run();

    assertThat(jedis.hkeys(key)).containsExactlyInAnyOrderElementsOf(testMap.keySet());
    for (String field : testMap.keySet()) {
      assertThat(jedis.hget(key, field)).isEqualTo(testMap.get(field));
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

  private Set<String> makeSet(int hashSize, String baseFieldName) {
    Set<String> set = new HashSet<>(HASH_SIZE);
    for (int i = 0; i < hashSize; i++) {
      set.add(baseFieldName + i);
    }
    return set;
  }
}
