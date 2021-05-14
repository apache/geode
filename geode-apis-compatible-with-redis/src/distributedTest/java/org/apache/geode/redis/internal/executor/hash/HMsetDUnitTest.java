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

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class HMsetDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int HASH_SIZE = 1000;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis;

  private static Properties locatorProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  private static int redisServerPort;

  @BeforeClass
  public static void classSetup() {
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    redisServerPort = clusterStartUp.getRedisPort(1);

    jedis = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort), JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    try (Jedis conn = jedis.getConnectionFromSlot(0)) {
      conn.flushAll();
    }
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();

    server1.stop();
    server2.stop();
    server3.stop();
  }


  @Test
  public void shouldDistributeDataAmongCluster() {
    String key = "key";

    Map<String, String> testMap = makeHashMap(HASH_SIZE, "field-", "value-");

    jedis.hmset(key, testMap);

    Map<String, String> result = jedis.hgetAll(key);

    assertThat(result.keySet().toArray()).containsExactlyInAnyOrder(testMap.keySet().toArray());
    assertThat(result.values().toArray()).containsExactlyInAnyOrder(testMap.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenAddingDifferentDataToSameHashConcurrently() {

    String key = "key";

    Map<String, String> testMap1 = makeHashMap(HASH_SIZE, "field1-", "value1-");
    Map<String, String> testMap2 = makeHashMap(HASH_SIZE, "field2-", "value2-");

    Map<String, String> wholeMap = new HashMap<>();
    wholeMap.putAll(testMap1);
    wholeMap.putAll(testMap2);

    String[] testMap1Fields = testMap1.keySet().toArray(new String[] {});
    String[] testMap2Fields = testMap2.keySet().toArray(new String[] {});

    Consumer<Integer> consumer1 = makeHMSetConsumer(testMap1, testMap1Fields, key, jedis);
    Consumer<Integer> consumer2 = makeHMSetConsumer(testMap2, testMap2Fields, key, jedis);

    new ConcurrentLoopingThreads(HASH_SIZE, consumer1, consumer2).run();

    Map<String, String> results = jedis.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(wholeMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(wholeMap.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenAddingSameDataToSameHashConcurrently() {

    String key = "key";

    Map<String, String> testMap = makeHashMap(HASH_SIZE, "field-", "value-");

    String[] testMapFields = testMap.keySet().toArray(new String[] {});

    Consumer<Integer> consumer1 = makeHMSetConsumer(testMap, testMapFields, key, jedis);
    Consumer<Integer> consumer2 = makeHMSetConsumer(testMap, testMapFields, key, jedis);

    new ConcurrentLoopingThreads(HASH_SIZE, consumer1, consumer2).run();

    Map<String, String> results = jedis.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(testMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(testMap.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenAddingToDifferentHashesConcurrently() {

    String key1 = "key1";
    String key2 = "key2";

    Map<String, String> testMap1 = makeHashMap(HASH_SIZE, "field1-", "value1-");
    Map<String, String> testMap2 = makeHashMap(HASH_SIZE, "field2-", "value2-");

    String[] testMap1Fields = testMap1.keySet().toArray(new String[] {});
    String[] testMap2Fields = testMap2.keySet().toArray(new String[] {});

    Consumer<Integer> consumer1 = makeHMSetConsumer(testMap1, testMap1Fields, key1, jedis);
    Consumer<Integer> consumer2 = makeHMSetConsumer(testMap2, testMap2Fields, key2, jedis);

    new ConcurrentLoopingThreads(HASH_SIZE, consumer1, consumer2).run();

    Map<String, String> results1 = jedis.hgetAll(key1);
    Map<String, String> results2 = jedis.hgetAll(key2);

    assertThat(results1.keySet().toArray()).containsExactlyInAnyOrder(testMap1.keySet().toArray());
    assertThat(results1.values().toArray()).containsExactlyInAnyOrder(testMap1.values().toArray());
    assertThat(results2.values().toArray()).containsExactlyInAnyOrder(testMap2.values().toArray());
    assertThat(results2.values().toArray()).containsExactlyInAnyOrder(testMap2.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenMultipleThreadsAddingSameDataToSameSetConcurrently() {

    String key = "key";

    Map<String, String> testMap = makeHashMap(HASH_SIZE, "field1-", "value1-");

    String[] testMapFields = testMap.keySet().toArray(new String[] {});

    Consumer<Integer> consumer1 = makeHMSetConsumer(testMap, testMapFields, key, jedis);
    Consumer<Integer> consumer1B = makeHMSetConsumer(testMap, testMapFields, key, jedis);
    Consumer<Integer> consumer2 = makeHMSetConsumer(testMap, testMapFields, key, jedis);
    Consumer<Integer> consumer2B = makeHMSetConsumer(testMap, testMapFields, key, jedis);

    new ConcurrentLoopingThreads(HASH_SIZE, consumer1, consumer1B, consumer2, consumer2B).run();

    Map<String, String> results = jedis.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(testMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(testMap.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenMultipleThreadsAddingDifferentDataToSameSetConcurrently() {

    String key = "key1";

    Map<String, String> testMap1 = makeHashMap(HASH_SIZE, "field1-", "value1-");
    Map<String, String> testMap2 = makeHashMap(HASH_SIZE, "field2-", "value2-");

    Map<String, String> wholeMap = new HashMap<>();
    wholeMap.putAll(testMap1);
    wholeMap.putAll(testMap2);

    String[] testMap1Fields = testMap1.keySet().toArray(new String[] {});
    String[] testMap2Fields = testMap2.keySet().toArray(new String[] {});

    Consumer<Integer> consumer1 = makeHMSetConsumer(testMap1, testMap1Fields, key, jedis);
    Consumer<Integer> consumer1B = makeHMSetConsumer(testMap1, testMap1Fields, key, jedis);
    Consumer<Integer> consumer2 = makeHMSetConsumer(testMap2, testMap2Fields, key, jedis);
    Consumer<Integer> consumer2B = makeHMSetConsumer(testMap2, testMap2Fields, key, jedis);

    new ConcurrentLoopingThreads(HASH_SIZE, consumer1, consumer1B, consumer2, consumer2B).run();

    Map<String, String> results = jedis.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(wholeMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(wholeMap.values().toArray());
  }

  private Consumer<Integer> makeHMSetConsumer(Map<String, String> testMap, String[] fields,
      String hashKey, JedisCluster jedis) {
    Consumer<Integer> consumer = (i) -> {
      String field = fields[i];
      Map<String, String> mapToAdd = new HashMap<>();
      mapToAdd.put(field, testMap.get(field));
      jedis.hmset(hashKey, mapToAdd);
    };

    return consumer;
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
