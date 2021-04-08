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
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class HsetDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int HASH_SIZE = 1000;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static Jedis jedis1;
  private static Jedis jedis2;
  private static Jedis jedis3;

  private static Properties locatorProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  private static int redisServerPort1;
  private static int redisServerPort2;
  private static int redisServerPort3;

  @BeforeClass
  public static void classSetup() {
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    redisServerPort1 = clusterStartUp.getRedisPort(1);
    redisServerPort2 = clusterStartUp.getRedisPort(2);
    redisServerPort3 = clusterStartUp.getRedisPort(3);

    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);
    jedis3 = new Jedis(LOCAL_HOST, redisServerPort3, JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    jedis1.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis1.disconnect();
    jedis2.disconnect();
    jedis3.disconnect();

    server1.stop();
    server2.stop();
    server3.stop();
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_givenMultipleClients() {

    String key = "key";

    Map<String, String> testMap = makeHashMap(HASH_SIZE, "field-", "value-");

    jedis1.hset(key, testMap);

    Map<String, String> result = jedis2.hgetAll(key);

    assertThat(result.keySet().toArray()).containsExactlyInAnyOrder(testMap.keySet().toArray());
    assertThat(result.values().toArray()).containsExactlyInAnyOrder(testMap.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_givenMultipleClients_addingDifferentDataToSameHashConcurrently() {

    String key = "key";

    Map<String, String> testMap1 = makeHashMap(HASH_SIZE, "field1-", "value1-");
    Map<String, String> testMap2 = makeHashMap(HASH_SIZE, "field2-", "value2-");

    Map<String, String> wholeMap = new HashMap<>();
    wholeMap.putAll(testMap1);
    wholeMap.putAll(testMap2);

    String[] testMap1Fields = testMap1.keySet().toArray(new String[] {});
    String[] testMap2Fields = testMap2.keySet().toArray(new String[] {});

    Consumer<Integer> hsetJedis1Consumer = makeHSetConsumer(testMap1, testMap1Fields, key, jedis1);
    Consumer<Integer> hsetJedis2Consumer = makeHSetConsumer(testMap2, testMap2Fields, key, jedis2);


    new ConcurrentLoopingThreads(HASH_SIZE, hsetJedis1Consumer, hsetJedis2Consumer).run();

    Map<String, String> results = jedis3.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(wholeMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(wholeMap.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_givenMultipleClients_addingSameDataToSameHashConcurrently() {

    String key = "key";

    Map<String, String> testMap = makeHashMap(HASH_SIZE, "field-", "value-");

    String[] testMapFields = testMap.keySet().toArray(new String[] {});

    Consumer<Integer> hsetJedis1Consumer = makeHSetConsumer(testMap, testMapFields, key, jedis1);
    Consumer<Integer> hsetJedis2Consumer = makeHSetConsumer(testMap, testMapFields, key, jedis2);

    new ConcurrentLoopingThreads(HASH_SIZE, hsetJedis1Consumer, hsetJedis2Consumer).run();

    Map<String, String> results = jedis3.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(testMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(testMap.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_givenMultipleClients_addingToDifferentHashesConcurrently() {

    String key1 = "key1";
    String key2 = "key2";

    Map<String, String> testMap1 = makeHashMap(HASH_SIZE, "field1-", "value1-");
    Map<String, String> testMap2 = makeHashMap(HASH_SIZE, "field2-", "value2-");

    String[] testMap1Fields = testMap1.keySet().toArray(new String[] {});
    String[] testMap2Fields = testMap2.keySet().toArray(new String[] {});

    Consumer<Integer> hsetJedis1Consumer = makeHSetConsumer(testMap1, testMap1Fields, key1, jedis1);
    Consumer<Integer> hsetJedis2Consumer = makeHSetConsumer(testMap2, testMap2Fields, key2, jedis2);

    new ConcurrentLoopingThreads(HASH_SIZE, hsetJedis1Consumer, hsetJedis2Consumer).run();

    Map<String, String> results1 = jedis3.hgetAll(key1);
    Map<String, String> results2 = jedis3.hgetAll(key2);

    assertThat(results1.keySet().toArray()).containsExactlyInAnyOrder(testMap1.keySet().toArray());
    assertThat(results1.values().toArray()).containsExactlyInAnyOrder(testMap1.values().toArray());
    assertThat(results2.values().toArray()).containsExactlyInAnyOrder(testMap2.values().toArray());
    assertThat(results2.values().toArray()).containsExactlyInAnyOrder(testMap2.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_givenMultipleClientsOnSameServer_addingSameDataToSameSetConcurrently() {

    Jedis jedis1B = new Jedis(LOCAL_HOST, redisServerPort1);
    Jedis jedis2B = new Jedis(LOCAL_HOST, redisServerPort2);

    String key = "key";

    Map<String, String> testMap = makeHashMap(HASH_SIZE, "field1-", "value1-");

    String[] testMapFields = testMap.keySet().toArray(new String[] {});

    Consumer<Integer> hsetJedis1Consumer = makeHSetConsumer(testMap, testMapFields, key, jedis1);
    Consumer<Integer> hsetJedis1BConsumer = makeHSetConsumer(testMap, testMapFields, key, jedis1B);
    Consumer<Integer> hsetJedis2Consumer = makeHSetConsumer(testMap, testMapFields, key, jedis2);
    Consumer<Integer> hsetJedis2BConsumer = makeHSetConsumer(testMap, testMapFields, key, jedis2B);

    new ConcurrentLoopingThreads(HASH_SIZE, hsetJedis1Consumer, hsetJedis1BConsumer,
        hsetJedis2Consumer, hsetJedis2BConsumer).run();

    Map<String, String> results = jedis3.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(testMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(testMap.values().toArray());

    jedis1B.disconnect();
    jedis2B.disconnect();
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_givenMultipleClientsOnSameServer_addingDifferentDataToSameSetConcurrently() {

    Jedis jedis1B = new Jedis(LOCAL_HOST, redisServerPort1);
    Jedis jedis2B = new Jedis(LOCAL_HOST, redisServerPort2);

    String key = "key1";

    Map<String, String> testMap1 = makeHashMap(HASH_SIZE, "field1-", "value1-");
    Map<String, String> testMap2 = makeHashMap(HASH_SIZE, "field2-", "value2-");

    Map<String, String> wholeMap = new HashMap<>();
    wholeMap.putAll(testMap1);
    wholeMap.putAll(testMap2);

    String[] testMap1Fields = testMap1.keySet().toArray(new String[] {});
    String[] testMap2Fields = testMap2.keySet().toArray(new String[] {});

    Consumer<Integer> consumer1 = makeHSetConsumer(testMap1, testMap1Fields, key, jedis1);
    Consumer<Integer> consumer1B = makeHSetConsumer(testMap1, testMap1Fields, key, jedis1B);
    Consumer<Integer> consumer2 = makeHSetConsumer(testMap2, testMap2Fields, key, jedis2);
    Consumer<Integer> consumer2B = makeHSetConsumer(testMap2, testMap2Fields, key, jedis2B);

    new ConcurrentLoopingThreads(HASH_SIZE, consumer1, consumer1B, consumer2, consumer2B).run();

    Map<String, String> results = jedis3.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(wholeMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(wholeMap.values().toArray());

    jedis1B.disconnect();
    jedis2B.disconnect();
  }

  private Consumer<Integer> makeHSetConsumer(Map<String, String> testMap, String[] fields,
      String hashKey, Jedis jedis) {
    Consumer<Integer> consumer = (i) -> {
      String field = fields[i];
      jedis.hset(hashKey, field, testMap.get(field));
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
