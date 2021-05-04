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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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

public class HExistsDUnitTest {

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

  private static int redisServerPort;

  @BeforeClass
  public static void classSetup() {
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());

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
  }


  @Test
  public void testConcurrentHExists_whileUpdatingValues() {
    String key = "key";

    Map<String, String> testMap = makeHashMap(HASH_SIZE, "field-", "value-");

    jedis.hset(key, testMap);

    new ConcurrentLoopingThreads(HASH_SIZE,
        (i) -> jedis.hset(key, "field-" + i, "changedValue-" + i),
        (i) -> assertThat(jedis.hexists(key, "field-" + i)).isTrue(),
        (i) -> assertThat(jedis.hexists(key, "field-" + i)).isTrue()).run();

    Map<String, String> expectedResult = makeHashMap(HASH_SIZE, "field-", "changedValue-");
    assertThat(jedis.hgetAll(key)).containsExactlyInAnyOrderEntriesOf(expectedResult);

  }


  @Test
  public void testConcurrentHExists_whileAddingValues() {
    String key = "key";

    Map<String, String> expectedValues = new HashMap<>();

    new ConcurrentLoopingThreads(HASH_SIZE,
        (i) -> {
          jedis.hset(key, "field-" + i, "value-" + i);
          expectedValues.put("field-" + i, "value-" + i);
        },
        (i) -> GeodeAwaitility.await().atMost(Duration.ofSeconds(60))
            .untilAsserted(() -> assertThat(jedis.hexists(key, "field-" + i)).isTrue()))
                .runInLockstep();

    assertThat(jedis.hgetAll(key)).containsExactlyInAnyOrderEntriesOf(expectedValues);

  }


  @Test
  public void testConcurrentHExists_whileDeletingValues() {
    String key = "key";

    Map<String, String> testMap = makeHashMap(HASH_SIZE, "field-", "value-");

    jedis.hset(key, testMap);

    new ConcurrentLoopingThreads(HASH_SIZE,
        (i) -> jedis.hdel(key, "field-" + i),
        (i) -> GeodeAwaitility.await().atMost(Duration.ofSeconds(60))
            .untilAsserted(() -> assertThat(jedis.hexists(key, "field-" + i)).isFalse()))
                .runInLockstep();

    assertThat(jedis.hgetAll(key)).isEmpty();

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
