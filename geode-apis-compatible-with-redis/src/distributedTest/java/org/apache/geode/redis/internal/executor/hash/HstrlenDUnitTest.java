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

import java.util.Random;

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

public class HstrlenDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis;

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(1, locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());

    int redisServerPort = clusterStartUp.getRedisPort(1);

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
  }


  @Test
  public void hstrlenDoesNotCorruptData_whileHashIsConcurrentlyUpdated() {
    String key = "key";
    String field = "field";
    int iterations = 10000;
    Random rand = new Random();

    jedis.hset(key, field, "22");

    new ConcurrentLoopingThreads(iterations,
        (i) -> {
          int newLength = rand.nextInt(9) + 1;
          String newVal = makeStringOfRepeatedDigits(newLength);
          jedis.hset(key, field, newVal);
        },
        (i) -> assertThat(jedis.hstrlen(key, field)).isBetween(1L, 9L),
        (i) -> assertThat(jedis.hstrlen(key, field)).isBetween(1L, 9L))
            .run();

    String value = jedis.hget(key, field);
    String encodedStringLength = Character.toString(value.charAt(0));
    int expectedLength = Integer.parseInt(encodedStringLength);
    assertThat(jedis.hstrlen(key, field)).isEqualTo(expectedLength);
  }

  private String makeStringOfRepeatedDigits(int newLength) {
    StringBuilder stringOfRepeatedDigits = new StringBuilder();
    for (int i = 0; i < newLength; i++) {
      stringOfRepeatedDigits.append(newLength);
    }
    return stringOfRepeatedDigits.toString();
  }
}
