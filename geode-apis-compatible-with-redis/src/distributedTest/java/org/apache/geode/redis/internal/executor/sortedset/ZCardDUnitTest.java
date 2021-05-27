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
package org.apache.geode.redis.internal.executor.sortedset;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.DEFAULT_MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class ZCardDUnitTest {


  @ClassRule
  public static RedisClusterStartupRule clusterStartup = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  private static JedisCluster jedis;

  @BeforeClass
  public static void classSetup() {
    Properties locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, DEFAULT_MAX_WAIT_TIME_RECONNECT);

    MemberVM locator = clusterStartup.startLocatorVM(0, locatorProperties);
    int locatorPort = locator.getPort();

    clusterStartup.startRedisVM(1, locatorPort);
    clusterStartup.startRedisVM(2, locatorPort);
    clusterStartup.startRedisVM(3, locatorPort);

    int redisServerPort = clusterStartup.getRedisPort(1);

    jedis = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort), JEDIS_TIMEOUT);
  }

  @Before
  public void beforeTest() {
    try (Jedis conn = jedis.getConnectionFromSlot(0)) {
      conn.flushAll();
    }
  }

  @Test
  public void shouldReturnSizeOfManySets_OnDifferentServers() {
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      int setSize = 1 + random.nextInt(1000);
      String key = "key" + i;
      Map<String, Double> memberScoreMap = makeMemberScoreMap("member" + i, setSize);
      jedis.zadd(key, memberScoreMap);
      assertThat(jedis.zcard(key)).isEqualTo(setSize);
    }
  }

  private Map<String, Double> makeMemberScoreMap(String baseString, int setSize) {
    Map<String, Double> scoreMemberPairs = new HashMap<>();
    for (int i = 0; i < setSize; i++) {
      scoreMemberPairs.put(baseString + i, Double.valueOf(i + ""));
    }
    return scoreMemberPairs;
  }
}
