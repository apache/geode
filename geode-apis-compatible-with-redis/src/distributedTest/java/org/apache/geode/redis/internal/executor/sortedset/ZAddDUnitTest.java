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

public class ZAddDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int SET_SIZE = 1000;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;
  private static int redisServerPort;

  @BeforeClass
  public static void classSetup() {
    Properties locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, DEFAULT_MAX_WAIT_TIME_RECONNECT);

    MemberVM locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    redisServerPort = clusterStartUp.getRedisPort(1);

    jedis = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort), JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    clusterStartUp.flushAll(redisServerPort);
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

    Map<String, Double> memberScoreMap = makeMemberScoreMap("member1-");

    jedis.zadd(key, memberScoreMap);

    for (String member : memberScoreMap.keySet()) {
      Double score = jedis.zscore(key, member);
      assertThat(score).isEqualTo(memberScoreMap.get(member));
    }
  }


  @Test
  public void shouldDistributeDataAmongCluster_givenConcurrentlyAddingDifferentDataToSameSet() {
    String key = "key";

    Map<String, Double> memberScoreMap1 = makeMemberScoreMap("member1-");
    Map<String, Double> memberScoreMap2 = makeMemberScoreMap("member2-");

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.zadd(key, memberScoreMap1),
        (i) -> jedis.zadd(key, memberScoreMap2)).runInLockstep();

    for (String member1 : memberScoreMap1.keySet()) {
      Double score = jedis.zscore(key, member1);
      assertThat(score).isEqualTo(memberScoreMap1.get(member1));
    }
    for (String member2 : memberScoreMap2.keySet()) {
      Double score = jedis.zscore(key, member2);
      assertThat(score).isEqualTo(memberScoreMap2.get(member2));
    }
  }


  @Test
  public void shouldDistributeDataAmongCluster_givenConcurrentlyAddingSameDataToSameSet() {
    String key = "key";

    Map<String, Double> memberScoreMap = makeMemberScoreMap("member1-");

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.zadd(key, memberScoreMap),
        (i) -> jedis.zadd(key, memberScoreMap)).run();

    for (String member : memberScoreMap.keySet()) {
      Double score = jedis.zscore(key, member);
      assertThat(score).isEqualTo(memberScoreMap.get(member));
    }
  }


  @Test
  public void shouldDistributeDataAmongCluster_givenConcurrentlyAddingDifferentSets() {

    String key1 = "key1";
    String key2 = "key2";

    Map<String, Double> memberScoreMap1 = makeMemberScoreMap("member1-");
    Map<String, Double> memberScoreMap2 = makeMemberScoreMap("member2-");

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.zadd(key1, memberScoreMap1),
        (i) -> jedis.zadd(key2, memberScoreMap2)).runInLockstep();

    for (String member1 : memberScoreMap1.keySet()) {
      Double score = jedis.zscore(key1, member1);
      assertThat(score).isEqualTo(memberScoreMap1.get(member1));
    }
    for (String member2 : memberScoreMap2.keySet()) {
      Double score = jedis.zscore(key2, member2);
      assertThat(score).isEqualTo(memberScoreMap2.get(member2));
    }

  }

  private Map<String, Double> makeMemberScoreMap(String baseString) {
    Map<String, Double> scoreMemberPairs = new HashMap<>();
    for (int i = 0; i < SET_SIZE; i++) {
      scoreMemberPairs.put(baseString + i, Double.valueOf(i + ""));
    }
    return scoreMemberPairs;
  }

}
