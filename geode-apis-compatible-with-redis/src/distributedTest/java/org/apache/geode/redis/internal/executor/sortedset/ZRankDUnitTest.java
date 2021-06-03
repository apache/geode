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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

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

public class ZRankDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final String KEY = "key";
  private static final int SET_SIZE = 1000;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

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
    server3.stop();
  }


  @Test
  public void shouldDistributeDataAmongCluster() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap("member1-", 0.0);

    jedis.zadd(KEY, memberScoreMap);

    for (String member : memberScoreMap.keySet()) {
      Long rank = jedis.zrank(KEY, member);
      assertThat(Double.valueOf(rank)).isEqualTo(memberScoreMap.get(member));
    }
  }


  @Test
  public void shouldDistributeDataAmongCluster_givenConcurrentlyAddingDifferentDataToSameSet() {
    Map<String, Double> memberScoreMap1 = makeMemberScoreMap("member1-", 0.0);
    Map<String, Double> memberScoreMap2 = makeMemberScoreMap("member2-", Double.valueOf(SET_SIZE));

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.zadd(KEY, memberScoreMap1),
        (i) -> jedis.zadd(KEY, memberScoreMap2)).runInLockstep();

    for (String member1 : memberScoreMap1.keySet()) {
      Long rank = jedis.zrank(KEY, member1);
      assertThat(Double.valueOf(rank)).isEqualTo(memberScoreMap1.get(member1));
    }
    for (String member2 : memberScoreMap2.keySet()) {
      Long rank = jedis.zrank(KEY, member2);
      assertThat(Double.valueOf(rank)).isEqualTo(memberScoreMap2.get(member2));
    }
  }


  @Test
  public void shouldDistributeDataAmongCluster_givenConcurrentlyAddingSameDataToSameSet() {
    Map<String, Double> memberScoreMap = makeMemberScoreMap("member1-", 0.0);

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.zadd(KEY, memberScoreMap),
        (i) -> jedis.zadd(KEY, memberScoreMap)).run();

    for (String member : memberScoreMap.keySet()) {
      Long rank = jedis.zrank(KEY, member);
      assertThat(Double.valueOf(rank)).isEqualTo(memberScoreMap.get(member));
    }
  }


  @Test
  public void shouldDistributeDataAmongCluster_givenConcurrentlyAddingDifferentSets() {
    String otherKey = "key2";

    Map<String, Double> memberScoreMap1 = makeMemberScoreMap("member1-", 0.0);
    Map<String, Double> memberScoreMap2 = makeMemberScoreMap("member2-", 0.0);

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.zadd(KEY, memberScoreMap1),
        (i) -> jedis.zadd(otherKey, memberScoreMap2)).runInLockstep();

    for (String member1 : memberScoreMap1.keySet()) {
      Long rank = jedis.zrank(KEY, member1);
      assertThat(Double.valueOf(rank)).isEqualTo(memberScoreMap1.get(member1));
    }
    for (String member2 : memberScoreMap2.keySet()) {
      Long rank = jedis.zrank(otherKey, member2);
      assertThat(Double.valueOf(rank)).isEqualTo(memberScoreMap2.get(member2));
    }

  }

  private Map<String, Double> makeMemberScoreMap(String baseString, Double baseScore) {
    Map<String, Double> scoreMemberPairs = new HashMap<>();
    for (int i = SET_SIZE - 1; i >= 0; i--) {
      scoreMemberPairs.put(baseString + i, Double.valueOf(baseScore + i + ""));
    }
    return scoreMemberPairs;
  }

}
