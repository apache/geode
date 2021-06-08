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
import java.util.function.Supplier;

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

  private static final String KEY_BASE = "key";
  private static final int NUM_SORTED_SETS = 100;
  private static final String MEMBER_BASE = "member-";

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int SET_SIZE = 10;
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
  public void shouldDistributeDataAmongCluster_givenConcurrentlyAddingMultipleSets()
      throws Exception {
    Map<String, Double> memberScoreMap1 = makeMemberScoreMapSlice(MEMBER_BASE, 0, SET_SIZE / 2);
    Map<String, Double> memberScoreMap2 =
        makeMemberScoreMapSlice(MEMBER_BASE, SET_SIZE / 2, SET_SIZE / 2);

    new ConcurrentLoopingThreads(NUM_SORTED_SETS / 2,
        (i) -> jedis.zadd(KEY_BASE + i, memberScoreMap1),
        (i) -> jedis.zadd(KEY_BASE + i, memberScoreMap2),
        (i) -> jedis.zadd(KEY_BASE + (i + NUM_SORTED_SETS / 2), memberScoreMap1),
        (i) -> jedis.zadd(KEY_BASE + (i + NUM_SORTED_SETS / 2), memberScoreMap2)).runInLockstep();

    confirmAllDataIsPresent();

    clusterStartUp.crashVM(3);

    confirmAllDataIsPresent();
  }


  private void confirmAllDataIsPresent() throws Exception {
    for (int i = 0; i < NUM_SORTED_SETS; i++) {
      for (int j = 0; j < SET_SIZE; j++) {
        final int final_i = i;
        final int final_j = j;
        assertThat(
            redisCommandWithRetries(() -> jedis.zscore(KEY_BASE + final_i, MEMBER_BASE + final_j),
                10)).isEqualTo((double) final_j);
        assertThat(
            redisCommandWithRetries(() -> jedis.zrank(KEY_BASE + final_i, MEMBER_BASE + final_j),
                10)).isEqualTo((long) final_j);
      }
    }
  }

  private Number redisCommandWithRetries(Supplier<Number> supplier, int maxRetries)
      throws Exception {
    Exception lastException = null;
    for (int i = 0; i < maxRetries; i++) {
      try {
        return supplier.get();
      } catch (Exception e) {
        lastException = e;
      }
    }
    throw lastException;
  }

  private Map<String, Double> makeMemberScoreMapSlice(String baseString, int start, int count) {
    Map<String, Double> scoreMemberPairs = new HashMap<>();
    for (int i = 0; i < count; i++) {
      scoreMemberPairs.put(baseString + (i + start), Double.valueOf((i + start) + ""));
    }
    return scoreMemberPairs;
  }
}
