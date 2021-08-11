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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractZRemIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private final String baseName = "member_";

  private static final String SORTED_SET_KEY = "ss_key";
  private static final int INITIAL_MEMBER_COUNT = 5;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void zRemThrowsIfTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.ZREM, 2);
  }

  @Test
  public void zRemThrowsErrorIfKeyIsNotASortedSet() {
    String key = "key";
    String member = "member1";
    jedis.sadd(key, member);

    assertThatThrownBy(() -> jedis.zrem(key, member))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void zRemDoesNotRemoveNonExistingMember() {
    Map<String, Double> map = makeMemberScoreMap(INITIAL_MEMBER_COUNT);
    jedis.zadd(SORTED_SET_KEY, map);

    String nonExistingMember = "nonExisting";
    long result = jedis.zrem(SORTED_SET_KEY, nonExistingMember);

    assertThat(result).isEqualTo(0);
  }

  @Test
  public void zRemCanRemoveAMemberInASortedSet() {
    Map<String, Double> map = makeMemberScoreMap(INITIAL_MEMBER_COUNT);
    Set<String> keys = map.keySet();
    jedis.zadd(SORTED_SET_KEY, map);

    String memberToRemove = baseName + 1;
    Long removed = jedis.zrem(SORTED_SET_KEY, memberToRemove);
    assertThat(removed).isEqualTo(1);

    for (String member : keys) {
      Double score = jedis.zscore(SORTED_SET_KEY, member);
      if (member.equals(memberToRemove)) {
        assertThat(score).isNull();
      } else {
        assertThat(score).isNotNull();
      }
    }
    assertThat(jedis.exists(SORTED_SET_KEY)).isTrue();
  }

  @Test
  public void zRemRemovesKeyIfAllMembersInASortedSetAreRemoved() {
    Map<String, Double> map = makeMemberScoreMap(INITIAL_MEMBER_COUNT);
    Set<String> keys = map.keySet();
    jedis.zadd(SORTED_SET_KEY, map);

    String[] membersToRemove = new String[keys.size()];
    Long removed = jedis.zrem(SORTED_SET_KEY, keys.toArray(membersToRemove));
    assertThat(removed).isEqualTo(keys.size());

    for (String member : keys) {
      Double score = jedis.zscore(SORTED_SET_KEY, member);
      assertThat(score).isNull();
    }
    assertThat(jedis.exists(SORTED_SET_KEY)).isFalse();
  }

  @Test
  public void zRemCanRemoveMembersConcurrentlyInASortedSet() {
    int membersCount = 1000;
    Map<String, Double> map = makeMemberScoreMap(membersCount);
    jedis.zadd(SORTED_SET_KEY, map);

    AtomicInteger totalRemoved = new AtomicInteger();
    new ConcurrentLoopingThreads(membersCount,
        (i) -> doZRemOnMembers(i, totalRemoved),
        (i) -> doZRemOnMembersInDifferentOrder(i, membersCount, totalRemoved)).run();

    assertThat(totalRemoved.get()).isEqualTo(membersCount);
    assertThat(jedis.exists(SORTED_SET_KEY)).isFalse();
  }

  private void doZRemOnMembers(int i, AtomicInteger total) {
    long count = jedis.zrem(SORTED_SET_KEY, baseName + i);
    total.addAndGet((int) count);
  }

  private void doZRemOnMembersInDifferentOrder(int i, int numToRemove, AtomicInteger total) {
    long count = jedis.zrem(SORTED_SET_KEY, baseName + (numToRemove - i - 1));
    total.addAndGet((int) count);
  }

  private Map<String, Double> makeMemberScoreMap(int membersCount) {
    Map<String, Double> map = new HashMap<>();

    for (int i = 0; i < membersCount; i++) {
      map.put(baseName + i, (double) (i));
    }
    return map;
  }
}
