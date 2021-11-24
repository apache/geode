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
package org.apache.geode.redis.internal.commands.executor.sortedset;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractZRevRankIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final String MEMBER = "member";

  private JedisCluster jedis;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void shouldError_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.ZREVRANK, 2);
  }

  @Test
  public void shouldReturnNil_givenKeyDoesNotExist() {
    assertThat(jedis.zrevrank("fakeKey", "fakeMember")).isEqualTo(null);
  }

  @Test
  public void shouldReturnNil_givenMemberDoesNotExist() {
    jedis.zadd(KEY, 1.0, MEMBER);
    assertThat(jedis.zrevrank(KEY, "fakeMember")).isEqualTo(null);
  }

  @Test
  public void shouldReturnReverseRank_givenKeyAndMemberExist() {
    jedis.zadd(KEY, 1.0, MEMBER);
    assertThat(jedis.zrevrank(KEY, MEMBER)).isEqualTo(0);
  }

  @Test
  public void shouldReturnReverseRankByScore_givenUniqueScoresAndMembers() {
    Map<String, Double> membersMap = new HashMap<>();
    List<String> membersInRankOrder = new ArrayList<>();
    // Add members with scores in increasing rank order, including positive and negative infinity,
    // and keep track of the rank order in membersInRankOrder
    String firstMember = MEMBER + membersMap.size();
    membersMap.put(firstMember, Double.NEGATIVE_INFINITY);
    membersInRankOrder.add(firstMember);

    for (int i = membersMap.size(); i < 10; ++i) {
      String memberName = MEMBER + i;
      membersMap.put(memberName, (double) i);
      membersInRankOrder.add(memberName);
    }

    String finalMember = MEMBER + membersMap.size();
    membersMap.put(finalMember, Double.POSITIVE_INFINITY);
    membersInRankOrder.add(finalMember);

    jedis.zadd(KEY, membersMap);

    // Assert that revRank returns the expected rank
    for (int i = 0; i < membersInRankOrder.size(); i++) {
      String member = membersInRankOrder.get(i);
      int expectedRevRank = membersInRankOrder.size() - i - 1;
      assertThat(jedis.zrevrank(KEY, member)).isEqualTo(expectedRevRank);
    }
  }

  @Test
  public void shouldReturnReverseRankByLex_givenMembersWithSameScore() {
    String firstMember = "aaa";
    String secondMember = "bbb";
    String thirdMember = "ccc";

    double score = 1.0;
    jedis.zadd(KEY, score, firstMember);
    jedis.zadd(KEY, score, secondMember);
    jedis.zadd(KEY, score, thirdMember);

    assertThat(jedis.zrevrank(KEY, firstMember)).isEqualTo(2);
    assertThat(jedis.zrevrank(KEY, secondMember)).isEqualTo(1);
    assertThat(jedis.zrevrank(KEY, thirdMember)).isEqualTo(0);
  }

  @Test
  public void shouldUpdateRank_whenScoreChanges() {
    String firstMember = "first";
    String secondMember = "second";
    String thirdMember = "third";

    jedis.zadd(KEY, 1.0, firstMember);
    jedis.zadd(KEY, 2.0, secondMember);
    jedis.zadd(KEY, 3.0, thirdMember);

    assertThat(jedis.zrevrank(KEY, firstMember)).isEqualTo(2);
    assertThat(jedis.zrevrank(KEY, secondMember)).isEqualTo(1);
    assertThat(jedis.zrevrank(KEY, thirdMember)).isEqualTo(0);

    jedis.zadd(KEY, 4.0, firstMember);

    assertThat(jedis.zrevrank(KEY, firstMember)).isEqualTo(0);
    assertThat(jedis.zrevrank(KEY, secondMember)).isEqualTo(2);
    assertThat(jedis.zrevrank(KEY, thirdMember)).isEqualTo(1);
  }
}
