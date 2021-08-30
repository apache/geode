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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;

@RunWith(JUnitParamsRunner.class)
public abstract class AbstractZRemRangeByRankIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final int BASE_SCORE = 1;
  public static final String BASE_MEMBER_NAME = "member";

  JedisCluster jedis;

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
    assertExactNumberOfArgs(jedis, Protocol.Command.ZREMRANGEBYRANK, 3);
  }

  @Test
  @Parameters({"a", "--", "++", "4="})
  public void shouldError_givenInvalidStartOrStop(String invalidArgument) {
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREMRANGEBYRANK, KEY, "1", invalidArgument))
            .hasMessageContaining(ERROR_NOT_INTEGER);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREMRANGEBYRANK, KEY, invalidArgument, "5"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREMRANGEBYRANK, KEY,
        invalidArgument, invalidArgument))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldReturnZero_givenNonExistentKey() {
    jedis.zadd(KEY, BASE_SCORE, BASE_MEMBER_NAME);
    assertThat(jedis.zremrangeByRank("fakeKey", 0, 1)).isEqualTo(0);
  }

  @Test
  public void shouldReturnZero_givenMinGreaterThanMax() {
    jedis.zadd(KEY, BASE_SCORE, BASE_MEMBER_NAME);

    assertThat(jedis.zremrangeByRank(KEY, 1, 0)).isEqualTo(0);
  }

  @Test
  public void shouldRemoveMember_givenMemberRankInRange() {
    jedis.zadd(KEY, BASE_SCORE, BASE_MEMBER_NAME);

    assertThat(jedis.zremrangeByRank(KEY, 0, 0)).isEqualTo(1);
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME)).isNull();
  }

  @Test
  public void shouldReturnOne_givenMemberRankInRange() {
    jedis.zadd(KEY, BASE_SCORE, BASE_MEMBER_NAME + "0");
    jedis.zadd(KEY, BASE_SCORE + 1, BASE_MEMBER_NAME + "1");
    jedis.zadd(KEY, BASE_SCORE + 2, BASE_MEMBER_NAME + "2");

    assertThat(jedis.zremrangeByRank(KEY, 2, 2)).isEqualTo(1);
    assertThat(jedis.zremrangeByRank(KEY, 1, 1)).isEqualTo(1);
    assertThat(jedis.zremrangeByRank(KEY, 0, 0)).isEqualTo(1);
    assertThat(jedis.zcard(KEY)).isEqualTo(0);
  }

  @Test
  public void shouldReturnZero_givenRangeExcludingMember() {
    jedis.zadd(KEY, BASE_SCORE, BASE_MEMBER_NAME);

    assertThat(jedis.zremrangeByRank(KEY, 1, 2)).isEqualTo(0);
    assertThat(jedis.zcard(KEY)).isEqualTo(1);
  }

  @Test
  public void shouldRemoveMembers_givenMultipleMembersInRange() {
    populateSortedSet();

    assertThat(jedis.zcard(KEY)).isEqualTo(10);

    assertThat(jedis.zremrangeByRank(KEY, 8, 9)).isEqualTo(2);
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 8)).isNull();
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 9)).isNull();

    assertThat(jedis.zremrangeByRank(KEY, 4, 7)).isEqualTo(4);
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 4)).isNull();
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 5)).isNull();
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 6)).isNull();
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 7)).isNull();

    assertThat(jedis.zremrangeByRank(KEY, 0, 3)).isEqualTo(4);
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 0)).isNull();
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 1)).isNull();
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 2)).isNull();
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 3)).isNull();

    assertThat(jedis.zcard(KEY)).isEqualTo(0);
  }

  @Test
  public void shouldReturnAccurateCountOfRemovedMembers_givenRangePastEndOfSet() {
    populateSortedSet();

    assertThat(jedis.zcard(KEY)).isEqualTo(10);
    assertThat(jedis.zremrangeByRank(KEY, 8, 15)).isEqualTo(2);
    assertThat(jedis.zcard(KEY)).isEqualTo(8);
  }

  @Test
  public void shouldRemoveCorrectMembers_givenNegativeValues() {
    populateSortedSet();

    assertThat(jedis.zcard(KEY)).isEqualTo(10);
    assertThat(jedis.zremrangeByRank(KEY, -2, -1)).isEqualTo(2);
    assertThat(jedis.zcard(KEY)).isEqualTo(8);
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 8)).isNull();
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 9)).isNull();
  }

  @Test
  public void shouldRemoveCorrectMembers_givenNegativeOffsetBeforeInitialIndex() {
    populateSortedSet();

    assertThat(jedis.zremrangeByRank(KEY, -15, 0)).isEqualTo(1);
    assertThat(jedis.zcard(KEY)).isEqualTo(9);
    assertThat(jedis.zscore(KEY, BASE_MEMBER_NAME + 0)).isNull();
  }

  @Test
  public void shouldNotRemoveMembers_givenStartAfterFinalIndex_andNegativeRangeEnd() {
    populateSortedSet();

    assertThat(jedis.zremrangeByRank(KEY, 15, -1)).isEqualTo(0);
    assertThat(jedis.zcard(KEY)).isEqualTo(10);
  }

  @Test
  public void shouldNotRemoveMembers_givenEndBeforeInitialIndex() {
    populateSortedSet();

    assertThat(jedis.zremrangeByRank(KEY, 0, -15)).isEqualTo(0);
    assertThat(jedis.zcard(KEY)).isEqualTo(10);
  }

  @Test
  public void shouldRemoveCorrectMembers_givenRangePastFinalIndex() {
    populateSortedSet();

    assertThat(jedis.zremrangeByRank(KEY, 0, 15)).isEqualTo(10);
    assertThat(jedis.zcard(KEY)).isEqualTo(0);
  }

  @Test
  public void shouldDeleteSet_whenAllMembersDeleted() {
    populateSortedSet();

    assertThat(jedis.zremrangeByRank(KEY, 0, -1)).isEqualTo(10);
    assertThat(jedis.exists(KEY)).isFalse();
  }


  // Add 10 members with the different scores and member names
  private void populateSortedSet() {
    for (int i = 0; i < 10; ++i) {
      jedis.zadd(KEY, BASE_SCORE + i, BASE_MEMBER_NAME + i);
    }
  }
}
