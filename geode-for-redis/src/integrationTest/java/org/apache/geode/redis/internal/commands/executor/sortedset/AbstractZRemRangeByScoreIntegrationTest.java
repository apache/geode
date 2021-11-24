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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_MIN_MAX_NOT_A_FLOAT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractZRemRangeByScoreIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final String MEMBER_NAME = "member";
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
    assertAtLeastNArgs(jedis, Protocol.Command.ZREMRANGEBYSCORE, 3);
  }

  @Test
  public void shouldError_givenInvalidMinOrMax() {
    assertThatThrownBy(() -> jedis.zremrangeByScore("fakeKey", "notANumber", "1"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zremrangeByScore("fakeKey", "1", "notANumber"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zremrangeByScore("fakeKey", "notANumber", "notANumber"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zremrangeByScore("fakeKey", "((", "1"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zremrangeByScore("fakeKey", "1", "(("))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zremrangeByScore("fakeKey", "(a", "(b"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zremrangeByScore("fakeKey", "str", "1"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zremrangeByScore("fakeKey", "1", "str"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zremrangeByScore("fakeKey", "1", "NaN"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
  }

  @Test
  public void shouldReturnZero_givenNonExistentKey() {
    assertThat(jedis.zremrangeByScore("fakeKey", "-inf", "inf")).isZero();
  }

  @Test
  public void shouldReturnZero_givenMinGreaterThanMax() {
    jedis.zadd(KEY, 1, MEMBER_NAME);

    // Range +inf <= score <= -inf
    assertThat(jedis.zremrangeByScore(KEY, "+inf", "-inf")).isZero();
    assertThat(jedis.zscore(KEY, MEMBER_NAME)).isNotNull();
  }

  @Test
  public void shouldRemoveRange_givenRangeIncludingScore() {
    jedis.zadd(KEY, 1, MEMBER_NAME);

    // Range -inf <= score <= +inf
    assertThat(jedis.zremrangeByScore(KEY, "-inf", "inf")).isOne();
    assertThat(jedis.zscore(KEY, MEMBER_NAME)).isNull();
  }

  @Test
  public void shouldRemoveRangeThenReturnZero_givenRangeIncludingScoreAndMultipleRemoves() {
    jedis.zadd(KEY, 1, MEMBER_NAME);

    // Range -inf <= score <= +inf
    assertThat(jedis.zremrangeByScore(KEY, "-inf", "inf")).isOne();
    assertThat(jedis.zscore(KEY, MEMBER_NAME)).isNull();
    assertThat(jedis.zremrangeByScore(KEY, "-inf", "inf")).isZero();
    assertThat(jedis.zscore(KEY, MEMBER_NAME)).isNull();
  }

  @Test
  public void shouldReturnZero_givenRangeExcludingScore() {
    int score = 1;
    jedis.zadd(KEY, score, MEMBER_NAME);

    // Range 2 <= score <= 3
    assertThat(jedis.zremrangeByScore(KEY, score + 1, score + 2)).isZero();
    assertThat(jedis.zscore(KEY, MEMBER_NAME)).isNotNull();
  }

  @Test
  public void shouldRemoveRange_givenMinAndMaxEqualToScore() {
    int score = 1;
    jedis.zadd(KEY, score, MEMBER_NAME);

    // Range 1 <= score <= 1
    assertThat(jedis.zremrangeByScore(KEY, score, score)).isOne();
    assertThat(jedis.zscore(KEY, MEMBER_NAME)).isNull();
  }

  @Test
  public void shouldRemoveRange_givenMultipleMembersWithDifferentScores() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", -10.0);
    map.put("member2", 1.0);
    map.put("member3", 10.0);

    jedis.zadd(KEY, map);

    // Range -5 <= score <= 15
    assertThat(jedis.zremrangeByScore(KEY, "-5", "15")).isEqualTo(2);
    assertThat(jedis.zscore(KEY, "member1")).isNotNull();
    assertThat(jedis.zscore(KEY, "member2")).isNull();
    assertThat(jedis.zscore(KEY, "member3")).isNull();
  }

  @Test
  public void shouldRemoveRange_givenMultipleMembersWithTheSameScoreAndMinAndMaxEqualToScore() {
    Map<String, Double> map = new HashMap<>();
    double score = 1;
    map.put("member1", score);
    map.put("member2", score);
    map.put("member3", score);

    jedis.zadd(KEY, map);

    // Range 1 <= score <= 1
    assertThat(jedis.zremrangeByScore(KEY, score, score)).isEqualTo(3);
    assertThat(jedis.zscore(KEY, "member1")).isNull();
    assertThat(jedis.zscore(KEY, "member2")).isNull();
    assertThat(jedis.zscore(KEY, "member3")).isNull();
  }

  @Test
  public void shouldRemoveRange_basicExclusivity() {
    Map<String, Double> map = new HashMap<>();

    map.put("member0", 0.0);
    map.put("member1", 1.0);
    map.put("member2", 2.0);
    map.put("member3", 3.0);
    map.put("member4", 4.0);

    jedis.zadd(KEY, map);

    assertThat(jedis.zremrangeByScore(KEY, "(1.0", "(3.0")).isOne();
    assertThat(jedis.zscore(KEY, "member2")).isNull();

    assertThat(jedis.zremrangeByScore(KEY, "(1.0", "3.0")).isOne();
    assertThat(jedis.zscore(KEY, "member3")).isNull();

    assertThat(jedis.zremrangeByScore(KEY, "1.0", "(3.0")).isOne();
    assertThat(jedis.zscore(KEY, "member0")).isNotNull();
    assertThat(jedis.zscore(KEY, "member1")).isNull();
    assertThat(jedis.zscore(KEY, "member2")).isNull();
    assertThat(jedis.zscore(KEY, "member3")).isNull();
    assertThat(jedis.zscore(KEY, "member4")).isNotNull();
  }

  @Test
  public void shouldRemoveRange_givenExclusiveMin() {
    Map<String, Double> map = getExclusiveTestMap();

    jedis.zadd(KEY, map);

    // Range -inf < score <= +inf
    assertThat(jedis.zremrangeByScore(KEY, "(-inf", "+inf")).isEqualTo(2);
    assertThat(jedis.zscore(KEY, "member1")).isNotNull();
    assertThat(jedis.zscore(KEY, "member2")).isNull();
    assertThat(jedis.zscore(KEY, "member3")).isNull();
  }

  @Test
  public void shouldRemoveRange_givenExclusiveMax() {
    Map<String, Double> map = getExclusiveTestMap();

    jedis.zadd(KEY, map);

    // Range -inf <= score < +inf
    assertThat(jedis.zremrangeByScore(KEY, "-inf", "(+inf")).isEqualTo(2);
    assertThat(jedis.zscore(KEY, "member1")).isNull();
    assertThat(jedis.zscore(KEY, "member2")).isNull();
    assertThat(jedis.zscore(KEY, "member3")).isNotNull();
  }

  @Test
  public void shouldRemoveRange_givenExclusiveMinAndMax() {
    Map<String, Double> map = getExclusiveTestMap();

    jedis.zadd(KEY, map);

    // Range -inf < score < +inf
    assertThat(jedis.zremrangeByScore(KEY, "(-inf", "(+inf")).isOne();
    assertThat(jedis.zscore(KEY, "member1")).isNotNull();
    assertThat(jedis.zscore(KEY, "member2")).isNull();
    assertThat(jedis.zscore(KEY, "member3")).isNotNull();
  }

  private Map<String, Double> getExclusiveTestMap() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", Double.NEGATIVE_INFINITY);
    map.put("member2", 1.0);
    map.put("member3", Double.POSITIVE_INFINITY);
    return map;
  }

  @Test
  public void shouldReturnZero_givenExclusiveMinAndMaxEqualToScore() {
    double score = 1;
    jedis.zadd(KEY, score, MEMBER_NAME);

    String scoreExclusive = "(" + score;
    assertThat(jedis.zremrangeByScore(KEY, scoreExclusive, scoreExclusive)).isZero();
    assertThat(jedis.zscore(KEY, MEMBER_NAME)).isNotNull();
  }

  @Test
  // Using only "(" as either the min or the max is equivalent to "(0"
  public void shouldRemoveRange_givenLeftParenOnlyForMinOrMax() {
    Map<String, Double> map = new HashMap<>();

    map.put("slightlyLessThanZero", -0.01);
    map.put("zero", 0.0);
    map.put("slightlyMoreThanZero", 0.01);

    jedis.zadd(KEY, map);

    // Range 0 < score <= inf
    assertThat(jedis.zremrangeByScore(KEY, "(", "inf")).isOne();
    assertThat(jedis.zscore(KEY, "slightlyMoreThanZero")).isNull();

    // Range -inf <= score < 0
    assertThat(jedis.zremrangeByScore(KEY, "-inf", "(")).isOne();
    assertThat(jedis.zscore(KEY, "slightlyLessThanZero")).isNull();
    assertThat(jedis.zscore(KEY, "zero")).isNotNull();
  }
}
