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

public abstract class AbstractZCountIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
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
    assertExactNumberOfArgs(jedis, Protocol.Command.ZCOUNT, 3);
  }

  @Test
  public void shouldError_givenInvalidMinOrMax() {
    assertThatThrownBy(() -> jedis.zcount("fakeKey", "notANumber", "1"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zcount("fakeKey", "1", "notANumber"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zcount("fakeKey", "notANumber", "notANumber"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zcount("fakeKey", "((", "1"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zcount("fakeKey", "1", "(("))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zcount("fakeKey", "(a", "(b"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
  }

  @Test
  public void shouldReturnZero_givenNonExistentKey() {
    assertThat(jedis.zcount("fakeKey", "-inf", "inf")).isEqualTo(0);
  }

  @Test
  public void shouldReturnZero_givenMinGreaterThanMax() {
    jedis.zadd(KEY, 1, "member");

    // Count +inf <= score <= -inf
    assertThat(jedis.zcount(KEY, "+inf", "-inf")).isEqualTo(0);
  }

  @Test
  public void shouldReturnCount_givenRangeIncludingScore() {
    jedis.zadd(KEY, 1, "member");

    // Count -inf <= score <= +inf
    assertThat(jedis.zcount(KEY, "-inf", "inf")).isEqualTo(1);
  }

  @Test
  public void shouldReturnZero_givenRangeExcludingScore() {
    int score = 1;
    jedis.zadd(KEY, score, "member");

    // Count 2 <= score <= 3
    assertThat(jedis.zcount(KEY, score + 1, score + 2)).isEqualTo(0);
  }

  @Test
  public void shouldReturnCount_givenMinAndMaxEqualToScore() {
    int score = 1;
    jedis.zadd(KEY, score, "member");

    // Count 1 <= score <= 1
    assertThat(jedis.zcount(KEY, score, score)).isEqualTo(1);
  }

  @Test
  public void shouldReturnCount_givenMultipleMembersWithDifferentScores() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", -10.0);
    map.put("member2", 1.0);
    map.put("member3", 10.0);

    jedis.zadd(KEY, map);

    // Count -5 <= score <= 15
    assertThat(jedis.zcount(KEY, "-5", "15")).isEqualTo(2);
  }

  @Test
  public void shouldReturnCount_givenMultipleMembersWithTheSameScoreAndMinAndMaxEqualToScore() {
    Map<String, Double> map = new HashMap<>();
    double score = 1;
    map.put("member1", score);
    map.put("member2", score);
    map.put("member3", score);

    jedis.zadd(KEY, map);

    // Count 1 <= score <= 1
    assertThat(jedis.zcount(KEY, score, score)).isEqualTo(map.size());
  }

  @Test
  public void shouldReturnCount_givenExclusiveMin() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", Double.NEGATIVE_INFINITY);
    map.put("member2", 1.0);
    map.put("member3", Double.POSITIVE_INFINITY);

    jedis.zadd(KEY, map);

    // Count -inf < score <= +inf
    assertThat(jedis.zcount(KEY, "(-inf", "+inf")).isEqualTo(2);
  }

  @Test
  public void shouldReturnCount_givenExclusiveMax() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", Double.NEGATIVE_INFINITY);
    map.put("member2", 1.0);
    map.put("member3", Double.POSITIVE_INFINITY);

    jedis.zadd(KEY, map);

    // Count -inf <= score < +inf
    assertThat(jedis.zcount(KEY, "-inf", "(+inf")).isEqualTo(2);
  }

  @Test
  public void shouldReturnCount_givenExclusiveMinAndMax() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", Double.NEGATIVE_INFINITY);
    map.put("member2", 1.0);
    map.put("member3", Double.POSITIVE_INFINITY);

    jedis.zadd(KEY, map);

    // Count -inf < score < +inf
    assertThat(jedis.zcount(KEY, "(-inf", "(+inf")).isEqualTo(1);
  }

  @Test
  public void shouldReturnZero_givenExclusiveMinAndMaxEqualToScore() {
    double score = 1;
    jedis.zadd(KEY, score, "member");

    String scoreExclusive = "(" + score;
    assertThat(jedis.zcount(KEY, scoreExclusive, scoreExclusive)).isEqualTo(0);
  }

  @Test
  // Using only "(" as either the min or the max is equivalent to "(0"
  public void shouldReturnCount_givenLeftParenOnlyForMinOrMax() {
    Map<String, Double> map = new HashMap<>();

    map.put("slightlyLessThanZero", -0.01);
    map.put("zero", 0.0);
    map.put("slightlyMoreThanZero", 0.01);

    jedis.zadd(KEY, map);

    // Count 0 < score <= inf
    assertThat(jedis.zcount(KEY, "(", "inf")).isEqualTo(1);

    // Count -inf <= score < 0
    assertThat(jedis.zcount(KEY, "-inf", "(")).isEqualTo(1);
  }
}
