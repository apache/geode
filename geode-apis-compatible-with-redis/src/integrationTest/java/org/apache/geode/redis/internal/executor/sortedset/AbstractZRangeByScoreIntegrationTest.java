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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_MIN_MAX_NOT_A_FLOAT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Tuple;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractZRangeByScoreIntegrationTest implements RedisIntegrationTest {
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
    assertAtLeastNArgs(jedis, Protocol.Command.ZRANGEBYSCORE, 3);
  }

  @Test
  public void shouldError_givenInvalidMinOrMax() {
    assertThatThrownBy(() -> jedis.zrangeByScore("fakeKey", "notANumber", "1"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zrangeByScore("fakeKey", "1", "notANumber"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zrangeByScore("fakeKey", "notANumber", "notANumber"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zrangeByScore("fakeKey", "((", "1"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zrangeByScore("fakeKey", "1", "(("))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
    assertThatThrownBy(() -> jedis.zrangeByScore("fakeKey", "(a", "(b"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
  }

  @Test
  public void shouldReturnZero_givenNonExistentKey() {
    assertThat(jedis.zrangeByScore("fakeKey", "-inf", "inf")).isEmpty();
  }

  @Test
  public void shouldReturnZero_givenMinGreaterThanMax() {
    jedis.zadd(KEY, 1, "member");

    // Count +inf <= score <= -inf
    assertThat(jedis.zrangeByScore(KEY, "+inf", "-inf")).isEmpty();
  }

  @Test
  public void shouldReturnCount_givenRangeIncludingScore() {
    jedis.zadd(KEY, 1, "member");

    // Count -inf <= score <= +inf
    assertThat(jedis.zrangeByScore(KEY, "-inf", "inf"))
        .containsExactly("member");
  }

  @Test
  public void shouldReturnEmptyArray_givenRangeExcludingScore() {
    int score = 1;
    jedis.zadd(KEY, score, "member");

    // Count 2 <= score <= 3
    assertThat(jedis.zrangeByScore(KEY, score + 1, score + 2)).isEmpty();
  }

  @Test
  public void shouldReturnRange_givenMinAndMaxEqualToScore() {
    int score = 1;
    jedis.zadd(KEY, score, "member");

    // Count 1 <= score <= 1
    assertThat(jedis.zrangeByScore(KEY, score, score))
        .containsExactly("member");
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersWithDifferentScores() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", -10.0);
    map.put("member2", 1.0);
    map.put("member3", 10.0);

    jedis.zadd(KEY, map);

    // Count -5 <= score <= 15
    assertThat(jedis.zrangeByScore(KEY, "-5", "15"))
        .containsExactlyElementsOf(Arrays.asList("member2", "member3"));
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersWithTheSameScoreAndMinAndMaxEqualToScore() {
    Map<String, Double> map = new HashMap<>();
    double score = 1;
    map.put("member1", score);
    map.put("member2", score);
    map.put("member3", score);

    jedis.zadd(KEY, map);

    // Count 1 <= score <= 1
    assertThat(jedis.zrangeByScore(KEY, score, score))
        .containsExactlyInAnyOrderElementsOf(map.keySet());
  }

  @Test
  public void shouldReturnRange_givenExclusiveMin() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", Double.NEGATIVE_INFINITY);
    map.put("member2", 1.0);
    map.put("member3", Double.POSITIVE_INFINITY);

    jedis.zadd(KEY, map);

    // Count -inf < score <= +inf
    assertThat(jedis.zrangeByScore(KEY, "(-inf", "+inf"))
        .containsExactlyElementsOf(Arrays.asList("member2", "member3"));
  }

  @Test
  public void shouldReturnRange_givenExclusiveMax() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", Double.NEGATIVE_INFINITY);
    map.put("member2", 1.0);
    map.put("member3", Double.POSITIVE_INFINITY);

    jedis.zadd(KEY, map);

    // Count -inf <= score < +inf
    assertThat(jedis.zrangeByScore(KEY, "-inf", "(+inf"))
        .containsExactlyElementsOf(Arrays.asList("member1", "member2"));
  }

  @Test
  public void shouldReturnRange_givenExclusiveMinAndMax() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", Double.NEGATIVE_INFINITY);
    map.put("member2", 1.0);
    map.put("member3", Double.POSITIVE_INFINITY);

    jedis.zadd(KEY, map);

    // Count -inf < score < +inf
    assertThat(jedis.zrangeByScore(KEY, "(-inf", "(+inf")).containsExactly("member2");
  }

  @Test
  public void shouldReturnZero_givenExclusiveMinAndMaxEqualToScore() {
    double score = 1;
    jedis.zadd(KEY, score, "member");

    String scoreExclusive = "(" + score;
    assertThat(jedis.zrangeByScore(KEY, scoreExclusive, scoreExclusive)).isEmpty();
  }

  @Test
  // Using only "(" as either the min or the max is equivalent to "(0"
  public void shouldReturnRange_givenLeftParenOnlyForMinOrMax() {
    Map<String, Double> map = new HashMap<>();

    map.put("slightlyLessThanZero", -0.01);
    map.put("zero", 0.0);
    map.put("slightlyMoreThanZero", 0.01);

    jedis.zadd(KEY, map);

    // Count 0 < score <= inf
    assertThat(jedis.zrangeByScore(KEY, "(", "inf")).containsExactly("slightlyMoreThanZero");

    // Count -inf <= score < 0
    assertThat(jedis.zrangeByScore(KEY, "-inf", "(")).containsExactly("slightlyLessThanZero");
  }

  @Test
  public void shouldReturnRange_boundedByLimit() {
    createZSetRangeTestMap();

    assertThat(jedis.zrangeByScore(KEY, "0", "10", 0, 2))
        .containsExactlyElementsOf(Arrays.asList("b", "c"));
    assertThat(jedis.zrangeByScore(KEY, "0", "10", 2, 3))
        .containsExactlyElementsOf(Arrays.asList("d", "e", "f"));
    assertThat(jedis.zrangeByScore(KEY, "0", "10", 2, 10))
        .containsExactlyElementsOf(Arrays.asList("d", "e", "f"));
    assertThat(jedis.zrangeByScore(KEY, "0", "10", 20, 10)).isEmpty();
  }

  @Test
  public void shouldReturnRange_withScores_boundedByLimit() {
    createZSetRangeTestMap();

    Set<Tuple> firstExpected = new LinkedHashSet<>();
    firstExpected.add(new Tuple("b", 1d));
    firstExpected.add(new Tuple("c", 2d));

    Set<Tuple> secondExpected = new LinkedHashSet<>();
    secondExpected.add(new Tuple("d", 3d));
    secondExpected.add(new Tuple("e", 4d));
    secondExpected.add(new Tuple("f", 5d));

    assertThat(jedis.zrangeByScoreWithScores(KEY, "0", "10", 0, 0))
        .isEmpty();
    assertThat(jedis.zrangeByScoreWithScores(KEY, "0", "10", 0, 2))
        .containsExactlyElementsOf(firstExpected);
    assertThat(jedis.zrangeByScoreWithScores(KEY, "0", "10", 2, 3))
        .containsExactlyElementsOf(secondExpected);
    assertThat(jedis.zrangeByScoreWithScores(KEY, "0", "10", 2, 10))
        .containsExactlyElementsOf(secondExpected);
    assertThat(jedis.zrangeByScoreWithScores(KEY, "0", "10", 2, -1))
        .containsExactlyElementsOf(secondExpected);
  }

  private void createZSetRangeTestMap() {
    Map<String, Double> map = new HashMap<>();

    map.put("a", Double.NEGATIVE_INFINITY);
    map.put("b", 1d);
    map.put("c", 2d);
    map.put("d", 3d);
    map.put("e", 4d);
    map.put("f", 5d);
    map.put("g", Double.POSITIVE_INFINITY);

    jedis.zadd(KEY, map);
  }
}
