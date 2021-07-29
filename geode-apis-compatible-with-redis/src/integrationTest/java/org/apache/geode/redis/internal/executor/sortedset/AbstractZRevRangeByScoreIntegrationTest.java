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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Tuple;

import org.apache.geode.redis.RedisIntegrationTest;

@RunWith(JUnitParamsRunner.class)
public abstract class AbstractZRevRangeByScoreIntegrationTest implements RedisIntegrationTest {
  private static final String MEMBER_BASE_NAME = "member";
  private static final String KEY = "key";
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
    assertAtLeastNArgs(jedis, Protocol.Command.ZREVRANGEBYSCORE, 3);
  }

  @Test
  @Parameters(method = "getInvalidRanges")
  @TestCaseName("{method}: max:{0}, min:{1}")
  public void shouldError_givenInvalidMinOrMax(String max, String min) {
    assertThatThrownBy(() -> jedis.zrevrangeByScore("fakeKey", max, min))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_FLOAT);
  }

  @Test
  public void shouldReturnSyntaxError_givenInvalidWithScoresFlag() {
    jedis.zadd(KEY, 1.0, MEMBER_BASE_NAME);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY, "1", "2", "WITSCOREZ"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void shouldReturnEmptyList_givenNonExistentKey() {
    assertThat(jedis.zrevrangeByScore("fakeKey", "-inf", "inf")).isEmpty();
  }

  @Test
  public void shouldReturnEmptyList_givenMaxLessThanMin() {
    jedis.zadd(KEY, 1, "member");

    // Range -inf >= score >= +inf
    assertThat(jedis.zrevrangeByScore(KEY, "-inf", "+inf")).isEmpty();
  }

  @Test
  public void shouldReturnElement_givenRangeIncludingScore() {
    jedis.zadd(KEY, 1, "member");

    // Range inf >= score >= -inf
    assertThat(jedis.zrevrangeByScore(KEY, "inf", "-inf"))
        .containsExactly("member");
  }

  @Test
  public void shouldReturnEmptyArray_givenRangeExcludingScore() {
    int score = 1;
    jedis.zadd(KEY, score, "member");

    // Range 2 <= score <= 3
    assertThat(jedis.zrevrangeByScore(KEY, score + 2, score + 1)).isEmpty();
  }

  @Test
  public void shouldReturnRange_givenMinAndMaxEqualToScore() {
    int score = 1;
    jedis.zadd(KEY, score, "member");

    // Range 1 <= score <= 1
    assertThat(jedis.zrevrangeByScore(KEY, score, score))
        .containsExactly("member");
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersWithDifferentScores() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", -10.0);
    map.put("member2", 1.0);
    map.put("member3", 10.0);

    jedis.zadd(KEY, map);

    // Range -5 <= score <= 15
    assertThat(jedis.zrevrangeByScore(KEY, "15", "-5"))
        .containsExactly("member3", "member2");
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersWithTheSameScoreAndMinAndMaxEqualToScore() {
    Map<String, Double> map = new HashMap<>();
    double score = 1;
    map.put("member1", score);
    map.put("member2", score);
    map.put("member3", score);

    jedis.zadd(KEY, map);

    // Range 1 <= score <= 1
    assertThat(jedis.zrevrangeByScore(KEY, score, score))
        .containsExactly("member3", "member2", "member1");
  }

  @Test
  public void shouldReturnRange_basicExclusivity() {
    Map<String, Double> map = new HashMap<>();

    map.put("member0", 0.0);
    map.put("member1", 1.0);
    map.put("member2", 2.0);
    map.put("member3", 3.0);
    map.put("member4", 4.0);

    jedis.zadd(KEY, map);

    assertThat(jedis.zrevrangeByScore(KEY, "(3.0", "(1.0"))
        .containsExactly("member2");
    assertThat(jedis.zrevrangeByScore(KEY, "(3.0", "1.0"))
        .containsExactly("member2", "member1");
    assertThat(jedis.zrevrangeByScore(KEY, "3.0", "(1.0"))
        .containsExactly("member3", "member2");
  }

  @Test
  public void shouldReturnRange_givenExclusiveMin() {
    Map<String, Double> map = getExclusiveTestMap();

    jedis.zadd(KEY, map);

    // Range +inf >= score > -inf
    assertThat(jedis.zrevrangeByScore(KEY, "+inf", "(-inf"))
        .containsExactly("member3", "member2");
  }

  @Test
  public void shouldReturnEmptyList_givenExclusiveMinAndMaxEqualToScore() {
    double score = 1;
    jedis.zadd(KEY, score, "member");

    String scoreExclusive = "(" + score;
    assertThat(jedis.zrevrangeByScore(KEY, scoreExclusive, scoreExclusive)).isEmpty();
  }

  @Test
  // Using only "(" as either the min or the max is equivalent to "(0"
  public void shouldReturnRange_givenLeftParenOnlyForMinOrMax() {
    Map<String, Double> map = new HashMap<>();

    map.put("slightlyLessThanZero", -0.01);
    map.put("zero", 0.0);
    map.put("slightlyMoreThanZero", 0.01);

    jedis.zadd(KEY, map);

    // Range inf >= score > 0
    assertThat(jedis.zrevrangeByScore(KEY, "inf", "(")).containsExactly("slightlyMoreThanZero");

    // Range 0 >= score > -inf
    assertThat(jedis.zrevrangeByScore(KEY, "(", "-inf")).containsExactly("slightlyLessThanZero");
  }

  @Test
  public void shouldReturnRange_boundedByLimit() {
    createZSetRangeTestMap();

    assertThat(jedis.zrevrangeByScore(KEY, "10", "0", 0, 2))
        .containsExactly("f", "e");
    assertThat(jedis.zrevrangeByScore(KEY, "10", "0", 2, 3))
        .containsExactly("d", "c", "b");
    assertThat(jedis.zrevrangeByScore(KEY, "10", "0", 2, 10))
        .containsExactly("d", "c", "b");
  }

  @Test
  public void shouldReturnEmptyList_givenOffsetGreaterThanReturnedCount() {
    createZSetRangeTestMap();

    assertThat(jedis.zrevrangeByScore(KEY, "10", "0", 20, 10)).isEmpty();
  }

  @Test
  public void shouldReturnRange_withScores_boundedByLimit() {
    createZSetRangeTestMap();

    Set<Tuple> firstExpected = new LinkedHashSet<>();
    firstExpected.add(new Tuple("f", 5d));
    firstExpected.add(new Tuple("e", 4d));

    Set<Tuple> secondExpected = new LinkedHashSet<>();
    secondExpected.add(new Tuple("d", 3d));
    secondExpected.add(new Tuple("c", 2d));
    secondExpected.add(new Tuple("b", 1d));

    assertThat(jedis.zrevrangeByScoreWithScores(KEY, "10", "0", 0, 0))
        .isEmpty();
    assertThat(jedis.zrevrangeByScoreWithScores(KEY, "10", "0", 0, 2))
        .containsExactlyElementsOf(firstExpected);
    assertThat(jedis.zrevrangeByScoreWithScores(KEY, "10", "0", 2, 3))
        .containsExactlyElementsOf(secondExpected);
    assertThat(jedis.zrevrangeByScoreWithScores(KEY, "10", "0", 2, 10))
        .containsExactlyElementsOf(secondExpected);
  }

  @Test
  public void shouldReturnFullRange_givenNegativeCount() {
    createZSetRangeTestMap();

    Set<Tuple> expected = new LinkedHashSet<>();
    expected.add(new Tuple("d", 3d));
    expected.add(new Tuple("c", 2d));
    expected.add(new Tuple("b", 1d));

    assertThat(jedis.zrevrangeByScoreWithScores(KEY, "10", "0", 2, -1))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnProperError_givenLimitWithWrongFormat() {
    createZSetRangeTestMap();

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY, "10", "0", "LIMIT"))
            .hasMessageContaining(ERROR_SYNTAX);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY, "10", "0", "LIMIT",
            "0"))
                .hasMessageContaining(ERROR_SYNTAX);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY, "10", "0", "LOMIT",
            "0", "1"))
                .hasMessageContaining(ERROR_SYNTAX);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY, "10", "0",
            "LIMIT", "0", "invalid"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldReturnProperError_givenMultipleLimitsIncludingWrongFormat() {
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY, "10", "0", "LIMIT",
            "0", "1", "LIMIT"))
                .hasMessageContaining(ERROR_SYNTAX);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY, "10", "0", "LIMIT",
            "0", "1", "WITHSCORES", "LIMIT"))
                .hasMessageContaining(ERROR_SYNTAX);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY, "10", "0", "LIMIT",
            "0", "invalid", "LIMIT", "0", "5"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY, "10", "0", "LIMIT",
            "0", "invalid", "WITHSCORES", "LIMIT", "0", "5"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldReturnRange_givenMultipleCopiesOfWithscoresAndOrLimit() {
    createZSetRangeTestMap();

    List<byte[]> expectedWithScores = new ArrayList<>();
    expectedWithScores.add("f".getBytes());
    expectedWithScores.add("5".getBytes());
    expectedWithScores.add("e".getBytes());
    expectedWithScores.add("4".getBytes());

    List<byte[]> expectedWithoutScores = new ArrayList<>();
    expectedWithoutScores.add("f".getBytes());
    expectedWithoutScores.add("e".getBytes());

    List<byte[]> result =
        uncheckedCast(jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY,
            "10", "0",
            "LIMIT", "0", "5",
            "LIMIT", "0", "2"));
    assertThat(result).containsExactlyElementsOf(expectedWithoutScores);
    result = uncheckedCast(jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY,
        "5", "4",
        "WITHSCORES",
        "WITHSCORES"));
    assertThat(result).containsExactlyElementsOf(expectedWithScores);
    result = uncheckedCast(jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYSCORE, KEY,
        "10", "0",
        "WITHSCORES",
        "LIMIT", "0", "5",
        "LIMIT", "0", "2",
        "WITHSCORES"));
    assertThat(result).containsExactlyElementsOf(expectedWithScores);
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

  private Map<String, Double> getExclusiveTestMap() {
    Map<String, Double> map = new HashMap<>();

    map.put("member1", Double.NEGATIVE_INFINITY);
    map.put("member2", 1.0);
    map.put("member3", Double.POSITIVE_INFINITY);
    return map;
  }

  @SuppressWarnings("unused")
  private Object[] getInvalidRanges() {
    return new Object[] {
        "notANumber, 1",
        "1, notANumber",
        "notANumber, notANumber",
        "((, 1",
        "1, ((",
        "(a, (b",
        "str, 1",
        "1, str",
        "1, NaN"
    };
  }
}
