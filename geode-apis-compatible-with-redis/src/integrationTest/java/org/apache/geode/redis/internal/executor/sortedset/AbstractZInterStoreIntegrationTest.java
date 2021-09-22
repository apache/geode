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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;

import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;

public abstract class AbstractZInterStoreIntegrationTest implements RedisIntegrationTest {

  private static final String NEW_SET = "{user1}new";
  private static final String KEY1 = "{user1}sset1";
  private static final String KEY2 = "{user1}sset2";
  private static final String KEY3 = "{user1}sset3";

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
  public void shouldError_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.ZINTERSTORE, 3);
  }

  @Test
  public void shouldError_givenWrongKeyType() {
    final String STRING_KEY = "{user1}stringKey";
    jedis.set(STRING_KEY, "value");
    assertThatThrownBy(() -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "2",
        STRING_KEY, KEY1)).hasMessage("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void shouldError_givenSetsCrossSlots() {
    final String WRONG_KEY = "{user2}another";
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "2", WRONG_KEY,
            KEY1)).hasMessage("CROSSSLOT " + RedisConstants.ERROR_WRONG_SLOT);
  }

  @Test
  public void shouldError_givenNumkeysTooLarge() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "2", KEY1))
            .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenNumkeysTooSmall() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "1", KEY1, KEY2))
            .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenTooManyWeights() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "1",
            KEY1, "WEIGHTS", "2", "3")).hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenTooFewWeights() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "2",
            KEY1, KEY2, "WEIGHTS", "1")).hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenWeightNotAFloat() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "1",
            KEY1, "WEIGHTS", "not-a-number"))
                .hasMessage("ERR " + RedisConstants.ERROR_WEIGHT_NOT_A_FLOAT);
  }

  @Test
  public void shouldError_givenWeightsWithoutAnyValues() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "1",
            KEY1, "WEIGHTS")).hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenMultipleWeightKeywords() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "1",
            KEY1, "WEIGHT", "1.0", "WEIGHT", "2.0"))
                .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenUnknownAggregate() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "1",
            KEY1, "AGGREGATE", "UNKNOWN", "WEIGHTS", "1"))
                .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenAggregateKeywordWithoutValue() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "1",
            KEY1, "AGGREGATE")).hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenMultipleAggregates() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "1",
            KEY1, "WEIGHTS", "1", "AGGREGATE", "SUM", "MIN"))
                .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldStoreIntersection_givenWeightOfOne_andOneRedisSortedSet() {
    Map<String, Double> scores = buildMapOfMembersAndScores();
    Set<Tuple> expectedResults = convertToTuples(scores, (ignore, value) -> value);
    jedis.zadd(KEY1, scores);

    assertThat(jedis.zinterstore(NEW_SET, new ZParams().weights(1), KEY1))
        .isEqualTo(expectedResults.size());

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
  }

  @Test
  public void shouldStoreIntersection_givenWeightOfZero_andOneRedisSortedSet() {
    Map<String, Double> scores = buildMapOfMembersAndScores();
    Set<Tuple> expectedResults = convertToTuples(scores, (ignore, value) -> 0D);
    jedis.zadd(KEY1, scores);

    assertThat(jedis.zinterstore(NEW_SET, new ZParams().weights(0), KEY1))
        .isEqualTo(scores.size());

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
  }

  @Test
  public void shouldStoreIntersection_givenWeightOfPositiveInfinity_andOneRedisSortedSet() {
    Map<String, Double> scores = buildMapOfMembersAndScores();
    Set<Tuple> expectedResults =
        convertToTuples(scores, (ignore, value) -> value > 0 ? Double.POSITIVE_INFINITY : value);
    jedis.zadd(KEY1, scores);

    assertThat(jedis.zinterstore(NEW_SET, new ZParams().weights(Double.POSITIVE_INFINITY),
        KEY1)).isEqualTo(scores.size());

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
  }

  @Test
  public void shouldStoreIntersection_givenWeightOfNegativeInfinity_andOneRedisSortedSet() {
    Map<String, Double> scores = buildMapOfMembersAndScores();
    jedis.zadd(KEY1, scores);

    Set<Tuple> expectedResults = new LinkedHashSet<>();
    expectedResults.add(new Tuple("player1", Double.POSITIVE_INFINITY));
    expectedResults.add(new Tuple("player2", 0D));
    expectedResults.add(new Tuple("player3", Double.NEGATIVE_INFINITY));
    expectedResults.add(new Tuple("player4", Double.NEGATIVE_INFINITY));
    expectedResults.add(new Tuple("player5", Double.NEGATIVE_INFINITY));

    assertThat(jedis.zinterstore(NEW_SET, new ZParams().weights(Double.NEGATIVE_INFINITY),
        KEY1)).isEqualTo(scores.size());

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
  }

  @Test
  public void shouldStoreIntersection_givenWeightOfN_andOneRedisSortedSet() {
    Map<String, Double> scores = buildMapOfMembersAndScores();
    jedis.zadd(KEY1, scores);

    double multiplier = 2.71D;

    Set<Tuple> expectedResults = new LinkedHashSet<>();
    expectedResults.add(new Tuple("player1", Double.NEGATIVE_INFINITY));
    expectedResults.add(new Tuple("player2", 0D));
    expectedResults.add(new Tuple("player3", multiplier));
    expectedResults.add(new Tuple("player4", Double.POSITIVE_INFINITY));
    expectedResults.add(new Tuple("player5", 3.2D * multiplier));

    assertThat(jedis.zinterstore(NEW_SET, new ZParams().weights(multiplier), KEY1))
        .isEqualTo(scores.size());

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
  }

  @Test
  public void shouldStoreIntersection_givenMultipleRedisSortedSets() {
    Map<String, Double> scores = buildMapOfMembersAndScores();
    Set<Tuple> expectedResults = new LinkedHashSet<>();
    expectedResults.add(new Tuple("player1", Double.NEGATIVE_INFINITY));
    expectedResults.add(new Tuple("player2", 0D));
    expectedResults.add(new Tuple("player3", 2D));
    expectedResults.add(new Tuple("player4", Double.POSITIVE_INFINITY));
    expectedResults.add(new Tuple("player5", 3.2D * 2));

    jedis.zadd(KEY1, scores);
    jedis.zadd(KEY2, scores);

    assertThat(jedis.zinterstore(NEW_SET, new ZParams(), KEY1, KEY2))
        .isEqualTo(expectedResults.size());

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
  }

  @Test
  public void shouldStoreIntersection_givenTwoRedisSortedSets_withDifferentWeights() {
    Map<String, Double> scores = buildMapOfMembersAndScores();
    Set<Tuple> expectedResults = new LinkedHashSet<>();
    expectedResults.add(new Tuple("player1", Double.NEGATIVE_INFINITY));
    expectedResults.add(new Tuple("player2", 0D));
    expectedResults.add(new Tuple("player3", 3D));
    expectedResults.add(new Tuple("player4", Double.POSITIVE_INFINITY));
    expectedResults.add(new Tuple("player5", 3.2D * 3));

    jedis.zadd(KEY1, scores);
    jedis.zadd(KEY2, scores);

    assertThat(jedis.zinterstore(NEW_SET, new ZParams().weights(1, 2), KEY1, KEY2))
        .isEqualTo(expectedResults.size());

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
  }

  @Test
  public void shouldStoreIntersection_givenMultipleIdenticalRedisSortedSets_withDifferentPositiveWeights() {
    Map<String, Double> scores = buildMapOfMembersAndScores();
    Set<Tuple> expectedResults = new LinkedHashSet<>();
    expectedResults.add(new Tuple("player1", Double.NEGATIVE_INFINITY));
    expectedResults.add(new Tuple("player2", 0D));
    expectedResults.add(new Tuple("player3", 4.5D));
    expectedResults.add(new Tuple("player4", Double.POSITIVE_INFINITY));
    expectedResults.add(new Tuple("player5", 3.2D * 4.5D));

    jedis.zadd(KEY1, scores);
    jedis.zadd(KEY2, scores);
    jedis.zadd(KEY3, scores);

    assertThat(jedis.zinterstore(NEW_SET, new ZParams().weights(1D, 2D, 1.5D), KEY1, KEY2, KEY3))
        .isEqualTo(expectedResults.size());

    Set<Tuple> actualResults = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThatActualScoresAreVeryCloseToExpectedScores(expectedResults, actualResults);
  }

  @Test
  public void shouldStoreIntersection_givenOneSetDoesNotExist() {
    Map<String, Double> scores = buildMapOfMembersAndScores(1, 10);
    Set<Tuple> expectedResults = convertToTuples(scores, (i, x) -> x);
    jedis.zadd(KEY1, scores);

    assertThat(jedis.zinterstore(NEW_SET, KEY1, KEY2)).isEqualTo(scores.size());

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expectedResults, results);
  }

  @Test
  public void shouldStoreNothingAtDestinationKey_givenTwoNonIntersectingSets() {
    Map<String, Double> scores = buildMapOfMembersAndScores(1, 5);
    Map<String, Double> nonIntersectionScores = buildMapOfMembersAndScores(6, 10);
    jedis.zadd(KEY1, scores);
    jedis.zadd(KEY2, nonIntersectionScores);

    assertThat(jedis.zinterstore(NEW_SET, KEY1, KEY2)).isZero();

    assertThat(jedis.zrangeWithScores(NEW_SET, 0, 10)).isEmpty();
  }

  @Test
  public void shouldStoreSumOfIntersection_givenThreePartiallyOverlappingSets() {
    Map<String, Double> scores1 = buildMapOfMembersAndScores(1, 10);
    Map<String, Double> scores2 = buildMapOfMembersAndScores(6, 13);
    Map<String, Double> scores3 = buildMapOfMembersAndScores(4, 11);

    jedis.zadd(KEY1, scores1);
    jedis.zadd(KEY2, scores2);
    jedis.zadd(KEY3, scores3);

    assertThat(jedis.zinterstore(NEW_SET, new ZParams().aggregate(ZParams.Aggregate.SUM),
        KEY1, KEY2, KEY3)).isEqualTo(5);

    Set<Tuple> expected = new HashSet<>();
    for (int i = 6; i <= 10; i++) {
      expected.add(tupleSumOfScores("player" + i, scores1, scores2, scores3));
    }

    Set<Tuple> actual = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expected, actual);
  }

  @Test
  public void shouldStoreMaxOfIntersection_givenThreePartiallyOverlappingSets() {
    Map<String, Double> scores1 = buildMapOfMembersAndScores(1, 10);
    Map<String, Double> scores2 = buildMapOfMembersAndScores(6, 13);
    Map<String, Double> scores3 = buildMapOfMembersAndScores(4, 11);

    jedis.zadd(KEY1, scores1);
    jedis.zadd(KEY2, scores2);
    jedis.zadd(KEY3, scores3);

    assertThat(jedis.zinterstore(NEW_SET, new ZParams().aggregate(ZParams.Aggregate.MAX),
        KEY1, KEY2, KEY3)).isEqualTo(5);

    Set<Tuple> expected = new HashSet<>();
    for (int i = 6; i <= 10; i++) {
      expected.add(tupleMaxOfScores("player" + i, scores1, scores2, scores3));
    }

    Set<Tuple> actual = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expected, actual);
  }

  @Test
  public void shouldStoreMinOfIntersection_givenThreePartiallyOverlappingSets() {
    Map<String, Double> scores1 = buildMapOfMembersAndScores(1, 10);
    Map<String, Double> scores2 = buildMapOfMembersAndScores(6, 13);
    Map<String, Double> scores3 = buildMapOfMembersAndScores(4, 11);

    jedis.zadd(KEY1, scores1);
    jedis.zadd(KEY2, scores2);
    jedis.zadd(KEY3, scores3);

    assertThat(jedis.zinterstore(NEW_SET, new ZParams().aggregate(ZParams.Aggregate.MIN),
        KEY1, KEY2, KEY3)).isEqualTo(5);

    Set<Tuple> expected = new HashSet<>();
    for (int i = 6; i <= 10; i++) {
      expected.add(tupleMinOfScores("player" + i, scores1, scores2, scores3));
    }

    Set<Tuple> actual = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expected, actual);
  }

  @Test
  public void shouldStoreSumOfIntersection_givenThreePartiallyOverlappingSets_andWeights() {
    double weight1 = 0D;
    double weight2 = 42D;
    double weight3 = -7.3D;

    Map<String, Double> scores1 = buildMapOfMembersAndScores(1, 10);
    Map<String, Double> scores2 = buildMapOfMembersAndScores(6, 13);
    Map<String, Double> scores3 = buildMapOfMembersAndScores(4, 11);

    jedis.zadd(KEY1, scores1);
    jedis.zadd(KEY2, scores2);
    jedis.zadd(KEY3, scores3);

    ZParams zParams = new ZParams().aggregate(ZParams.Aggregate.SUM)
        .weights(weight1, weight2, weight3);

    assertThat(jedis.zinterstore(NEW_SET, zParams, KEY1, KEY2, KEY3)).isEqualTo(5);

    Set<Tuple> expected = new HashSet<>();
    for (int i = 6; i <= 10; i++) {
      expected.add(tupleSumOfScoresWithWeights("player" + i, scores1, scores2, scores3, weight1,
          weight2, weight3));
    }

    Set<Tuple> actual = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expected, actual);
  }

  @Test
  public void shouldStoreMaxOfIntersection_givenThreePartiallyOverlappingSets_andWeights() {
    double weight1 = 0D;
    double weight2 = 42D;
    double weight3 = -7.3D;

    Map<String, Double> scores1 = buildMapOfMembersAndScores(1, 10);
    Map<String, Double> scores2 = buildMapOfMembersAndScores(6, 13);
    Map<String, Double> scores3 = buildMapOfMembersAndScores(4, 11);

    jedis.zadd(KEY1, scores1);
    jedis.zadd(KEY2, scores2);
    jedis.zadd(KEY3, scores3);

    ZParams zParams = new ZParams().aggregate(ZParams.Aggregate.MAX)
        .weights(weight1, weight2, weight3);

    assertThat(jedis.zinterstore(NEW_SET, zParams, KEY1, KEY2, KEY3)).isEqualTo(5);

    Set<Tuple> expected = new HashSet<>();
    for (int i = 6; i <= 10; i++) {
      expected.add(tupleMaxOfScoresWithWeights("player" + i, scores1, scores2, scores3, weight1,
          weight2, weight3));
    }

    Set<Tuple> actual = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expected, actual);
  }

  @Test
  public void shouldStoreMinOfIntersection_givenThreePartiallyOverlappingSets_andWeights() {
    double weight1 = 0D;
    double weight2 = 42D;
    double weight3 = -7.3D;

    Map<String, Double> scores1 = buildMapOfMembersAndScores(1, 10);
    Map<String, Double> scores2 = buildMapOfMembersAndScores(6, 13);
    Map<String, Double> scores3 = buildMapOfMembersAndScores(4, 11);

    jedis.zadd(KEY1, scores1);
    jedis.zadd(KEY2, scores2);
    jedis.zadd(KEY3, scores3);

    ZParams zParams = new ZParams().aggregate(ZParams.Aggregate.MIN)
        .weights(weight1, weight2, weight3);

    assertThat(jedis.zinterstore(NEW_SET, zParams, KEY1, KEY2, KEY3)).isEqualTo(5);

    Set<Tuple> expected = new HashSet<>();
    for (int i = 6; i <= 10; i++) {
      expected.add(tupleMinOfScoresWithWeights("player" + i, scores1, scores2, scores3, weight1,
          weight2, weight3));
    }

    Set<Tuple> actual = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expected, actual);
  }

  @Test
  public void shouldStoreMaxOfIntersection_givenThreePartiallyOverlappingSetsWithIdenticalScores() {
    double score = 3.141592;
    Map<String, Double> scores1 = buildMapOfMembersAndIdenticalScores(1, 10, score);
    Map<String, Double> scores2 = buildMapOfMembersAndIdenticalScores(6, 13, score);
    Map<String, Double> scores3 = buildMapOfMembersAndIdenticalScores(4, 11, score);

    jedis.zadd(KEY1, scores1);
    jedis.zadd(KEY2, scores2);
    jedis.zadd(KEY3, scores3);

    ZParams zParams = new ZParams().aggregate(ZParams.Aggregate.MAX);

    assertThat(jedis.zinterstore(NEW_SET, zParams, KEY1, KEY2, KEY3)).isEqualTo(5);

    Set<Tuple> expected = new HashSet<>();
    for (int i = 6; i <= 10; i++) {
      expected.add(new Tuple("player" + i, score));
    }

    Set<Tuple> actual = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expected, actual);
  }

  @Test
  public void shouldStoreMinOfIntersection_givenThreePartiallyOverlappingSetsWithIdenticalScores() {
    double score = 3.141592;
    Map<String, Double> scores1 = buildMapOfMembersAndIdenticalScores(1, 10, score);
    Map<String, Double> scores2 = buildMapOfMembersAndIdenticalScores(6, 13, score);
    Map<String, Double> scores3 = buildMapOfMembersAndIdenticalScores(4, 11, score);

    jedis.zadd(KEY1, scores1);
    jedis.zadd(KEY2, scores2);
    jedis.zadd(KEY3, scores3);

    ZParams zParams = new ZParams().aggregate(ZParams.Aggregate.MAX);

    assertThat(jedis.zinterstore(NEW_SET, zParams, KEY1, KEY2, KEY3)).isEqualTo(5);

    Set<Tuple> expected = new HashSet<>();
    for (int i = 6; i <= 10; i++) {
      expected.add(new Tuple("player" + i, score));
    }

    Set<Tuple> actual = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expected, actual);
  }

  @Test
  public void shouldStoreIntersectionUsingLastAggregate_givenMultipleAggregateKeywords() {
    Map<String, Double> scores1 = buildMapOfMembersAndScores(1, 15);
    Map<String, Double> scores2 = buildMapOfMembersAndScores(9, 13);
    Map<String, Double> scores3 = buildMapOfMembersAndScores(12, 18);

    jedis.zadd(KEY1, scores1);
    jedis.zadd(KEY2, scores2);
    jedis.zadd(KEY3, scores3);

    Set<Tuple> expected = new HashSet<>();
    for (int i = 12; i <= 13; i++) {
      expected.add(tupleMaxOfScores("player" + i, scores1, scores2, scores3));
    }

    jedis.sendCommand(NEW_SET, Protocol.Command.ZINTERSTORE, NEW_SET, "3",
        KEY1, KEY2, KEY3, "AGGREGATE", "MIN", "AGGREGATE", "MAX");

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expected, results);
  }

  @Test
  public void shouldStoreIntersection_whenTargetExistsAndSetsAreDuplicated() {
    Map<String, Double> scores = buildMapOfMembersAndScores(0, 10);
    jedis.zadd(KEY1, scores);
    jedis.zadd(KEY2, scores);

    Set<Tuple> expectedResults = convertToTuples(scores, (ignore, score) -> score * 2);

    // destination key is a key that exists
    assertThat(jedis.zinterstore(KEY1, KEY1, KEY2)).isEqualTo(scores.size());

    Set<Tuple> results = jedis.zrangeWithScores(KEY1, 0, 10);

    assertThatActualScoresAreVeryCloseToExpectedScores(expectedResults, results);
  }

  @Test
  public void ensureSetConsistency_andNoExceptions_whenRunningConcurrently() {
    int scoreCount = 1000;
    jedis.zadd("{A}ones", buildMapOfMembersAndScores(0, scoreCount - 1));

    jedis.zadd("{A}scores1", buildMapOfMembersAndScores(0, scoreCount - 1));
    jedis.zadd("{A}scores2", buildMapOfMembersAndScores(0, scoreCount - 1));
    jedis.zadd("{A}scores3", buildMapOfMembersAndScores(0, scoreCount - 1));

    new ConcurrentLoopingThreads(1000,
        i -> jedis.zadd("{A}scores1", (double) i, "player" + i),
        i -> jedis.zadd("{A}scores2", (double) i, "player" + i),
        i -> jedis.zadd("{A}scores3", (double) i, "player" + i),
        i -> jedis.zinterstore("{A}maxSet", new ZParams().aggregate(ZParams.Aggregate.MAX),
            "{A}scores1", "{A}scores2", "{A}scores3"),
        // This ensures that the lock ordering for keys is working
        i -> jedis.zinterstore("{A}minSet", new ZParams().aggregate(ZParams.Aggregate.MIN),
            "{A}scores1", "{A}scores2", "{A}scores3"))
                .runWithAction(() -> {
                  assertThat(jedis.zrangeWithScores("{A}maxSet", 0, scoreCount))
                      .hasSize(scoreCount);
                  assertThat(jedis.zrangeWithScores("{A}minSet", 0, scoreCount))
                      .hasSize(scoreCount);
                });
  }

  /************* Helper Methods *************/

  private Map<String, Double> buildMapOfMembersAndScores() {
    Map<String, Double> scores = new LinkedHashMap<>();
    scores.put("player1", Double.NEGATIVE_INFINITY);
    scores.put("player2", 0D);
    scores.put("player3", 1D);
    scores.put("player4", Double.POSITIVE_INFINITY);
    scores.put("player5", 3.2D);
    return scores;
  }

  private Map<String, Double> buildMapOfMembersAndScores(int start, int end) {
    Map<String, Double> scores = new LinkedHashMap<>();
    Random random = new Random();

    for (int i = start; i <= end; i++) {
      scores.put("player" + i, random.nextDouble());
    }

    return scores;
  }

  private Map<String, Double> buildMapOfMembersAndIdenticalScores(int start, int end,
      double score) {
    Map<String, Double> scores = new LinkedHashMap<>();

    for (int i = start; i <= end; i++) {
      scores.put("player" + i, score);
    }

    return scores;
  }

  Tuple tupleSumOfScores(String memberName, Map<String, Double> scores1,
      Map<String, Double> scores2, Map<String, Double> scores3) {
    return tupleSumOfScoresWithWeights(memberName, scores1, scores2, scores3, 1, 1, 1);
  }

  Tuple tupleSumOfScoresWithWeights(String memberName, Map<String, Double> scores1,
      Map<String, Double> scores2, Map<String, Double> scores3, double weight1, double weight2,
      double weight3) {
    return new Tuple(memberName, scores1.get(memberName) * weight1
        + scores2.get(memberName) * weight2
        + scores3.get(memberName) * weight3);
  }

  Tuple tupleMaxOfScores(String memberName, Map<String, Double> scores1,
      Map<String, Double> scores2, Map<String, Double> scores3) {
    return tupleMaxOfScoresWithWeights(memberName, scores1, scores2, scores3, 1, 1, 1);
  }

  Tuple tupleMaxOfScoresWithWeights(String memberName, Map<String, Double> scores1,
      Map<String, Double> scores2, Map<String, Double> scores3, double weight1, double weight2,
      double weight3) {
    return new Tuple(memberName, max(max(scores1.get(memberName) * weight1,
        scores2.get(memberName) * weight2), scores3.get(memberName) * weight3));
  }

  Tuple tupleMinOfScores(String memberName, Map<String, Double> scores1,
      Map<String, Double> scores2, Map<String, Double> scores3) {
    return tupleMinOfScoresWithWeights(memberName, scores1, scores2, scores3, 1, 1, 1);
  }

  Tuple tupleMinOfScoresWithWeights(String memberName, Map<String, Double> scores1,
      Map<String, Double> scores2, Map<String, Double> scores3, double weight1, double weight2,
      double weight3) {
    return new Tuple(memberName, min(min(scores1.get(memberName) * weight1,
        scores2.get(memberName) * weight2), scores3.get(memberName) * weight3));
  }

  private Set<Tuple> convertToTuples(Map<String, Double> map,
      BiFunction<Integer, Double, Double> function) {
    Set<Tuple> tuples = new LinkedHashSet<>();
    int x = 0;
    for (Map.Entry<String, Double> e : map.entrySet()) {
      tuples.add(new Tuple(e.getKey().getBytes(), function.apply(x++, e.getValue())));
    }

    return tuples;
  }

  private void assertThatActualScoresAreVeryCloseToExpectedScores(
      Set<Tuple> expectedResults, Set<Tuple> results) {
    for (Tuple expectedResult : expectedResults) {
      for (Tuple actualResult : results) {
        if (Objects.equals(actualResult.getElement(), expectedResult.getElement())) {
          assertThat(actualResult.getScore()).isCloseTo(expectedResult.getScore(),
              Offset.offset(0.0001D));
        }
      }
    }
  }
}
