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
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

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

public abstract class AbstractZUnionStoreIntegrationTest implements RedisIntegrationTest {

  private static final String NEW_SET = "{user1}new";
  private static final String KEY1 = "{user1}sset1";
  private static final String KEY2 = "{user1}sset2";
  private static final String SORTED_SET_KEY3 = "{user1}sset3";

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
  public void shouldError_givenWrongKeyType() {
    final String STRING_KEY = "{user1}stringKey";
    jedis.set(STRING_KEY, "value");
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "2", STRING_KEY,
            KEY1))
                .hasMessage("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void shouldError_givenSetsCrossSlots() {
    final String WRONG_KEY = "{user2}another";
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "2", WRONG_KEY,
            KEY1))
                .hasMessage("CROSSSLOT " + RedisConstants.ERROR_WRONG_SLOT);
  }

  @Test
  public void shouldError_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.ZUNIONSTORE, 3);
  }

  @Test
  public void shouldError_givenNumkeysTooLarge() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "2", KEY1))
            .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenNumkeysTooSmall() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "1", KEY1, KEY2))
            .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenNumKeysOfZero() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "0", KEY1, KEY2))
            .hasMessage("ERR " + RedisConstants.ERROR_KEY_REQUIRED);
  }

  @Test
  public void shouldError_givenNegativeNumKeys() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "-2", KEY1, KEY2))
            .hasMessage("ERR " + RedisConstants.ERROR_KEY_REQUIRED);
  }

  @Test
  public void shouldError_givenTooManyWeights() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "1", KEY1,
            "WEIGHTS", "2", "3"))
                .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenTooFewWeights() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "2",
            KEY1, KEY2, "WEIGHTS", "1"))
                .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenWeightNotANumber() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "1",
            KEY1, "WEIGHTS", "not-a-number"))
                .hasMessage("ERR " + RedisConstants.ERROR_WEIGHT_NOT_A_FLOAT);
  }

  @Test
  public void shouldError_givenWeightsWithoutAnyValues() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "1",
            KEY1, "WEIGHTS"))
                .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenMultipleWeightKeywords() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "1",
            KEY1, "WEIGHT", "1.0", "WEIGHT", "2.0"))
                .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenUnknownAggregate() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "1",
            KEY1, "AGGREGATE", "UNKNOWN", "WEIGHTS", "1"))
                .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenAggregateKeywordWithoutValue() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "1",
            KEY1, "AGGREGATE"))
                .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenMultipleAggregates() {
    assertThatThrownBy(
        () -> jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "1",
            KEY1, "WEIGHTS", "1", "AGGREGATE", "SUM", "MIN"))
                .hasMessage("ERR " + RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldUnionize_givenBoundaryScoresAndWeights() {
    Map<String, Double> scores = new LinkedHashMap<>();
    scores.put("player1", Double.NEGATIVE_INFINITY);
    scores.put("player2", 0D);
    scores.put("player3", 1D);
    scores.put("player4", Double.POSITIVE_INFINITY);
    Set<Tuple> expectedResults = convertToTuples(scores, (i, x) -> x);
    jedis.zadd(KEY1, scores);

    jedis.zunionstore(NEW_SET, new ZParams().weights(1), KEY1);

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyElementsOf(expectedResults);

    jedis.zunionstore(NEW_SET, new ZParams().weights(0), KEY1);

    expectedResults = convertToTuples(scores, (i, x) -> 0D);
    results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyElementsOf(expectedResults);

    jedis.zunionstore(NEW_SET, new ZParams().weights(Double.POSITIVE_INFINITY), KEY1);

    expectedResults = convertToTuples(scores, (i, x) -> x == 1 ? Double.POSITIVE_INFINITY : x);
    results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyElementsOf(expectedResults);

    jedis.zunionstore(NEW_SET, new ZParams().weights(Double.NEGATIVE_INFINITY), KEY1);

    expectedResults = new LinkedHashSet<>();
    expectedResults.add(new Tuple("player3", Double.NEGATIVE_INFINITY));
    expectedResults.add(new Tuple("player4", Double.NEGATIVE_INFINITY));
    expectedResults.add(new Tuple("player2", 0D));
    expectedResults.add(new Tuple("player1", Double.POSITIVE_INFINITY));
    results = jedis.zrangeWithScores(NEW_SET, 0, scores.size());
    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionize_givenASingleSet() {
    Map<String, Double> scores = makeScoreMap(10, x -> (double) x);
    Set<Tuple> expectedResults = convertToTuples(scores, (i, x) -> x);
    jedis.zadd(KEY1, scores);

    assertThat(jedis.zunionstore(NEW_SET, KEY1)).isEqualTo(10);

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionize_givenOneSetDoesNotExist() {
    Map<String, Double> scores = makeScoreMap(10, x -> (double) x);
    Set<Tuple> expectedResults = convertToTuples(scores, (i, x) -> x);
    jedis.zadd(KEY1, scores);

    jedis.zunionstore(NEW_SET, KEY1, KEY2);

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionize_givenWeight() {
    Map<String, Double> scores = makeScoreMap(10, x -> (double) x);
    Set<Tuple> expectedResults = convertToTuples(scores, (i, x) -> x * 1.5);
    jedis.zadd(KEY1, scores);

    jedis.zunionstore(KEY1, new ZParams().weights(1.5), KEY1);

    Set<Tuple> results = jedis.zrangeWithScores(KEY1, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionizeWithWeightAndDefaultAggregate_givenMultipleSetsWithWeights() {
    Map<String, Double> scores1 = makeScoreMap(10, x -> (double) x);
    jedis.zadd(KEY1, scores1);

    Map<String, Double> scores2 = makeScoreMap(10, x -> (double) (9 - x));
    jedis.zadd(KEY2, scores2);

    Set<Tuple> expectedResults = convertToTuples(scores1, (i, x) -> (x * 2.0) + ((9 - x) * 1.5));

    jedis.zunionstore(NEW_SET, new ZParams().weights(2.0, 1.5), KEY1, KEY2);

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionize_givenMinAggregate() {
    Map<String, Double> scores1 = makeScoreMap(10, x -> (double) x);
    jedis.zadd(KEY1, scores1);

    Map<String, Double> scores2 = makeScoreMap(10, x -> 0D);
    jedis.zadd(KEY2, scores2);

    Set<Tuple> expectedResults = convertToTuples(scores2, (i, x) -> x);

    jedis.zunionstore(NEW_SET, new ZParams().aggregate(ZParams.Aggregate.MIN),
        KEY1, KEY2);

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionize_givenMaxAggregate() {
    Map<String, Double> scores1 = makeScoreMap(10, x -> (double) ((x % 2 == 0) ? 0 : x));
    jedis.zadd(KEY1, scores1);

    Map<String, Double> scores2 = makeScoreMap(10, x -> (double) ((x % 2 == 0) ? x : 0));
    jedis.zadd(KEY2, scores2);

    Set<Tuple> expectedResults = convertToTuples(scores1, (i, x) -> (double) i);

    jedis.zunionstore(NEW_SET, new ZParams().aggregate(ZParams.Aggregate.MAX),
        KEY1, KEY2);

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionizeUsingLastAggregate_givenMultipleAggregateKeywords() {
    Map<String, Double> scores1 = makeScoreMap(10, x -> (double) 0);
    jedis.zadd(KEY1, scores1);

    Map<String, Double> scores2 = makeScoreMap(10, x -> (double) 1);
    jedis.zadd(KEY2, scores2);

    Set<Tuple> expectedResults = convertToTuples(scores2, (i, x) -> x);

    jedis.sendCommand(NEW_SET, Protocol.Command.ZUNIONSTORE, NEW_SET, "2",
        KEY1, KEY2, "AGGREGATE", "MIN", "AGGREGATE", "MAX");

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionize_givenMaxAggregateAndMultipleWeights() {
    Map<String, Double> scores1 = makeScoreMap(10, x -> (double) ((x % 2 == 0) ? 0 : x));
    jedis.zadd(KEY1, scores1);

    Map<String, Double> scores2 = makeScoreMap(10, x -> (double) ((x % 2 == 0) ? x : 0));
    jedis.zadd(KEY2, scores2);

    Set<Tuple> expectedResults = convertToTuples(scores1, (i, x) -> (double) (i * 2));

    jedis.zunionstore(NEW_SET, new ZParams().aggregate(ZParams.Aggregate.MAX).weights(2, 2),
        KEY1, KEY2);

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionize_givenSumAggregateAndMultipleSets() {
    Map<String, Double> scores1 = makeScoreMap(10, x -> (double) x);
    jedis.zadd(KEY1, scores1);

    Map<String, Double> scores2 = makeScoreMap(10, x -> (double) (x * 2));
    jedis.zadd(KEY2, scores2);

    Map<String, Double> scores3 = makeScoreMap(10, x -> (double) (x * 3));
    jedis.zadd(SORTED_SET_KEY3, scores3);

    Set<Tuple> expectedResults = convertToTuples(scores1, (i, x) -> x * 6);

    jedis.zunionstore(NEW_SET, new ZParams().aggregate(ZParams.Aggregate.SUM),
        KEY1, KEY2, SORTED_SET_KEY3);

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionize_givenSetsDoNotOverlap() {
    Map<String, Double> scores1 = makeScoreMap(0, 2, 10, x -> (double) x);
    jedis.zadd(KEY1, scores1);

    Map<String, Double> scores2 = makeScoreMap(1, 2, 10, x -> (double) x);
    jedis.zadd(KEY2, scores2);

    Set<Tuple> expectedResults =
        convertToTuples(makeScoreMap(0, 1, 20, x -> (double) x), (i, x) -> x);

    jedis.zunionstore(NEW_SET, KEY1, KEY2);

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 20);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionize_givenSetsPartiallyOverlap() {
    Map<String, Double> scores1 = makeScoreMap(10, x -> (double) x);
    jedis.zadd(KEY1, scores1);

    Map<String, Double> scores2 = makeScoreMap(5, 1, 10, x -> (double) (x < 10 ? x : x * 2));
    jedis.zadd(KEY2, scores2);

    Set<Tuple> expectedResults = convertToTuples(makeScoreMap(0, 1, 15, x -> (double) x),
        (i, x) -> (double) (i < 5 ? i : i * 2));

    jedis.zunionstore(NEW_SET, KEY1, KEY2);

    Set<Tuple> results = jedis.zrangeWithScores(NEW_SET, 0, 20);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void ensureWeightsAreAppliedBeforeAggregation() {
    Map<String, Double> scores1 = makeScoreMap(10, x -> (double) x * 5);
    jedis.zadd(KEY1, scores1);

    Map<String, Double> scores2 = makeScoreMap(10, x -> (double) x);
    jedis.zadd(KEY2, scores2);

    Set<Tuple> expectedResults = convertToTuples(scores1, (i, x) -> (double) (i * 10));

    jedis.zunionstore(KEY1,
        new ZParams().weights(1, 10).aggregate(ZParams.Aggregate.MAX), KEY1,
        KEY2);

    Set<Tuple> results = jedis.zrangeWithScores(KEY1, 0, 20);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldUnionize_whenTargetExistsAndSetsAreDuplicated() {
    Map<String, Double> scores1 = makeScoreMap(10, x -> (double) x);
    jedis.zadd(KEY1, scores1);

    Set<Tuple> expectedResults = convertToTuples(scores1, (i, x) -> x * 2);

    // Default aggregation is SUM
    jedis.zunionstore(KEY1, KEY1, KEY1);

    Set<Tuple> results = jedis.zrangeWithScores(KEY1, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void shouldPreserveSet_givenDestinationAndSourceAreTheSame() {
    Map<String, Double> scores1 = makeScoreMap(10, x -> (double) x);
    jedis.zadd(KEY1, scores1);

    Set<Tuple> expectedResults = convertToTuples(scores1, (i, x) -> x);

    jedis.zunionstore(KEY1, KEY1);

    Set<Tuple> results = jedis.zrangeWithScores(KEY1, 0, 10);

    assertThat(results).containsExactlyElementsOf(expectedResults);
  }

  @Test
  public void ensureSetConsistency_andNoExceptions_whenRunningConcurrently() {
    int scoreCount = 1000;
    jedis.zadd("{A}ones", makeScoreMap(scoreCount, x -> 1D));

    Map<String, Double> scores1 = makeScoreMap(scoreCount, x -> 1D);
    jedis.zadd("{A}scores1", scores1);
    Map<String, Double> scores2 = makeScoreMap(scoreCount, x -> 2D);
    jedis.zadd("{A}scores2", scores2);

    new ConcurrentLoopingThreads(1000,
        i -> jedis.zadd("{A}scores1", (double) i, String.format("member-%05d", i)),
        i -> jedis.zadd("{A}scores2", (double) i, String.format("member-%05d", i)),
        i -> jedis.zunionstore("{A}maxSet", new ZParams().aggregate(ZParams.Aggregate.MAX),
            "{A}scores1", "{A}scores2"),
        // This ensures that the lock ordering for keys is working
        i -> jedis.zunionstore("{A}minSet", new ZParams().aggregate(ZParams.Aggregate.MIN),
            "{A}scores2", "{A}scores1"))
                .runWithAction(() -> assertThat(jedis.zrangeWithScores("{A}maxSet", 0, scoreCount))
                    .hasSize(scoreCount));
  }

  private Map<String, Double> makeScoreMap(int count, Function<Integer, Double> scoreProducer) {
    return makeScoreMap(0, 1, count, scoreProducer);
  }

  private Map<String, Double> makeScoreMap(int startIndex, int increment, int count,
      Function<Integer, Double> scoreProducer) {
    Map<String, Double> map = new LinkedHashMap<>();

    int index = startIndex;
    for (int i = 0; i < count; i++) {
      map.put(String.format("member-%05d", index), scoreProducer.apply(index));
      index += increment;
    }
    return map;
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
}
