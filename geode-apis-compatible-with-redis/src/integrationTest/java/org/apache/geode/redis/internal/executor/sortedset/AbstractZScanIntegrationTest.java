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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.cluster.CRC16;

public abstract class AbstractZScanIntegrationTest implements RedisIntegrationTest {
  protected JedisCluster jedis;

  public static final String KEY = "key";
  public static final int SLOT_FOR_KEY = CRC16.calculate(KEY) % RegionProvider.REDIS_SLOTS;
  public static final String ZERO_CURSOR = "0";
  public static final BigInteger UNSIGNED_LONG_CAPACITY = new BigInteger("18446744073709551615");

  public static final String MEMBER_ONE = "member1";
  public static final double SCORE_ONE = 1.1;
  public static final String MEMBER_TWO = "member12";
  public static final double SCORE_TWO = 2.2;
  public static final String MEMBER_THREE = "member3";
  public static final double SCORE_THREE = 3.3;
  public static final Offset<Double> doubleOffset = Offset.offset(0.0000001);

  public static final String BASE_MEMBER = "baseMember_";
  private final int SIZE_OF_ENTRY_SET = 100;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  /********* Parameter Checks **************/

  @Test
  public void givenLessThanTwoArguments_returnsWrongNumberOfArgumentsError() {
    assertAtLeastNArgs(jedis, Protocol.Command.ZSCAN, 2);
  }

  @Test
  public void givenMatchArgumentWithoutPatternOnExistingKey_returnsSyntaxError() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, ZERO_CURSOR, "MATCH"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenMatchArgumentWithoutPatternOnNonExistentKey_returnsEmptyArray() {
    List<Object> result =
        uncheckedCast(jedis.sendCommand(KEY, Protocol.Command.ZSCAN, "nonexistentKey",
            ZERO_CURSOR, "MATCH"));

    assertThat((List<?>) result.get(1)).isEmpty();
  }

  @Test
  public void givenCountArgumentWithoutNumberOnExistingKey_returnsSyntaxError() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, ZERO_CURSOR, "COUNT"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCountArgumentWithoutNumberOnNonExistentKey_returnsEmptyArray() {
    List<Object> result =
        uncheckedCast(
            jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, ZERO_CURSOR, "COUNT"));

    assertThat((List<?>) result.get(1)).isEmpty();
  }

  @Test
  public void givenMatchOrCountKeywordNotSpecified_returnsSyntaxError() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, ZERO_CURSOR, "a*", "1"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, ZERO_CURSOR, "COUNT",
            "MATCH"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, ZERO_CURSOR, "COUNT",
            "3",
            "COUNT", "sjlfs", "COUNT", "1"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenCount_whenCountParameterIsZero_returnsSyntaxError() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, ZERO_CURSOR, "COUNT",
            ZERO_CURSOR))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNegative_returnsSyntaxError() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, ZERO_CURSOR, "COUNT",
            "-37"))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsLessThanOne_returnsSyntaxError() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, ZERO_CURSOR,
            "COUNT", "3",
            "COUNT", "0",
            "COUNT", "1"))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenKeyIsNotASortedSet_returnsWrongTypeError() {
    jedis.sadd(KEY, "member");

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY,
        ZERO_CURSOR))
            .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void givenKeyIsNotASortedSet_andCursorIsNotAnInteger_returnsCursorError() {
    jedis.sadd(KEY, "member");

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_andCursorIsNotAnInteger_returnsCursorError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("notReal", Protocol.Command.ZSCAN, "notReal", "sjfls"))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenExistentSortedSetKey_andCursorIsNotAnInteger_returnsCursorError() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_returnsEmptyArray() {
    ScanResult<Tuple> result = jedis.zscan("nonexistent", ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenNegativeCursor_returnsEntriesUsingAbsoluteValueOfCursor() {
    Map<String, Double> entryMap = initializeThreeFieldSortedSet();

    String cursor = "-100";
    ScanResult<Tuple> result;
    List<Tuple> allEntries = new ArrayList<>();

    do {
      result = jedis.zscan(KEY, cursor);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(tupleCollectionToMap(allEntries)).containsExactlyInAnyOrderEntriesOf(entryMap);
  }

  @Test
  public void shouldReturnError_givenCursorGreaterThanUnsignedLongMaxValue() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY,
        UNSIGNED_LONG_CAPACITY.add(new BigInteger("10")).toString()))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void shouldNotError_givenCursorEqualToLongMaxValue() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);
    jedis.sendCommand(KEY, Protocol.Command.ZSCAN, KEY, String.valueOf(Long.MAX_VALUE));
  }

  @Test
  public void givenInvalidRegexSyntax_returnsEmptyArray() {
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);
    ScanParams scanParams = new ScanParams().count(1).match("\\p");

    ScanResult<Tuple> result = jedis.zscan(KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);

    assertThat(result.getResult()).isEmpty();
  }

  // Happy path behavior checks start here

  @Test
  public void givenSortedSetWithOneEntry_returnsEntry() {
    Map<String, Double> expected = new HashMap<>();
    expected.put(MEMBER_ONE, SCORE_ONE);
    jedis.zadd(KEY, SCORE_ONE, MEMBER_ONE);

    ScanResult<Tuple> result = jedis.zscan(KEY, ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(tupleCollectionToMap(result.getResult()))
        .containsExactlyInAnyOrderEntriesOf(expected);
  }

  @Test
  public void givenSortedSetWithMultipleEntries_returnsAllEntries() {
    Map<String, Double> entryMap = initializeThreeFieldSortedSet();

    ScanResult<Tuple> result = jedis.zscan(KEY, ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(tupleCollectionToMap(result.getResult()))
        .containsExactlyInAnyOrderEntriesOf(entryMap);
  }

  @Test
  public void givenMultipleCounts_DoesNotFail() {
    initializeThreeFieldSortedSet();

    List<Object> result;
    String cursor = ZERO_CURSOR;

    do {
      result = uncheckedCast(jedis.sendCommand(KEY, Protocol.Command.ZSCAN,
          KEY,
          cursor,
          "COUNT", "2",
          "COUNT", "1"));

      cursor = new String((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), ZERO_CURSOR.getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());
  }

  @Test
  public void givenCompleteIteration_shouldReturnCursorWithValueOfZero() {
    initializeThreeFieldSortedSet();

    ScanParams scanParams = new ScanParams().count(1);
    ScanResult<Tuple> result;
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.zscan(KEY.getBytes(), cursor.getBytes(), scanParams);
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(result.getCursor()).isEqualTo(ZERO_CURSOR);
  }

  @Test
  public void givenMatch_returnsAllMatchingEntries() {
    Map<String, Double> entryMap = initializeThreeFieldSortedSet();

    ScanParams scanParams = new ScanParams().match("*1*");

    ScanResult<Tuple> result =
        jedis.zscan(KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);

    entryMap.remove(MEMBER_THREE);
    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(tupleCollectionToMap(result.getResult()))
        .containsExactlyInAnyOrderEntriesOf(entryMap);
  }

  @Test
  public void givenMultipleMatches_returnsEntriesMatchingLastMatchParameter() {
    Map<String, Double> expectedResults = initializeThreeFieldSortedSet();
    expectedResults.remove(MEMBER_THREE);

    List<Object> result =
        uncheckedCast(jedis.sendCommand(KEY.getBytes(), Protocol.Command.ZSCAN,
            KEY.getBytes(), ZERO_CURSOR.getBytes(),
            "MATCH".getBytes(), "*3".getBytes(),
            "MATCH".getBytes(), "*1*".getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());
    List<byte[]> membersAndScores = uncheckedCast(result.get(1));

    Map<String, Double> scanResults = byteArrayListToMap(membersAndScores);
    assertThat(scanResults.keySet()).containsExactlyInAnyOrderElementsOf(expectedResults.keySet());
    for (Map.Entry<String, Double> entry : scanResults.entrySet()) {
      assertThat(expectedResults.get(entry.getKey())).isCloseTo(entry.getValue(), doubleOffset);
    }
  }

  @Test
  public void givenMatchAndCount_returnsAllMatchingKeys() {
    Map<String, Double> entryMap = initializeThreeFieldSortedSet();

    ScanParams scanParams = new ScanParams().count(1).match("*1*");
    ScanResult<Tuple> result;
    List<Tuple> allEntries = new ArrayList<>();
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.zscan(KEY.getBytes(), cursor.getBytes(), scanParams);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    entryMap.remove(MEMBER_THREE);
    assertThat(tupleCollectionToMap(allEntries)).containsExactlyInAnyOrderEntriesOf(entryMap);
  }

  @Test
  public void givenMultipleCountsAndMatches_returnsEntriesMatchingLastMatchParameter() {
    Map<String, Double> expectedResults = initializeThreeFieldSortedSet();
    expectedResults.remove(MEMBER_THREE);

    List<Object> result;
    List<byte[]> allEntries = new ArrayList<>();
    String cursor = ZERO_CURSOR;

    do {
      result =
          uncheckedCast(jedis.sendCommand(KEY.getBytes(), Protocol.Command.ZSCAN,
              KEY.getBytes(), cursor.getBytes(),
              "COUNT".getBytes(), "37".getBytes(),
              "MATCH".getBytes(), "3*".getBytes(),
              "COUNT".getBytes(), "2".getBytes(),
              "COUNT".getBytes(), "1".getBytes(),
              "MATCH".getBytes(), "*1*".getBytes()));
      List<byte[]> membersAndScores = uncheckedCast(result.get(1));
      allEntries.addAll(membersAndScores);
      cursor = new String((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), ZERO_CURSOR.getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());

    Map<String, Double> scanResults = byteArrayListToMap(allEntries);
    assertThat(scanResults.keySet()).containsExactlyInAnyOrderElementsOf(expectedResults.keySet());
    for (Map.Entry<String, Double> entry : scanResults.entrySet()) {
      assertThat(expectedResults.get(entry.getKey())).isCloseTo(entry.getValue(), doubleOffset);
    }
  }

  @Test
  public void should_notReturnMember_givenMemberWasRemovedBeforeZscanIsCalled() {
    Map<String, Double> entryMap = initializeThreeFieldSortedSet();

    jedis.zrem(KEY, MEMBER_THREE);
    entryMap.remove(MEMBER_THREE);

    assertThat(jedis.zscore(KEY, MEMBER_THREE)).isNull();

    ScanResult<Tuple> result = jedis.zscan(KEY, ZERO_CURSOR);

    assertThat(tupleCollectionToMap(result.getResult()))
        .containsExactlyInAnyOrderEntriesOf(entryMap);
  }

  @Test
  public void should_notErrorGivenNonzeroCursorOnFirstCall() {
    initializeThreeFieldSortedSet();

    ScanResult<Tuple> result = jedis.zscan(KEY, "5");

    assertThat(result.getResult().size()).isNotZero();
  }

  @Test
  public void should_notErrorGivenCountEqualToIntegerMaxValue() {
    Map<String, Double> entryMap = initializeThreeFieldSortedSet();

    ScanParams scanParams = new ScanParams().count(Integer.MAX_VALUE);

    ScanResult<Tuple> result =
        jedis.zscan(KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);
    assertThat(tupleCollectionToMap(result.getResult()))
        .containsExactlyInAnyOrderEntriesOf(entryMap);
  }

  @Test
  public void should_notErrorGivenCountGreaterThanIntegerMaxValue() {
    Map<String, Double> initialEntries = initializeThreeFieldSortedSet();

    String greaterThanInt = String.valueOf(2L * Integer.MAX_VALUE);
    List<Object> result =
        uncheckedCast(jedis.sendCommand(KEY.getBytes(), Protocol.Command.ZSCAN,
            KEY.getBytes(), ZERO_CURSOR.getBytes(),
            "COUNT".getBytes(), greaterThanInt.getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());

    List<byte[]> membersAndScores = uncheckedCast(result.get(1));
    Map<String, Double> scanResults = byteArrayListToMap(membersAndScores);
    assertThat(scanResults.keySet()).containsExactlyInAnyOrderElementsOf(initialEntries.keySet());
    for (Map.Entry<String, Double> entry : scanResults.entrySet()) {
      assertThat(initialEntries.get(entry.getKey())).isCloseTo(entry.getValue(), doubleOffset);
    }
  }

  // Concurrency

  @Test
  public void should_notLoseMembers_givenConcurrentThreadsDoingZScansAndChangingValues() {
    final Map<String, Double> initialSortedSetData = makeEntryMap();
    jedis.zadd(KEY, initialSortedSetData);
    final int iterationCount = 500;

    Jedis jedis1 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);
    Jedis jedis2 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleZScanAndAssertOnContentOfResultSet(jedis1, initialSortedSetData, true),
        (i) -> multipleZScanAndAssertOnContentOfResultSet(jedis2, initialSortedSetData, true),
        (i) -> jedis.zadd(KEY, i + SIZE_OF_ENTRY_SET, BASE_MEMBER + i % SIZE_OF_ENTRY_SET)).run();

    jedis1.close();
    jedis2.close();
  }

  @Test
  public void should_notLoseKeysForConsistentlyPresentFields_givenConcurrentThreadsAddingAndRemovingFields() {
    final Map<String, Double> initialSortedSetData = makeEntryMap();
    jedis.zadd(KEY, initialSortedSetData);
    final int iterationCount = 500;

    Jedis jedis1 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);
    Jedis jedis2 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleZScanAndAssertOnContentOfResultSet(jedis1, initialSortedSetData, false),
        (i) -> multipleZScanAndAssertOnContentOfResultSet(jedis2, initialSortedSetData, false),
        (i) -> {
          String member = "new_" + BASE_MEMBER + i;
          jedis.zadd(KEY, 0.1, member);
          jedis.zrem(KEY, member);
        }).run();

    jedis1.close();
    jedis2.close();
  }

  private void multipleZScanAndAssertOnContentOfResultSet(Jedis jedis,
      final Map<String, Double> initialSortedSetData, boolean assertOnSizeOnly) {
    Set<Tuple> allEntries = new HashSet<>();
    ScanResult<Tuple> result;
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.zscan(KEY, cursor);
      cursor = result.getCursor();
      allEntries.addAll(result.getResult());
    } while (!result.isCompleteIteration());

    if (assertOnSizeOnly) {
      assertThat(allEntries.size()).isEqualTo(initialSortedSetData.size());
    } else {
      assertThat(tupleCollectionToMap(allEntries)).containsAllEntriesOf(initialSortedSetData);
    }
  }

  Map<String, Double> initializeThreeFieldSortedSet() {
    Map<String, Double> entryMap = new HashMap<>();
    entryMap.put(MEMBER_ONE, SCORE_ONE);
    entryMap.put(MEMBER_TWO, SCORE_TWO);
    entryMap.put(MEMBER_THREE, SCORE_THREE);
    jedis.zadd(KEY, entryMap);
    return entryMap;
  }

  private Map<String, Double> makeEntryMap() {
    Map<String, Double> dataSet = new HashMap<>();
    for (int i = 0; i < SIZE_OF_ENTRY_SET; i++) {
      dataSet.put(BASE_MEMBER + i, (double) i);
    }
    return dataSet;
  }

  private Map<String, Double> tupleCollectionToMap(Collection<Tuple> allEntries) {
    return allEntries.stream().collect(Collectors.toMap(Tuple::getElement, Tuple::getScore));
  }

  private Map<String, Double> byteArrayListToMap(List<byte[]> byteArrayList) {
    assertThat(byteArrayList.size() % 2 == 0);
    Map<String, Double> outputMap = new HashMap<>();
    for (int i = 0; i < byteArrayList.size(); i += 2) {
      outputMap.put(new String(byteArrayList.get(i)),
          Double.parseDouble(new String(byteArrayList.get(i + 1))));
    }
    return outputMap;
  }
}
