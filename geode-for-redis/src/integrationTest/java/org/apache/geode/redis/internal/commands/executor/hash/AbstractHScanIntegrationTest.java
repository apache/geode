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

package org.apache.geode.redis.internal.commands.executor.hash;

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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.commands.executor.cluster.CRC16;
import org.apache.geode.redis.internal.services.RegionProvider;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractHScanIntegrationTest implements RedisIntegrationTest {
  protected JedisCluster jedis;

  public static final String HASH_KEY = "key";
  public static final int SLOT_FOR_KEY = CRC16.calculate(HASH_KEY) % RegionProvider.REDIS_SLOTS;
  public static final String ZERO_CURSOR = "0";
  public static final BigInteger UNSIGNED_LONG_CAPACITY = new BigInteger("18446744073709551615");

  public static final String FIELD_ONE = "1";
  public static final String VALUE_ONE = "yellow";
  public static final String FIELD_TWO = "12";
  public static final String VALUE_TWO = "green";
  public static final String FIELD_THREE = "3";
  public static final byte[] FIELD_THREE_BYTES = FIELD_THREE.getBytes();
  public static final String VALUE_THREE = "orange";

  public static final String BASE_FIELD = "baseField_";
  private final int SIZE_OF_ENTRY_MAP = 100;

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
    assertAtLeastNArgs(jedis, Protocol.Command.HSCAN, 2);
  }

  @Test
  public void givenMatchArgumentWithoutPatternOnExistingKey_returnsSyntaxError() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, ZERO_CURSOR, "MATCH"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenMatchArgumentWithoutPatternOnNonExistentKey_returnsEmptyArray() {
    List<Object> result =
        uncheckedCast(jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, "nonexistentKey",
            ZERO_CURSOR, "MATCH"));

    assertThat((List<?>) result.get(1)).isEmpty();
  }

  @Test
  public void givenCountArgumentWithoutNumberOnExistingKey_returnsSyntaxError() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, ZERO_CURSOR, "COUNT"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCountArgumentWithoutNumberOnNonExistentKey_returnsEmptyArray() {
    List<Object> result =
        uncheckedCast(
            jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, ZERO_CURSOR, "COUNT"));

    assertThat((List<?>) result.get(1)).isEmpty();
  }

  @Test
  public void givenMatchOrCountKeywordNotSpecified_returnsSyntaxError() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, ZERO_CURSOR, "a*", "1"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, ZERO_CURSOR, "COUNT",
            "MATCH"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, ZERO_CURSOR, "COUNT",
            "3",
            "COUNT", "sjlfs", "COUNT", "1"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenCount_whenCountParameterIsZero_returnsSyntaxError() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, ZERO_CURSOR, "COUNT",
            ZERO_CURSOR))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNegative_returnsSyntaxError() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, ZERO_CURSOR, "COUNT",
            "-37"))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsLessThanOne_returnsSyntaxError() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, ZERO_CURSOR,
            "COUNT", "3",
            "COUNT", "0",
            "COUNT", "1"))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenKeyIsNotAHash_returnsWrongTypeError() {
    jedis.sadd(HASH_KEY, "member");

    assertThatThrownBy(() -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY,
        ZERO_CURSOR))
            .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void givenKeyIsNotAHash_andCursorIsNotAnInteger_returnsCursorError() {
    jedis.sadd(HASH_KEY, "member");

    assertThatThrownBy(() -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_andCursorIsNotAnInteger_returnsCursorError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("notReal", Protocol.Command.HSCAN, "notReal", "sjfls"))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenExistentHashKey_andCursorIsNotAnInteger_returnsCursorError() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);

    assertThatThrownBy(() -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_returnsEmptyArray() {
    ScanResult<Map.Entry<String, String>> result = jedis.hscan("nonexistent", ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenNegativeCursor_returnsEntriesUsingAbsoluteValueOfCursor() {
    Map<String, String> entryMap = initializeThreeFieldHash();

    String cursor = "-100";
    ScanResult<Map.Entry<String, String>> result;
    List<Map.Entry<String, String>> allEntries = new ArrayList<>();

    do {
      result = jedis.hscan(HASH_KEY, cursor);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allEntries).hasSize(3);
    assertThat(new HashSet<>(allEntries)).isEqualTo(entryMap.entrySet());
  }

  @Test
  public void shouldReturnError_givenCursorGreaterThanUnsignedLongMaxValue() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);

    assertThatThrownBy(() -> jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY,
        UNSIGNED_LONG_CAPACITY.add(new BigInteger("10")).toString()))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void shouldNotError_givenCursorEqualToLongMaxValue() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);
    jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN, HASH_KEY, String.valueOf(Long.MAX_VALUE));
  }

  @Test
  public void givenInvalidRegexSyntax_returnsEmptyArray() {
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);
    ScanParams scanParams = new ScanParams().count(1).match("\\p");

    ScanResult<Map.Entry<byte[], byte[]>> result =
        jedis.hscan(HASH_KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);

    assertThat(result.getResult()).isEmpty();
  }

  /*** Happy path behavior checks start here **/

  @Test
  public void givenHashWithOneEntry_returnsEntry() {
    Map<String, String> expected = new HashMap<>();
    expected.put(FIELD_ONE, VALUE_ONE);
    jedis.hset(HASH_KEY, FIELD_ONE, VALUE_ONE);

    ScanResult<Map.Entry<String, String>> result = jedis.hscan(HASH_KEY, ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactly(expected.entrySet().iterator().next());
  }

  @Test
  public void givenHashWithMultipleEntries_returnsAllEntries() {
    Map<String, String> entryMap = initializeThreeFieldHash();

    ScanResult<Map.Entry<String, String>> result = jedis.hscan(HASH_KEY, ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(new HashSet<>(result.getResult())).isEqualTo(entryMap.entrySet());
  }

  @Test
  public void givenMultipleCounts_DoesNotFail() {
    initializeThreeFieldHash();

    List<Object> result;
    String cursor = ZERO_CURSOR;

    do {
      result = uncheckedCast(jedis.sendCommand(HASH_KEY, Protocol.Command.HSCAN,
          HASH_KEY,
          cursor,
          "COUNT", "2",
          "COUNT", "1"));

      cursor = new String((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), ZERO_CURSOR.getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());
  }

  @Test
  public void givenCompleteIteration_shouldReturnCursorWithValueOfZero() {
    initializeThreeFieldHash();

    ScanParams scanParams = new ScanParams().count(1);
    ScanResult<Map.Entry<byte[], byte[]>> result;
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.hscan(HASH_KEY.getBytes(), cursor.getBytes(), scanParams);
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(result.getCursor()).isEqualTo(ZERO_CURSOR);
  }

  @Test
  public void givenMatch_returnsAllMatchingEntries() {
    Map<byte[], byte[]> entryMap = initializeThreeFieldHashBytes();

    ScanParams scanParams = new ScanParams().match("1*");

    ScanResult<Map.Entry<byte[], byte[]>> result =
        jedis.hscan(HASH_KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);

    entryMap.remove(FIELD_THREE_BYTES);
    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(new HashSet<>(result.getResult()))
        .usingElementComparator(new MapEntryWithByteArraysComparator())
        .containsExactlyInAnyOrderElementsOf(entryMap.entrySet());
  }

  @Test
  public void givenMultipleMatches_returnsEntriesMatchingLastMatchParameter() {
    initializeThreeFieldHash();

    List<Object> result =
        uncheckedCast(jedis.sendCommand(HASH_KEY.getBytes(), Protocol.Command.HSCAN,
            HASH_KEY.getBytes(), ZERO_CURSOR.getBytes(),
            "MATCH".getBytes(), "3*".getBytes(),
            "MATCH".getBytes(), "1*".getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());
    List<byte[]> fieldsAndValues = uncheckedCast(result.get(1));
    assertThat(fieldsAndValues).containsAll(
        Arrays.asList(FIELD_ONE.getBytes(), VALUE_ONE.getBytes(),
            FIELD_TWO.getBytes(), VALUE_TWO.getBytes()));
  }

  @Test
  public void givenMatchAndCount_returnsAllMatchingKeys() {
    Map<byte[], byte[]> entryMap = initializeThreeFieldHashBytes();

    ScanParams scanParams = new ScanParams().count(1).match("1*");
    ScanResult<Map.Entry<byte[], byte[]>> result;
    List<Map.Entry<byte[], byte[]>> allEntries = new ArrayList<>();
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.hscan(HASH_KEY.getBytes(), cursor.getBytes(), scanParams);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    entryMap.remove(FIELD_THREE_BYTES);

    assertThat(new HashSet<>(allEntries))
        .usingElementComparator(new MapEntryWithByteArraysComparator())
        .containsAll(entryMap.entrySet());
  }

  @Test
  public void givenMultipleCountsAndMatches_returnsEntriesMatchingLastMatchParameter() {
    initializeThreeFieldHash();
    List<Object> result;

    List<byte[]> allEntries = new ArrayList<>();
    String cursor = ZERO_CURSOR;

    do {
      result =
          uncheckedCast(jedis.sendCommand(HASH_KEY.getBytes(), Protocol.Command.HSCAN,
              HASH_KEY.getBytes(), cursor.getBytes(),
              "COUNT".getBytes(), "37".getBytes(),
              "MATCH".getBytes(), "3*".getBytes(),
              "COUNT".getBytes(), "2".getBytes(),
              "COUNT".getBytes(), "1".getBytes(),
              "MATCH".getBytes(), "1*".getBytes()));
      List<byte[]> fieldsAndValues = uncheckedCast(result.get(1));
      allEntries.addAll(fieldsAndValues);
      cursor = new String((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), ZERO_CURSOR.getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());
    assertThat(allEntries).containsExactlyInAnyOrder(
        FIELD_ONE.getBytes(), VALUE_ONE.getBytes(),
        FIELD_TWO.getBytes(), VALUE_TWO.getBytes());
  }

  @Test
  public void should_notReturnValue_givenValueWasRemovedBeforeHSCANISCalled() {
    Map<String, String> entryMap = initializeThreeFieldHash();

    jedis.hdel(HASH_KEY, FIELD_THREE);
    entryMap.remove(FIELD_THREE);

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(jedis.hget(HASH_KEY, FIELD_THREE)).isNull());

    ScanResult<Map.Entry<String, String>> result = jedis.hscan(HASH_KEY, ZERO_CURSOR);

    assertThat(new HashSet<>(result.getResult()))
        .containsExactlyInAnyOrderElementsOf(entryMap.entrySet());
  }

  @Test
  public void should_notErrorGivenNonzeroCursorOnFirstCall() {
    Map<String, String> entryMap = initializeThreeFieldHash();

    ScanResult<Map.Entry<String, String>> result = jedis.hscan(HASH_KEY, "5");

    assertThat(new HashSet<>(result.getResult()))
        .isSubsetOf(entryMap.entrySet());
  }

  @Test
  public void should_notErrorGivenCountEqualToIntegerMaxValue() {
    Map<byte[], byte[]> entryMap = initializeThreeFieldHashBytes();

    ScanParams scanParams = new ScanParams().count(Integer.MAX_VALUE);

    ScanResult<Map.Entry<byte[], byte[]>> result =
        jedis.hscan(HASH_KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);
    assertThat(result.getResult())
        .usingElementComparator(new MapEntryWithByteArraysComparator())
        .containsExactlyInAnyOrderElementsOf(entryMap.entrySet());
  }

  @Test
  public void should_notErrorGivenCountGreaterThanIntegerMaxValue() {
    initializeThreeFieldHash();

    String greaterThanInt = String.valueOf(2L * Integer.MAX_VALUE);
    List<Object> result =
        uncheckedCast(jedis.sendCommand(HASH_KEY.getBytes(), Protocol.Command.HSCAN,
            HASH_KEY.getBytes(), ZERO_CURSOR.getBytes(),
            "COUNT".getBytes(), greaterThanInt.getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());

    List<byte[]> fieldsAndValues = uncheckedCast(result.get(1));
    assertThat(fieldsAndValues).containsExactlyInAnyOrder(
        FIELD_ONE.getBytes(), VALUE_ONE.getBytes(),
        FIELD_TWO.getBytes(), VALUE_TWO.getBytes(),
        FIELD_THREE_BYTES, VALUE_THREE.getBytes());
  }

  /**** Concurrency ***/

  @Test
  public void should_notLoseFields_givenConcurrentThreadsDoingHScansAndChangingValues() {
    final Map<String, String> initialHashData = makeEntryMap();
    jedis.hset(HASH_KEY, initialHashData);
    final int iterationCount = 500;

    Jedis jedis1 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);
    Jedis jedis2 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleHScanAndAssertOnSizeOfResultSet(jedis1, initialHashData),
        (i) -> multipleHScanAndAssertOnSizeOfResultSet(jedis2, initialHashData),
        (i) -> {
          int fieldSuffix = i % SIZE_OF_ENTRY_MAP;
          jedis.hset(HASH_KEY, BASE_FIELD + fieldSuffix, "new_value_" + i);
        }).run();

    jedis1.close();
    jedis2.close();
  }

  @Test
  public void should_notLoseKeysForConsistentlyPresentFields_givenConcurrentThreadsAddingAndRemovingFields() {
    final Map<String, String> initialHashData = makeEntryMap();
    jedis.hset(HASH_KEY, initialHashData);
    final int iterationCount = 500;

    Jedis jedis1 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);
    Jedis jedis2 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleHScanAndAssertOnContentOfResultSet(i, jedis1, initialHashData),
        (i) -> multipleHScanAndAssertOnContentOfResultSet(i, jedis2, initialHashData),
        (i) -> {
          String field = "new_" + BASE_FIELD + i;
          jedis.hset(HASH_KEY, field, "whatever");
          jedis.hdel(HASH_KEY, field);
        }).run();

    jedis1.close();
    jedis2.close();
  }

  @Test
  public void should_notAlterUnderlyingData_givenMultipleConcurrentHscans() {
    final Map<String, String> initialHashData = makeEntryMap();
    jedis.hset(HASH_KEY, initialHashData);
    final int iterationCount = 500;

    Jedis jedis1 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);
    Jedis jedis2 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleHScanAndAssertOnContentOfResultSet(i, jedis1, initialHashData),
        (i) -> multipleHScanAndAssertOnContentOfResultSet(i, jedis2, initialHashData))
            .run();

    initialHashData
        .forEach((field, value) -> assertThat(jedis.hget(HASH_KEY, field).equals(value)));

    jedis1.close();
    jedis2.close();
  }

  private void multipleHScanAndAssertOnContentOfResultSet(int iteration, Jedis jedis,
      final Map<String, String> initialHashData) {

    List<String> allEntries = new ArrayList<>();
    ScanResult<Map.Entry<String, String>> result;
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.hscan(HASH_KEY, cursor);
      cursor = result.getCursor();
      List<Map.Entry<String, String>> resultEntries = result.getResult();
      resultEntries
          .forEach((entry) -> allEntries.add(entry.getKey()));
    } while (!result.isCompleteIteration());

    assertThat(allEntries).as("failed on iteration " + iteration)
        .containsAll(initialHashData.keySet());
  }

  private void multipleHScanAndAssertOnSizeOfResultSet(Jedis jedis,
      final Map<String, String> initialHashData) {
    List<Map.Entry<String, String>> allEntries = new ArrayList<>();
    ScanResult<Map.Entry<String, String>> result;
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.hscan(HASH_KEY, cursor);
      cursor = result.getCursor();
      allEntries.addAll(result.getResult());
    } while (!result.isCompleteIteration());

    List<Map.Entry<String, String>> allDistinctEntries =
        allEntries
            .stream()
            .distinct()
            .collect(Collectors.toList());

    assertThat(allDistinctEntries.size())
        .isEqualTo(initialHashData.size());
  }

  private Map<String, String> initializeThreeFieldHash() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put(FIELD_ONE, VALUE_ONE);
    entryMap.put(FIELD_TWO, VALUE_TWO);
    entryMap.put(FIELD_THREE, VALUE_THREE);
    jedis.hmset(HASH_KEY, entryMap);
    return entryMap;
  }

  Map<byte[], byte[]> initializeThreeFieldHashBytes() {
    Map<byte[], byte[]> entryMap = new HashMap<>();
    entryMap.put(FIELD_ONE.getBytes(), VALUE_ONE.getBytes());
    entryMap.put(FIELD_TWO.getBytes(), VALUE_TWO.getBytes());
    entryMap.put(FIELD_THREE_BYTES, VALUE_THREE.getBytes());
    jedis.hmset(HASH_KEY.getBytes(), entryMap);
    return entryMap;
  }

  private Map<String, String> makeEntryMap() {
    Map<String, String> dataSet = new HashMap<>();
    for (int i = 0; i < SIZE_OF_ENTRY_MAP; i++) {
      dataSet.put(BASE_FIELD + i, "value_" + i);
    }
    return dataSet;
  }

  private static class MapEntryWithByteArraysComparator
      implements Comparator<Map.Entry<byte[], byte[]>> {

    @Override
    public int compare(Map.Entry<byte[], byte[]> o1, Map.Entry<byte[], byte[]> o2) {
      return Arrays.equals(o1.getKey(), o2.getKey()) &&
          Arrays.equals(o1.getValue(), o2.getValue()) ? 0 : 1;
    }
  }
}
