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
package org.apache.geode.redis.internal.commands.executor.set;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

public abstract class AbstractSScanIntegrationTest implements RedisIntegrationTest {
  protected JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  public static final String KEY = "key";
  public static final int SLOT_FOR_KEY = CRC16.calculate(KEY) % RegionProvider.REDIS_SLOTS;
  public static final String ZERO_CURSOR = "0";
  public static final BigInteger UNSIGNED_LONG_CAPACITY = new BigInteger("18446744073709551615");
  public static final BigInteger SIGNED_LONG_CAPACITY = new BigInteger("-18446744073709551615");

  public static final String FIELD_ONE = "1";
  public static final String FIELD_TWO = "12";
  public static final String FIELD_THREE = "3";

  public static final String BASE_FIELD = "baseField_";
  private final int SIZE_OF_SET = 100;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void givenLessThanTwoArguments_returnsWrongNumberOfArgumentsError() {
    assertAtLeastNArgs(jedis, Protocol.Command.SSCAN, 2);
  }

  @Test
  public void givenNoKeyArgument_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.SSCAN))
        .hasMessageContaining("ERR wrong number of arguments for 'sscan' command");
  }

  @Test
  public void givenNoCursorArgument_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY))
        .hasMessageContaining("ERR wrong number of arguments for 'sscan' command");
  }

  @Test
  public void givenArgumentsAreNotOddAndKeyExists_returnsSyntaxError() {
    jedis.sadd(KEY, FIELD_ONE);
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "a*"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenArgumentsAreNotOddAndKeyDoesNotExist_returnsEmptyArray() {
    List<Object> result =
        (List<Object>) jedis.sendCommand("key!", Protocol.Command.SSCAN, "key!", ZERO_CURSOR, "a*");

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());
    assertThat((List<Object>) result.get(1)).isEmpty();
  }

  @Test
  public void givenMatchArgumentWithoutPatternOnExistingKey_returnsSyntaxError() {
    jedis.sadd(KEY, FIELD_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "MATCH"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenMatchArgumentWithoutPatternOnNonExistentKey_returnsEmptyArray() {
    List<Object> result =
        uncheckedCast(jedis.sendCommand("nonexistentKey", Protocol.Command.SSCAN, "nonexistentKey",
            ZERO_CURSOR, "MATCH"));

    assertThat((List<?>) result.get(1)).isEmpty();
  }

  @Test
  public void givenCountArgumentWithoutNumberOnExistingKey_returnsSyntaxError() {
    jedis.sadd(KEY, FIELD_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCountArgumentWithoutNumberOnNonExistentKey_returnsEmptyArray() {
    List<Object> result =
        uncheckedCast(
            jedis.sendCommand("nonexistentKey", Protocol.Command.SSCAN, "nonexistentKey",
                ZERO_CURSOR, "COUNT"));

    assertThat((List<?>) result.get(1)).isEmpty();
  }

  @Test
  public void givenMatchOrCountKeywordNotSpecified_returnsSyntaxError() {
    jedis.sadd(KEY, FIELD_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "a*", FIELD_ONE))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.sadd(KEY, FIELD_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "MATCH"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.sadd(KEY, FIELD_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", FIELD_TWO,
            "COUNT", "sjlfs", "COUNT", FIELD_ONE))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsLessThanOne_returnsSyntaxError() {
    jedis.sadd(KEY, FIELD_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", FIELD_TWO,
            "COUNT", "0", "COUNT", FIELD_ONE))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsZero_returnsSyntaxError() {
    jedis.sadd(KEY, FIELD_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "0"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNegative_returnsSyntaxError() {
    jedis.sadd(KEY, FIELD_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "-37"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenKeyIsNotASet_returnsWrongTypeError() {
    jedis.hset(KEY, "b", FIELD_ONE);

    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "-37"))
            .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void givenKeyIsNotASet_andCursorIsNotAnInteger_returnsInvalidCursorError() {
    jedis.hset(KEY, "b", FIELD_ONE);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_andCursorIsNotInteger_returnsInvalidCursorError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("notReal", Protocol.Command.SSCAN, "notReal", "notReal", "sjfls"))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenExistentSetKey_andCursorIsNotAnInteger_returnsInvalidCursorError() {
    jedis.set(KEY, "b");

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_returnsEmptyArray() {
    ScanResult<String> result = jedis.sscan("nonexistent", ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenNegativeCursor_returnsEntriesUsingAbsoluteValueOfCursor() {
    List<String> set = initializeThreeMemberSet();

    String cursor = "-100";
    ScanResult<String> result;
    List<String> allEntries = new ArrayList<>();

    do {
      result = jedis.sscan(KEY, cursor);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allEntries).hasSize(3);
    assertThat(allEntries).containsExactlyInAnyOrderElementsOf(set);
  }

  @Test
  public void givenSetWithOneMember_returnsMember() {
    jedis.sadd(KEY, FIELD_ONE);
    ScanResult<String> result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactly(FIELD_ONE);
  }

  @Test
  public void givenSetWithMultipleMembers_returnsAllMembers() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);
    ScanResult<String> result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactlyInAnyOrder(FIELD_ONE, FIELD_TWO, FIELD_THREE);
  }

  @Test
  public void givenCount_returnsAllMembersWithoutDuplicates() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);

    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    String cursor = ZERO_CURSOR;
    ScanResult<byte[]> result;
    List<byte[]> allMembersFromScan = new ArrayList<>();

    do {
      result = jedis.sscan(KEY.getBytes(), cursor.getBytes(), scanParams);
      allMembersFromScan.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allMembersFromScan).containsExactlyInAnyOrder(FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes(),
        FIELD_THREE.getBytes());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenMultipleCounts_returnsAllEntriesWithoutDuplicates() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);

    List<Object> result;

    List<byte[]> allEntries = new ArrayList<>();
    String cursor = ZERO_CURSOR;

    do {
      result =
          (List<Object>) jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, cursor, "COUNT",
              FIELD_TWO,
              "COUNT", FIELD_ONE);
      allEntries.addAll((List<byte[]>) result.get(1));
      cursor = new String((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), ZERO_CURSOR.getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());
    assertThat(allEntries).containsExactlyInAnyOrder(FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes(),
        FIELD_THREE.getBytes());
  }

  @Test
  public void givenMatch_returnsAllMatchingMembersWithoutDuplicates() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);

    ScanParams scanParams = new ScanParams();
    scanParams.match("1*");

    ScanResult<byte[]> result =
        jedis.sscan(KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactlyInAnyOrder(FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenMultipleMatches_returnsMembersMatchingLastMatchParameter() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);

    List<Object> result =
        (List<Object>) jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR,
            "MATCH", "3*", "MATCH", "1*");

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());
    assertThat((List<byte[]>) result.get(1)).containsExactlyInAnyOrder(FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes());
  }

  @Test
  public void givenMatchAndCount_returnsAllMembersWithoutDuplicates() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);

    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    scanParams.match("1*");
    ScanResult<byte[]> result;
    List<byte[]> allMembersFromScan = new ArrayList<>();
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.sscan(KEY.getBytes(), cursor.getBytes(), scanParams);
      allMembersFromScan.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allMembersFromScan).containsExactlyInAnyOrder(FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenMultipleCountsAndMatches_returnsAllEntriesWithoutDuplicates() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);

    List<Object> result;
    List<byte[]> allEntries = new ArrayList<>();
    String cursor = ZERO_CURSOR;

    do {
      result =
          (List<Object>) jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, cursor, "COUNT", "37",
              "MATCH", "3*", "COUNT", FIELD_TWO, "COUNT", FIELD_ONE, "MATCH", "1*");
      allEntries.addAll((List<byte[]>) result.get(1));
      cursor = new String((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), ZERO_CURSOR.getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());
    assertThat(allEntries).containsExactlyInAnyOrder(FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes());
  }

  @Test
  public void givenNegativeCursor_returnsMembersUsingAbsoluteValueOfCursor() {
    jedis.sadd("b", "green", "orange", "yellow");

    List<String> allEntries = new ArrayList<>();

    String cursor = "-100";
    ScanResult<String> result;
    do {
      result = jedis.sscan("b", cursor);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allEntries).containsExactlyInAnyOrder("green", "orange", "yellow");
  }

  @Test
  public void givenCursorGreaterThanUnsignedLongCapacity_returnsCursorError() {
    assertThatThrownBy(
        () -> jedis.sscan(KEY, UNSIGNED_LONG_CAPACITY.add(BigInteger.ONE).toString()))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNegativeCursorGreaterThanUnsignedLongCapacity_returnsCursorError() {
    assertThatThrownBy(
        () -> jedis.sscan(KEY, SIGNED_LONG_CAPACITY.add(BigInteger.valueOf(-1)).toString()))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenInvalidRegexSyntax_returnsEmptyArray() {
    jedis.sadd(KEY, FIELD_ONE);
    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    scanParams.match("\\p");

    ScanResult<byte[]> result =
        jedis.sscan(KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);

    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void should_notReturnValue_givenValueWasRemovedBeforeSSCANISCalled() {
    List<String> set = initializeThreeMemberSet();

    jedis.srem(KEY, FIELD_THREE);
    set.remove(FIELD_THREE);

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(jedis.sismember(KEY, FIELD_THREE)).isFalse());

    ScanResult<String> result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.getResult()).containsExactlyInAnyOrderElementsOf(set);
  }

  @Test
  public void should_notErrorGivenNonzeroCursorOnFirstCall() {
    List<String> set = initializeThreeMemberSet();

    ScanResult<String> result = jedis.sscan(KEY, "5");

    assertThat(result.getResult()).isSubsetOf(set);
  }

  @Test
  public void should_notErrorGivenCountEqualToIntegerMaxValue() {
    List<byte[]> set = initializeThreeMemberByteSet();

    ScanParams scanParams = new ScanParams().count(Integer.MAX_VALUE);

    ScanResult<byte[]> result =
        jedis.sscan(KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);
    assertThat(result.getResult())
        .containsExactlyInAnyOrderElementsOf(set);
  }

  @Test
  public void should_notErrorGivenCountGreaterThanIntegerMaxValue() {
    initializeThreeMemberByteSet();

    String greaterThanInt = String.valueOf(Integer.MAX_VALUE);
    List<Object> result =
        uncheckedCast(jedis.sendCommand(KEY.getBytes(), Protocol.Command.SSCAN,
            KEY.getBytes(), ZERO_CURSOR.getBytes(),
            "COUNT".getBytes(), greaterThanInt.getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo(ZERO_CURSOR.getBytes());

    List<byte[]> fields = uncheckedCast(result.get(1));
    assertThat(fields).containsExactlyInAnyOrder(
        FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes(),
        FIELD_THREE.getBytes());
  }

  /**** Concurrency ***/

  @Test
  public void should_notLoseFields_givenConcurrentThreadsDoingSScansAndChangingValues() {
    final List<String> initialMemberData = makeSet();
    jedis.sadd(KEY, initialMemberData.toArray(new String[initialMemberData.size()]));
    final int iterationCount = 500;

    Jedis jedis1 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);
    Jedis jedis2 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleSScanAndAssertOnSizeOfResultSet(jedis1, initialMemberData),
        (i) -> multipleSScanAndAssertOnSizeOfResultSet(jedis2, initialMemberData),
        (i) -> {
          int fieldSuffix = i % SIZE_OF_SET;
          jedis.sadd(KEY, BASE_FIELD + fieldSuffix);
        }).run();

    jedis1.close();
    jedis2.close();
  }

  @Test
  public void should_notLoseKeysForConsistentlyPresentFields_givenConcurrentThreadsAddingAndRemovingFields() {
    final List<String> initialMemberData = makeSet();
    jedis.sadd(KEY, initialMemberData.toArray(new String[initialMemberData.size()]));
    final int iterationCount = 500;

    Jedis jedis1 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);
    Jedis jedis2 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis1, initialMemberData),
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis2, initialMemberData),
        (i) -> {
          String field = "new_" + BASE_FIELD + i;
          jedis.sadd(KEY, field);
          jedis.srem(KEY, field);
        }).run();

    jedis1.close();
    jedis2.close();
  }

  @Test
  public void should_notAlterUnderlyingData_givenMultipleConcurrentSscans() {
    final List<String> initialMemberData = makeSet();
    jedis.sadd(KEY, initialMemberData.toArray(new String[initialMemberData.size()]));
    final int iterationCount = 500;

    Jedis jedis1 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);
    Jedis jedis2 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis1, initialMemberData),
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis2, initialMemberData))
            .run();

    initialMemberData
        .forEach((field) -> assertThat(jedis.sismember(KEY, field)).isTrue());

    jedis1.close();
    jedis2.close();
  }

  private void multipleSScanAndAssertOnContentOfResultSet(int iteration, Jedis jedis,
      final List<String> initialMemberData) {

    List<String> allEntries = new ArrayList<>();
    ScanResult<String> result;
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.sscan(KEY, cursor);
      cursor = result.getCursor();
      List<String> resultEntries = result.getResult();
      resultEntries
          .forEach((entry) -> allEntries.add(entry));
    } while (!result.isCompleteIteration());

    assertThat(allEntries).as("failed on iteration " + iteration)
        .containsAll(initialMemberData);
  }

  private void multipleSScanAndAssertOnSizeOfResultSet(Jedis jedis,
      final List<String> initialMemberData) {
    List<String> allEntries = new ArrayList<>();
    ScanResult<String> result;
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.sscan(KEY, cursor);
      cursor = result.getCursor();
      allEntries.addAll(result.getResult());
    } while (!result.isCompleteIteration());

    List<String> allDistinctEntries =
        allEntries
            .stream()
            .distinct()
            .collect(Collectors.toList());

    assertThat(allDistinctEntries.size())
        .isEqualTo(initialMemberData.size());
  }

  private List<String> initializeThreeMemberSet() {
    List<String> set = new ArrayList<>();
    set.add(FIELD_ONE);
    set.add(FIELD_TWO);
    set.add(FIELD_THREE);
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);
    return set;
  }

  private List<byte[]> initializeThreeMemberByteSet() {
    List<byte[]> set = new ArrayList<>();
    set.add(FIELD_ONE.getBytes());
    set.add(FIELD_TWO.getBytes());
    set.add(FIELD_THREE.getBytes());
    jedis.sadd(KEY.getBytes(), FIELD_ONE.getBytes(), FIELD_TWO.getBytes(), FIELD_THREE.getBytes());
    return set;
  }

  private List<String> makeSet() {
    List<String> set = new ArrayList<>();
    for (int i = 0; i < SIZE_OF_SET; i++) {
      set.add((BASE_FIELD + i));
    }
    return set;
  }

}
