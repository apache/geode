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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.geode.redis.internal.data.KeyHashUtil;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public abstract class AbstractSScanIntegrationTest implements RedisIntegrationTest {
  protected JedisCluster jedis;
  protected ScanResult<byte[]> result;
  public static final String KEY = "key";
  public static final int SLOT_FOR_KEY = KeyHashUtil.slotForKey(KEY.getBytes());
  public static final String ZERO_CURSOR = "0";
  public static final BigInteger SIGNED_LONG_MAX = new BigInteger(Long.toString(Long.MAX_VALUE));
  public static final BigInteger SIGNED_LONG_MIN = new BigInteger(Long.toString(Long.MIN_VALUE));

  public static final String FIELD_ONE = "1";
  public static final String FIELD_TWO = "12";
  public static final String FIELD_THREE = "3";

  public static final String BASE_FIELD = "baseField_";

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(RedisClusterStartupRule.BIND_ADDRESS, getPort()),
        RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT);
    result = null;
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
  public void givenNonexistentKey_returnsEmptyArray() {
    ScanResult<String> result = jedis.sscan("nonexistent", ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenNonexistentKeyAndIncorrectOptionalArguments_returnsEmptyArray() {
    result = sendCustomSscanCommand("nonexistentKey", "nonexistentKey", ZERO_CURSOR, "ANY");
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenIncorrectOptionalArgumentAndKeyExists_returnsSyntaxError() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY))
        .hasMessageContaining("ERR wrong number of arguments for 'sscan' command");
  }

  @Test
  public void givenIncorrectOptionalArgumentsAndKeyExists_returnsSyntaxError() {
    jedis.sadd(KEY, "1");
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "a*"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenMatchArgumentWithoutPatternOnExistingKey_returnsSyntaxError() {
    jedis.sadd(KEY, "1");
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "MATCH"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCountArgumentWithoutNumberOnExistingKey_returnsSyntaxError() {
    jedis.sadd(KEY, "1");
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenMatchOrCountKeywordNotSpecified_returnsSyntaxError() {
    jedis.sadd(KEY, "1");
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "a*", "1"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.sadd(KEY, "1");
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "MATCH"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.sadd(KEY, "1");
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "12",
            "COUNT", "sjlfs", "COUNT", "1"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsLessThanOne_returnsSyntaxError() {
    jedis.sadd(KEY, "1");
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "12",
            "COUNT", "0", "COUNT", "1"))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsZero_returnsSyntaxError() {
    jedis.sadd(KEY, "1");
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "0"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNegative_returnsSyntaxError() {
    jedis.sadd(KEY, "1");
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
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, "not-int"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_andCursorIsNotInteger_returnsInvalidCursorError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("notReal", Protocol.Command.SSCAN, "notReal", "notReal", "not-int"))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenExistentSetKey_andCursorIsNotAnInteger_returnsInvalidCursorError() {
    jedis.set(KEY, "b");
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, "not-int"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNegativeCursor_doesNotError() {
    initializeThousandMemberSet();
    assertThatNoException().isThrownBy(() -> jedis.sscan(KEY, "-1"));
  }

  @Test
  public void givenSetWithOneMember_returnsMember() {
    jedis.sadd(KEY, "1");

    ScanResult<String> result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsOnly(FIELD_ONE);
  }

  @Test
  public void givenSetWithMultipleMembers_returnsSubsetOfMembers() {
    final Set<String> initialMemberData = initializeThousandMemberSet();

    ScanResult<String> result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.getResult()).isSubsetOf(initialMemberData);
  }

  @Test
  public void givenCount_returnsAllMembersWithoutDuplicates() {
    Set<byte[]> initialTotalSet = initializeThousandMemberByteSet();
    int count = 10;
    ScanParams scanParams = new ScanParams();
    scanParams.count(10);

    result = jedis.sscan(KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);

    assertThat(result.getResult().size()).isGreaterThanOrEqualTo(count);
    assertThat(result.getResult()).isSubsetOf(initialTotalSet);
  }

  @Test
  public void givenMultipleCounts_returnsAllEntriesWithoutDuplicates() {
    Set<byte[]> initialMemberData = initializeThousandMemberByteSet();
    List<byte[]> allEntries = new ArrayList<>();
    String cursor = ZERO_CURSOR;

    do {
      result = sendCustomSscanCommand(KEY, KEY, cursor, "COUNT", "1", "COUNT", "2");
      cursor = result.getCursor();
      allEntries.addAll(result.getResult());
    } while (!cursor.equals(ZERO_CURSOR));

    assertThat(allEntries).containsExactlyInAnyOrderElementsOf(initialMemberData);
  }

  @Test
  public void givenSetWithThreeEntriesAndMatch_returnsOnlyMatchingElements() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);
    ScanParams scanParams = new ScanParams();
    scanParams.match("1*");

    result = jedis.sscan(KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsOnly(FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes());
  }

  @Test
  public void givenSetWithThreeEntriesAndMultipleMatchArguments_returnsOnlyElementsMatchingLastMatchArgument() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);

    result = sendCustomSscanCommand(KEY, KEY, ZERO_CURSOR, "MATCH", "3*", "MATCH", "1*");

    assertThat(result.getCursor()).isEqualTo(ZERO_CURSOR);
    assertThat(result.getResult()).containsOnly(FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes());
  }

  @Test
  public void givenSetWithThreeMembersAndMatchAndCount_returnsAllMatchingMembers() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);
    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    scanParams.match("1*");
    List<byte[]> allMembersFromScan = new ArrayList<>();
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.sscan(KEY.getBytes(), cursor.getBytes(), scanParams);
      allMembersFromScan.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allMembersFromScan).containsOnly(FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes());
  }

  @Test
  public void givenSetWithThreeMembersAndMultipleMatchAndCountArguments_returnsAllMatchingMembers() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);
    List<byte[]> allEntries = new ArrayList<>();
    String cursor = ZERO_CURSOR;

    do {
      result = sendCustomSscanCommand(KEY, KEY, cursor, "COUNT", "37", "MATCH", "3*", "COUNT",
          FIELD_TWO, "COUNT", FIELD_ONE, "MATCH", "1*");
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!cursor.equals(ZERO_CURSOR));

    assertThat(allEntries).containsOnly(FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes());
  }

  @Test
  public void givenNonMatchingPattern_returnsEmptyResult() {
    jedis.sadd(KEY, "cat dog emu");
    ScanParams scanParams = new ScanParams();
    scanParams.match("*fish*");

    ScanResult<byte[]> result =
        jedis.sscan(KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);

    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void should_notReturnValue_givenValueWasRemovedBeforeSscanIsCalled() {
    initializeThreeMemberSet();

    jedis.srem(KEY, FIELD_THREE);
    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(jedis.sismember(KEY, FIELD_THREE)).isFalse());
    ScanResult<String> result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.getResult()).doesNotContain(FIELD_THREE);
  }

  @Test
  public void should_notErrorGivenNonzeroCursorOnFirstCall() {
    initializeThreeMemberSet();
    assertThatNoException().isThrownBy(() -> jedis.sscan(KEY, "5"));
  }

  @Test
  public void should_notErrorGivenCountEqualToIntegerMaxValue() {
    Set<byte[]> set = initializeThreeMemberByteSet();
    ScanParams scanParams = new ScanParams().count(Integer.MAX_VALUE);

    ScanResult<byte[]> result =
        jedis.sscan(KEY.getBytes(), ZERO_CURSOR.getBytes(), scanParams);

    assertThat(result.getResult())
        .containsExactlyInAnyOrderElementsOf(set);
  }

  @Test
  public void should_notErrorGivenCountGreaterThanIntegerMaxValue() {
    initializeThreeMemberByteSet();
    String greaterThanInt = String.valueOf(2L * Integer.MAX_VALUE);

    result = sendCustomSscanCommand(KEY, KEY, ZERO_CURSOR, "COUNT", greaterThanInt);

    assertThat(result.getCursor()).isEqualTo(ZERO_CURSOR);
    assertThat(result.getResult()).containsExactlyInAnyOrder(
        FIELD_ONE.getBytes(),
        FIELD_TWO.getBytes(),
        FIELD_THREE.getBytes());
  }

  /**** Concurrency ***/

  @Test
  public void should_returnAllConsistentlyPresentMembers_givenConcurrentThreadsAddingAndRemovingMembers() {
    final Set<String> initialMemberData = initializeThousandMemberSet();
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
    final Set<String> initialMemberData = initializeThousandMemberSet();
    jedis.sadd(KEY, initialMemberData.toArray(new String[0]));
    final int iterationCount = 500;
    Jedis jedis1 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);
    Jedis jedis2 = jedis.getConnectionFromSlot(SLOT_FOR_KEY);

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis1, initialMemberData),
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis2, initialMemberData))
            .run();
    assertThat(jedis.smembers(KEY)).containsExactlyInAnyOrderElementsOf(initialMemberData);

    jedis1.close();
    jedis2.close();
  }

  private void multipleSScanAndAssertOnContentOfResultSet(int iteration, Jedis jedis,
      final Set<String> initialMemberData) {

    List<String> allEntries = new ArrayList<>();
    ScanResult<String> result;
    String cursor = ZERO_CURSOR;

    do {
      result = jedis.sscan(KEY, cursor);
      cursor = result.getCursor();
      List<String> resultEntries = result.getResult();
      allEntries.addAll(resultEntries);
    } while (!result.isCompleteIteration());

    assertThat(allEntries).as("failed on iteration " + iteration)
        .containsAll(initialMemberData);
  }

  @SuppressWarnings("unchecked")
  private ScanResult<byte[]> sendCustomSscanCommand(String key, String... args) {
    List<Object> result = (List<Object>) (jedis.sendCommand(key, Protocol.Command.SSCAN, args));
    return new ScanResult<>((byte[]) result.get(0), (List<byte[]>) result.get(1));
  }

  private void initializeThreeMemberSet() {
    jedis.sadd(KEY, FIELD_ONE, FIELD_TWO, FIELD_THREE);
  }

  private Set<byte[]> initializeThreeMemberByteSet() {
    Set<byte[]> set = new HashSet<>();
    set.add(FIELD_ONE.getBytes());
    set.add(FIELD_TWO.getBytes());
    set.add(FIELD_THREE.getBytes());
    jedis.sadd(KEY.getBytes(), FIELD_ONE.getBytes(), FIELD_TWO.getBytes(), FIELD_THREE.getBytes());
    return set;
  }

  private Set<String> initializeThousandMemberSet() {
    Set<String> set = new HashSet<>();
    int SIZE_OF_SET = 1000;
    for (int i = 0; i < SIZE_OF_SET; i++) {
      set.add((BASE_FIELD + i));
    }
    jedis.sadd(KEY, set.toArray(new String[0]));
    return set;
  }

  private Set<byte[]> initializeThousandMemberByteSet() {
    Set<byte[]> set = new HashSet<>();
    int SIZE_OF_SET = 1000;
    for (int i = 0; i < SIZE_OF_SET; i++) {
      byte[] memberToAdd = Integer.toString(i).getBytes();
      set.add(memberToAdd);
      jedis.sadd(KEY.getBytes(), memberToAdd);
    }
    return set;
  }

}
