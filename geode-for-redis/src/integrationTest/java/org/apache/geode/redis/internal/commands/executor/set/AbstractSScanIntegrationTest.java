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
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.data.KeyHashUtil;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public abstract class AbstractSScanIntegrationTest implements RedisIntegrationTest {
  protected JedisCluster jedis;
  public static final String KEY = "key";
  public static final byte[] KEY_BYTES = KEY.getBytes();
  public static final int SLOT_FOR_KEY = KeyHashUtil.slotForKey(KEY_BYTES);
  public static final String ZERO_CURSOR = "0";
  public static final byte[] ZERO_CURSOR_BYTES = ZERO_CURSOR.getBytes();
  public static final BigInteger SIGNED_LONG_MAX = new BigInteger(Long.toString(Long.MAX_VALUE));
  public static final BigInteger SIGNED_LONG_MIN = new BigInteger(Long.toString(Long.MIN_VALUE));
  public static final String MEMBER_ONE = "1";
  public static final String MEMBER_TWELVE = "12";
  public static final String MEMBER_THREE = "3";
  public static final String BASE_MEMBER_NAME = "baseMember_";

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(RedisClusterStartupRule.BIND_ADDRESS, getPort()),
        RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT);
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
    ScanResult<String> result = jedis.sscan("nonexistentKey", ZERO_CURSOR);
    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenNonexistentKeyAndIncorrectOptionalArguments_returnsEmptyArray() {
    ScanResult<byte[]> result =
        sendCustomSscanCommand("nonexistentKey", "nonexistentKey", ZERO_CURSOR, "ANY");
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenIncorrectOptionalArgumentsAndKeyExists_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "a*"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenMatchArgumentWithoutPatternOnExistingKey_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "MATCH"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCountArgumentWithoutNumberOnExistingKey_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenAdditionalArgumentNotEqualToMatchOrCount_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "a*", "1"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT",
            "notAnInteger"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "12",
            "COUNT", "notAnInteger", "COUNT", "1"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsLessThanOne_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "12",
            "COUNT", "0", "COUNT", "1"))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsZero_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, new ScanParams().count(0)))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNegative_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, new ScanParams().count(-37)))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenKeyIsNotASet_returnsWrongTypeError() {
    jedis.hset(KEY, "b", MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sscan(KEY, ZERO_CURSOR))
            .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void givenKeyIsNotASetAndCountIsNegative_returnsWrongTypeError() {
    jedis.hset(KEY, "b", MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, new ScanParams().count(-37)))
            .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void givenKeyIsNotASet_andCursorIsNotAnInteger_returnsInvalidCursorError() {
    jedis.hset(KEY, "b", MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sscan(KEY, "notAnInteger"))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_andCursorIsNotInteger_returnsInvalidCursorError() {
    assertThatThrownBy(
        () -> jedis.sscan("nonexistentKey", "notAnInteger"))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenExistentSetKey_andCursorIsNotAnInteger_returnsInvalidCursorError() {
    jedis.set(KEY, "b");
    assertThatThrownBy(
        () -> jedis.sscan(KEY, "notAnInteger"))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNegativeCursor_doesNotError() {
    initializeThousandMemberSet();
    assertThatNoException().isThrownBy(() -> jedis.sscan(KEY, "-1"));
  }

  @Test
  public void givenSetWithOneMember_returnsMember() {
    jedis.sadd(KEY, MEMBER_ONE);

    ScanResult<String> result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsOnly(MEMBER_ONE);
  }

  @Test
  public void givenSetWithMultipleMembers_returnsSubsetOfMembers() {
    Set<String> initialMemberData = initializeThousandMemberSet();

    ScanResult<String> result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.getResult()).isSubsetOf(initialMemberData);
  }

  @Test
  public void givenCount_returnsAllMembersWithoutDuplicates() {
    Set<byte[]> initialTotalSet = initializeThousandMemberByteSet();
    int count = 99;

    ScanResult<byte[]> result =
        jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, new ScanParams().count(count));

    assertThat(result.getResult().size()).isGreaterThanOrEqualTo(count);
    assertThat(result.getResult()).isSubsetOf(initialTotalSet);
  }

  @Test
  public void givenMultipleCounts_usesLastCountSpecified() {
    Set<byte[]> initialMemberData = initializeThousandMemberByteSet();
    // Choose two COUNT arguments with a large difference, so that it's extremely unlikely that if
    // the first COUNT is used, a number of members greater than or equal to the second COUNT will
    // be returned.
    int firstCount = 1;
    int secondCount = 500;
    ScanResult<byte[]> result = sendCustomSscanCommand(KEY, KEY, ZERO_CURSOR,
        "COUNT", String.valueOf(firstCount),
        "COUNT", String.valueOf(secondCount));

    List<byte[]> returnedMembers = result.getResult();
    assertThat(returnedMembers.size()).isGreaterThanOrEqualTo(secondCount);
    assertThat(returnedMembers).isSubsetOf(initialMemberData);
  }

  @Test
  public void givenSetWithThreeEntriesAndMatch_returnsOnlyMatchingElements() {
    jedis.sadd(KEY, MEMBER_ONE, MEMBER_TWELVE, MEMBER_THREE);
    ScanParams scanParams = new ScanParams().match("1*");

    ScanResult<byte[]> result = jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, scanParams);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsOnly(MEMBER_ONE.getBytes(),
        MEMBER_TWELVE.getBytes());
  }

  @Test
  public void givenSetWithThreeEntriesAndMultipleMatchArguments_returnsOnlyElementsMatchingLastMatchArgument() {
    jedis.sadd(KEY, MEMBER_ONE, MEMBER_TWELVE, MEMBER_THREE);

    ScanResult<byte[]> result =
        sendCustomSscanCommand(KEY, KEY, ZERO_CURSOR, "MATCH", "3*", "MATCH", "1*");

    assertThat(result.getCursor()).isEqualTo(ZERO_CURSOR);
    assertThat(result.getResult()).containsOnly(MEMBER_ONE.getBytes(),
        MEMBER_TWELVE.getBytes());
  }

  @Test
  public void givenLargeCountAndMatch_returnsOnlyMatchingMembers() {
    Set<byte[]> initialMemberData = initializeThousandMemberByteSet();
    ScanParams scanParams = new ScanParams();
    // There are 111 matching members in the set 0..999
    scanParams.match("9*");
    // Choose a large COUNT to ensure that some matching members are returned
    scanParams.count(950);

    ScanResult<byte[]> result = jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, scanParams);

    List<byte[]> returnedMembers = result.getResult();
    // We know that we must have found at least 61 matching members, given the size of COUNT and the
    // number of matching members in the set
    assertThat(returnedMembers.size()).isGreaterThanOrEqualTo(61);
    assertThat(returnedMembers).isSubsetOf(initialMemberData);
    assertThat(returnedMembers).allSatisfy(bytes -> assertThat(new String(bytes)).startsWith("9"));
  }

  @Test
  public void givenMultipleCountAndMatch_usesLastSpecified() {
    Set<byte[]> initialMemberData = initializeThousandMemberByteSet();
    // Choose a large COUNT to ensure that some matching members are returned
    // There are 111 matching members in the set 0..999

    ScanResult<byte[]> result = sendCustomSscanCommand(KEY, KEY, ZERO_CURSOR,
        "COUNT", "20",
        "MATCH", "1*",
        "COUNT", "950",
        "MATCH", "9*");

    List<byte[]> returnedMembers = result.getResult();
    // We know that we must have found at least 61 matching members, given the size of COUNT and the
    // number of matching members in the set
    assertThat(returnedMembers.size()).isGreaterThanOrEqualTo(61);
    assertThat(returnedMembers).isSubsetOf(initialMemberData);
    assertThat(returnedMembers).allSatisfy(bytes -> assertThat(new String(bytes)).startsWith("9"));
  }

  @Test
  public void givenNonMatchingPattern_returnsEmptyResult() {
    jedis.sadd(KEY, "cat dog elk");
    ScanParams scanParams = new ScanParams().match("*fish*");

    ScanResult<byte[]> result = jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, scanParams);

    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void should_notReturnValue_givenValueWasRemovedBeforeSscanIsCalled() {
    initializeThreeMemberSet();
    jedis.srem(KEY, MEMBER_THREE);
    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(jedis.sismember(KEY, MEMBER_THREE)).isFalse());

    ScanResult<String> result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.getResult()).doesNotContain(MEMBER_THREE);
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

    ScanResult<byte[]> result = jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, scanParams);

    assertThat(result.getResult())
        .containsExactlyInAnyOrderElementsOf(set);
  }

  @Test
  public void should_notErrorGivenCountGreaterThanIntegerMaxValue() {
    initializeThreeMemberByteSet();
    String greaterThanInt = String.valueOf(2L * Integer.MAX_VALUE);

    ScanResult<byte[]> result =
        sendCustomSscanCommand(KEY, KEY, ZERO_CURSOR, "COUNT", greaterThanInt);

    assertThat(result.getCursor()).isEqualTo(ZERO_CURSOR);
    assertThat(result.getResult()).containsExactlyInAnyOrder(
        MEMBER_ONE.getBytes(),
        MEMBER_TWELVE.getBytes(),
        MEMBER_THREE.getBytes());
  }

  /**** Concurrency ***/

  @Test
  public void should_returnAllConsistentlyPresentMembers_givenConcurrentThreadsAddingAndRemovingMembers() {
    final Set<String> initialMemberData = initializeThousandMemberSet();
    final int iterationCount = 500;
    final Jedis jedis1 = new Jedis(jedis.getConnectionFromSlot(SLOT_FOR_KEY));
    final Jedis jedis2 = new Jedis(jedis.getConnectionFromSlot(SLOT_FOR_KEY));

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis1, initialMemberData),
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis2, initialMemberData),
        (i) -> {
          String member = "new_" + BASE_MEMBER_NAME + i;
          jedis.sadd(KEY, member);
          jedis.srem(KEY, member);
        }).run();

    jedis1.close();
    jedis2.close();
  }

  @Test
  public void should_notAlterUnderlyingData_givenMultipleConcurrentSscans() {
    final Set<String> initialMemberData = initializeThousandMemberSet();
    jedis.sadd(KEY, initialMemberData.toArray(new String[0]));
    final int iterationCount = 500;
    Jedis jedis1 = new Jedis(jedis.getConnectionFromSlot(SLOT_FOR_KEY));
    Jedis jedis2 = new Jedis(jedis.getConnectionFromSlot(SLOT_FOR_KEY));

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
    String cursor = ZERO_CURSOR;
    ScanResult<String> result;

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
    jedis.sadd(KEY, MEMBER_ONE, MEMBER_TWELVE, MEMBER_THREE);
  }

  private Set<byte[]> initializeThreeMemberByteSet() {
    Set<byte[]> set = new HashSet<>();
    set.add(MEMBER_ONE.getBytes());
    set.add(MEMBER_TWELVE.getBytes());
    set.add(MEMBER_THREE.getBytes());
    jedis.sadd(KEY_BYTES, MEMBER_ONE.getBytes(), MEMBER_TWELVE.getBytes(),
        MEMBER_THREE.getBytes());
    return set;
  }

  private Set<String> initializeThousandMemberSet() {
    Set<String> set = new HashSet<>();
    int sizeOfSet = 1000;
    for (int i = 0; i < sizeOfSet; i++) {
      set.add((BASE_MEMBER_NAME + i));
    }
    jedis.sadd(KEY, set.toArray(new String[0]));
    return set;
  }

  private Set<byte[]> initializeThousandMemberByteSet() {
    Set<byte[]> set = new HashSet<>();
    int sizeOfSet = 1000;
    for (int i = 0; i < sizeOfSet; i++) {
      byte[] memberToAdd = Integer.toString(i).getBytes();
      set.add(memberToAdd);
      jedis.sadd(KEY_BYTES, memberToAdd);
    }
    return set;
  }

}
