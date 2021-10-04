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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_MIN_MAX_NOT_A_VALID_STRING;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import junitparams.Parameters;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public abstract class AbstractZRevRangeByLexIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final int SCORE = 1;
  public static final String BASE_MEMBER_NAME = "v";

  JedisCluster jedis;

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
    assertAtLeastNArgs(jedis, Protocol.Command.ZREVRANGEBYLEX, 3);
  }

  @Test
  @Parameters({"a", "--", "++"})
  public void shouldError_givenInvalidMinOrMax(String invalidArgument) {
    assertThatThrownBy(() -> jedis.zrevrangeByLex("fakeKey", invalidArgument, "-"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_VALID_STRING);
    assertThatThrownBy(() -> jedis.zrevrangeByLex("fakeKey", "+", invalidArgument))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_VALID_STRING);
    assertThatThrownBy(() -> jedis.zrevrangeByLex("fakeKey", invalidArgument, invalidArgument))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_VALID_STRING);
  }

  @Test
  public void shouldError_givenInvalidLimitFormat() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "+", "-",
        "LIMIT"))
            .hasMessageContaining(ERROR_SYNTAX);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "+", "-",
        "LIMIT", "0"))
            .hasMessageContaining(ERROR_SYNTAX);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "+", "-",
        "LAMAT", "0", "10"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenNonIntegerLimitArguments() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "+", "-",
        "LIMIT", "0", "invalid"))
            .hasMessageContaining(ERROR_NOT_INTEGER);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "+", "-",
        "LIMIT", "invalid", "10"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldError_givenNegativeZeroLimitOffset() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "+", "-",
        "LIMIT", "-0", "10"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldError_givenMultipleLimits_withFirstLimitIncorrectlySpecified() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "+", "-",
        "LIMIT", "0", "invalid",
        "LIMIT", "0", "10"))
            .hasMessageContaining(ERROR_NOT_INTEGER);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "+", "-",
        "LIMIT", "0",
        "LIMIT", "0", "10"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldError_givenMultipleLimits_withLastLimitIncorrectlySpecified() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "+", "-",
        "LIMIT", "0", "10",
        "LIMIT", "0", "invalid"))
            .hasMessageContaining(ERROR_NOT_INTEGER);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "+", "-",
        "LIMIT", "0", "10",
        "LIMIT", "0"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void shouldReturnEmptyCollection_givenNonExistentKey() {
    assertThat(jedis.zrevrangeByLex("fakeKey", "+", "-")).isEmpty();
  }

  @Test
  public void shouldReturnEmptyCollection_givenMinGreaterThanMax() {
    jedis.zadd(KEY, SCORE, "member");

    // Range - >= member name >= +
    assertThat(jedis.zrevrangeByLex(KEY, "-", "+")).isEmpty();
    // Range a >= member name >= z
    assertThat(jedis.zrevrangeByLex(KEY, "[a", "[z")).isEmpty();
  }

  @Test
  public void shouldReturnMember_givenMemberNameInRange() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range n >= member name >= m
    assertThat(jedis.zrevrangeByLex(KEY, "[n", "[m")).containsExactly(memberName);
    // Range n >= member name >= -
    assertThat(jedis.zrevrangeByLex(KEY, "[n", "-")).containsExactly(memberName);
    // Range + >= member name >= m
    assertThat(jedis.zrevrangeByLex(KEY, "+", "[m")).containsExactly(memberName);
  }

  @Test
  public void shouldReturnMember_givenMinEqualToMemberNameAndMinInclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range n >= member name >= member
    assertThat(jedis.zrevrangeByLex(KEY, "[n", "[" + memberName)).containsExactly(memberName);
  }

  @Test
  public void shouldReturnMember_givenMaxEqualToMemberNameAndMaxInclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range member >= member name >= a
    assertThat(jedis.zrevrangeByLex(KEY, "[" + memberName, "[a")).containsExactly(memberName);
  }

  @Test
  public void shouldReturnMember_givenMinAndMaxEqualToMemberNameAndMinAndMaxInclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range member >= member name >= member
    assertThat(jedis.zrevrangeByLex(KEY, "[" + memberName, "[" + memberName))
        .containsExactly(memberName);
  }

  @Test
  @Parameters({"[", "(", "", "+", "-"})
  public void shouldReturnMember_givenMemberNameIsSpecialCharacter(String memberName) {
    jedis.zadd(KEY, SCORE, memberName);

    assertThat(jedis.zrevrangeByLex(KEY, "[" + memberName, "[" + memberName))
        .containsExactly(memberName);
  }

  @Test
  public void shouldReturnEmptyCollection_givenMinEqualToMemberNameAndMinExclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range n >= member name > member
    assertThat(jedis.zrevrangeByLex(KEY, "[n", "(" + memberName)).isEmpty();
  }

  @Test
  public void shouldReturnEmptyCollection_givenMaxEqualToMemberNameAndMaxExclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range member > member name >= a
    assertThat(jedis.zrevrangeByLex(KEY, "(" + memberName, "[a")).isEmpty();
  }

  @Test
  public void shouldReturnEmptyCollection_givenRangeExcludingMember() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range o >= member name >= n
    assertThat(jedis.zrevrangeByLex(KEY, "[o", "[n")).isEmpty();
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersInRange_withInclusiveMinAndMax() {
    List<String> members = populateSortedSet();

    int minLength = 3;
    int maxLength = 6;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    int sublistMin = members.size() - maxLength;
    int sublistMax = members.size() - minLength + 1;
    List<String> expected = members.subList(sublistMin, sublistMax);

    // Range (v * 6) >= member name >= (v * 3)
    assertThat(jedis.zrevrangeByLex(KEY, "[" + max, "[" + min))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersInRange_withExclusiveMinAndMax() {
    List<String> members = populateSortedSet();

    int minLength = 1;
    int maxLength = 7;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    int sublistMin = members.size() - maxLength + 1;
    int sublistMax = members.size() - minLength;
    List<String> expected = members.subList(sublistMin, sublistMax);

    // Range (v * 7) > member name > (v * 1)
    assertThat(jedis.zrevrangeByLex(KEY, "(" + max, "(" + min))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersInRange_withInclusiveMinAndExclusiveMax() {
    List<String> members = populateSortedSet();

    int minLength = 5;
    int maxLength = 8;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    int sublistMin = members.size() - maxLength + 1;
    int sublistMax = members.size() - minLength + 1;
    List<String> expected = members.subList(sublistMin, sublistMax);

    // Range (v * 8) > member name >= (v * 5)
    assertThat(jedis.zrevrangeByLex(KEY, "(" + max, "[" + min))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersInRange_withExclusiveMinAndInclusiveMax() {
    List<String> members = populateSortedSet();

    int minLength = 2;
    int maxLength = 5;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    List<String> expected = members.subList(members.size() - maxLength, members.size() - minLength);

    // Range (v * 5) >= member name > (v * 2)
    assertThat(jedis.zrevrangeByLex(KEY, "[" + max, "(" + min))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersInRangeUsingMinusAndPlusArguments() {
    List<String> members = populateSortedSet();

    int minLength = 4;
    int maxLength = 8;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    List<String> expected = members.subList(members.size() - maxLength, members.size());

    // Range (v * 8) >= member name >= -infinity
    assertThat(jedis.zrevrangeByLex(KEY, "[" + max, "-"))
        .containsExactlyElementsOf(expected);

    expected = members.subList(0, members.size() - minLength + 1);

    // Range +infinity > member name >= (v * 4)
    assertThat(jedis.zrevrangeByLex(KEY, "+", "[" + min))
        .containsExactlyElementsOf(expected);

    // Range +infinity >= member name >= -infinity
    assertThat(jedis.zrevrangeByLex(KEY, "+", "-"))
        .containsExactlyElementsOf(members);
  }

  @Test
  public void shouldReturnRange_givenValidLimit() {
    List<String> members = populateSortedSet();

    int minLength = 1;
    int maxLength = 7;

    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    int offset = 2;
    int count = 3;

    int sublistMin = members.size() - maxLength + offset;
    int sublistMax = sublistMin + count;

    List<String> expected = members.subList(sublistMin, sublistMax);

    // Range (v * 7) >= member name >= (v * 1), offset = 2, count = 3
    assertThat(jedis.zrevrangeByLex(KEY, "[" + max, "[" + min, offset, count))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnAllElementsInRange_givenNegativeCount() {
    List<String> members = populateSortedSet();

    int minLength = 1;

    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);

    int offset = 2;

    int sublistMin = minLength + offset - 1;
    List<String> expected = members.subList(sublistMin, members.size());

    // Range +infinity >= member name >= (v * 1), offset = 2, count = -1
    assertThat(jedis.zrevrangeByLex(KEY, "+", "[" + min, offset, -1))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnRange_givenCountLargerThanRange() {
    List<String> members = populateSortedSet();

    int minLength = 4;
    int maxLength = 6;

    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    int sublistMin = members.size() - maxLength;
    int sublistMax = members.size() - minLength + 1;
    List<String> expected = members.subList(sublistMin, sublistMax);

    // Range (v * 6) >= member name >= (v * 4), offset = 0, count = 10
    assertThat(jedis.zrevrangeByLex(KEY, "[" + max, "[" + min, 0, 10))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnEmptyCollection_givenNonZeroNegativeLimitOffset() {
    populateSortedSet();

    assertThat(jedis.zrevrangeByLex(KEY, "+", "-", -7, 10)).isEmpty();
  }

  @Test
  public void shouldReturnEmptyCollection_givenOffsetLargerThanRange() {
    populateSortedSet();

    int minLength = 1;
    int maxLength = 3;

    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    int offset = 7;

    // Range (v * 3) >= member name >= (v * 1), offset = 7, count = 10
    assertThat(jedis.zrevrangeByLex(KEY, "[" + max, "[" + min, offset, 10)).isEmpty();
  }

  @Test
  public void shouldUseLastLimit_givenMultipleValidLimitsProvided() {
    List<String> members = populateSortedSet();

    int minLength = 2;
    int maxLength = 8;

    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    int offset = 2;
    int count = 3;

    int sublistMin = members.size() - maxLength + offset;
    int sublistMax = sublistMin + count;

    // Add 1 to sublistMax, as subList uses exclusive maximum
    List<String> expected = members.subList(sublistMin, sublistMax);

    // Range (v * 8) >= member name >= (v * 2), offset = 2, count = 3
    List<byte[]> result = uncheckedCast(
        jedis.sendCommand(KEY, Protocol.Command.ZREVRANGEBYLEX, KEY, "[" + max, "[" + min,
            "LIMIT", "0", "10",
            "LIMIT", String.valueOf(offset), String.valueOf(count)));

    List<String> actual = result.stream().map(Coder::bytesToString).collect(Collectors.toList());
    assertThat(actual).containsExactlyElementsOf(expected);
  }

  // Add 10 members with the same score and member names consisting of 'v' repeated a decreasing
  // number of times
  private List<String> populateSortedSet() {
    List<String> members = new ArrayList<>();
    for (int i = 10; i > 0; --i) {
      String memberName = StringUtils.repeat("v", i);
      jedis.zadd(KEY, SCORE, memberName);
      members.add(memberName);
    }
    return members;
  }
}
