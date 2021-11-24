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
package org.apache.geode.redis.internal.commands.executor.sortedset;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_MIN_MAX_NOT_A_VALID_STRING;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

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
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public abstract class AbstractZLexCountIntegrationTest implements RedisIntegrationTest {
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
    assertAtLeastNArgs(jedis, Protocol.Command.ZLEXCOUNT, 3);
  }

  @Test
  @Parameters({"a", "--", "++"})
  public void shouldError_givenInvalidMinOrMax(String invalidArgument) {
    assertThatThrownBy(() -> jedis.zlexcount("fakeKey", invalidArgument, "+"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_VALID_STRING);
    assertThatThrownBy(() -> jedis.zlexcount("fakeKey", "-", invalidArgument))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_VALID_STRING);
    assertThatThrownBy(() -> jedis.zlexcount("fakeKey", invalidArgument, invalidArgument))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_VALID_STRING);
  }

  @Test
  public void shouldReturnZero_givenNonExistentKey() {
    assertThat(jedis.zlexcount("fakeKey", "-", "+")).isEqualTo(0);
  }

  @Test
  public void shouldReturnZero_givenMinGreaterThanMax() {
    jedis.zadd(KEY, SCORE, "member");

    // Range + <= result <= -
    assertThat(jedis.zlexcount(KEY, "+", "-")).isEqualTo(0);
    // Range z <= result <= a
    assertThat(jedis.zlexcount(KEY, "[z", "[a")).isEqualTo(0);
  }

  @Test
  public void shouldReturnOne_givenMemberNameInRange() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range m <= result <= n
    assertThat(jedis.zlexcount(KEY, "[m", "[n")).isEqualTo(1);
    // Range -infinity <= result <= n
    assertThat(jedis.zlexcount(KEY, "-", "[n")).isEqualTo(1);
    // Range m <= result <= +infinity
    assertThat(jedis.zlexcount(KEY, "[m", "+")).isEqualTo(1);
  }

  @Test
  public void shouldReturnOne_givenMinEqualToMemberNameAndMinInclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range member <= result <= n
    assertThat(jedis.zlexcount(KEY, "[" + memberName, "[n")).isEqualTo(1);
  }

  @Test
  public void shouldReturnOne_givenMaxEqualToMemberNameAndMaxInclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range a <= result <= member
    assertThat(jedis.zlexcount(KEY, "[a", "[" + memberName)).isEqualTo(1);
  }

  @Test
  public void shouldReturnOne_givenMinAndMaxEqualToMemberNameAndMinAndMaxInclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    assertThat(jedis.zlexcount(KEY, "[" + memberName, "[" + memberName)).isEqualTo(1);
  }

  @Test
  @Parameters({"[", "(", "", "-", "+"})
  public void shouldReturnOne_givenMemberNameIsSpecialCharacter(String memberName) {
    jedis.zadd(KEY, SCORE, memberName);

    assertThat(jedis.zlexcount(KEY, "[" + memberName, "[" + memberName)).isEqualTo(1);
  }

  @Test
  public void shouldReturnZero_givenMinEqualToMemberNameAndMinExclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range member < result <= n
    assertThat(jedis.zlexcount(KEY, "(" + memberName, "[n")).isEqualTo(0);
  }

  @Test
  public void shouldReturnZero_givenMaxEqualToMemberNameAndMaxExclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range a <= result < member
    assertThat(jedis.zlexcount(KEY, "[a", "(" + memberName)).isEqualTo(0);
  }

  @Test
  public void shouldReturnZero_givenRangeExcludingMember() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range n <= result <= o
    assertThat(jedis.zlexcount(KEY, "[n", "[o")).isEqualTo(0);
  }

  @Test
  public void shouldReturnCount_givenMultipleMembersInRange_withInclusiveMinAndMax() {
    populateSortedSet();

    int minLength = 3;
    int maxLength = 6;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    // Range (v * 3) <= result <= (v * 6)
    assertThat(jedis.zlexcount(KEY, "[" + min, "[" + max)).isEqualTo(4);
  }

  @Test
  public void shouldReturnCount_givenMultipleMembersInRange_withExclusiveMinAndMax() {
    populateSortedSet();

    int minLength = 1;
    int maxLength = 7;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    // Range (v * 1) < result < (v * 7)
    assertThat(jedis.zlexcount(KEY, "(" + min, "(" + max)).isEqualTo(5);
  }

  @Test
  public void shouldReturnCount_givenMultipleMembersInRange_withInclusiveMinAndExclusiveMax() {
    populateSortedSet();

    int minLength = 5;
    int maxLength = 8;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    // Range (v * 5) <= result < (v * 8)
    assertThat(jedis.zlexcount(KEY, "[" + min, "(" + max)).isEqualTo(3);
  }

  @Test
  public void shouldReturnCount_givenMultipleMembersInRange_withExclusiveMinAndInclusiveMax() {
    populateSortedSet();

    int minLength = 2;
    int maxLength = 5;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    // Range (v * 2) < result <= (v * 5)
    assertThat(jedis.zlexcount(KEY, "(" + min, "[" + max)).isEqualTo(3);
  }

  @Test
  public void shouldReturnCount_givenMultipleMembersInRangeUsingMinusAndPlusArguments() {
    populateSortedSet();

    int minLength = 4;
    int maxLength = 8;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    // Range -infinity <= result <= (v * 8)
    assertThat(jedis.zlexcount(KEY, "-", "[" + max)).isEqualTo(8);

    // Range (v * 4) <= result < +infinity
    assertThat(jedis.zlexcount(KEY, "[" + min, "+")).isEqualTo(7);

    // Range -infinity <= result < +infinity
    assertThat(jedis.zlexcount(KEY, "-", "+")).isEqualTo(10);
  }

  // Add 10 members with the same score and member names consisting of 'v' repeated an increasing
  // number of times
  private List<String> populateSortedSet() {
    List<String> members = new ArrayList<>();
    String memberName = BASE_MEMBER_NAME;
    for (int i = 0; i < 10; ++i) {
      jedis.zadd(KEY, SCORE, memberName);
      members.add(memberName);
      memberName += BASE_MEMBER_NAME;
    }
    return members;
  }
}
