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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_MIN_MAX_NOT_A_VALID_STRING;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junitparams.JUnitParamsRunner;
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

@RunWith(JUnitParamsRunner.class)
public abstract class AbstractZRemRangeByLexIntegrationTest implements RedisIntegrationTest {
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
    assertExactNumberOfArgs(jedis, Protocol.Command.ZREMRANGEBYLEX, 3);
  }

  @Test
  @Parameters({"a", "--", "++", "4"})
  public void shouldError_givenInvalidMinOrMax(String invalidArgument) {
    assertThatThrownBy(() -> jedis.zremrangeByLex("fakeKey", invalidArgument, "+"))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_VALID_STRING);
    assertThatThrownBy(() -> jedis.zremrangeByLex("fakeKey", "-", invalidArgument))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_VALID_STRING);
    assertThatThrownBy(() -> jedis.zremrangeByLex("fakeKey", invalidArgument, invalidArgument))
        .hasMessageContaining(ERROR_MIN_MAX_NOT_A_VALID_STRING);
  }

  @Test
  public void shouldReturnZero_givenNonExistentKey() {
    jedis.zadd(KEY, SCORE, "member1");
    assertThat(jedis.zremrangeByLex("fakeKey", "-", "+")).isEqualTo(0);
  }

  @Test
  public void shouldReturnZero_givenMinGreaterThanMax() {
    jedis.zadd(KEY, SCORE, "member");

    // Range + <= member name <= -
    assertThat(jedis.zrangeByLex(KEY, "+", "-")).isEmpty();
    // Range z <= member name <= a
    assertThat(jedis.zrangeByLex(KEY, "[z", "[a")).isEmpty();
  }

  @Test
  public void shouldReturnOne_givenOneMemberAndMemberNameInRangeInclusiveMtoN() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range m <= member name <= n
    assertThat(jedis.zremrangeByLex(KEY, "[m", "[n")).isEqualTo(1);
  }

  @Test
  public void shouldReturnOne_givenOneMemberAndMemberNameInRangeMinusToN() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range -infinity <= member name <= n
    assertThat(jedis.zremrangeByLex(KEY, "-", "[n")).isEqualTo(1);
  }

  @Test
  public void shouldReturnOne_givenOneMemberAndMemberNameInRangeInclusiveMToPlus() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range m <= member name <= +infinity
    assertThat(jedis.zremrangeByLex(KEY, "[m", "+")).isEqualTo(1);
  }

  @Test
  public void shouldReturnOne_givenOneMemberInRange_MinEqualToMemberNameAndMinInclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range member <= member name <= n
    assertThat(jedis.zremrangeByLex(KEY, "[" + memberName, "[n")).isEqualTo(1);
  }

  @Test
  public void shouldReturnOne_givenMaxEqualToMemberNameAndMaxInclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range a <= member name <= member
    assertThat(jedis.zremrangeByLex(KEY, "[a", "[" + memberName)).isEqualTo(1);
  }

  @Test
  public void shouldReturnOne_givenMinAndMaxEqualToMemberNameAndMinAndMaxInclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    assertThat(jedis.zremrangeByLex(KEY, "[" + memberName, "[" + memberName)).isEqualTo(1);
  }

  @Test
  @Parameters({"[", "(", "", "-", "+"})
  public void shouldReturnOne_givenMemberNameIsSpecialCharacterInRange(String memberName) {
    jedis.zadd(KEY, SCORE, memberName);

    assertThat(jedis.zremrangeByLex(KEY, "[" + memberName, "[" + memberName)).isEqualTo(1);
  }

  @Test
  public void shouldReturnZero_givenMinEqualToMemberNameAndMinExclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range member < member name <= n
    assertThat(jedis.zremrangeByLex(KEY, "(" + memberName, "[n")).isEqualTo(0);
  }

  @Test
  public void shouldReturnZero_givenMaxEqualToMemberNameAndMaxExclusive() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range a <= member name < member
    assertThat(jedis.zremrangeByLex(KEY, "[a", "(" + memberName)).isEqualTo(0);
  }

  @Test
  public void shouldReturnZero_givenMemberNotInRange() {
    String memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range member name <= n <= o
    assertThat(jedis.zremrangeByLex(KEY, "[n", "[o")).isEqualTo(0);
  }

  @Test
  public void shouldReturnNumMembersRemoved_givenMultipleMembersInRange_withInclusiveMinAndMax() {
    List<String> members = populateSortedSet();

    int minLength = 3;
    int maxLength = 6;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    List<String> sublist = new ArrayList<>(members.subList(minLength - 1, maxLength));
    members.removeAll(sublist);

    // Range (v * 3) <= member name <= (v * 6)
    assertThat(jedis.zremrangeByLex(KEY, "[" + min, "[" + max)).isEqualTo(sublist.size());
    assertThat(jedis.zrange(KEY, 0, -1)).hasSameElementsAs(members);
  }

  @Test
  public void shouldReturnNumMembersRemoved_givenMultipleMembersInRange_withExclusiveMinAndMax() {
    List<String> members = populateSortedSet();

    int minLength = 1;
    int maxLength = 7;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    List<String> sublist = new ArrayList<>(members.subList(minLength, maxLength - 1));
    members.removeAll(sublist);

    // Range (v * 1) < member name < (v * 7)
    assertThat(jedis.zremrangeByLex(KEY, "(" + min, "(" + max)).isEqualTo(sublist.size());
    assertThat(jedis.zrange(KEY, 0, -1)).hasSameElementsAs(members);
  }

  @Test
  public void shouldReturnNumMembersRemoved_givenMultipleMembersInRange_withInclusiveMinAndExclusiveMax() {
    List<String> members = populateSortedSet();

    int minLength = 5;
    int maxLength = 8;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    List<String> sublist = new ArrayList<>(members.subList(minLength - 1, maxLength - 1));
    members.removeAll(sublist);

    // Range (v * 5) <= member name < (v * 8)
    assertThat(jedis.zremrangeByLex(KEY, "[" + min, "(" + max)).isEqualTo(sublist.size());
    assertThat(jedis.zrange(KEY, 0, -1)).hasSameElementsAs(members);
  }

  @Test
  public void shouldReturnNumMembersRemoved_givenMultipleMembersInRange_withExclusiveMinAndInclusiveMax() {
    List<String> members = populateSortedSet();

    int minLength = 2;
    int maxLength = 5;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    List<String> sublist = new ArrayList<>(members.subList(minLength, maxLength));
    members.removeAll(sublist);

    // Range (v * 2) < member name <= (v * 5)
    assertThat(jedis.zremrangeByLex(KEY, "(" + min, "[" + max)).isEqualTo(sublist.size());
    assertThat(jedis.zrange(KEY, 0, -1)).hasSameElementsAs(members);
  }

  @Test
  public void shouldReturnNumMembersRemoved_givenMinusAndInclusiveMaxRange() {
    List<String> members = populateSortedSet();

    int maxLength = 8;
    String max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    List<String> sublist = new ArrayList<>(members.subList(0, maxLength));
    members.removeAll(sublist);

    // Range -infinity <= member name <= (v * 8)
    assertThat(jedis.zremrangeByLex(KEY, "-", "[" + max)).isEqualTo(sublist.size());
    assertThat(jedis.zrange(KEY, 0, -1)).hasSameElementsAs(members);
  }

  @Test
  public void shouldReturnNumMembersRemoved_givenInclusiveMinAndPlusRange() {
    List<String> members = populateSortedSet();

    int minLength = 4;
    String min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);

    List<String> sublist = new ArrayList<>(members.subList(minLength - 1, members.size()));
    members.removeAll(sublist);

    // Range (v * 4) <= member name < +infinity
    assertThat(jedis.zremrangeByLex(KEY, "[" + min, "+")).isEqualTo(sublist.size());
    assertThat(jedis.zrange(KEY, 0, -1)).hasSameElementsAs(members);
  }

  @Test
  public void shouldReturnNumMembersRemoved_givenMinusAndPlusRange() {
    List<String> members = populateSortedSet();

    // Range -infinity <= member name < +infinity
    assertThat(jedis.zremrangeByLex(KEY, "-", "+")).isEqualTo(members.size());
    assertThat(jedis.zcard(KEY)).isEqualTo(0);
  }

  @Test
  public void shouldRemoveMemberInRangeAndKey_givenOneMember() {
    jedis.zadd(KEY, 1.0, "member");

    assertThat(jedis.zremrangeByLex(KEY, "-", "+")).isEqualTo(1);
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void shouldRemoveMembersInRange_givenMultipleMembers() {
    List<String> members = populateSortedSet();

    Set<String> expectedMembers = new HashSet<>();
    expectedMembers.add(members.get(0));
    expectedMembers.add(members.get(4));
    expectedMembers.add(members.get(5));
    expectedMembers.add(members.get(6));
    expectedMembers.add(members.get(7));
    expectedMembers.add(members.get(8));
    expectedMembers.add(members.get(9));

    // range of [vv to (vvvvv
    assertThat(jedis.zremrangeByLex(KEY, "[" + members.get(1), "(" + members.get(4)))
        .isEqualTo(3);
    assertThat(jedis.zcard(KEY)).isEqualTo(7);
    assertThat(jedis.zrange(KEY, 0, -1)).hasSameElementsAs(expectedMembers);
  }

  /************** Helper Methods ************************/
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
