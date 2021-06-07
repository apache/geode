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

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ZADD_OPTION_NX_XX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.ZAddParams;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractZAddIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private final String member = "member";
  private final String incrOption = "INCR";

  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final String SORTED_SET_KEY = "ss_key";
  private static final int INITIAL_MEMBER_COUNT = 5;

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
  public void zaddErrors_givenWrongKeyType() {
    final String STRING_KEY = "stringKey";
    jedis.set(STRING_KEY, "value");
    assertThatThrownBy(
        () -> jedis.sendCommand(STRING_KEY, Protocol.Command.ZADD, STRING_KEY, "1", member))
            .hasMessage("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void zaddErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.ZADD, 3);
  }

  @Test
  public void zaddErrors_givenUnevenPairsOfArguments() {
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "1", member, "2"))
            .hasMessageContaining("ERR syntax error");
  }

  @Test
  public void zaddErrors_givenNonNumericScore() {
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "xlerb", member))
            .hasMessageContaining(ERROR_NOT_A_VALID_FLOAT);
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "1.0", "member01",
            "purple flurp", "member02", "3.0", "member03"))
                .hasMessageContaining(ERROR_NOT_A_VALID_FLOAT);
  }

  @Test
  public void zaddErrors_givenBothNXAndXXOptions() {
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "NX", "XX", "1.0",
            "fakeMember"))
                .hasMessageContaining(ERROR_INVALID_ZADD_OPTION_NX_XX);
  }

  @Test
  public void zadd_prioritizesErrors_inTheCorrectOrder() {
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "NX", "XX", "xlerb",
            member, "2"))
                .hasMessageContaining(ERROR_SYNTAX);
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "NX", "XX", "xlerb",
            member))
                .hasMessageContaining(ERROR_INVALID_ZADD_OPTION_NX_XX);
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "xlerb", member))
            .hasMessageContaining(ERROR_NOT_A_VALID_FLOAT);
  }

  @Test
  public void zaddStoresScores_givenCorrectArguments() {
    Map<String, Double> map = makeMemberScoreMap(INITIAL_MEMBER_COUNT, 0);

    Set<String> keys = map.keySet();
    Long count = 0L;

    for (String member : keys) {
      Double score = map.get(member);
      Long res = jedis.zadd(SORTED_SET_KEY, score, member);
      assertThat(res).isEqualTo(1);
      assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
      count += res;
    }
    assertThat(count).isEqualTo(keys.size());
  }

  @Test
  public void zaddStoresScores_givenMultipleMembersAndScores() {
    Map<String, Double> map = makeMemberScoreMap(INITIAL_MEMBER_COUNT, 0);
    Set<String> keys = map.keySet();

    long added = jedis.zadd(SORTED_SET_KEY, map);
    assertThat(added).isEqualTo(keys.size());

    for (String member : keys) {
      Double score = map.get(member);
      assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }
  }

  @Test
  public void zaddCountsOnlyNewMembers_givenMultipleCopiesOfTheSameMember() {
    Long addCount = (Long) jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY,
        "1", member, "2", member, "3", member);
    assertThat(addCount).isEqualTo(1);
    assertThat(jedis.zcard(SORTED_SET_KEY)).isEqualTo(1);
    assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(3.0);
  }

  @Test
  public void zaddCountsOnlyNewMembers_givenMultipleCopiesOfTheSameMember_toAnExistingSet() {
    Long addCount = jedis.zadd(SORTED_SET_KEY, 1.0, "otherMember");
    assertThat(addCount).isEqualTo(1);
    jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY,
        "1", member, "2", member, "3", member);
    assertThat(jedis.zcard(SORTED_SET_KEY)).isEqualTo(2);
    assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(3.0);
  }

  @Test
  public void zaddDoesNotCountExistingMembersWithoutChanges_whenCHSpecified() {
    Map<String, Double> initMap = makeMemberScoreMap(INITIAL_MEMBER_COUNT, 0);
    jedis.zadd(SORTED_SET_KEY, initMap);

    ZAddParams zAddParam = new ZAddParams();
    zAddParam.ch();
    Long addCount = jedis.zadd(SORTED_SET_KEY, initMap, zAddParam);
    assertThat(addCount).isEqualTo(0);
    assertThat(jedis.zcard(SORTED_SET_KEY)).isEqualTo(INITIAL_MEMBER_COUNT);
  }

  @Test
  public void zaddCountsExistingMemberChanges_whenCHSpecified() {
    Map<String, Double> initMap = makeMemberScoreMap(INITIAL_MEMBER_COUNT, 0);
    jedis.zadd(SORTED_SET_KEY, initMap);

    ZAddParams zAddParam = new ZAddParams();
    zAddParam.ch();
    Map<String, Double> updateMap = new HashMap<>();
    for (String member : initMap.keySet()) {
      Double score = initMap.get(member);
      score++;
      updateMap.put(member, score);
    }
    Long addCount = jedis.zadd(SORTED_SET_KEY, updateMap, zAddParam);
    assertThat(addCount).isEqualTo(INITIAL_MEMBER_COUNT);
    assertThat(jedis.zcard(SORTED_SET_KEY)).isEqualTo(INITIAL_MEMBER_COUNT);
  }

  @Test
  public void zaddCountsNewMembers_whenCHSpecified() {
    Map<String, Double> initMap = makeMemberScoreMap(INITIAL_MEMBER_COUNT, 0);
    jedis.zadd(SORTED_SET_KEY, initMap);

    ZAddParams zAddParam = new ZAddParams();
    zAddParam.ch();
    Map<String, Double> updateMap = new HashMap<>();
    final int newMemberCount = 5;
    for (int i = INITIAL_MEMBER_COUNT; i < INITIAL_MEMBER_COUNT + newMemberCount; i++) {
      updateMap.put("member_" + i, Double.valueOf((i) + ""));
    }
    Long addCount = jedis.zadd(SORTED_SET_KEY, updateMap, zAddParam);
    assertThat(addCount).isEqualTo(newMemberCount);
    assertThat(jedis.zcard(SORTED_SET_KEY)).isEqualTo(INITIAL_MEMBER_COUNT + newMemberCount);
  }

  @Test
  public void zaddDoesNotUpdateMembers_whenNXSpecified() {
    Map<String, Double> initMap = makeMemberScoreMap(INITIAL_MEMBER_COUNT, 0);
    Long addCount = jedis.zadd(SORTED_SET_KEY, initMap);
    assertThat(addCount).isEqualTo(INITIAL_MEMBER_COUNT);
    assertThat(jedis.zcard(SORTED_SET_KEY)).isEqualTo(INITIAL_MEMBER_COUNT);

    for (String member : initMap.keySet()) {
      Double score = initMap.get(member);
      assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }

    Map<String, Double> updateMap = makeMemberScoreMap(2 * INITIAL_MEMBER_COUNT, 10);

    ZAddParams zAddParams = new ZAddParams();
    zAddParams.nx();
    addCount = jedis.zadd(SORTED_SET_KEY, updateMap, zAddParams);
    assertThat(addCount).isEqualTo(INITIAL_MEMBER_COUNT);
    assertThat(jedis.zcard(SORTED_SET_KEY)).isEqualTo(2 * INITIAL_MEMBER_COUNT);

    for (String member : updateMap.keySet()) {
      Double score;
      if (initMap.get(member) != null) {
        score = initMap.get(member);
      } else {
        score = updateMap.get(member);
      }
      assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }
  }

  @Test
  public void zaddDoesNotAddNewMembers_whenXXSpecified() {
    Map<String, Double> initMap = makeMemberScoreMap(INITIAL_MEMBER_COUNT, 0);

    Long addCount = jedis.zadd(SORTED_SET_KEY, initMap);
    assertThat(addCount).isEqualTo(INITIAL_MEMBER_COUNT);
    assertThat(jedis.zcard(SORTED_SET_KEY)).isEqualTo(INITIAL_MEMBER_COUNT);

    for (String member : initMap.keySet()) {
      Double score = initMap.get(member);
      assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }

    Map<String, Double> updateMap = makeMemberScoreMap(2 * INITIAL_MEMBER_COUNT, 10);
    ZAddParams zAddParams = new ZAddParams();
    zAddParams.xx();
    addCount = jedis.zadd(SORTED_SET_KEY, updateMap, zAddParams);
    assertThat(addCount).isEqualTo(0);
    assertThat(jedis.zcard(SORTED_SET_KEY)).isEqualTo(INITIAL_MEMBER_COUNT);

    for (String member : updateMap.keySet()) {
      Double score;
      if (initMap.get(member) != null) {
        score = updateMap.get(member);
      } else {
        score = null;
      }
      assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }
  }

  @Test
  public void shouldStoreScore_whenScoreIsSetToInfinity() {
    final String key = "key";
    final String member = "member";
    final double score = POSITIVE_INFINITY;

    jedis.zadd(key, score, member);
    assertThat(jedis.zscore(key, member)).isEqualTo(score);
  }

  @Test
  public void shouldStoreScore_whenScoreIsSetToNegativeInfinity() {
    final String key = "key";
    final String member = "member";
    final double score = NEGATIVE_INFINITY;

    jedis.zadd(key, score, member);
    assertThat(jedis.zscore(key, member)).isEqualTo(score);
  }

  @Test
  public void shouldUpdateScore_whenSettingMemberThatAlreadyExists() {
    final String key = "key";
    final String member = "member";
    jedis.zadd(key, 0.0, member);

    assertThat(jedis.zadd(key, 1.0, member)).isEqualTo(0);
    assertThat(jedis.zscore(key, member)).isEqualTo(1.0);
  }


  @Test
  public void zaddIncrOptionSupportsOnlyOneIncrementingElementPair() {
    assertThatThrownBy(() -> jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD,
        SORTED_SET_KEY, incrOption, "1", member, "2", "member_1"))
            .hasMessageContaining(RedisConstants.ERROR_ZADD_OPTION_TOO_MANY_INCR_PAIR);
  }

  @Test
  public void zaddIncrOptionThrowsIfIncorrectScorePair() {
    assertThatThrownBy(() -> jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD,
        SORTED_SET_KEY, incrOption, "1", member, "2")).hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void zaddIncrOptionDoseNotAddANewMemberWithXXOption() {
    Object result = jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY, "XX",
        incrOption, "1", member);
    assertThat(result).isNull();
    assertThat(jedis.zscore(SORTED_SET_KEY, member)).isNull();
  }

  private final double initial = 355.681000005;
  private final double increment = 9554257.921450001;
  private final double expected = initial + increment;

  @Test
  public void zaddIncrOptionIncrementsScoreForExistingMemberWithXXOption() {
    jedis.zadd(SORTED_SET_KEY, initial, member);

    Object result = jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY, "XX",
        incrOption, Coder.doubleToString(increment), member);
    assertThat(Coder.bytesToDouble((byte[]) result)).isEqualTo(expected);
    assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(expected);
  }

  @Test
  public void zaddIncrOptionAddsANewMemberWithNXOption() {
    Object result = jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY, "NX",
        incrOption, Coder.doubleToString(increment), member);
    assertThat(Coder.bytesToDouble((byte[]) result)).isEqualTo(increment);

    assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(increment);
  }

  @Test
  public void zaddIncrOptionDoseNotIncrementScoreForAExistingMemberWithNXOption() {
    jedis.zadd(SORTED_SET_KEY, initial, member);
    Object result = jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY, "NX",
        incrOption, "1", member);
    assertThat(result).isNull();

    assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(initial);
  }

  @Test
  public void zaddIncrOptionCanIncrementScoreForExistingMemberWithChangeOption() {
    jedis.zadd(SORTED_SET_KEY, initial, member);
    Object result = jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY, "CH",
        incrOption, Coder.doubleToString(increment), member);
    assertThat(Coder.bytesToDouble((byte[]) result)).isEqualTo(expected);

    assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(expected);
  }

  @Test
  public void zaddIncrOptionCanIncrementScoreForExistingMember() {
    jedis.zadd(SORTED_SET_KEY, initial, member);
    Object result =
        jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY, incrOption,
            Coder.doubleToString(increment), member);

    assertThat(Coder.bytesToDouble((byte[]) result)).isEqualTo(expected);
    result = jedis.zscore(SORTED_SET_KEY, member);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void zaddIncrOptionCanIncrementScoreConcurrently() {
    int membersCount = 1000;
    double increment1 = 1.1;
    double increment2 = 3.5;
    double total = increment1 + increment2;
    new ConcurrentLoopingThreads(membersCount,
        (i) -> doZAddIncr(i, increment1, total),
        (i) -> doZAddIncr(i, increment2, total)).run();

    assertThat(jedis.zcard(SORTED_SET_KEY)).isEqualTo(membersCount);
    for (int i = 0; i < membersCount; i++) {
      assertThat(jedis.zscore(SORTED_SET_KEY, member + i)).isEqualTo(total);
    }
  }

  private void doZAddIncr(int i, double increment, double total) {
    Object result =
        jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY, incrOption,
            Coder.doubleToString(increment), member + i);
    assertThat(Coder.bytesToDouble((byte[]) result)).isIn(increment, total);
  }

  private Map<String, Double> makeMemberScoreMap(int memberCount, double baseScore) {
    Map<String, Double> map = new HashMap<>();

    for (int i = 0; i < memberCount; i++) {
      map.put("member_" + i, i + baseScore);
    }
    return map;
  }

}
