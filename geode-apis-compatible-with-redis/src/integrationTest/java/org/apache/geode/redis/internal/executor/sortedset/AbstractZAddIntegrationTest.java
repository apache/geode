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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ZADD_OPTION_NX_XX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.ZAddParams;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractZAddIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
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
        () -> jedis.sendCommand(STRING_KEY, Protocol.Command.ZADD, STRING_KEY, "1", "member"))
            .hasMessage("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void zaddErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.ZADD, 3);
  }

  @Test
  public void zaddErrors_givenUnevenPairsOfArguments() {
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "1", "member", "2"))
            .hasMessageContaining("ERR syntax error");
  }

  @Test
  public void zaddErrors_givenNonNumericScore() {
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "xlerb", "member"))
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
            "member", "2"))
                .hasMessageContaining(ERROR_SYNTAX);
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "NX", "XX", "xlerb",
            "member"))
                .hasMessageContaining(ERROR_INVALID_ZADD_OPTION_NX_XX);
    assertThatThrownBy(
        () -> jedis.sendCommand("fakeKey", Protocol.Command.ZADD, "fakeKey", "xlerb", "member"))
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
      Assertions.assertThat(res).isEqualTo(1);
      Assertions.assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
      count += res;
    }
    Assertions.assertThat(count).isEqualTo(keys.size());
  }

  @Test
  public void zaddStoresScores_givenMultipleMembersAndScores() {
    Map<String, Double> map = makeMemberScoreMap(INITIAL_MEMBER_COUNT, 0);
    Set<String> keys = map.keySet();

    long added = jedis.zadd(SORTED_SET_KEY, map);
    assertThat(added).isEqualTo(keys.size());

    for (String member : keys) {
      Double score = map.get(member);
      Assertions.assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }
  }

  @Test
  public void zaddCountsOnlyNewMembers_givenMultipleCopiesOfTheSameMember() {
    Long addCount = (Long) jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY,
        "1", "member", "2", "member", "3", "member");
    assertThat(addCount).isEqualTo(1);
    // TODO: use ZCARD to confirm set size once command is implemented
    assertThat(jedis.zscore(SORTED_SET_KEY, "member")).isEqualTo(3.0);
  }

  @Test
  public void zaddCountsOnlyNewMembers_givenMultipleCopiesOfTheSameMember_toAnExistingSet() {
    jedis.zadd(SORTED_SET_KEY, 1.0, "otherMember");
    Long addCount = (Long) jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZADD, SORTED_SET_KEY,
        "1", "member", "2", "member", "3", "member");
    assertThat(addCount).isEqualTo(1);
    // TODO: use ZCARD to confirm set size once command is implemented
    assertThat(jedis.zscore(SORTED_SET_KEY, "member")).isEqualTo(3.0);
  }

  @Test
  public void zaddDoesNotUpdateMembers_whenNXSpecified() {
    Map<String, Double> initMap = makeMemberScoreMap(INITIAL_MEMBER_COUNT, 0);

    long added = jedis.zadd(SORTED_SET_KEY, initMap);
    assertThat(added).isEqualTo(INITIAL_MEMBER_COUNT);
    // TODO: use ZCARD to confirm set size once command is implemented

    for (String member : initMap.keySet()) {
      Double score = initMap.get(member);
      assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }

    Map<String, Double> updateMap = makeMemberScoreMap(2 * INITIAL_MEMBER_COUNT, 10);

    ZAddParams zAddParams = new ZAddParams();
    zAddParams.nx();
    added = jedis.zadd(SORTED_SET_KEY, updateMap, zAddParams);
    assertThat(added).isEqualTo(INITIAL_MEMBER_COUNT);
    // TODO: use ZCARD to confirm set size once command is implemented

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

    long added = jedis.zadd(SORTED_SET_KEY, initMap);
    assertThat(added).isEqualTo(INITIAL_MEMBER_COUNT);
    // TODO: use ZCARD to confirm set size once command is implemented

    for (String member : initMap.keySet()) {
      Double score = initMap.get(member);
      assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }

    Map<String, Double> updateMap = makeMemberScoreMap(2 * INITIAL_MEMBER_COUNT, 10);

    ZAddParams zAddParams = new ZAddParams();
    zAddParams.xx();
    added = jedis.zadd(SORTED_SET_KEY, updateMap, zAddParams);
    assertThat(added).isEqualTo(0);
    // TODO: use ZCARD to confirm set size once command is implemented

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
  public void zaddDoesNotUpdateMember_whenNXSpecified() {
    Long res = jedis.zadd(SORTED_SET_KEY, 1.0, "mamba");
    assertThat(res).isEqualTo(1);

    ZAddParams zAddParams = new ZAddParams();
    zAddParams.nx();
    res = jedis.zadd(SORTED_SET_KEY, 2.0, "mamba", zAddParams);
    assertThat(res).isEqualTo(0);
    assertThat(jedis.zscore(SORTED_SET_KEY, "mamba")).isEqualTo(1.0);
  }

  @Test
  public void zaddDoesNotAddMember_whenXXSpecified() {
    ZAddParams zAddParams = new ZAddParams();
    zAddParams.xx();
    Long res = jedis.zadd(SORTED_SET_KEY, 1.0, "mamba", zAddParams);
    assertThat(res).isEqualTo(0);
    assertThat(jedis.zscore(SORTED_SET_KEY, "mamba")).isEqualTo(null);
    assertThat(jedis.exists(SORTED_SET_KEY)).isFalse();
  }

  private Map<String, Double> makeMemberScoreMap(int memberCount, int baseScore) {
    Map<String, Double> map = new HashMap<>();

    for (int i = 0; i < memberCount; i++) {
      map.put("member_" + i, Double.valueOf((i + baseScore) + ""));
    }
    return map;
  }
}
