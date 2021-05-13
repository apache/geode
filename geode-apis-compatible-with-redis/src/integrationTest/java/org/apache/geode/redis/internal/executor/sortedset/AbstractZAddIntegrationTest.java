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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ZADD_OPTION_GT_LT_NX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ZADD_OPTION_NX_XX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.ZAddParams;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractZAddIntegrationTest implements RedisIntegrationTest {
  private Jedis jedis;
  private Jedis jedis2;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final String SORTED_SET_KEY = "ss_key";
  private static final int INITIAL_MEMBER_COUNT = 5;

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
    jedis2.close();
  }

  @Test
  public void zaddErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.ZADD, 3);
  }

  @Test
  @Ignore("restore when we understand why there's a difference between jedis and command line")
  public void zaddErrors_givenUnevenPairsOfArguments() {
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.ZADD, "fakeKey", "1", "member", "2"))
            .hasMessageContaining("ERR wrong number of arguments for 'zadd' command");
  }

  @Test
  public void zaddErrors_givenNonNumericScore() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZADD, "fakeKey", "xlerb", "member"))
        .hasMessageContaining(ERROR_NOT_A_VALID_FLOAT);
  }

  @Test
  public void zaddErrors_givenBothNXAndXXOptions() {
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.ZADD, "fakeKey", "NX", "XX", "1.0", "fakeMember"))
            .hasMessageContaining(ERROR_INVALID_ZADD_OPTION_NX_XX);
  }

  @Test
  @Ignore("restore when we understand why there's a difference between jedis and command line")
  public void zaddErrors_givenBothGTAndLTOptions() {
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.ZADD, "fakeKey", "LT", "GT", "1", "fakeMember"))
            .hasMessageContaining(ERROR_INVALID_ZADD_OPTION_GT_LT_NX);
  }

  @Test
  public void zaddStoresScores_givenCorrectArguments() {
    Map<String, Double> map = makeMemberScoreMap("member_", INITIAL_MEMBER_COUNT, 0);

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
    Map<String, Double> map = makeMemberScoreMap("member_", INITIAL_MEMBER_COUNT, 0);
    Set<String> keys = map.keySet();

    long added = jedis.zadd(SORTED_SET_KEY, map);
    assertThat(added).isEqualTo(keys.size());

    for (String member : keys) {
      Double score = map.get(member);
      Assertions.assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }
  }

  @Test
  public void zaddDoesNotUpdateMembers_whenNXSpecified() {
    Map<String, Double> initMap = makeMemberScoreMap("member_", INITIAL_MEMBER_COUNT, 0);

    long added = jedis.zadd(SORTED_SET_KEY, initMap);
    assertThat(added).isEqualTo(INITIAL_MEMBER_COUNT);
    // TODO: use ZCARD to confirm set size once command is implemented

    for (String member : initMap.keySet()) {
      Double score = initMap.get(member);
      assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }

    Map<String, Double> updateMap = makeMemberScoreMap("member_", 2 * INITIAL_MEMBER_COUNT, 10);

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
    Map<String, Double> initMap = makeMemberScoreMap("member_", INITIAL_MEMBER_COUNT, 0);

    long added = jedis.zadd(SORTED_SET_KEY, initMap);
    assertThat(added).isEqualTo(INITIAL_MEMBER_COUNT);
    // TODO: use ZCARD to confirm set size once command is implemented

    for (String member : initMap.keySet()) {
      Double score = initMap.get(member);
      assertThat(jedis.zscore(SORTED_SET_KEY, member)).isEqualTo(score);
    }

    Map<String, Double> updateMap = makeMemberScoreMap("member_", 2 * INITIAL_MEMBER_COUNT, 10);

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

  private Map<String, Double> makeMemberScoreMap(String baseName, int memberCount, int baseScore) {
    Map<String, Double> map = new HashMap<>();

    for (int i = 0; i < memberCount; i++) {
      map.put(baseName + i, Double.valueOf((i + baseScore) + ""));
    }
    return map;
  }

  @Test
  public void zaddStoresScores_givenCorrectArguments() {
    String key = "ss_key";
    Map<String, Double> map = getMemberScoreMap("member_", 10, 0);

    Set<String> keys = map.keySet();
    Long count = 0L;

    for (String member : keys) {
      Double score = map.get(member);
      System.out.println("test adding: member:" + member + " score:" + score);
      Long res = jedis.zadd(key, score, member);
      Assertions.assertThat(res).isEqualTo(1);
      System.out.println("test getting: member:" + member + " score:" + score);
      Assertions.assertThat(jedis.zscore(key, member)).isEqualTo(score);
      count += res;
    }
    Assertions.assertThat(count).isEqualTo(keys.size());
  }

  @Test
  public void zaddStoresScores_givenMultipleMembersAndScores() {
    String otherKeyEntirely = "ss_key";

    Map<String, Double> map = getMemberScoreMap("member_", 10, 0);
    Set<String> keys = map.keySet();

    long added = jedis.zadd(otherKeyEntirely, map);
    System.out.println("**** actually added: " + added);
    assertThat(added).isEqualTo(keys.size());

    for (String member : keys) {
      Double score = map.get(member);
      Assertions.assertThat(jedis.zscore(otherKeyEntirely, member)).isEqualTo(score);
    }
  }

  @Test
  public void zaddDoesNotUpdateMember_whenNXSpecified() {
    String key = "ss_key";

    Long res = jedis.zadd(key, 1.0, "mamba");
    assertThat(res).isEqualTo(1);

    ZAddParams zAddParams = new ZAddParams();
    zAddParams.nx();
    res = jedis.zadd(key, 2.0, "mamba", zAddParams);
    assertThat(res).isEqualTo(0);
    assertThat(jedis.zscore(key, "mamba")).isEqualTo(1.0);
  }

  @Test
  public void zaddDoesNotAddMember_whenXXSpecified() {
    String key = "ss_key";

    ZAddParams zAddParams = new ZAddParams();
    zAddParams.xx();
    Long res = jedis.zadd(key, 1.0, "mamba", zAddParams);
    assertThat(res).isEqualTo(0);
    assertThat(jedis.keys("*").size()).isEqualTo(0);
  }

  @Test
  public void zaddDoesNotAddNewMembers_whenXXSpecified() {
    String key = "ss_key";
    Map<String, Double> init_map = getMemberScoreMap("member_", 5, 0);
    Set<String> keys = init_map.keySet();

    long added = jedis.zadd(key, init_map);
    assertThat(added).isEqualTo(5);

    for (String member : keys) {
      Double score = init_map.get(member);
      Assertions.assertThat(jedis.zscore(key, member)).isEqualTo(score);
    }

    Map<String, Double> updateMap = getMemberScoreMap("member_", 10, 10);
    Set<String> updateKeys = updateMap.keySet();

    ZAddParams zAddParams = new ZAddParams();
    zAddParams.xx();
    added = jedis.zadd(key, updateMap, zAddParams);
    assertThat(added).isEqualTo(0);

    for (String member : updateKeys) {
      Double score;
      if (init_map.get(member) != null) {
        score = updateMap.get(member);
      } else {
        score = null;
      }
      Assertions.assertThat(jedis.zscore(key, member)).isEqualTo(score);
    }
  }

  @Test
  public void zaddUpdatesScoresCorrectly_whenGTOptionSpecified() {

  }

  private Map<String, Double> getMemberScoreMap(String baseName, int memberCount, int baseScore) {
    Map<String, Double> map = new HashMap<>();

    for (int i = 0; i < memberCount; i++) {
      map.put(baseName + i, Double.valueOf((i + baseScore) + ""));
    }
    return map;
  }
}
