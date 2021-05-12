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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.ZAddParams;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractZAddIntegrationTest implements RedisPortSupplier {
  private Jedis jedis;
  private Jedis jedis2;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

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
  public void zaddErrors_givenBothNXAndXXOptions() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZADD, "fakeKey", "NX", "XX"))
        .hasMessageContaining(ERROR_INVALID_ZADD_OPTION_NX_XX);
  }

  @Test
  public void zaddErrors_givenBothGTAndLTOptions() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.ZADD, "fakeKey", "GT", "LT"))
        .hasMessageContaining(ERROR_INVALID_ZADD_OPTION_GT_LT_NX);
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
