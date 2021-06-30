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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Tuple;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;

public abstract class AbstractZRangeIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String SORTED_SET_KEY = "ss_key";
  private List<String> members;
  private List<Double> scores;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);
    members = new ArrayList<>(Arrays.asList("mem1", "mem2", "mem3", "mem4", "mem5"));
    scores = new ArrayList<>(Arrays.asList(.5d, 1.0d, 1.7d, 2.002d, 3.14159d));
    createRedisSortedSet(SORTED_SET_KEY, members, scores);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void shouldError_givenWrongKeyType() {
    final String STRING_KEY = "stringKey";
    jedis.set(STRING_KEY, "value");
    assertThatThrownBy(
        () -> jedis.sendCommand(STRING_KEY, Protocol.Command.ZRANGE, STRING_KEY, "1", "2"))
            .hasMessage("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void shouldError_givenNonIntegerRangeValues() {
    jedis.zadd(SORTED_SET_KEY, 1.0, "member");
    assertThatThrownBy(
        () -> jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZRANGE, SORTED_SET_KEY,
            "NOT_AN_INT", "2"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
    assertThatThrownBy(
        () -> jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZRANGE, SORTED_SET_KEY, "1",
            "ALSO_NOT_AN_INT"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldReturnSyntaxError_givenWrongWithScoresFlag() {
    jedis.zadd(SORTED_SET_KEY, 1.0, "member");
    assertThatThrownBy(
        () -> jedis.sendCommand(SORTED_SET_KEY, Protocol.Command.ZRANGE, SORTED_SET_KEY, "1", "2",
            "WITSCOREZ"))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void shouldReturnEmptyList_GivenInvalidRanges() {
    assertThat(jedis.zrange(SORTED_SET_KEY, 5, 0)).isEmpty();
    assertThat(jedis.zrange(SORTED_SET_KEY, 27, 30)).isEmpty();
    assertThat(jedis.zrange(SORTED_SET_KEY, 8, -2)).isEmpty();
    assertThat(jedis.zrange(SORTED_SET_KEY, -18, -10)).isEmpty();
  }

  @Test
  public void shouldReturnSimpleRanges() {
    assertThat(jedis.zrange(SORTED_SET_KEY, 0, 5)).containsAll(members);
    assertThat(jedis.zrange(SORTED_SET_KEY, 0, 10)).containsAll(members);
    assertThat(jedis.zrange(SORTED_SET_KEY, 3, 4)).containsAll(members.subList(3, 4));
    assertThat(jedis.zrange(SORTED_SET_KEY, 0, 2)).containsAll(members.subList(0, 2));
  }

  @Test
  public void shouldReturnRanges_SpecifiedWithNegativeOffsets() {
    assertThat(jedis.zrange(SORTED_SET_KEY, -5, -1)).containsAll(members);
    assertThat(jedis.zrange(SORTED_SET_KEY, -2, -1)).containsAll(members.subList(3, 4));
    assertThat(jedis.zrange(SORTED_SET_KEY, -8, -3)).containsAll(members.subList(0, 2));
  }

  @Test
  public void shouldAlsoReturnScores_whenWithScoresSpecified() {
    Set<Tuple> expected = new LinkedHashSet<>();
    for (int i = 0; i < members.size(); i++) {
      expected.add(new Tuple(members.get(i), scores.get(i)));
    }
    assertThat(jedis.zrangeWithScores(SORTED_SET_KEY, 0, 5)).isEqualTo(expected);
  }

  private void createRedisSortedSet(String setName, List<String> members, List<Double> scores) {
    Map<String, Double> scoreMembers = new HashMap<>();
    for (int i = 0; i < members.size(); i++) {
      scoreMembers.put(members.get(i), scores.get(i));
    }
    jedis.zadd(setName, scoreMembers);
  }
}
