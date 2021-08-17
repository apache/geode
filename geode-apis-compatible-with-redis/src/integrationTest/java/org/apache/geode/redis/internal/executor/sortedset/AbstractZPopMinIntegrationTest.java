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

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Tuple;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;

public abstract class AbstractZPopMinIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;

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
    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.ZPOPMIN, "key", "1", "2"))
            .hasMessageContaining(RedisConstants.ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenWrongNumberFormat() {
    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.ZPOPMIN, "key", "wat"))
            .hasMessageContaining(RedisConstants.ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldReturnEmpty_givenNonExistentSortedSet() {
    assertThat(jedis.zpopmin("unknown", 1)).isEmpty();
  }

  @Test
  public void shouldReturnEmpty_givenNegativeCount() {
    jedis.zadd("key", 1, "player1");

    List<?> result = (List<?>) jedis.sendCommand("key", Protocol.Command.ZPOPMIN, "key", "-1");
    assertThat(result).isEmpty();
    assertThat(jedis.zrange("key", 0, 10)).containsExactly("player1");
  }

  @Test
  public void shouldReturn_highestLex_whenScoresAreEqual() {
    jedis.zadd("key", 1, "player1");
    jedis.zadd("key", 1, "player2");
    jedis.zadd("key", 1, "player3");

    assertThat(jedis.zpopmin("key").getElement()).isEqualTo("player1");
    assertThat(jedis.zrange("key", 0, 10))
        .containsExactlyInAnyOrder("player2", "player3");
  }

  @Test
  public void shouldReturn_memberWithLowestScore() {
    jedis.zadd("key", 3, "player1");
    jedis.zadd("key", 2, "player2");
    jedis.zadd("key", 1, "player3");

    assertThat(jedis.zpopmin("key").getElement()).isEqualTo("player3");
    assertThat(jedis.zrange("key", 0, 10))
        .containsExactlyInAnyOrder("player1", "player2");
  }

  @Test
  public void withCountShouldReturn_membersInScoreOrder_whenScoresAreDifferent() {
    List<Tuple> tuples = new ArrayList<>();
    int count = 10;
    // Make sure that the results are not somehow dependent on the order of insertion
    List<Integer> shuffles = makeShuffledList(count);
    for (int i = 0; i < count; i++) {
      jedis.zadd("key", count - shuffles.get(i), "player" + shuffles.get(i));
      tuples.add(new Tuple("player" + (count - i - 1), (double) (i + 1)));
    }

    assertThat(jedis.zpopmin("key", count)).containsExactlyElementsOf(tuples);
  }

  @Test
  public void withCountShouldReturn_membersInReverseLexicalOrder_whenScoresAreTheSame() {
    List<Tuple> tuples = new ArrayList<>();
    int count = 10;
    // Make sure that the results are not somehow dependent on the order of insertion
    List<Integer> shuffles = makeShuffledList(count);
    for (int i = 0; i < count; i++) {
      jedis.zadd("key", 1D, "player" + shuffles.get(i));
      tuples.add(new Tuple("player" + i, 1D));
    }

    assertThat(jedis.zpopmin("key", count)).containsExactlyElementsOf(tuples);
  }

  @Test
  public void shouldReturn_countLowestScores_whenCountIsPassed() {
    for (int i = 0; i < 5; i++) {
      jedis.zadd("key", i, "player" + i);
    }

    assertThat(jedis.zpopmin("key", 3))
        .containsExactly(
            new Tuple("player0", 0D),
            new Tuple("player1", 1D),
            new Tuple("player2", 2D));

    assertThat(jedis.zrange("key", 0, 10))
        .containsExactly("player3", "player4");
  }

  /**
   * Create a shuffled list of a range of integers from 0..count (exclusive).
   */
  private List<Integer> makeShuffledList(int count) {
    List<Integer> result = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      result.add(i);
    }

    Collections.shuffle(result);
    return result;
  }
}
