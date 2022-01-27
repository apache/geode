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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_VALUE_MUST_BE_POSITIVE;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSPopIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String NON_EXISTENT_SET_KEY = "{tag1}nonExistentSet";
  private static final String SET_KEY = "{tag1}setKey";
  private static final String[] SET_MEMBERS =
      {"one", "two", "three", "four", "five", "six", "seven", "eight"};

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
  public void spopTooFewArgs_returnsError() {
    assertAtLeastNArgs(jedis, Protocol.Command.SPOP, 1);
  }

  @Test
  public void spopTooManyArgs_returnsError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(SET_KEY, Protocol.Command.SPOP, SET_KEY, "5", "5"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void spop_withNonNumericCount_returnsError() {
    assertThatThrownBy(() -> jedis.sendCommand(SET_KEY, Protocol.Command.SPOP, SET_KEY, "b"))
        .hasMessageContaining(ERROR_VALUE_MUST_BE_POSITIVE);
  }

  @Test
  public void spop_withNegativeCount_returnsError() {
    assertThatThrownBy(() -> jedis.spop(SET_KEY, -1))
        .hasMessageContaining(ERROR_VALUE_MUST_BE_POSITIVE);
  }

  @Test
  public void spop_withoutCount_withNonExistentSet_returnsNull_setNotCreated() {
    assertThat(jedis.spop(NON_EXISTENT_SET_KEY)).isNull();
    assertThat(jedis.exists(NON_EXISTENT_SET_KEY)).isFalse();
  }

  @Test
  public void spop_withoutCount_withExistentSet_returnsOneMember_removesReturnedMemberFromSet() {
    jedis.sadd(SET_KEY, SET_MEMBERS);

    String result = jedis.spop(SET_KEY);
    assertThat(result).isIn(Arrays.asList(SET_MEMBERS));
    assertThat(jedis.smembers(SET_KEY)).doesNotContain(result).isSubsetOf(SET_MEMBERS)
        .doesNotHaveDuplicates();
  }

  @Test
  public void spop_withCountAsZero_withExistentSet_returnsEmptyList_setNotModified() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    assertThat(jedis.spop(SET_KEY, 0)).isEmpty();
    assertThat(jedis.smembers(SET_KEY)).containsExactlyInAnyOrder(SET_MEMBERS);
  }

  @Test
  public void spop_withCount_withNonExistentSet_returnsEmptyList_setNotCreated() {
    assertThat(jedis.spop(NON_EXISTENT_SET_KEY, 1)).isEmpty();
    assertThat(jedis.exists(NON_EXISTENT_SET_KEY)).isFalse();
  }

  @Test
  public void spop_withSmallCount_withExistentSet_returnsCorrectNumberOfMembers_removesReturnedMembersFromSet() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    int count = 2;

    Set<String> result = jedis.spop(SET_KEY, count);
    assertThat(result.size()).isEqualTo(count);
    assertThat(result).isSubsetOf(SET_MEMBERS).doesNotHaveDuplicates();

    assertThat(jedis.smembers(SET_KEY)).doesNotContainAnyElementsOf(result).isSubsetOf(SET_MEMBERS)
        .doesNotHaveDuplicates();
  }

  @Test
  public void spop_withLargeCount_withExistentSet_returnsCorrectNumberOfMembers_removesReturnedMembersFromSet() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    int count = 6;

    Set<String> result = jedis.spop(SET_KEY, count);
    assertThat(result.size()).isEqualTo(count);
    assertThat(result).isSubsetOf(SET_MEMBERS).doesNotHaveDuplicates();

    assertThat(jedis.smembers(SET_KEY)).doesNotContainAnyElementsOf(result).isSubsetOf(SET_MEMBERS)
        .doesNotHaveDuplicates();
  }

  @Test
  public void spop_withCountAsSetSize_withExistentSet_returnsAllMembers_setKeyIsDeleted() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    assertThat(jedis.spop(SET_KEY, SET_MEMBERS.length)).containsExactlyInAnyOrder(SET_MEMBERS);
    assertThat(jedis.exists(SET_KEY)).isFalse();
  }

  @Test
  public void spop_withCountGreaterThanSetSize_withExistentSet_returnsAllMembers_setKeyIsDeleted() {
    jedis.sadd(SET_KEY, SET_MEMBERS);
    assertThat(jedis.spop(SET_KEY, SET_MEMBERS.length * 2)).containsExactlyInAnyOrder(SET_MEMBERS);
    assertThat(jedis.exists(SET_KEY)).isFalse();
  }

  @Test
  public void spop_withoutCount_withWrongKeyType_returnsWrongTypeError() {
    String key = "ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.spop(key)).hasMessageContaining(ERROR_WRONG_TYPE);
  }


  @Test
  public void spop_withCountAsZero_withWrongKeyType_returnsWrongTypeError() {
    String key = "ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.spop(key, 0)).hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void spop_withCountAsNegative_withWrongKeyType_returnsWrongTypeError() {
    String key = "ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.spop(key, -1))
        .hasMessageContaining(ERROR_VALUE_MUST_BE_POSITIVE);
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    final AtomicReference<String> spopResultReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.sadd(SET_KEY, SET_MEMBERS),
        i -> spopResultReference.set(jedis.spop(SET_KEY)))
            .runWithAction(() -> {
              assertThat(spopResultReference).satisfiesAnyOf(
                  spopResult -> assertThat(spopResult.get()).isNull(),
                  spopResult -> assertThat(spopResult.get()).isIn(Arrays.asList(SET_MEMBERS)));
              assertThat(SET_KEY).satisfiesAnyOf(
                  key -> assertThat(jedis.exists(key)).isFalse(),
                  key -> assertThat(jedis.smembers(key))
                      .doesNotContain(spopResultReference.get()).isSubsetOf(SET_MEMBERS)
                      .doesNotHaveDuplicates());
              jedis.srem(SET_KEY, SET_MEMBERS);
            });
  }
}
