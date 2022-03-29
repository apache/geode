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
package org.apache.geode.redis.internal.commands.executor.list;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractLRemIntegrationTest implements RedisIntegrationTest {
  private static final String NON_EXISTENT_LIST_KEY = "{tag1}nonExistentKey";
  private static final String LIST_KEY = "{tag1}listKey";
  private static final String[] LIST_ELEMENTS =
      {"pause", "cynic", "sugar", "skill", "pause", "pause", "pause", "aroma", "sugar", "pause",
          "elder"};
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
  public void lrem_wrongNumberOfArgs_returnsError() {
    assertExactNumberOfArgs(jedis, Protocol.Command.LREM, 3);
  }

  @Test
  public void lrem_withNonExistentList_returnsZero() {
    assertThat(jedis.lrem(NON_EXISTENT_LIST_KEY, 2, "element")).isEqualTo(0);
    assertThat(jedis.lrem(NON_EXISTENT_LIST_KEY, -2, "element")).isEqualTo(0);
    assertThat(jedis.lrem(NON_EXISTENT_LIST_KEY, 0, "element")).isEqualTo(0);
  }

  @Test
  public void lrem_withElementNotInList_returnsZero() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);
    assertThat(jedis.lrem(LIST_KEY, 3, "magic")).isEqualTo(0);
  }

  @Test
  public void lrem_withCountAsZero_returnsNumberOfAllMatchingElementsRemoved() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);

    final String[] result =
        {"elder", "sugar", "aroma", "skill", "sugar", "cynic"};
    assertThat(jedis.lrem(LIST_KEY, 0, "pause")).isEqualTo(5);
    assertThat(jedis.lrange(LIST_KEY, 0, -1)).containsExactly(result);
  }

  @Test
  public void lrem_withPositiveCount_returnsNumberOfElementsRemoved() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);

    // Amount of elements to remove is SMALLER than the amount in the list
    final String[] result1 =
        {"elder", "sugar", "aroma", "pause", "skill", "sugar", "cynic", "pause"};
    assertThat(jedis.lrem(LIST_KEY, 3, "pause")).isEqualTo(3);
    assertThat(jedis.lrange(LIST_KEY, 0, -1)).containsExactly(result1);

    // Amount of elements to remove is GREATER than the amount in the list
    final String[] result2 = {"elder", "aroma", "pause", "skill", "cynic", "pause"};
    assertThat(jedis.lrem(LIST_KEY, 10, "sugar")).isEqualTo(2);
    assertThat(jedis.lrange(LIST_KEY, 0, -1)).containsExactly(result2);
  }

  @Test
  public void lrem_withNegativeCount_returnsNumberOfElementsRemoved() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);

    // Amount of elements to remove is SMALLER than the amount in the list
    final String[] result1 =
        {"elder", "pause", "sugar", "aroma", "pause", "pause", "skill", "sugar", "cynic"};
    assertThat(jedis.lrem(LIST_KEY, -2, "pause")).isEqualTo(2);
    assertThat(jedis.lrange(LIST_KEY, 0, -1)).containsExactly(result1);

    // Amount of elements to remove is GREATER than the amount in the list
    final String[] result2 = {"elder", "sugar", "aroma", "skill", "sugar", "cynic"};
    assertThat(jedis.lrem(LIST_KEY, -10, "pause")).isEqualTo(3);
    assertThat(jedis.lrange(LIST_KEY, 0, -1)).containsExactly(result2);
  }

  @Test
  public void lrem_withInvalidCount_returnsErrorNotInteger() {
    // Non Existent List
    assertThatThrownBy(() -> jedis.sendCommand(NON_EXISTENT_LIST_KEY, Protocol.Command.LREM,
        NON_EXISTENT_LIST_KEY, "b", "element")).hasMessage(ERROR_NOT_INTEGER);

    // Existent List
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);
    assertThatThrownBy(
        () -> jedis.sendCommand(LIST_KEY, Protocol.Command.LREM, LIST_KEY, "b", "element"))
            .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void lrem_withWrongTypeKey_returnsErrorWrongType() {
    String key = "{tag1}ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.LREM, key, "0", "element"))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void ensureListConsistency_whenRunningConcurrently() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);

    final String[] elementsToAdd = {"pause", "magic", "loved", "pause"};
    String[] resultPushThenRemove =
        {"loved", "magic", "elder", "sugar", "aroma", "skill", "sugar", "cynic"};
    String[] resultRemoveThenPush =
        {"pause", "loved", "magic", "pause", "elder", "sugar", "aroma", "skill", "sugar", "cynic"};
    final AtomicLong lremResultReference = new AtomicLong();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.lpush(LIST_KEY, elementsToAdd),
        i -> lremResultReference.set(jedis.lrem(LIST_KEY, 0, "pause")))
            .runWithAction(() -> {
              // Checks number of elements removed
              assertThat(lremResultReference).satisfiesAnyOf(
                  lremResult -> assertThat(lremResult.get()).isEqualTo(7),
                  lremResult -> assertThat(lremResult.get()).isEqualTo(5));

              // Checks the elements stored at key
              assertThat(jedis.lrange(LIST_KEY, 0, -1)).satisfiesAnyOf(
                  elements -> assertThat(elements).isEqualTo(Arrays.asList(resultPushThenRemove)),
                  elements -> assertThat(elements).isEqualTo(Arrays.asList(resultRemoveThenPush)));
              jedis.del(LIST_KEY);
              jedis.lpush(LIST_KEY, LIST_ELEMENTS);
            });
  }
}
