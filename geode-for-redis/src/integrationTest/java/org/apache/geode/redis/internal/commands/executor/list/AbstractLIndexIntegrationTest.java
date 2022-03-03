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

import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractLIndexIntegrationTest implements RedisIntegrationTest {
  private static final String NON_EXISTENT_LIST_KEY = "{tag1}nonExistentKey";
  private static final String LIST_KEY = "{tag1}listKey";
  private static final String[] LIST_ELEMENTS =
      {"aardvark", "bats", "chameleon", "deer", "elephant", "flamingo", "goat"};
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
  public void lindex_wrongNumberOfArgs_returnsError() {
    assertExactNumberOfArgs(jedis, Protocol.Command.LINDEX, 2);
  }

  @Test
  public void lindex_withPositiveIndex_withNonExistentList_returnsNull() {
    assertThat(jedis.lindex(NON_EXISTENT_LIST_KEY, 2)).isNull();
  }

  @Test
  public void lindex_withNegativeIndex_withNonExistentList_returnsNull() {
    assertThat(jedis.lindex(NON_EXISTENT_LIST_KEY, -2)).isNull();
  }

  @Test
  public void lindex_withPositiveIndexes_returnsElement() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);
    for (int i = 0; i < LIST_ELEMENTS.length; i++) {
      // LIST_ELEMENTS[LIST_ELEMENTS.length - 1 - i] iterates LIST_ELEMENTS backwards
      assertThat(jedis.lindex(LIST_KEY, i)).isEqualTo(LIST_ELEMENTS[LIST_ELEMENTS.length - 1 - i]);
    }
  }

  @Test
  public void lindex_withNegativeIndexes_returnsElement() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);
    for (int i = 0; i < LIST_ELEMENTS.length; i++) {
      assertThat(jedis.lindex(LIST_KEY, -(i + 1))).isEqualTo(LIST_ELEMENTS[i]);

    }
  }

  @Test
  public void lindex_withPositiveOutOfRangeIndex_returnsNull() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);
    assertThat(jedis.lindex(LIST_KEY, 10)).isNull();
  }

  @Test
  public void lindex_withNegativeOutOfRangeIndex_returnsNull() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);
    assertThat(jedis.lindex(LIST_KEY, -10)).isNull();
  }

  @Test
  public void lindex_withInvalidIndex_withNonExistentList_returnsNull() {
    assertThat(jedis.sendCommand(NON_EXISTENT_LIST_KEY, Protocol.Command.LINDEX,
        NON_EXISTENT_LIST_KEY, "b")).isNull();
  }

  @Test
  public void lindex_withInvalidIndex_returnsError() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);
    assertThatThrownBy(() -> jedis.sendCommand(LIST_KEY, Protocol.Command.LINDEX, LIST_KEY, "b"))
        .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void lindex_withWrongKeyType_returnsWrongTypeError() {
    String key = "{tag1}ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.lindex(key, 2)).hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void lindex_withWrongKeyType_withInvalidIndex_returnsWrongTypeError() {
    String key = "{tag1}ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.sendCommand(LIST_KEY, Protocol.Command.LINDEX, key, "b"))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void ensureListConsistency_whenRunningConcurrently() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);

    String[] elementsToAdd = {"vulture", "walrus", "xouba fish", "yak", "zebra"};
    final AtomicReference<String> lindexResultReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.lpush(LIST_KEY, elementsToAdd),
        i -> lindexResultReference.set(jedis.lindex(LIST_KEY, 0)))
            .runWithAction(() -> {
              assertThat(lindexResultReference).satisfiesAnyOf(
                  lindexResult -> assertThat(lindexResult.get()).isEqualTo("zebra"),
                  lindexResult -> assertThat(lindexResult.get()).isEqualTo("goat"));
              jedis.del(LIST_KEY);
              jedis.lpush(LIST_KEY, LIST_ELEMENTS);
            });
  }
}
