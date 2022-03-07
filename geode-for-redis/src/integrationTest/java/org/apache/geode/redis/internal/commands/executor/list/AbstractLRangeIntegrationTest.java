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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractLRangeIntegrationTest implements RedisIntegrationTest {
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
  public void lrange_wrongNumberOfArgs_returnsError() {
    assertExactNumberOfArgs(jedis, Protocol.Command.LRANGE, 3);
  }

  @Test
  public void lrange_withNonExistentList_withStartIndexLessThanStopIndex_returnsEmptyList() {
    assertThat(jedis.lrange(NON_EXISTENT_LIST_KEY, -10, 10)).isEmpty();
  }

  @Test
  public void lrange_withNonExistentList_withStartIndexGreaterThanStopIndex_returnsEmptyList() {
    assertThat(jedis.lrange(NON_EXISTENT_LIST_KEY, 10, -10)).isEmpty();
  }

  @Test
  public void lrange_withInvalidRange_returnsEmptyList() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);

    assertThat(getLRangeResult(4, 0)).isEmpty();
    assertThat(getLRangeResult(2, -10)).isEmpty();
    assertThat(getLRangeResult(-3, 0)).isEmpty();
    assertThat(getLRangeResult(-4, -6)).isEmpty();
    assertThat(getLRangeResult(-5, -10)).isEmpty();
    assertThat(getLRangeResult(10, 5)).isEmpty();
    assertThat(getLRangeResult(10, -6)).isEmpty();
    assertThat(getLRangeResult(8, 10)).isEmpty();
    assertThat(getLRangeResult(10, -10)).isEmpty();
    assertThat(getLRangeResult(-12, -10)).isEmpty();
  }

  @Test
  public void lrange_withValidRange_returnsElementsInRange() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);

    String[] result1 = {"goat", "flamingo", "elephant", "deer"};
    assertThat(getLRangeResult(0, -4)).containsExactly(result1);
    assertThat(getLRangeResult(-10, 3)).containsExactly(result1);

    String[] result2 = {"chameleon", "bats", "aardvark"};
    assertThat(getLRangeResult(4, -1)).containsExactly(result2);
    assertThat(getLRangeResult(4, 10)).containsExactly(result2);
    assertThat(getLRangeResult(-3, 10)).containsExactly(result2);

    String[] result3 = {"goat", "flamingo", "elephant", "deer", "chameleon", "bats"};
    assertThat(getLRangeResult(-10, -2)).containsExactly(result3);

    String[] result4 = {"goat", "flamingo", "elephant", "deer", "chameleon", "bats", "aardvark"};
    assertThat(getLRangeResult(0, 6)).containsExactly(result4);
    assertThat(getLRangeResult(-10, 10)).containsExactly(result4);
  }

  @Test
  public void lrange_withSameValueForStartIndexAndStopIndex_returnsElement() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);
    assertThat(getLRangeResult(0, 0)).containsExactly(new String[] {"goat"});
  }

  @Test
  public void lrange_withInvalidInputs_withNonExisistentList_returnsErrorNotInteger() {
    assertThatThrownBy(
        () -> jedis.sendCommand(LIST_KEY, Protocol.Command.LRANGE, NON_EXISTENT_LIST_KEY, "b", "2"))
            .hasMessage(ERROR_NOT_INTEGER);

    assertThatThrownBy(
        () -> jedis.sendCommand(LIST_KEY, Protocol.Command.LRANGE, NON_EXISTENT_LIST_KEY, "2", "b"))
            .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void lrange_withInvalidInputs_returnsErrorNotInteger() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);
    assertThatThrownBy(
        () -> jedis.sendCommand(LIST_KEY, Protocol.Command.LRANGE, LIST_KEY, "b", "2"))
            .hasMessage(ERROR_NOT_INTEGER);

    assertThatThrownBy(
        () -> jedis.sendCommand(LIST_KEY, Protocol.Command.LRANGE, LIST_KEY, "2", "b"))
            .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void lrange_withWrongTypeKey_returnsErrorWrongType() {
    String key = "{tag1}ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.LRANGE, key, "0", "2"))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void lrange_withWrongTypeKey_withInvalidInputs_returnsErrorNotInteger() {
    String key = "{tag1}ding";
    jedis.set(key, "dong");
    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.LRANGE, key, "b", "2"))
        .hasMessage(ERROR_NOT_INTEGER);

    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.LRANGE, key, "2", "b"))
        .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void ensureListConsistency_whenRunningConcurrently() {
    jedis.lpush(LIST_KEY, LIST_ELEMENTS);

    final String[] result =
        {"goat", "flamingo", "elephant", "deer", "chameleon", "bats", "aardvark"};

    final String[] elementsToAdd =
        {"vainglorious", "williwaw", "xiphoid", "ypsiliform", "zinziberaceous"};
    final String[] resultWithElementsAdded =
        {"zinziberaceous", "ypsiliform", "xiphoid", "williwaw", "vainglorious", "goat", "flamingo",
            "elephant", "deer", "chameleon", "bats", "aardvark"};
    final AtomicReference<List<String>> lrangeResultReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.lpush(LIST_KEY, elementsToAdd),
        i -> lrangeResultReference.set(jedis.lrange(LIST_KEY, -15, 15)))
            .runWithAction(() -> {
              assertThat(lrangeResultReference).satisfiesAnyOf(
                  lrangeResult -> assertThat(lrangeResult.get())
                      .containsExactly(resultWithElementsAdded),
                  lrangeResult -> assertThat(lrangeResult.get())
                      .containsExactly(result));
              jedis.del(LIST_KEY);
              jedis.lpush(LIST_KEY, LIST_ELEMENTS);
            });
  }

  private List<String> getLRangeResult(int startIndex, int stopIndex) {
    return jedis.lrange(LIST_KEY, startIndex, stopIndex);
  }
}
