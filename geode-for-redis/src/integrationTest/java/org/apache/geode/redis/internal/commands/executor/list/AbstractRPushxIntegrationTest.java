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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
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

public abstract class AbstractRPushxIntegrationTest implements RedisIntegrationTest {
  private static final String NON_EXISTENT_LIST_KEY = "{tag1}nonExistentKey";
  private static final String LIST_KEY = "{tag1}listKey";
  private static final String[] LIST_ELEMENTS = {"turtle", "eel", "turtle", "narwhal"};
  private static final String INITIAL_ELEMENT = "dolphin";
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
  public void rpushx_givenTooFewArguments_returnsError() {
    assertAtLeastNArgs(jedis, Protocol.Command.RPUSHX, 2);
  }

  @Test
  public void rpushx_withNonExistentList_doesNotCreateList_returnsZero() {
    assertThat(jedis.rpushx(NON_EXISTENT_LIST_KEY, LIST_ELEMENTS)).isEqualTo(0);
    assertThat(jedis.exists(NON_EXISTENT_LIST_KEY)).isFalse();
  }

  @Test
  public void rpushx_returnsUpdatedListLength() {
    jedis.rpush(LIST_KEY, INITIAL_ELEMENT);
    for (int i = 0; i < LIST_ELEMENTS.length; i++) {
      assertThat(jedis.rpushx(LIST_KEY, LIST_ELEMENTS[i])).isEqualTo(i + 2);
    }
  }

  @Test
  public void rpushx_addsElementsInCorrectOrder_onRepeatedInvocation() {
    jedis.rpush(LIST_KEY, INITIAL_ELEMENT);
    for (int i = 0; i < LIST_ELEMENTS.length; i++) {
      jedis.rpushx(LIST_KEY, LIST_ELEMENTS[i]);
    }

    String[] result = {"dolphin", "turtle", "eel", "turtle", "narwhal"};
    assertThat(jedis.lrange(LIST_KEY, 0, -1)).containsExactly(result);
  }

  @Test
  public void rpushx_withWrongTypeKey_returnsErrorWrongType_shouldNotOverWriteExistingKey() {
    String value = "notListValue";
    jedis.set(LIST_KEY, value);
    assertThatThrownBy(() -> jedis.rpushx(LIST_KEY, LIST_ELEMENTS)).hasMessage(ERROR_WRONG_TYPE);
    assertThat(jedis.get(LIST_KEY)).isEqualTo(value);
  }

  @Test
  public void ensureListConsistency_whenRunningConcurrently() {
    final String[] initialList = {"river", "turtle", "ocean", "turtle"};
    jedis.rpush(LIST_KEY, initialList);

    String[] resultLremThenRpushx =
        {"river", "ocean", "turtle", "eel", "turtle", "narwhal"};
    String[] resultRpushxThenLrem =
        {"river", "ocean", "eel", "narwhal"};

    final AtomicLong rpushxResultReference = new AtomicLong();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.lrem(LIST_KEY, 0, "turtle"),
        i -> rpushxResultReference.set(jedis.rpushx(LIST_KEY, LIST_ELEMENTS)))
            .runWithAction(() -> {
              // Check size of resulting array
              assertThat(rpushxResultReference).satisfiesAnyOf(
                  rpushxResult -> assertThat(rpushxResult.get()).isEqualTo(6),
                  rpushxResult -> assertThat(rpushxResult.get()).isEqualTo(8));

              // Checks the elements stored at key
              assertThat(jedis.lrange(LIST_KEY, 0, -1)).satisfiesAnyOf(
                  elements -> assertThat(elements).isEqualTo(Arrays.asList(resultLremThenRpushx)),
                  elements -> assertThat(elements).isEqualTo(Arrays.asList(resultRpushxThenLrem)));
              jedis.del(LIST_KEY);
              jedis.rpush(LIST_KEY, initialList);
            });
  }
}
