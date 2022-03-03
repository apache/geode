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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INDEX_OUT_OF_RANGE;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NO_SUCH_KEY;
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

public abstract class AbstractLSetIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final String initialValue = "initialValue";
  public static final String newValue = "newValue";
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
  public void lset_errors_givenTooFewArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.LSET, 3);
  }

  @Test
  public void lset_onKeyThatDoesNotExist_returnsError_doesNotCreateKey() {
    assertThatThrownBy(() -> jedis.lset(KEY, 1, newValue)).hasMessage(ERROR_NO_SUCH_KEY);
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void lset_errors_withIndexOutOfRange() {
    jedis.lpush(KEY, initialValue);
    assertThatThrownBy(() -> jedis.lset(KEY, 10, newValue))
        .hasMessage(ERROR_INDEX_OUT_OF_RANGE);

    assertThatThrownBy(() -> jedis.lset(KEY, -10, newValue))
        .hasMessage(ERROR_INDEX_OUT_OF_RANGE);
  }

  @Test
  public void lset_returnsNotAnIntegerError_givenKeyExistsAndIndexIsNotAValidInteger() {
    jedis.lpush(KEY, initialValue);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.LSET, KEY, "notAnInteger", "newElement"))
            .hasMessage("ERR " + ERROR_NOT_INTEGER);
  }

  @Test
  public void lset_returnsNoSuchKeyError_givenKeyDoesNotExistAndIndexIsNotAValidInteger() {
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.LSET, KEY, "notAnInteger", "newElement"))
            .hasMessage("ERR " + ERROR_NO_SUCH_KEY);
  }

  @Test
  public void lset_setsValue_givenValidPositiveIndex_withOneItemInList() {
    jedis.lpush(KEY, initialValue);
    assertThat(jedis.lset(KEY, 0, newValue)).isEqualTo("OK");
    assertThat(jedis.lpop(KEY)).isEqualTo(newValue);
  }

  @Test
  public void lset_setsValue_givenValidPositiveIndex_withMultipleItemsInList() {
    for (int i = 4; i >= 0; i--) {
      jedis.lpush(KEY, initialValue + i);
    }

    assertThat(jedis.lset(KEY, 2, newValue)).isEqualTo("OK");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "0");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "1");
    assertThat(jedis.lpop(KEY)).isEqualTo(newValue);
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "3");
  }

  @Test
  public void lset_setsValue_givenValidNegativeIndex_withOneItemInList() {
    jedis.lpush(KEY, initialValue);
    assertThat(jedis.lset(KEY, -1, newValue)).isEqualTo("OK");
    assertThat(jedis.lpop(KEY)).isEqualTo(newValue);
  }

  @Test
  public void lset_setsValue_givenValidNegativeIndex_withMultipleItemsInList() {
    for (int i = 4; i >= 0; i--) {
      jedis.lpush(KEY, initialValue + i);
    }

    assertThat(jedis.lset(KEY, -2, newValue)).isEqualTo("OK");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "0");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "1");
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "2");
    assertThat(jedis.lpop(KEY)).isEqualTo(newValue);
    assertThat(jedis.lpop(KEY)).isEqualTo(initialValue + "4");
  }

  @Test
  public void testConcurrentLSetAndLPush() {
    String[] initialElements = {"snake", "lizard", "turtle", "tuatara", "crocodilian", "bird"};
    jedis.lpush(KEY, initialElements);

    String[] elementsToAdd = {"python", "monitor", "sulcata", "tuatara", "caiman", "raven"};
    new ConcurrentLoopingThreads(1000,
        i -> jedis.lpush(KEY, elementsToAdd),
        i -> jedis.lset(KEY, 0, newValue)).runWithAction(() -> {
          assertThat(jedis.llen(KEY)).isEqualTo(initialElements.length + elementsToAdd.length);
          assertThat(jedis).satisfiesAnyOf(
              // LPUSH happened first
              jedisClient -> {
                assertThat(jedisClient.lindex(KEY, 0)).isEqualTo(newValue);
                assertThat(jedisClient.lindex(KEY, elementsToAdd.length))
                    .isEqualTo(initialElements[initialElements.length - 1]);
              },
              // LSET happened first
              jedisClient -> {
                assertThat(jedisClient.lindex(KEY, 0))
                    .isEqualTo(elementsToAdd[elementsToAdd.length - 1]);
                assertThat(jedisClient.lindex(KEY, elementsToAdd.length)).isEqualTo(newValue);
              });
          jedis.del(KEY);
          jedis.lpush(KEY, initialElements);
        });
  }

  @Test
  public void lset_withConcurrentLpop_behavesCorrectly() {
    jedis.lpush(KEY, initialValue);
    AtomicReference<String> lpopResult = new AtomicReference<>();
    AtomicReference<Throwable> lsetException = new AtomicReference<>();

    new ConcurrentLoopingThreads(1000,
        i -> lpopResult.set(jedis.lpop(KEY)),
        i -> {
          try {
            jedis.lset(KEY, 0, newValue);
          } catch (Exception e) {
            lsetException.set(e);
          }
        })
            .runWithAction(() -> {
              assertThat(jedis.exists(KEY)).isFalse();
              assertThat(lpopResult.get()).satisfiesAnyOf(
                  // LPOP was applied first
                  poppedValue -> {
                    assertThat(poppedValue).isEqualTo(initialValue);
                    assertThat(lsetException.get()).hasMessage("ERR " + ERROR_NO_SUCH_KEY);
                  },
                  // LSET was applied first
                  poppedValue -> {
                    assertThat(poppedValue).isEqualTo(newValue);
                    assertThat(lsetException.get()).isNull();
                  });

              // Reset the list to its original contents and clear the atomic references
              jedis.lpush(KEY, initialValue);
              lpopResult.set(null);
              lsetException.set(null);
            });
  }
}
