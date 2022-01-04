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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtMostNArgs;
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

public abstract class AbstractLPopIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final String PREEXISTING_VALUE = "preexistingValue";
  // TODO: make private when we implement Redis 6.2+ behavior for LPOP
  protected JedisCluster jedis;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  // Overridden in LPopIntegrationTest until we implement Redis 6.2+ semantics
  @Test
  public void lpop_givenWrongNumOfArgs_returnsError() {
    assertAtMostNArgs(jedis, Protocol.Command.LPOP, 2);
  }

  @Test
  public void lpop_withNonListKey_Fails() {
    jedis.set("string", PREEXISTING_VALUE);
    assertThatThrownBy(() -> jedis.lpop("string")).hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void lpop_withNonExistentKey_returnsNull() {
    assertThat(jedis.lpop("nonexistent")).isNull();
  }

  @Test
  public void lpop_returnsLeftmostMember() {
    jedis.lpush(KEY, "e1", "e2");
    String result = jedis.lpop(KEY);
    assertThat(result).isEqualTo("e2");
  }

  @Test
  public void lpop_removesKey_whenLastElementRemoved() {
    final String keyWithTagForKeysCommand = "{tag}" + KEY;

    jedis.lpush(keyWithTagForKeysCommand, "e1");
    jedis.lpop(keyWithTagForKeysCommand);
    assertThat(jedis.keys(keyWithTagForKeysCommand)).isEmpty();
  }

  @Test
  public void lpop_removesKey_whenLastElementRemoved_multipleTimes() {
    final String key = KEY;

    jedis.lpush(key, "e1");
    assertThat(jedis.lpop(key)).isEqualTo("e1");
    assertThat(jedis.lpop(key)).isNull();
    assertThat(jedis.lpop(key)).isNull();
    assertThat(jedis.exists(key)).isFalse();
  }

  @Test
  public void lpop_withConcurrentLPush_returnsCorrectValue() {
    String[] valuesInitial = new String[] {"one", "two", "three"};
    String[] valuesToAdd = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.lpush(KEY, valuesInitial);

    final AtomicReference<String> lpopReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.lpush(KEY, valuesToAdd),
        i -> lpopReference.set(jedis.lpop(KEY)))
            .runWithAction(() -> {
              assertThat(lpopReference).satisfiesAnyOf(
                  lpopResult -> assertThat(lpopReference.get()).isEqualTo("peach"),
                  lpopResult -> assertThat(lpopReference.get()).isEqualTo("three"),
                  lpopResult -> assertThat(lpopResult.get()).isNull());
              jedis.del(KEY);
              jedis.lpush(KEY, valuesInitial);
            });
  }
}
