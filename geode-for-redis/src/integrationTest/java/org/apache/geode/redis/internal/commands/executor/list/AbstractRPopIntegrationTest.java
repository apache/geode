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

public abstract class AbstractRPopIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final String PREEXISTING_VALUE = "preexistingValue";
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

  // Overridden in RPopIntegrationTest until we implement Redis 6.2+ semantics
  @Test
  public void rpop_givenWrongNumOfArgs_returnsError() {
    assertAtMostNArgs(jedis, Protocol.Command.RPOP, 2);
  }

  @Test
  public void rpop_withNonListKey_Fails() {
    jedis.set("nonKey", PREEXISTING_VALUE);
    assertThatThrownBy(() -> jedis.rpop("nonKey")).hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void rpop_withNonExistentKey_returnsNull() {
    assertThat(jedis.rpop("nonexistentKey")).isNull();
  }

  @Test
  public void rpop_returnsRightmostMember() {
    jedis.lpush(KEY, "e1", "e2");
    String result = jedis.rpop(KEY);
    assertThat(result).isEqualTo("e1");
  }

  @Test
  public void rpop_removesKey_whenLastElementRemoved() {
    jedis.lpush(KEY, "e1");
    jedis.rpop(KEY);
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void rpop_removesKey_whenLastElementRemoved_multipleTimes() {
    jedis.lpush(KEY, "e1");
    assertThat(jedis.rpop(KEY)).isEqualTo("e1");
    assertThat(jedis.rpop(KEY)).isNull();
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void rpop_removesElementsInCorrectOrder_onRepeatedInvocation() {
    jedis.lpush(KEY, "e1");
    jedis.lpush(KEY, "e2");
    jedis.lpush(KEY, "e3");

    assertThat(jedis.rpop(KEY)).isEqualTo("e1");
    assertThat(jedis.rpop(KEY)).isEqualTo("e2");
    assertThat(jedis.rpop(KEY)).isEqualTo("e3");
  }

  @Test
  public void rpop_withConcurrentLPush_returnsCorrectValue() {
    String[] valuesInitial = new String[] {"un", "deux", "troix"};
    String[] valuesToAdd = new String[] {"plum", "peach", "orange"};
    jedis.lpush(KEY, valuesInitial);

    final AtomicReference<String> rpopReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.lpush(KEY, valuesToAdd),
        i -> rpopReference.set(jedis.rpop(KEY)))
            .runWithAction(() -> {
              assertThat(rpopReference).satisfiesAnyOf(
                  rpopResult -> assertThat(rpopReference.get()).isEqualTo("plum"),
                  rpopResult -> assertThat(rpopReference.get()).isEqualTo("un"));
              jedis.del(KEY);
              jedis.lpush(KEY, valuesInitial);
            });
  }

}
