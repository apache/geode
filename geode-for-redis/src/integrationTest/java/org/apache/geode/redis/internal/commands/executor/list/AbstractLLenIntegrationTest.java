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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractLLenIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final String PREEXISTING_VALUE = "preexistingValue";
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
  public void llen_withStringFails() {
    jedis.set("string", PREEXISTING_VALUE);
    assertThatThrownBy(() -> jedis.llen("string")).hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void llen_givenWrongNumOfArgs_returnsError() {
    assertExactNumberOfArgs(jedis, Protocol.Command.LLEN, 1);
  }

  @Test
  public void llen_givenNonexistentList_returnsZero() {
    assertThat(jedis.llen("nonexistent")).isEqualTo(0L);
  }

  @Test
  public void llen_returnsListLength() {
    jedis.lpush(KEY, "e1", "e2", "e3");
    assertThat(jedis.llen(KEY)).isEqualTo(3L);

    jedis.lpop(KEY);
    assertThat(jedis.llen(KEY)).isEqualTo(2L);

    jedis.lpop(KEY);
    assertThat(jedis.llen(KEY)).isEqualTo(1L);

    jedis.lpop(KEY);
    assertThat(jedis.llen(KEY)).isEqualTo(0L);
  }

  @Test
  public void llen_withConcurrentLPush_returnsCorrectValue() {
    String[] valuesInitial = new String[] {"one", "two", "three"};
    String[] valuesToAdd = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.lpush(KEY, valuesInitial);

    final AtomicLong llenReference = new AtomicLong();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.lpush(KEY, valuesToAdd),
        i -> llenReference.set(jedis.llen(KEY)))
            .runWithAction(() -> {
              assertThat(llenReference).satisfiesAnyOf(
                  llenResult -> assertThat(llenResult.get())
                      .isEqualTo(valuesInitial.length),
                  llenResult -> assertThat(llenResult.get())
                      .isEqualTo(valuesInitial.length + valuesToAdd.length));
              for (int i = 0; i < valuesToAdd.length; i++) {
                jedis.lpop(KEY);
              }
            });
  }
}
