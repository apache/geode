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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NO_SUCH_KEY;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

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
    assertThatThrownBy(() -> jedis.lset(KEY, 1, newValue))
        .hasMessage(ERROR_NO_SUCH_KEY);
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
}
