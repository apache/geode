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
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractLPushxIntegrationTest implements RedisIntegrationTest {
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
  public void lpushxErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.LPUSHX, 2);
  }

  @Test
  public void lpushx_withExistingKey_ofWrongType_returnsWrongTypeError_shouldNotOverWriteExistingKey() {
    String elementValue = "list element value that should never get added";

    jedis.set(KEY, PREEXISTING_VALUE);

    assertThatThrownBy(() -> jedis.lpushx(KEY, elementValue))
        .isInstanceOf(JedisDataException.class);

    String result = jedis.get(KEY);

    assertThat(result).isEqualTo(PREEXISTING_VALUE);
  }

  @Test
  public void lpushx_doesNothingIfKeyDoesntExist() {
    assertThat(jedis.exists(KEY)).isFalse();
    Long result = jedis.lpushx(KEY, "e1");
    assertThat(result).isEqualTo(0);
    assertThat(jedis.exists(KEY)).isFalse();
  }

  @Test
  public void lpushx_returnsUpdatedListLength() {
    Long result = jedis.lpush(KEY, "e0");
    assertThat(result).isEqualTo(1);

    result = jedis.lpushx(KEY, "e1");
    assertThat(result).isEqualTo(2);

    result = jedis.lpushx(KEY, "e2");
    assertThat(result).isEqualTo(3);

    result = jedis.lpushx(KEY, "e3", "e4");
    assertThat(result).isEqualTo(5);
  }

  @Test
  public void lpushx_addsElementsInCorrectOrder_onRepeatedInvocation() {
    jedis.lpush(KEY, "e0");
    jedis.lpushx(KEY, "e1");
    jedis.lpushx(KEY, "e2");
    jedis.lpushx(KEY, "e3");

    String result = jedis.lpop(KEY);
    assertThat(result).isEqualTo("e3");

    result = jedis.lpop(KEY);
    assertThat(result).isEqualTo("e2");

    result = jedis.lpop(KEY);
    assertThat(result).isEqualTo("e1");
  }

  @Test
  public void lpushx_addsElementsInCorrectOrder_givenMultipleElements() {
    jedis.lpush(KEY, "e0");
    jedis.lpushx(KEY, "e1", "e2", "e3");
    jedis.lpushx(KEY, "e4", "e5", "e6");

    String result = jedis.lpop(KEY);
    assertThat(result).isEqualTo("e6");

    result = jedis.lpop(KEY);
    assertThat(result).isEqualTo("e5");

    result = jedis.lpop(KEY);
    assertThat(result).isEqualTo("e4");

    result = jedis.lpop(KEY);
    assertThat(result).isEqualTo("e3");

    result = jedis.lpop(KEY);
    assertThat(result).isEqualTo("e2");

    result = jedis.lpop(KEY);
    assertThat(result).isEqualTo("e1");
  }
}
