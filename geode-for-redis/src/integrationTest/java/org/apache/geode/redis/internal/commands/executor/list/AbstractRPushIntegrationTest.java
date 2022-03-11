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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractRPushIntegrationTest implements RedisIntegrationTest {
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
  public void rpushErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.RPUSH, 2);
  }

  @Test
  public void rpush_withExistingKey_ofWrongType_returnsWrongTypeError_shouldNotOverWriteExistingKey() {
    String elementValue = "list element value that should never get added";

    jedis.set(KEY, PREEXISTING_VALUE);

    assertThatThrownBy(() -> jedis.rpush(KEY, elementValue)).hasMessage(ERROR_WRONG_TYPE);

    assertThat(jedis.get(KEY)).isEqualTo(PREEXISTING_VALUE);
  }

  @Test
  public void rpush_returnsUpdatedListLength() {
    assertThat(jedis.rpush(KEY, "e1")).isEqualTo(1L);
    assertThat(jedis.rpush(KEY, "e2")).isEqualTo(2L);
    assertThat(jedis.rpush(KEY, "e3", "e4")).isEqualTo(4L);
  }

  @Test
  public void rpush_addsElementsInCorrectOrder_onRepeatedInvocation() {
    jedis.rpush(KEY, "e1");
    jedis.rpush(KEY, "e2");
    jedis.rpush(KEY, "e3");

    assertThat(jedis.lpop(KEY)).isEqualTo("e1");
    assertThat(jedis.lpop(KEY)).isEqualTo("e2");
    assertThat(jedis.lpop(KEY)).isEqualTo("e3");
  }

  @Test
  public void rpush_addsElementsInCorrectOrder_givenMultipleElements() {
    jedis.rpush(KEY, "e1", "e2", "e3");
    jedis.rpush(KEY, "e4", "e5", "e6");

    assertThat(jedis.lpop(KEY)).isEqualTo("e1");
    assertThat(jedis.lpop(KEY)).isEqualTo("e2");
    assertThat(jedis.lpop(KEY)).isEqualTo("e3");
    assertThat(jedis.lpop(KEY)).isEqualTo("e4");
    assertThat(jedis.lpop(KEY)).isEqualTo("e5");
    assertThat(jedis.lpop(KEY)).isEqualTo("e6");
  }
}
