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
package org.apache.geode.redis.internal.executor.string;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractSetEXIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void testSetEX() {
    jedis.setex("key", 20L, "value");

    assertThat(jedis.ttl("key")).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.SETEX, 3);
  }

  @Test
  public void givenMoreThanFourArgumentsProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.SETEX, "key", "10", "value", "extraArg"))
            .hasMessageContaining("ERR wrong number of arguments for 'setex' command");
  }

  @Test
  public void testSetEXWithIllegalSeconds() {
    assertThatThrownBy(() -> jedis.setex("key", -1L, "value"))
        .hasMessage("ERR invalid expire time in setex");
  }
}
