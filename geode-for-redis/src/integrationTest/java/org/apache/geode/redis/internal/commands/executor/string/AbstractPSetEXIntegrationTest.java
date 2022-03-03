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
package org.apache.geode.redis.internal.commands.executor.string;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_EXPIRE_TIME;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.Protocol.Command.PSETEX;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractPSetEXIntegrationTest implements RedisIntegrationTest {

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
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, PSETEX, 3);
  }

  @Test
  public void testPSetEX() {
    jedis.psetex("key", 20000, "value");

    assertThat(jedis.pttl("key")).isGreaterThanOrEqualTo(1500);
  }

  @Test
  public void testPSetEXWithIllegalMilliseconds() {
    assertThatThrownBy(() -> jedis.psetex("key", -1L, "value"))
        .hasMessage(String.format(ERROR_INVALID_EXPIRE_TIME, "psetex"));
  }

  @Test
  public void pSetEXWithNonIntegerExpiration_returnsError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", PSETEX, "key", "notAnInteger", "value"))
        .hasMessage(ERROR_NOT_INTEGER);
  }
}
