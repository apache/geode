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
 *
 */

package org.apache.geode.redis.internal.commands.executor.connection;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtMostNArgs;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractPingIntegrationTest implements RedisIntegrationTest {

  protected static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  protected static Jedis jedis;

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void ping_withNoArgs_returnsPong() {
    String result = jedis.ping();
    assertThat(result).isEqualTo("PONG");
  }

  @Test
  public void ping_withArg_returnsArg() {
    String result = jedis.ping("hello world");
    assertThat(result).isEqualTo("hello world");
  }

  @Test
  public void ping_withBinaryArg_returnsBinaryArg() {
    byte[] binaryArg = new byte[256];
    byte byteValue = Byte.MIN_VALUE;
    for (int i = 0; i < binaryArg.length; i++) {
      binaryArg[i] = byteValue++;
    }
    byte[] result = jedis.ping(binaryArg);
    assertThat(result).isEqualTo(binaryArg);
  }

  @Test
  public void errors_GivenTooManyArguments() {
    assertAtMostNArgs(jedis, Protocol.Command.PING, 1);
  }
}
