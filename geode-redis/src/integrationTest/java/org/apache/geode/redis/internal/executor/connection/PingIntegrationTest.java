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

package org.apache.geode.redis.internal.executor.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.Protocol.Command.PING;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class PingIntegrationTest {
  protected static int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  protected static Jedis jedis;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @AfterClass
  public static void tearDown() {
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
  public void ping_withTwoArgs_fails() {
    assertThatThrownBy(() -> jedis.sendCommand(PING, "one", "two"))
        .hasMessageContaining("ERR wrong number of arguments for 'ping' command");
  }
}
