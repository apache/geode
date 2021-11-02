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

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class QuitIntegrationTest {
  protected static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  protected static Jedis jedis;

  @ClassRule
  public static GeodeRedisServerRule server =
      new GeodeRedisServerRule();

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void quit_returnsOK() {
    String reply = jedis.quit();
    assertThat(reply).isEqualTo("OK");
  }

  @Test
  public void quit_closesConnection() {
    jedis.sendCommand(Protocol.Command.QUIT);
    assertThatThrownBy(() -> jedis.ping()).isInstanceOf(JedisConnectionException.class);
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

  @Test
  public void quit_preventsSubsequentCommandExecution() {
    Pipeline pipeline = jedis.pipelined();

    String key = randString();
    String value = randString();

    pipeline.sendCommand(Protocol.Command.QUIT, new String[] {});
    pipeline.sendCommand(Protocol.Command.SET, key, value);

    assertThatThrownBy(pipeline::sync).isInstanceOf(JedisConnectionException.class);

    jedis = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
    assertThat(jedis.get(key)).isNull();
  }
}
