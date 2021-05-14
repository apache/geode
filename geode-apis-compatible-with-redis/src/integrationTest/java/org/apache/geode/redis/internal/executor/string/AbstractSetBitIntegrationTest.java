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

public abstract class AbstractSetBitIntegrationTest implements RedisIntegrationTest {

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
    assertExactNumberOfArgs(jedis, Protocol.Command.SETBIT, 3);
  }

  @Test
  public void givenMoreThanFourArgumentsProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.SETBIT, "key", "1", "value", "extraArg"))
            .hasMessageContaining("ERR wrong number of arguments for 'setbit' command");
  }

  @Test
  public void setbit_givenSetFails() {
    jedis.sadd("key", "m1");
    assertThatThrownBy(() -> jedis.setbit("key", 1, true)).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void setbit_givenNonExistentKeyCreatesString() {
    assertThat(jedis.setbit("newKey", 1, true)).isFalse();
    assertThat(jedis.exists("newKey")).isTrue();
    assertThat(jedis.type("newKey")).isEqualTo("string");
    assertThat(jedis.getbit("newKey", 1)).isTrue();
  }

  @Test
  public void setbit_canSetOneBit() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0};
    jedis.set(key, bytes);
    assertThat(jedis.setbit(key, 1, true)).isFalse();
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0x40);
  }

  @Test
  public void setbit_canSetOneBitAlreadySet() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1};
    jedis.set(key, bytes);
    assertThat(jedis.setbit(key, 7, true)).isTrue();
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 1);
  }

  @Test
  public void setbit_canSetOneBitPastEnd() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0};
    jedis.set(key, bytes);
    assertThat(jedis.setbit(key, 1 + 8, true)).isFalse();
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0);
    assertThat(newbytes[1]).isEqualTo((byte) 0x40);
  }
}
