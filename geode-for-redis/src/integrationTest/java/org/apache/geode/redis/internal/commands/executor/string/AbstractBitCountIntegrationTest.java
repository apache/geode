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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
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

public abstract class AbstractBitCountIntegrationTest implements RedisIntegrationTest {

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
  public void bitcount_errors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.BITCOUNT, 1);
  }

  @Test
  public void bitcount_givenSetFails() {
    jedis.sadd("key", "m1");
    assertThatThrownBy(() -> jedis.bitcount("key")).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void bitcount_givenNonExistentKeyReturnsZero() {
    assertThat(jedis.bitcount("does not exist")).isEqualTo(0);
    assertThat(jedis.exists("does not exist")).isFalse();
  }

  @Test
  public void bitcount_withStartAndEnd_givenNonExistentKeyReturnsZero() {
    assertThat(jedis.bitcount("does not exist", 1, 3)).isEqualTo(0);
    assertThat(jedis.exists("does not exist")).isFalse();
  }

  @Test
  public void bitcount_givenEmptyStringReturnsZero() {
    jedis.set("key", "");
    assertThat(jedis.bitcount("key")).isEqualTo(0);
  }

  @Test
  public void bitcount_givenOneBitReturnsOne() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 0, 0, 0, 0};
    jedis.set(key, bytes);
    assertThat(jedis.bitcount(key)).isEqualTo(1);
  }

  @Test
  public void bitcount_givenTwoBitsReturnsTwo() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 0, 0, 0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitcount(key)).isEqualTo(2);
  }

  @Test
  public void bitcount_givenEmptyRangeReturnsZero() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 0, 0, 0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitcount(key, 1, 3)).isEqualTo(0);
  }

  @Test
  public void bitcount_correctForAllByteValues() {
    byte[] key = {1, 2, 3};
    byte[] value = {0};
    for (int b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; b++) {
      value[0] = (byte) b;
      jedis.set(key, value);
      assertThat(jedis.bitcount(key)).as("b=" + b).isEqualTo(Integer.bitCount(0xFF & b));
    }
  }
}
