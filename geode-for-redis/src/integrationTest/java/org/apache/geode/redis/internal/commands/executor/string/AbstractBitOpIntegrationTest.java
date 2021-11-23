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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.redis.internal.commands.executor.string.BitOpExecutor.ERROR_BITOP_NOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractBitOpIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private final String hashTag = "{111}";
  private final String destKey = "destKey" + hashTag;
  private final String srcKey = "srcKey" + hashTag;
  private final String value = "value";
  private final byte[] key = {1, '{', 111, '}'};
  private final byte[] other = {2, '{', 111, '}'};

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
  public void bitOp_errors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.BITOP, 3);
  }

  @Test
  public void bitop_givenInvalidOperationType_returnsSyntaxError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(hashTag, Protocol.Command.BITOP, "invalidOp", destKey,
            srcKey)).hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void bitop_givenSetFails() {
    jedis.sadd(srcKey, "m1");
    assertThatThrownBy(() -> jedis.bitop(BitOP.AND, destKey, srcKey))
        .hasMessageContaining(ERROR_WRONG_TYPE);
    assertThatThrownBy(() -> jedis.bitop(BitOP.OR, destKey, srcKey))
        .hasMessageContaining(ERROR_WRONG_TYPE);
    assertThatThrownBy(() -> jedis.bitop(BitOP.XOR, destKey, srcKey))
        .hasMessageContaining(ERROR_WRONG_TYPE);
    assertThatThrownBy(() -> jedis.bitop(BitOP.NOT, destKey, srcKey))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void bitopNOT_givenMoreThanOneSourceKey_returnsError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(
            hashTag, Protocol.Command.BITOP, "NOT", destKey, srcKey, "srcKey2" + hashTag))
                .hasMessageContaining(ERROR_BITOP_NOT);
  }

  @Test
  public void bitopNOT_givenNothingLeavesKeyUnset() {
    assertThat(jedis.bitop(BitOP.NOT, destKey, srcKey)).isEqualTo(0);
    assertThat(jedis.exists(destKey)).isFalse();
  }

  @Test
  public void bitopNOT_givenNothingDeletesKey() {
    jedis.set(destKey, value);
    assertThat(jedis.bitop(BitOP.NOT, destKey, srcKey)).isEqualTo(0);
    assertThat(jedis.exists(destKey)).isFalse();
  }

  @Test
  public void bitopNOT_givenNothingDeletesSet() {
    jedis.sadd(destKey, value);
    assertThat(jedis.bitop(BitOP.NOT, destKey, srcKey)).isEqualTo(0);
    assertThat(jedis.exists(destKey)).isFalse();
  }

  @Test
  public void bitopNOT_givenEmptyStringDeletesKey() {
    jedis.set(destKey, value);
    jedis.set(srcKey, "");
    assertThat(jedis.bitop(BitOP.NOT, destKey, srcKey)).isEqualTo(0);
    assertThat(jedis.exists(destKey)).isFalse();
  }

  @Test
  public void bitopNOT_givenEmptyStringDeletesSet() {
    jedis.sadd(destKey, value);
    jedis.set(srcKey, "");
    assertThat(jedis.bitop(BitOP.NOT, destKey, srcKey)).isEqualTo(0);
    assertThat(jedis.exists(destKey)).isFalse();
  }

  @Test
  public void bitopNOT_negatesSelf() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1};
    jedis.set(key, bytes);
    assertThat(jedis.bitop(BitOP.NOT, key, key)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0xFE);
  }

  @Test
  public void bitopNOT_createsNonExistingKey() {
    byte[] bytes = {1};
    jedis.set(other, bytes);
    assertThat(jedis.bitop(BitOP.NOT, key, other)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0xFE);
  }

  @Test
  public void bitopAND_givenSelfAndOther() {
    byte[] bytes = {1};
    byte[] otherBytes = {-1};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.AND, key, key, other)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 1);
  }

  @Test
  public void bitopAND_givenSelfAndLongerOther() {
    byte[] bytes = {1};
    byte[] otherBytes = {-1, 3};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.AND, key, key, other)).isEqualTo(2);
    assertThat(jedis.strlen(key)).isEqualTo(2);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 1);
    assertThat(newbytes[1]).isEqualTo((byte) 0);
  }

  @Test
  public void bitopOR_givenSelfAndOther() {
    byte[] bytes = {1};
    byte[] otherBytes = {8};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.OR, key, key, other)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 9);
  }

  @Test
  public void bitopOR_givenSelfAndLongerOther() {
    byte[] bytes = {1};
    byte[] otherBytes = {-1, 3};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.OR, key, key, other)).isEqualTo(2);
    assertThat(jedis.strlen(key)).isEqualTo(2);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) -1);
    assertThat(newbytes[1]).isEqualTo((byte) 3);
  }

  @Test
  public void bitopXOR_givenSelfAndOther() {
    byte[] bytes = {9};
    byte[] otherBytes = {8};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.XOR, key, key, other)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 1);
  }

  @Test
  public void bitopXOR_givenSelfAndLongerOther() {
    byte[] bytes = {1};
    byte[] otherBytes = {-1, 3};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.XOR, key, key, other)).isEqualTo(2);
    assertThat(jedis.strlen(key)).isEqualTo(2);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0xFE);
    assertThat(newbytes[1]).isEqualTo((byte) 3);
  }
}
