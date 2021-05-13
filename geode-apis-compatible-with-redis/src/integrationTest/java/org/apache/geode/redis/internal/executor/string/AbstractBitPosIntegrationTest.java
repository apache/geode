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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractBitPosIntegrationTest implements RedisIntegrationTest {

  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void bitpos_errors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.BITPOS, 2);
  }

  @Test
  public void bitpos_givenSetFails() {
    jedis.sadd("key", "m1");
    assertThatThrownBy(() -> jedis.bitpos("key", false)).hasMessageContaining("WRONGTYPE");
    assertThatThrownBy(() -> jedis.bitpos("key", true)).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void bitpos_givenNonExistentKeyReturnsExpectedValue() {
    assertThat(jedis.bitpos("does not exist", false)).isEqualTo(0);
    assertThat(jedis.bitpos("does not exist", true)).isEqualTo(-1);
    assertThat(jedis.bitpos("does not exist", false, new BitPosParams(4, 7))).isEqualTo(0);
    assertThat(jedis.bitpos("does not exist", true, new BitPosParams(4, 7))).isEqualTo(-1);
    assertThat(jedis.exists("does not exist")).isFalse();
  }

  @Test
  public void bitpos_givenEmptyKeyReturnsExpectedValue() {
    jedis.set("emptyKey", "");
    assertThat(jedis.bitpos("emptyKey", false)).isEqualTo(-1);
    assertThat(jedis.bitpos("emptyKey", true)).isEqualTo(-1);
    assertThat(jedis.bitpos("emptyKey", false, new BitPosParams(4, 7))).isEqualTo(-1);
    assertThat(jedis.bitpos("emptyKey", true, new BitPosParams(4, 7))).isEqualTo(-1);
  }

  @Test
  public void bitpos_givenStartGreaterThanEnd() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(3, 2))).isEqualTo(-1);
  }

  @Test
  public void bitpos_givenBitInFirstByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true)).isEqualTo(7);
  }

  @Test
  public void bitpos_givenOneInSecondByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0, 1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true)).isEqualTo(7 + 8);
  }

  @Test
  public void bitposFalse_givenBitInFirstByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {-2, 1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, false)).isEqualTo(7);
  }

  @Test
  public void bitposFalse_givenOneInSecondByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {-1, -2, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, false)).isEqualTo(7 + 8);
  }

  @Test
  public void bitposWithStart_givenOneInLastByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(-1))).isEqualTo(7 + 3 * 8);
  }

  @Test
  public void bitposWithStartAndEnd_givenStartAndEndEqual() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(2, 2))).isEqualTo(7 + 2 * 8);
  }

  @Test
  public void bitposWithStartAndEnd_givenStartAndEndNegative() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(-2, -1))).isEqualTo(7 + 2 * 8);
  }

  @Test
  public void bitposWithStartAndEnd_givenEndGreaterThanOrEqualToByteArrayLength() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(0, bytes.length))).isEqualTo(7);
    assertThat(jedis.bitpos(key, true, new BitPosParams(0, bytes.length + 1))).isEqualTo(7);
  }

  @Test
  public void bitposWithStartAndEnd_givenNoBits() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 0, 0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(1, 2))).isEqualTo(-1);
  }

  @Test
  public void bitposWithStart_givenStartMoreNegativeThanByteArrayLength() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(-(bytes.length + 1)))).isEqualTo(7);
  }

  @Test
  public void bitposFalseWithStartAndEnd_givenEndGreaterThanByteArrayLengthAndNoBitFound() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {-1, -1, -1, -1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, false, new BitPosParams(0, bytes.length))).isEqualTo(-1);
  }

  @Test
  public void bitposFalseWithStart_givenNoBitFound() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {-1, -1, -1, -1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, false, new BitPosParams(0))).isEqualTo(bytes.length * 8);
  }
}
