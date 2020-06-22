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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;

public class BitCountIntegrationTest {

  static Jedis jedis;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), 10000000);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
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

  @Test
  public void bitcount_givenBitInFirstByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true)).isEqualTo(7);
  }

  @Test
  public void bitcount_givenOneInSecondByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0, 1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true)).isEqualTo(7 + 8);
  }

  @Test
  public void bitcountFalse_givenBitInFirstByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {-2, 1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, false)).isEqualTo(7);
  }

  @Test
  public void bitcountFalse_givenOneInSecondByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {-1, -2, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, false)).isEqualTo(7 + 8);
  }

  @Test
  public void bitcountWithStart_givenOneInLastByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(-1))).isEqualTo(7 + 3 * 8);
  }

  @Test
  public void bitcountWithStartAndEnd_givenNoBits() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 0, 0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(1, 2))).isEqualTo(-1);
  }
}
