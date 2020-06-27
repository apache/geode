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
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;

public class BitOpIntegrationTest {

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
  public void bitop_givenSetFails() {
    jedis.sadd("foo", "m1");
    assertThatThrownBy(() -> jedis.bitop(BitOP.AND, "key", "foo"))
        .hasMessageContaining("WRONGTYPE");
    assertThatThrownBy(() -> jedis.bitop(BitOP.OR, "key", "foo")).hasMessageContaining("WRONGTYPE");
    assertThatThrownBy(() -> jedis.bitop(BitOP.XOR, "key", "foo"))
        .hasMessageContaining("WRONGTYPE");
    assertThatThrownBy(() -> jedis.bitop(BitOP.NOT, "key", "foo"))
        .hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void bitopNOT_givenNothingLeavesKeyUnset() {
    assertThat(jedis.bitop(BitOP.NOT, "key", "foo")).isEqualTo(0);
    assertThat(jedis.exists("key")).isFalse();
  }

  @Test
  public void bitopNOT_givenNothingDeletesKey() {
    jedis.set("key", "value");
    assertThat(jedis.bitop(BitOP.NOT, "key", "foo")).isEqualTo(0);
    assertThat(jedis.exists("key")).isFalse();
  }

  @Test
  public void bitopNOT_givenNothingDeletesSet() {
    jedis.sadd("key", "value");
    assertThat(jedis.bitop(BitOP.NOT, "key", "foo")).isEqualTo(0);
    assertThat(jedis.exists("key")).isFalse();
  }

  @Test
  public void bitopNOT_givenEmptyStringDeletesKey() {
    jedis.set("key", "value");
    jedis.set("foo", "");
    assertThat(jedis.bitop(BitOP.NOT, "key", "foo")).isEqualTo(0);
    assertThat(jedis.exists("key")).isFalse();
  }

  @Test
  public void bitopNOT_givenEmptyStringDeletesSet() {
    jedis.sadd("key", "value");
    jedis.set("foo", "");
    assertThat(jedis.bitop(BitOP.NOT, "key", "foo")).isEqualTo(0);
    assertThat(jedis.exists("key")).isFalse();
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
    byte[] key = {1};
    byte[] other = {2};
    byte[] bytes = {1};
    jedis.set(other, bytes);
    assertThat(jedis.bitop(BitOP.NOT, key, other)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0xFE);
  }

  @Test
  public void bitopAND_givenSelfAndOther() {
    byte[] key = {1};
    byte[] other = {2};
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
    byte[] key = {1};
    byte[] other = {2};
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
    byte[] key = {1};
    byte[] other = {2};
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
    byte[] key = {1};
    byte[] other = {2};
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
    byte[] key = {1};
    byte[] other = {2};
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
    byte[] key = {1};
    byte[] other = {2};
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
