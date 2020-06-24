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

package org.apache.geode.redis.internal.executor.key;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class KeysIntegrationTest {

  public static Jedis jedis;
  public static int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
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
  public void givenSplat_withASCIIdata_returnsExpectedMatches() {
    jedis.set("string1", "v1");
    jedis.sadd("set1", "member1");
    jedis.hset("hash1", "key1", "field1");
    assertThat(jedis.keys("*")).containsExactlyInAnyOrder("string1", "set1", "hash1");
    assertThat(jedis.keys("s*")).containsExactlyInAnyOrder("string1", "set1");
    assertThat(jedis.keys("h*")).containsExactlyInAnyOrder("hash1");
    assertThat(jedis.keys("foo*")).isEmpty();
  }

  @Test
  public void givenSplat_withBinaryData_returnsExpectedMatches() {
    byte[] stringKey =
        new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 't', 'r', 'i', 'n', 'g', '1'};
    byte[] value = new byte[] {'v', '1'};
    jedis.set(stringKey, value);
    byte[] setKey = new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 'e', 't', '1'};
    byte[] member = new byte[] {'m', '1'};
    jedis.sadd(setKey, member);
    byte[] hashKey = new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 'h', 'a', 's', 'h', '1'};
    byte[] key = new byte[] {'k', 'e', 'y', '1'};
    jedis.hset(hashKey, key, value);
    assertThat(jedis.exists(stringKey));
    assertThat(jedis.exists(setKey));
    assertThat(jedis.exists(hashKey));
    assertThat(jedis.keys(new byte[] {'*'})).containsExactlyInAnyOrder(stringKey, setKey, hashKey);
    assertThat(jedis.keys(new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', '*'}))
        .containsExactlyInAnyOrder(stringKey, setKey);
    assertThat(jedis.keys(new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 'h', '*'}))
        .containsExactlyInAnyOrder(hashKey);
    assertThat(jedis.keys(new byte[] {'f', '*'})).isEmpty();
  }

  @Test
  public void givenBinaryValue_withExactMatch_preservesBinaryData() {
    byte[] stringKey =
        new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 't', 'r', 'i', 'n', 'g', '1'};
    jedis.set(stringKey, stringKey);
    assertThat(jedis.keys(stringKey)).containsExactlyInAnyOrder(stringKey);
  }

  @Test
  public void givenMalformedGlobPattern_returnsEmptySet() {
    assertThat(jedis.keys("[][]")).isEmpty();
  }
}
