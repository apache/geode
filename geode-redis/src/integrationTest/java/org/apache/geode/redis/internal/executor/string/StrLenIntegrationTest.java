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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.redis.internal.RedisConstants;

public class StrLenIntegrationTest {

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
  public void testStrlen_requestNonexistentKey_returnsZero() {
    Long result = jedis.strlen("Nohbdy");
    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testStrlen_requestKey_returnsLengthOfStringValue() {
    String value = "byGoogle";

    jedis.set("golang", value);

    Long result = jedis.strlen("golang");
    assertThat(result).isEqualTo(value.length());
  }

  @Test
  public void testStrlen_requestWrongType_shouldReturnError() {
    String key = "hashKey";
    jedis.hset(key, "field", "this value doesn't matter");

    assertThatThrownBy(() -> jedis.strlen(key))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void testStrlen_withBinaryData() {
    byte[] zero = new byte[] {0};
    jedis.set(zero, zero);

    assertThat(jedis.strlen(zero)).isEqualTo(1);
  }

}
