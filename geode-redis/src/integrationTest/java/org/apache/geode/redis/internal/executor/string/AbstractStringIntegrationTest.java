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

import java.nio.charset.StandardCharsets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractStringIntegrationTest implements RedisPortSupplier {

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
  public void strlen_errorsGivenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.STRLEN, 1);
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
  public void testStrlen_withEmptyByte() {
    byte[] key = new byte[] {0};
    jedis.set(key, new byte[] {});

    assertThat(jedis.strlen(key)).isEqualTo(0);
  }

  @Test
  public void testStrlen_withBinaryData() {
    byte[] zero = new byte[] {0};
    jedis.set(zero, zero);

    assertThat(jedis.strlen(zero)).isEqualTo(1);
  }

  @Test
  public void testStrlen_withUTF16BinaryData() {
    String test_utf16_string = "最𐐷𤭢";
    byte[] testBytes = test_utf16_string.getBytes(StandardCharsets.UTF_16);
    jedis.set(testBytes, testBytes);

    assertThat(jedis.strlen(testBytes)).isEqualTo(12);
  }

  @Test
  public void testStrlen_withIntData() {
    byte[] key = new byte[] {0};
    byte[] value = new byte[] {1, 0, 0};
    jedis.set(key, value);

    assertThat(jedis.strlen(key)).isEqualTo(value.length);
  }

  @Test
  public void testStrlen_withFloatData() {
    byte[] key = new byte[] {0};
    byte[] value = new byte[] {'0', '.', '9'};
    jedis.set(key, value);

    assertThat(jedis.strlen(key)).isEqualTo(value.length);
  }

  @Test
  public void testDecr_ErrorsWithWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.DECR, 1);
  }

  @Test
  public void testDecr_withWrongType_shouldError() {
    String key = "hashKey";
    jedis.hset(key, "field", "non-int value");

    assertThatThrownBy(() -> jedis.decr(key))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void testDecr_decrementsPositiveIntegerValues() {
    String key = "key";
    jedis.set(key, "10");

    assertThat(jedis.decr(key)).isEqualTo(9L);
    assertThat(jedis.get(key)).isEqualTo("9");
  }

  @Test
  public void testDecr_returnsValueWhenDecrementingResultsInNegativeNumber() {
    String key = "key";
    jedis.set(key, "0");

    assertThat(jedis.decr(key)).isEqualTo(-1L);
    assertThat(jedis.get(key)).isEqualTo("-1");
  }
}
