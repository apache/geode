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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractRenameNXIntegrationTest implements RedisPortSupplier {

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
  public void shouldFail_givenKeyToRenameDoesNotExist() {
    assertThatThrownBy(() -> jedis.renamenx("nonexistentKey", "pineApple"))
        .hasMessageContaining("ERR no such key");
  }

  @Test
  public void shouldRenameKey_givenNewNameDoesNotExist() {

    jedis.set("key", "value");
    jedis.renamenx("key", "pineApple");

    assertThat(jedis.get("key")).isNull();
    assertThat(jedis.get("pineApple")).isEqualTo("value");
  }

  @Test
  public void shouldReturn1_givenNewNameDoesNotExist() {

    jedis.set("key", "value");
    Long result = jedis.renamenx("key", "pineApple");

    assertThat(result).isEqualTo(1);
  }

  @Test
  public void shouldNotRenameKey_givenNewNameDoesExist() {
    jedis.set("key", "value");
    jedis.set("anotherExistingKey", "anotherValue");

    jedis.renamenx("key", "anotherExistingKey");

    assertThat(jedis.get("key")).isEqualTo("value");
    assertThat(jedis.get("anotherExistingKey")).isEqualTo("anotherValue");
  }

  @Test
  public void shouldReturn0_givenNewNameDoesNotExist() {
    jedis.set("key", "value");
    jedis.set("anotherExistingKey", "anotherValue");

    Long result = jedis.renamenx("key", "anotherExistingKey");

    assertThat(result).isEqualTo(0);
  }
}
