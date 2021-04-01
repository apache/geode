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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractGetBitIntegrationTest implements RedisPortSupplier {

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
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.GETBIT, 2);
  }

  @Test
  public void getbit_givenSetFails() {
    jedis.sadd("key", "m1");
    assertThatThrownBy(() -> jedis.getbit("key", 1)).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void getbit_givenNonExistentKeyReturnsFalse() {
    assertThat(jedis.getbit("does not exist", 1)).isFalse();
    assertThat(jedis.exists("does not exist")).isFalse();
  }

  @Test
  public void getbit_givenNoBitsReturnsFalse() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0};
    jedis.set(key, bytes);
    assertThat(jedis.getbit(key, 1)).isFalse();
  }

  @Test
  public void getbit_givenOneBitReturnsTrue() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.getbit(key, 8 + 7)).isTrue();
  }

  @Test
  public void getbit_pastEndReturnsFalse() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.getbit(key, 8 + 8 + 7)).isFalse();
  }
}
