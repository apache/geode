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

import org.apache.geode.redis.GeodeRedisServerRule;

public class GetBitIntegrationTest {

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
