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
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractBitPosIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), 10000000);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
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
}
