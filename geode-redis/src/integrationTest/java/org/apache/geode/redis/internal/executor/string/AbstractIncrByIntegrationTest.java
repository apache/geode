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

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractIncrByIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;
  private Random rand;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    rand = new Random();
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.INCRBY, 2);
  }

  @Test
  public void testIncrBy() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int incr1 = rand.nextInt(100);
    int incr2 = rand.nextInt(100);
    Long incr3 = Long.MAX_VALUE / 2;
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, "" + num1);
    jedis.set(key2, "" + num2);
    jedis.set(key3, "" + Long.MAX_VALUE);

    jedis.incrBy(key1, incr1);
    jedis.incrBy(key2, incr2);
    assertThat(jedis.get(key1)).isEqualTo("" + (num1 + incr1 * 1));
    assertThat(jedis.get(key2)).isEqualTo("" + (num2 + incr2 * 1));

    Exception ex = null;
    try {
      jedis.incrBy(key3, incr3);
    } catch (Exception e) {
      ex = e;
    }
    assertThat(ex).isNotNull();
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
