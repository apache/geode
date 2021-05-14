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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_OVERFLOW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractIncrIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.INCR, 1);
  }

  @Test
  public void testIncr() {
    String oneHundredKey = randString();
    String negativeOneHundredKey = randString();
    String unsetKey = randString();

    final int oneHundredValue = 100;
    final int negativeOneHundredValue = -100;

    jedis.set(oneHundredKey, Integer.toString(oneHundredValue));
    jedis.set(negativeOneHundredKey, Integer.toString(negativeOneHundredValue));

    jedis.incr(oneHundredKey);
    jedis.incr(negativeOneHundredKey);
    jedis.incr(unsetKey);

    assertThat(jedis.get(oneHundredKey)).isEqualTo(Integer.toString(oneHundredValue + 1));
    assertThat(jedis.get(negativeOneHundredKey))
        .isEqualTo(Integer.toString(negativeOneHundredValue + 1));
    assertThat(jedis.get(unsetKey)).isEqualTo(Integer.toString(1));
  }

  @Test
  public void testIncr_whenOverflow_shouldReturnError() {
    String key = "key";
    String max64BitIntegerValue = "9223372036854775807";
    jedis.set(key, max64BitIntegerValue);

    assertThatThrownBy(() -> jedis.incr(key)).hasMessageContaining(ERROR_OVERFLOW);
    assertThat(jedis.get(key)).isEqualTo(max64BitIntegerValue);
  }

  @Test
  public void testIncr_whenWrongType_shouldReturnError() {
    String key = "key";
    String nonIntegerValue = "I am not a number! I am a free man!";

    assertThat(jedis.set(key, nonIntegerValue)).isEqualTo("OK");
    assertThatThrownBy(() -> jedis.incr(key)).hasMessageContaining(ERROR_NOT_INTEGER);
    assertThat(jedis.get(key)).isEqualTo(nonIntegerValue);
  }

  @Test
  public void testIncr_shouldBeAtomic() {
    jedis.set("contestedKey", "0");

    int ITERATION_COUNT = 4000;
    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> jedis.incr("contestedKey"),
        (i) -> jedis.incr("contestedKey"))
            .run();

    assertThat(jedis.get("contestedKey")).isEqualTo(Integer.toString(2 * ITERATION_COUNT));
  }

  @Test
  public void testIncr_shouldError_onValueGreaterThanMax() {
    jedis.set("key", "9223372036854775808");

    assertThatThrownBy(() -> jedis.incr("key")).hasMessageContaining(ERROR_NOT_INTEGER);
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
