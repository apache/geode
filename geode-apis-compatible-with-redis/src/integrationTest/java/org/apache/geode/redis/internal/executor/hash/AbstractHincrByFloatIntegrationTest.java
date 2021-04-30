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

package org.apache.geode.redis.internal.executor.hash;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

import java.math.BigDecimal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractHincrByFloatIntegrationTest implements RedisIntegrationTest {

  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  private JedisCluster jedis;
  private JedisCluster jedis2;
  private static int ITERATION_COUNT = 4000;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
    jedis2 = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
    jedis2.close();
  }

  @Test
  public void errors_GivenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.HINCRBYFLOAT, 3);
  }

  @Test
  public void testHincrByFloat_withInfinityAndVariants() {
    jedis.hset("key", "number", "1.4");

    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.HINCRBYFLOAT, "key", "number", "+inf"))
            .hasMessage("ERR increment would produce NaN or Infinity");

    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.HINCRBYFLOAT, "key", "number", "-inf"))
            .hasMessage("ERR increment would produce NaN or Infinity");

    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.HINCRBYFLOAT, "key", "number", "inf"))
            .hasMessage("ERR increment would produce NaN or Infinity");

    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.HINCRBYFLOAT, "key", "number", "+infinity"))
            .hasMessage("ERR increment would produce NaN or Infinity");

    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.HINCRBYFLOAT, "key", "number", "-infinity"))
            .hasMessage("ERR increment would produce NaN or Infinity");

    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.HINCRBYFLOAT, "key", "number", "infinity"))
            .hasMessage("ERR increment would produce NaN or Infinity");

    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.HINCRBYFLOAT, "key", "number", "nan"))
            .hasMessage("ERR value is not a valid float");

    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.HINCRBYFLOAT, "key", "number", "infant"))
            .hasMessage("ERR value is not a valid float");
  }

  @Test
  public void testHincrByFloat() {
    double incr = 0.37373;
    String key = "key";
    String field = "field";
    jedis.hset(key, field, "1.0");

    double response = jedis.hincrByFloat(key, field, incr);
    assertThat(response).isEqualTo(incr + 1, offset(.00001));
  }

  @Test
  public void testHincrByFloat_likeRedisDoesIt() {
    String key = "key";
    String field = "field";

    Object response = jedis.sendCommand(key, Protocol.Command.HINCRBYFLOAT, key, field, "1.23");
    assertThat(new String((byte[]) response)).isEqualTo("1.23");

    response = jedis.sendCommand(key, Protocol.Command.HINCRBYFLOAT, key, field, "0.77");
    assertThat(new String((byte[]) response)).isEqualTo("2");

    response = jedis.sendCommand(key, Protocol.Command.HINCRBYFLOAT, key, field, "-0.1");
    assertThat(new String((byte[]) response)).isEqualTo("1.9");
  }

  @Test
  public void testHincrByFloat_whenFieldDoesNotExist() {
    double incr = 0.37373;
    String key = "key";
    String field = "field";
    jedis.hset(key, "other-field", "a");

    double response = jedis.hincrByFloat(key, field, incr);
    assertThat(response).isEqualTo(incr);
  }

  @Test
  public void testHincrByFloat_whenKeyDoesNotExist() {
    double incr = 0.37373;

    double response = jedis.hincrByFloat("new", "newField", incr);
    assertThat(response).isEqualTo(incr);
  }

  @Test
  public void testIncrByFloat_withReallyBigNumbers() {
    // max unsigned long long - 1
    BigDecimal biggy = new BigDecimal("18446744073709551614");
    jedis.hset("key", "number", biggy.toPlainString());

    // Beyond this, native redis produces inconsistent results.
    Object rawResult =
        jedis.sendCommand("key", Protocol.Command.HINCRBYFLOAT, "key", "number", "1");
    BigDecimal result = new BigDecimal(new String((byte[]) rawResult));

    assertThat(result.toPlainString()).isEqualTo(biggy.add(BigDecimal.ONE).toPlainString());
  }

  @Test
  public void hincrByFloatFails_whenFieldIsNotANumber() {
    String key = "key";
    String field = "field";
    jedis.hset(key, field, "foobar");
    assertThatThrownBy(() -> jedis.hincrByFloat(key, field, 1.5))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("ERR hash value is not a float");
  }

  @Test
  public void testConcurrentHincrByFloat_sameKeyPerClient() {
    String key = "HSET_KEY";
    String field = "HSET_FIELD";

    jedis.hset(key, field, "0");

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> jedis.hincrByFloat(key, field, 0.5),
        (i) -> jedis2.hincrByFloat(key, field, 1.0)).run();

    String value = jedis.hget(key, field);
    assertThat(Float.valueOf(value)).isEqualTo(ITERATION_COUNT * 1.5f);
  }

}
