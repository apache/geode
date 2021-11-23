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
package org.apache.geode.redis.internal.commands.executor.string;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_OVERFLOW;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractDecrByIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private final String hashTag = "{111}";
  private final String someKey = "someKey" + hashTag;

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
  public void shouldDecrementByGivenAmount_givenValidInputsAndKey() {
    String key1 = "key1";
    jedis.set(key1, "100");
    jedis.decrBy(key1, 10);
    String result = jedis.get(key1);
    assertThat(Integer.parseInt(result)).isEqualTo(90);

    jedis.set(key1, "100");
    jedis.decrBy(key1, -10);
    result = jedis.get(key1);
    assertThat(Integer.parseInt(result)).isEqualTo(110);

    String key2 = "key2";
    jedis.set(key2, "-100");
    jedis.decrBy(key2, 10);
    result = jedis.get(key2);
    assertThat(Integer.parseInt(result)).isEqualTo(-110);

    jedis.set(key2, "-100");
    jedis.decrBy(key2, -10);
    result = jedis.get(key2);
    assertThat(Integer.parseInt(result)).isEqualTo(-90);
  }

  @Test
  public void should_returnNewValue_givenSuccessfulDecrby() {
    jedis.set("key", "100");
    Long decrByresult = jedis.decrBy("key", 50);

    String getResult = jedis.get("key");

    assertThat(decrByresult).isEqualTo(Long.valueOf(getResult)).isEqualTo(50l);
  }


  @Test
  public void should_setKeyToZeroAndThenDecrement_givenKeyThatDoesNotExist() {

    Long returnValue = jedis.decrBy("noneSuch", 10);
    assertThat(returnValue).isEqualTo(-10);

  }

  @Test
  public void shouldThrowError_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.DECRBY, 2);
  }

  @Test
  public void shouldThrowArithmeticException_givenDecrbyMoreThanMaxLong() {
    jedis.set(someKey, "1");

    BigInteger maxLongValue = new BigInteger(String.valueOf(Long.MAX_VALUE));
    BigInteger biggerThanMaxLongValue = maxLongValue.add(new BigInteger("1"));

    assertThatThrownBy(
        () -> jedis.sendCommand(hashTag, Protocol.Command.DECRBY,
            someKey, String.valueOf(biggerThanMaxLongValue)))
                .hasMessageContaining(ERROR_NOT_INTEGER);

    jedis.set("key", String.valueOf((Long.MIN_VALUE)));
  }

  @Test
  public void shouldReturnArithmeticError_givenDecrbyLessThanMinLong() {

    jedis.set(someKey, "1");

    BigInteger minLongValue = new BigInteger(String.valueOf(Long.MIN_VALUE));
    BigInteger smallerThanMinLongValue = minLongValue.subtract(new BigInteger("1"));

    assertThatThrownBy(
        () -> jedis.sendCommand(hashTag,
            Protocol.Command.DECRBY, someKey,
            smallerThanMinLongValue.toString()))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldReturnOverflowError_givenDecrbyThatWouldResultInValueLessThanMinLong() {

    BigInteger minLongValue = new BigInteger(String.valueOf(Long.MIN_VALUE));
    jedis.set(someKey, String.valueOf(minLongValue));

    assertThatThrownBy(
        () -> jedis.sendCommand(hashTag,
            Protocol.Command.DECRBY, someKey,
            "1"))
                .hasMessageContaining(ERROR_OVERFLOW);
  }


  @Test
  public void should_returnWrongTypeError_givenKeyContainsNonStringValue() {

    jedis.hset("setKey", "1", "1");
    assertThatThrownBy(
        () -> jedis.decrBy("setKey", 1)).hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void should_returnNotAnIntegerError_givenKeyContainsNonNumericStringValue() {

    jedis.set("key", "walrus");

    assertThatThrownBy(
        () -> jedis.decrBy("key", 1)).hasMessageContaining(ERROR_NOT_INTEGER);
  }
}
