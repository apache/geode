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

import java.math.BigDecimal;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractIncrByFloatIntegrationTest implements RedisPortSupplier {

  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), JEDIS_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void givenKeyNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCRBYFLOAT))
        .hasMessageContaining("ERR wrong number of arguments for 'incrbyfloat' command");
  }

  @Test
  public void givenIncrementNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCRBYFLOAT, "key"))
        .hasMessageContaining("ERR wrong number of arguments for 'incrbyfloat' command");
  }

  @Test
  public void givenMoreThanThreeArgumentsProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.INCRBYFLOAT, "key", "5", "extraArg"))
            .hasMessageContaining("ERR wrong number of arguments for 'incrbyfloat' command");
  }

  @Test
  public void testIncrByFloat() {
    String key1 = "key1";
    String key2 = "key2";
    double incr1 = 23.5;
    double incr2 = -14.78;
    double num1 = 100;
    double num2 = -100;
    jedis.set(key1, "" + num1);
    jedis.set(key2, "" + num2);

    jedis.incrByFloat(key1, incr1);
    jedis.incrByFloat(key2, incr2);

    assertThat(Double.valueOf(jedis.get(key1))).isEqualTo(num1 + incr1);
    assertThat(Double.valueOf(jedis.get(key2))).isEqualTo(num2 + incr2);
  }

  @Test
  public void testIncrByFloat_whenUsingExponents() {
    String key1 = "key1";
    double num1 = 5e2;
    jedis.set(key1, "5e2");

    double incr1 = 2.0e4;
    jedis.sendCommand(Protocol.Command.INCRBYFLOAT, key1, "2.0e4");
    assertThat(Double.valueOf(jedis.get(key1))).isEqualTo(num1 + incr1);
  }

  @Test
  public void testCorrectErrorIsReturned_whenKeyIsNotANumber() {
    jedis.set("nan", "abc");

    assertThatThrownBy(() -> jedis.incrByFloat("nan", 1))
        .hasMessage("ERR value is not a valid float");
  }

  @Test
  public void testCorrectErrorIsReturned_whenIncrByIsInvalid() {
    double number1 = 1.4;
    jedis.set("number", "" + number1);

    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCRBYFLOAT, "number", " a b c"))
        .hasMessage("ERR value is not a valid float");
  }

  @Test
  public void testIncrByFloat_withInfinity() {
    double number1 = 1.4;
    jedis.set("number", "" + number1);

    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCRBYFLOAT, "number", "+inf"))
        .hasMessage("ERR increment would produce NaN or Infinity");
  }

  @Test
  public void testIncrByFloat_whenIncrWillOverflowProducesCorrectError() {
    BigDecimal longDouble = new BigDecimal("1.1E+4932");
    jedis.set("number", longDouble.toPlainString());

    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.INCRBYFLOAT, "number", longDouble.toString()))
            .hasMessage("ERR increment would produce NaN or Infinity");
  }

  @Test
  @Ignore("GEODE-8624: Improve INCRBYFLOAT accuracy for very large values")
  public void testIncrByFloat_withReallyBigNumbers() {
    // max unsigned long long - 1
    BigDecimal biggy = new BigDecimal("18446744073709551614");
    jedis.set("number", biggy.toPlainString());

    // Beyond this, native redis produces inconsistent results.
    Object rawResult = jedis.sendCommand(Protocol.Command.INCRBYFLOAT, "number", "1");
    BigDecimal result = new BigDecimal(new String((byte[]) rawResult));

    assertThat(result.toPlainString()).isEqualTo(biggy.add(BigDecimal.ONE).toPlainString());
  }
}
