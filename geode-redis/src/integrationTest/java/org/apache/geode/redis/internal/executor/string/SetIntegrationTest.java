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
import static redis.clients.jedis.Protocol.Command.SET;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.redis.internal.RedisConstants;

public class SetIntegrationTest {

  static Jedis jedis;
  static Jedis jedis2;
  private static int ITERATION_COUNT = 4000;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), 10000000);
    jedis2 = new Jedis("localhost", server.getPort(), 10000000);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenEmptyKey() {

    String key = "key";
    String value = "value";

    String result = jedis.get(key);
    assertThat(result).isNull();

    jedis.set(key, value);
    result = jedis.get(key);
    assertThat(result).isEqualTo(value);
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenKeyIsOfDataTypeSet() {
    String key = "key";
    String stringValue = "value";

    jedis.sadd(key, "member1", "member2");

    jedis.set(key, stringValue);
    String result = jedis.get(key);

    assertThat(result).isEqualTo(stringValue);
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenKeyIsOfDataTypeHash() {
    String key = "key";
    String stringValue = "value";

    jedis.hset(key, "field", "something else");

    String result = jedis.set(key, stringValue);
    assertThat(result).isEqualTo("OK");

    assertThat(stringValue).isEqualTo(jedis.get(key));
  }

  @Test
  public void testSET_shouldSetNX_evenIfKeyContainsOtherDataType() {
    String key = "key";
    String stringValue = "value";

    jedis.sadd(key, "member1", "member2");
    SetParams setParams = new SetParams();
    setParams.nx();

    String result = jedis.set(key, stringValue, setParams);
    assertThat(result).isNull();
  }

  @Test
  public void testSET_shouldSetXX_evenIfKeyContainsOtherDataType() {
    String key = "key";
    String stringValue = "value";

    jedis.sadd(key, "member1", "member2");
    SetParams setParams = new SetParams();
    setParams.xx();

    jedis.set(key, stringValue, setParams);
    String result = jedis.get(key);

    assertThat(result).isEqualTo(stringValue);
  }


  @Test
  public void testSET_withNXAndExArguments() {
    String key = "key";
    String stringValue = "value";

    SetParams setParams = new SetParams();
    setParams.nx();
    setParams.ex(20);

    jedis.set(key, stringValue, setParams);
    assertThat(jedis.ttl(key)).isGreaterThan(15);
    assertThat(jedis.get(key)).isEqualTo(stringValue);
  }

  @Test
  public void testSET_withXXAndExArguments() {
    String key = "key";
    String stringValue = "value";

    jedis.set(key, "differentValue");

    SetParams setParams = new SetParams();
    setParams.xx();
    setParams.ex(20);

    jedis.set(key, stringValue, setParams);
    assertThat(jedis.ttl(key)).isGreaterThan(15);
    assertThat(jedis.get(key)).isEqualTo(stringValue);
  }

  @Test
  public void testSET_withNXAndPxArguments() {
    String key = "key";
    String stringValue = "value";

    SetParams setParams = new SetParams();
    setParams.nx();
    setParams.px(2000);

    jedis.set(key, stringValue, setParams);
    assertThat(jedis.pttl(key)).isGreaterThan(1500);
    assertThat(jedis.get(key)).isEqualTo(stringValue);
  }

  @Test
  public void testSET_withXXAndPxArguments() {
    String key = "key";
    String stringValue = "value";

    jedis.set(key, "differentValue");

    SetParams setParams = new SetParams();
    setParams.xx();
    setParams.px(2000);

    jedis.set(key, stringValue, setParams);
    assertThat(jedis.pttl(key)).isGreaterThan(1500);
    assertThat(jedis.get(key)).isEqualTo(stringValue);
  }

  @Test
  public void setNX_shouldNotConflictWithRegularSet() {
    List<String> keys = new ArrayList<>();
    List<String> values = new ArrayList<>();
    for (int i = 0; i < ITERATION_COUNT; i++) {
      keys.add("key-" + i);
      values.add("value-" + i);
    }

    AtomicInteger counter = new AtomicInteger(0);
    SetParams setParams = new SetParams();
    setParams.nx();

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> {
          String ok = jedis.set(keys.get(i), values.get(i));
          if ("OK".equals(ok)) {
            counter.addAndGet(1);
          }
        },
        (i) -> jedis2.set(keys.get(i), values.get(i), setParams))
            .run();

    assertThat(counter.get()).isEqualTo(ITERATION_COUNT);
  }

  @Test
  public void testSET_withEXArgument_shouldSetExpireTime() {
    String key = "key";
    String value = "value";
    int secondsUntilExpiration = 20;

    SetParams setParams = new SetParams();
    setParams.ex(secondsUntilExpiration);

    jedis.set(key, value, setParams);

    Long result = jedis.ttl(key);

    assertThat(result).isGreaterThan(15l);
  }

  @Test
  public void testSET_withNegativeEXTime_shouldReturnError() {
    String key = "key";
    String value = "value";
    int millisecondsUntilExpiration = -1;

    SetParams setParams = new SetParams();
    setParams.ex(millisecondsUntilExpiration);

    assertThatThrownBy(() -> jedis.set(key, value, setParams))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_INVALID_EXPIRE_TIME);
  }

  @Test
  public void testSET_withPXArgument_shouldSetExpireTime() {
    String key = "key";
    String value = "value";
    int millisecondsUntilExpiration = 20000;

    SetParams setParams = new SetParams();
    setParams.px(millisecondsUntilExpiration);

    jedis.set(key, value, setParams);

    Long result = jedis.ttl(key);

    assertThat(result).isGreaterThan(15l);
  }

  @Test
  public void testSET_withNegativePXTime_shouldReturnError() {
    String key = "key";
    String value = "value";
    int millisecondsUntilExpiration = -1;

    SetParams setParams = new SetParams();
    setParams.px(millisecondsUntilExpiration);

    assertThatThrownBy(() -> jedis.set(key, value, setParams))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_INVALID_EXPIRE_TIME);
  }

  @Test
  public void testSET_shouldClearPreviousTTL() {
    String key = "key";
    String value = "value";
    int secondsUntilExpiration = 20;

    SetParams setParams = new SetParams();
    setParams.ex(secondsUntilExpiration);

    jedis.set(key, value, setParams);

    jedis.set(key, "other value");

    Long result = jedis.ttl(key);

    assertThat(result).isEqualTo(-1L);
  }

  @Test
  public void testSET_withXXArgument_shouldClearPreviousTTL() {
    String key = "xx_key";
    String value = "did exist";
    int secondsUntilExpiration = 20;
    SetParams setParamsXX = new SetParams();
    setParamsXX.xx();
    SetParams setParamsEX = new SetParams();
    setParamsEX.ex(secondsUntilExpiration);
    String result_EX = jedis.set(key, value, setParamsEX);
    assertThat(result_EX).isEqualTo("OK");
    assertThat(jedis.ttl(key)).isGreaterThan(15L);

    String result_XX = jedis.set(key, value, setParamsXX);

    assertThat(result_XX).isEqualTo("OK");
    Long result = jedis.ttl(key);
    assertThat(result).isEqualTo(-1L);
  }

  @Test
  public void testSET_shouldNotClearPreviousTTL_onFailure() {
    String key_NX = "nx_key";
    String value_NX = "set only if key did not exist";
    int secondsUntilExpiration = 20;

    SetParams setParamsEX = new SetParams();
    setParamsEX.ex(secondsUntilExpiration);

    SetParams setParamsNX = new SetParams();
    setParamsNX.nx();

    jedis.set(key_NX, value_NX, setParamsEX);
    String result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isNull();

    Long result = jedis.ttl(key_NX);
    assertThat(result).isGreaterThan(15L);
  }

  @Test
  @Ignore("KEEPTTL is part of redis 6")
  public void testSET_withKEEPTTL_shouldRetainPreviousTTL_onSuccess() {
    String key = "key";
    String value = "value";
    int secondsToExpire = 30;

    SetParams setParamsEx = new SetParams();
    setParamsEx.ex(secondsToExpire);

    jedis.set(key, value, setParamsEx);

    SetParams setParamsKeepTTL = new SetParams();
    // setParamsKeepTTL.keepTtl();
    // Jedis Doesn't support KEEPTTL yet.

    jedis.set(key, "newValue", setParamsKeepTTL);

    Long result = jedis.ttl(key);
    assertThat(result).isGreaterThan(15L);
  }

  @Test
  public void testSET_withNXArgument_shouldOnlySetKeyIfKeyDoesNotExist() {
    String key1 = "key_1";
    String key2 = "key_2";
    String value1 = "value_1";
    String value2 = "value_2";

    jedis.set(key1, value1);

    SetParams setParams = new SetParams();
    setParams.nx();

    jedis.set(key1, value2, setParams);
    String result1 = jedis.get(key1);

    assertThat(result1).isEqualTo(value1);

    jedis.set(key2, value2, setParams);
    String result2 = jedis.get(key2);

    assertThat(result2).isEqualTo(value2);
  }

  @Test
  public void testSET_withXXArgument_shouldOnlySetKeyIfKeyExists() {
    String key1 = "key_1";
    String key2 = "key_2";
    String value1 = "value_1";
    String value2 = "value_2";

    jedis.set(key1, value1);

    SetParams setParams = new SetParams();
    setParams.xx();

    jedis.set(key1, value2, setParams);
    String result1 = jedis.get(key1);

    assertThat(result1).isEqualTo(value2);

    jedis.set(key2, value2, setParams);
    String result2 = jedis.get(key2);

    assertThat(result2).isNull();
  }

  @Test
  public void testSET_XX_NX_arguments_shouldReturnOK_if_Successful() {
    String key_NX = "nx_key";
    String key_XX = "xx_key";
    String value_NX = "did not exist";
    String value_XX = "did exist";

    SetParams setParamsXX = new SetParams();
    setParamsXX.xx();

    SetParams setParamsNX = new SetParams();
    setParamsNX.nx();

    String result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isEqualTo("OK");

    jedis.set(key_XX, value_XX);
    String result_XX = jedis.set(key_NX, value_NX, setParamsXX);
    assertThat(result_XX).isEqualTo("OK");
  }

  @Test
  public void testSET_XX_NX_arguments_should_return_NULL_if_Not_Successful() {
    String key_NX = "nx_key";
    String key_XX = "xx_key";
    String value_NX = "set only if key did not exist";
    String value_XX = "set only if key did exist";

    SetParams setParamsXX = new SetParams();
    setParamsXX.xx();

    SetParams setParamsNX = new SetParams();
    setParamsNX.nx();

    jedis.set(key_NX, value_NX);
    String result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isNull();

    String result_XX = jedis.set(key_XX, value_XX, setParamsXX);
    assertThat(result_XX).isNull();
  }

  @Test
  public void testSET_withInvalidOptions() {
    SoftAssertions soft = new SoftAssertions();

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET))
        .as("invalid options #1")
        .isInstanceOf(JedisDataException.class);

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "EX", "0"))
        .as("invalid options #2")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "EX", "a"))
        .as("invalid options #3")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("value is not an integer");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "PX", "1", "EX", "0"))
        .as("invalid options #4")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "PX", "1", "XX", "0"))
        .as("invalid options #5")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "PX", "XX", "0"))
        .as("invalid options #6")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "1", "PX", "1"))
        .as("invalid options #7")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "NX", "XX"))
        .as("invalid options #8")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");


    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "key", "value", "blah"))
        .as("invalid options #9")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertAll();
  }

  @Test
  public void testSET_withBinaryKeyAndValue() {
    byte[] blob = new byte[256];
    for (int i = 0; i < 256; i++) {
      blob[i] = (byte) i;
    }

    jedis.set(blob, blob);
    byte[] result = jedis.get(blob);

    assertThat(result).isEqualTo(blob);
  }

}
