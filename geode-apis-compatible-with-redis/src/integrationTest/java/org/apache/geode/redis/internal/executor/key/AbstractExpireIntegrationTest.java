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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractExpireIntegrationTest implements RedisIntegrationTest {

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
    assertExactNumberOfArgs(jedis, Protocol.Command.EXPIRE, 2);
  }

  @Test
  public void givenInvalidTimestamp_returnsNotIntegerError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", Protocol.Command.EXPIRE, "key", "notInteger"))
        .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void Should_SetExpiration_givenKeyTo_StringValue() {

    String key = "key";
    String value = "value";
    jedis.set(key, value);

    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);

    jedis.expire(key, 20);

    timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void should_setExpiration_givenKeyTo_SetValue() {

    String key = "key";
    String value = "value";

    jedis.sadd(key, value);
    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);

    jedis.expire(key, 20);
    timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void should_setExpiration_givenKeyTo_HashValue() {

    String key = "key";
    String field = "field";
    String value = "value";

    jedis.hset(key, field, value);
    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);

    jedis.expire(key, 20);
    timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void should_setExpiration_givenKeyTo_BitMapValue() {

    String key = "key";
    Long offset = 1L;
    String value = "0";

    jedis.setbit(key, offset, value);

    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isEqualTo(-1);

    jedis.expire(key, 20);
    timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void settingAnExistingKeyToANewValue_ShouldClearExpirationTime() {

    String key = "key";
    String value = "value";
    String anotherValue = "anotherValue";
    jedis.set(key, value);

    jedis.expire(key, 20);

    jedis.set(key, anotherValue);

    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void callingGETSETonExistingKey_ShouldClearExpirationTime() {
    String key = "key";
    String value = "value";
    String anotherValue = "anotherValue";

    jedis.set(key, value);
    jedis.expire(key, 20);

    jedis.getSet(key, anotherValue);
    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void deletingAnExistingKeyAndRecreatingTheSameKey_ShouldClearExistingExpirationTime() {

    String key = "key";
    String value = "value";

    jedis.set(key, value);
    jedis.expire(key, 20);

    jedis.del(key);
    jedis.set(key, value);
    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void callingSDIFFSTOREonExistingKey_ShouldClearExpirationTime() {

    String key1 = "{user1}key1";
    String key2 = "{user1}key2";
    String key3 = "{user1}key3";
    String value1 = "value1";
    String value2 = "value2";
    String value3 = "value3";

    jedis.sadd(key1, value1);
    jedis.sadd(key1, value2);

    jedis.sadd(key2, value2);

    jedis.sadd(key3, value3);
    jedis.expire(key3, 20);

    jedis.sdiffstore(key3, key1, key2);

    Long timeToLive = jedis.ttl(key3);
    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void callingSINTERSTOREonExistingKey_ShouldClearExpirationTime() {
    String key1 = "{user1}key1";
    String key2 = "{user1}key2";
    String key3 = "{user1}key3";
    String value1 = "value1";
    String value2 = "value2";
    String value3 = "value3";

    jedis.sadd(key1, value1);
    jedis.sadd(key1, value2);

    jedis.sadd(key2, value2);

    jedis.sadd(key3, value3);
    jedis.expire(key3, 20);

    jedis.sinterstore(key3, key1, key2);

    Long timeToLive = jedis.ttl(key3);
    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void callingSUNIONSTOREonExistingKey_ShouldClearExpirationTime() {
    String key1 = "{user1}key1";
    String key2 = "{user1}key2";
    String key3 = "{user1}key3";
    String value1 = "value1";
    String value2 = "value2";
    String value3 = "value3";

    jedis.sadd(key1, value1);
    jedis.sadd(key1, value2);

    jedis.sadd(key2, value2);

    jedis.sadd(key3, value3);
    jedis.expire(key3, 20);

    jedis.sinterstore(key3, key1, key2);

    Long timeToLive = jedis.ttl(key3);
    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void callingINCRonExistingKey_should_NOT_ClearExpirationTime() {
    String key = "key";
    String value = "0";

    jedis.set(key, value);
    jedis.expire(key, 20);

    jedis.incr(key);

    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void usingHSETCommandToAlterAFieldValue_should_NOT_ClearExpirationTimeOnKey() {
    String key = "key";
    String field = "field";
    String value = "value";
    String value2 = "value2";

    jedis.hset(key, field, value);

    jedis.expire(key, 20);

    jedis.hset(key, field, value2);

    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void callingRENAMEonExistingKey_shouldTransferExpirationTimeToNewKeyName_GivenNewName_Not_InUse() {
    String key = "{user1}key";
    String newKeyName = "{user1}new key name";
    String value = "value";
    jedis.set(key, value);
    jedis.expire(key, 20);

    jedis.rename(key, newKeyName);

    Long timeToLive = jedis.ttl(newKeyName);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void callingRENAMEonExistingKey_shouldTransferExpirationTimeToNewKeyName_GivenNewName_is_InUse_ButNo_ExpirationSet() {
    String key = "{user1}key";
    String key2 = "{user1}key2";
    String value = "value";

    jedis.set(key, value);
    jedis.expire(key, 20);

    jedis.set(key2, value);

    jedis.rename(key, key2);

    Long timeToLive = jedis.ttl(key2);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void callingRENAMEonExistingKey_shouldTransferExpirationTimeToNewKeyName_GivenNewName_is_InUse_AndHas_ExpirationSet() {
    String key = "{user1}key";
    String key2 = "{user1}key2";
    String value = "value";

    jedis.set(key, value);
    jedis.expire(key, 20);

    jedis.set(key2, value);
    jedis.expire(key2, 14);

    jedis.rename(key, key2);

    Long timeToLive = jedis.ttl(key2);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void SettingExpirationToNegativeValue_ShouldDeleteKey() {

    String key = "key";
    String value = "value";
    jedis.set(key, value);

    Long expirationWasSet = jedis.expire(key, -5);
    assertThat(expirationWasSet).isEqualTo(1);

    Boolean keyExists = jedis.exists(key);
    assertThat(keyExists).isFalse();
  }


  @Test
  public void CallingExpireOnAKeyThatAlreadyHasAnExpirationTime_ShouldUpdateTheExpirationTime() {
    String key = "key";
    String value = "value";
    jedis.set(key, value);

    jedis.expire(key, 20);
    jedis.expire(key, 20000);

    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isGreaterThan(21);
  }
}
