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

package org.apache.geode.redis.general;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class ExpireIntegrationTest {

  private static Jedis jedis;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static Random rand;
  private static int port = 6379;

  @BeforeClass
  public static void setUp() {
    rand = new Random();
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    cache.close();
    server.shutdown();
  }

  @Test
  public void Should_SetExipration_givenKeyTo_StringValue() {

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
  public void should_setExpiration_givenKeyTo_SortedSetValue() {

    String key = "key";
    double score = 2.0;
    String member = "member";

    jedis.zadd(key, score, member);
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
  public void should_setExpiration_givenKeyTo_GeoValue() {

    String key = "sicily";
    double latitude = 13.361389;
    double longitude = 38.115556;
    String member = "Palermo Catina";

    jedis.geoadd(key, latitude, longitude, member);

    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isEqualTo(-1);

    jedis.expire(key, 20);
    timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void should_setExpiration_givenKeyTo_HyperLogLogValue() {

    String key = "crawled:127.0.0.2";
    String value = "www.insideTheHouse.com";

    jedis.pfadd(key, value);

    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isEqualTo(-1);

    jedis.expire(key, 20);
    timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void should_setExpiration_givenKeyTo_ListValue() {

    String key = "list";
    String value = "value";

    jedis.lpush(key, value);

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
  public void should_removeKey_AfterExpirationPeriod() {
    String key = "key";
    String value = "value";
    jedis.set(key, value);

    jedis.expire(key, 1);
    GeodeAwaitility.await().until(() -> jedis.get(key) == null);
  }

  @Test
  @Ignore("this test needs to pass to have feature parity with native redis")
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
  @Ignore("this test needs to pass to have feature parity with native redis")
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

    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
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
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
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
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
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
  public void usingLPUSHCommandToAddNewValueToAKey_should_NOT_ClearExpirationTime() {
    String key = "list";
    String value = "value";
    String value2 = "value2";

    jedis.lpush(key, value);
    jedis.expire(key, 20);

    jedis.lpush(key, value2);
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
  @Ignore("this test needs to pass to have feature parity with native redis")
  public void callingRENAMEonExistingKey_shouldTransferExpirationTimeToNewKeyName_GivenNewName_Not_InUse() {
    String key = "key";
    String newKeyName = "new key name";
    String value = "value";
    jedis.set(key, value);
    jedis.expire(key, 20);

    jedis.rename(key, newKeyName);

    Long timeToLive = jedis.ttl(newKeyName);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  @Ignore("this test needs to pass to have feature parity with native redis")
  public void callingRENAMEonExistingKey_shouldTransferExpirationTimeToNewKeyName_GivenNewName_is_InUse_ButNo_ExpirationSet() {
    String key = "key";
    String key2 = "key2";
    String value = "value";

    jedis.set(key, value);
    jedis.expire(key, 20);

    jedis.set(key2, value);

    jedis.rename(key, key2);

    Long timeToLive = jedis.ttl(key2);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  @Ignore("this test needs to pass to have feature parity with native redis")
  public void callingRENAMEonExistingKey_shouldTransferExpirationTimeToNewKeyName_GivenNewName_is_InUse_AndHas_ExpirationSet() {
    String key = "key";
    String key2 = "key2";
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
  public void PERSISTCommand_ShouldClearExpirationTimeForGivenKey() {
    String key = "key";
    String value = "value";
    jedis.set(key, value);
    jedis.expire(key, 20);

    jedis.persist(key);

    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  @Ignore("this test needs to pass to have feature parity with native redis")
  public void SettingExiprationToNegativeValue_ShouldDeleteKey() {

    String key = "key";
    String value = "value";
    jedis.set(key, value);

    Long expirationWasSet = jedis.expire(key, -5);
    assertThat(expirationWasSet).isEqualTo(1);

    Boolean keyExists = jedis.exists(key);
    assertThat(keyExists).isTrue();
  }


  @Test
  public void CallingExpireOnAKeyThatAlreadyHasAnExiprationTime_ShouldUpdateTheExpirationTime() {
    String key = "key";
    String value = "value";
    jedis.set(key, value);

    jedis.expire(key, 20);
    jedis.expire(key, 20000);

    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isGreaterThan(21);
  }
}
