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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractPexpireIntegrationTest implements RedisIntegrationTest {

  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void teardown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void errors_GivenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.PEXPIRE, 2);
  }

  @Test
  public void givenInvalidTimestamp_returnsNotIntegerError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.PEXPIRE, "key", "notInteger"))
        .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void should_SetExpiration_givenKeyTo_StringValueInMilliSeconds() {

    String key = "key";
    String value = "value";
    long millisecondsToLive = 20000L;

    jedis.set(key, value);
    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isEqualTo(-1);

    jedis.pexpire(key, millisecondsToLive);

    timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isLessThanOrEqualTo(20);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void should_removeKey_AfterExpirationPeriod() {
    String key = "key";
    String value = "value";
    jedis.set(key, value);

    jedis.pexpire(key, 10);

    GeodeAwaitility.await().until(() -> jedis.get(key) == null);
  }

  @Test
  public void should_removeSetKey_AfterExpirationPeriod() {
    String key = "key";
    String value = "value";
    jedis.sadd(key, value);

    jedis.pexpire(key, 10);
    GeodeAwaitility.await().until(() -> !jedis.exists(key));
  }

  @Test
  public void should_passivelyExpireKeys() {
    jedis.sadd("key", "value");
    jedis.pexpire("key", 100);

    GeodeAwaitility.await().until(() -> jedis.keys("key").isEmpty());
  }
}
