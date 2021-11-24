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

package org.apache.geode.redis.internal.commands.executor.key;

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

public abstract class AbstractExpireAtIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final String key = "key";
  private static final String value = "value";
  private long unixTimeStampInTheFutureInSeconds;
  private final long unixTimeStampFromThePast = 0L;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
    unixTimeStampInTheFutureInSeconds = (System.currentTimeMillis() / 1000) + 60;
  }

  @After
  public void testLevelTearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void errors_GivenWrongNumberOfParameters() {
    assertExactNumberOfArgs(jedis, Protocol.Command.EXPIREAT, 2);
  }

  @Test
  public void givenInvalidTimestamp_returnsNotIntegerError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.EXPIREAT, "key", "notInteger"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void should_return_1_given_validKey_andTimeStampInThePast() {
    jedis.set(key, value);

    Long result = jedis.expireAt(key, unixTimeStampFromThePast);

    assertThat(result).isEqualTo(1L);
  }

  @Test
  public void should_delete_key_given_aTimeStampInThePast() {
    jedis.set(key, value);

    jedis.expireAt(key, unixTimeStampFromThePast);

    assertThat(jedis.get(key)).isNull();
  }

  @Test
  public void should_return_0_given_nonExistentKey_andTimeStampInFuture() {
    String non_existent_key = "I don't exist";

    long result = jedis.expireAt(
        non_existent_key,
        unixTimeStampInTheFutureInSeconds);

    assertThat(result).isEqualTo(0);
  }

  @Test
  public void should_return_0_given_nonExistentKey_andTimeStampInPast() {
    String non_existent_key = "I don't exist";

    long result = jedis.expireAt(
        non_existent_key,
        unixTimeStampFromThePast);

    assertThat(result).isEqualTo(0);
  }

  @Test
  public void should_return_1_given_validKey_andValidTimeStampInFuture() {

    jedis.set(key, value);

    long result = jedis.expireAt(
        key,
        unixTimeStampInTheFutureInSeconds);

    assertThat(result).isEqualTo(1);
  }

  @Test
  public void should_expireKeyAtTimeSpecified() {
    long unixTimeStampInTheNearFuture = (System.currentTimeMillis() / 1000) + 5;
    jedis.set(key, value);
    jedis.expireAt(key, unixTimeStampInTheNearFuture);

    GeodeAwaitility.await().until(
        () -> jedis.get(key) == null);
  }

}
