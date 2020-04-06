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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class ExpireAtIntegrationTest {

  public static Jedis jedis;
  public static int REDIS_CLIENT_TIMEOUT = 10000000;
  private static GeodeRedisServer server;
  private long unixTimeStampInTheFutureInSeconds;
  private long unixTimeStampFromThePast = 0L;
  String key = "key";
  String value = "value";

  @BeforeClass
  public static void setUp() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    server = new GeodeRedisServer("localhost", port);
    server.start();
    jedis = new Jedis("localhost", port, REDIS_CLIENT_TIMEOUT);
  }

  @Before
  public void testSetUp() {
    unixTimeStampInTheFutureInSeconds = (System.currentTimeMillis() / 1000) + 60;
  }


  @After
  public void testLevelTearDown() {
    jedis.flushAll();
  }

  @AfterClass
  public static void classLevelTearDown() {
    jedis.close();
    server.shutdown();
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
