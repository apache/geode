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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.GeodeRedisServer;

public class PexpireIntegrationTest {

  public static Jedis jedis;
  public static int REDIS_CLIENT_TIMEOUT = 10000000;
  private static GeodeRedisServer server;

  @BeforeClass
  public static void setUp() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    server = new GeodeRedisServer("localhost", port);
    server.start();
    jedis = new Jedis("localhost", port, REDIS_CLIENT_TIMEOUT);
  }

  @AfterClass
  public static void classLevelTearDown() {
    jedis.close();
    server.shutdown();
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
}
