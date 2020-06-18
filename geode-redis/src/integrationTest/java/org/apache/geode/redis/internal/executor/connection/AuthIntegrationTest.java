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
package org.apache.geode.redis.internal.executor.connection;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class AuthIntegrationTest {

  private static final String PASSWORD = "pwd";
  Jedis jedis;
  GeodeRedisServer server;
  GemFireCache cache;
  Random rand;
  int port;

  @Before
  public void setUp() {
    rand = new Random();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    this.jedis = new Jedis("localhost", port, 100000);
  }

  @After
  public void tearDown() {
    server.shutdown();
    cache.close();
  }

  private void setupCacheWithPassword() {
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cf.set(ConfigurationProperties.REDIS_PASSWORD, PASSWORD);
    cache = cf.create();
    server = new GeodeRedisServer("localhost", port);
    server.start();
  }

  @Test
  public void testAuthIncorrectNumberOfArguments() {
    setupCacheWithPassword();
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.AUTH))
        .hasMessageContaining("wrong number of arguments");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.AUTH, "fhqwhgads", "extraArg"))
        .hasMessageContaining("wrong number of arguments");
  }

  @Test
  public void testAuthConfig() {
    setupCacheWithPassword();
    InternalDistributedSystem iD = (InternalDistributedSystem) cache.getDistributedSystem();
    assert (iD.getConfig().getRedisPassword().equals(PASSWORD));
  }

  @Test
  public void testAuthRejectAccept() {
    setupCacheWithPassword();

    assertThatThrownBy(() -> jedis.auth("wrongpwd"))
        .hasMessageContaining("Attemping to authenticate with an invalid password");

    assertThat(jedis.auth(PASSWORD)).isEqualTo("OK");
  }

  @Test
  public void testAuthNoPwd() {
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    server = new GeodeRedisServer("localhost", port);
    server.start();

    assertThatThrownBy(() -> jedis.auth(PASSWORD))
        .hasMessageContaining("Attempting to authenticate when no password has been set");
  }

  @Test
  public void testAuthAcceptRequests() {
    setupCacheWithPassword();

    assertThatThrownBy(() -> jedis.set("foo", "bar"))
        .hasMessageContaining("Must authenticate before sending any requests");

    String res = jedis.auth(PASSWORD);
    assertThat(res).isEqualTo("OK");

    jedis.set("foo", "bar"); // No exception
  }

  @Test
  public void testSeparateClientRequests() {
    setupCacheWithPassword();
    Jedis nonAuthorizedJedis = new Jedis("localhost", port, 100000);
    Jedis authorizedJedis = new Jedis("localhost", port, 100000);

    assertThat(authorizedJedis.auth(PASSWORD)).isEqualTo("OK");
    authorizedJedis.set("foo", "bar"); // No exception for authorized client
    authorizedJedis.auth(PASSWORD);

    assertThatThrownBy(() -> nonAuthorizedJedis.set("foo", "bar"))
        .hasMessageContaining("Must authenticate before sending any requests");

    authorizedJedis.close();
    nonAuthorizedJedis.close();
  }
}
