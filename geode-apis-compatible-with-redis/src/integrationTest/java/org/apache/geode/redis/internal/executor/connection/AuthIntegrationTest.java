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

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.After;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.redis.internal.GeodeRedisServer;

public class AuthIntegrationTest {

  static final String PASSWORD = "pwd";
  Jedis jedis;
  GeodeRedisServer server;
  GemFireCache cache;
  int port;

  int getPort() {
    return port;
  }

  @After
  public void tearDown() {
    server.shutdown();
    cache.close();
  }

  public void setupCacheWithPassword() throws Exception {
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cf.set(ConfigurationProperties.REDIS_PASSWORD, PASSWORD);
    cache = cf.create();
    server = new GeodeRedisServer("localhost", port, (InternalCache) cache);
    server.getRegionProvider().getSlotAdvisor().getBucketSlots();
    this.jedis = new Jedis("localhost", port, 100000);
  }

  public void setupCacheWithoutPassword() {
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    server = new GeodeRedisServer("localhost", port, (InternalCache) cache);
    this.jedis = new Jedis("localhost", port, 100000);
  }

  @Test
  public void testAuthIncorrectNumberOfArguments() throws Exception {
    setupCacheWithPassword();
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.AUTH))
        .hasMessageContaining("wrong number of arguments");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.AUTH, "fhqwhgads", "extraArg"))
        .hasMessageContaining("wrong number of arguments");
  }

  @Test
  public void testAuthConfig() throws Exception {
    setupCacheWithPassword();
    InternalDistributedSystem iD = (InternalDistributedSystem) cache.getDistributedSystem();
    assertThat(iD.getConfig().getRedisPassword()).isEqualTo(PASSWORD);
  }

  @Test
  public void testAuthRejectAccept() throws Exception {
    setupCacheWithPassword();

    assertThatThrownBy(() -> jedis.auth("wrongpwd"))
        .hasMessageContaining("ERR invalid password");

    assertThat(jedis.auth(PASSWORD)).isEqualTo("OK");
  }

  @Test
  public void testAuthNoPwd() {
    setupCacheWithoutPassword();

    assertThatThrownBy(() -> jedis.auth(PASSWORD))
        .hasMessage("ERR Client sent AUTH, but no password is set");
  }

  @Test
  public void testAuthAcceptRequests() throws Exception {
    setupCacheWithPassword();

    assertThatThrownBy(() -> jedis.set("foo", "bar"))
        .hasMessage("NOAUTH Authentication required.");

    String res = jedis.auth(PASSWORD);
    assertThat(res).isEqualTo("OK");

    jedis.set("foo", "bar"); // No exception
  }

  @Test
  public void testSeparateClientRequests() throws Exception {
    setupCacheWithPassword();
    Jedis nonAuthorizedJedis = new Jedis("localhost", getPort(), 100000);
    Jedis authorizedJedis = new Jedis("localhost", getPort(), 100000);

    assertThat(authorizedJedis.auth(PASSWORD)).isEqualTo("OK");
    authorizedJedis.set("foo", "bar"); // No exception for authorized client
    authorizedJedis.auth(PASSWORD);

    assertThatThrownBy(() -> nonAuthorizedJedis.set("foo", "bar"))
        .hasMessage("NOAUTH Authentication required.");

    authorizedJedis.close();
    nonAuthorizedJedis.close();
  }

  @Test
  public void lettuceAuthClient_withLettuceVersion6() throws Exception {
    setupCacheWithPassword();

    RedisURI uri = RedisURI.create(String.format("redis://%s@localhost:%d", PASSWORD, getPort()));
    RedisClient client = RedisClient.create(uri);

    client.connect().sync().ping();
  }

  @Test
  public void lettuceAuthClient_withLettuceVersion6_andNoAuthentication() {
    setupCacheWithoutPassword();

    RedisURI uri = RedisURI.create(String.format("redis://%s@localhost:%d", PASSWORD, getPort()));
    RedisClient client = RedisClient.create(uri);

    assertThatThrownBy(() -> client.connect().sync().ping())
        .hasRootCauseMessage("ERR Client sent AUTH, but no password is set");
  }
}
