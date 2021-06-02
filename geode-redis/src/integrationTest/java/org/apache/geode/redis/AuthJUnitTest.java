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
package org.apache.geode.redis;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class AuthJUnitTest {

  private static final String PASSWORD = "pwd";
  Jedis jedis;
  GeodeRedisServer server;
  GemFireCache cache;
  Random rand;
  int port;

  int runs = 150;

  @Before
  public void setUp() throws IOException {
    rand = new Random();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    this.jedis = new Jedis("localhost", port, 100000);
  }

  @After
  public void tearDown() throws InterruptedException {
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
  public void testAuthConfig() {
    setupCacheWithPassword();
    InternalDistributedSystem iD = (InternalDistributedSystem) cache.getDistributedSystem();
    assert (iD.getConfig().getRedisPassword().equals(PASSWORD));
  }

  @Test
  public void testAuthRejectAccept() {
    setupCacheWithPassword();
    Exception ex = null;
    try {
      jedis.auth("wrongpwd");
    } catch (JedisDataException e) {
      ex = e;
    }
    assertNotNull(ex);

    String res = jedis.auth(PASSWORD);
    assertEquals(res, "OK");
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

    Exception ex = null;
    try {
      jedis.auth(PASSWORD);
    } catch (JedisDataException e) {
      ex = e;
    }
    assertNotNull(ex);
  }

  @Test
  public void testAuthAcceptRequests() {
    setupCacheWithPassword();
    Exception ex = null;
    try {
      jedis.set("foo", "bar");
    } catch (JedisDataException e) {
      ex = e;
    }
    assertNotNull(ex);

    String res = jedis.auth(PASSWORD);
    assertEquals(res, "OK");

    jedis.set("foo", "bar"); // No exception
  }

  @Test
  public void testSeparateClientRequests() {
    setupCacheWithPassword();
    Jedis authorizedJedis = null;
    Jedis nonAuthorizedJedis = null;
    try {
      authorizedJedis = new Jedis("localhost", port, 100000);
      nonAuthorizedJedis = new Jedis("localhost", port, 100000);
      String res = authorizedJedis.auth(PASSWORD);
      assertEquals(res, "OK");
      authorizedJedis.set("foo", "bar"); // No exception for authorized client

      authorizedJedis.auth(PASSWORD);
      Exception ex = null;
      try {
        nonAuthorizedJedis.set("foo", "bar");
      } catch (JedisDataException e) {
        ex = e;
      }
      assertNotNull(ex);
    } finally {
      if (authorizedJedis != null) {
        authorizedJedis.close();
      }
      if (nonAuthorizedJedis != null) {
        nonAuthorizedJedis.close();
      }
    }
  }

}
