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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

@Category(IntegrationTest.class)
public class AuthJUnitTest extends RedisTestBase {

  private static final String PASSWORD = "pwd";

  private void setupCacheWithPassword() {
    if (cache != null) {
      cache.close();
    }
    Properties redisCacheProperties = getDefaultRedisCacheProperties();
    redisCacheProperties.setProperty(ConfigurationProperties.REDIS_PASSWORD, PASSWORD);
    cache = (GemFireCacheImpl) createCacheInstance(redisCacheProperties);
  }

  @Test
  public void testAuthConfig() {
    setupCacheWithPassword();
    InternalDistributedSystem distributedSystem = cache.getDistributedSystem();
    assert (distributedSystem.getConfig().getRedisPassword().equals(PASSWORD));
    cache.close();
  }

  @Test
  public void testAuthRejectAccept() {
    setupCacheWithPassword();
    try (Jedis jedis = defaultJedisInstance()) {
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
  }

  @Test
  public void testAuthNoPwd() {
    try (Jedis jedis = defaultJedisInstance()) {
      jedis.auth(PASSWORD);
      fail(
          "We expecting either a JedisConnectionException or JedisDataException to be thrown here");
    } catch (JedisConnectionException | JedisDataException exception) {
    }
  }

  @Test
  public void testAuthAcceptRequests() {
    setupCacheWithPassword();
    Exception ex = null;
    try (Jedis jedis = defaultJedisInstance()) {
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
  }

  @Test
  public void testSeparateClientRequests() {
    setupCacheWithPassword();
    try (Jedis authorizedJedis = new Jedis(hostName, Integer.parseInt(redisPort), 100000);
         Jedis nonAuthorizedJedis = new Jedis(hostName, Integer.parseInt(redisPort), 100000)) {
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
    }
  }
}
