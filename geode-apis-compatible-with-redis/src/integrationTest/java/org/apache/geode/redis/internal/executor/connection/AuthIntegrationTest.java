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
 *
 */

package org.apache.geode.redis.internal.executor.connection;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.redis.internal.GeodeRedisServer;

public class AuthIntegrationTest extends AbstractAuthIntegrationTest {

  private GeodeRedisServer server;
  private GemFireCache cache;
  private int port;

  @After
  public void tearDown() {
    server.shutdown();
    cache.close();
  }

  @Override
  public int getPort() {
    return port;
  }

  public void setupCacheWithSecurity(boolean withSecurityManager) throws Exception {
    setupCache(true, withSecurityManager);
  }

  public void setupCacheWithoutSecurity() throws Exception {
    setupCache(false, false);
  }

  private void setupCache(boolean withUsername, boolean withSecurityManager) throws Exception {
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    if (withUsername) {
      cf.set(ConfigurationProperties.REDIS_USERNAME, USERNAME);
    }
    if (withSecurityManager) {
      cf.set(ConfigurationProperties.SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    }
    cache = cf.create();
    server = new GeodeRedisServer("localhost", port, (InternalCache) cache);
    server.getRegionProvider().getSlotAdvisor().getBucketSlots();
    this.jedis = new Jedis("localhost", port, 100000);
  }

  @Test
  public void testAuthConfig() throws Exception {
    setupCacheWithSecurity(true);
    InternalDistributedSystem iD = (InternalDistributedSystem) cache.getDistributedSystem();
    assertThat(iD.getConfig().getRedisUsername()).isEqualTo(USERNAME);
  }

}
