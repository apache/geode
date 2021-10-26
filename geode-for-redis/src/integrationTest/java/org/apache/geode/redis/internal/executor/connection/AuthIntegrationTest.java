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
import static org.apache.geode.redis.internal.RedisProperties.REDIS_REGION_NAME_PROPERTY;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.RedisConstants;

public class AuthIntegrationTest extends AbstractAuthIntegrationTest {

  private GeodeRedisServer server;
  private GemFireCache cache;
  private int port;
  private boolean needsWritePermission;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @After
  public void tearDown() {
    server.shutdown();
    cache.close();
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public String getUsername() {
    if (needsWritePermission) {
      return "dataWrite";
    }
    return "dataRead";
  }

  @Override
  public String getPassword() {
    return getUsername();
  }

  @Override
  protected void setupCacheWithSecurityAndRegionName(String regionName) throws Exception {
    setupCacheWithRegionName(getUsername(), regionName, true);
  }

  public void setupCacheWithSecurity(boolean needsWritePermission) throws Exception {
    this.needsWritePermission = needsWritePermission;
    setupCache(getUsername(), true);
  }

  public void setupCacheWithoutSecurity() throws Exception {
    setupCache(null, false);
  }

  private void setupCache(String username, boolean withSecurityManager) throws Exception {
    /**
     * See {@link #givenSecurity_separateClientRequest_doNotInteract} for some reasoning behind
     * setting this value.
     */
    System.setProperty("io.netty.eventLoopThreads", "1");
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    if (username != null) {
      cf.set(ConfigurationProperties.REDIS_USERNAME, username);
    }
    if (withSecurityManager) {
      cf.set(ConfigurationProperties.SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    }
    cache = cf.create();
    server = new GeodeRedisServer("localhost", port, (InternalCache) cache);
    server.getRegionProvider().getSlotAdvisor().getBucketSlots();
    this.jedis = new Jedis("localhost", port, 100000);
  }

  private void setupCacheWithRegionName(String username, String regionName,
      boolean withSecurityManager) throws Exception {
    System.setProperty(REDIS_REGION_NAME_PROPERTY, regionName);
    setupCache(username, withSecurityManager);
  }

  @Test
  public void testAuthConfig() throws Exception {
    setupCacheWithSecurity(false);
    InternalDistributedSystem iD = (InternalDistributedSystem) cache.getDistributedSystem();
    assertThat(iD.getConfig().getRedisUsername()).isEqualTo(getUsername());
  }

  @Test
  public void givenNoSecurity_accessWithAuthAndOnlyPassword_fails() throws Exception {
    setupCacheWithoutSecurity();

    assertThatThrownBy(() -> jedis.auth("password"))
        .hasMessageContaining(RedisConstants.ERROR_AUTH_CALLED_WITHOUT_SECURITY_CONFIGURED);
  }

  @Test
  public void givenNoSecurity_accessWithAuthAndUsernamePassword_fails() throws Exception {
    setupCacheWithoutSecurity();

    assertThatThrownBy(() -> jedis.auth("username", "password"))
        .hasMessageContaining(RedisConstants.ERROR_AUTH_CALLED_WITHOUT_SECURITY_CONFIGURED);
  }

  @Test
  public void givenSecurity_accessWithCorrectAuthorization_passes() throws Exception {
    setupCacheWithSecurity(false);

    jedis.auth("dataWrite", "dataWrite");

    assertThat(jedis.set("foo", "bar")).isEqualTo("OK");
  }

  @Test
  public void givenSecurity_readOpWithReadAuthorization_passes() throws Exception {
    setupCacheWithSecurity(false);

    jedis.auth("dataRead", "dataRead");

    assertThat(jedis.get("foo")).isNull();
  }

  @Test
  public void givenSecurity_readOpWithWriteAuthorization_fails() throws Exception {
    setupCacheWithSecurity(false);

    jedis.auth("dataWrite", "dataWrite");

    assertThatThrownBy(() -> jedis.get("foo"))
        .hasMessageContaining(RedisConstants.ERROR_NOT_AUTHORIZED);
  }

  @Test
  public void givenSecurity_writeOpWithReadAuthorization_fails() throws Exception {
    setupCacheWithSecurity(false);

    jedis.auth("dataRead", "dataRead");

    assertThatThrownBy(() -> jedis.set("foo", "bar"))
        .hasMessageContaining(RedisConstants.ERROR_NOT_AUTHORIZED);
  }

  @Test
  public void givenSecurity_multipleClientsConnectIndependently() throws Exception {
    setupCacheWithSecurity(false);

    jedis.auth("dataWrite", "dataWrite");
    assertThat(jedis.set("foo", "bar")).isEqualTo("OK");

    try (Jedis jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT)) {
      assertThatThrownBy(() -> jedis2.set("foo", "bar"))
          .hasMessageContaining(RedisConstants.ERROR_NOT_AUTHENTICATED);
    }
  }

  @Test
  public void givenSecurity_accessWithIncorrectAuthorization_fails() throws Exception {
    setupCacheWithSecurity(false);

    // Authentication is successful
    jedis.auth("dataWriteOther", "dataWriteOther");

    // Permissions are incorrect
    assertThatThrownBy(() -> jedis.set("foo", "bar"))
        .hasMessageContaining(RedisConstants.ERROR_NOT_AUTHORIZED);
  }

  @Test
  public void givenSecurityWithReadPermission_clusterCommandSucceeds() throws Exception {
    setupCacheWithSecurity(false);

    assertThat(jedis.auth(getUsername(), getPassword())).isEqualTo("OK");
    assertThat(jedis.clusterNodes()).isNotEmpty();
  }

  @Test
  public void givenSecurityWithWritePermission_setCommandSucceeds() throws Exception {
    setupCacheWithSecurity(true);

    assertThat(jedis.auth(getUsername(), getPassword())).isEqualTo("OK");
    assertThat(jedis.set("foo", "bar")).isEqualTo("OK");
  }

  @Test
  public void givenSecurityWithWritePermission_getCommandFails() throws Exception {
    setupCacheWithSecurity(true);

    assertThat(jedis.auth(getUsername(), getPassword())).isEqualTo("OK");
    assertThatThrownBy(() -> jedis.get("foo"))
        .hasMessageContaining(RedisConstants.ERROR_NOT_AUTHORIZED);
  }
}
