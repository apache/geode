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

package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.Objects;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.security.ExpirableSecurityManager;
import org.apache.geode.security.UpdatableUserAuthInitialize;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({WanTest.class})
public class AuthenticationExpiredWANDunitTest {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public RestoreSystemProperties restore = new RestoreSystemProperties();

  private MemberVM locator;
  private MemberVM server;
  private MemberVM remoteServer;

  @Before
  public void init() {

    locator = clusterStartupRule.startLocatorVM(0,
        l -> l.withSecurityManager(ExpirableSecurityManager.class)
            .withProperty(DISTRIBUTED_SYSTEM_ID, "1"));

    int locatorPort = locator.getPort();
    MemberVM remoteLocator = clusterStartupRule.startLocatorVM(1,
        l -> l.withSecurityManager(ExpirableSecurityManager.class)
            .withProperty(REMOTE_LOCATORS, "localhost[" + locatorPort + "]")
            .withProperty(DISTRIBUTED_SYSTEM_ID, "2"));

    server = clusterStartupRule.startServerVM(2,
        s -> s.withSecurityManager(ExpirableSecurityManager.class)
            .withCredential("test", "test")
            .withConnectionToLocator(locatorPort));

    int locator1Port = remoteLocator.getPort();
    remoteServer = clusterStartupRule.startServerVM(3,
        s -> s.withSecurityManager(ExpirableSecurityManager.class)
            .withCredential("test", "test")
            .withConnectionToLocator(locator1Port));

    remoteServer.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      int receiverPort = getRandomAvailableTCPPort();
      internalCache.createGatewayReceiverFactory().setStartPort(receiverPort)
          .setEndPort(receiverPort)
          .setHostnameForSenders("localhost").create().start();

      internalCache.createRegionFactory(REPLICATE).create("regionName");
    });

    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      internalCache.createGatewaySenderFactory().setParallel(false).create("sId", 2)
          .start();

      internalCache.createRegionFactory(REPLICATE).addGatewaySenderId("sId").create("regionName");
    });
  }

  @Test
  public void clientCanPutWithExpirationOnWAN() throws Exception {
    String regionName = "regionName";

    UpdatableUserAuthInitialize.setUser("user1");
    clientCacheRule
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withLocatorConnection(locator.getPort());

    clientCacheRule.createCache();
    Region<Object, Object> region = clientCacheRule.createProxyRegion(regionName);
    region.put("0", "value0");

    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("user1"), locator,
        server);

    UpdatableUserAuthInitialize.setUser("user2");
    region.put("1", "value1");

    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      Region<Object, Object> region1 = internalCache.getRegion(SEPARATOR + regionName);
      await().untilAsserted(() -> {
        assertEquals("value0", region1.get("0"));
        assertEquals("value1", region1.get("1"));
      });

      ExpirableSecurityManager securityManager = getSecurityManager();
      assertThat(securityManager.getExpiredUsers()).containsExactly("user1");
      assertThat(securityManager.getUnAuthorizedOps().get("user1"))
          .containsExactly("DATA:WRITE:regionName:1");
      assertThat(securityManager.getAuthorizedOps().get("user1"))
          .containsExactly("DATA:WRITE:regionName:0");
      assertThat(securityManager.getAuthorizedOps().get("user2"))
          .containsExactly("DATA:WRITE:regionName:1");
    });

    remoteServer.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      Region<Object, Object> region2 = internalCache.getRegion(SEPARATOR + "regionName");
      await().untilAsserted(() -> {
        assertEquals("value0", region2.get("0"));
        assertEquals("value1", region2.get("1"));
      });

      ExpirableSecurityManager securityManager1 = getSecurityManager();
      assertThat(securityManager1.getExpiredUsers()).hasSize(0);
      assertThat(securityManager1.getUnAuthorizedOps()).hasSize(0);
      assertThat(securityManager1.getAuthorizedOps()).hasSize(0);
    });
  }

  protected static ExpirableSecurityManager getSecurityManager() {
    return (ExpirableSecurityManager) Objects.requireNonNull(ClusterStartupRule.getCache())
        .getSecurityService()
        .getSecurityManager();
  }
}
