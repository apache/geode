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
package org.apache.geode.cache;

import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.ClientHealthStatus;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({ClientServerTest.class})
public class ClientStatisticsPublicationSecurityDUnitTest {
  private static MemberVM locator, server;

  @Rule
  public final GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public final ClusterStartupRule cluster = new ClusterStartupRule();

  private static final Logger log = LogService.getLogger();

  @Before
  public void before() {
    Properties locatorProps = new Properties();
    locatorProps.setProperty(CliStrings.START_LOCATOR__MEMBER_NAME, "locator1");
    locatorProps.setProperty("security-manager", "org.apache.geode.examples.SimpleSecurityManager");
    locator = cluster.startLocatorVM(0, locatorProps);

    Properties serverProps = new Properties();
    serverProps.setProperty(CliStrings.START_SERVER__NAME, "server1");
    serverProps.setProperty(CliStrings.START_SERVER__LOCATORS,
        "localhost[" + locator.getPort() + "]");
    serverProps.setProperty("security-manager", "org.apache.geode.examples.SimpleSecurityManager");
    serverProps.setProperty("security-username", "cluster");
    serverProps.setProperty("security-password", "cluster");
    server = cluster.startServerVM(1, serverProps);
    server.invoke(() -> {
      InternalCache cache = InternalDistributedSystem.getConnectedInstance().getCache();

      cache.createRegionFactory(RegionShortcut.REPLICATE).create("regionName");
    });
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/regionName", 1);
  }

  @Test
  public void testClientCanPublishStatisticsWithSecurity() throws Exception {
    final String regionName = "regionName";

    ClientCache client = new ClientCacheFactory()
        .set(ConfigurationProperties.LOG_LEVEL, "fine")
        .set(ConfigurationProperties.LOG_FILE, "")
        .set("security-manager", "org.apache.geode.examples.SimpleSecurityManager")
        .set("security-username", "data")
        .set("security-password", "data")
        .set(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName())
        .create();
    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", server.getPort());
    poolFactory.setStatisticInterval(1000);
    poolFactory.setSubscriptionEnabled(true);
    Pool pool = poolFactory.create("poolName");


    Region region =
        client.createClientRegionFactory(CACHING_PROXY).setPoolName("poolName").create(regionName);

    checkClientHealthStatus(0, "poolName");

    region.put("key", "value");

    checkClientHealthStatus(1, "poolName");

    client.close(false);
  }

  private void checkClientHealthStatus(int expectedNumPuts,
      String... expectedPoolStatKeys) {
    final int serverPort = server.getPort();
    server.invoke(() -> {
      await().untilAsserted(() -> {
        Cache cache = ClusterStartupRule.getCache();
        SystemManagementService service =
            (SystemManagementService) ManagementService.getExistingManagementService(cache);
        CacheServerMXBean serviceMBean = service.getJMXAdapter().getClientServiceMXBean(serverPort);
        String clientId = serviceMBean.getClientIds()[0];
        ClientHealthStatus status = serviceMBean.showClientStats(clientId);

        assertThat(status.getNumOfPuts()).isEqualTo(expectedNumPuts);
        assertThat(status.getPoolStats().keySet())
            .containsExactlyInAnyOrder(expectedPoolStatKeys);
      });
    });
  }
}
