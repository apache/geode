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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(RegionsTest.class) // GEODE-973 GEODE-2009
@SuppressWarnings("serial")
public class RegionChangesPersistThroughClusterConfigurationDUnitTest {

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  private MemberVM locator;
  private MemberVM server2;

  private static final String REGION_NAME = "testRegionSharedConfigRegion";
  private static final String REGION_PATH = "/" + REGION_NAME;
  private static final String GROUP_NAME = "cluster";

  @Before
  public void setup() throws Exception {
    int[] randomPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int jmxPort = randomPorts[0];
    int httpPort = randomPorts[1];
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "fine");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + jmxPort);
    props.setProperty(ConfigurationProperties.MAX_WAIT_TIME_RECONNECT, "5000");
    props.setProperty(HTTP_SERVICE_PORT, "" + httpPort);

    locator = lsRule.startLocatorVM(0, props);

    IgnoredException
        .addIgnoredException("Possible loss of quorum due to the loss of 1 cache processes");

    gfsh.connectAndVerify(locator);

    Properties serverProps = new Properties();
    serverProps.setProperty(MCAST_PORT, "0");
    serverProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
    @SuppressWarnings("unused")
    MemberVM server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    server2 = lsRule.startServerVM(2, serverProps, locator.getPort());

    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE --enable-statistics=true --name=" + REGION_PATH)
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, 2);

  }

  @Test
  @SuppressWarnings("unchecked")
  public void createdRegionPersistsThroughCacheConfig() {
    locator.invoke(() -> {
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();

      assertThat(sharedConfig.getConfiguration(GROUP_NAME).getCacheXmlContent())
          .contains(REGION_NAME);
    });

    server2.forceDisconnect();

    server2.waitTilFullyReconnected();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, 2);

    server2.invoke(() -> {
      InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
      InternalCache cache = system.getCache();
      assertThat(cache.getInternalRegionByPath(REGION_PATH)).isNotNull();
    });
  }

  @Test
  @SuppressWarnings("unchecked")
  public void regionUpdatePersistsThroughClusterConfig() {
    server2.invoke(() -> {
      InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
      InternalCache cache = system.getCache();
      assertThat(cache.getInternalRegionByPath(REGION_PATH).isEntryExpiryPossible()).isFalse();
    });

    gfsh.executeAndAssertThat(
        "alter region --name=" + REGION_PATH + " --entry-time-to-live-expiration=45635 " +
            "--entry-time-to-live-expiration-action=destroy")
        .statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();

      assertThat(sharedConfig.getConfiguration(GROUP_NAME).getCacheXmlContent()).contains("45635");
    });

    server2.forceDisconnect();

    server2.waitTilFullyReconnected();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, 2);

    server2.invoke(() -> {
      InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
      InternalCache cache = system.getCache();
      assertThat(cache.getInternalRegionByPath(REGION_PATH)).isNotNull();
      assertThat(cache.getInternalRegionByPath(REGION_PATH).isEntryExpiryPossible()).isTrue();
    });
  }

  @Test
  @SuppressWarnings("unchecked")
  public void destroyRegionPersistsThroughClusterConfig() {
    gfsh.executeAndAssertThat("destroy region --name=" + REGION_PATH).statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();

      assertThat(sharedConfig.getConfiguration(GROUP_NAME).getCacheXmlContent())
          .doesNotContain(REGION_NAME);
    });

    server2.forceDisconnect();

    server2.waitTilFullyReconnected();

    server2.invoke(() -> {
      InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
      InternalCache cache = system.getCache();
      assertThat(cache.getInternalRegionByPath(REGION_PATH)).isNull();
    });
  }
}
