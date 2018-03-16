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
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.query.Index;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.domain.Stock;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class IndexCommandsShareConfigurationDUnitTest {

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();
  private static String partitionedRegionName = "partitionedRegion";
  private static String indexName = "index1";
  private static String groupName = "group1";
  private MemberVM serverVM;
  private MemberVM locator;

  @Before
  public void before() throws Exception {
    int jmxPort, httpPort;

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    jmxPort = ports[0];
    httpPort = ports[1];

    final Properties locatorProps = new Properties();
    locatorProps.setProperty(NAME, "Locator");
    locatorProps.setProperty(LOG_LEVEL, "fine");
    locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    locatorProps.setProperty(JMX_MANAGER, "true");
    locatorProps.setProperty(JMX_MANAGER_START, "true");
    locatorProps.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
    locatorProps.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));

    locator = startupRule.startLocatorVM(0, x -> x.withProperties(locatorProps));

    gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    Properties props = new Properties();
    props.setProperty(GROUPS, groupName);
    props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.management.internal.cli.domain.Stock");
    serverVM = startupRule.startServerVM(1, props, locator.getPort());
    serverVM.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Region parReg =
          createPartitionedRegion(partitionedRegionName, cache, String.class, Stock.class);
      parReg.put("VMW", new Stock("VMW", 98));
    });
  }

  @Test
  public void testCreateAndDestroyUpdatesSharedConfiguration() throws Exception {
    CommandStringBuilder createStringBuilder = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    createStringBuilder.addOption(CliStrings.GROUP, groupName);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__REGION, "/" + partitionedRegionName);
    gfsh.executeAndAssertThat(createStringBuilder.toString()).statusIsSuccess();

    assertTrue(indexIsListed());

    locator.invoke(() -> {
      InternalClusterConfigurationService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
      String xmlFromConfig;
      try {
        xmlFromConfig = sharedConfig.getConfiguration(groupName).getCacheXmlContent();
        assertTrue(xmlFromConfig.contains(indexName));
      } catch (Exception e) {
        Assert.fail("Error occurred in cluster configuration service", e);
      }
    });

    createStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    createStringBuilder.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    createStringBuilder.addOption(CliStrings.GROUP, groupName);
    createStringBuilder.addOption(CliStrings.DESTROY_INDEX__REGION, "/" + partitionedRegionName);
    gfsh.executeAndAssertThat(createStringBuilder.toString()).statusIsSuccess();

    locator.invoke(() -> {
      InternalClusterConfigurationService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
      String xmlFromConfig;
      try {
        xmlFromConfig = sharedConfig.getConfiguration(groupName).getCacheXmlContent();
        assertFalse(xmlFromConfig.contains(indexName));
      } catch (Exception e) {
        Assert.fail("Error occurred in cluster configuration service", e);
      }
    });

    // Restart the data member cache to make sure that the index is destroyed.
    startupRule.stopVM(1);
    serverVM = startupRule.startServerVM(1, groupName, locator.getPort());

    serverVM.invoke(() -> {
      InternalCache restartedCache = ClusterStartupRule.getCache();
      assertNotNull(restartedCache);
      Region region = restartedCache.getRegion(partitionedRegionName);
      assertNotNull(region);
      Index index = restartedCache.getQueryService().getIndex(region, indexName);
      assertNull(index);
    });
  }

  private static Region<?, ?> createPartitionedRegion(String regionName, Cache cache,
      Class keyConstraint, Class valueConstraint) {
    RegionFactory regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PARTITION);
    regionFactory.setKeyConstraint(keyConstraint);
    regionFactory.setValueConstraint(valueConstraint);
    return regionFactory.create(regionName);
  }

  private boolean indexIsListed() throws Exception {
    gfsh.executeAndAssertThat(CliStrings.LIST_INDEX).statusIsSuccess();
    return gfsh.getGfshOutput().contains(indexName);
  }
}
