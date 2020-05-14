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

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Index;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.domain.Stock;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({OQLIndexTest.class})
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
    locator = startupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    serverVM = startupRule.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort).withProperty(GROUPS, groupName).withProperty(
            SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.management.internal.cli.domain.Stock"));

    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat(
        "create region  --group=group1 --name=partitionedRegion --type=PARTITION "
            + "--key-constraint=java.lang.String --value-constraint=org.apache.geode.management.internal.cli.domain.Stock");

    serverVM.invoke(() -> {
      Region<String, Stock> region = ClusterStartupRule.getCache().getRegion(partitionedRegionName);
      region.put("VMW", new Stock("VMW", 98));
    });
  }

  @Test
  public void testCreateAndDestroyUpdatesSharedConfiguration() {
    CommandStringBuilder createStringBuilder = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    createStringBuilder.addOption(CliStrings.GROUP, groupName);
    createStringBuilder.addOption(CliStrings.CREATE_INDEX__REGION, "/" + partitionedRegionName);
    gfsh.executeAndAssertThat(createStringBuilder.toString()).statusIsSuccess();

    gfsh.executeAndAssertThat(CliStrings.LIST_INDEX).statusIsSuccess().containsOutput(indexName);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();
      String xmlFromConfig = sharedConfig.getConfiguration(groupName).getCacheXmlContent();
      assertThat(xmlFromConfig).contains(indexName);
    });

    createStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    createStringBuilder.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    createStringBuilder.addOption(CliStrings.GROUP, groupName);
    createStringBuilder.addOption(CliStrings.DESTROY_INDEX__REGION, "/" + partitionedRegionName);
    gfsh.executeAndAssertThat(createStringBuilder.toString()).statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();
      String xmlFromConfig = sharedConfig.getConfiguration(groupName).getCacheXmlContent();
      assertThat(xmlFromConfig).doesNotContain(indexName);
    });

    // Restart the data member cache to make sure that the index is destroyed.
    startupRule.stop(1);
    serverVM = startupRule.startServerVM(1, groupName, locator.getPort());

    serverVM.invoke(() -> {
      InternalCache restartedCache = ClusterStartupRule.getCache();
      assertNotNull(restartedCache);
      Region<?, ?> region = restartedCache.getRegion(partitionedRegionName);
      assertNotNull(region);
      Index index = restartedCache.getQueryService().getIndex(region, indexName);
      assertNull(index);
    });
  }

}
