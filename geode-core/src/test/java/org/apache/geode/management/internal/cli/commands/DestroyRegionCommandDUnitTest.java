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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({DistributedTest.class, RegionsTest.class})
public class DestroyRegionCommandDUnitTest {
  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator, server1, server2, server3;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    server2 = lsRule.startServerVM(2, "group2", locator.getPort());
    server3 = lsRule.startServerVM(3, "group2", locator.getPort());
  }

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testDestroyDistributedRegion() {
    gfsh.executeAndAssertThat("create region --name=Customer --type=PARTITION").statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=Order --type=PARTITION --colocated-with=Customer").statusIsSuccess();

    locator.waitTillRegionsAreReadyOnServers("/Customer", 3);
    locator.waitTillRegionsAreReadyOnServers("/Order", 3);

    // Test unable to destroy with co-location
    gfsh.executeAndAssertThat("destroy region --name=/Customer").statusIsError()
        .tableHasRowCount("Member", 3)
        .containsOutput("The parent region [/Customer] in colocation chain cannot be destroyed");

    // Test success
    gfsh.executeAndAssertThat("destroy region --name=/Order").statusIsSuccess()
        .tableHasRowCount("Member", 3).containsOutput("destroyed successfully");
    gfsh.executeAndAssertThat("destroy region --name=/Customer").statusIsSuccess()
        .tableHasRowCount("Member", 3).containsOutput("destroyed successfully");

    // destroy something that's not exist anymore
    gfsh.executeAndAssertThat("destroy region --name=/Customer").statusIsError()
        .containsOutput("Could not find a Region");
    gfsh.executeAndAssertThat("destroy region --name=/Customer --if-exists")
        .containsOutput("Skipping: Could not find a Region").statusIsSuccess();
  }

  @Test
  public void testDestroyLocalRegions() {
    gfsh.executeAndAssertThat("create region --name=region1 --type=LOCAL").statusIsSuccess();

    locator.waitTillRegionsAreReadyOnServers("/region1", 3);

    gfsh.executeAndAssertThat("destroy region --name=region1").statusIsSuccess()
        .tableHasRowCount("Member", 3).containsOutput("destroyed successfully");

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getRegion("region1")).isNull();
    }, server1, server2, server3);
  }

  @Test
  public void testDestroyDistributedRegions() {
    gfsh.executeAndAssertThat("create region --name=region1 --type=REPLICATE_PROXY --group=group1")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=region1 --type=REPLICATE --group=group2")
        .statusIsSuccess();

    locator.waitTillRegionsAreReadyOnServers("/region1", 3);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration group1Config = service.getConfiguration("group1");
      assertThat(group1Config.getCacheXmlContent()).contains("<region name=\"region1\">\n"
          + "    <region-attributes data-policy=\"empty\" scope=\"distributed-ack\"/>");

      Configuration clusterConfig = service.getConfiguration("group2");
      assertThat(clusterConfig.getCacheXmlContent()).contains("<region name=\"region1\">\n"
          + "    <region-attributes data-policy=\"replicate\" scope=\"distributed-ack\"/>");
    });

    gfsh.executeAndAssertThat("destroy region --name=region1").statusIsSuccess()
        .tableHasRowCount("Member", 3).containsOutput("destroyed successfully");

    // verify that all cc entries are deleted, no matter what the scope is
    locator.invoke(() -> {
      InternalConfigurationPersistenceService service =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration group1Config = service.getConfiguration("group1");
      assertThat(group1Config.getCacheXmlContent()).doesNotContain("region1");

      Configuration clusterConfig = service.getConfiguration("group2");
      assertThat(clusterConfig.getCacheXmlContent()).doesNotContain("region1");
    });

    // verify that all regions are destroyed, no matter what the scope is
    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getRegion("region1")).isNull();
    }, server1, server2, server3);
  }

  @Test
  public void testDestroyRegionWithSharedConfig() throws IOException {
    gfsh.executeAndAssertThat("create region --name=Customer --type=REPLICATE").statusIsSuccess();

    // Make sure the region exists in the cluster config
    locator.invoke(() -> {
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();
      assertThat(sharedConfig.getConfiguration("cluster").getCacheXmlContent())
          .contains("Customer");
    });

    // destroy the region
    gfsh.executeAndAssertThat("destroy region --name=Customer").statusIsSuccess()
        .containsOutput("destroyed successfully");

    // make sure the region was removed from cluster config
    locator.invoke(() -> {
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();
      assertThat(sharedConfig.getConfiguration("cluster").getCacheXmlContent())
          .doesNotContain("Customer");
    });

    // restart one server to make sure the region does not exist anymore
    lsRule.stopMember(1);
    lsRule.startServerVM(1, locator.getPort());

    // make sure region does not exist
    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getRegion("Customer")).isNull();
    }, server1, server2, server3);
  }
}
