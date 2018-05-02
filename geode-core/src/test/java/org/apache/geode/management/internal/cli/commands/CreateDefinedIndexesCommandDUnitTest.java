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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({DistributedTest.class, GfshTest.class})
public class CreateDefinedIndexesCommandDUnitTest {
  private MemberVM locator, server1, server2, server3;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @Before
  public void before() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0);
    server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, locator.getPort());
    server3 = clusterStartupRule.startServerVM(3, "group1", locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("clear defined indexes").statusIsSuccess()
        .containsOutput("Index definitions successfully cleared");
  }

  @Test
  public void noDefinitions() {
    gfsh.executeAndAssertThat("create defined indexes").statusIsSuccess()
        .containsOutput("No indexes defined");
  }

  @Test
  public void nonexistentRegion() {
    String regionName = testName.getMethodName();

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getRegion(regionName)).isNull();
    }, server1, server2, server3);

    gfsh.executeAndAssertThat("define index --name=index_" + regionName
        + " --expression=value1 --region=" + regionName + "1").statusIsSuccess()
        .containsOutput("Index successfully defined");

    gfsh.executeAndAssertThat("create defined indexes").statusIsError()
        .containsOutput("RegionNotFoundException");

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();

      assertThat(queryService.getIndexes().isEmpty()).isTrue();
    }, server1, server2, server3);
  }

  @Test
  public void multipleIndexesOnMultipleRegionsClusterWide() {
    String region1Name = testName.getMethodName() + "1";
    String region2Name = testName.getMethodName() + "2";
    String index1Name = "index_" + region1Name;
    String index2Name = "index_" + region2Name;

    gfsh.executeAndAssertThat("create region --name=" + region1Name + " --type=REPLICATE")
        .statusIsSuccess().containsOutput("Region \"/" + region1Name + "\" created on \"server-1\"")
        .containsOutput("Region \"/" + region1Name + "\" created on \"server-2\"")
        .containsOutput("Region \"/" + region1Name + "\" created on \"server-3\"");

    gfsh.executeAndAssertThat("create region --name=" + region2Name + " --type=REPLICATE")
        .statusIsSuccess().containsOutput("Region \"/" + region2Name + "\" created on \"server-1\"")
        .containsOutput("Region \"/" + region2Name + "\" created on \"server-2\"")
        .containsOutput("Region \"/" + region2Name + "\" created on \"server-3\"");

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getRegion(region1Name)).isNotNull();
      assertThat(cache.getRegion(region2Name)).isNotNull();
    }, server1, server2, server3);

    gfsh.executeAndAssertThat(
        "define index --name=" + index1Name + " --expression=value1 --region=" + region1Name)
        .statusIsSuccess().containsOutput("Index successfully defined");

    gfsh.executeAndAssertThat(
        "define index --name=" + index2Name + " --expression=value2 --region=" + region2Name)
        .statusIsSuccess().containsOutput("Index successfully defined");

    gfsh.executeAndAssertThat("create defined indexes").statusIsSuccess()
        .containsOutput("Indexes successfully created");

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();
      Region region1 = cache.getRegion(region1Name);
      Region region2 = cache.getRegion(region2Name);

      assertThat(queryService.getIndexes(region1).size()).isEqualTo(1);
      assertThat(queryService.getIndexes(region2).size()).isEqualTo(1);
      assertThat(queryService.getIndex(region1, index1Name)).isNotNull();
      assertThat(queryService.getIndex(region2, index2Name)).isNotNull();
    }, server1, server2, server3);

    locator.invoke(() -> {
      // Make sure the indexes exist in the cluster config
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();
      assertThat(sharedConfig.getConfiguration("cluster").getCacheXmlContent()).contains(index1Name,
          index2Name);
      assertThat(sharedConfig.getConfiguration("group1")).isNull();
    });
  }

  @Test
  public void multipleIndexesOnMultipleRegionsInMemberGroup() {
    String region1Name = testName.getMethodName() + "1";
    String region2Name = testName.getMethodName() + "2";
    String index1Name = "index_" + region1Name;
    String index2Name = "index_" + region2Name;

    gfsh.executeAndAssertThat(
        "create region --name=" + region1Name + " --type=REPLICATE --group=group1")
        .statusIsSuccess()
        .containsOutput("Region \"/" + region1Name + "\" created on \"server-3\"");

    gfsh.executeAndAssertThat(
        "create region --name=" + region2Name + " --type=REPLICATE --group=group1")
        .statusIsSuccess()
        .containsOutput("Region \"/" + region2Name + "\" created on \"server-3\"");

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getRegion(region1Name)).isNull();
      assertThat(cache.getRegion(region2Name)).isNull();
    }, server1, server2);

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getRegion(region1Name)).isNotNull();
      assertThat(cache.getRegion(region2Name)).isNotNull();
    }, server3);

    gfsh.executeAndAssertThat(
        "define index --name=" + index1Name + " --expression=value1 --region=" + region1Name)
        .statusIsSuccess().containsOutput("Index successfully defined");

    gfsh.executeAndAssertThat(
        "define index --name=" + index2Name + " --expression=value1 --region=" + region2Name)
        .statusIsSuccess().containsOutput("Index successfully defined");

    gfsh.executeAndAssertThat("create defined indexes --group=group1").statusIsSuccess()
        .containsOutput("Indexes successfully created");

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();
      Region region1 = cache.getRegion(region1Name);
      Region region2 = cache.getRegion(region2Name);

      assertThat(queryService.getIndexes(region1).size()).isEqualTo(1);
      assertThat(queryService.getIndexes(region2).size()).isEqualTo(1);
      assertThat(queryService.getIndex(region1, index1Name)).isNotNull();
      assertThat(queryService.getIndex(region2, index2Name)).isNotNull();
    }, server3);

    locator.invoke(() -> {
      // Make sure the indexes exist in the cluster config
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();
      assertThat(sharedConfig.getConfiguration("group1").getCacheXmlContent()).contains(index2Name,
          index1Name);
      assertThat(sharedConfig.getConfiguration("cluster").getCacheXmlContent()).isNullOrEmpty();
    });
  }
}
