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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({OQLIndexTest.class})
public class CreateDefinedIndexesCommandDUnitTest {
  private static MemberVM locator, server1, server2, server3;

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0);
    server1 = clusterStartupRule.startServerVM(1, "group1", locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, "group1", locator.getPort());
    server3 = clusterStartupRule.startServerVM(3, "group2", locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Before
  public void before() {
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

    String indexName = "index_" + regionName;
    gfsh.executeAndAssertThat("define index --name=index_" + regionName
        + " --expression=value1 --region=" + regionName + "1").statusIsSuccess()
        .containsOutput("Index successfully defined");

    CommandResult result = gfsh.executeCommand("create defined indexes");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    Map<String, List<String>> table =
        result.getMapFromTableContent(CreateDefinedIndexesCommand.CREATE_DEFINED_INDEXES_SECTION);
    assertThat(table.get("Status")).contains("ERROR", "ERROR", "ERROR");

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();

      List<String> currentIndexes =
          queryService.getIndexes().stream().map(Index::getName).collect(Collectors.toList());
      assertThat(currentIndexes).doesNotContain(indexName);
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

    CommandResult result = gfsh.executeCommand("create defined indexes");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    Map<String, List<String>> table =
        result.getMapFromTableContent(CreateDefinedIndexesCommand.CREATE_DEFINED_INDEXES_SECTION);
    assertThat(table.get("Member").size()).isEqualTo(6);
    assertThat(result.getMessageFromContent())
        .contains("Changes to configuration for group 'cluster' are persisted");

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
      RegionConfig region1Config =
          sharedConfig.getCacheConfig("cluster").findRegionConfiguration(region1Name);
      assertThat(region1Config.getIndexes().stream().map(RegionConfig.Index::getName)
          .collect(Collectors.toList())).contains(index1Name);

      RegionConfig region2Config =
          sharedConfig.getCacheConfig("cluster").findRegionConfiguration(region2Name);
      assertThat(region2Config.getIndexes().stream().map(RegionConfig.Index::getName)
          .collect(Collectors.toList())).contains(index2Name);
    });
  }

  @Test
  public void multipleIndexesOnMultipleRegionsInMemberGroup() {
    String region1Name = testName.getMethodName() + "1";
    String region2Name = testName.getMethodName() + "2";
    String index1Name = "index_" + region1Name;
    String index2Name = "index_" + region2Name;

    CommandResult result = gfsh
        .executeCommand("create region --name=" + region1Name + " --type=REPLICATE --group=group1");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMapFromTableContent("0", "0").get("Member")).contains("server-1",
        "server-2");

    result = gfsh
        .executeCommand("create region --name=" + region2Name + " --type=REPLICATE --group=group1");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMapFromTableContent("0", "0").get("Member")).contains("server-1",
        "server-2");

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getRegion(region1Name)).isNull();
      assertThat(cache.getRegion(region2Name)).isNull();
    }, server3);

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getRegion(region1Name)).isNotNull();
      assertThat(cache.getRegion(region2Name)).isNotNull();
    }, server1, server2);

    gfsh.executeAndAssertThat(
        "define index --name=" + index1Name + " --expression=value1 --region=" + region1Name)
        .statusIsSuccess().containsOutput("Index successfully defined");

    gfsh.executeAndAssertThat(
        "define index --name=" + index2Name + " --expression=value1 --region=" + region2Name)
        .statusIsSuccess().containsOutput("Index successfully defined");

    gfsh.executeAndAssertThat("create defined indexes --group=group1").statusIsSuccess()
        .containsOutput("Changes to configuration for group 'group1' are persisted.");

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();
      Region region1 = cache.getRegion(region1Name);
      Region region2 = cache.getRegion(region2Name);

      assertThat(queryService.getIndexes(region1).size()).isEqualTo(1);
      assertThat(queryService.getIndexes(region2).size()).isEqualTo(1);
      assertThat(queryService.getIndex(region1, index1Name)).isNotNull();
      assertThat(queryService.getIndex(region2, index2Name)).isNotNull();
    }, server1, server2);

    locator.invoke(() -> {
      // Make sure the indexes exist in the cluster config
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();
      assertThat(sharedConfig.getConfiguration("group1").getCacheXmlContent()).contains(index2Name,
          index1Name);
    });
  }

  @Test
  public void disjointRegionsAndGroupsFailsToDefineIndexes() {
    String region1Name = testName.getMethodName() + "1";
    String region2Name = testName.getMethodName() + "2";
    String index1Name = "index_" + region1Name;
    String index2Name = "index_" + region2Name;

    CommandResult result = gfsh
        .executeCommand("create region --name=" + region1Name + " --type=REPLICATE --group=group1");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMapFromTableContent("0", "0").get("Member")).contains("server-1",
        "server-2");

    result = gfsh
        .executeCommand("create region --name=" + region2Name + " --type=REPLICATE --group=group2");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMapFromTableContent("0", "0").get("Member")).contains("server-3");

    gfsh.executeAndAssertThat(
        "define index --name=" + index1Name + " --expression=value1 --region=" + region1Name)
        .statusIsSuccess().containsOutput("Index successfully defined");

    gfsh.executeAndAssertThat(
        "define index --name=" + index2Name + " --expression=value1 --region=" + region2Name)
        .statusIsSuccess().containsOutput("Index successfully defined");

    result = gfsh.executeCommand("create defined indexes --group=group1,group2");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(
        result.getMapFromTableContent(CreateDefinedIndexesCommand.CREATE_DEFINED_INDEXES_SECTION)
            .get("Status")).contains("ERROR", "ERROR", "ERROR");

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();
      Region region1 = cache.getRegion(region1Name);

      assertThat(queryService.getIndexes(region1)).isNotNull();
      assertThat(queryService.getIndexes(region1).isEmpty()).isTrue();
    }, server1, server2);

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      QueryService queryService = cache.getQueryService();
      Region region2 = cache.getRegion(region2Name);

      assertThat(queryService.getIndexes(region2)).isNotNull();
      assertThat(queryService.getIndexes(region2).isEmpty()).isTrue();
    }, server3);

    locator.invoke(() -> {
      // Make sure the indexes do not exist in the cluster config
      InternalConfigurationPersistenceService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getConfigurationPersistenceService();
      assertThat(sharedConfig.getConfiguration("group1").getCacheXmlContent())
          .doesNotContain(index1Name);
      assertThat(sharedConfig.getConfiguration("group2").getCacheXmlContent())
          .doesNotContain(index2Name);
    });
  }

}
