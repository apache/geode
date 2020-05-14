/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.assertions.TabularResultModelAssert;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category({OQLIndexTest.class})
public class CreateIndexCommandDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator, server1, server2;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, "group2", locator.getPort());
    gfsh.connectAndVerify(locator);

    // create a region on server-2 in group 2
    gfsh.executeAndAssertThat("create region --name=regionB --group=group2 --type=REPLICATE")
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/regionB", 1);

    // cache config in cluster group is null
    locator.invoke(() -> {
      CacheConfig cacheConfig = ClusterStartupRule.getLocator().getConfigurationPersistenceService()
          .getCacheConfig("cluster");
      assertThat(cacheConfig).isNull();
    });
  }

  @Test
  public void regionNotExistInClusterConfig() {
    gfsh.executeAndAssertThat("create index --name=myIndex --expression=id --region=/noExist")
        .statusIsError()
        .hasInfoSection()
        .hasOutput().contains("Region /noExist does not exist");
  }

  @Test
  public void regionNotExistInThatMember() throws Exception {
    gfsh.executeAndAssertThat(
        "create index --name=myIndex --expression=id --region=/regionB --member=server-1")
        .statusIsError()
        .hasTableSection()
        .hasRowSize(1)
        .hasRow(0)
        .contains("ERROR", "Region not found : \"/regionB\"");
  }

  @Test
  public void regionExistOnServerButNotInClusterConfig() {
    IgnoredException.addIgnoredException(
        "org.apache.geode.management.internal.exceptions.EntityNotFoundException");
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.REPLICATE).create("regionA");
    });

    // regionA does not exist as far as cluster configuration is concerned
    gfsh.executeAndAssertThat("create index --name=myIndex --expression=id --region=regionA")
        .statusIsError()
        .hasInfoSection()
        .hasOutput().contains("Region /regionA does not exist");

    // you can only create index on regionA when specifying a --member option
    gfsh.executeAndAssertThat(
        "create index --name=myIndex --expression=id --region=regionA --member=server-1")
        .statusIsSuccess()
        .hasTableSection()
        .hasRowSize(1)
        .hasRow(0)
        .contains("OK", "Index successfully created");

    // after index is created, the cluster config is not updated with regionA nor index
    locator.invoke(() -> {
      InternalConfigurationPersistenceService configurationService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      assertThat(
          configurationService.getCacheConfig("cluster", true).findRegionConfiguration("regionA"))
              .isNull();
    });
  }

  @Test
  public void regionExistInGroup2ClusterConfig() {
    locator.invoke(() -> {
      InternalConfigurationPersistenceService configurationService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      assertThat(configurationService.getConfiguration("group2").getCacheXmlContent())
          .contains("<region name=\"regionB\"");
    });

    // make sure index is created on server-2 (which is only in group-2)
    CommandResultAssert commandAssert =
        gfsh.executeAndAssertThat("create index --name=myIndex --expression=id --region=regionB")
            .statusIsSuccess();
    TabularResultModelAssert createIndexTableAssert =
        commandAssert.hasTableSection("createIndex");
    createIndexTableAssert.hasRowSize(1).hasRow(0).contains("OK", "Index successfully created");
    createIndexTableAssert.hasColumn("Member").asList().first().toString().contains("server-2");
    commandAssert.containsOutput("Cluster configuration for group 'group2' is updated.");

    // make sure index is inserted in group2's cluster configuration
    locator.invoke(() -> {
      InternalConfigurationPersistenceService configurationService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      assertThat(configurationService.getConfiguration("group2").getCacheXmlContent())
          .contains("<region name=\"regionB\"").contains("<index").contains("expression=\"id\" ")
          .contains("from-clause=\"/regionB\"").contains("name=\"myIndex\"")
          .contains("type=\"range\"");
    });
  }

  @Test
  public void regionExistInClusterConfigButDifferentGroup() {
    // regionB is only in group2, not in group1
    CommandResultAssert commandAssert =
        gfsh.executeAndAssertThat(
            "create index --name=index2 --expression=key --region=regionB --group=group1")
            .statusIsError();

    commandAssert.hasInfoSection().hasOutput()
        .contains("Region /regionB does not exist in some of the groups.");
  }
}
