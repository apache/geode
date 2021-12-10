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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
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
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "regionB", 1);

    // cache config in cluster group is null
    locator.invoke(() -> {
      CacheConfig cacheConfig = ClusterStartupRule.getLocator().getConfigurationPersistenceService()
          .getCacheConfig("cluster");
      assertThat(cacheConfig).isNull();
    });
  }

  @Test
  public void createIndexOnSubRegion() throws Exception {
    gfsh.executeAndAssertThat(
        "create region --name=regionB" + SEPARATOR + "child --group=group2 --type=REPLICATE")
        .statusIsSuccess();

    // make sure index on sub region can be created successfully
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat(
            "create index --name=childIndex --region='" + SEPARATOR + "regionB" + SEPARATOR
                + "child c' --expression=id")
            .statusIsSuccess();
    commandResultAssert.hasTableSection()
        .hasColumn("Message")
        .containsExactly("Index successfully created");
    commandResultAssert.hasInfoSection("groupStatus")
        .hasOutput().isEqualTo("Cluster configuration for group 'group2' is updated.");

    // make sure cluster config is updated correctly
    locator.invoke(() -> {
      InternalConfigurationPersistenceService configurationService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig cacheConfig = configurationService.getCacheConfig("group2");
      assertThat(cacheConfig.getRegions()).extracting(RegionConfig::getName)
          .containsExactly("regionB");

      RegionConfig regionB = cacheConfig.getRegions().get(0);
      assertThat(regionB.getRegions()).extracting(RegionConfig::getName)
          .containsExactly("child");

      assertThat(regionB.getRegions().get(0).getRegions()).isEmpty();
    });
  }

  @Test
  // index can't be created on region name with ".".
  // GEODE-7523
  public void createIndexOnRegionNameWithDot() throws Exception {
    gfsh.executeAndAssertThat("create region --name=A.B --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create index --name=indexWithDot --region=A.B --expression=id")
        .statusIsError().containsOutput("Region A does not exist");
  }

  @Test
  public void regionNotExistInClusterConfig() {
    gfsh.executeAndAssertThat(
        "create index --name=myIndex --expression=id --region=" + SEPARATOR + "noExist")
        .statusIsError()
        .hasInfoSection()
        .hasOutput().contains("Region noExist does not exist");
  }

  @Test
  public void regionNotExistInThatMember() throws Exception {
    gfsh.executeAndAssertThat(
        "create index --name=myIndex --expression=id --region=" + SEPARATOR
            + "regionB --member=server-1")
        .statusIsError()
        .hasTableSection()
        .hasRowSize(1)
        .hasRow(0)
        .contains("ERROR", "Region not found : \"" + SEPARATOR + "regionB\"");
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
        .hasOutput().contains("Region regionA does not exist");

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
          .contains("from-clause=\"" + SEPARATOR + "regionB\"").contains("name=\"myIndex\"")
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
        .contains("Region regionB does not exist in some of the groups.");
  }

  @Test
  public void indexCreationOnPartitionedRegionUpdateClusterConfig() {
    int serversNo = 8;
    MemberVM serversArray[] = new MemberVM[serversNo];

    int initialIndex = 1;
    for (int index = 2; index < serversArray.length; index++) {
      serversArray[index] =
          cluster.startServerVM(initialIndex + index, locator.getPort());
    }

    gfsh.executeAndAssertThat("create region --name=regionC --type=PARTITION")
        .statusIsSuccess();

    gfsh.executeAndAssertThat(
        "create index --name=index1 --expression=id --region=regionC")
        .statusIsSuccess()
        .hasTableSection()
        .hasRowSize(8)
        .hasRow(0)
        .contains("OK", "Index successfully created");

    gfsh.executeAndAssertThat("export cluster-configuration")
        .statusIsSuccess()
        .hasInfoSection()
        .hasOutput()
        .contains(
            "index name=\"index1\" expression=\"id\" from-clause=\"/regionC\" key-index=\"false\" type=\"range\"");
  }
}
