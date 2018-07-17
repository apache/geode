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
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category({DistributedTest.class, OQLIndexTest.class})
public class CreateIndexCommandDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator, server1;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startServerVM(1, locator.getPort());
    gfsh.connectAndVerify(locator);

    // when locator started, the cacheConfig is null
    locator.invoke(() -> {
      CacheConfig cacheConfig = ClusterStartupRule.getLocator().getConfigurationPersistenceService()
          .getCacheConfig("cluster");
      assertThat(cacheConfig).isNull();
    });
  }

  @Test
  public void regionNotExist() {
    gfsh.executeAndAssertThat("create index --name=myIndex --expression=id --region=/noExist")
        .statusIsError().tableHasColumnWithExactValuesInAnyOrder("Status", "ERROR")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Region not found : \"/noExist\"");

    locator.invoke(() -> {
      InternalConfigurationPersistenceService configurationService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig cacheConfig = configurationService.getCacheConfig("cluster");
      assertThat(cacheConfig.findRegionConfiguration("noExist")).isNull();
    });
  }

  @Test
  public void regionExistOnServerButNotInClusterConfig() {
    IgnoredException.addIgnoredException(
        "org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException");
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.REPLICATE).create("regionA");
    });

    // no region exists in cluster config
    locator.invoke(() -> {
      InternalConfigurationPersistenceService configurationService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig cacheConfig = configurationService.getCacheConfig("cluster");
      assertThat(cacheConfig.findRegionConfiguration("regionA")).isNull();
    });

    gfsh.executeAndAssertThat("create index --name=myIndex --expression=id --region=regionA")
        .statusIsSuccess().tableHasColumnWithValuesContaining("Status", "OK")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Index successfully created");

    // after index is created, the cluster config is not udpated with regionA or index
    locator.invoke(() -> {
      InternalConfigurationPersistenceService configurationService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      assertThat(configurationService.getCacheConfig("cluster").findRegionConfiguration("regionA"))
          .isNull();
    });
  }

  @Test
  public void regionExistInClusterConfig() {
    gfsh.executeAndAssertThat("create region --name=regionB --type=REPLICATE").statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/regionB", 1);
    locator.invoke(() -> {
      InternalConfigurationPersistenceService configurationService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      assertThat(configurationService.getConfiguration("cluster").getCacheXmlContent())
          .contains("<region name=\"regionB\">");
    });

    gfsh.executeAndAssertThat("create index --name=myIndex --expression=id --region=regionB")
        .statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService configurationService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      assertThat(configurationService.getConfiguration("cluster").getCacheXmlContent())
          .contains("<region name=\"regionB\">").contains("<index").contains("expression=\"id\" ")
          .contains("from-clause=\"/regionB\"").contains("name=\"myIndex\"")
          .contains("type=\"range\"");
    });
  }
}
