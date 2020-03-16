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

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({OQLIndexTest.class})
public class DestroyIndexCommandsDUnitTest {

  private static final String REGION_1 = "REGION1";
  private static final String INDEX_1 = "INDEX1";
  private static final String INDEX_2 = "INDEX2";
  private static final String GROUP_1 = "group1";
  private static final String GROUP_2 = "group2";

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  @Rule
  public ClusterStartupRule rule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.management.internal.cli.domain.Stock");
    locator = rule.startLocatorVM(0);

    props.setProperty("groups", GROUP_1);
    server1 = rule.startServerVM(1, props, locator.getPort());
    props.setProperty("groups", GROUP_2);
    server2 = rule.startServerVM(2, props, locator.getPort());

    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat(String.format("create region --name=%s --type=REPLICATE", REGION_1))
        .statusIsSuccess();

    CommandResultAssert createIndex1Assert = gfsh.executeAndAssertThat(
        String.format("create index --name=%s --expression=key --region=%s", INDEX_1, REGION_1))
        .statusIsSuccess();
    createIndex1Assert.hasTableSection("createIndex").hasRowSize(2);
    createIndex1Assert.containsOutput("Cluster configuration for group 'cluster' is updated");
    CommandResultAssert createIndex2Assert = gfsh.executeAndAssertThat(
        String.format("create index --name=%s --expression=id --region=%s", INDEX_2, REGION_1))
        .statusIsSuccess();
    createIndex2Assert.hasTableSection("createIndex").hasRowSize(2);
    createIndex2Assert.containsOutput("Cluster configuration for group 'cluster' is updated");

    assertIndexCount(REGION_1, 2);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDestroyAllIndexesOnRegion() {
    gfsh.executeAndAssertThat("destroy index --region=" + REGION_1).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status", "OK", "OK")
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "Destroyed all indexes on region REGION1", "Destroyed all indexes on region REGION1");

    assertIndexCount(REGION_1, 0);

    // Check idempotency
    gfsh.executeAndAssertThat("destroy index --if-exists --region=" + REGION_1).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status", "OK", "OK")
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "Destroyed all indexes on region REGION1", "Destroyed all indexes on region REGION1");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDestroyOneIndexOnRegion() {
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --region=" + REGION_1)
        .statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder("Message",
            "Destroyed index INDEX1 on region REGION1", "Destroyed index INDEX1 on region REGION1");
    assertIndexCount(REGION_1, 1);

    // Check idempotency
    gfsh.executeAndAssertThat(
        "destroy index --if-exists --name=" + INDEX_1 + " --region=" + REGION_1).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status", "IGNORED", "IGNORED")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Index named \"INDEX1\" not found",
            "Index named \"INDEX1\" not found");
    assertIndexCount(REGION_1, 1);

    // Check error result is correct
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --region=" + REGION_1)
        .tableHasColumnWithExactValuesInAnyOrder("Status", "ERROR", "ERROR").statusIsError()
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Index named \"INDEX1\" not found",
            "Index named \"INDEX1\" not found");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDestroyAllIndexesOnOneMember() {
    gfsh.executeAndAssertThat("destroy index --member=server-1").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Destroyed all indexes");

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes()).isEmpty();
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(2);
    });

    // Check idempotency
    gfsh.executeAndAssertThat("destroy index --if-exists --member=server-1").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Destroyed all indexes");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDestroyOneIndexOnOneMember() {
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --member=server-1")
        .statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Destroyed index INDEX1");

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(1);
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(2);
    });

    // Check idempotency
    gfsh.executeAndAssertThat("destroy index --if-exists --name=" + INDEX_1 + " --member=server-1")
        .statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder("Status", "IGNORED")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Index named \"INDEX1\" not found");

    // Check error result is correct
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --member=server-1")
        .statusIsError().tableHasColumnWithExactValuesInAnyOrder("Status", "ERROR")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Index named \"INDEX1\" not found");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testPartialSuccessResultDestroyOneIndex() {
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --member=server-1")
        .statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder("Status", "OK")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Destroyed index INDEX1");

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(1);
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(2);
    });

    // Check error on partial failure
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status", "ERROR", "OK")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Index named \"INDEX1\" not found",
            "Destroyed index INDEX1");

    assertIndexCount(REGION_1, 1);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void destroyIndexOnOneGroupWithoutAssociatedClusterConfig() {
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --group=" + GROUP_1)
        .statusIsSuccess().tableHasColumnWithValuesContaining("Member", "server-1")
        .tableHasColumnWithExactValuesInAnyOrder("Status", "OK")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Destroyed index INDEX1");

    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_2 + " --group=" + GROUP_2)
        .statusIsSuccess().tableHasColumnWithValuesContaining("Member", "server-2")
        .tableHasColumnWithExactValuesInAnyOrder("Status", "OK")
        .tableHasColumnWithExactValuesInAnyOrder("Message", "Destroyed index INDEX2");

    // The index count on each server and the cluster config will now have diverged because the
    // index+region were not originally defined per group but at the cluster level.
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(1);
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(1);
    });

    locator.invoke(() -> {
      InternalConfigurationPersistenceService svc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      RegionConfig regionConfig = svc.getCacheConfig("cluster").findRegionConfiguration(REGION_1);
      assertThat(regionConfig.getIndexes().size()).isEqualTo(2);
    });
  }

  @Test
  @SuppressWarnings("deprecation")
  public void destroyIndexOnRegionNotInClusterConfig() {
    IgnoredException.addIgnoredException("failed to update cluster config for cluster");
    IgnoredException.addIgnoredException(
        "org.apache.geode.management.internal.exceptions.EntityNotFoundException");

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
      factory.create("REGION3");
      cache.getQueryService().createIndex("INDEX3", "key", "/REGION3");
    });

    gfsh.executeAndAssertThat("destroy index --name=INDEX3" + " --region=REGION3").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status", "OK", "ERROR")
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "Destroyed index INDEX3 on region REGION3", "Region \"REGION3\" not found");

    assertIndexCount(REGION_1, 2);
  }

  private void assertIndexCount(String region, int indexCount) {
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(indexCount);
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(indexCount);
    });

    locator.invoke(() -> {
      InternalConfigurationPersistenceService svc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      RegionConfig regionConfig = svc.getCacheConfig("cluster").findRegionConfiguration(region);
      assertThat(regionConfig.getIndexes().size()).isEqualTo(indexCount);
    });
  }
}
