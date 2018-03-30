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
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.management.internal.cli.domain.Stock;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class, GfshTest.class})
public class DestroyIndexCommandsDUnitTest {

  private static final String REGION_1 = "REGION1";
  private static final String INDEX_1 = "INDEX1";
  private static final String INDEX_2 = "INDEX2";

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
    server1 = rule.startServerVM(1, props, locator.getPort());
    server2 = rule.startServerVM(2, props, locator.getPort());

    server1.invoke(() -> {
      createRegionAndIndex();
    });

    server2.invoke(() -> {
      createRegionAndIndex();
    });

    gfsh.connectAndVerify(locator);
  }

  private static void createRegionAndIndex() throws Exception {
    Cache cache = ClusterStartupRule.getCache();
    RegionFactory factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region region = factory.create(REGION_1);

    cache.getQueryService().createIndex(INDEX_1, "key", "/" + REGION_1);
    cache.getQueryService().createIndex(INDEX_2, "id", "/" + REGION_1);
    region.put(1, new Stock("SUNW", 10));
  }

  @Test
  public void testDestroyAllIndexesOnRegion() throws Exception {
    gfsh.executeAndAssertThat("destroy index --region=" + REGION_1).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status",
            "Destroyed all indexes on region REGION1", "Destroyed all indexes on region REGION1");

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes()).isEmpty();
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes()).isEmpty();
    });

    // Check idempotency
    gfsh.executeAndAssertThat("destroy index --if-exists --region=" + REGION_1).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status",
            "Destroyed all indexes on region REGION1", "Destroyed all indexes on region REGION1");
  }

  @Test
  public void testDestroyOneIndexOnRegion() throws Exception {
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --region=" + REGION_1)
        .statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder("Status",
            "Destroyed index INDEX1 on region REGION1", "Destroyed index INDEX1 on region REGION1");

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(1);
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getQueryService().getIndexes().size()).isEqualTo(1);
    });

    // Check idempotency
    gfsh.executeAndAssertThat(
        "destroy index --if-exists --name=" + INDEX_1 + " --region=" + REGION_1).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status", "Index INDEX1 not found - skipped",
            "Index INDEX1 not found - skipped");

    // Check error result is correct
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --region=" + REGION_1)
        .statusIsError().tableHasColumnWithExactValuesInAnyOrder("Status",
            "ERROR: Index named \"INDEX1\" not found", "ERROR: Index named \"INDEX1\" not found");
  }

  @Test
  public void testDestroyAllIndexesOnOneMember() throws Exception {
    gfsh.executeAndAssertThat("destroy index --member=server-1").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status", "Destroyed all indexes");

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
        .tableHasColumnWithExactValuesInAnyOrder("Status", "Destroyed all indexes");
  }

  @Test
  public void testDestroyOneIndexOnOneMember() throws Exception {
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --member=server-1")
        .statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status", "Destroyed index INDEX1");

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
        .statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status", "Index INDEX1 not found - skipped");

    // Check error result is correct
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --member=server-1")
        .statusIsError().tableHasColumnWithExactValuesInAnyOrder("Status",
            "ERROR: Index named \"INDEX1\" not found");
  }

  @Test
  public void testVariableResultDestroyOneIndex() throws Exception {
    gfsh.executeAndAssertThat("destroy index --name=" + INDEX_1 + " --member=server-1")
        .statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Status", "Destroyed index INDEX1");

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
        .tableHasColumnWithExactValuesInAnyOrder("Status", "Destroyed index INDEX1",
            "ERROR: Index named \"INDEX1\" not found");

  }
}
