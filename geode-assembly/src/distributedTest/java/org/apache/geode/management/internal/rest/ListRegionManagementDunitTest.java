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

package org.apache.geode.management.internal.rest;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class ListRegionManagementDunitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator, server1, server2;

  private static ClusterManagementService client;

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static RegionConfig filter;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService());
    server1 = cluster.startServerVM(1, "group1", locator.getPort());
    server2 = cluster.startServerVM(2, "group2", locator.getPort());

    client =
        ClusterManagementServiceBuilder.buildWithHostAddress()
            .setHostAddress("localhost", locator.getHttpPort())
            .build();
    gfsh.connect(locator);

    // create regions
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("customers1");
    regionConfig.setGroup("group1");
    regionConfig.setType(RegionType.PARTITION);
    client.create(regionConfig);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/customers1", 1);

    // create a region that has different type on different group
    regionConfig = new RegionConfig();
    regionConfig.setName("customers2");
    regionConfig.setGroup("group1");
    regionConfig.setType(RegionType.PARTITION_PROXY);
    client.create(regionConfig);

    regionConfig = new RegionConfig();
    regionConfig.setName("customers2");
    regionConfig.setGroup("group2");
    regionConfig.setType(RegionType.PARTITION);
    client.create(regionConfig);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/customers2", 2);

    regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setType(RegionType.PARTITION);
    client.create(regionConfig);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/customers", 2);

    // create a region that belongs to multiple groups
    regionConfig = new RegionConfig();
    regionConfig.setName("customers3");
    regionConfig.setGroup("group1");
    regionConfig.setType(RegionType.PARTITION);
    client.create(regionConfig);
    regionConfig.setGroup("group2");
    client.create(regionConfig);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/customers3", 2);
  }

  @Before
  public void before() throws Exception {
    filter = new RegionConfig();
  }

  @Test
  public void listAll() throws Exception {
    // list all
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(5);
    RuntimeRegionConfig element = CacheElement.findElement(regions, "customers");
    assertThat(element.getGroup()).isNull();

    element = CacheElement.findElement(regions, "customers1");
    assertThat(element.getGroup()).isEqualTo("group1");

    RuntimeRegionConfig region = CacheElement.findElement(regions, "customers2");
    assertThat(region.getGroup()).isIn("group1", "group2");
    assertThat(region.getType()).isIn("PARTITION", "PARTITION_PROXY");

    element = CacheElement.findElement(regions, "customers3");
    assertThat(element.getGroups()).containsExactlyInAnyOrder("group1", "group2");
  }

  @Test
  public void listClusterLevel() throws Exception {
    // list cluster level only
    filter.setGroup("cluster");
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(1);
    assertThat(regions.get(0).getId()).isEqualTo("customers");
    assertThat(regions.get(0).getGroup()).isNull();
  }

  @Test
  public void testEntryCount() throws Exception {
    server1.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getCache().getRegion("/customers");
      region.put("k1", "v1");
      region.put("k2", "v2");
    });

    // wait till entry size are correctly gathered by the mbean
    locator.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(ClusterStartupRule.memberStarter.getRegionMBean("/customers")
              .getSystemRegionEntryCount()).isEqualTo(2));
    });

    filter.setName("customers");
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(1);
    RuntimeRegionConfig regionConfig = regions.get(0);
    assertThat(regionConfig.getName()).isEqualTo("customers");
    assertThat(regionConfig).isInstanceOf(RuntimeRegionConfig.class);
    assertThat(regionConfig.getEntryCount()).isEqualTo(2);
  }

  @Test
  public void listGroup1() throws Exception {
    // list group1
    filter.setGroup("group1");
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(3);
    // when filtering by group, the returned list should not have group info
    RuntimeRegionConfig region = CacheElement.findElement(regions, "customers1");
    assertThat(region.getGroup()).isEqualTo("group1");

    region = CacheElement.findElement(regions, "customers2");
    assertThat(region.getGroup()).isEqualTo("group1");

    region = CacheElement.findElement(regions, "customers3");
    assertThat(region.getGroups()).containsExactlyInAnyOrder("group1", "group2");
  }

  @Test
  public void listGroup2() throws Exception {
    // list group1
    filter.setGroup("group2");
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(2);

    RuntimeRegionConfig region = CacheElement.findElement(regions, "customers2");
    assertThat(region.getGroup()).isEqualTo("group2");

    region = CacheElement.findElement(regions, "customers3");
    assertThat(region.getGroups()).containsExactlyInAnyOrder("group1", "group2");
  }

  @Test
  public void listNonExistentGroup() throws Exception {
    // list non-existent group
    filter.setGroup("group3");
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(0);
  }

  @Test
  public void listRegionByName() throws Exception {
    filter.setName("customers");
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(1);
    assertThat(regions.get(0).getId()).isEqualTo("customers");
    assertThat(regions.get(0).getGroup()).isNull();
  }

  @Test
  public void listRegionByName1() throws Exception {
    filter.setName("customers1");
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(1);
    assertThat(regions.get(0).getId()).isEqualTo("customers1");
    assertThat(regions.get(0).getGroup()).isEqualTo("group1");
  }

  @Test
  public void listRegionByName2() throws Exception {
    filter.setName("customers2");
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(2);
    assertThat(
        regions.stream().map(CacheElement::getGroup).collect(Collectors.toList()))
            .containsExactlyInAnyOrder("group1", "group2");
    assertThat(regions.stream().map(RegionConfig.class::cast)
        .map(RegionConfig::getType)
        .collect(Collectors.toList()))
            .containsExactlyInAnyOrder("PARTITION", "PARTITION_PROXY");
  }

  @Test
  public void listRegionByName3() throws Exception {
    filter.setName("customers3");
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(1);
    assertThat(regions.get(0).getId()).isEqualTo("customers3");
    assertThat(regions.get(0).getGroups()).containsExactlyInAnyOrder("group1", "group2");
  }

  @Test
  public void listNonExistentRegion() throws Exception {
    // list non-existent region
    filter.setName("customer4");
    List<RuntimeRegionConfig> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(0);
  }
}
