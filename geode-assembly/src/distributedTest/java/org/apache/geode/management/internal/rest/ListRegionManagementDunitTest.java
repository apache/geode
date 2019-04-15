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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
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

    client = ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort());
    gfsh.connect(locator);

    // create regions
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("customers1");
    regionConfig.setGroup("group1");
    client.create(regionConfig);

    regionConfig = new RegionConfig();
    regionConfig.setName("customers2");
    regionConfig.setGroup("group2");
    client.create(regionConfig);

    regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    client.create(regionConfig);
  }

  @Before
  public void before() throws Exception {
    filter = new RegionConfig();
  }

  @Test
  public void listAll() throws Exception {
    // list all
    List<CacheElement> regions = client.list(filter).getResult();
    assertThat(regions.stream().map(CacheElement::getId).collect(Collectors.toList()))
        .containsExactlyInAnyOrder("customers", "customers1", "customers2");
    assertThat(regions.stream().map(CacheElement::getConfigGroup).collect(Collectors.toList()))
        .containsExactlyInAnyOrder("cluster", "group1", "group2");
  }

  @Test
  public void listClusterLevel() throws Exception {
    // list cluster level only
    filter.setGroup("cluster");
    List<CacheElement> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(1);
    assertThat(regions.get(0).getId()).isEqualTo("customers");
    assertThat(regions.get(0).getConfigGroup()).isEqualTo("cluster");
    assertThat(regions.get(0).getGroup()).isNull();
  }

  @Test
  public void listGroup1() throws Exception {
    // list group1
    filter.setGroup("group1");
    List<CacheElement> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(1);
    assertThat(regions.get(0).getId()).isEqualTo("customers1");
    assertThat(regions.get(0).getConfigGroup()).isEqualTo("group1");
    assertThat(regions.get(0).getGroup()).isEqualTo("group1");
  }

  @Test
  public void listGroup2() throws Exception {
    // list group1
    filter.setGroup("group2");
    List<CacheElement> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(1);
    assertThat(regions.get(0).getId()).isEqualTo("customers2");
    assertThat(regions.get(0).getConfigGroup()).isEqualTo("group2");
    assertThat(regions.get(0).getGroup()).isEqualTo("group2");
  }

  @Test
  public void listNonExistentGroup() throws Exception {
    // list non-existent group
    filter.setGroup("group3");
    List<CacheElement> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(0);
  }

  @Test
  public void listRegionByName() throws Exception {
    filter.setName("customers");
    List<CacheElement> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(1);
    assertThat(regions.get(0).getId()).isEqualTo("customers");
    assertThat(regions.get(0).getConfigGroup()).isEqualTo("cluster");
    assertThat(regions.get(0).getGroup()).isNull();
  }

  @Test
  public void listNonExistentRegion() throws Exception {
    // list non-existent region
    filter.setName("customer3");
    List<CacheElement> regions = client.list(filter).getResult();
    assertThat(regions).hasSize(0);
  }
}
