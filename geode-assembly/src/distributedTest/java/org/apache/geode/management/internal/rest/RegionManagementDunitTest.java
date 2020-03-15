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

import static org.apache.geode.lang.Identifiable.find;
import static org.apache.geode.test.junit.assertions.ClusterManagementRealizationResultAssert.assertManagementResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class RegionManagementDunitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator;
  private static MemberVM server1;

  private static GeodeDevRestClient restClient;
  private static ClusterManagementService cms;

  @BeforeClass
  public static void beforeClass() {
    locator = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    server1 = cluster.startServerVM(1, "group1", locator.getPort());
    cluster.startServerVM(2, "group2", locator.getPort());
    cluster.startServerVM(3, "group2,group3", locator.getPort());

    restClient =
        new GeodeDevRestClient("/management/v1", "localhost", locator.getHttpPort(),
            false);
    cms = new ClusterManagementServiceBuilder()
        .setPort(locator.getHttpPort())
        .build();
  }

  @Test
  public void createsRegion() {
    Region regionConfig = new Region();
    regionConfig.setName("customers");
    regionConfig.setGroup("group1");
    regionConfig.setType(RegionType.REPLICATE);

    ClusterManagementRealizationResult result = cms.create(regionConfig);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMemberStatuses()).extracting(RealizationResult::getMemberName)
        .containsExactly("server-1");

    // make sure region is created
    server1.invoke(() -> verifyRegionCreated("customers", "REPLICATE"));

    // make sure region is persisted
    locator.invoke(() -> verifyRegionPersisted("customers", "REPLICATE", "group1"));
  }

  @Test
  public void createRegionWithKeyValueConstraint() {
    Region config = new Region();
    config.setName("customers2");
    config.setGroup("group1");
    config.setType(RegionType.PARTITION);
    config.setKeyConstraint("java.lang.Boolean");
    config.setValueConstraint("java.lang.Integer");
    cms.create(config);

    Region config1 = cms.get(config).getResult().getConfigurations().get(0);

    assertThat(config1.getType()).isEqualTo(RegionType.PARTITION);
    assertThat(config1.getValueConstraint()).isEqualTo("java.lang.Integer");
    assertThat(config1.getKeyConstraint()).isEqualTo("java.lang.Boolean");

    server1.invoke(() -> {
      @SuppressWarnings("unchecked")
      org.apache.geode.cache.Region<Object, Object> customers2 =
          ClusterStartupRule.getCache().getInternalRegionByPath("/customers2");
      assertThatThrownBy(() -> customers2.put("key", 2)).isInstanceOf(ClassCastException.class)
          .hasMessageContaining("does not satisfy keyConstraint");
      assertThatThrownBy(() -> customers2.put(Boolean.TRUE, "2"))
          .isInstanceOf(ClassCastException.class)
          .hasMessageContaining("does not satisfy valueConstraint");
    });
  }

  @Test
  public void createsAPartitionedRegion() throws Exception {
    String json = "{\"name\": \"orders\", \"type\": \"PARTITION\", \"group\": \"group1\"}";

    ClusterManagementRealizationResult result =
        restClient.doPostAndAssert("/regions", json)
            .hasStatusCode(201)
            .getClusterManagementRealizationResult();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMemberStatuses()).extracting(RealizationResult::getMemberName)
        .containsExactly("server-1");

    // make sure region is created
    server1.invoke(() -> verifyRegionCreated("orders", "PARTITION"));

    // make sure region is persisted
    locator.invoke(() -> verifyRegionPersisted("orders", "PARTITION", "group1"));

    // create the same region 2nd time
    result = restClient.doPostAndAssert("/regions", json)
        .hasStatusCode(409)
        .getClusterManagementRealizationResult();
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void noNameInConfig() throws Exception {
    IgnoredException.addIgnoredException("Region name is required.");
    String json = "{\"type\": \"REPLICATE\"}";

    ClusterManagementResult result =
        restClient.doPostAndAssert("/regions", json)
            .hasStatusCode(400)
            .getClusterManagementResult();

    assertThat(result.isSuccessful()).isFalse();
  }

  static void verifyRegionPersisted(String regionName, String type, String group) {
    CacheConfig cacheConfig =
        ClusterStartupRule.getLocator().getConfigurationPersistenceService()
            .getCacheConfig(group);
    RegionConfig regionConfig = find(cacheConfig.getRegions(), regionName);
    assertThat(regionConfig.getType()).isEqualTo(type);
  }

  static void verifyRegionCreated(String regionName, String type) {
    Cache cache = ClusterStartupRule.getCache();
    org.apache.geode.cache.Region<?, ?> region = cache.getRegion(regionName);
    assertThat(region).isNotNull();
    assertThat(region.getAttributes().getDataPolicy().toString()).isEqualTo(type);
  }

  @Test
  public void createSameRegionOnDisjointGroups() {
    Region regionConfig = new Region();
    regionConfig.setName("disJoint");
    regionConfig.setGroup("group1");
    regionConfig.setType(RegionType.REPLICATE);
    assertManagementResult(cms.create(regionConfig)).isSuccessful();

    regionConfig.setName("disJoint");
    regionConfig.setGroup("group2");
    regionConfig.setType(RegionType.REPLICATE);
    assertManagementResult(cms.create(regionConfig)).isSuccessful();
  }

  @Test
  public void createSameRegionOnGroupsWithCommonMember() {
    Region regionConfig = new Region();
    regionConfig.setName("commonMember");
    regionConfig.setGroup("group2");
    regionConfig.setType(RegionType.REPLICATE);
    assertManagementResult(cms.create(regionConfig)).isSuccessful();

    assertThatThrownBy(() -> cms.create(regionConfig)).hasMessageContaining("ENTITY_EXISTS")
        .hasMessageContaining("already exists in group group2");

    regionConfig.setGroup("group3");
    assertThatThrownBy(() -> cms.create(regionConfig)).hasMessageContaining("ENTITY_EXISTS")
        .hasMessageContaining("already exists on member(s) server-3.");
  }

  @Test
  public void createIncompatibleRegionOnDisjointGroups() {
    Region regionConfig = new Region();
    regionConfig.setName("incompatible");
    regionConfig.setGroup("group4");
    regionConfig.setType(RegionType.REPLICATE);
    assertManagementResult(cms.create(regionConfig)).isSuccessful();

    regionConfig.setName("incompatible");
    regionConfig.setGroup("group5");
    regionConfig.setType(RegionType.PARTITION);
    assertThatThrownBy(() -> cms.create(regionConfig)).hasMessageContaining("ILLEGAL_ARGUMENT");

    regionConfig.setName("incompatible");
    regionConfig.setGroup("group5");
    regionConfig.setType(RegionType.REPLICATE_PROXY);
    assertManagementResult(cms.create(regionConfig)).isSuccessful();

  }

  @Test
  public void createRegionWithExpiration() {
    Region region = new Region();
    String regionName = "createRegionWithExpiration";
    region.setName(regionName);
    region.setType(RegionType.REPLICATE);
    region.addExpiry(Region.ExpirationType.ENTRY_IDLE_TIME, 10000, null);
    region.addExpiry(Region.ExpirationType.ENTRY_TIME_TO_LIVE, 20000,
        Region.ExpirationAction.INVALIDATE);

    assertManagementResult(cms.create(region)).isSuccessful();

    locator.invoke(() -> {
      CacheConfig cacheConfig =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService()
              .getCacheConfig("cluster");
      RegionConfig regionConfig = find(cacheConfig.getRegions(), regionName);
      RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
      assertThat(regionAttributes.isStatisticsEnabled()).isTrue();
      assertThat(regionAttributes.getEntryTimeToLive().getTimeout()).isEqualTo("20000");
      assertThat(regionAttributes.getEntryTimeToLive().getAction()).isEqualTo("invalidate");
      assertThat(regionAttributes.getEntryTimeToLive().getCustomExpiry()).isNull();

      assertThat(regionAttributes.getEntryIdleTime().getTimeout()).isEqualTo("10000");
      assertThat(regionAttributes.getEntryIdleTime().getAction()).isEqualTo("destroy");
      assertThat(regionAttributes.getEntryIdleTime().getCustomExpiry()).isNull();

      assertThat(regionAttributes.getRegionTimeToLive()).isNull();
      assertThat(regionAttributes.getRegionIdleTime()).isNull();
    });

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      org.apache.geode.cache.Region<?, ?> actualRegion = cache.getRegion(regionName);
      RegionAttributes<?, ?> attributes = actualRegion.getAttributes();
      assertThat(attributes.getStatisticsEnabled()).isTrue();
      assertThat(attributes.getEntryIdleTimeout().getTimeout()).isEqualTo(10000);
      assertThat(attributes.getEntryIdleTimeout().getAction()).isEqualTo(ExpirationAction.DESTROY);
      assertThat(attributes.getEntryTimeToLive().getTimeout()).isEqualTo(20000);
      assertThat(attributes.getEntryTimeToLive().getAction())
          .isEqualTo(ExpirationAction.INVALIDATE);
      assertThat(attributes.getRegionIdleTimeout().getTimeout()).isEqualTo(0);
      assertThat(attributes.getRegionTimeToLive().getTimeout()).isEqualTo(0);
      assertThat(attributes.getCustomEntryIdleTimeout()).isNull();
      assertThat(attributes.getCustomEntryTimeToLive()).isNull();
    });

    Region regionResult = cms.get(region).getResult().getConfigurations().get(0);
    List<Region.Expiration> expirations = regionResult.getExpirations();
    assertThat(expirations).hasSize(2);
    assertThat(expirations.get(0).getTimeInSeconds()).isEqualTo(10000);
    assertThat(expirations.get(0).getAction()).isEqualTo(Region.ExpirationAction.DESTROY);
    assertThat(expirations.get(0).getType()).isEqualTo(Region.ExpirationType.ENTRY_IDLE_TIME);
    assertThat(expirations.get(1).getTimeInSeconds()).isEqualTo(20000);
    assertThat(expirations.get(1).getAction()).isEqualTo(Region.ExpirationAction.INVALIDATE);
    assertThat(expirations.get(1).getType()).isEqualTo(Region.ExpirationType.ENTRY_TIME_TO_LIVE);
  }

  @Test
  public void createRegionWithEviction() {
    Region region = new Region();
    String regionName = "createRegionWithEviction";
    region.setName(regionName);
    region.setType(RegionType.REPLICATE);
    Region.Eviction eviction = new Region.Eviction();
    eviction.setAction(Region.EvictionAction.OVERFLOW_TO_DISK);
    eviction.setEntryCount(100);
    region.setEviction(eviction);

    assertManagementResult(cms.create(region)).isSuccessful();

    locator.invoke(() -> {
      CacheConfig cacheConfig =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService()
              .getCacheConfig("cluster");
      RegionConfig regionConfig = find(cacheConfig.getRegions(), regionName);
      RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
      RegionAttributesType.EvictionAttributes evictionAttributes =
          regionAttributes.getEvictionAttributes();
      assertThat(evictionAttributes).isNotNull();
      assertThat(evictionAttributes.getLruEntryCount()).isNotNull();
      assertThat(evictionAttributes.getLruEntryCount().getAction()).isEqualTo(
          EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
      assertThat(evictionAttributes.getLruEntryCount().getMaximum()).isEqualTo("100");
    });

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      org.apache.geode.cache.Region<?, ?> actualRegion = cache.getRegion(regionName);
      RegionAttributes<?, ?> attributes = actualRegion.getAttributes();
      EvictionAttributes evictionAttributes = attributes.getEvictionAttributes();
      assertThat(evictionAttributes).isNotNull();
      assertThat(evictionAttributes.getAlgorithm()).isEqualTo(EvictionAlgorithm.LRU_ENTRY);
      assertThat(evictionAttributes.getAction()).isEqualTo(EvictionAction.OVERFLOW_TO_DISK);
      assertThat(evictionAttributes.getMaximum()).isEqualTo(100);

    });

    Region regionResult = cms.get(region).getResult().getConfigurations().get(0);
    Region.Eviction eviction2 = regionResult.getEviction();
    assertThat(eviction2).isNotNull();
    assertThat(eviction2.getType()).isEqualTo(Region.EvictionType.ENTRY_COUNT);
    assertThat(eviction2.getEntryCount()).isEqualTo(100);
    assertThat(eviction2.getAction()).isEqualTo(Region.EvictionAction.OVERFLOW_TO_DISK);
  }
}
