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

package org.apache.geode.management.internal.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationManager;
import org.apache.geode.management.internal.configuration.mutators.GatewayReceiverConfigManager;
import org.apache.geode.management.internal.configuration.mutators.RegionConfigManager;
import org.apache.geode.management.internal.configuration.validators.CacheElementValidator;
import org.apache.geode.management.internal.configuration.validators.ConfigurationValidator;
import org.apache.geode.management.internal.configuration.validators.MemberValidator;
import org.apache.geode.management.internal.configuration.validators.RegionConfigValidator;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;

public class LocatorClusterManagementServiceTest {

  private LocatorClusterManagementService service;
  private InternalCache cache;
  private InternalConfigurationPersistenceService persistenceService;
  private RegionConfig regionConfig;
  private ClusterManagementResult<RegionConfig> result;
  private Map<Class, ConfigurationValidator> validators = new HashMap<>();
  private Map<Class, ConfigurationManager> managers = new HashMap<>();
  private ConfigurationValidator<RegionConfig> regionValidator;
  private CacheElementValidator cacheElementValidator;
  private ConfigurationManager<RegionConfig, RuntimeRegionConfig> regionManager;
  private MemberValidator memberValidator;

  @Before
  public void before() throws Exception {
    cache = mock(InternalCache.class);
    regionValidator = mock(RegionConfigValidator.class);
    doCallRealMethod().when(regionValidator).validate(eq(CacheElementOperation.DELETE), any());
    regionManager = spy(RegionConfigManager.class);
    cacheElementValidator = spy(CacheElementValidator.class);
    validators.put(RegionConfig.class, regionValidator);
    managers.put(RegionConfig.class, regionManager);
    managers.put(GatewayReceiverConfig.class, new GatewayReceiverConfigManager(cache));

    memberValidator = mock(MemberValidator.class);
    persistenceService = spy(InternalConfigurationPersistenceService.class);

    Set<String> groups = new HashSet<>();
    groups.add("cluster");
    doReturn(groups).when(persistenceService).getGroups();
    doReturn(new CacheConfig()).when(persistenceService).getCacheConfig(any(), anyBoolean());
    doReturn(true).when(persistenceService).lockSharedConfiguration();
    doNothing().when(persistenceService).unlockSharedConfiguration();
    service =
        spy(new LocatorClusterManagementService(persistenceService, managers, validators,
            memberValidator, cacheElementValidator));
    regionConfig = new RegionConfig();
    regionConfig.setName("region1");
  }

  @Test
  public void create_persistenceIsNull() throws Exception {
    service = new LocatorClusterManagementService(cache, null);
    result = service.create(regionConfig);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage())
        .contains("Cluster configuration service needs to be enabled");
  }

  @Test
  public void create_validatorIsCalledCorrectly() throws Exception {
    doReturn(Collections.emptySet()).when(memberValidator).findMembers(anyString());
    doNothing().when(persistenceService).updateCacheConfig(any(), any());
    service.create(regionConfig);
    verify(cacheElementValidator).validate(CacheElementOperation.CREATE, regionConfig);
    verify(regionValidator).validate(CacheElementOperation.CREATE, regionConfig);
    verify(memberValidator).validateCreate(regionConfig, regionManager);
  }

  @Test
  public void delete_validatorIsCalledCorrectly() throws Exception {
    doReturn(Collections.emptySet()).when(memberValidator).findMembers(anyString());
    doReturn(new String[] {"cluster"}).when(memberValidator).findGroupsWithThisElement(
        regionConfig.getId(),
        regionManager);
    doNothing().when(persistenceService).updateCacheConfig(any(), any());
    service.delete(regionConfig);
    verify(cacheElementValidator).validate(CacheElementOperation.DELETE, regionConfig);
    verify(regionValidator).validate(CacheElementOperation.DELETE, regionConfig);
    verify(memberValidator).findGroupsWithThisElement(regionConfig.getId(), regionManager);
    verify(memberValidator).findMembers("cluster");
  }

  @Test
  public void create_partialFailureOnMembers() throws Exception {
    List<CliFunctionResult> functionResults = new ArrayList<>();
    functionResults.add(new CliFunctionResult("member1", true, "success"));
    functionResults.add(new CliFunctionResult("member2", false, "failed"));
    doReturn(functionResults).when(service).executeAndGetFunctionResult(any(), any(), any());

    doReturn(Collections.singleton(mock(DistributedMember.class))).when(memberValidator)
        .findMembers(any());

    when(persistenceService.getCacheConfig("cluster", true)).thenReturn(new CacheConfig());
    regionConfig.setName("test");
    result = service.create(regionConfig);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage())
        .contains("Failed to apply the update on all members");
  }

  @Test
  public void create_succeedsOnAllMembers() throws Exception {
    List<CliFunctionResult> functionResults = new ArrayList<>();
    functionResults.add(new CliFunctionResult("member1", true, "success"));
    functionResults.add(new CliFunctionResult("member2", true, "failed"));
    doReturn(functionResults).when(service).executeAndGetFunctionResult(any(), any(), any());

    doReturn(Collections.singleton(mock(DistributedMember.class))).when(memberValidator)
        .findMembers(any());

    CacheConfig cacheConfig = new CacheConfig();
    when(persistenceService.getCacheConfig("cluster", true)).thenReturn(cacheConfig);
    doReturn(null).when(persistenceService).getConfiguration(any());
    Region mockRegion = mock(Region.class);
    doReturn(mockRegion).when(persistenceService).getConfigurationRegion();

    regionConfig.setName("test");
    result = service.create(regionConfig);
    assertThat(result.isSuccessful()).isTrue();

    assertThat(cacheConfig.getRegions()).hasSize(1);
  }

  @Test
  public void create_non_supportedConfigObject() throws Exception {
    MemberConfig config = new MemberConfig();
    assertThatThrownBy(() -> service.create(config)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Configuration type MemberConfig is not supported");
  }

  @Test
  public void list_oneGroup() throws Exception {
    regionConfig.setGroup("cluster");
    doReturn(Sets.newHashSet("cluster", "group1")).when(persistenceService).getGroups();

    service.list(regionConfig);
    // even we are listing regions in one group, we still need to go through all the groups
    verify(persistenceService).getCacheConfig("cluster", true);
    verify(persistenceService).getCacheConfig("group1", true);
    verify(regionManager, times(2)).list(any(), any());
  }

  @Test
  public void list_aRegionInClusterAndGroup1() throws Exception {
    doReturn(Sets.newHashSet("cluster", "group1")).when(persistenceService).getGroups();
    RuntimeRegionConfig region1 = new RuntimeRegionConfig();
    region1.setName("region1");
    region1.setType("REPLICATE");
    RuntimeRegionConfig region2 = new RuntimeRegionConfig();
    region2.setName("region1");
    region2.setType("REPLICATE");

    List clusterRegions = Arrays.asList(region1);
    List group1Regions = Arrays.asList(region2);
    doReturn(clusterRegions, group1Regions).when(regionManager).list(any(), any());

    // this is to make sure when 'cluster" is in one of the group, it will show
    // the cluster and the other group name
    List<RuntimeRegionConfig> results =
        service.list(new RegionConfig()).getResult();
    assertThat(results).hasSize(1);
    RuntimeRegionConfig result = results.get(0);
    assertThat(result.getName()).isEqualTo("region1");
    assertThat(result.getGroups()).containsExactlyInAnyOrder("cluster", "group1");
  }

  @Test
  public void delete_unknownRegionFails() {
    RegionConfig config = new RegionConfig();
    config.setName("unknown");
    doReturn(new String[] {}).when(memberValidator).findGroupsWithThisElement(any(), any());
    assertThatThrownBy(() -> service.delete(config))
        .isInstanceOf(EntityNotFoundException.class)
        .hasMessage("Cache element 'unknown' does not exist");
  }

  @Test
  public void delete_usingGroupFails() {
    RegionConfig config = new RegionConfig();
    config.setName("test");
    config.setGroup("group1");
    assertThatThrownBy(() -> service.delete(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("group is an invalid option when deleting region.");
  }

  @Test
  public void delete_partialFailureOnMembers() throws Exception {
    List<CliFunctionResult> functionResults = new ArrayList<>();
    functionResults.add(new CliFunctionResult("member1", true, "success"));
    functionResults.add(new CliFunctionResult("member2", false, "failed"));
    doReturn(functionResults).when(service).executeAndGetFunctionResult(any(), any(), any());

    doReturn(new String[] {"cluster"}).when(memberValidator).findGroupsWithThisElement(any(),
        any());
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(memberValidator)
        .findMembers(any());

    CacheConfig config = new CacheConfig();
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("test");
    config.getRegions().add(regionConfig);
    doReturn(config).when(persistenceService).getCacheConfig(eq("cluster"), anyBoolean());

    result = service.delete(regionConfig);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage())
        .contains("Failed to apply the update on all members");

    assertThat(config.getRegions()).hasSize(1);
  }

  @Test
  public void delete_succeedsOnAllMembers() throws Exception {
    List<CliFunctionResult> functionResults = new ArrayList<>();
    functionResults.add(new CliFunctionResult("member1", true, "success"));
    functionResults.add(new CliFunctionResult("member2", true, "success"));
    doReturn(functionResults).when(service).executeAndGetFunctionResult(any(), any(), any());

    doReturn(new String[] {"cluster"}).when(memberValidator).findGroupsWithThisElement(any(),
        any());
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(memberValidator)
        .findMembers(any());

    CacheConfig config = new CacheConfig();
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("test");
    config.getRegions().add(regionConfig);
    doReturn(config).when(persistenceService).getCacheConfig(eq("cluster"), anyBoolean());
    doReturn(null).when(persistenceService).getConfiguration(any());
    Region mockRegion = mock(Region.class);
    doReturn(mockRegion).when(persistenceService).getConfigurationRegion();

    result = service.delete(regionConfig);
    assertThat(result.isSuccessful()).isTrue();

    assertThat(config.getRegions()).isEmpty();
  }

  @Test
  public void deleteWithNoMember() throws Exception {
    // region exists in cluster configuration
    doReturn(new String[] {"cluster"}).when(memberValidator).findGroupsWithThisElement(any(),
        any());
    // no members found in any group
    doReturn(Collections.emptySet()).when(memberValidator).findMembers(any());
    doReturn(null).when(persistenceService).getConfiguration(any());
    Region mockRegion = mock(Region.class);
    doReturn(mockRegion).when(persistenceService).getConfigurationRegion();

    ClusterManagementResult<RegionConfig> result = service.delete(regionConfig);
    verify(regionManager).delete(eq(regionConfig), any());
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMemberStatuses()).hasSize(0);
    assertThat(result.getStatusMessage()).contains("Successfully removed config for [cluster]");
  }
}
