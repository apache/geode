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

import static org.apache.geode.test.junit.assertions.ClusterManagementResultAssert.assertManagementResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.List;
import java.util.Map;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationManager;
import org.apache.geode.management.internal.configuration.validators.CacheElementValidator;
import org.apache.geode.management.internal.configuration.validators.ConfigurationValidator;
import org.apache.geode.management.internal.configuration.validators.RegionConfigValidator;

public class LocatorClusterManagementServiceTest {

  private LocatorClusterManagementService service;
  private InternalCache cache;
  private ConfigurationPersistenceService persistenceService;
  private RegionConfig regionConfig;
  private ClusterManagementResult result;
  private Map<Class, ConfigurationValidator> validators = new HashMap<>();
  private Map<Class, ConfigurationManager> managers = new HashMap<>();
  private ConfigurationValidator<RegionConfig> regionValidator;
  private ConfigurationValidator<CacheElement> cacheElementValidator;
  private ConfigurationManager<RegionConfig, RuntimeRegionConfig> regionManager;

  @Before
  public void before() throws Exception {
    regionValidator = mock(RegionConfigValidator.class);
    regionManager = mock(ConfigurationManager.class);
    cacheElementValidator = mock(CacheElementValidator.class);
    validators.put(RegionConfig.class, regionValidator);
    validators.put(CacheElement.class, cacheElementValidator);
    managers.put(RegionConfig.class, regionManager);

    cache = mock(InternalCache.class);
    persistenceService = mock(ConfigurationPersistenceService.class);
    service =
        spy(new LocatorClusterManagementService(cache, persistenceService, managers, validators));
    regionConfig = new RegionConfig();
  }

  @Test
  public void persistenceIsNull() throws Exception {
    service = new LocatorClusterManagementService(cache, null);
    result = service.create(regionConfig);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage())
        .contains("Cluster configuration service needs to be enabled");
  }

  @Test
  public void validatorIsCalledCorrectly() throws Exception {
    doReturn(Collections.emptySet()).when(service).findMembers(anyString());
    assertManagementResult(service.create(regionConfig))
        .failed().hasStatusCode(ClusterManagementResult.StatusCode.ERROR)
        .containsStatusMessage("no members found");
    verify(cacheElementValidator).validate(regionConfig);
    verify(regionValidator).validate(regionConfig);
    verify(regionValidator).exists(eq(regionConfig), any());
  }

  @Test
  public void partialFailureOnMembers() throws Exception {
    List<CliFunctionResult> functionResults = new ArrayList<>();
    functionResults.add(new CliFunctionResult("member1", true, "success"));
    functionResults.add(new CliFunctionResult("member2", false, "failed"));
    doReturn(functionResults).when(service).executeAndGetFunctionResult(any(), any(), any());

    doReturn(Collections.singleton(mock(DistributedMember.class))).when(service).findMembers(any());

    when(persistenceService.getCacheConfig("cluster", true)).thenReturn(new CacheConfig());
    regionConfig.setName("test");
    result = service.create(regionConfig);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage())
        .contains("Failed to apply the update on all members");
  }

  @Test
  public void non_supportedConfigObject() throws Exception {
    MemberConfig config = new MemberConfig();
    assertThatThrownBy(() -> service.create(config)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Configuration type MemberConfig is not supported");
  }

  @Test
  public void listOneGroup() throws Exception {
    regionConfig.setGroup("cluster");
    when(persistenceService.getGroups()).thenReturn(Sets.newHashSet("cluster", "group1"));

    service.list(regionConfig, RuntimeRegionConfig.class);
    // even we are listing regions in one group, we still need to go through all the groups
    verify(persistenceService).getCacheConfig("cluster", true);
    verify(persistenceService).getCacheConfig("group1", true);
    verify(regionManager, times(2)).list(any(), any());
  }

  @Test
  public void aRegionInClusterAndGroup1() throws Exception {
    when(persistenceService.getGroups()).thenReturn(Sets.newHashSet("cluster", "group1"));
    RuntimeRegionConfig region1 = new RuntimeRegionConfig();
    region1.setName("region1");
    region1.setType("REPLICATE");
    RuntimeRegionConfig region2 = new RuntimeRegionConfig();
    region2.setName("region1");
    region2.setType("REPLICATE");

    List clusterRegions = Arrays.asList(region1);
    List group1Regions = Arrays.asList(region2);
    when(regionManager.list(any(), any())).thenReturn(clusterRegions)
        .thenReturn(group1Regions);

    // this is to make sure when 'cluster" is in one of the group, it will show
    // the cluster and the other group name
    List<RuntimeRegionConfig> results =
        service.list(new RegionConfig(), RuntimeRegionConfig.class).getResult();
    assertThat(results).hasSize(1);
    RuntimeRegionConfig result = (RuntimeRegionConfig) results.get(0);
    assertThat(result.getName()).isEqualTo("region1");
    assertThat(result.getGroups()).containsExactlyInAnyOrder("cluster", "group1");
  }
}
