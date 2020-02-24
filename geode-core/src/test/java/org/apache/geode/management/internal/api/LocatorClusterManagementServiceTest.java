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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.EntityGroupInfo;
import org.apache.geode.management.api.EntityInfo;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.Member;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.configuration.mutators.CacheConfigurationManager;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationManager;
import org.apache.geode.management.internal.configuration.mutators.GatewayReceiverConfigManager;
import org.apache.geode.management.internal.configuration.mutators.RegionConfigManager;
import org.apache.geode.management.internal.configuration.validators.CommonConfigurationValidator;
import org.apache.geode.management.internal.configuration.validators.ConfigurationValidator;
import org.apache.geode.management.internal.configuration.validators.MemberValidator;
import org.apache.geode.management.internal.configuration.validators.RegionConfigValidator;
import org.apache.geode.management.internal.operation.OperationManager;
import org.apache.geode.management.internal.operation.OperationState;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.IndexInfo;
import org.apache.geode.management.runtime.MemberInformation;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.management.runtime.RuntimeRegionInfo;

public class LocatorClusterManagementServiceTest {

  private LocatorClusterManagementService service;
  private InternalCache cache;
  private InternalConfigurationPersistenceService persistenceService;
  private Region regionConfig;
  private ClusterManagementResult result;
  private Map<Class, ConfigurationValidator> validators = new HashMap<>();
  private Map<Class, ConfigurationManager> managers = new HashMap<>();
  private OperationManager executorManager;
  private ConfigurationValidator<Region> regionValidator;
  private CommonConfigurationValidator cacheElementValidator;
  private CacheConfigurationManager<Region> regionManager;
  private MemberValidator memberValidator;
  private RebalanceOperation rebalanceOperation;

  @Before
  public void before() throws Exception {
    persistenceService = spy(new InternalConfigurationPersistenceService(
        JAXBService.create(CacheConfig.class)));

    cache = mock(InternalCache.class);
    regionValidator = mock(RegionConfigValidator.class);
    doCallRealMethod().when(regionValidator).validate(eq(CacheElementOperation.DELETE), any());
    regionManager = spy(new RegionConfigManager(persistenceService));
    cacheElementValidator = spy(CommonConfigurationValidator.class);
    validators.put(Region.class, regionValidator);
    managers.put(Region.class, regionManager);
    managers.put(GatewayReceiverConfig.class, new GatewayReceiverConfigManager(null));

    memberValidator = mock(MemberValidator.class);

    Set<String> groups = new HashSet<>();
    groups.add("cluster");
    doReturn(groups).when(persistenceService).getGroups();
    doReturn(new CacheConfig()).when(persistenceService).getCacheConfig(any(), anyBoolean());
    doReturn(true).when(persistenceService).lockSharedConfiguration();
    doNothing().when(persistenceService).unlockSharedConfiguration();
    executorManager = mock(OperationManager.class);
    service =
        spy(new LocatorClusterManagementService(cache, persistenceService, managers, validators,
            memberValidator, cacheElementValidator, executorManager));

    regionConfig = new Region();
    regionConfig.setName("region1");

    rebalanceOperation = new RebalanceOperation();
  }

  @Test
  public void create_persistenceIsNull() {
    org.apache.geode.cache.Region<Object, Object> region =
        mock(org.apache.geode.cache.Region.class);
    when(cache.getRegion(any())).thenReturn(region);
    service = new LocatorClusterManagementService(cache, null);
    assertThatThrownBy(() -> service.create(regionConfig))
        .hasMessageContaining("Cluster configuration service needs to be enabled");
  }

  @Test
  public void create_validatorIsCalledCorrectly() {
    doReturn(Collections.emptySet()).when(memberValidator).findMembers(eq(false), anyString());
    doNothing().when(persistenceService).updateCacheConfig(any(), any());
    service.create(regionConfig);
    verify(cacheElementValidator).validate(CacheElementOperation.CREATE, regionConfig);
    verify(regionValidator).validate(CacheElementOperation.CREATE, regionConfig);
    verify(memberValidator).validateCreate(regionConfig, regionManager);
  }

  @Test
  public void delete_validatorIsCalledCorrectly() {
    doReturn(Collections.emptySet()).when(memberValidator).findServers(anyString());
    doReturn(new String[] {"cluster"}).when(memberValidator).findGroupsWithThisElement(
        regionConfig,
        regionManager);
    doNothing().when(persistenceService).updateCacheConfig(any(), any());
    service.delete(regionConfig);
    verify(cacheElementValidator).validate(CacheElementOperation.DELETE, regionConfig);
    verify(regionValidator).validate(CacheElementOperation.DELETE, regionConfig);
    verify(memberValidator).findGroupsWithThisElement(regionConfig, regionManager);
    memberValidator.findServers("cluster");
  }

  @Test
  public void create_partialFailureOnMembers() {
    List<RealizationResult> functionResults = new ArrayList<>();
    functionResults.add(new RealizationResult().setMemberName("member1"));
    functionResults.add(
        new RealizationResult().setMemberName("member2").setSuccess(false).setMessage("failed"));
    doReturn(functionResults).when(service).executeAndGetFunctionResult(any(), any(), any(), any());

    doReturn(Collections.singleton(mock(DistributedMember.class))).when(memberValidator)
        .findServers();

    when(persistenceService.getCacheConfig("cluster", true)).thenReturn(new CacheConfig());
    regionConfig.setName("test");
    assertThatThrownBy(() -> service.create(regionConfig))
        .hasMessageContaining("Failed to create on all members");
  }

  @Test
  public void create_succeedsOnAllMembers() {
    List<RealizationResult> functionResults = new ArrayList<>();
    functionResults.add(new RealizationResult().setMemberName("member1"));
    functionResults.add(new RealizationResult().setMemberName("member2"));
    doReturn(functionResults).when(service).executeAndGetFunctionResult(any(), any(), any(), any());

    doReturn(Collections.singleton(mock(DistributedMember.class))).when(memberValidator)
        .findServers();

    CacheConfig cacheConfig = new CacheConfig();
    when(persistenceService.getCacheConfig("cluster", true)).thenReturn(cacheConfig);
    doReturn(null).when(persistenceService).getConfiguration(any());
    org.apache.geode.cache.Region<String, Object> mockRegion =
        mock(org.apache.geode.cache.Region.class);
    doReturn(mockRegion).when(persistenceService).getConfigurationRegion();

    regionConfig.setName("test");
    regionConfig.setType(RegionType.REPLICATE);
    result = service.create(regionConfig);
    assertThat(result.isSuccessful()).isTrue();

    assertThat(cacheConfig.getRegions()).hasSize(1);
  }

  @Test
  public void create_non_supportedConfigObject() {
    Member config = new Member();
    assertThatThrownBy(() -> service.create(config)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ILLEGAL_ARGUMENT: Member is not supported.");
  }

  @Test
  public void list_oneGroup() {
    regionConfig.setGroup("cluster");
    doReturn(Sets.newHashSet("cluster", "group1")).when(persistenceService).getGroups();

    service.list(regionConfig);
    verify(persistenceService).getCacheConfig("cluster", true);
    verify(regionManager).list(any(Region.class), any(CacheConfig.class));
  }

  @Test
  public void list_oneGroupCaseInsensitive() {
    regionConfig.setGroup("CLUSTER");
    doReturn(Sets.newHashSet("cluster", "group1")).when(persistenceService).getGroups();

    service.list(regionConfig);
    verify(persistenceService).getCacheConfig("cluster", true);
    verify(regionManager).list(any(Region.class), any(CacheConfig.class));
  }

  @Test
  public void list_aRegionInMultipleGroups() {
    doReturn(Sets.newHashSet("group1", "group2")).when(persistenceService).getGroups();
    Region region1group2 = new Region();
    region1group2.setName("region1");
    region1group2.setType(RegionType.REPLICATE);
    Region region1group1 = new Region();
    region1group1.setName("region1");
    region1group1.setType(RegionType.REPLICATE);

    List<Region> group2Regions = Collections.singletonList(region1group2);
    List<Region> group1Regions = Collections.singletonList(region1group1);
    CacheConfig mockCacheConfigGroup2 = mock(CacheConfig.class);
    CacheConfig mockCacheConfigGroup1 = mock(CacheConfig.class);
    doReturn(mockCacheConfigGroup2).when(persistenceService).getCacheConfig(eq("group2"),
        anyBoolean());
    doReturn(mockCacheConfigGroup1).when(persistenceService).getCacheConfig(eq("group1"),
        anyBoolean());
    doReturn(group2Regions).when(regionManager).list(any(), same(mockCacheConfigGroup2));
    doReturn(group1Regions).when(regionManager).list(any(), same(mockCacheConfigGroup1));

    List<Region> results = service.list(new Region()).getConfigResult();
    assertThat(results).hasSize(2);
    Region result1 = results.get(0);
    assertThat(result1.getName()).isEqualTo("region1");
    Region result2 = results.get(1);
    assertThat(result2.getName()).isEqualTo("region1");
    assertThat(results).extracting(Region::getGroup).containsExactlyInAnyOrder("group1", "group2");
  }

  @Test
  public void delete_unknownRegionFails() {
    Region config = new Region();
    config.setName("unknown");
    doReturn(new String[] {}).when(memberValidator).findGroupsWithThisElement(any(), any());
    assertThatThrownBy(() -> service.delete(config))
        .isInstanceOf(ClusterManagementException.class)
        .hasMessage("ENTITY_NOT_FOUND: Region 'unknown' does not exist.");
  }

  @Test
  public void delete_usingGroupFails() {
    Region config = new Region();
    config.setName("test");
    config.setGroup("group1");
    assertThatThrownBy(() -> service.delete(config))
        .isInstanceOf(ClusterManagementException.class)
        .hasMessage("ILLEGAL_ARGUMENT: Group is an invalid option when deleting region.");
  }

  @Test
  public void delete_partialFailureOnMembers() {
    List<RealizationResult> functionResults = new ArrayList<>();
    functionResults.add(new RealizationResult().setMemberName("member1"));
    functionResults.add(
        new RealizationResult().setMemberName("member2").setSuccess(false).setMessage("failed"));
    doReturn(functionResults).when(service).executeAndGetFunctionResult(any(), any(), any(), any());

    doReturn(new String[] {"cluster"}).when(memberValidator).findGroupsWithThisElement(any(),
        any());
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(memberValidator);
    memberValidator.findServers();

    CacheConfig config = new CacheConfig();
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("test");
    config.getRegions().add(regionConfig);
    doReturn(config).when(persistenceService).getCacheConfig(eq("cluster"), anyBoolean());

    Region region = new Region();
    region.setName("test");
    result = service.delete(region);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage())
        .contains("Failed to delete on all members.");

    assertThat(config.getRegions()).hasSize(1);
  }

  @Test
  public void delete_succeedsOnAllMembers() {
    List<RealizationResult> functionResults = new ArrayList<>();
    functionResults.add(new RealizationResult().setMemberName("member1"));
    functionResults.add(new RealizationResult().setMemberName("member2"));
    doReturn(functionResults).when(service).executeAndGetFunctionResult(any(), any(), any(), any());

    doReturn(new String[] {"cluster"}).when(memberValidator).findGroupsWithThisElement(any(),
        any());
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(memberValidator);
    memberValidator.findServers();

    CacheConfig config = new CacheConfig();
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("test");
    config.getRegions().add(regionConfig);
    doReturn(config).when(persistenceService).getCacheConfig(eq("cluster"), anyBoolean());
    doReturn(null).when(persistenceService).getConfiguration(any());
    org.apache.geode.cache.Region<String, Object> mockRegion =
        mock(org.apache.geode.cache.Region.class);
    doReturn(mockRegion).when(persistenceService).getConfigurationRegion();

    Region region = new Region();
    region.setName("test");
    result = service.delete(region);
    assertThat(result.isSuccessful()).isTrue();

    assertThat(config.getRegions()).isEmpty();
  }

  @Test
  public void deleteWithNoMember() {
    // region exists in cluster configuration
    doReturn(new String[] {"cluster"}).when(memberValidator).findGroupsWithThisElement(any(),
        any());
    // no members found in any group
    doReturn(Collections.emptySet()).when(memberValidator);
    memberValidator.findServers();
    doReturn(null).when(persistenceService).getConfiguration(any());
    org.apache.geode.cache.Region<String, Object> mockRegion =
        mock(org.apache.geode.cache.Region.class);
    doReturn(mockRegion).when(persistenceService).getConfigurationRegion();

    ClusterManagementRealizationResult result = service.delete(regionConfig);
    verify(regionManager).delete(eq(regionConfig), any(CacheConfig.class));
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMemberStatuses()).hasSize(0);
    assertThat(result.getStatusMessage())
        .contains("Successfully updated configuration for cluster");
  }

  @Test
  public void startOperation() {
    final String URI = "/test/uri";
    ClusterManagementOperation<OperationResult> operation = mock(ClusterManagementOperation.class);
    when(operation.getEndpoint()).thenReturn(URI);
    when(executorManager.submit(any()))
        .thenReturn(new OperationState<>("42", operation, new Date()));
    ClusterManagementOperationResult<?, ?> result = service.start(operation);
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.ACCEPTED);
    assertThat(result.getStatusMessage()).contains("Operation started");
  }

  @Test
  public void getRebalanceWithNoOpThrows() {
    assertThatThrownBy(() -> service.get(rebalanceOperation, "123"))
        .isInstanceOf(ClusterManagementException.class);
  }

  @Test
  public void getRebalance() {
    OperationState operationState = mock(OperationState.class);
    when(executorManager.get(any())).thenReturn(operationState);
    ClusterManagementOperationResult<RebalanceOperation, RebalanceResult> result =
        service.get(rebalanceOperation, "456");
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.IN_PROGRESS);
    assertThat(result.getOperationResult()).isNull();

    when(operationState.getOperationEnd()).thenReturn(new Date());
    result = service.get(rebalanceOperation, "456");
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
  }

  @Test
  public void getRuntimeClass() {
    assertSoftly(softly -> {
      softly.assertThat(service.getRuntimeClass(Region.class)).isEqualTo(RuntimeRegionInfo.class);
      softly.assertThat(service.hasRuntimeInfo(Region.class)).isTrue();
      softly.assertThat(service.getRuntimeClass(Index.class)).isEqualTo(IndexInfo.class);
      softly.assertThat(service.hasRuntimeInfo(Index.class)).isTrue();
    });
  }

  @Test
  public void get_whenResponseHasNoConfigurationResults() {
    Region filter = new Region();
    ClusterManagementListResult<Region, RuntimeRegionInfo> result =
        mock(ClusterManagementListResult.class);
    when(result.getEntityGroupInfo()).thenReturn(emptyList());

    doReturn(result).when(service).list(same(filter));

    assertThatThrownBy(() -> service.get(filter))
        .isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");
  }

  @Test
  public void get_whenResponseHasConfigurationResults() {
    Region filter = new Region();
    ClusterManagementListResult<Region, RuntimeRegionInfo> listResult =
        mock(ClusterManagementListResult.class);

    List<EntityGroupInfo<Region, RuntimeRegionInfo>> entityGroupInfos =
        asList(mock(EntityGroupInfo.class));
    List<EntityInfo<Region, RuntimeRegionInfo>> entityInfos =
        asList(new EntityInfo<>("id", entityGroupInfos));

    when(listResult.getResult()).thenReturn(entityInfos);

    doReturn(listResult).when(service).list(same(filter));

    ClusterManagementGetResult<Region, RuntimeRegionInfo> getResult = service.get(filter);

    assertThat(getResult.getResult().getGroups()).isSameAs(entityGroupInfos);
  }

  @Test
  public void getMember_whenNoMembers() {
    Member member = new Member();
    ClusterManagementListResult<Member, MemberInformation> listResult =
        mock(ClusterManagementListResult.class);
    List<EntityGroupInfo<Member, MemberInformation>> entityGroupInfos = new ArrayList<>();
    EntityGroupInfo<Member, MemberInformation> entityGroupInfo =
        mock(EntityGroupInfo.class);
    when(entityGroupInfo.getRuntimeInfo()).thenReturn(emptyList());
    entityGroupInfos.add(entityGroupInfo);
    when(listResult.getEntityGroupInfo()).thenReturn(entityGroupInfos);

    assertThatThrownBy(() -> service.get(member))
        .isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");
  }

  @Test
  public void setResultStatus() {
    ClusterManagementRealizationResult result = new ClusterManagementRealizationResult();
    service.setResultStatus(result, Arrays.asList("group1"), Collections.EMPTY_LIST);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusMessage())
        .isEqualTo("Successfully updated configuration for group1.");

    service.setResultStatus(result, Arrays.asList("group1", "group2"), Collections.EMPTY_LIST);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusMessage())
        .isEqualTo("Successfully updated configuration for group1, group2.");

    service.setResultStatus(result, Collections.EMPTY_LIST, Arrays.asList("group1, group2"));
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage())
        .isEqualTo("Failed to update configuration for group1, group2.");

    service.setResultStatus(result, Arrays.asList("group1"), Arrays.asList("group2"));
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage()).isEqualTo(
        "Successfully updated configuration for group1. Failed to update configuration for group2.");
  }
}
