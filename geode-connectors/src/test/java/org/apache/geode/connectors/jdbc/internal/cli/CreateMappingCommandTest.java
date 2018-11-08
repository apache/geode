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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

public class CreateMappingCommandTest {

  private InternalCache cache;
  private CreateMappingCommand createRegionMappingCommand;

  private String regionName;
  private String dataSourceName;
  private String tableName;
  private String pdxClass;
  private DistributionManager distributionManager;
  private Set<InternalDistributedMember> members;
  private List<CliFunctionResult> results;
  private CliFunctionResult successFunctionResult;
  private RegionMapping mapping;
  private CacheConfig cacheConfig;
  RegionConfig matchingRegion;

  @Before
  public void setup() {
    regionName = "regionName";
    dataSourceName = "connection";
    tableName = "testTable";
    pdxClass = "myPdxClass";
    cache = mock(InternalCache.class);
    distributionManager = mock(DistributionManager.class);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    members = new HashSet<>();
    members.add(mock(InternalDistributedMember.class));
    when(distributionManager.getNormalDistributionManagerIds()).thenReturn(members);
    createRegionMappingCommand = spy(CreateMappingCommand.class);
    createRegionMappingCommand.setCache(cache);
    results = new ArrayList<>();
    successFunctionResult = mock(CliFunctionResult.class);
    when(successFunctionResult.isSuccessful()).thenReturn(true);

    doReturn(results).when(createRegionMappingCommand).executeAndGetFunctionResult(any(), any(),
        any());

    mapping = mock(RegionMapping.class);
    when(mapping.getRegionName()).thenReturn(regionName);

    cacheConfig = mock(CacheConfig.class);

    matchingRegion = mock(RegionConfig.class);
    when(matchingRegion.getName()).thenReturn(regionName);

  }

  private void setupRequiredPreconditions() {
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService.getCacheConfig(null)).thenReturn(cacheConfig);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
  }

  @Test
  public void createsMappingReturnsStatusOKWhenFunctionResultSuccess() {
    setupRequiredPreconditions();
    results.add(successFunctionResult);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    RegionMapping regionMapping = (RegionMapping) result.getConfigObject();
    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo(regionName);
    assertThat(regionMapping.getDataSourceName()).isEqualTo(dataSourceName);
    assertThat(regionMapping.getTableName()).isEqualTo(tableName);
    assertThat(regionMapping.getPdxName()).isEqualTo(pdxClass);
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenFunctionResultIsEmpty() {
    setupRequiredPreconditions();
    results.clear();

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenClusterConfigIsDisabled() {
    results.add(successFunctionResult);
    doReturn(null).when(createRegionMappingCommand).getConfigurationPersistenceService();

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString()).contains("Cluster Configuration must be enabled.");
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenClusterConfigDoesNotContainRegion() {
    results.add(successFunctionResult);
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService.getCacheConfig(null)).thenReturn(cacheConfig);
    when(cacheConfig.getRegions()).thenReturn(Collections.emptyList());

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString())
        .contains("Cluster Configuration must contain a region named " + regionName);
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenClusterConfigRegionHasLoader() {
    results.add(successFunctionResult);
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService.getCacheConfig(null)).thenReturn(cacheConfig);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<RegionAttributesType> attributes = new ArrayList<>();
    RegionAttributesType loaderAttribute = mock(RegionAttributesType.class);
    DeclarableType loaderDeclarable = mock(DeclarableType.class);
    when(loaderDeclarable.getClassName()).thenReturn("MyCacheLoaderClass");
    when(loaderAttribute.getCacheLoader()).thenReturn(loaderDeclarable);
    attributes.add(loaderAttribute);
    when(matchingRegion.getRegionAttributes()).thenReturn(attributes);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString()).contains("The existing region " + regionName
        + " must not already have a cache-loader, but it has MyCacheLoaderClass");
  }

  @Test
  public void testUpdateClusterConfigWithNoRegionsAndNoExistingElement() {
    doReturn(null).when(cacheConfig).findCustomRegionElement(any(), any(), any());
    when(cacheConfig.getRegions()).thenReturn(Collections.emptyList());
    createRegionMappingCommand.updateClusterConfig(null, cacheConfig, mapping);
  }

  @Test
  public void testUpdateClusterConfigWithOneMatchingRegionAndNoExistingElement() {
    doReturn(null).when(cacheConfig).findCustomRegionElement(any(), any(), any());
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    createRegionMappingCommand.updateClusterConfig(null, cacheConfig, mapping);

    assertThat(listCacheElements.size()).isEqualTo(1);
    assertThat(listCacheElements).contains(mapping);
  }

  @Test
  public void testUpdateClusterConfigWithOneNonMatchingRegionAndNoExistingElement() {
    doReturn(null).when(cacheConfig).findCustomRegionElement(any(), any(), any());
    List<RegionConfig> list = new ArrayList<>();
    RegionConfig nonMatchingRegion = mock(RegionConfig.class);
    when(nonMatchingRegion.getName()).thenReturn("nonMatchingRegion");
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(nonMatchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(nonMatchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    createRegionMappingCommand.updateClusterConfig(null, cacheConfig, mapping);

    assertThat(listCacheElements).isEmpty();
  }

  @Test
  public void testUpdateClusterConfigWithOneMatchingRegionAndExistingElement() {
    RegionMapping existingMapping = mock(RegionMapping.class);
    doReturn(existingMapping).when(cacheConfig).findCustomRegionElement(any(), any(), any());
    RegionConfig matchingRegion = mock(RegionConfig.class);
    when(matchingRegion.getName()).thenReturn(regionName);
    List<CacheElement> listCacheElements = new ArrayList<>();
    listCacheElements.add(existingMapping);
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    createRegionMappingCommand.updateClusterConfig(null, cacheConfig, mapping);

    assertThat(listCacheElements.size()).isEqualTo(1);
    assertThat(listCacheElements).contains(mapping);
  }

  @Test
  public void testUpdateClusterConfigWithOneNonMatchingRegionAndExistingElement() {
    RegionMapping existingMapping = mock(RegionMapping.class);
    doReturn(existingMapping).when(cacheConfig).findCustomRegionElement(any(), any(), any());
    List<RegionConfig> list = new ArrayList<>();
    RegionConfig nonMatchingRegion = mock(RegionConfig.class);
    when(nonMatchingRegion.getName()).thenReturn("nonMatchingRegion");
    List<CacheElement> listCacheElements = new ArrayList<>();
    listCacheElements.add(existingMapping);
    when(nonMatchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(nonMatchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    createRegionMappingCommand.updateClusterConfig(null, cacheConfig, mapping);

    assertThat(listCacheElements.size()).isEqualTo(1);
    assertThat(listCacheElements).contains(existingMapping);
  }

}
