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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheConfig.AsyncEventQueue;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.JdbcAsyncWriter;
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
  private final Object[] arguments = new Object[2];
  private CacheConfig cacheConfig;
  RegionConfig matchingRegion;
  RegionAttributesType matchingRegionAttributes;

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
    List<RegionAttributesType> attributesList = new ArrayList<>();
    matchingRegionAttributes = mock(RegionAttributesType.class);
    when(matchingRegionAttributes.getDataPolicy()).thenReturn(RegionAttributesDataPolicy.REPLICATE);
    attributesList.add(matchingRegionAttributes);
    when(matchingRegion.getRegionAttributes()).thenReturn(attributesList);

    arguments[0] = mapping;
    arguments[1] = false;
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
        tableName, pdxClass, false);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    Object[] results = (Object[]) result.getConfigObject();
    RegionMapping regionMapping = (RegionMapping) results[0];
    boolean synchronous = (boolean) results[1];
    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo(regionName);
    assertThat(regionMapping.getDataSourceName()).isEqualTo(dataSourceName);
    assertThat(regionMapping.getTableName()).isEqualTo(tableName);
    assertThat(regionMapping.getPdxName()).isEqualTo(pdxClass);
    assertThat(synchronous).isFalse();
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenFunctionResultIsEmpty() {
    setupRequiredPreconditions();
    results.clear();

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, false);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenClusterConfigIsDisabled() {
    results.add(successFunctionResult);
    doReturn(null).when(createRegionMappingCommand).getConfigurationPersistenceService();

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, false);

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
        tableName, pdxClass, false);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString())
        .contains("A region named " + regionName + " must already exist.");
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenRegionMappingExists() {
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
    when(loaderDeclarable.getClassName()).thenReturn(null);
    when(loaderAttribute.getCacheLoader()).thenReturn(loaderDeclarable);
    attributes.add(loaderAttribute);
    when(matchingRegion.getRegionAttributes()).thenReturn(attributes);
    List<CacheElement> customList = new ArrayList<>();
    RegionMapping existingMapping = mock(RegionMapping.class);
    customList.add(existingMapping);
    when(matchingRegion.getCustomRegionElements()).thenReturn(customList);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, false);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString()).contains("A jdbc-mapping for " + regionName + " already exists.");
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
        tableName, pdxClass, false);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString()).contains("The existing region " + regionName
        + " must not already have a cache-loader, but it has MyCacheLoaderClass");
  }

  @Test
  public void createMappingWithSynchronousReturnsStatusERRORWhenClusterConfigRegionHasWriter() {
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
    RegionAttributesType writerAttribute = mock(RegionAttributesType.class);
    DeclarableType writerDeclarable = mock(DeclarableType.class);
    when(writerDeclarable.getClassName()).thenReturn("MyCacheWriterClass");
    when(writerAttribute.getCacheWriter()).thenReturn(writerDeclarable);
    attributes.add(writerAttribute);
    when(matchingRegion.getRegionAttributes()).thenReturn(attributes);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, true);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString()).contains("The existing region " + regionName
        + " must not already have a cache-writer, but it has MyCacheWriterClass");
  }

  @Test
  public void createMappingWithSynchronousReturnsStatusOKWhenAsycnEventQueueAlreadyExists() {
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
    when(loaderAttribute.getCacheLoader()).thenReturn(null);
    attributes.add(loaderAttribute);
    when(matchingRegion.getRegionAttributes()).thenReturn(attributes);
    List<AsyncEventQueue> asyncEventQueues = new ArrayList<>();
    AsyncEventQueue matchingQueue = mock(AsyncEventQueue.class);
    String queueName = createRegionMappingCommand.createAsyncEventQueueName(regionName);
    when(matchingQueue.getId()).thenReturn(queueName);
    asyncEventQueues.add(matchingQueue);
    when(cacheConfig.getAsyncEventQueues()).thenReturn(asyncEventQueues);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, true);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
  }


  @Test
  public void createsMappingReturnsStatusERRORWhenAsycnEventQueueAlreadyExists() {
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
    when(loaderAttribute.getCacheLoader()).thenReturn(null);
    attributes.add(loaderAttribute);
    when(matchingRegion.getRegionAttributes()).thenReturn(attributes);
    List<AsyncEventQueue> asyncEventQueues = new ArrayList<>();
    AsyncEventQueue matchingQueue = mock(AsyncEventQueue.class);
    String queueName = createRegionMappingCommand.createAsyncEventQueueName(regionName);
    when(matchingQueue.getId()).thenReturn(queueName);
    asyncEventQueues.add(matchingQueue);
    when(cacheConfig.getAsyncEventQueues()).thenReturn(asyncEventQueues);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, false);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString())
        .contains("An async-event-queue named " + queueName + " must not already exist.");
  }

  @Test
  public void updateClusterConfigWithNoRegionsDoesNotThrowException() {
    when(cacheConfig.getRegions()).thenReturn(Collections.emptyList());

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAddsMappingToRegion() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(listCacheElements.size()).isEqualTo(1);
    assertThat(listCacheElements).contains(mapping);
  }

  @Test
  public void updateClusterConfigWithOneNonMatchingRegionDoesNotAddMapping() {
    List<RegionConfig> list = new ArrayList<>();
    RegionConfig nonMatchingRegion = mock(RegionConfig.class);
    when(nonMatchingRegion.getName()).thenReturn("nonMatchingRegion");
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(nonMatchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(nonMatchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(listCacheElements).isEmpty();
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionCreatesAsyncEventQueue() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(queueList.size()).isEqualTo(1);
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    AsyncEventQueue createdQueue = queueList.get(0);
    assertThat(createdQueue.getId()).isEqualTo(queueName);
    assertThat(createdQueue.isParallel()).isFalse();
    assertThat(createdQueue.getAsyncEventListener().getClassName())
        .isEqualTo(JdbcAsyncWriter.class.getName());
  }

  @Test
  public void updateClusterConfigWithOneMatchingPartitionedRegionCreatesParallelAsyncEventQueue() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getDataPolicy())
        .thenReturn(RegionAttributesDataPolicy.PARTITION);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(queueList.get(0).isParallel()).isTrue();
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionCallsSetCacheLoader() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    verify(matchingRegionAttributes).setCacheLoader(any());
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndNullQueuesAddsAsyncEventQueueIdToRegion() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn(null);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(matchingRegionAttributes).setAsyncEventQueueIds(argument.capture());
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    assertThat(argument.getValue()).isEqualTo(queueName);
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndEmptyQueuesAddsAsyncEventQueueIdToRegion() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn("");

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(matchingRegionAttributes).setAsyncEventQueueIds(argument.capture());
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    assertThat(argument.getValue()).isEqualTo(queueName);
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndExistingQueuesAddsAsyncEventQueueIdToRegion() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn("q1,q2");

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(matchingRegionAttributes).setAsyncEventQueueIds(argument.capture());
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    assertThat(argument.getValue()).isEqualTo("q1,q2," + queueName);
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndQueuesContainingDuplicateDoesNotModifyAsyncEventQueueIdOnRegion() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    String existingQueues = "q1," + queueName + ",q2";
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn(existingQueues);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    verify(matchingRegionAttributes, never()).setAsyncEventQueueIds(any());
  }

  @Test
  public void updateClusterConfigWithSynchronousSetsTheCacheWriterOnTheMatchingRegion() {
    arguments[1] = true;
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    verify(matchingRegionAttributes).setCacheWriter(any());
  }

  @Test
  public void updateClusterConfigWithSynchronousAndOneMatchingRegionAndExistingQueuesDoesNotAddsAsyncEventQueueIdToRegion() {
    arguments[1] = true;
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn("q1,q2");

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    verify(matchingRegionAttributes, never()).setAsyncEventQueueIds(any());
  }

  @Test
  public void updateClusterConfigWithSynchronousAndOneMatchingRegionDoesNotCreateAsyncEventQueue() {
    arguments[1] = true;
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(queueList).isEmpty();
  }
}
