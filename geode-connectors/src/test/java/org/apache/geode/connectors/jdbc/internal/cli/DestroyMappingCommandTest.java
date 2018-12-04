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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheConfig.AsyncEventQueue;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.JdbcWriter;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

public class DestroyMappingCommandTest {

  private InternalCache cache;
  private DestroyMappingCommand destroyRegionMappingCommand;

  private String regionName;
  private DistributionManager distributionManager;
  private Set<InternalDistributedMember> members;
  private List<CliFunctionResult> results;
  private CliFunctionResult successFunctionResult;
  private CacheConfig cacheConfig;
  RegionConfig matchingRegion;
  RegionAttributesType matchingRegionAttributes;

  @Before
  public void setup() {
    regionName = "regionName";
    cache = mock(InternalCache.class);
    distributionManager = mock(DistributionManager.class);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    members = new HashSet<>();
    members.add(mock(InternalDistributedMember.class));
    when(distributionManager.getNormalDistributionManagerIds()).thenReturn(members);
    destroyRegionMappingCommand = spy(DestroyMappingCommand.class);
    destroyRegionMappingCommand.setCache(cache);
    results = new ArrayList<>();
    successFunctionResult = mock(CliFunctionResult.class);
    when(successFunctionResult.isSuccessful()).thenReturn(true);

    doReturn(results).when(destroyRegionMappingCommand).executeAndGetFunctionResult(any(), any(),
        any());

    cacheConfig = mock(CacheConfig.class);

    matchingRegion = mock(RegionConfig.class);
    when(matchingRegion.getName()).thenReturn(regionName);
    matchingRegionAttributes = mock(RegionAttributesType.class);
    when(matchingRegionAttributes.getDataPolicy()).thenReturn(RegionAttributesDataPolicy.REPLICATE);
    when(matchingRegion.getRegionAttributes()).thenReturn(matchingRegionAttributes);
  }

  @Test
  public void destroyMappingGivenARegionNameReturnsTheNameAsTheConfigObject() {
    results.add(successFunctionResult);

    ResultModel result = destroyRegionMappingCommand.destroyMapping(regionName);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    assertThat(result.getConfigObject()).isEqualTo(regionName);
  }

  @Test
  public void destroyMappingGivenARegionPathReturnsTheNoSlashRegionNameAsTheConfigObject() {
    results.add(successFunctionResult);

    ResultModel result = destroyRegionMappingCommand.destroyMapping("/" + regionName);

    verify(destroyRegionMappingCommand, times(1)).executeAndGetFunctionResult(any(), eq(regionName),
        any());
    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    assertThat(result.getConfigObject()).isEqualTo(regionName);
  }

  @Test
  public void updateClusterConfigWithNoRegionsDoesNotThrowException() {
    when(cacheConfig.getRegions()).thenReturn(Collections.emptyList());

    boolean modified =
        destroyRegionMappingCommand.updateConfigForGroup(null, cacheConfig, regionName);

    assertThat(modified).isFalse();
  }

  @Test
  public void updateClusterConfigWithOneNonMatchingRegionDoesNotRemoveMapping() {
    List<RegionConfig> list = new ArrayList<>();
    RegionConfig nonMatchingRegion = mock(RegionConfig.class);
    when(nonMatchingRegion.getName()).thenReturn("nonMatchingRegion");
    List<CacheElement> listCacheElements = new ArrayList<>();
    RegionMapping nonMatchingMapping = mock(RegionMapping.class);
    listCacheElements.add(nonMatchingMapping);
    when(nonMatchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(nonMatchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    boolean modified =
        destroyRegionMappingCommand.updateConfigForGroup(null, cacheConfig, regionName);

    assertThat(listCacheElements).isEqualTo(Arrays.asList(nonMatchingMapping));
    assertThat(modified).isFalse();
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionDoesRemoveMapping() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    RegionMapping matchingMapping = mock(RegionMapping.class);
    listCacheElements.add(matchingMapping);
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    boolean modified =
        destroyRegionMappingCommand.updateConfigForGroup(null, cacheConfig, regionName);

    assertThat(listCacheElements).isEmpty();
    assertThat(modified).isTrue();
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndJdbcAsyncQueueRemovesTheQueue() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    RegionMapping matchingMapping = mock(RegionMapping.class);
    listCacheElements.add(matchingMapping);
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    AsyncEventQueue queue = mock(AsyncEventQueue.class);
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    when(queue.getId()).thenReturn(queueName);
    List<AsyncEventQueue> queueList = new ArrayList<>();
    queueList.add(queue);
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);

    boolean modified =
        destroyRegionMappingCommand.updateConfigForGroup(null, cacheConfig, regionName);

    assertThat(queueList).isEmpty();
    assertThat(modified).isTrue();
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndJdbcWriterRemovesTheWriter() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    RegionMapping matchingMapping = mock(RegionMapping.class);
    listCacheElements.add(matchingMapping);
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    DeclarableType cacheWriter = mock(DeclarableType.class);
    when(cacheWriter.getClassName()).thenReturn(JdbcWriter.class.getName());
    when(matchingRegionAttributes.getCacheWriter()).thenReturn(cacheWriter);

    boolean modified =
        destroyRegionMappingCommand.updateConfigForGroup(null, cacheConfig, regionName);

    verify(matchingRegionAttributes, times(1)).setCacheWriter(null);
    assertThat(modified).isTrue();
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndJdbcAsyncQueueIdRemovesTheId() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    RegionMapping matchingMapping = mock(RegionMapping.class);
    listCacheElements.add(matchingMapping);
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn(queueName);

    boolean modified =
        destroyRegionMappingCommand.updateConfigForGroup(null, cacheConfig, regionName);

    verify(matchingRegionAttributes, times(1)).setAsyncEventQueueIds("");
    assertThat(modified).isTrue();
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndJdbcAsyncQueueIdsRemovesTheId() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    RegionMapping matchingMapping = mock(RegionMapping.class);
    listCacheElements.add(matchingMapping);
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    String queueName = CreateMappingCommand.createAsyncEventQueueName(regionName);
    when(matchingRegionAttributes.getAsyncEventQueueIds())
        .thenReturn(queueName + "1," + queueName + "," + queueName + "2");

    boolean modified =
        destroyRegionMappingCommand.updateConfigForGroup(null, cacheConfig, regionName);

    verify(matchingRegionAttributes, times(1))
        .setAsyncEventQueueIds(queueName + "1," + queueName + "2");
    assertThat(modified).isTrue();
  }
}
