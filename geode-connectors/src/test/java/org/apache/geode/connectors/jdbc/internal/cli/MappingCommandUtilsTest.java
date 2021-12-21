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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheConfig.AsyncEventQueue;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;


public class MappingCommandUtilsTest {

  private final String TESTREGION = "testRegion";
  private final String GROUPNAME = "cluster";

  ConfigurationPersistenceService configurationPersistenceService;
  CacheConfig cacheConfig;
  RegionConfig regionConfig;
  RegionAttributesType regionAttributesType;

  @Before
  public void setup() {
    configurationPersistenceService = mock(ConfigurationPersistenceService.class);
    cacheConfig = mock(CacheConfig.class);
    regionConfig = mock(RegionConfig.class);
    regionAttributesType = mock(RegionAttributesType.class);
  }

  @Test
  public void getCacheConfigReturnsCorrectCacheConfig() throws PreconditionException {
    when(configurationPersistenceService.getCacheConfig(GROUPNAME)).thenReturn(cacheConfig);
    CacheConfig result =
        MappingCommandUtils.getCacheConfig(configurationPersistenceService, GROUPNAME);

    assertThat(result).isEqualTo(cacheConfig);
  }

  @Test
  public void checkForRegionReturnsCorrectRegionConfig() throws PreconditionException {
    List<RegionConfig> regionsList = new ArrayList<>();
    regionsList.add(regionConfig);
    when(regionConfig.getName()).thenReturn(TESTREGION);
    when(cacheConfig.getRegions()).thenReturn(regionsList);

    RegionConfig result = MappingCommandUtils.checkForRegion(TESTREGION, cacheConfig, GROUPNAME);

    assertThat(result).isEqualTo(regionConfig);
  }

  @Test
  public void getMappingsFromRegionConfigReturnsCorrectMappings() {

    RegionMapping validRegionMapping = mock(RegionMapping.class);
    CacheElement invalidRegionMapping = mock(CacheElement.class);
    List<CacheElement> cacheElements = new ArrayList<>();
    cacheElements.add(validRegionMapping);
    cacheElements.add(invalidRegionMapping);

    when(regionConfig.getCustomRegionElements()).thenReturn(cacheElements);

    List<RegionMapping> results =
        MappingCommandUtils.getMappingsFromRegionConfig(cacheConfig, regionConfig, GROUPNAME);

    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(validRegionMapping);
  }

  @Test
  public void createAsyncEventQueueNameProducesCorrectName() {
    String result = MappingCommandUtils.createAsyncEventQueueName(TESTREGION);

    assertThat(result).isEqualTo("JDBC#testRegion");
  }

  @Test
  public void testFindAsyncEventQueueReturnsCorrectObject() {
    AsyncEventQueue asyncEventQueue = mock(AsyncEventQueue.class);
    AsyncEventQueue wrongAsyncEventQueue = mock(AsyncEventQueue.class);
    when(asyncEventQueue.getId())
        .thenReturn(MappingCommandUtils.createAsyncEventQueueName(TESTREGION));
    when(wrongAsyncEventQueue.getId()).thenReturn("Wrong Id");

    List<AsyncEventQueue> asyncEventQueues = new ArrayList<>();
    asyncEventQueues.add(asyncEventQueue);
    asyncEventQueues.add(wrongAsyncEventQueue);

    when(regionConfig.getName()).thenReturn(TESTREGION);
    when(cacheConfig.getAsyncEventQueues()).thenReturn(asyncEventQueues);

    AsyncEventQueue result = MappingCommandUtils.findAsyncEventQueue(cacheConfig, regionConfig);

    assertThat(result).isEqualTo(asyncEventQueue);
  }

  @Test
  public void testIsMappingAsyncReturnsCorrectValue() {
    AsyncEventQueue asyncEventQueue = mock(AsyncEventQueue.class);
    AsyncEventQueue wrongAsyncEventQueue = mock(AsyncEventQueue.class);
    when(asyncEventQueue.getId())
        .thenReturn(MappingCommandUtils.createAsyncEventQueueName(TESTREGION));
    when(wrongAsyncEventQueue.getId()).thenReturn("Wrong Id");

    List<AsyncEventQueue> asyncEventQueues = new ArrayList<>();
    asyncEventQueues.add(asyncEventQueue);
    asyncEventQueues.add(wrongAsyncEventQueue);

    when(regionConfig.getName()).thenReturn(TESTREGION);
    when(cacheConfig.getAsyncEventQueues()).thenReturn(asyncEventQueues);

    boolean result = MappingCommandUtils.isMappingSynchronous(cacheConfig, regionConfig);

    assertThat(result).isEqualTo(false);
  }

  @Test
  public void testIsPartitionWithPartitionDataPolicyReturnsTrue() {
    when(regionAttributesType.getDataPolicy()).thenReturn(RegionAttributesDataPolicy.PARTITION);

    boolean result = MappingCommandUtils.isPartition(regionAttributesType);

    assertThat(result).isEqualTo(true);
  }

  @Test
  public void testIsPartitionWithPersistentPartitionDataPolicyReturnsTrue() {
    when(regionAttributesType.getDataPolicy())
        .thenReturn(RegionAttributesDataPolicy.PERSISTENT_PARTITION);

    boolean result = MappingCommandUtils.isPartition(regionAttributesType);

    assertThat(result).isEqualTo(true);
  }

  @Test
  public void testIsPartitionWithPersistentPartitionRefidReturnsTrue() {
    when(regionAttributesType.getRefid()).thenReturn(RegionShortcut.PARTITION.name());

    boolean result = MappingCommandUtils.isPartition(regionAttributesType);

    assertThat(result).isEqualTo(true);
  }

  @Test
  public void testIsPartitionWithLocalRefidReturnsFalse() {
    when(regionAttributesType.getRefid()).thenReturn(RegionShortcut.LOCAL.name());

    boolean result = MappingCommandUtils.isPartition(regionAttributesType);

    assertThat(result).isEqualTo(false);
  }

  @Test
  public void testIsPartitionWithDataPolicyAndRefidIsNullReturnsFalse() {
    when(regionAttributesType.getDataPolicy()).thenReturn(null);
    when(regionAttributesType.getRefid()).thenReturn(null);

    boolean result = MappingCommandUtils.isPartition(regionAttributesType);

    assertThat(result).isEqualTo(false);
  }

  @Test
  public void testIsPartitionWithReplicateDataPolicyReturnsFalse() {
    when(regionAttributesType.getDataPolicy()).thenReturn(RegionAttributesDataPolicy.REPLICATE);

    boolean result = MappingCommandUtils.isPartition(regionAttributesType);

    assertThat(result).isEqualTo(false);
  }
}
