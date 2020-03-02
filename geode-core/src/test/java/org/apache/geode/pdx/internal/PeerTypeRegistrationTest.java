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

package org.apache.geode.pdx.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.pdx.PdxInitializationException;

public class PeerTypeRegistrationTest {

  private final InternalDistributedSystem internalDistributedSystem =
      mock(InternalDistributedSystem.class);
  private final DistributionManager distributionManager = mock(DistributionManager.class);
  private final StatisticsManager statisticsManager = mock(StatisticsManager.class);
  private final StatisticsType statisticsType = mock(StatisticsType.class);
  private final Statistics statistics = mock(Statistics.class);
  private final InternalCache internalCache = mock(InternalCache.class);
  @SuppressWarnings("unchecked")
  private final Region<Object, Object> region = mock(Region.class);
  private final TXManagerImpl txManager = mock(TXManagerImpl.class);
  @SuppressWarnings("unchecked")
  private final InternalRegionFactory<Object, Object> factory = mock(InternalRegionFactory.class);
  private PeerTypeRegistration spyTypeRegistration;

  @Before
  public void setUp() throws IOException, ClassNotFoundException {
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(statisticsManager.createType(any(), any(), any())).thenReturn(statisticsType);
    when(statisticsManager.createAtomicStatistics(any(), any())).thenReturn(statistics);
    when(internalDistributedSystem.getStatisticsManager()).thenReturn(statisticsManager);
    when(internalCache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(internalCache.createInternalRegionFactory()).thenReturn(factory);
    when(factory.create(eq(PeerTypeRegistration.REGION_NAME))).thenReturn(region);
    when(region.getRegionService()).thenReturn(internalCache);
    when(internalCache.getCacheTransactionManager()).thenReturn(txManager);
    spyTypeRegistration = spy(new PeerTypeRegistration(internalCache));
  }

  @Test
  public void getDistributedSystemIdReturnsZeroWhenDistributedSystemIdIsDefaultValue() {
    when(distributionManager.getDistributedSystemId())
        .thenReturn(DistributionConfig.DEFAULT_DISTRIBUTED_SYSTEM_ID);
    assertThat(spyTypeRegistration.getDistributedSystemId(internalDistributedSystem)).isEqualTo(0);
  }

  @Test
  public void getDistributedSystemIdReturnsCorrectValueWhenDistributedSystemIdIsNonDefaultValue() {
    int expectedValue = 5;
    when(distributionManager.getDistributedSystemId()).thenReturn(expectedValue);
    assertThat(spyTypeRegistration.getDistributedSystemId(internalDistributedSystem))
        .isEqualTo(expectedValue);
  }

  @Test
  public void getLocalSizeThrowsWhenNotInitialized() {
    assertThatThrownBy(spyTypeRegistration::getLocalSize)
        .isInstanceOf(PdxInitializationException.class);
  }

  @Test
  public void getLocalSizeReturnsValueAfterInitialized() {
    when(region.size()).thenReturn(1);
    spyTypeRegistration.initialize();

    assertThat(spyTypeRegistration.getLocalSize()).isEqualTo(1);
  }

  @Test
  public void typeRegistrationCacheIsFlushedWhenTypeRegistryIsNotNull() {
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    when(internalCache.getPdxRegistry()).thenReturn(typeRegistry);

    spyTypeRegistration.initialize();

    verify(spyTypeRegistration).flushCache();
  }

  @Test
  public void pdxPersistenceIsSetWithUserDefinedDiskStore() throws NoSuchFieldException {
    doReturn(factory).when(internalCache).createRegionFactory();
    when(internalCache.getPdxPersistent()).thenReturn(true);
    CacheConfig config = mock(CacheConfig.class);
    when(internalCache.getCacheConfig()).thenReturn(config);

    FieldSetter.setField(config, config.getClass().getField("pdxDiskStoreUserSet"), true);
    final String diskStoreName = "userDiskStore";
    when(internalCache.getPdxDiskStore()).thenReturn(diskStoreName);

    spyTypeRegistration.initialize();

    verify(factory).setDiskStoreName(diskStoreName);
    verify(factory).setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
  }

  @Test
  public void pdxPersistenceIsSetWithDefaultDiskStore() {
    doReturn(factory).when(internalCache).createRegionFactory();
    when(internalCache.getPdxPersistent()).thenReturn(true);
    CacheConfig config = mock(CacheConfig.class);
    when(internalCache.getCacheConfig()).thenReturn(config);

    DiskStoreImpl defaultDiskStore = mock(DiskStoreImpl.class);
    when(internalCache.getOrCreateDefaultDiskStore()).thenReturn(defaultDiskStore);
    when(defaultDiskStore.getName()).thenReturn(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);

    spyTypeRegistration.initialize();

    verify(factory).setDiskStoreName(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
    verify(factory).setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
  }

  @Test
  public void pdxPersistenceIsNotSetWhenPdxPersistenceIsFalse() {
    doReturn(factory).when(internalCache).createRegionFactory();

    spyTypeRegistration.initialize();

    verify(factory, times(0)).setDiskStoreName(any(String.class));
    verify(factory).setDataPolicy(DataPolicy.REPLICATE);
  }

  @Test
  public void pdxInitializationExceptionIsThrownInInitializeWhenRegionCreationFails() {
    doThrow(new RegionExistsException(region)).when(factory).create(any());
    assertThatThrownBy(() -> spyTypeRegistration.initialize())
        .isInstanceOf(PdxInitializationException.class);
  }

  @Test
  public void buildLocalMapsFromRegionIsNotCalledIfLocalMapsAreUpToDate() {
    DistributedLockService dlockService = mock(DistributedLockService.class);
    doReturn(dlockService).when(spyTypeRegistration).getLockService();

    when(dlockService.lock(anyString(), anyLong(), anyLong())).thenReturn(true);

    when(internalCache.getDistributionManager()).thenReturn(distributionManager);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(distributionManager.getSystem()).thenReturn(internalDistributedSystem);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(distributionManager.getCancelCriterion()).thenReturn(cancelCriterion);

    spyTypeRegistration.initialize();

    PdxType newType = mock(PdxType.class);
    spyTypeRegistration.defineType(newType);

    verify(spyTypeRegistration, times(0)).buildLocalAndReverseMapsFromRegion();
  }

  @Test
  public void shouldReloadFromRegionWillNotInvokeInTX() {
    TXStateProxy txStateProxy = mock(TXStateProxy.class);
    when(txStateProxy.getTxMgr()).thenReturn(txManager);
    when(txManager.internalSuspend()).thenReturn(txStateProxy);

    spyTypeRegistration.initialize();
    spyTypeRegistration.shouldReload();

    verify(txManager, times(1)).internalSuspend();
    verify(txManager, times(1)).internalResume(txStateProxy);
  }
}
