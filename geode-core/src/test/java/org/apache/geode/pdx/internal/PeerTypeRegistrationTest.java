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
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.distributed.DistributedLockService;
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
  private final InternalRegionFactory factory = mock(InternalRegionFactory.class);

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
  }

  @Test
  public void getLocalSizeThrowsWhenNotInitialized() {
    PeerTypeRegistration peerTypeRegistration = new PeerTypeRegistration(internalCache);
    assertThatThrownBy(peerTypeRegistration::getLocalSize)
        .isInstanceOf(PdxInitializationException.class);
  }

  @Test
  public void shouldReloadFromRegionWillNotInvokeInTX() {
    TXStateProxy txStateProxy = mock(TXStateProxy.class);
    when(txStateProxy.getTxMgr()).thenReturn(txManager);
    when(txManager.internalSuspend()).thenReturn(txStateProxy);

    PeerTypeRegistration peerTypeRegistration = new PeerTypeRegistration(internalCache);
    peerTypeRegistration.initialize();
    peerTypeRegistration.shouldReload();

    verify(txManager, times(1)).internalSuspend();
    verify(txManager, times(1)).internalResume(txStateProxy);
  }

  @Test
  public void getLocalSizeReturnsValueAfterInitialized() {
    when(region.size()).thenReturn(1);
    PeerTypeRegistration peerTypeRegistration = new PeerTypeRegistration(internalCache);
    peerTypeRegistration.initialize();

    assertThat(peerTypeRegistration.getLocalSize()).isEqualTo(1);
  }

  @Test
  public void typeRegistryCacheIsFlushedWhenNotNull() {
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    when(internalCache.getPdxRegistry()).thenReturn(typeRegistry);

    PeerTypeRegistration peerTypeRegistration = new PeerTypeRegistration(internalCache);
    peerTypeRegistration.initialize();

    verify(typeRegistry).flushCache();
  }

  @Test
  public void pdxPersistenceIsSetWithUserDefinedDiskStore() throws NoSuchFieldException {
    PeerTypeRegistration peerTypeRegistration = spy(new PeerTypeRegistration(internalCache));
    doReturn(factory).when(internalCache).createRegionFactory();
    when(internalCache.getPdxPersistent()).thenReturn(true);
    CacheConfig config = mock(CacheConfig.class);
    when(internalCache.getCacheConfig()).thenReturn(config);

    FieldSetter.setField(config, config.getClass().getField("pdxDiskStoreUserSet"), true);
    final String diskStoreName = "userDiskStore";
    when(internalCache.getPdxDiskStore()).thenReturn(diskStoreName);

    peerTypeRegistration.initialize();

    verify(factory).setDiskStoreName(diskStoreName);
    verify(factory).setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
  }

  @Test
  public void pdxPersistenceIsSetWithDefaultDiskStore() {
    PeerTypeRegistration peerTypeRegistration = spy(new PeerTypeRegistration(internalCache));
    doReturn(factory).when(internalCache).createRegionFactory();
    when(internalCache.getPdxPersistent()).thenReturn(true);
    CacheConfig config = mock(CacheConfig.class);
    when(internalCache.getCacheConfig()).thenReturn(config);

    DiskStoreImpl defaultDiskStore = mock(DiskStoreImpl.class);
    when(internalCache.getOrCreateDefaultDiskStore()).thenReturn(defaultDiskStore);
    final String diskStoreName = "defaultDiskStoreName";
    when(defaultDiskStore.getName()).thenReturn(diskStoreName);

    peerTypeRegistration.initialize();

    verify(factory).setDiskStoreName(diskStoreName);
    verify(factory).setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
  }

  @Test
  public void pdxPersistenceIsNotSetWhenPdxPersistenceIsFalse() {
    PeerTypeRegistration peerTypeRegistration = spy(new PeerTypeRegistration(internalCache));
    doReturn(factory).when(internalCache).createRegionFactory();

    peerTypeRegistration.initialize();

    verify(factory, times(0)).setDiskStoreName(any(String.class));
    verify(factory).setDataPolicy(DataPolicy.REPLICATE);
  }

  @Test(expected = PdxInitializationException.class)
  public void pdxInitializationExceptionIsThrownInInitializeWhenRegionCreationFails()
      throws IOException, ClassNotFoundException {
    doThrow(new RegionExistsException(region)).when(factory).create(any());
    PeerTypeRegistration peerTypeRegistration = new PeerTypeRegistration(internalCache);
    peerTypeRegistration.initialize();
  }

  @Test
  public void buildLocalMapsFromRegionIsNotCalledIfLocalMapsAreUpToDate() {
    PeerTypeRegistration peerTypeRegistration = spy(new PeerTypeRegistration(internalCache));

    DistributedLockService dlockService = mock(DistributedLockService.class);
    doReturn(dlockService).when(peerTypeRegistration).getLockService();

    when(dlockService.lock(anyString(), anyLong(), anyLong())).thenReturn(true);

    when(internalCache.getDistributionManager()).thenReturn(distributionManager);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(distributionManager.getSystem()).thenReturn(internalDistributedSystem);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(distributionManager.getCancelCriterion()).thenReturn(cancelCriterion);

    peerTypeRegistration.initialize();

    PdxType newType = mock(PdxType.class);
    peerTypeRegistration.defineType(newType);

    verify(peerTypeRegistration, times(0)).buildReverseMapsFromRegion();
  }
}
