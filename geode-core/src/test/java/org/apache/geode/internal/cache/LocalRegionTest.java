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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.internal.QueryConfigurationService;
import org.apache.geode.cache.query.internal.index.IndexCreationData;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.PartitionedIndex;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.AbstractRegion.PoolFinder;
import org.apache.geode.internal.cache.LocalRegion.RegionMapConstructor;
import org.apache.geode.internal.cache.LocalRegion.ServerRegionProxyConstructor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.util.concurrent.FutureResult;
import org.apache.geode.pdx.PdxInstance;

public class LocalRegionTest {
  private EntryEventFactory entryEventFactory;
  private InternalCache cache;
  private InternalDataView internalDataView;
  private InternalDistributedSystem internalDistributedSystem;
  private InternalRegionArguments internalRegionArguments;
  private PoolFinder poolFinder;
  private RegionAttributes<?, ?> regionAttributes;
  private RegionMapConstructor regionMapConstructor;
  private Function<LocalRegion, RegionPerfStats> regionPerfStatsFactory;
  private ServerRegionProxyConstructor serverRegionProxyConstructor;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    entryEventFactory = mock(EntryEventFactory.class);
    cache = mock(InternalCache.class);
    internalDataView = mock(InternalDataView.class);
    internalDistributedSystem = mock(InternalDistributedSystem.class);
    internalRegionArguments = mock(InternalRegionArguments.class);
    poolFinder = mock(PoolFinder.class);
    regionAttributes = mock(RegionAttributes.class);
    regionMapConstructor = mock(RegionMapConstructor.class);
    regionPerfStatsFactory = localRegion -> {
      localRegion.getLocalSize();
      return mock(RegionPerfStats.class);
    };
    serverRegionProxyConstructor = mock(ServerRegionProxyConstructor.class);

    DiskWriteAttributes diskWriteAttributes = mock(DiskWriteAttributes.class);
    EvictionAttributes evictionAttributes = mock(EvictionAttributes.class);
    ExpirationAttributes expirationAttributes = mock(ExpirationAttributes.class);

    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(evictionAttributes.getAction()).thenReturn(EvictionAction.NONE);
    when(internalDistributedSystem.getClock()).thenReturn(mock(DSClock.class));
    when(regionAttributes.getDataPolicy()).thenReturn(DataPolicy.DEFAULT);
    when(regionAttributes.getDiskWriteAttributes()).thenReturn(diskWriteAttributes);
    when(regionAttributes.getEntryIdleTimeout()).thenReturn(expirationAttributes);
    when(regionAttributes.getEntryTimeToLive()).thenReturn(expirationAttributes);
    when(regionAttributes.getEvictionAttributes()).thenReturn(evictionAttributes);
    when(regionAttributes.getRegionIdleTimeout()).thenReturn(expirationAttributes);
    when(regionAttributes.getRegionTimeToLive()).thenReturn(expirationAttributes);
    when(regionMapConstructor.create(any(), any(), any())).thenReturn(mock(RegionMap.class));
  }

  @Test
  public void getLocalSizeDoesNotThrowNullPointerExceptionDuringConstruction() {
    Function<LocalRegion, RegionPerfStats> regionPerfStatsFactory = localRegion -> {
      localRegion.getLocalSize();
      return mock(RegionPerfStats.class);
    };

    assertThatCode(
        () -> new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock()))
                .doesNotThrowAnyException();
  }

  @Test
  public void destroyRegionClosesCachePerfStatsIfHasOwnStatsIsTrue() {
    CachePerfStats cachePerfStats = mock(CachePerfStats.class);
    HasCachePerfStats hasCachePerfStats = mock(HasCachePerfStats.class);
    InternalRegionArguments internalRegionArguments = mock(InternalRegionArguments.class);

    when(cache.getCancelCriterion())
        .thenReturn(mock(CancelCriterion.class));
    when(cache.getDistributedSystem())
        .thenReturn(internalDistributedSystem);
    when(cache.getInternalResourceManager(eq(false)))
        .thenReturn(mock(InternalResourceManager.class));
    when(cache.getTXMgr())
        .thenReturn(mock(TXManagerImpl.class));
    when(hasCachePerfStats.getCachePerfStats())
        .thenReturn(cachePerfStats);
    when(internalDistributedSystem.getDistributionManager())
        .thenReturn(mock(DistributionManager.class));
    when(internalDistributedSystem.getDistributedMember())
        .thenReturn(mock(InternalDistributedMember.class));
    when(internalRegionArguments.getCachePerfStatsHolder())
        .thenReturn(hasCachePerfStats);
    when(regionAttributes.getMembershipAttributes())
        .thenReturn(mock(MembershipAttributes.class));

    when(hasCachePerfStats.hasOwnStats())
        .thenReturn(true);

    Region<?, ?> region =
        new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock());

    region.destroyRegion();

    verify(cachePerfStats).close();
  }

  @Test
  public void destroyRegionDoesNotCloseCachePerfStatsIfHasOwnStatsIsFalse() {
    CachePerfStats cachePerfStats = mock(CachePerfStats.class);
    HasCachePerfStats hasCachePerfStats = mock(HasCachePerfStats.class);
    InternalRegionArguments internalRegionArguments = mock(InternalRegionArguments.class);

    when(cache.getCancelCriterion())
        .thenReturn(mock(CancelCriterion.class));
    when(cache.getDistributedSystem())
        .thenReturn(internalDistributedSystem);
    when(cache.getInternalResourceManager(eq(false)))
        .thenReturn(mock(InternalResourceManager.class));
    when(cache.getTXMgr())
        .thenReturn(mock(TXManagerImpl.class));
    when(hasCachePerfStats.getCachePerfStats())
        .thenReturn(cachePerfStats);
    when(internalDistributedSystem.getDistributionManager())
        .thenReturn(mock(DistributionManager.class));
    when(internalDistributedSystem.getDistributedMember())
        .thenReturn(mock(InternalDistributedMember.class));
    when(internalRegionArguments.getCachePerfStatsHolder())
        .thenReturn(hasCachePerfStats);
    when(regionAttributes.getMembershipAttributes())
        .thenReturn(mock(MembershipAttributes.class));

    when(hasCachePerfStats.hasOwnStats())
        .thenReturn(false);

    Region<?, ?> region =
        new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock());

    region.destroyRegion();

    verify(cachePerfStats, never()).close();
  }

  @Test
  public void getAllShouldNotThrowExceptionWhenEntryIsLocallyDeletedBetweenFetches() {
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    LocalRegion region =
        spy(new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock()));
    when(region.hasServerProxy()).thenReturn(true);

    @SuppressWarnings("unchecked")
    Region.Entry<String, String> mockEntryKey1 = mock(Region.Entry.class);
    when(mockEntryKey1.getValue()).thenThrow(new EntryDestroyedException("Mock Exception"));
    doReturn(mockEntryKey1).when(region).accessEntry("key1", true);
    when(region.getServerProxy()).thenReturn(mock(ServerRegionProxy.class));
    when(region.getServerProxy().getAll(any(), any())).thenReturn(new VersionedObjectList());

    @SuppressWarnings("unchecked")
    Region.Entry<String, String> mockEntryKey2 = mock(Region.Entry.class);
    when(mockEntryKey2.getValue()).thenReturn("value2");
    doReturn(mockEntryKey2).when(region).accessEntry("key2", true);

    @SuppressWarnings("unchecked")
    Map<String, String> result = region.getAll(Arrays.asList("key1", "key2"));
    assertThat(result.get("key1")).isNull();
    assertThat(result.get("key2")).isEqualTo("value2");
  }

  @Test
  public void initializeStatsInvokesDiskRegionStatsMethods() {
    LocalRegion region =
        spy(new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock()));

    // Mock DiskRegion and DiskRegionStats
    DiskRegion dr = mock(DiskRegion.class);
    when(region.getDiskRegion()).thenReturn(dr);
    DiskRegionStats drs = mock(DiskRegionStats.class);
    when(dr.getStats()).thenReturn(drs);

    // Invoke initializeStats
    int numEntriesInVM = 100;
    long numOverflowOnDisk = 200l;
    long numOverflowBytesOnDisk = 300l;
    region.initializeStats(numEntriesInVM, numOverflowOnDisk, numOverflowBytesOnDisk);

    // Verify the DiskRegionStats methods were invoked
    verify(drs).incNumEntriesInVM(numEntriesInVM);
    verify(drs).incNumOverflowOnDisk(numOverflowOnDisk);
    verify(drs).incNumOverflowBytesOnDisk(numOverflowBytesOnDisk);
  }

  @Test
  public void forPdxInstanceByPassTheFutureInLocalRegionOptimizedGetObject() {
    LocalRegion region =
        spy(new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock()));
    KeyInfo keyInfo = mock(KeyInfo.class);
    Object key = new Object();
    Object result = new Object();
    when(keyInfo.getKey()).thenReturn(key);
    FutureResult thisFuture = new FutureResult(mock(CancelCriterion.class));
    thisFuture.set(new Object[] {result, mock(VersionTag.class)});
    region.getGetFutures().put(key, thisFuture);
    // For non-PdxInstance, use the value in the Future
    Object object = region.optimizedGetObject(keyInfo, true, true,
        new Object(), true, true,
        mock(ClientProxyMembershipID.class), mock(EntryEventImpl.class),
        true);
    assertThat(object).isSameAs(result);

    // For PdxInstance, return a new reference by getObject(), bypassing the Future
    result = mock(PdxInstance.class);
    thisFuture.set(new Object[] {result, mock(VersionTag.class)});
    Object newResult = new Object();
    Object localValue = new Object();
    ClientProxyMembershipID requestingClient = mock(ClientProxyMembershipID.class);
    EntryEventImpl clientEvent = mock(EntryEventImpl.class);
    when(region.getObject(keyInfo, true, true,
        localValue, true, true,
        requestingClient, clientEvent,
        true)).thenReturn(newResult);
    object = region.optimizedGetObject(keyInfo, true, true,
        localValue, true, true,
        requestingClient, clientEvent,
        true);
    assertThat(object).isNotSameAs(result);
    assertThat(object).isSameAs(newResult);
  }

  @Test
  public void verifyBasicBridgePutSetsVersionTagOnClientEventIfConcurrencyConflictAndPossibleDuplicate() {
    // Create the region
    LocalRegion region =
        spy(new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock()));

    // Create the client event
    EventIDHolder clientEvent = new EventIDHolder(new EventID(new byte[] {1}, 1, 1));
    clientEvent.setPossibleDuplicate(true);

    // Make assertions of the initial client event
    assertThat(clientEvent.isPossibleDuplicate()).isTrue();
    assertThat(clientEvent.isConcurrencyConflict()).isFalse();
    assertThat(clientEvent.getVersionTag()).isNull();

    // Create the mock EntryEventImpl
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.isConcurrencyConflict()).thenReturn(true);
    doNothing().when(event).isConcurrencyConflict(true);
    VersionTag<?> tag = mock(VersionTag.class);
    when(event.getVersionTag()).thenReturn(tag);
    when(entryEventFactory.create(eq(region), eq(Operation.UPDATE), eq(0), eq(null), eq(null),
        eq(false), any(), eq(true), eq(clientEvent.getEventId()))).thenReturn(event);

    // Cause a ConcurrentModificationException to be thrown when basicUpdate is called
    when(region.basicUpdate(event, false, false, 0L, false))
        .thenThrow(ConcurrentCacheModificationException.class);

    // Invoke basicBridgePut
    boolean result = region.basicBridgePut(0, new byte[1], null, true,
        null, mock(ClientProxyMembershipID.class), clientEvent, true);

    // Make assertions of basicBridgePut
    assertThat(result).isFalse();
    verify(event).isConcurrencyConflict(true);

    // Make assertions of the output clientEvent
    assertThat(clientEvent.isPossibleDuplicate()).isTrue();
    assertThat(clientEvent.isConcurrencyConflict()).isTrue();
    assertThat(clientEvent.getVersionTag()).isEqualTo(tag);
  }

  @Test
  public void createOQLIndexOnPartitionedRegionAddsIndexesToSets() throws Exception {
    LocalRegion region =
        new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock());
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(cache.getService(any())).thenReturn(mock(QueryConfigurationService.class));
    IndexManager indexManager = mock(IndexManager.class);
    region.setIndexManager(indexManager);
    Set<Index> indexes = new HashSet<>();
    Set<Index> prIndexes = new HashSet<>();
    Index index = mock(Index.class);
    PartitionedIndex prIndex = mock(PartitionedIndex.class);
    IndexCreationData indexCreationData = mock(IndexCreationData.class);
    when(indexCreationData.getPartitionedIndex()).thenReturn(prIndex);
    when(indexManager.createIndex(eq(null), eq(null), eq(null), eq(null), eq(null), any(),
        eq(prIndex), eq(true))).thenReturn(index);

    region.createOQLIndexOnPartitionedRegion(internalRegionArguments, false, indexes, prIndexes,
        indexCreationData);

    assertThat(indexes).contains(index);
    assertThat(prIndexes).contains(prIndex);
  }

  @Test
  public void createOQLIndexOnPartitionedRegionThrowsIfIndexManagerIsNullDueToCacheClosed() {
    LocalRegion region =
        spy(new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock()));
    CacheClosedException exception = new CacheClosedException();
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    doThrow(exception).when(cancelCriterion).checkCancelInProgress(null);
    doReturn(true).when(region).isDestroyed();

    assertThatThrownBy(() -> region.createOQLIndexOnPartitionedRegion(internalRegionArguments,
        false, new HashSet<>(), new HashSet<>(), mock(IndexCreationData.class)))
            .isEqualTo(exception);
  }

  @Test
  public void setIndexManagerChecksReadiness() {
    LocalRegion region =
        spy(new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock()));
    doNothing().when(region).checkRegionDestroyed(true);

    region.setIndexManager(mock(IndexManager.class));

    verify(region).checkReadiness();
    verify(region).checkRegionDestroyed(true);
  }
}
