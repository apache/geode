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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.AbstractRegion.PoolFinder;
import org.apache.geode.internal.cache.LocalRegion.RegionMapConstructor;
import org.apache.geode.internal.cache.LocalRegion.ServerRegionProxyConstructor;
import org.apache.geode.internal.cache.control.InternalResourceManager;

public class LocalRegionTest {

  private EntryEventFactory entryEventFactory;
  private InternalCache cache;
  private InternalDataView internalDataView;
  private InternalDistributedSystem internalDistributedSystem;
  private InternalRegionArguments internalRegionArguments;
  private PoolFinder poolFinder;
  private RegionAttributes regionAttributes;
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

    Region region =
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

    Region region =
        new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock());

    region.destroyRegion();

    verify(cachePerfStats, never()).close();
  }
}
