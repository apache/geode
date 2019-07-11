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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.AbstractRegion.PoolFinder;
import org.apache.geode.internal.cache.LocalRegion.RegionMapConstructor;
import org.apache.geode.internal.cache.LocalRegion.ServerRegionProxyConstructor;

public class LocalRegionTest {

  private RegionAttributes regionAttributes;
  private InternalCache cache;
  private InternalRegionArguments internalRegionArguments;
  private InternalDataView internalDataView;
  private RegionMapConstructor regionMapConstructor;
  private ServerRegionProxyConstructor serverRegionProxyConstructor;
  private EntryEventFactory entryEventFactory;
  private PoolFinder poolFinder;

  @Before
  public void setup() {
    cache = mock(InternalCache.class);
    entryEventFactory = mock(EntryEventFactory.class);
    internalDataView = mock(InternalDataView.class);
    internalRegionArguments = mock(InternalRegionArguments.class);
    poolFinder = mock(PoolFinder.class);
    regionAttributes = mock(RegionAttributes.class);
    regionMapConstructor = mock(RegionMapConstructor.class);
    serverRegionProxyConstructor = mock(ServerRegionProxyConstructor.class);

    DiskWriteAttributes diskWriteAttributes = mock(DiskWriteAttributes.class);
    EvictionAttributes evictionAttributes = mock(EvictionAttributes.class);
    ExpirationAttributes expirationAttributes = mock(ExpirationAttributes.class);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);

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
    Function<LocalRegion, RegionPerfStats> regionPerfStatsFactory = (localRegion) -> {
      localRegion.getLocalSize();
      return mock(RegionPerfStats.class);
    };

    assertThatCode(
        () -> new LocalRegion("region", regionAttributes, null, cache, internalRegionArguments,
            internalDataView, regionMapConstructor, serverRegionProxyConstructor, entryEventFactory,
            poolFinder, regionPerfStatsFactory, disabledClock()))
                .doesNotThrowAnyException();
  }
}
