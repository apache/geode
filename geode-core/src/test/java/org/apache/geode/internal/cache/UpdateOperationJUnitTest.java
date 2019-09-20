/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;

public class UpdateOperationJUnitTest {

  private EntryEventImpl event;
  private UpdateOperation.UpdateMessage message;
  private DistributedRegion region;

  @Before
  public void setup() {
    event = mock(EntryEventImpl.class);
    region = mock(DistributedRegion.class);
    RegionAttributes attr = mock(RegionAttributes.class);
    CachePerfStats stats = mock(CachePerfStats.class);
    InternalCache cache = mock(InternalCache.class);

    when(event.isOriginRemote()).thenReturn(false);
    when(stats.endPut(anyLong(), eq(false))).thenReturn(0L);

    message = new UpdateOperation.UpdateMessage();
    message.event = event;
    when(region.getAttributes()).thenReturn(attr);
    when(region.isUsedForPartitionedRegionBucket()).thenReturn(false);
    when(region.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(region.getConcurrencyChecksEnabled()).thenReturn(true);
    when(region.getCachePerfStats()).thenReturn(stats);
    when(region.getCache()).thenReturn(cache);
    // when(cache.getStatisticsClock()).thenReturn(disabledClock());
    when(attr.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(event.getOperation()).thenReturn(Operation.CREATE);
  }

  /**
   * AUO's doPutOrCreate will try with create first. If it succeed, it will not try update
   * retry update
   */
  @Test
  public void createSucceedShouldNotRetryAnymore() {
    when(region.isAllEvents()).thenReturn(true);
    when(region.basicUpdate(eq(event), eq(true), eq(false), anyLong(), eq(true), eq(true)))
        .thenReturn(true);
    message.basicOperateOnRegion(event, region);
    verify(region, times(1)).basicUpdate(eq(event), eq(true), eq(false), anyLong(), eq(true),
        eq(true));
    verify(region, times(0)).basicUpdate(eq(event), eq(false), eq(true), anyLong(), eq(true),
        eq(true));
    verify(region, times(0)).basicUpdate(eq(event), eq(false), eq(false), anyLong(), eq(true),
        eq(true));
  }

  /**
   * AUO's doPutOrCreate will try update.
   * If update succeed, no more retry
   */
  @Test
  public void updateSucceedShouldNotRetryAnymore() {
    when(region.isAllEvents()).thenReturn(false);

    when(region.basicUpdate(eq(event), eq(false), eq(true), anyLong(), eq(true), eq(true)))
        .thenReturn(true);
    message.basicOperateOnRegion(event, region);
    verify(region, times(0)).basicUpdate(eq(event), eq(true), eq(false), anyLong(), eq(true),
        eq(true));
    verify(region, times(1)).basicUpdate(eq(event), eq(false), eq(true), anyLong(), eq(true),
        eq(true));
    verify(region, times(0)).basicUpdate(eq(event), eq(false), eq(false), anyLong(), eq(true),
        eq(true));
  }


}
