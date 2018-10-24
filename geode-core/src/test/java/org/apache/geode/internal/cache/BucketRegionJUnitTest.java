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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.geode.cache.RegionAttributes;

public class BucketRegionJUnitTest extends DistributedRegionJUnitTest {

  @Override
  protected void setInternalRegionArguments(InternalRegionArguments ira) {
    // PR specific
    PartitionedRegion pr = mock(PartitionedRegion.class);
    BucketAdvisor ba = mock(BucketAdvisor.class);
    ReadWriteLock primaryMoveLock = new ReentrantReadWriteLock();
    Lock primaryMoveReadLock = primaryMoveLock.readLock();
    when(ba.getPrimaryMoveReadLock()).thenReturn(primaryMoveReadLock);
    when(ba.getProxyBucketRegion()).thenReturn(mock(ProxyBucketRegion.class));
    when(ba.isPrimary()).thenReturn(true);

    ira.setPartitionedRegion(pr).setPartitionedRegionBucketRedundancy(1).setBucketAdvisor(ba);
  }

  @Override
  protected DistributedRegion createAndDefineRegion(boolean isConcurrencyChecksEnabled,
      RegionAttributes ra, InternalRegionArguments ira, GemFireCacheImpl cache) {
    BucketRegion br = new BucketRegion("testRegion", ra, null, cache, ira);
    // it is necessary to set the event tracker to initialized, since initialize() in not being
    // called on the instantiated region
    br.getEventTracker().setInitialized();

    // since br is a real bucket region object, we need to tell mockito to monitor it
    br = spy(br);

    // doNothing().when(dm).addMembershipListener(any());
    doNothing().when(br).distributeUpdateOperation(any(), anyLong());
    doNothing().when(br).distributeDestroyOperation(any());
    doNothing().when(br).distributeInvalidateOperation(any());
    doNothing().when(br).distributeUpdateEntryVersionOperation(any());
    doNothing().when(br).checkForPrimary();
    doNothing().when(br).handleWANEvent(any());
    doReturn(false).when(br).needWriteLock(any());

    return br;
  }

  @Override
  protected void verifyDistributeUpdate(DistributedRegion region, EntryEventImpl event, int cnt) {
    assertTrue(region instanceof BucketRegion);
    BucketRegion br = (BucketRegion) region;
    br.virtualPut(event, false, false, null, false, 12345L, false);
    // verify the result
    if (cnt > 0) {
      verify(br, times(cnt)).distributeUpdateOperation(eq(event), eq(12345L));
    } else {
      verify(br, never()).distributeUpdateOperation(eq(event), eq(12345L));
    }
  }

  @Override
  protected void verifyDistributeDestroy(DistributedRegion region, EntryEventImpl event, int cnt) {
    assertTrue(region instanceof BucketRegion);
    BucketRegion br = (BucketRegion) region;
    br.basicDestroy(event, false, null);
    // verify the result
    if (cnt > 0) {
      verify(br, times(cnt)).distributeDestroyOperation(eq(event));
    } else {
      verify(br, never()).distributeDestroyOperation(eq(event));
    }
  }

  @Override
  protected void verifyDistributeInvalidate(DistributedRegion region, EntryEventImpl event,
      int cnt) {
    assertTrue(region instanceof BucketRegion);
    BucketRegion br = (BucketRegion) region;
    br.basicInvalidate(event);
    // verify the result
    if (cnt > 0) {
      verify(br, times(cnt)).distributeInvalidateOperation(eq(event));
    } else {
      verify(br, never()).distributeInvalidateOperation(eq(event));
    }
  }

  @Override
  protected void verifyDistributeUpdateEntryVersion(DistributedRegion region, EntryEventImpl event,
      int cnt) {
    assertTrue(region instanceof BucketRegion);
    BucketRegion br = (BucketRegion) region;
    br.basicUpdateEntryVersion(event);
    // verify the result
    if (cnt > 0) {
      verify(br, times(cnt)).distributeUpdateEntryVersionOperation(eq(event));
    } else {
      verify(br, never()).distributeUpdateEntryVersionOperation(eq(event));
    }
  }

}
