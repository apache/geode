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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.event.BulkOperationHolder;
import org.apache.geode.internal.cache.event.EventTracker;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionTag;

public class DistributedRegionJUnitTest extends AbstractDistributedRegionJUnitTest {

  @Override
  protected void setInternalRegionArguments(InternalRegionArguments ira) {}

  @Override
  protected DistributedRegion createAndDefineRegion(boolean isConcurrencyChecksEnabled,
      RegionAttributes ra, InternalRegionArguments ira, GemFireCacheImpl cache) {
    DistributedRegion region = new DistributedRegion("testRegion", ra, null, cache, ira);
    if (isConcurrencyChecksEnabled) {
      region.enableConcurrencyChecks();
    }

    // since it is a real region object, we need to tell mockito to monitor it
    region = spy(region);

    doNothing().when(region).distributeUpdate(any(), anyLong(), anyBoolean(), anyBoolean(), any(),
        anyBoolean());
    doNothing().when(region).distributeDestroy(any(), any());
    doNothing().when(region).distributeInvalidate(any());
    doNothing().when(region).distributeUpdateEntryVersion(any());

    return region;
  }

  @Override
  protected void verifyDistributeUpdate(DistributedRegion region, EntryEventImpl event, int cnt) {
    region.virtualPut(event, false, false, null, false, 12345L, false);
    // verify the result
    if (cnt > 0) {
      verify(region, times(cnt)).distributeUpdate(eq(event), eq(12345L), anyBoolean(), anyBoolean(),
          any(), anyBoolean());
    } else {
      verify(region, never()).distributeUpdate(eq(event), eq(12345L), anyBoolean(), anyBoolean(),
          any(), anyBoolean());
    }
  }

  @Override
  protected void verifyDistributeDestroy(DistributedRegion region, EntryEventImpl event, int cnt) {
    region.basicDestroy(event, false, null);
    // verify the result
    if (cnt > 0) {
      verify(region, times(cnt)).distributeDestroy(eq(event), any());
    } else {
      verify(region, never()).distributeDestroy(eq(event), any());
    }
  }

  @Override
  protected void verifyDistributeInvalidate(DistributedRegion region, EntryEventImpl event,
      int cnt) {
    region.basicInvalidate(event);
    // verify the result
    if (cnt > 0) {
      verify(region, times(cnt)).distributeInvalidate(eq(event));
    } else {
      verify(region, never()).distributeInvalidate(eq(event));
    }
  }

  @Override
  protected void verifyDistributeUpdateEntryVersion(DistributedRegion region, EntryEventImpl event,
      int cnt) {
    region.basicUpdateEntryVersion(event);
    // verify the result
    if (cnt > 0) {
      verify(region, times(cnt)).distributeUpdateEntryVersion(eq(event));
    } else {
      verify(region, never()).distributeUpdateEntryVersion(eq(event));
    }
  }

  @Test
  public void retriedBulkOpGetsSavedVersionTag() {
    DistributedRegion region = prepare(true, true);
    DistributedMember member = mock(DistributedMember.class);
    ClientProxyMembershipID memberId = mock(ClientProxyMembershipID.class);

    byte[] memId = {1, 2, 3};
    long threadId = 1;
    long retrySeqId = 1;
    ThreadIdentifier tid = new ThreadIdentifier(memId, threadId);
    EventID retryEventID = new EventID(memId, threadId, retrySeqId);
    boolean skipCallbacks = true;
    int size = 2;
    recordPutAllEvents(region, memId, threadId, skipCallbacks, member, memberId, size);
    EventTracker eventTracker = region.getEventTracker();

    ConcurrentMap<ThreadIdentifier, BulkOperationHolder> map =
        eventTracker.getRecordedBulkOpVersionTags();
    BulkOperationHolder holder = map.get(tid);

    EntryEventImpl retryEvent = EntryEventImpl.create(region, Operation.PUTALL_CREATE, "key1",
        "value1", null, false, member, !skipCallbacks, retryEventID);
    retryEvent.setContext(memberId);
    retryEvent.setPutAllOperation(mock(DistributedPutAllOperation.class));

    region.hasSeenEvent(retryEvent);
    assertTrue(retryEvent.getVersionTag().equals(holder.getEntryVersionTags().get(retryEventID)));
  }

  protected void recordPutAllEvents(DistributedRegion region, byte[] memId, long threadId,
      boolean skipCallbacks, DistributedMember member, ClientProxyMembershipID memberId, int size) {
    EntryEventImpl[] events = new EntryEventImpl[size];
    EventTracker eventTracker = region.getEventTracker();
    for (int i = 0; i < size; i++) {
      events[i] = EntryEventImpl.create(region, Operation.PUTALL_CREATE, "key" + i, "value" + i,
          null, false, member, !skipCallbacks, new EventID(memId, threadId, i + 1));
      events[i].setContext(memberId);
      events[i].setVersionTag(mock(VersionTag.class));
      eventTracker.recordEvent(events[i]);
    }
  }

  @Test
  public void testThatMemoryThresholdInfoRelectsStateOfRegion() {
    InternalDistributedMember internalDM = mock(InternalDistributedMember.class);
    DistributedRegion distRegion = prepare(true, false);
    distRegion.addCriticalMember(internalDM);

    MemoryThresholdInfo info = distRegion.getAtomicThresholdInfo();

    assertThat(distRegion.isMemoryThresholdReached()).isTrue();
    assertThat(distRegion.getAtomicThresholdInfo().getMembersThatReachedThreshold())
        .containsExactly(internalDM);
    assertThat(info.isMemoryThresholdReached()).isTrue();
    assertThat(info.getMembersThatReachedThreshold()).containsExactly(internalDM);
  }

  @Test
  public void testThatMemoryThresholdInfoDoesNotChangeWhenRegionChanges() {
    InternalDistributedMember internalDM = mock(InternalDistributedMember.class);
    DistributedRegion distRegion = prepare(true, false);

    MemoryThresholdInfo info = distRegion.getAtomicThresholdInfo();
    distRegion.addCriticalMember(internalDM);

    assertThat(distRegion.isMemoryThresholdReached()).isTrue();
    assertThat(distRegion.getAtomicThresholdInfo().getMembersThatReachedThreshold())
        .containsExactly(internalDM);
    assertThat(info.isMemoryThresholdReached()).isFalse();
    assertThat(info.getMembersThatReachedThreshold()).isEmpty();
  }

}
