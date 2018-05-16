/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License; private Version
 * 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing; private software distributed under the
 * License
 * is distributed on an "AS IS" BASIS; private WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND; private
 * either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.util.concurrent.ConcurrentMapWithReusableEntries;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AbstractRegionMapTxApplyDestroyTest {
  // parameters
  private Object key = "key";
  private TXId txId = mock(TXId.class);
  private TXRmtEvent txEvent;
  private boolean inTokenMode;
  private boolean inRI;
  private Operation op;
  private EventID eventId;
  private Object aCallbackArgument;
  private final List<EntryEventImpl> pendingCallbacks = new ArrayList<>();
  private FilterRoutingInfo filterRoutingInfo;
  private ClientProxyMembershipID bridgeContext;
  private boolean isOriginRemote;
  private TXEntryState txEntryState;
  private VersionTag versionTag;
  private long tailKey;

  private final InternalDistributedMember myId = mock(InternalDistributedMember.class);
  private final InternalDistributedMember remoteId = mock(InternalDistributedMember.class);
  private final KeyInfo keyInfo = mock(KeyInfo.class);
  private final ConcurrentMapWithReusableEntries entryMap =
      mock(ConcurrentMapWithReusableEntries.class);
  private final RegionEntryFactory regionEntryFactory = mock(RegionEntryFactory.class);
  private final RegionEntry factoryRegionEntry = mock(RegionEntry.class);
  private final RegionEntry existingRegionEntry = mock(RegionEntry.class);
  private final PartitionedRegion pr = mock(PartitionedRegion.class);

  private LocalRegion owner;
  private TestableAbstractRegionMap regionMap;

  @Before
  public void setup() {
    when(regionEntryFactory.createEntry(any(), any(), any())).thenReturn(factoryRegionEntry);
    when(existingRegionEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    when(keyInfo.getKey()).thenReturn(key);
    when(txId.getMemberId()).thenReturn(myId);
  }

  @Test
  public void txApplyDestroySetCorrectPendingCallback_givenNoRegionEntryNotInTokenModeNoconcurrencyChecks() {
    givenLocalRegion();
    givenNoRegionEntry();
    givenNotInTokenMode();
    givenNoConcurrencyChecks();

    doTxApplyDestroy();

    validatePendingCallbacks(expectedEvent());
  }

  @Test
  public void txApplyDestroySetCorrectPendingCallback_givenNoRegionEntryNotInTokenModeNoconcurrencyChecks_andBucket() {
    givenBucketRegion();
    givenNoRegionEntry();
    givenNotInTokenMode();
    givenNoConcurrencyChecks();
    txEntryState = mock(TXEntryState.class);

    doTxApplyDestroy();

    EntryEventImpl expectedEvent = expectedEvent();
    expectedEvent.setRegion(pr);
    validatePendingCallbacks(expectedEvent);
    verify(owner, times(1)).handleWANEvent(any());
    verify(txEntryState, times(1)).setTailKey(tailKey);
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenRemovedRegionEntry() {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(true);

    doTxApplyDestroy();

    validateNoPendingCallbacks();
  }

  @Test
  public void txApplyDestroyInvokesRescheduleTombstone_givenRemovedTombstoneRegionEntry() {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(true);
    when(existingRegionEntry.isTombstone()).thenReturn(true);

    doTxApplyDestroy();

    verify(owner, times(1)).rescheduleTombstone(same(existingRegionEntry), any());
  }

  private void validateNoPendingCallbacks() {
    assertThat(pendingCallbacks).isEmpty();
  }

  private void validatePendingCallbacks(EntryEventImpl expectedEvent) {
    assertThat(pendingCallbacks).hasSize(1);
    EntryEventImpl callbackEvent = pendingCallbacks.get(0);
    if (!callbackEvent.checkEquality(expectedEvent)) {
      assertThat(callbackEvent).isEqualTo(expectedEvent);
    }
  }

  private EntryEventImpl expectedEvent() {
    EntryEventImpl expectedEvent =
        AbstractRegionMap.createCallbackEvent(owner, op, key, null, txId, txEvent, eventId,
            aCallbackArgument, filterRoutingInfo, bridgeContext, txEntryState, versionTag, tailKey);
    expectedEvent.setOriginRemote(isOriginRemote);
    return expectedEvent;
  }

  private void givenNoRegionEntry() {
    when(entryMap.get(eq(key))).thenReturn(null);
  }

  private void givenExistingRegionEntry() {
    when(entryMap.get(eq(key))).thenReturn(existingRegionEntry);
  }

  private void givenNotInTokenMode() {
    this.inTokenMode = false;
  }

  private void givenNoConcurrencyChecks() {
    when(owner.getConcurrencyChecksEnabled()).thenReturn(false);
  }

  private void givenLocalRegion() {
    owner = mock(LocalRegion.class);
    setupLocalRegion();
    regionMap = new TestableAbstractRegionMap();
  }

  private void givenBucketRegion() {
    BucketRegion bucketRegion = mock(BucketRegion.class);
    when(bucketRegion.isUsedForPartitionedRegionBucket()).thenReturn(true);
    when(bucketRegion.getPartitionedRegion()).thenReturn(pr);
    when(bucketRegion.getBucketAdvisor()).thenReturn(mock(BucketAdvisor.class));
    owner = bucketRegion;
    setupLocalRegion();
    regionMap = new TestableAbstractRegionMap();
  }

  private void setupLocalRegion() {
    when(owner.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(owner.getCache()).thenReturn(mock(InternalCache.class));
    when(owner.getMyId()).thenReturn(myId);
    when(owner.getKeyInfo(any(), any(), any())).thenReturn(keyInfo);
  }

  private void doTxApplyDestroy() {
    regionMap.txApplyDestroy(key, txId, txEvent, inTokenMode, inRI, op, eventId, aCallbackArgument,
        pendingCallbacks, filterRoutingInfo, bridgeContext, isOriginRemote, txEntryState,
        versionTag, tailKey);
  }

  private class TestableAbstractRegionMap extends AbstractRegionMap {

    public TestableAbstractRegionMap() {
      super(null);
      initialize(owner, new Attributes(), null, false);
      setEntryMap(entryMap);
      setEntryFactory(regionEntryFactory);
    }

  }
}
