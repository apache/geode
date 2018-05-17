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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
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
  private Object aCallbackArgument = "aCallbackArgument";
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
  private final VersionTag existingVersionTag = mock(VersionTag.class);

  private LocalRegion owner;
  private TestableAbstractRegionMap regionMap;

  @Before
  public void setup() {
    when(regionEntryFactory.createEntry(any(), any(), any())).thenReturn(factoryRegionEntry);
    VersionStamp versionStamp = mock(VersionStamp.class);
    when(versionStamp.asVersionTag()).thenReturn(existingVersionTag);
    when(existingRegionEntry.getVersionStamp()).thenReturn(versionStamp);
    when(existingRegionEntry.getKey()).thenReturn(key);
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
  public void txApplyDestroyHasNoPendingCallback_givenExistingRegionEntryThatIsRemoved() {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(true);

    doTxApplyDestroy();

    validateNoPendingCallbacks();
  }

  @Test
  public void txApplyDestroyInvokesRescheduleTombstone_givenExistingRegionEntryThatIsTombstone() {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(true);
    when(existingRegionEntry.isTombstone()).thenReturn(true);

    doTxApplyDestroy();

    verify(owner, times(1)).rescheduleTombstone(same(existingRegionEntry), any());
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenExistingRegionEntryThatIsValidWithInTokenModeAndNotInRI() {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(false);
    when(existingRegionEntry.isTombstone()).thenReturn(false);
    this.inTokenMode = true;
    this.inRI = false;

    doTxApplyDestroy();

    validateNoPendingCallbacks();
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key),
        eq(this.inTokenMode), eq(false));
  }

  @Test
  public void txApplyDestroySetsRegionEntryOnEvent_givenExistingRegionEntryThatIsValid() {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.getRegionEntry()).isSameAs(existingRegionEntry);
  }

  @Test
  public void txApplyDestroySetsOldValueOnEvent_givenExistingRegionEntryThatIsValid() {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    Object oldValue = "oldValue";
    when(existingRegionEntry.getValueInVM(owner)).thenReturn(oldValue);

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.getOldValue()).isSameAs(oldValue);
  }

  @Test
  public void txApplyDestroyUpdateIndexes_givenExistingRegionEntryThatIsValid()
      throws QueryException {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    IndexManager indexManager = mock(IndexManager.class);
    when(owner.getIndexManager()).thenReturn(indexManager);

    doTxApplyDestroy();

    verify(indexManager, times(1)).updateIndexes(same(existingRegionEntry),
        eq(IndexManager.REMOVE_ENTRY), eq(IndexProtocol.OTHER_OP));
  }

  @Test
  public void txApplyDestroyCallsAddDestroy_givenExistingRegionEntryThatIsValidAndTxRmtEvent() {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    txEvent = mock(TXRmtEvent.class);

    doTxApplyDestroy();

    verify(txEvent, times(1)).addDestroy(same(owner), same(existingRegionEntry), eq(key),
        same(aCallbackArgument));
  }


  @Test
  public void txApplyDestroyCallsProcessAndGenerateTXVersionTag_givenExistingRegionEntryThatIsValid() {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    txEntryState = mock(TXEntryState.class);

    doTxApplyDestroy();

    verify(regionMap, times(1)).processAndGenerateTXVersionTag(any(), same(existingRegionEntry),
        same(txEntryState));
  }

  @Test
  public void txApplyDestroySetsValueToDestoryToken_givenExistingRegionEntryThatIsValidWithInTokenMode()
      throws RegionClearedException {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(existingRegionEntry, times(1)).setValue(same(owner), eq(Token.DESTROYED));
  }

  @Test
  public void txApplyDestroyCallsUnscheduleTombstone_givenExistingRegionEntryThatIsTombstoneWithInTokenMode()
      throws RegionClearedException {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    inTokenMode = true;
    when(existingRegionEntry.getValueInVM(owner)).thenReturn(Token.TOMBSTONE);

    doTxApplyDestroy();

    verify(owner, times(1)).unscheduleTombstone(same(existingRegionEntry));
  }

  @Test
  public void txApplyDestroyCallsRescheduleTombstone_givenExistingRegionEntryThatIsValidWithoutInTokenModeWithoutConcurrencyCheck()
      throws RegionClearedException {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(false);
    givenExistingRegionEntry();
    inTokenMode = false;

    doTxApplyDestroy();

    verify(existingRegionEntry, times(1)).removePhase1(same(owner), eq(false));
    verify(existingRegionEntry, times(1)).removePhase2();
    verify(regionMap, times(1)).removeEntry(eq(key), same(existingRegionEntry), eq(false));
  }

  @Test
  public void txApplyDestroyCallsRescheduleTombstone_givenExistingRegionEntryThatIsValidWithoutInTokenModeWithConcurrencyCheckButNoVersionTag()
      throws RegionClearedException {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    inTokenMode = false;
    versionTag = null;

    doTxApplyDestroy();

    verify(existingRegionEntry, times(1)).removePhase1(same(owner), eq(false));
    verify(existingRegionEntry, times(1)).removePhase2();
    verify(regionMap, times(1)).removeEntry(eq(key), same(existingRegionEntry), eq(false));
  }

  @Test
  public void txApplyDestroyCallsRescheduleTombstone_givenExistingRegionEntryThatIsValidWithoutInTokenModeWithConcurrencyCheckAndVersionTag()
      throws RegionClearedException {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    inTokenMode = false;
    versionTag = mock(VersionTag.class);

    doTxApplyDestroy();

    verify(existingRegionEntry, times(1)).makeTombstone(same(owner), same(versionTag));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRegionEntryThatIsValidWithoutInTokenModeAndNotInRI() {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(false);
    when(existingRegionEntry.isTombstone()).thenReturn(false);
    this.inTokenMode = false;
    this.inRI = false;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key),
        eq(this.inTokenMode), eq(false));

  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRegionEntryThatIsValidWithoutInTokenModeAndWithInRI() {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(false);
    when(existingRegionEntry.isTombstone()).thenReturn(false);
    this.inTokenMode = false;
    this.inRI = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key),
        eq(this.inTokenMode), eq(false));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRegionEntryThatIsValidWithInTokenModeAndInRI() {
    givenLocalRegion();
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(false);
    when(existingRegionEntry.isTombstone()).thenReturn(false);
    this.inTokenMode = true;
    this.inRI = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key),
        eq(this.inTokenMode), eq(false));
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenExistingRegionEntryThatIsValidWithPartitionedRegion() {
    givenBucketRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(false);
    when(existingRegionEntry.isTombstone()).thenReturn(false);

    doTxApplyDestroy();

    validateNoPendingCallbacks();
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key),
        eq(this.inTokenMode), eq(false));
  }

  @Test
  public void txApplyDestroyCallsHandleWanEvent_givenExistingRegionEntryThatIsValidWithPartitionedRegion() {
    givenBucketRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(false);
    when(existingRegionEntry.isTombstone()).thenReturn(false);
    txEntryState = mock(TXEntryState.class);

    doTxApplyDestroy();

    verify(owner, times(1)).handleWANEvent(any());
    verify(txEntryState, times(1)).setTailKey(tailKey);
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenExistingRegionEntryThatIsValidWithoutConcurrencyChecks() {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(false);
    when(existingRegionEntry.isTombstone()).thenReturn(false);
    when(owner.getConcurrencyChecksEnabled()).thenReturn(false);

    doTxApplyDestroy();

    validateNoPendingCallbacks();
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key),
        eq(this.inTokenMode), eq(false));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRegionEntryThatIsValidWithShouldDispatchListenerEvent() {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(false);
    when(existingRegionEntry.isTombstone()).thenReturn(false);
    when(owner.shouldDispatchListenerEvent()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key),
        eq(this.inTokenMode), eq(false));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRegionEntryThatIsValidWithshouldNotifyBridgeClients() {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(false);
    when(existingRegionEntry.isTombstone()).thenReturn(false);
    when(owner.shouldNotifyBridgeClients()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key),
        eq(this.inTokenMode), eq(false));
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
    regionMap = spy(new TestableAbstractRegionMap());
  }

  private void givenBucketRegion() {
    BucketRegion bucketRegion = mock(BucketRegion.class);
    when(bucketRegion.isUsedForPartitionedRegionBucket()).thenReturn(true);
    when(bucketRegion.getPartitionedRegion()).thenReturn(pr);
    when(bucketRegion.getBucketAdvisor()).thenReturn(mock(BucketAdvisor.class));
    owner = bucketRegion;
    setupLocalRegion();
    regionMap = spy(new TestableAbstractRegionMap());
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
