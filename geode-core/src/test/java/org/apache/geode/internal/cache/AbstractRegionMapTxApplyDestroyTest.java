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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.Mock;

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXEntryState.DistTxThinEntryState;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.util.concurrent.ConcurrentMapWithReusableEntries;
import org.apache.geode.test.junit.categories.UnitTest;

@SuppressWarnings("unchecked")
@Category(UnitTest.class)
public class AbstractRegionMapTxApplyDestroyTest {
  // parameters
  private Object key = "key";
  @Mock
  private TXId txId;
  private TXRmtEvent txEvent;
  private boolean inTokenMode;
  private boolean inRI;
  private Operation operation = Operation.DESTROY;
  @Mock
  private EventID eventId;
  private Object aCallbackArgument = "aCallbackArgument";
  private final List<EntryEventImpl> pendingCallbacks = new ArrayList<>();
  private FilterRoutingInfo filterRoutingInfo = null; // Provide a meaningful value for this?
  @Mock
  private ClientProxyMembershipID bridgeContext;
  private boolean isOriginRemote = false;
  @Mock
  private TXEntryState txEntryState;
  private VersionTag versionTag;
  private long tailKey = 223L;

  @Mock
  private InternalDistributedMember myId;
  @Mock
  private InternalDistributedMember remoteId;
  @Mock
  private KeyInfo keyInfo;
  @Mock
  private ConcurrentMapWithReusableEntries entryMap;
  @Mock
  private RegionEntryFactory regionEntryFactory;
  @Mock
  private RegionEntry existingRegionEntry;
  @Mock
  private RegionEntry factoryRegionEntry;
  @Mock
  private RegionEntry oldRegionEntry;
  @Mock
  private PartitionedRegion partitionedRegion;
  @Mock
  private VersionTag existingVersionTag;
  @Mock
  private CachePerfStats cachePerfStats;

  private LocalRegion owner;
  private TestableAbstractRegionMap regionMap;

  @Before
  public void setup() {
    initMocks(this);
    VersionStamp versionStamp = mock(VersionStamp.class);
    when(versionStamp.asVersionTag()).thenReturn(existingVersionTag);
    when(existingRegionEntry.getVersionStamp()).thenReturn(versionStamp);
    when(existingRegionEntry.getKey()).thenReturn(key);
    when(factoryRegionEntry.getKey()).thenReturn(key);
    when(oldRegionEntry.getKey()).thenReturn(key);
    when(keyInfo.getKey()).thenReturn(key);
    when(txId.getMemberId()).thenReturn(myId);
  }

  // tests for no region entry.
  // Each of these tests requires givenNotInTokenMode and givenNoConcurrencyChecks

  @Test
  public void txApplyDestroySetCorrectPendingCallback_givenNoRegionEntryNotInTokenModeNoConcurrencyChecks() {
    givenLocalRegion();
    givenNoRegionEntry();
    givenNotInTokenMode();
    givenNoConcurrencyChecks();
    when(owner.generateEventID()).thenReturn(true);
    when(keyInfo.getCallbackArg()).thenReturn(aCallbackArgument);

    doTxApplyDestroy();
    assertThat(pendingCallbacks).hasSize(1);
    EntryEventImpl callbackEvent = pendingCallbacks.get(0);

    // noinspection Duplicates
    assertSoftly(softly -> {
      softly.assertThat(callbackEvent.getRegion()).isSameAs(owner);
      softly.assertThat(callbackEvent.getOperation()).isSameAs(operation);
      softly.assertThat(callbackEvent.getKey()).isSameAs(key);
      softly.assertThat(callbackEvent.getNewValue()).isNull();
      softly.assertThat(callbackEvent.getTransactionId()).isSameAs(txId);
      softly.assertThat(callbackEvent.getEventId()).isSameAs(eventId);
      softly.assertThat(callbackEvent.getCallbackArgument()).isSameAs(aCallbackArgument);
      softly.assertThat(callbackEvent.getLocalFilterInfo()).isSameAs(filterRoutingInfo);
      softly.assertThat(callbackEvent.getContext()).isSameAs(bridgeContext);
      softly.assertThat(callbackEvent.isOriginRemote()).isEqualTo(isOriginRemote);
      softly.assertThat(callbackEvent.getVersionTag()).isEqualTo(versionTag);
      softly.assertThat(callbackEvent.getTailKey()).isEqualTo(tailKey);
    });
  }

  @Test
  public void addsCallbackEvent_givenNoRegionEntryNotInTokenModeNoConcurrencyChecks_andBucket() {
    givenBucketRegion();
    givenNoRegionEntry();
    givenNotInTokenMode();
    givenNoConcurrencyChecks();

    when(partitionedRegion.generateEventID()).thenReturn(true);
    when(keyInfo.getCallbackArg()).thenReturn(aCallbackArgument);

    doTxApplyDestroy();

    verify(owner, times(1)).handleWANEvent(any());
    verify(txEntryState, times(1)).setTailKey(tailKey);

    assertThat(pendingCallbacks).hasSize(1);
    EntryEventImpl callbackEvent = pendingCallbacks.get(0);

    // noinspection Duplicates
    assertSoftly(softly -> {
      softly.assertThat(callbackEvent.getRegion()).isSameAs(partitionedRegion);
      softly.assertThat(callbackEvent.getOperation()).isSameAs(operation);
      softly.assertThat(callbackEvent.getKey()).isSameAs(key);
      softly.assertThat(callbackEvent.getNewValue()).isNull();
      softly.assertThat(callbackEvent.getTransactionId()).isSameAs(txId);
      softly.assertThat(callbackEvent.getEventId()).isSameAs(eventId);
      softly.assertThat(callbackEvent.getCallbackArgument()).isSameAs(aCallbackArgument);
      softly.assertThat(callbackEvent.getLocalFilterInfo()).isSameAs(filterRoutingInfo);
      softly.assertThat(callbackEvent.getContext()).isSameAs(bridgeContext);
      softly.assertThat(callbackEvent.isOriginRemote()).isEqualTo(isOriginRemote);
      softly.assertThat(callbackEvent.getVersionTag()).isEqualTo(versionTag);
      softly.assertThat(callbackEvent.getTailKey()).isEqualTo(tailKey);
    });
  }

  @Test
  public void txApplyDestroyCallReleaseEvent_givenNoRegionEntryNotInTokenModeNoConcurrencyChecksAndBucket_whenHandleWANEventThrows() {
    givenBucketRegion();
    givenNoRegionEntry();
    givenNotInTokenMode();
    givenNoConcurrencyChecks();
    doThrow(RuntimeException.class).when(owner).handleWANEvent(any());

    assertThatThrownBy(this::doTxApplyDestroy).isInstanceOf(RuntimeException.class);

    verify(regionMap, times(1)).releaseEvent(any());
  }

  // tests for "existingRegionEntry"

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenExistingRegionEntryThatIsRemoved() {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
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
  public void doesNotAddCallbackEvent_ifExistingRegionEntryIsTombstone() throws Exception {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(true);
    when(existingRegionEntry.isTombstone()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
    verify(existingRegionEntry, never()).makeTombstone(any(), any());
    verify(txEntryState, never()).setVersionTag(any());
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenExistingRegionEntryWithInTokenModeAndNotInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    this.inTokenMode = true;
    this.inRI = false;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key),
        eq(this.inTokenMode), eq(false), eq(false));
  }

  @Test
  public void doesNotAddCallbackEvent_givenExistingRegionEntryWithInTokenModeAndNotInRI()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    this.inTokenMode = true;
    this.inRI = false;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
    verify(owner).generateAndSetVersionTag(any(EntryEventImpl.class), same(existingRegionEntry));
    verify(existingRegionEntry, never()).makeTombstone(any(), any());
    verify(txEntryState).setVersionTag(any());
  }

  @Test
  public void setsRegionEntryOnEvent_ifExistingRegionEntryIsValid() throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();

    Object oldValue = "oldValue";
    when(existingRegionEntry.getValueInVM(owner)).thenReturn(oldValue);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).size().isEqualTo(1);
    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.getRegionEntry()).isSameAs(existingRegionEntry);
    assertThat(event.getOldValue()).isSameAs(oldValue);

    verify(owner).generateAndSetVersionTag(any(EntryEventImpl.class), same(existingRegionEntry));
    verify(existingRegionEntry, never()).makeTombstone(any(), any());
    verify(txEntryState).setVersionTag(event.getVersionTag());
  }

  @Test
  public void txApplyDestroyUpdateIndexes_givenExistingRegionEntryThatIsValid() throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
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
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    txEvent = mock(TXRmtEvent.class);

    doTxApplyDestroy();

    verify(txEvent, times(1)).addDestroy(same(owner), same(existingRegionEntry), eq(key),
        same(aCallbackArgument));
  }

  @Test
  public void callsProcessAndGenerateTXVersionTag_givenExistingRegionEntryThatIsValid() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();

    doTxApplyDestroy();

    EntryEventImpl callbackEvent = pendingCallbacks.get(0);
    verify(regionMap, times(1)).processAndGenerateTXVersionTag(same(callbackEvent),
        same(existingRegionEntry), same(txEntryState));
    assertThat(callbackEvent.getNextRegionVersion()).isEqualTo(-1L); // Default value
  }

  @Test
  public void setsEventNextRegionVersionOnCallbackEvent_givenExistingRegionEntryThatIsValidAndDistTxEntryStateExists() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();

    DistTxThinEntryState distTxEntryStates = mock(DistTxThinEntryState.class);
    when(txEntryState.getDistTxEntryStates()).thenReturn(distTxEntryStates);
    when(distTxEntryStates.getRegionVersion()).thenReturn(999L);

    doTxApplyDestroy();

    EntryEventImpl callbackEvent = pendingCallbacks.get(0);
    assertThat(callbackEvent.getNextRegionVersion()).isEqualTo(999L); // Default value
  }

  @Test
  public void txApplyDestroySetsValueToDestroyToken_givenExistingRegionEntryThatIsValidWithInTokenMode()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(existingRegionEntry, times(1)).setValue(same(owner), eq(Token.DESTROYED));
  }

  @Test
  public void txApplyDestroyHandlesClear_givenExistingRegionEntryThatIsValidWithInTokenModeAndSetValueThrowsRegionClearedException()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    inTokenMode = true;
    doThrow(RegionClearedException.class).when(existingRegionEntry).setValue(same(owner),
        eq(Token.DESTROYED));

    doTxApplyDestroy();

    verify(owner, times(1)).txApplyDestroyPart2(any(), any(), anyBoolean(), eq(true), anyBoolean());
    verify(regionMap, never()).lruEntryDestroy(any());
  }

  @Test
  public void txApplyDestroyHandlesNoClear_givenExistingRegionEntryThatIsValidWithInTokenMode() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(owner, times(1)).txApplyDestroyPart2(any(), any(), anyBoolean(), eq(false),
        anyBoolean());
    verify(regionMap, times(1)).lruEntryDestroy(any());
  }

  @Test
  public void txApplyDestroyCallsUnscheduleTombstone_givenExistingRegionEntryThatIsTombstoneWithInTokenMode() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    inTokenMode = true;
    when(existingRegionEntry.getValueInVM(owner)).thenReturn(Token.TOMBSTONE);

    doTxApplyDestroy();

    verify(owner, times(1)).unscheduleTombstone(same(existingRegionEntry));
  }

  @Test
  public void txApplyDestroyCallsRemoveEntry_givenExistingRegionEntryThatIsValidWithoutInTokenModeWithoutConcurrencyCheck()
      throws Exception {
    givenLocalRegion();
    givenNoConcurrencyChecks();
    givenExistingRegionEntry();
    inTokenMode = false;

    doTxApplyDestroy();

    verify(existingRegionEntry, times(1)).removePhase1(same(owner), eq(false));
    verify(existingRegionEntry, times(1)).removePhase2();
    verify(regionMap, times(1)).removeEntry(eq(key), same(existingRegionEntry), eq(false));
  }

  @Test
  public void txApplyDestroyCallsRescheduleTombstone_givenExistingRegionEntryThatIsValidWithoutInTokenModeWithConcurrencyCheckButNoVersionTag()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    inTokenMode = false;
    versionTag = null;

    doTxApplyDestroy();

    verify(existingRegionEntry, times(1)).removePhase1(same(owner), eq(false));
    verify(existingRegionEntry, times(1)).removePhase2();
    verify(regionMap, times(1)).removeEntry(eq(key), same(existingRegionEntry), eq(false));
  }

  @Test
  public void txApplyDestroyCallsMakeTombstone_givenExistingRegionEntryThatIsValidWithoutInTokenModeWithConcurrencyCheckAndVersionTag()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    inTokenMode = false;
    versionTag = mock(VersionTag.class);

    doTxApplyDestroy();

    verify(existingRegionEntry, times(1)).makeTombstone(same(owner), same(versionTag));
  }

  @Test
  public void txApplyDestroyCallsUpdateSizeOnRemove_givenExistingRegionEntryThatIsValid() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    int oldSize = 79;
    when(owner.calculateRegionEntryValueSize(same(existingRegionEntry))).thenReturn(oldSize);

    doTxApplyDestroy();

    verify(owner, times(1)).updateSizeOnRemove(eq(key), eq(oldSize));
  }

  @Test
  public void txApplyDestroyCallsReleaseEvent_givenExistingRegionEntry() {
    givenLocalRegion();
    givenExistingRegionEntry();

    doTxApplyDestroy();

    verify(regionMap, times(1)).releaseEvent(any());
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRegionEntryWithoutInTokenModeAndNotInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    inTokenMode = false;
    inRI = false;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(false));

  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRemovedRegionEntryWithoutInTokenModeAndNotInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRemovedRegionEntry();
    inTokenMode = false;
    inRI = false;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(true));

  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRegionEntryWithoutInTokenModeAndWithInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    inTokenMode = false;
    inRI = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(false));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRemovedRegionEntryWithoutInTokenModeAndWithInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRemovedRegionEntry();
    inTokenMode = false;
    inRI = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(true));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRegionEntryWithInTokenModeAndInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRegionEntry();
    inTokenMode = true;
    inRI = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(false));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRemovedRegionEntryWithInTokenModeAndInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenExistingRemovedRegionEntry();
    inTokenMode = true;
    inRI = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(true));
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenExistingRegionEntryWithPartitionedRegion() {
    givenBucketRegion();
    givenExistingRegionEntry();

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(false));
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenExistingRemovedRegionEntryWithPartitionedRegion() {
    givenBucketRegion();
    givenExistingRemovedRegionEntry();

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(true));
  }

  @Test
  public void txApplyDestroyCallsHandleWanEvent_givenExistingRegionEntryWithPartitionedRegion() {
    givenBucketRegion();
    givenExistingRegionEntry();

    doTxApplyDestroy();

    verify(owner, times(1)).handleWANEvent(any());
    verify(txEntryState, times(1)).setTailKey(tailKey);
  }

  @Test
  public void txApplyDestroyDoesNotCallSetVersionTag_givenExistingRegionEntryWithPartitionedRegionButNoConcurrencyChecks() {
    givenBucketRegion();
    givenExistingRegionEntry();
    versionTag = mock(VersionTag.class);
    givenNoConcurrencyChecks();

    doTxApplyDestroy();

    verify(txEntryState, never()).setVersionTag(any());
  }

  @Test
  public void txApplyDestroyPreparesAndReleasesIndexManager_givenExistingRegionEntryWithIndexManager() {
    givenLocalRegion();
    givenExistingRegionEntry();
    IndexManager indexManager = mock(IndexManager.class);
    when(owner.getIndexManager()).thenReturn(indexManager);

    doTxApplyDestroy();

    InOrder inOrder = inOrder(indexManager);
    inOrder.verify(indexManager, times(1)).waitForIndexInit();
    inOrder.verify(indexManager, times(1)).countDownIndexUpdaters();
  }

  @Test
  public void txApplyDestroyCallsSetVersionTag_givenExistingRegionEntryWithPartitionedRegionAndConcurrencyChecks() {
    givenBucketRegion();
    givenExistingRegionEntry();
    versionTag = mock(VersionTag.class);
    givenConcurrencyChecks();

    doTxApplyDestroy();

    verify(txEntryState, times(1)).setVersionTag(same(versionTag));
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenExistingRegionEntryWithoutConcurrencyChecks() {
    givenLocalRegion();
    givenExistingRegionEntry();
    givenNoConcurrencyChecks();

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(false));
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenExistingRemovedRegionEntryWithoutConcurrencyChecks() {
    givenLocalRegion();
    givenExistingRemovedRegionEntry();
    givenNoConcurrencyChecks();

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(true));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRegionEntryWithShouldDispatchListenerEvent() {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(owner.shouldDispatchListenerEvent()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(false));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRemovedRegionEntryWithShouldDispatchListenerEvent() {
    givenLocalRegion();
    givenExistingRemovedRegionEntry();
    when(owner.shouldDispatchListenerEvent()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(true));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRegionEntryWithshouldNotifyBridgeClients() {
    givenLocalRegion();
    givenExistingRegionEntry();
    when(owner.shouldNotifyBridgeClients()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(false));
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenExistingRemovedRegionEntryWithshouldNotifyBridgeClients() {
    givenLocalRegion();
    givenExistingRemovedRegionEntry();
    when(owner.shouldNotifyBridgeClients()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    verify(owner, times(1)).txApplyDestroyPart2(same(existingRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(true));
  }

  @Test
  public void txApplyDestroyDoesCallTxApplyDestroyPart2_givenFactoryRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();
    versionTag = mock(VersionTag.class);

    doTxApplyDestroy();

    verify(owner, times(1)).txApplyDestroyPart2(same(factoryRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(true));
  }

  // tests for "factoryRegionEntry" (that is, no existing region entry and no oldRegionEntry).
  // All these tests require no existing region entry (that is, not found by getEntry).
  // All these tests need one of the following: givenConcurrencyChecks OR inTokenMode

  @Test
  public void txApplyDestroyCallsCreateEntry_givenFactoryRegionEntryAndInTokenMode() {
    givenLocalRegion();
    givenFactoryRegionEntry();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(regionEntryFactory, times(1)).createEntry(same(owner), eq(key),
        eq(Token.REMOVED_PHASE1));
  }

  @Test
  public void txApplyDestroyCallsCreateEntry_givenFactoryRegionEntryAndConcurrencyChecks() {
    givenLocalRegion();
    givenFactoryRegionEntry();
    givenConcurrencyChecks();

    doTxApplyDestroy();

    verify(regionEntryFactory, times(1)).createEntry(same(owner), eq(key),
        eq(Token.REMOVED_PHASE1));
  }

  @Test
  public void txApplyDestroyPreparesAndReleasesIndexManager_givenFactoryRegionEntryAndConcurrencyChecksWithIndexManager() {
    givenLocalRegion();
    givenFactoryRegionEntry();
    givenConcurrencyChecks();
    IndexManager indexManager = mock(IndexManager.class);
    when(owner.getIndexManager()).thenReturn(indexManager);

    doTxApplyDestroy();

    InOrder inOrder = inOrder(indexManager);
    inOrder.verify(indexManager, times(1)).waitForIndexInit();
    inOrder.verify(indexManager, times(1)).countDownIndexUpdaters();
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenFactoryRegionEntryWithInTokenModeAndNotInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();
    inTokenMode = true;
    inRI = false;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenFactoryRegionEntryWithoutInTokenModeAndNotInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();
    inTokenMode = false;
    inRI = false;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenFactoryRegionEntryWithoutInTokenModeAndWithInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();
    inTokenMode = false;
    inRI = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenFactoryRegionEntryWithInTokenModeAndInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();
    inTokenMode = true;
    inRI = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenFactoryRegionEntryWithPartitionedRegion() {
    givenBucketRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();
    when(partitionedRegion.getConcurrencyChecksEnabled()).thenReturn(false);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenFactoryRegionEntryWithoutConcurrencyChecksInTokenMode() {
    givenLocalRegion();
    givenFactoryRegionEntry();
    givenNoConcurrencyChecks();
    inTokenMode = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenFactoryRegionEntryWithShouldDispatchListenerEvent() {
    givenLocalRegion();
    givenNoConcurrencyChecks();
    givenFactoryRegionEntry();
    inTokenMode = true;
    inRI = true;
    when(owner.shouldDispatchListenerEvent()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenFactoryRegionEntryWithshouldNotifyBridgeClients() {
    givenLocalRegion();
    givenNoConcurrencyChecks();
    givenFactoryRegionEntry();
    inTokenMode = true;
    inRI = true;
    when(owner.shouldNotifyBridgeClients()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
  }

  @Test
  public void txApplyDestroySetsRegionEntryOnEvent_givenFactoryRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.getRegionEntry()).isSameAs(factoryRegionEntry);
  }

  @Test
  public void txApplyDestroySetsOldValueOnEventToNotAvailable_givenFactoryRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.getRawOldValue()).isSameAs(Token.NOT_AVAILABLE);
  }

  @Test
  public void txApplyDestroyCallsHandleWanEvent_givenFactoryRegionEntryWithPartitionedRegion() {
    givenBucketRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();

    doTxApplyDestroy();

    verify(owner, times(1)).handleWANEvent(any());
    verify(txEntryState, times(1)).setTailKey(tailKey);
  }

  @Test
  public void txApplyDestroyCallsProcessAndGenerateTXVersionTag_givenFactoryRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();

    doTxApplyDestroy();

    verify(regionMap, times(1)).processAndGenerateTXVersionTag(any(), same(factoryRegionEntry),
        same(txEntryState));
  }

  @Test
  public void txApplyDestroySetsRemoteOrigin_givenFactoryRegionEntryAndRemoteTxId() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();
    when(txId.getMemberId()).thenReturn(remoteId);

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.isOriginRemote()).isTrue();
  }

  @Test
  public void txApplyDestroySetsRemoteOrigin_givenFactoryRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.isOriginRemote()).isFalse();
  }

  @Test
  public void txApplyDestroySetsRegionOnEvent_givenFactoryRegionEntryAndBucket() {
    givenBucketRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.getRegion()).isSameAs(partitionedRegion);
  }

  @Test
  public void txApplyDestroyNeverCallsUpdateSizeOnCreate_givenFactoryRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();

    doTxApplyDestroy();

    verify(owner, never()).updateSizeOnCreate(any(), anyInt());
  }

  @Test
  public void txApplyDestroyCallsMakeTombstone_givenFactoryRegionEntryWithConcurrencyCheckAndVersionTag()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();
    versionTag = mock(VersionTag.class);

    doTxApplyDestroy();

    verify(factoryRegionEntry, times(1)).makeTombstone(same(owner), same(versionTag));
  }

  @Test
  public void txApplyDestroyNeverCallsMakeTombstone_givenFactoryRegionEntryWithConcurrencyCheckButNoVersionTag()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();

    doTxApplyDestroy();

    verify(factoryRegionEntry, never()).makeTombstone(any(), any());
  }

  @Test
  public void txApplyDestroyNeverCallsMakeTombstone_givenFactoryRegionEntryWithoutConcurrencyCheckButWithInTokenModeAndVersionTag()
      throws Exception {
    givenLocalRegion();
    givenNoConcurrencyChecks();
    givenFactoryRegionEntry();
    inTokenMode = true;
    versionTag = mock(VersionTag.class);

    doTxApplyDestroy();

    verify(factoryRegionEntry, never()).makeTombstone(any(), any());
  }

  @Test
  public void txApplyDestroyCallsRemoveEntry_givenFactoryRegionEntryWithConcurrencyCheck()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();

    doTxApplyDestroy();

    verify(factoryRegionEntry, times(1)).removePhase1(same(owner), eq(false));
    verify(factoryRegionEntry, times(1)).removePhase2();
    verify(regionMap, times(1)).removeEntry(eq(key), same(factoryRegionEntry), eq(false));
  }

  @Test
  public void txApplyDestroyCallsReleaseEvent_givenFactoryRegionEntryWithoutConcurrencyChecksInTokenMode() {
    givenLocalRegion();
    givenFactoryRegionEntry();
    givenNoConcurrencyChecks();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(regionMap, times(1)).releaseEvent(any());
  }

  @Test
  public void txApplyDestroyCallsSetValueWithDestroyedToken_givenFactoryRegionEntryWithoutConcurrencyChecksInTokenMode()
      throws RegionClearedException {
    givenLocalRegion();
    givenFactoryRegionEntry();
    givenNoConcurrencyChecks();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(factoryRegionEntry, times(1)).setValue(same(owner), eq(Token.DESTROYED));
  }

  @Test
  public void txApplyDestroyDoesNotCallSetVersionTag_givenFactoryRegionEntryWithPartitionedRegionButNoConcurrencyChecks() {
    givenBucketRegion();
    givenFactoryRegionEntry();
    versionTag = mock(VersionTag.class);
    givenNoConcurrencyChecks();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(txEntryState, never()).setVersionTag(any());
  }

  @Test
  public void txApplyDestroyDoesNotCallSetVersionTag_givenFactoryRegionEntryWithPartitionedRegionAndConcurrencyChecks() {
    givenBucketRegion();
    givenFactoryRegionEntry();
    versionTag = mock(VersionTag.class);
    givenConcurrencyChecks();

    doTxApplyDestroy();

    verify(txEntryState, times(1)).setVersionTag(same(versionTag));
  }

  @Test
  public void txApplyDestroyDoesNotCallTxApplyDestroyPart2_givenFactoryRegionEntryWithMakeTombstoneThrowingRegionDestroyedException()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenFactoryRegionEntry();
    versionTag = mock(VersionTag.class);
    doThrow(RegionClearedException.class).when(factoryRegionEntry).makeTombstone(any(), any());

    doTxApplyDestroy();

    verify(owner, never()).txApplyDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean());
  }

  // tests for "oldRegionEntry" (that is, an existing region entry found by putIfAbsent).
  // All these tests require no existing region entry (that is, not found by getEntry).
  // All these tests need one of the following: givenConcurrencyChecks OR inTokenMode

  @Test
  public void txApplyDestroyRetries_givenOldRegionEntryWithRemovedPhase2() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    inTokenMode = true;
    when(oldRegionEntry.isRemovedPhase2()).thenReturn(true).thenReturn(false);

    doTxApplyDestroy();

    verify(cachePerfStats, times(1)).incRetries();
    verify(entryMap, times(1)).remove(eq(key), same(oldRegionEntry));
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenOldRegionEntryWithInTokenModeAndNotInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    inTokenMode = true;
    inRI = false;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenOldRegionEntrydWithoutInTokenModeAndNotInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    inTokenMode = false;
    inRI = false;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenOldRegionEntryWithoutInTokenModeAndWithInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    inTokenMode = false;
    inRI = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
  }

  @Test
  public void addsCallbackEvent_givenOldRegionEntryWithInTokenModeAndInRI() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    inTokenMode = true;
    inRI = true;

    when(owner.generateEventID()).thenReturn(true);
    when(keyInfo.getCallbackArg()).thenReturn(aCallbackArgument);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
    EntryEventImpl callbackEvent = pendingCallbacks.get(0);

    // noinspection Duplicates
    assertSoftly(softly -> {
      softly.assertThat(callbackEvent.getRegion()).isSameAs(owner);
      softly.assertThat(callbackEvent.getOperation()).isSameAs(operation);
      softly.assertThat(callbackEvent.getKey()).isSameAs(key);
      softly.assertThat(callbackEvent.getNewValue()).isNull();
      softly.assertThat(callbackEvent.getTransactionId()).isSameAs(txId);
      softly.assertThat(callbackEvent.getEventId()).isSameAs(eventId);
      softly.assertThat(callbackEvent.getCallbackArgument()).isSameAs(aCallbackArgument);
      softly.assertThat(callbackEvent.getLocalFilterInfo()).isSameAs(filterRoutingInfo);
      softly.assertThat(callbackEvent.getContext()).isSameAs(bridgeContext);
      softly.assertThat(callbackEvent.isOriginRemote()).isEqualTo(isOriginRemote);
      softly.assertThat(callbackEvent.getVersionTag()).isEqualTo(versionTag);
      softly.assertThat(callbackEvent.getTailKey()).isEqualTo(tailKey);
      softly.assertThat(callbackEvent.getRegionEntry()).isSameAs(oldRegionEntry);
      softly.assertThat(callbackEvent.getOldValue()).isNull();
    });
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenOldRegionEntryWithPartitionedRegion() {
    givenBucketRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    when(partitionedRegion.getConcurrencyChecksEnabled()).thenReturn(false);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
  }

  @Test
  public void txApplyDestroyHasNoPendingCallback_givenOldRegionEntryWithoutConcurrencyChecksInTokenMode() {
    givenLocalRegion();
    givenOldRegionEntry();
    givenNoConcurrencyChecks();
    inTokenMode = true;

    doTxApplyDestroy();

    assertThat(pendingCallbacks).isEmpty();
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenOldRegionEntryWithShouldDispatchListenerEvent() {
    givenLocalRegion();
    givenNoConcurrencyChecks();
    givenOldRegionEntry();
    inTokenMode = true;
    inRI = true;
    when(owner.shouldDispatchListenerEvent()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
  }

  @Test
  public void txApplyDestroyHasPendingCallback_givenOldRegionEntryWithshouldNotifyBridgeClients() {
    givenLocalRegion();
    givenNoConcurrencyChecks();
    givenOldRegionEntry();
    inTokenMode = true;
    inRI = true;
    when(owner.shouldNotifyBridgeClients()).thenReturn(true);

    doTxApplyDestroy();

    assertThat(pendingCallbacks).hasSize(1);
  }

  @Test
  public void txApplyDestroySetsRegionEntryOnEvent_givenOldRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.getRegionEntry()).isSameAs(oldRegionEntry);
  }

  @Test
  public void txApplyDestroySetsOldValueOnEventToNotAvailable_givenOldRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.getRawOldValue()).isSameAs(Token.NOT_AVAILABLE);
  }

  @Test
  public void txApplyDestroyCallsHandleWanEvent_givenOldRegionEntryWithPartitionedRegion() {
    givenBucketRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();

    doTxApplyDestroy();

    verify(owner, times(1)).handleWANEvent(any());
    verify(txEntryState, times(1)).setTailKey(tailKey);
  }

  @Test
  public void txApplyDestroyCallsProcessAndGenerateTXVersionTag_givenOldRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();

    doTxApplyDestroy();

    verify(regionMap, times(1)).processAndGenerateTXVersionTag(any(), same(oldRegionEntry),
        same(txEntryState));
  }

  @Test
  public void txApplyDestroySetsRemoteOrigin_givenOldRegionEntryAndRemoteTxId() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    when(txId.getMemberId()).thenReturn(remoteId);

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.isOriginRemote()).isTrue();
  }

  @Test
  public void txApplyDestroySetsRemoteOrigin_givenOldRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();

    doTxApplyDestroy();

    EntryEventImpl event = pendingCallbacks.get(0);
    assertThat(event.isOriginRemote()).isFalse();
  }

  @Test
  public void addsCallbackEvent_givenOldRegionEntryAndBucket() {
    givenBucketRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();

    when(partitionedRegion.generateEventID()).thenReturn(true);
    when(keyInfo.getCallbackArg()).thenReturn(aCallbackArgument);

    doTxApplyDestroy();

    EntryEventImpl callbackEvent = pendingCallbacks.get(0);

    // noinspection Duplicates
    assertSoftly(softly -> {
      softly.assertThat(callbackEvent.getRegion()).isSameAs(partitionedRegion);
      softly.assertThat(callbackEvent.getOperation()).isSameAs(operation);
      softly.assertThat(callbackEvent.getKey()).isSameAs(key);
      softly.assertThat(callbackEvent.getNewValue()).isNull();
      softly.assertThat(callbackEvent.getTransactionId()).isSameAs(txId);
      softly.assertThat(callbackEvent.getEventId()).isSameAs(eventId);
      softly.assertThat(callbackEvent.getCallbackArgument()).isSameAs(aCallbackArgument);
      softly.assertThat(callbackEvent.getLocalFilterInfo()).isSameAs(filterRoutingInfo);
      softly.assertThat(callbackEvent.getContext()).isSameAs(bridgeContext);
      softly.assertThat(callbackEvent.isOriginRemote()).isEqualTo(isOriginRemote);
      softly.assertThat(callbackEvent.getVersionTag()).isEqualTo(versionTag);
      softly.assertThat(callbackEvent.getTailKey()).isEqualTo(tailKey);
      softly.assertThat(callbackEvent.getRegionEntry()).isSameAs(oldRegionEntry);
      softly.assertThat(callbackEvent.getOldValue()).isNull();
    });
  }

  @Test
  public void txApplyDestroyCallUpdateSizeOnRemoveWithZero_givenOldRegionEntryThatIsTombstone() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    when(oldRegionEntry.isTombstone()).thenReturn(true);

    doTxApplyDestroy();

    verify(owner, times(1)).updateSizeOnRemove(eq(key), eq(0));
  }

  @Test
  public void txApplyDestroyCallUpdateSizeOnRemoveWithOldSize_givenOldRegionEntryThatIsNotTombstone() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    int oldSize = 79;
    when(owner.calculateRegionEntryValueSize(oldRegionEntry)).thenReturn(oldSize);
    when(oldRegionEntry.isTombstone()).thenReturn(false);

    doTxApplyDestroy();

    verify(owner, times(1)).updateSizeOnRemove(eq(key), eq(oldSize));
  }

  @Test
  public void txApplyDestroySetsValueToTokenDestroyed_givenOldRegionEntry() throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();

    doTxApplyDestroy();

    verify(oldRegionEntry, times(1)).setValue(same(owner), eq(Token.DESTROYED));
  }

  @Test
  public void txApplyDestroySetsValueToTokenDestroyed_givenOldRegionEntryAndInTokenMode()
      throws Exception {
    givenLocalRegion();
    givenNoConcurrencyChecks();
    givenOldRegionEntry();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(oldRegionEntry, times(1)).setValue(same(owner), eq(Token.DESTROYED));
  }

  @Test
  public void txApplyDestroyCallsUnscheduleTombstone_givenOldRegionEntryThatIsTombstone() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    when(oldRegionEntry.isTombstone()).thenReturn(true);

    doTxApplyDestroy();

    verify(owner, times(1)).unscheduleTombstone(same(oldRegionEntry));
  }

  @Test
  public void txApplyDestroyCallsTxApplyDestroyPart2_givenOldRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();

    doTxApplyDestroy();

    verify(owner, times(1)).txApplyDestroyPart2(same(oldRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(false));
  }

  @Test
  public void txApplyDestroyCallsTxApplyDestroyPart2_givenOldRemovedRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRemovedRegionEntry();

    doTxApplyDestroy();

    verify(owner, times(1)).txApplyDestroyPart2(same(oldRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(true));
  }

  @Test
  public void txApplyDestroyCallsTxApplyDestroyPart2_givenOldRegionEntryWithInTokenMode() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(owner, times(1)).txApplyDestroyPart2(same(oldRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(false));
  }

  @Test
  public void txApplyDestroyCallsTxApplyDestroyPart2_givenOldRemovedRegionEntryWithInTokenMode() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRemovedRegionEntry();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(owner, times(1)).txApplyDestroyPart2(same(oldRegionEntry), eq(key), eq(inTokenMode),
        eq(false), eq(true));
  }

  @Test
  public void txApplyDestroyCallsLruEntryDestroy_givenOldRegionEntry() {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();

    doTxApplyDestroy();

    verify(regionMap, times(1)).lruEntryDestroy(oldRegionEntry);
  }

  @Test
  public void txApplyDestroyCallsReleaseEvent_givenOldRegionEntryWithoutConcurrencyChecksInTokenMode() {
    givenLocalRegion();
    givenOldRegionEntry();
    givenNoConcurrencyChecks();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(regionMap, times(1)).releaseEvent(any());
  }

  @Test
  public void txApplyDestroyDoesCallTxApplyDestroyPart2_givenOldRegionEntryWithMakeTombstoneThrowingRegionDestroyedException()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    doThrow(RegionClearedException.class).when(oldRegionEntry).setValue(any(), any());

    doTxApplyDestroy();

    verify(owner, times(1)).txApplyDestroyPart2(same(oldRegionEntry), eq(key), eq(inTokenMode),
        eq(true), eq(true));
  }

  @Test
  public void txApplyDestroyCallsMakeTombstone_givenOldRegionEntryWithConcurrencyCheckAndVersionTag()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    versionTag = mock(VersionTag.class);

    doTxApplyDestroy();

    verify(oldRegionEntry, times(1)).makeTombstone(same(owner), same(versionTag));
  }

  @Test
  public void txApplyDestroyCallsRemoveEntry_givenOldRegionEntry() throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();

    doTxApplyDestroy();

    verify(oldRegionEntry, never()).makeTombstone(any(), any());
    verify(oldRegionEntry, times(1)).removePhase1(same(owner), eq(false));
    verify(oldRegionEntry, times(1)).removePhase2();
    verify(regionMap, times(1)).removeEntry(eq(key), same(oldRegionEntry), eq(false));
  }

  @Test
  public void txApplyDestroyNeverCallsRemoveEntry_givenOldRegionEntryAndInTokenMode()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    inTokenMode = true;

    doTxApplyDestroy();

    verify(oldRegionEntry, never()).makeTombstone(any(), any());
    verify(oldRegionEntry, never()).removePhase1(any(), anyBoolean());
    verify(oldRegionEntry, never()).removePhase2();
    verify(regionMap, never()).removeEntry(any(), any(), anyBoolean());
  }

  @Test
  public void txApplyDestroyDoesCallsHandleDiskAccessException_givenOldRegionEntrySetValueThatThrowsDiskAccessException()
      throws Exception {
    givenLocalRegion();
    givenConcurrencyChecks();
    givenOldRegionEntry();
    doThrow(DiskAccessException.class).when(oldRegionEntry).setValue(any(), any());

    assertThatThrownBy(this::doTxApplyDestroy).isInstanceOf(DiskAccessException.class);

    verify(owner, times(1)).handleDiskAccessException(any());
  }

  // helper methods for the tests

  private void givenNoRegionEntry() {
    when(entryMap.get(eq(key))).thenReturn(null);
  }

  private void givenExistingRegionEntry() {
    when(entryMap.get(eq(key))).thenReturn(existingRegionEntry);
  }

  private void givenExistingRemovedRegionEntry() {
    givenExistingRegionEntry();
    when(existingRegionEntry.isDestroyedOrRemoved()).thenReturn(true);
  }

  private void givenFactoryRegionEntry() {
    when(entryMap.get(eq(key))).thenReturn(null);
    when(regionEntryFactory.createEntry(any(), any(), any())).thenReturn(factoryRegionEntry);
  }

  private void givenOldRegionEntry() {
    when(entryMap.get(eq(key))).thenReturn(null);
    when(regionEntryFactory.createEntry(any(), any(), any())).thenReturn(factoryRegionEntry);
    when(entryMap.putIfAbsent(eq(key), same(factoryRegionEntry))).thenReturn(oldRegionEntry);
  }

  private void givenOldRemovedRegionEntry() {
    givenOldRegionEntry();
    when(oldRegionEntry.isDestroyedOrRemoved()).thenReturn(true);
  }

  private void givenNotInTokenMode() {
    inTokenMode = false;
  }

  private void givenConcurrencyChecks() {
    when(owner.getConcurrencyChecksEnabled()).thenReturn(true);
    when(partitionedRegion.getConcurrencyChecksEnabled()).thenReturn(true);
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
    when(bucketRegion.getPartitionedRegion()).thenReturn(partitionedRegion);
    when(bucketRegion.getBucketAdvisor()).thenReturn(mock(BucketAdvisor.class));
    owner = bucketRegion;
    setupLocalRegion();
    regionMap = spy(new TestableAbstractRegionMap());
  }

  private void setupLocalRegion() {
    when(owner.getCachePerfStats()).thenReturn(cachePerfStats);
    when(owner.getCache()).thenReturn(mock(InternalCache.class));
    when(owner.getMyId()).thenReturn(myId);
    when(owner.getKeyInfo(any(), any(), any())).thenReturn(keyInfo);
  }

  private void doTxApplyDestroy() {
    regionMap.txApplyDestroy(key, txId, txEvent, inTokenMode, inRI, operation, eventId,
        aCallbackArgument, pendingCallbacks, filterRoutingInfo, bridgeContext, isOriginRemote,
        txEntryState, versionTag, tailKey);
  }

  private class TestableAbstractRegionMap extends AbstractRegionMap {

    TestableAbstractRegionMap() {
      super(null);
      initialize(owner, new Attributes(), null, false);
      setEntryMap(entryMap);
      setEntryFactory(regionEntryFactory);
    }

  }
}
