/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.TXEntryState;
import org.apache.geode.internal.cache.TXRmtEvent;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.versions.VersionTag;

public class RegionMapCommitPutTest {
  private final InternalRegion internalRegion = mock(InternalRegion.class);
  private final FocusedRegionMap focusedRegionMap = mock(FocusedRegionMap.class);
  @SuppressWarnings("rawtypes")
  private final Map entryMap = mock(Map.class);
  private final EntryEventImpl event = mock(EntryEventImpl.class);
  private final RegionEntry regionEntry = mock(RegionEntry.class);
  private final TransactionId transactionId = mock(TransactionId.class);
  private final TXRmtEvent txRmtEvent = mock(TXRmtEvent.class);
  private final List<EntryEventImpl> pendingCallbacks = new ArrayList<>();
  private final TXEntryState localTxEntryState = mock(TXEntryState.class);
  private final InternalDistributedMember myId = mock(InternalDistributedMember.class);
  private final InternalDistributedMember remoteId = mock(InternalDistributedMember.class);
  private final Object key = "theKey";
  private final Object newValue = "newValue";
  private RegionMapCommitPut instance;

  @Before
  public void setup() {
    RegionEntryFactory regionEntryFactory = mock(RegionEntryFactory.class);
    when(regionEntryFactory.createEntry(any(), any(), any())).thenReturn(regionEntry);
    when(focusedRegionMap.getEntryFactory()).thenReturn(regionEntryFactory);
    when(focusedRegionMap.getEntryMap()).thenReturn(entryMap);
    when(internalRegion.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(internalRegion.getCache()).thenReturn(mock(InternalCache.class));
    when(internalRegion.getMyId()).thenReturn(myId);
    when(transactionId.getMemberId()).thenReturn(myId);
    when(event.getRawNewValueAsHeapObject()).thenReturn(newValue);
    when(event.getKey()).thenReturn(key);
  }

  private void createInstance(Operation putOp, boolean didDestroy, TXRmtEvent txEvent,
      TXEntryState txEntryState) {
    instance = new RegionMapCommitPut(focusedRegionMap, internalRegion, event, putOp, didDestroy,
        transactionId, txEvent, pendingCallbacks, txEntryState);
  }

  @Test
  public void localCreateIsNotRemoteOrigin() {
    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isRemoteOrigin()).isFalse();
  }

  @Test
  public void remoteCreateIsRemoteOrigin() {
    when(transactionId.getMemberId()).thenReturn(remoteId);

    createInstance(Operation.CREATE, false, txRmtEvent, null);

    assertThat(instance.isRemoteOrigin()).isTrue();
  }

  @Test
  public void localCreateIsNotOnlyExisting() {
    when(transactionId.getMemberId()).thenReturn(myId);

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isOnlyExisting()).isFalse();
  }

  @Test
  public void remoteCreateWithNotAllEventsIsOnlyExisting() {
    when(transactionId.getMemberId()).thenReturn(remoteId);
    when(internalRegion.isAllEvents()).thenReturn(false);

    createInstance(Operation.CREATE, false, txRmtEvent, null);

    assertThat(instance.isOnlyExisting()).isTrue();
  }

  @Test
  public void remoteCreateWithLocalTxEntryStateIsNotOnlyExisting() {
    when(transactionId.getMemberId()).thenReturn(remoteId);

    createInstance(Operation.CREATE, false, txRmtEvent, localTxEntryState);

    assertThat(instance.isOnlyExisting()).isFalse();
  }

  @Test
  public void remoteCreateWithAllEventsIsNotOnlyExisting() {
    when(transactionId.getMemberId()).thenReturn(remoteId);
    when(internalRegion.isAllEvents()).thenReturn(true);

    createInstance(Operation.CREATE, false, txRmtEvent, null);

    assertThat(instance.isOnlyExisting()).isFalse();
  }

  @Test
  public void remoteUpdateWithAllEventsIsNotOnlyExisting() {
    when(transactionId.getMemberId()).thenReturn(remoteId);
    when(internalRegion.isAllEvents()).thenReturn(true);

    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    assertThat(instance.isOnlyExisting()).isFalse();
  }

  @Test
  public void uninitializedNonPartitionedRegionDoesNotInvokeCallbacks() {
    when(internalRegion.shouldDispatchListenerEvent()).thenReturn(true);
    when(internalRegion.shouldNotifyBridgeClients()).thenReturn(true);
    when(internalRegion.getConcurrencyChecksEnabled()).thenReturn(true);

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isInvokeCallbacks()).isFalse();
  }

  //
  @Test
  public void uninitializedPartitionedRegionDoesNotInvokeCallbacks() {
    when(internalRegion.isUsedForPartitionedRegionBucket()).thenReturn(true);
    when(internalRegion.getPartitionedRegion()).thenReturn(mock(LocalRegion.class));

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isInvokeCallbacks()).isFalse();
  }

  @Test
  public void initializedNonPartitionedRegionWithFalseAttributesDoesNotInvokeCallbacks() {
    when(internalRegion.isInitialized()).thenReturn(true);

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isInvokeCallbacks()).isFalse();
  }

  @Test
  public void initializedNonPartitionedRegionWithShouldDispatchListenerEventDoesInvokeCallbacks() {
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.shouldDispatchListenerEvent()).thenReturn(true);

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isInvokeCallbacks()).isTrue();
  }

  @Test
  public void initializedNonPartitionedRegionWithShouldNotifyBridgeClientsDoesInvokeCallbacks() {
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.shouldNotifyBridgeClients()).thenReturn(true);

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isInvokeCallbacks()).isTrue();
  }

  @Test
  public void initializedNonPartitionedRegionWithConcurrencyChecksEnabledDoesInvokeCallbacks() {
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.getConcurrencyChecksEnabled()).thenReturn(true);

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isInvokeCallbacks()).isTrue();
  }

  @Test
  public void uninitializedPartitionedRegionWithFalseAttributesDoesNotInvokeCallbacks() {
    when(internalRegion.isUsedForPartitionedRegionBucket()).thenReturn(true);
    when(internalRegion.getPartitionedRegion()).thenReturn(mock(LocalRegion.class));

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isInvokeCallbacks()).isFalse();
  }

  @Test
  public void uninitializedPartitionedRegionWithShouldDispatchListenerEventDoesInvokeCallbacks() {
    when(internalRegion.isUsedForPartitionedRegionBucket()).thenReturn(true);
    LocalRegion partitionedRegion = mock(LocalRegion.class);
    when(internalRegion.getPartitionedRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.shouldDispatchListenerEvent()).thenReturn(true);

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isInvokeCallbacks()).isTrue();
  }

  @Test
  public void uninitializedPartitionedRegionWithShouldNotifyBridgeClientsDoesInvokeCallbacks() {
    when(internalRegion.isUsedForPartitionedRegionBucket()).thenReturn(true);
    LocalRegion partitionedRegion = mock(LocalRegion.class);
    when(internalRegion.getPartitionedRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.shouldNotifyBridgeClients()).thenReturn(true);

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isInvokeCallbacks()).isTrue();
  }

  @Test
  public void uninitializedPartitionedRegionWithConcurrencyChecksEnabledDoesInvokeCallbacks() {
    when(internalRegion.isUsedForPartitionedRegionBucket()).thenReturn(true);
    LocalRegion partitionedRegion = mock(LocalRegion.class);
    when(internalRegion.getPartitionedRegion()).thenReturn(partitionedRegion);
    when(partitionedRegion.getConcurrencyChecksEnabled()).thenReturn(true);

    createInstance(Operation.CREATE, false, null, localTxEntryState);

    assertThat(instance.isInvokeCallbacks()).isTrue();
  }

  @Test
  public void remoteUpdateWithAllEventsAndInitializedIsOnlyExisting() {
    when(transactionId.getMemberId()).thenReturn(remoteId);
    when(internalRegion.isAllEvents()).thenReturn(true);
    when(internalRegion.isInitialized()).thenReturn(true);

    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    assertThat(instance.isOnlyExisting()).isTrue();
  }

  @Test
  public void successfulPutCallsUpdateStatsForPut() {
    createInstance(Operation.CREATE, false, null, localTxEntryState);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(regionEntry);
    final long lastModifiedTime = instance.getLastModifiedTime();
    verify(regionEntry, times(1)).updateStatsForPut(eq(lastModifiedTime), eq(lastModifiedTime));
  }

  @Test
  public void doBeforeCompletionActionsCallsTxApplyPutPart2() {
    final boolean didDestroy = true;
    createInstance(Operation.CREATE, didDestroy, null, localTxEntryState);

    instance.put();

    final long lastModifiedTime = instance.getLastModifiedTime();
    verify(internalRegion, times(1)).txApplyPutPart2(eq(regionEntry), eq(key), eq(lastModifiedTime),
        eq(true), eq(didDestroy), eq(false));
  }

  @Test
  public void putThatDoesNotAddEventToPendingCallbacksCallsEventRelease() {
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    instance.put();

    assertThat(instance.isCallbackEventInPending()).isFalse();
    assertThat(pendingCallbacks).doesNotContain(event);
    verify(event, never()).changeRegionToBucketsOwner();
    verify(event, never()).setOriginRemote(anyBoolean());
    verify(event, times(1)).release();
  }

  @Test
  public void putThatDoesAddEventToPendingCallbacksDoesNotCallEventRelease() {
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isNotNull();
    verify(event, never()).release();
  }

  @Test
  public void putThatDoesNotInvokesCallbacksDoesNotAddToPendingCallbacks() {
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    instance.put();

    assertThat(instance.isCallbackEventInPending()).isFalse();
    assertThat(pendingCallbacks).doesNotContain(event);
    verify(event, never()).changeRegionToBucketsOwner();
    verify(event, never()).setOriginRemote(anyBoolean());
  }

  @Test
  public void putThatInvokesCallbacksAddsToPendingCallbacks() {
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isNotNull();
    assertThat(instance.isCallbackEventInPending()).isTrue();
    assertThat(pendingCallbacks).contains(event);
    verify(event, times(1)).changeRegionToBucketsOwner();
    verify(event, times(1)).setOriginRemote(instance.isRemoteOrigin());
  }

  @Test
  public void updateThatDoesNotSeeClearCallsLruEntryUpdate() {
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    instance.put();

    verify(focusedRegionMap, times(1)).lruEntryUpdate(existingEntry);
  }

  @Test
  public void createThatDoesNotSeeClearCallsLruEntryCreate() {
    createInstance(Operation.CREATE, false, null, localTxEntryState);

    instance.put();

    verify(focusedRegionMap, times(1)).lruEntryCreate(regionEntry);
    verify(focusedRegionMap, times(1)).incEntryCount(1);
  }

  @Test
  public void createCallsUpdateSizeOnCreate() {
    final int newSize = 79;
    when(internalRegion.calculateRegionEntryValueSize(eq(regionEntry))).thenReturn(newSize);
    createInstance(Operation.CREATE, false, null, localTxEntryState);

    instance.put();

    verify(internalRegion, times(1)).updateSizeOnCreate(eq(key), eq(newSize));
  }

  @Test
  public void updateCallsUpdateSizeOnPut() {
    final int oldSize = 12;
    final int newSize = 79;
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    when(internalRegion.calculateRegionEntryValueSize(eq(existingEntry))).thenReturn(oldSize)
        .thenReturn(newSize);
    createInstance(Operation.UPDATE, false, null, localTxEntryState);

    instance.put();

    verify(internalRegion, times(1)).updateSizeOnPut(eq(key), eq(oldSize), eq(newSize));
  }

  @Test
  public void createThatDoesSeeClearDoesNotMakeLruCalls() throws RegionClearedException {
    doThrow(RegionClearedException.class).when(regionEntry).setValue(any(), any());
    createInstance(Operation.CREATE, false, null, localTxEntryState);

    instance.put();

    assertThat(instance.isClearOccurred()).isTrue();
    verify(focusedRegionMap, never()).lruEntryUpdate(any());
    verify(focusedRegionMap, never()).lruEntryCreate(any());
    verify(focusedRegionMap, never()).incEntryCount(1);
  }

  @Test
  public void putWithoutConcurrencyChecksEnabledDoesNotCallSetVersionTag() {
    createInstance(Operation.UPDATE, false, null, localTxEntryState);

    instance.put();

    verify(localTxEntryState, never()).setVersionTag(any());
  }

  @Test
  public void putWithConcurrencyChecksEnabledDoesCallSetVersionTag() {
    when(internalRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    VersionTag versionTag = mock(VersionTag.class);
    when(event.getVersionTag()).thenReturn(versionTag);
    createInstance(Operation.UPDATE, false, null, localTxEntryState);

    instance.put();

    verify(localTxEntryState, times(1)).setVersionTag(eq(versionTag));
  }

  @Test
  public void failedUpdateWithDidDestroyDoesCallTxApplyPutHandleDidDestroy() {
    when(transactionId.getMemberId()).thenReturn(remoteId);
    when(internalRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    createInstance(Operation.UPDATE, true, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isNull();
    assertThat(instance.isOnlyExisting()).isTrue();
    verify(internalRegion, times(1)).txApplyPutHandleDidDestroy(eq(key));
  }

  @Test
  public void successfulUpdateWithDidDestroyDoesNotCallTxApplyPutHandleDidDestroy() {
    when(transactionId.getMemberId()).thenReturn(remoteId);
    when(internalRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    createInstance(Operation.UPDATE, true, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isNotNull();
    assertThat(instance.isOnlyExisting()).isTrue();
    verify(internalRegion, never()).txApplyPutHandleDidDestroy(any());
  }

  @Test
  public void failedUpdateDoesCallInvokeTXCallbacks() {
    when(transactionId.getMemberId()).thenReturn(remoteId);
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isNull();
    assertThat(instance.isOnlyExisting()).isTrue();
    assertThat(instance.isInvokeCallbacks()).isTrue();
    verify(event, times(1)).makeUpdate();
    verify(internalRegion, times(1)).invokeTXCallbacks(eq(EnumListenerEvent.AFTER_UPDATE),
        eq(event), eq(false));
  }

  @Test
  public void successfulUpdateDoesNotCallInvokeTXCallbacks() {
    when(transactionId.getMemberId()).thenReturn(remoteId);
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isNotNull();
    assertThat(instance.isOnlyExisting()).isTrue();
    assertThat(instance.isInvokeCallbacks()).isTrue();
    verify(internalRegion, never()).invokeTXCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void localUpdateSetsOldValueOnEvent() {
    Object oldValue = new Object();
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.getValueInVM(any())).thenReturn(oldValue);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    createInstance(Operation.UPDATE, false, null, localTxEntryState);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(existingEntry);
    verify(event, times(1)).setOldValue(oldValue);
    verify(existingEntry, never()).txDidDestroy(anyLong());
  }

  @Test
  public void localUpdateThatAlsoDidDestroyCallsTxDidDestroy() {
    Object oldValue = new Object();
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.getValueInVM(any())).thenReturn(oldValue);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    final long lastModifiedTime = 123L;
    when(internalRegion.cacheTimeMillis()).thenReturn(lastModifiedTime);
    createInstance(Operation.UPDATE, true, null, localTxEntryState);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(existingEntry);
    verify(event, times(1)).setOldValue(oldValue);
    verify(existingEntry, times(1)).txDidDestroy(lastModifiedTime);
  }

  @Test
  public void localCreateDoesSetsOldValueToNullOnEvent() {
    createInstance(Operation.CREATE, false, null, localTxEntryState);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(regionEntry);
    verify(event, times(1)).setOldValue(null);
  }

  @Test
  public void localCreateCallsProcessAndGenerateTXVersionTag() {
    createInstance(Operation.SEARCH_CREATE, false, null, localTxEntryState);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(regionEntry);
    verify(focusedRegionMap, times(1)).processAndGenerateTXVersionTag(eq(event), eq(regionEntry),
        eq(localTxEntryState));
  }

  @Test
  public void localSearchCreateCallsSetValueResultOfSearchWithTrue() {
    createInstance(Operation.SEARCH_CREATE, false, null, localTxEntryState);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(regionEntry);
    verify(regionEntry, times(1)).setValueResultOfSearch(true);
  }

  @Test
  public void remoteUpdateWithOnlyExistingSucceeds() throws Exception {
    when(internalRegion.isAllEvents()).thenReturn(true);
    when(internalRegion.isInitialized()).thenReturn(true);
    Object oldValue = new Object();
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.getValueInVM(any())).thenReturn(oldValue);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    when(existingEntry.prepareValueForCache(any(), eq(newValue), eq(event), eq(true)))
        .thenReturn(newValue);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(existingEntry);
    verify(existingEntry, times(1)).setValue(eq(internalRegion), eq(newValue));
  }

  @Test
  public void remoteUpdateWithOnlyExistingCallsAddPut() throws Exception {
    when(internalRegion.isAllEvents()).thenReturn(true);
    when(internalRegion.isInitialized()).thenReturn(true);
    Object oldValue = new Object();
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.getValueInVM(any())).thenReturn(oldValue);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    when(existingEntry.prepareValueForCache(any(), eq(newValue), eq(event), eq(true)))
        .thenReturn(newValue);
    final Object callbackArgument = "callbackArgument";
    when(event.getCallbackArgument()).thenReturn(callbackArgument);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(existingEntry);
    verify(txRmtEvent, times(1)).addPut(eq(Operation.UPDATE), eq(internalRegion), eq(existingEntry),
        eq(key), eq(newValue), eq(callbackArgument));
  }

  @Test
  public void remoteUpdateWithInvalidateWithOnlyExistingSucceeds() throws Exception {
    when(internalRegion.isAllEvents()).thenReturn(true);
    when(internalRegion.isInitialized()).thenReturn(true);
    Object oldValue = new Object();
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.getValueInVM(any())).thenReturn(oldValue);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    when(event.getRawNewValueAsHeapObject()).thenReturn(null);
    when(existingEntry.prepareValueForCache(any(), eq(Token.INVALID), eq(event), eq(true)))
        .thenReturn(Token.INVALID);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(existingEntry);
    verify(existingEntry, times(1)).setValue(eq(internalRegion), eq(Token.INVALID));
  }

  @Test
  public void remoteUpdateWithLocalInvalidateWithOnlyExistingSucceeds() throws Exception {
    when(internalRegion.isAllEvents()).thenReturn(true);
    when(internalRegion.isInitialized()).thenReturn(true);
    Object oldValue = new Object();
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.getValueInVM(any())).thenReturn(oldValue);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    when(event.getRawNewValueAsHeapObject()).thenReturn(null);
    when(event.isLocalInvalid()).thenReturn(true);
    when(existingEntry.prepareValueForCache(any(), eq(Token.LOCAL_INVALID), eq(event), eq(true)))
        .thenReturn(Token.LOCAL_INVALID);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(existingEntry);
    verify(existingEntry, times(1)).setValue(eq(internalRegion), eq(Token.LOCAL_INVALID));
  }

  @Test
  public void remoteUpdateWithOnlyExistingOnRemovedEntryFails() {
    when(transactionId.getMemberId()).thenReturn(remoteId);
    when(internalRegion.isAllEvents()).thenReturn(true);
    when(internalRegion.isInitialized()).thenReturn(true);
    Object oldValue = new Object();
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.getValueInVM(any())).thenReturn(oldValue);
    when(existingEntry.isDestroyedOrRemoved()).thenReturn(false).thenReturn(true);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isNull();
  }

  @Test
  public void putThatUpdatesTombstoneCallsUnscheduleTombstone() {
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.isTombstone()).thenReturn(true);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    RegionEntry result = instance.put();

    assertThat(result).isNotNull();
    verify(internalRegion, times(1)).unscheduleTombstone(eq(existingEntry));
  }



  @Test
  public void entryExistsWithNullReturnsFalse() {
    createInstance(Operation.UPDATE, false, txRmtEvent, null);

    boolean result = instance.entryExists(null);

    assertThat(result).isFalse();
  }

  @Test
  public void entryExistsWithRemovedEntryReturnsFalse() {
    createInstance(Operation.UPDATE, false, txRmtEvent, null);
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(regionEntry.isDestroyedOrRemoved()).thenReturn(true);

    boolean result = instance.entryExists(regionEntry);

    assertThat(result).isFalse();
  }

  @Test
  public void entryExistsWithExistingEntryReturnsTrue() {
    createInstance(Operation.UPDATE, false, txRmtEvent, null);
    RegionEntry regionEntry = mock(RegionEntry.class);

    boolean result = instance.entryExists(regionEntry);

    assertThat(result).isTrue();
  }

  @Test
  public void runWileLockedForCacheModificationDoesNotLockGIIClearLockWhenRegionIsInitialized()
      throws Exception {
    DistributedRegion region = mock(DistributedRegion.class);
    when(region.isInitialized()).thenReturn(true);
    when(region.lockWhenRegionIsInitializing()).thenCallRealMethod();
    RegionMapCommitPut regionMapCommitPut =
        new RegionMapCommitPut(focusedRegionMap, region, event, Operation.UPDATE, false,
            transactionId, txRmtEvent, pendingCallbacks, null);


    regionMapCommitPut.runWhileLockedForCacheModification(() -> {
    });

    verify(region).lockWhenRegionIsInitializing();
    assertThat(region.lockWhenRegionIsInitializing()).isFalse();
    verify(region, never()).unlockWhenRegionIsInitializing();
  }

  @Test
  public void runWileLockedForCacheModificationLockGIIClearLockWhenRegionIsInitializing() {
    DistributedRegion region = mock(DistributedRegion.class);
    when(region.isInitialized()).thenReturn(false);
    when(region.lockWhenRegionIsInitializing()).thenCallRealMethod();
    RegionMapCommitPut regionMapCommitPut =
        new RegionMapCommitPut(focusedRegionMap, region, event, Operation.UPDATE, false,
            transactionId, txRmtEvent, pendingCallbacks, null);

    regionMapCommitPut.runWhileLockedForCacheModification(() -> {
    });

    verify(region).lockWhenRegionIsInitializing();
    assertThat(region.lockWhenRegionIsInitializing()).isTrue();
    verify(region).unlockWhenRegionIsInitializing();
  }

}
