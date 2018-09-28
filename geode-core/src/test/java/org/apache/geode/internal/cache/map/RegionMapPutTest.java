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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntryEventSerialization;
import org.apache.geode.internal.cache.ImageState;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;

public class RegionMapPutTest {
  private final InternalRegion internalRegion = mock(InternalRegion.class);
  private final FocusedRegionMap focusedRegionMap = mock(FocusedRegionMap.class);
  private final CacheModificationLock cacheModificationLock = mock(CacheModificationLock.class);
  private final EntryEventSerialization entryEventSerialization =
      mock(EntryEventSerialization.class);
  private final RegionEntry createdRegionEntry = mock(RegionEntry.class);
  private final EntryEventImpl event = mock(EntryEventImpl.class);
  private boolean ifNew = false;
  private boolean ifOld = false;
  private boolean requireOldValue = false;
  private Object expectedOldValue = null;
  private boolean overwriteDestroyed = false;
  private RegionMapPut instance;
  private final RegionEntry existingRegionEntry = mock(RegionEntry.class);

  @Before
  public void setup() {
    RegionEntryFactory regionEntryFactory = mock(RegionEntryFactory.class);
    when(regionEntryFactory.createEntry(any(), any(), any())).thenReturn(createdRegionEntry);
    when(internalRegion.getScope()).thenReturn(Scope.LOCAL);
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(internalRegion.getAttributes()).thenReturn(mock(RegionAttributes.class));
    when(internalRegion.getImageState()).thenReturn(mock(ImageState.class));
    when(focusedRegionMap.getEntryMap()).thenReturn(mock(Map.class));
    when(focusedRegionMap.getEntryFactory()).thenReturn(regionEntryFactory);
    givenAnOperationThatDoesNotGuaranteeOldValue();
    when(event.getKey()).thenReturn("key");
    when(event.getRegion()).thenReturn(internalRegion);
    when(createdRegionEntry.getValueAsToken()).thenReturn(Token.REMOVED_PHASE1);
    when(createdRegionEntry.isRemoved()).thenReturn(true);
    when(createdRegionEntry.isDestroyedOrRemoved()).thenReturn(true);
  }

  private void createInstance() {
    instance = new RegionMapPut(focusedRegionMap, internalRegion, cacheModificationLock,
        entryEventSerialization, event, ifNew, ifOld, overwriteDestroyed, requireOldValue,
        expectedOldValue);
  }

  private RegionEntry doPut() {
    createInstance();
    return instance.put();
  }

  @Test
  public void doesNotSetEventOldValueToExistingRegionEntryValue_ifNotRequired() {
    givenExistingRegionEntry();
    givenAnOperationThatDoesNotGuaranteeOldValue();
    givenPutDoesNotNeedToDoCacheWrite();
    this.requireOldValue = false;

    Object oldValue = new Object();
    when(existingRegionEntry.getValue()).thenReturn(oldValue);

    doPut();

    verify(event, never()).setOldValue(any());
    verify(event, never()).setOldValue(any(), anyBoolean());
  }

  @Test
  public void setsEventOldValueToExistingRegionEntryValue_ifOldValueIsGatewaySenderEvent() {
    givenExistingRegionEntry();

    GatewaySenderEventImpl oldValue = new GatewaySenderEventImpl();
    when(existingRegionEntry.getValue()).thenReturn(oldValue);

    doPut();

    verify(event, times(1)).setOldValue(same(oldValue), eq(true));
    verify(event, never()).setOldValue(not(same(oldValue)), eq(true));
  }

  @Test
  public void setsEventOldValueToExistingRegionEntryValue_ifOperationGuaranteesOldValue() {
    givenExistingRegionEntry();
    givenAnOperationThatGuaranteesOldValue();

    Object oldValue = new Object();
    when(existingRegionEntry.getValueOffHeapOrDiskWithoutFaultIn(same(internalRegion)))
        .thenReturn(oldValue);

    doPut();

    verify(event, times(1)).setOldValue(same(oldValue), eq(true));
    verify(event, never()).setOldValue(not(same(oldValue)), eq(true));
  }

  @Test
  public void eventPutExistingEntryGivenOldValue_ifRetrieveOldValueForDelta()
      throws RegionClearedException {
    givenThatRunWhileEvictionDisabledCallsItsRunnable();
    givenExistingRegionEntry();
    Object oldValue = new Object();
    when(existingRegionEntry.getValue(any())).thenReturn(oldValue);
    when(event.getDeltaBytes()).thenReturn(new byte[1]);

    doPut();

    verify(event, times(1)).putExistingEntry(same(internalRegion), same(existingRegionEntry),
        eq(false), same(oldValue));
  }

  @Test
  public void eventPutExistingEntryGivenNullOldValue_ifNotRetrieveOldValueForDelta()
      throws RegionClearedException {
    givenThatRunWhileEvictionDisabledCallsItsRunnable();
    givenExistingRegionEntry();
    Object oldValue = new Object();
    when(existingRegionEntry.getValue(any())).thenReturn(oldValue);
    when(event.getDeltaBytes()).thenReturn(null);

    doPut();

    verify(event, times(1)).putExistingEntry(same(internalRegion), same(existingRegionEntry),
        eq(false), eq(null));
  }

  @Test
  public void setsEventOldValueToExistingRegionEntryValue_ifIsRequiredOldValueAndOperationDoesNotGuaranteeOldValue() {
    this.requireOldValue = true;
    givenExistingRegionEntry();
    givenAnOperationThatDoesNotGuaranteeOldValue();

    Object oldValue = new Object();
    when(existingRegionEntry.getValueRetain(same(internalRegion), eq(true))).thenReturn(oldValue);

    doPut();

    verify(event, times(1)).setOldValue(same(oldValue));
    verify(event, never()).setOldValue(not(same(oldValue)));
  }

  @Test
  public void setsEventOldValueToExistingRegionEntryValue_ifIsCacheWriteAndOperationDoesNotGuaranteeOldValue() {
    givenExistingRegionEntry();
    givenAnOperationThatDoesNotGuaranteeOldValue();
    givenPutNeedsToDoCacheWrite();

    Object oldValue = new Object();
    when(existingRegionEntry.getValueRetain(same(internalRegion), eq(true))).thenReturn(oldValue);

    doPut();

    verify(event, times(1)).setOldValue(same(oldValue));
    verify(event, never()).setOldValue(not(same(oldValue)));
  }

  @Test
  public void setsEventOldValueToNotAvailable_ifIsCacheWriteAndOperationDoesNotGuaranteeOldValue_andExistingValueIsNull() {
    givenPutNeedsToDoCacheWrite();
    givenAnOperationThatDoesNotGuaranteeOldValue();
    givenExistingRegionEntry();

    when(existingRegionEntry.getValueRetain(same(internalRegion), eq(true))).thenReturn(null);

    doPut();

    verify(event, times(1)).setOldValue(Token.NOT_AVAILABLE);
  }

  @Test
  public void retrieveOldValueForDeltaDefaultToFalse() {
    createInstance();

    assertThat(instance.isRetrieveOldValueForDelta()).isFalse();
  }


  @Test
  public void eventOldValueNotAvailableCalled_ifCacheWriteNotNeededAndNotInitialized() {
    givenPutDoesNotNeedToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(false);

    doPut();

    verify(event, times(1)).oldValueNotAvailable();
  }

  @Test
  public void eventOperationNotSet_ifCacheWriteNeededAndInitializedAndReplaceOnClient() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    givenReplaceOnClient();

    doPut();

    verify(event, never()).makeCreate();
    verify(event, never()).makeUpdate();
  }

  @Test
  public void putExistingEntryCalled_ifReplaceOnClient() throws RegionClearedException {
    givenPutDoesNotNeedToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    givenReplaceOnClient();

    doPut();

    verify(event, times(1)).putExistingEntry(any(), any(), anyBoolean(), any());
  }

  @Test
  public void eventOperationMadeCreate_ifCacheWriteNeededAndInitializedAndNotReplaceOnClientAndEntryRemoved() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    givenReplaceOnPeer();
    when(createdRegionEntry.isDestroyedOrRemoved()).thenReturn(true);

    doPut();

    verify(event, times(1)).makeCreate();
    verify(event, never()).makeUpdate();
  }

  @Test
  public void eventOperationMadeUpdate_ifCacheWriteNeededAndInitializedAndNotReplaceOnClientAndEntryExists() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    givenReplaceOnPeer();
    when(createdRegionEntry.isDestroyedOrRemoved()).thenReturn(false);

    doPut();

    verify(event, never()).makeCreate();
    verify(event, times(1)).makeUpdate();
  }

  @Test
  public void cacheWriteBeforePutNotCalled_ifNotInitialized() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(false);

    doPut();

    verify(internalRegion, never()).cacheWriteBeforePut(any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void cacheWriteBeforePutNotCalled_ifCacheWriteNotNeeded() {
    givenPutDoesNotNeedToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);

    doPut();

    verify(internalRegion, never()).cacheWriteBeforePut(any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void cacheWriteBeforePutCalledWithRequireOldValue_givenRequireOldValueTrue() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    this.requireOldValue = true;

    doPut();

    verify(internalRegion, times(1)).cacheWriteBeforePut(same(event), any(), any(),
        eq(this.requireOldValue), eq(null));
  }

  @Test
  public void cacheWriteBeforePutCalledWithExpectedOldValue_givenRequireOldValueTrue() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    givenAnOperationThatGuaranteesOldValue();
    givenExistingRegionEntry();
    when(existingRegionEntry.getValueRetain(same(internalRegion), eq(true))).thenReturn(null);
    expectedOldValue = "expectedOldValue";
    when(event.getRawOldValue()).thenReturn(expectedOldValue);

    doPut();

    verify(internalRegion, times(1)).cacheWriteBeforePut(same(event), any(), any(), anyBoolean(),
        same(expectedOldValue));
  }

  @Test
  public void putWithExpectedOldValueReturnsNull_ifExistingValueIsNotExpected() {
    givenAnOperationThatGuaranteesOldValue();
    expectedOldValue = "expectedOldValue";
    when(event.getRawOldValue()).thenReturn("unexpectedValue");

    RegionEntry result = doPut();

    assertThat(result).isNull();
  }

  @Test
  public void putWithExpectedOldValueReturnsRegionEntry_ifExistingValueIsExpected() {
    givenAnOperationThatGuaranteesOldValue();
    expectedOldValue = "expectedOldValue";
    when(event.getRawOldValue()).thenReturn(expectedOldValue);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(createdRegionEntry);
  }

  @Test
  public void putWithExpectedOldValueReturnsRegionEntry_ifExistingValueIsNotExpectedButIsReplaceOnClient() {
    givenAnOperationThatGuaranteesOldValue();
    expectedOldValue = "expectedOldValue";
    when(event.getRawOldValue()).thenReturn("unexpectedValue");
    givenReplaceOnClient();

    RegionEntry result = doPut();

    assertThat(result).isSameAs(createdRegionEntry);
  }

  @Test
  public void cacheWriteBeforePutCalledWithCacheWriter_givenACacheWriter() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    CacheWriter cacheWriter = mock(CacheWriter.class);
    when(internalRegion.basicGetWriter()).thenReturn(cacheWriter);

    doPut();

    verify(internalRegion, times(1)).cacheWriteBeforePut(same(event), eq(null), same(cacheWriter),
        anyBoolean(), eq(null));
  }

  @Test
  public void cacheWriteBeforePutCalledWithNetWriteRecipients_ifAdviseNetWrite() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.basicGetWriter()).thenReturn(null);
    Set netWriteRecipients = mock(Set.class);
    when(internalRegion.adviseNetWrite()).thenReturn(netWriteRecipients);

    doPut();

    verify(internalRegion, times(1)).cacheWriteBeforePut(same(event), eq(netWriteRecipients),
        eq(null), anyBoolean(), eq(null));
  }

  @Test
  public void retrieveOldValueForDeltaTrueIfEventHasDeltaBytes() {
    when(event.getDeltaBytes()).thenReturn(new byte[1]);

    createInstance();

    assertThat(instance.isRetrieveOldValueForDelta()).isTrue();
  }

  @Test
  public void retrieveOldValueForDeltaFalseIfEventHasDeltaBytesAndRawNewValue() {
    when(event.getDeltaBytes()).thenReturn(new byte[1]);
    when(event.getRawNewValue()).thenReturn(new Object());

    createInstance();

    assertThat(instance.isRetrieveOldValueForDelta()).isFalse();
  }

  @Test
  public void replaceOnClientDefaultsToFalse() {
    createInstance();

    assertThat(instance.isReplaceOnClient()).isFalse();
  }

  @Test
  public void replaceOnClientIsTrueIfOperationIsReplaceAndOwnerIsClient() {
    givenReplaceOnClient();

    createInstance();

    assertThat(instance.isReplaceOnClient()).isTrue();
  }

  @Test
  public void replaceOnClientIsFalseIfOperationIsReplaceAndOwnerIsNotClient() {
    givenReplaceOnPeer();

    createInstance();

    assertThat(instance.isReplaceOnClient()).isFalse();
  }

  @Test
  public void putReturnsNull_ifOnlyExistingAndEntryIsTombstone() {
    ifOld = true;
    givenExistingRegionEntry();
    when(existingRegionEntry.isTombstone()).thenReturn(true);

    RegionEntry result = doPut();

    assertThat(result).isNull();
  }

  @Test
  public void putReturnsExistingEntry_ifOnlyExistingAndEntryIsNotTombstone() {
    ifOld = true;
    givenExistingRegionEntry();
    when(existingRegionEntry.isTombstone()).thenReturn(false);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(existingRegionEntry);
  }

  @Test
  public void putReturnsNull_ifOnlyExistingAndEntryIsRemoved() {
    ifOld = true;
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(true);
    givenReplaceOnPeer();

    RegionEntry result = doPut();

    assertThat(result).isNull();
  }

  @Test
  public void putReturnsExistingEntry_ifOnlyExistingEntryIsRemovedAndReplaceOnClient() {
    ifOld = true;
    givenExistingRegionEntry();
    when(existingRegionEntry.isRemoved()).thenReturn(true);
    givenReplaceOnClient();

    RegionEntry result = doPut();

    assertThat(result).isSameAs(existingRegionEntry);
  }

  @Test
  public void putReturnsExistingEntry_ifReplaceOnClientAndTombstoneButNoVersionTag() {
    ifOld = true;
    givenReplaceOnClient();
    givenExistingRegionEntry();
    when(existingRegionEntry.isTombstone()).thenReturn(true);
    when(event.getVersionTag()).thenReturn(null);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(existingRegionEntry);
  }

  @Test
  public void putReturnsNull_ifReplaceOnClientAndTombstoneAndVersionTag()
      throws RegionClearedException {
    ifOld = true;
    givenReplaceOnClient();
    givenExistingRegionEntry();
    when(existingRegionEntry.isTombstone()).thenReturn(true);
    when(existingRegionEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    when(event.getVersionTag()).thenReturn(mock(VersionTag.class));

    RegionEntry result = doPut();

    assertThat(result).isNull();
    verify(existingRegionEntry, times(1)).setValue(internalRegion, Token.TOMBSTONE);
    verify(internalRegion, times(1)).rescheduleTombstone(same(existingRegionEntry), any());
  }

  @Test
  public void createWithoutOverwriteDestroyedReturnsNullAndCallsSetOldValueDestroyedToken_ifRegionUninitializedAndCurrentValueIsDestroyed() {
    overwriteDestroyed = false;
    when(internalRegion.isInitialized()).thenReturn(false);
    when(createdRegionEntry.getValueAsToken()).thenReturn(Token.DESTROYED);

    RegionEntry result = doPut();

    assertThat(result).isNull();
    verify(event, times(1)).setOldValueDestroyedToken();
  }

  @Test
  public void createWithoutOverwriteDestroyedReturnsNullAndCallsSetOldValueDestroyedToken_ifRegionUninitializedAndCurrentValueIsTombstone() {
    overwriteDestroyed = false;
    when(internalRegion.isInitialized()).thenReturn(false);
    when(createdRegionEntry.getValueAsToken()).thenReturn(Token.TOMBSTONE);

    RegionEntry result = doPut();

    assertThat(result).isNull();
    verify(event, times(1)).setOldValueDestroyedToken();
  }

  @Test
  public void createWithOverwriteDestroyedReturnsCreatedRegionEntry_ifRegionUninitializedAndCurrentValueIsDestroyed() {
    overwriteDestroyed = true;
    when(internalRegion.isInitialized()).thenReturn(false);
    when(createdRegionEntry.getValueAsToken()).thenReturn(Token.DESTROYED);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(createdRegionEntry);
    verify(event, never()).setOldValueDestroyedToken();
  }

  @Test
  public void putIgnoresRegionClearedException_ifReplaceOnClientAndTombstoneAndVersionTag()
      throws RegionClearedException {
    ifOld = true;
    givenReplaceOnClient();
    givenExistingRegionEntry();
    when(existingRegionEntry.isTombstone()).thenReturn(true);
    when(existingRegionEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    when(event.getVersionTag()).thenReturn(mock(VersionTag.class));
    doThrow(RegionClearedException.class).when(existingRegionEntry).setValue(internalRegion,
        Token.TOMBSTONE);

    RegionEntry result = doPut();

    assertThat(result).isNull();
    verify(existingRegionEntry, times(1)).setValue(internalRegion, Token.TOMBSTONE);
    verify(internalRegion, times(1)).rescheduleTombstone(same(existingRegionEntry), any());
  }

  @Test
  public void onlyExistingDefaultsToFalse() {
    createInstance();

    assertThat(instance.isOnlyExisting()).isFalse();
  }

  @Test
  public void onlyExistingIsTrueIfOld() {
    ifOld = true;

    createInstance();

    assertThat(instance.isOnlyExisting()).isTrue();
  }

  @Test
  public void onlyExistingIsFalseIfOldAndReplaceOnClient() {
    ifOld = true;
    givenReplaceOnClient();

    createInstance();

    assertThat(instance.isOnlyExisting()).isFalse();
  }

  @Test
  public void cacheWriteDefaultToFalse() {
    createInstance();

    assertThat(instance.isCacheWrite()).isFalse();
  }

  @Test
  public void cacheWriteIsFalseIfGenerateCallbacksButNotDistributedEtc() {
    when(event.isGenerateCallbacks()).thenReturn(true);
    createInstance();

    assertThat(instance.isCacheWrite()).isFalse();
  }

  @Test
  public void cacheWriteIsTrueIfGenerateCallbacksAndDistributed() {
    when(event.isGenerateCallbacks()).thenReturn(true);
    when(internalRegion.getScope()).thenReturn(Scope.DISTRIBUTED_ACK);
    createInstance();

    assertThat(instance.isCacheWrite()).isTrue();
  }

  @Test
  public void cacheWriteIsTrueIfGenerateCallbacksAndServerProxy() {
    when(event.isGenerateCallbacks()).thenReturn(true);
    when(internalRegion.hasServerProxy()).thenReturn(true);
    createInstance();

    assertThat(instance.isCacheWrite()).isTrue();
  }

  @Test
  public void cacheWriteIsTrueIfGenerateCallbacksAndCacheWriter() {
    when(event.isGenerateCallbacks()).thenReturn(true);
    when(internalRegion.basicGetWriter()).thenReturn(mock(CacheWriter.class));
    createInstance();

    assertThat(instance.isCacheWrite()).isTrue();
  }

  @Test
  public void isOriginRemoteCausesCacheWriteToBeFalse() {
    when(event.isOriginRemote()).thenReturn(true);
    when(event.isGenerateCallbacks()).thenReturn(true);
    when(internalRegion.basicGetWriter()).thenReturn(mock(CacheWriter.class));
    createInstance();

    assertThat(instance.isCacheWrite()).isFalse();
  }

  @Test
  public void netSearchCausesCacheWriteToBeFalse() {
    when(event.isNetSearch()).thenReturn(true);
    when(event.isGenerateCallbacks()).thenReturn(true);
    when(internalRegion.basicGetWriter()).thenReturn(mock(CacheWriter.class));
    createInstance();

    assertThat(instance.isCacheWrite()).isFalse();
  }

  @Test
  public void basicPutPart2ToldClearDidNotOccur_ifPutDoneWithoutAClear() throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);

    doPut();

    verify(internalRegion, times(1)).basicPutPart2(any(), any(), anyBoolean(), anyLong(),
        eq(false));
  }

  @Test
  public void basicPutPart2ToldClearDidOccur_ifPutDoneWithAClear() throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);
    doThrow(RegionClearedException.class).when(event).putNewEntry(any(), any());

    doPut();

    verify(internalRegion, times(1)).basicPutPart2(any(), any(), anyBoolean(), anyLong(), eq(true));
  }

  @Test
  public void lruUpdateCallbackCalled_ifPutDoneWithoutAClear() throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);

    doPut();

    verify(focusedRegionMap, times(1)).lruUpdateCallback();
  }

  @Test
  public void lruUpdateCallbackNotCalled_ifPutDoneWithAClear() throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);
    doThrow(RegionClearedException.class).when(event).putNewEntry(any(), any());

    doPut();

    verify(focusedRegionMap, never()).lruUpdateCallback();
  }

  @Test
  public void lruEnryCreateCalled_ifCreateDoneWithoutAClear() throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);

    doPut();

    verify(focusedRegionMap, times(1)).lruEntryCreate(createdRegionEntry);
  }

  @Test
  public void lruEnryCreateNotCalled_ifCreateDoneWithAClear() throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);
    doThrow(RegionClearedException.class).when(event).putNewEntry(any(), any());

    doPut();

    verify(focusedRegionMap, never()).lruEntryCreate(createdRegionEntry);
  }

  @Test
  public void lruEnryUpdateCalled_ifUpdateDoneWithoutAClear() throws Exception {
    ifOld = true;
    RegionEntry existingRegionEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(event)).thenReturn(existingRegionEntry);

    doPut();

    verify(focusedRegionMap, times(1)).lruEntryUpdate(existingRegionEntry);
  }

  @Test
  public void lruEnryUpdateNotCalled_ifUpdateDoneWithAClear() throws Exception {
    ifOld = true;
    RegionEntry existingRegionEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(event)).thenReturn(existingRegionEntry);
    doThrow(RegionClearedException.class).when(event).putExistingEntry(any(), any(), anyBoolean(),
        any());

    doPut();

    verify(focusedRegionMap, never()).lruEntryUpdate(existingRegionEntry);
  }

  @Test
  public void putThrows_ifCreateDoneWithConcurrentCacheModificationException() throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);
    doThrow(ConcurrentCacheModificationException.class).when(event).putNewEntry(any(), any());

    assertThatThrownBy(() -> doPut()).isInstanceOf(ConcurrentCacheModificationException.class);

    verify(event, times(1)).getVersionTag();
  }

  @Test
  public void putInvokesNotifyTimestampsToGateways_ifCreateDoneWithConcurrentCacheModificationException()
      throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);
    doThrow(ConcurrentCacheModificationException.class).when(event).putNewEntry(any(), any());
    VersionTag versionTag = mock(VersionTag.class);
    when(versionTag.isTimeStampUpdated()).thenReturn(true);
    when(event.getVersionTag()).thenReturn(versionTag);

    assertThatThrownBy(() -> doPut()).isInstanceOf(ConcurrentCacheModificationException.class);

    verify(internalRegion, times(1)).notifyTimestampsToGateways(same(event));
  }

  @Test
  public void putThrows_ifLruUpdateCallbackThrowsDiskAccessException() throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);
    doThrow(DiskAccessException.class).when(focusedRegionMap).lruUpdateCallback();

    assertThatThrownBy(() -> doPut()).isInstanceOf(DiskAccessException.class);

    verify(internalRegion, times(1)).handleDiskAccessException(any());
  }

  @Test
  public void createOnEmptyMapAddsEntry() throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(createdRegionEntry);
    verify(event, times(1)).putNewEntry(internalRegion, createdRegionEntry);
    verify(internalRegion, times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(internalRegion, times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }

  @Test
  public void putWithTombstoneNewValue_callsBasicPutPart3WithFalse() {
    when(event.basicGetNewValue()).thenReturn(Token.TOMBSTONE);

    doPut();

    verify(internalRegion, times(1)).basicPutPart3(any(), any(), anyBoolean(), anyLong(), eq(false),
        anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void putWithNonTombstoneNewValue_callsBasicPutPart3WithTrue() {
    when(event.basicGetNewValue()).thenReturn("newValue");

    doPut();

    verify(internalRegion, times(1)).basicPutPart3(any(), any(), anyBoolean(), anyLong(), eq(true),
        anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void putOnEmptyMapAddsEntry() throws Exception {
    ifNew = false;
    ifOld = false;
    when(event.getOperation()).thenReturn(Operation.CREATE);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(createdRegionEntry);
    verify(event, times(1)).putNewEntry(internalRegion, createdRegionEntry);
    verify(internalRegion, times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(internalRegion, times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }

  @Test
  public void updateOnEmptyMapReturnsNull() throws Exception {
    ifOld = true;

    RegionEntry result = doPut();

    assertThat(result).isNull();
    verify(event, never()).putNewEntry(any(), any());
    verify(event, never()).putExistingEntry(any(), any(), anyBoolean(), any());
    verify(internalRegion, never()).basicPutPart2(any(), any(), anyBoolean(), anyLong(),
        anyBoolean());
    verify(internalRegion, never()).basicPutPart3(any(), any(), anyBoolean(), anyLong(),
        anyBoolean(), anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void createOnExistingEntryReturnsNull() throws RegionClearedException {
    ifNew = true;
    when(focusedRegionMap.getEntry(event)).thenReturn(mock(RegionEntry.class));
    when(event.getOperation()).thenReturn(Operation.CREATE);

    RegionEntry result = doPut();

    assertThat(result).isNull();
    verify(event, never()).putNewEntry(any(), any());
    verify(event, never()).putExistingEntry(any(), any(), anyBoolean(), any());
    verify(internalRegion, never()).basicPutPart2(any(), any(), anyBoolean(), anyLong(),
        anyBoolean());
    verify(internalRegion, never()).basicPutPart3(any(), any(), anyBoolean(), anyLong(),
        anyBoolean(), anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void createOnEntryReturnedFromPutIfAbsentDoesNothing() throws RegionClearedException {
    ifNew = true;
    when(focusedRegionMap.getEntry(event)).thenReturn(mock(RegionEntry.class));
    when(focusedRegionMap.putEntryIfAbsent(event.getKey(), createdRegionEntry))
        .thenReturn(mock(RegionEntry.class));
    when(event.getOperation()).thenReturn(Operation.CREATE);

    RegionEntry result = doPut();

    assertThat(result).isNull();
    verify(event, never()).putNewEntry(any(), any());
    verify(event, never()).putExistingEntry(any(), any(), anyBoolean(), any());
    verify(internalRegion, never()).basicPutPart2(any(), any(), anyBoolean(), anyLong(),
        anyBoolean());
    verify(internalRegion, never()).basicPutPart3(any(), any(), anyBoolean(), anyLong(),
        anyBoolean(), anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void createOnExistingEntryWithRemovePhase2DoesCreate() throws RegionClearedException {
    ifNew = true;
    RegionEntry existingRegionEntry = mock(RegionEntry.class);
    when(existingRegionEntry.isRemovedPhase2()).thenReturn(true);
    when(focusedRegionMap.putEntryIfAbsent(event.getKey(), createdRegionEntry))
        .thenReturn(existingRegionEntry).thenReturn(null);
    when(event.getOperation()).thenReturn(Operation.CREATE);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(createdRegionEntry);
    verify(event, times(1)).putNewEntry(internalRegion, createdRegionEntry);
    verify(internalRegion, times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(internalRegion, times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }

  @Test
  public void updateOnExistingEntryDoesUpdate() throws Exception {
    ifOld = true;
    Object updateValue = "update";
    RegionEntry existingRegionEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(event)).thenReturn(existingRegionEntry);
    when(event.getNewValue()).thenReturn(updateValue);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(existingRegionEntry);
    verify(event, times(1)).putExistingEntry(eq(internalRegion), eq(existingRegionEntry),
        anyBoolean(), any());
    verify(internalRegion, times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(internalRegion, times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }

  @Test
  public void putOnExistingEntryDoesUpdate() throws Exception {
    ifOld = false;
    ifNew = false;
    Object updateValue = "update";
    RegionEntry existingRegionEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(event)).thenReturn(existingRegionEntry);
    when(event.getNewValue()).thenReturn(updateValue);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(existingRegionEntry);
    verify(event, times(1)).putExistingEntry(eq(internalRegion), eq(existingRegionEntry),
        anyBoolean(), any());
    verify(internalRegion, times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(internalRegion, times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }

  @Test
  public void runWileLockedForCacheModificationDoesNotLockGIIClearLockWhenRegionIsInitialized()
      throws Exception {
    DistributedRegion region = mock(DistributedRegion.class);
    when(region.isInitialized()).thenReturn(true);
    when(region.lockWhenRegionIsInitializing()).thenCallRealMethod();
    RegionMapPut regionMapPut = new RegionMapPut(focusedRegionMap, region, cacheModificationLock,
        entryEventSerialization, event, ifNew, ifOld, overwriteDestroyed, requireOldValue,
        expectedOldValue);

    regionMapPut.runWhileLockedForCacheModification(() -> {
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
    RegionMapPut regionMapPut = new RegionMapPut(focusedRegionMap, region, cacheModificationLock,
        entryEventSerialization, event, ifNew, ifOld, overwriteDestroyed, requireOldValue,
        expectedOldValue);

    regionMapPut.runWhileLockedForCacheModification(() -> {
    });

    verify(region).lockWhenRegionIsInitializing();
    assertThat(region.lockWhenRegionIsInitializing()).isTrue();
    verify(region).unlockWhenRegionIsInitializing();
  }

  private void givenAnOperationThatDoesNotGuaranteeOldValue() {
    when(event.getOperation()).thenReturn(Operation.UPDATE);
  }

  private void givenAnOperationThatGuaranteesOldValue() {
    when(event.getOperation()).thenReturn(Operation.PUT_IF_ABSENT);
  }

  private void givenPutNeedsToDoCacheWrite() {
    when(event.isGenerateCallbacks()).thenReturn(true);
    when(internalRegion.getScope()).thenReturn(Scope.DISTRIBUTED_ACK);
  }

  private void givenPutDoesNotNeedToDoCacheWrite() {
    when(event.isGenerateCallbacks()).thenReturn(false);
    when(internalRegion.getScope()).thenReturn(Scope.DISTRIBUTED_ACK);
  }

  private void givenExistingRegionEntry() {
    when(focusedRegionMap.getEntry(event)).thenReturn(existingRegionEntry);
  }

  private void givenReplaceOnClient() {
    when(event.getOperation()).thenReturn(Operation.REPLACE);
    when(internalRegion.hasServerProxy()).thenReturn(true);
  }

  private void givenReplaceOnPeer() {
    when(event.getOperation()).thenReturn(Operation.REPLACE);
    when(internalRegion.hasServerProxy()).thenReturn(false);
  }

  private void givenThatRunWhileEvictionDisabledCallsItsRunnable() {
    doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
    }).when(focusedRegionMap).runWhileEvictionDisabled(any());
  }

}
