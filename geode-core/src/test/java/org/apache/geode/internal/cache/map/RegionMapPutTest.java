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
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntryEventSerialization;
import org.apache.geode.internal.cache.ImageState;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionMapPutTest {
  private final InternalRegion internalRegion = mock(InternalRegion.class);
  private final FocusedRegionMap focusedRegionMap = mock(FocusedRegionMap.class);
  private final CacheModificationLock cacheModificationLock = mock(CacheModificationLock.class);
  private final EntryEventSerialization entryEventSerialization =
      mock(EntryEventSerialization.class);
  private final RegionEntry regionEntry = mock(RegionEntry.class);
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
    when(regionEntryFactory.createEntry(any(), any(), any())).thenReturn(regionEntry);
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
    when(regionEntry.getValueAsToken()).thenReturn(Token.REMOVED_PHASE1);
    when(regionEntry.isRemoved()).thenReturn(true);
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
    givenExistingRegionEntry();
    Object oldValue = new Object();
    when(existingRegionEntry.getValue(any())).thenReturn(oldValue);
    when(event.getDeltaBytes()).thenReturn(new byte[1]);
    doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
    }).when(focusedRegionMap).runWhileEvictionDisabled(any());

    doPut();

    verify(event, times(1)).putExistingEntry(same(internalRegion), same(existingRegionEntry),
        eq(false), same(oldValue));
  }

  @Test
  public void eventPutExistingEntryGivenNullOldValue_ifNotRetrieveOldValueForDelta()
      throws RegionClearedException {
    givenExistingRegionEntry();
    Object oldValue = new Object();
    when(existingRegionEntry.getValue(any())).thenReturn(oldValue);
    when(event.getDeltaBytes()).thenReturn(null);
    doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
    }).when(focusedRegionMap).runWhileEvictionDisabled(any());

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
    when(event.getOperation()).thenReturn(Operation.REPLACE);
    when(internalRegion.hasServerProxy()).thenReturn(true);

    doPut();

    verify(event, never()).makeCreate();
    verify(event, never()).makeUpdate();
  }

  @Test
  public void eventOperationMadeCreate_ifCacheWriteNeededAndInitializedAndNotReplaceOnClientAndEntryRemoved() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    when(event.getOperation()).thenReturn(Operation.REPLACE);
    when(internalRegion.hasServerProxy()).thenReturn(false);
    when(regionEntry.isDestroyedOrRemoved()).thenReturn(true);

    doPut();

    verify(event, times(1)).makeCreate();
    verify(event, never()).makeUpdate();
  }

  @Test
  public void eventOperationMadeUpdate_ifCacheWriteNeededAndInitializedAndNotReplaceOnClientAndEntryExists() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    when(event.getOperation()).thenReturn(Operation.REPLACE);
    when(internalRegion.hasServerProxy()).thenReturn(false);
    when(regionEntry.isDestroyedOrRemoved()).thenReturn(false);

    doPut();

    verify(event, never()).makeCreate();
    verify(event, times(1)).makeUpdate();
  }

  @Test
  public void cacheWriterBeforePutNotCalled_ifNotInitialized() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(false);

    doPut();

    verify(internalRegion, never()).cacheWriteBeforePut(any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void cacheWriterBeforePutNotCalled_ifCacheWriteNotNeeded() {
    givenPutDoesNotNeedToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);

    doPut();

    verify(internalRegion, never()).cacheWriteBeforePut(any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void cacheWriterBeforePutCalledWithRequireOldValue_givenRequireOldValueTrue() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    this.requireOldValue = true;

    doPut();

    verify(internalRegion, times(1)).cacheWriteBeforePut(same(event), any(), any(),
        eq(this.requireOldValue), eq(null));
  }

  @Test
  public void cacheWriterBeforePutCalledWithExpectedOldValue_givenRequireOldValueTrue() {
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
  public void cacheWriterBeforePutCalledWithCacheWriter_givenACacheWriter() {
    givenPutNeedsToDoCacheWrite();
    when(internalRegion.isInitialized()).thenReturn(true);
    CacheWriter cacheWriter = mock(CacheWriter.class);
    when(internalRegion.basicGetWriter()).thenReturn(cacheWriter);

    doPut();

    verify(internalRegion, times(1)).cacheWriteBeforePut(same(event), eq(null), same(cacheWriter),
        anyBoolean(), eq(null));
  }

  @Test
  public void cacheWriterBeforePutCalledWithNetWriteRecipients_ifAdviseNetWrite() {
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
    when(event.getOperation()).thenReturn(Operation.REPLACE);
    when(internalRegion.hasServerProxy()).thenReturn(true);

    createInstance();

    assertThat(instance.isReplaceOnClient()).isTrue();
  }

  @Test
  public void replaceOnClientIsFalseIfOperationIsReplaceAndOwnerIsNotClient() {
    when(event.getOperation()).thenReturn(Operation.REPLACE);
    when(internalRegion.hasServerProxy()).thenReturn(false);

    createInstance();

    assertThat(instance.isReplaceOnClient()).isFalse();
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
    when(event.getOperation()).thenReturn(Operation.REPLACE);
    when(internalRegion.hasServerProxy()).thenReturn(true);

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
  public void createOnEmptyMapAddsEntry() throws Exception {
    ifNew = true;
    when(event.getOperation()).thenReturn(Operation.CREATE);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(regionEntry);
    verify(event, times(1)).putNewEntry(internalRegion, regionEntry);
    verify(internalRegion, times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(internalRegion, times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }

  @Test
  public void putOnEmptyMapAddsEntry() throws Exception {
    ifNew = false;
    ifOld = false;
    when(event.getOperation()).thenReturn(Operation.CREATE);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(regionEntry);
    verify(event, times(1)).putNewEntry(internalRegion, regionEntry);
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
    when(focusedRegionMap.putEntryIfAbsent(event.getKey(), regionEntry))
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
    when(focusedRegionMap.putEntryIfAbsent(event.getKey(), regionEntry))
        .thenReturn(existingRegionEntry).thenReturn(null);
    when(event.getOperation()).thenReturn(Operation.CREATE);

    RegionEntry result = doPut();

    assertThat(result).isSameAs(regionEntry);
    verify(event, times(1)).putNewEntry(internalRegion, regionEntry);
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

}
