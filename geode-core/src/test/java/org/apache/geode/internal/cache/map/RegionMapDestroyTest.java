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
package org.apache.geode.internal.cache.map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;

public class RegionMapDestroyTest {

  private static final Object KEY = "key";

  @SuppressWarnings("rawtypes")
  private Map entryMap;
  private FocusedRegionMap regionMap;
  private boolean withConcurrencyChecks;
  private RegionEntryFactory factory;
  private RegionEntry newRegionEntry;
  private RegionEntry existingRegionEntry;
  private InternalRegion owner;
  private EvictableEntry evictableEntry;
  private EvictionController evictionController;
  private EvictionAttributes evictionAttributes;

  private EntryEventImpl event;
  private Object expectedOldValue;

  private boolean inTokenMode;
  private boolean duringRI;
  private boolean cacheWrite;
  private boolean isEviction;
  private boolean removeRecoveredEntry;
  private boolean fromRILocalDestroy;

  private RegionMapDestroy regionMapDestroy;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    factory = mock(RegionEntryFactory.class);
    entryMap = mock(Map.class);
    newRegionEntry = mock(RegionEntry.class);
    givenEntryDestroyReturnsTrue(newRegionEntry);
    when(newRegionEntry.isDestroyedOrRemoved()).thenReturn(true);
    existingRegionEntry = mock(RegionEntry.class);
    givenEntryDestroyReturnsTrue(existingRegionEntry);

    owner = mock(InternalRegion.class);
    when(owner.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(owner.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
    // Instead of mocking checkEntryNotFound to throw an exception,
    // this test now just verifies that checkEntryNotFound was called.
    // Having the mock throw the exception confuses the code coverage tools.

    evictionAttributes = mock(EvictionAttributes.class);
    when(evictionAttributes.getAlgorithm()).thenReturn(EvictionAlgorithm.LRU_ENTRY);
    when(evictionAttributes.getAction()).thenReturn(EvictionAction.DEFAULT_EVICTION_ACTION);
    when(evictionAttributes.getMaximum()).thenReturn(EvictionAttributes.DEFAULT_ENTRIES_MAXIMUM);
    evictionController = mock(EvictionController.class);
    when(evictionController.getEvictionAlgorithm()).thenReturn(EvictionAlgorithm.LRU_ENTRY);
    when(evictionController.getCounters()).thenReturn(mock(EvictionCounters.class));

    evictableEntry = mock(EvictableEntry.class);

    regionMap = mock(FocusedRegionMap.class);
    when(regionMap.getEntryFactory()).thenReturn(factory);
    when(factory.createEntry(any(), any(), any())).thenReturn(newRegionEntry);
    when(regionMap.getEntryMap()).thenReturn(entryMap);

    event = mock(EntryEventImpl.class);
    when(event.getRegion()).thenReturn(owner);
    when(event.getOperation()).thenReturn(Operation.DESTROY);
    when(event.getKey()).thenReturn(KEY);
    when(event.isGenerateCallbacks()).thenReturn(true);

    inTokenMode = false;
    duringRI = false;
    cacheWrite = false;
    isEviction = false;
    expectedOldValue = null;
    removeRecoveredEntry = false;

    regionMapDestroy = new RegionMapDestroy(owner, regionMap, mock(CacheModificationLock.class));
  }

  @Test
  public void destroyWithDuplicateVersionInvokesListener() {
    givenConcurrencyChecks(true);
    givenExistingTombstone();
    givenExistingEntryWithVersionTag(mock(VersionTag.class));
    givenEventWithClientOrigin();

    assertThat(doDestroy()).isTrue();

    verify(event, times(1)).setIsRedestroyedEntry(true);
    verifyPart3();
  }

  @Test
  public void destroyWithEmptyRegionThrowsException() {
    givenConcurrencyChecks(false);

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeAddsAToken() throws Exception {
    givenConcurrencyChecks(false);
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verify(owner, never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyInvokesTestHook() {
    givenConcurrencyChecks(false);
    givenInTokenMode();
    Runnable testHook = mock(Runnable.class);
    RegionMapDestroy.testHookRunnableForConcurrentOperation = testHook;
    try {
      doDestroy();
    } finally {
      RegionMapDestroy.testHookRunnableForConcurrentOperation = null;
    }

    verify(testHook, times(1)).run();
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeWithRegionClearedExceptionDoesDestroy()
      throws Exception {
    givenConcurrencyChecks(false);
    givenDestroyThrowsRegionClearedException();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verifyInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void evictDestroyWithEmptyRegionInTokenModeDoesNothing() {
    givenEviction();
    givenInTokenMode();

    assertThat(doDestroy()).isFalse();

    verifyEntryRemoved(newRegionEntry);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithExistingTombstoneInTokenModeDestroyExistingEntry() throws Exception {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingTombstone();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verifyEntryDestroyed(existingRegionEntry, false);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void evictDestroyWithExistingTombstoneInTokenModeNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingTombstone();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verify(owner, never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void evictDestroyWithExistingTombstoneInUseByTransactionInTokenModeDoesNothing()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEvictableEntry(Token.TOMBSTONE);
    givenEvictableEntryIsInUseByTransaction();
    givenInTokenMode();

    assertThat(doDestroy()).isFalse();

    verifyNoDestroyInvocationsOnEvictableEntry();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithConcurrentChangeFromNullToInUseByTransactionInTokenModeDoesNothing()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEvictableEntry(null);
    givenEvictableEntryIsInUseByTransaction();
    givenInTokenMode();

    assertThat(doDestroy()).isFalse();

    verifyNoDestroyInvocationsOnEvictableEntry();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndDoesDestroy()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    when(regionMap.getEntry(event)).thenReturn(null).thenReturn(existingRegionEntry);

    assertThat(doDestroy()).isTrue();

    verifyEntryDestroyed(existingRegionEntry, false);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndThrowsConcurrentCacheModificationException()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    when(regionMap.getEntry(event)).thenReturn(null).thenReturn(existingRegionEntry);
    givenEntryDestroyThrows(existingRegionEntry, ConcurrentCacheModificationException.class);

    Throwable thrown = catchThrowable(() -> doDestroy());

    assertThat(thrown).isInstanceOf(ConcurrentCacheModificationException.class);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesCallsDestroyWhichReturnsFalseCausingDestroyToNotHappen()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    when(existingRegionEntry.getVersionStamp()).thenReturn(null);
    when(regionMap.getEntry(event)).thenReturn(null).thenReturn(existingRegionEntry);
    givenEntryDestroyReturnsFalse(existingRegionEntry);

    assertThat(doDestroy()).isTrue();

    verify(existingRegionEntry, times(1)).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithInTokenModeAndTombstoneCallsDestroyWhichReturnsFalseCausingDestroyToNotHappen()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEvictableEntry(Token.TOMBSTONE);
    when(regionMap.getEntry(event)).thenReturn(null).thenReturn(evictableEntry);
    givenInTokenMode();
    givenEntryDestroyReturnsFalse(evictableEntry);

    assertThat(doDestroy()).isTrue(); // TODO since destroy returns false it seems like doDestroy
                                      // should return true

    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithInTokenModeAndTombstoneCallsDestroyWhichThrowsRegionClearedStillDoesDestroy()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEvictableEntry(Token.TOMBSTONE);
    when(regionMap.getEntry(event)).thenReturn(null).thenReturn(evictableEntry);
    givenInTokenMode();
    givenEntryDestroyThrows(evictableEntry, RegionClearedException.class);

    assertThat(doDestroy()).isTrue();

    verifyInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void destroyWithInTokenModeCallsDestroyWhichReturnsFalseCausingDestroyToNotHappen()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEvictableEntry("value");
    when(regionMap.getEntry(event)).thenReturn(evictableEntry);
    givenEntryDestroyReturnsFalse(evictableEntry);
    inTokenMode = true;

    assertThat(doDestroy()).isFalse();

    verify(evictableEntry, never()).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyExistingEntryWithVersionStampCallsDestroyWhichReturnsFalseCausingDestroyToNotHappenAndDoesNotCallRemovePhase2()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEvictableEntry("value");
    when(regionMap.getEntry(event)).thenReturn(evictableEntry);
    givenEntryDestroyReturnsFalse(evictableEntry);
    when(evictableEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    givenOriginIsRemote();

    assertThat(doDestroy()).isTrue();

    verify(owner, never()).rescheduleTombstone(any(), any());
    verify(evictableEntry, never()).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyTombstoneWithRemoveRecoveredEntryAndVersionStampCallsRescheduleTombstone()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEvictableEntry(Token.TOMBSTONE);
    when(evictableEntry.isTombstone()).thenReturn(true);
    when(regionMap.getEntry(event)).thenReturn(evictableEntry);
    givenEntryDestroyReturnsFalse(evictableEntry);
    when(evictableEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    givenOriginIsRemote();
    removeRecoveredEntry = true;

    assertThat(doDestroy()).isTrue();

    verify(owner, times(1)).rescheduleTombstone(eq(evictableEntry), any());
    verify(evictableEntry, never()).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyTombstoneWithLocalOriginAndRemoveRecoveredEntryAndVersionStampDoesNotCallRescheduleTombstone()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEvictableEntry(Token.TOMBSTONE);
    when(evictableEntry.isTombstone()).thenReturn(true);
    when(regionMap.getEntry(event)).thenReturn(evictableEntry);
    givenEntryDestroyReturnsFalse(evictableEntry);
    when(evictableEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    removeRecoveredEntry = true;

    assertThat(doDestroy()).isTrue();

    verify(owner, never()).rescheduleTombstone(eq(evictableEntry), any());
    verify(evictableEntry, never()).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    when(regionMap.getEntry(event)).thenReturn(null).thenReturn(existingRegionEntry);

    assertThat(doDestroy()).isTrue();

    verify(owner, times(1)).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyInTokenModeWithConcurrentChangeFromNullToRemovePhase2RetriesAndDoesDestroy()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenInTokenMode();
    when(existingRegionEntry.isRemovedPhase2()).thenReturn(true);
    when(regionMap.getEntry(event)).thenReturn(null);
    when(regionMap.putEntryIfAbsent(eq(KEY), any())).thenReturn(existingRegionEntry)
        .thenReturn(null);

    assertThat(doDestroy()).isTrue();

    verify(entryMap).remove(eq(KEY), eq(existingRegionEntry));
    verify(regionMap, times(2)).putEntryIfAbsent(eq(KEY), any());
    verify(existingRegionEntry, never()).destroy(eq(owner), eq(event), eq(false), anyBoolean(),
        eq(expectedOldValue), anyBoolean(), anyBoolean());
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyInTokenModeWithConcurrentChangeFromNullToRemovePhase2RetriesAndNeverCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenInTokenMode();
    when(existingRegionEntry.isRemovedPhase2()).thenReturn(true);
    when(regionMap.getEntry(event)).thenReturn(null);
    when(regionMap.putEntryIfAbsent(eq(KEY), any())).thenReturn(existingRegionEntry)
        .thenReturn(null);

    assertThat(doDestroy()).isTrue();

    verify(owner, never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingEntryInTokenModeAddsAToken() throws Exception {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verifyEntryDestroyed(existingRegionEntry, false);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryInTokenModeInhibitsCacheListenerNotification() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verify(event, times(1)).inhibitCacheListenerNotification(true);
  }

  @Test
  public void destroyOfExistingEntryInTokenModeDuringRegisterInterestDoesNotInhibitCacheListenerNotification() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenInTokenMode();
    duringRI = true;

    assertThat(doDestroy()).isTrue();

    verify(event, never()).inhibitCacheListenerNotification(true);
  }

  @Test
  public void destroyOfExistingEntryDoesNotInhibitCacheListenerNotification() {
    givenConcurrencyChecks(false);
    givenExistingEntry();

    assertThat(doDestroy()).isTrue();

    verify(event, never()).inhibitCacheListenerNotification(true);
  }

  @Test
  public void destroyOfExistingEntryCallsIndexManager() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    IndexManager indexManager = mock(IndexManager.class);
    when(owner.getIndexManager()).thenReturn(indexManager);

    assertThat(doDestroy()).isTrue();

    verifyIndexManagerOrder(indexManager);
  }

  @Test
  public void destroyOfExistingEntryInTokenModeCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verify(owner, times(1)).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingTombstoneInTokenModeWithConcurrencyChecksDoesNothing()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    // why not DESTROY token? since it was already destroyed why do we do the parts?
    verifyEntryDestroyed(existingRegionEntry, false);
    verify(regionMap, never()).removeEntry(eq(KEY), same(existingRegionEntry), anyBoolean());
    verify(regionMap, never()).removeEntry(eq(KEY), same(existingRegionEntry), anyBoolean(),
        same(event), same(owner));
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingFromPutIfAbsentWithRemoteOriginCallsBasicDestroyBeforeRemoval()
      throws Exception {
    givenConcurrencyChecks(true);
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.getValue()).thenReturn("value");
    when(regionMap.getEntry(event)).thenReturn(null);
    RegionEntry newUnusedEntry = mock(RegionEntry.class);
    when(factory.createEntry(any(), any(), any())).thenReturn(newUnusedEntry);
    when(regionMap.putEntryIfAbsent(eq(KEY), any())).thenReturn(existingEntry);
    givenEntryDestroyReturnsTrue(existingEntry);
    givenOriginIsRemote();

    assertThat(doDestroy()).isTrue();

    verify(owner, times(1)).basicDestroyBeforeRemoval(existingEntry, event);
  }

  @Test
  public void destroyOfExistingFromPutIfAbsentWithTokenModeAndLocalOriginDoesNotCallBasicDestroyBeforeRemoval()
      throws Exception {
    givenConcurrencyChecks(true);
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.getValue()).thenReturn("value");
    when(regionMap.getEntry(event)).thenReturn(null);
    RegionEntry newUnusedEntry = mock(RegionEntry.class);
    when(factory.createEntry(any(), any(), any())).thenReturn(newUnusedEntry);
    when(regionMap.putEntryIfAbsent(eq(KEY), any())).thenReturn(existingEntry);
    givenEntryDestroyReturnsTrue(existingEntry);
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    // TODO: this seems like a bug in the product. See the comment in:
    // RegionMapDestroy.destroyExistingFromPutIfAbsent(RegionEntry)
    verify(owner, never()).basicDestroyBeforeRemoval(existingEntry, event);
  }

  @Test
  public void destroyOfExistingTombstoneInTokenModeWithConcurrencyChecksNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verify(owner, never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingTombstoneWillThrowConcurrentCacheModificationException()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenInTokenMode();
    givenEntryDestroyThrows(existingRegionEntry, ConcurrentCacheModificationException.class);

    Throwable thrown = catchThrowable(() -> doDestroy());

    assertThat(thrown).isInstanceOf(ConcurrentCacheModificationException.class);
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksThrowsEntryNotFound() {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyOfExistingTombstoneThatThrowsConcurrentCacheModificationExceptionNeverCallsNotify() {
    givenConcurrencyChecks(true);
    givenExistingTombstone();
    givenVersionStampThatDetectsConflict();
    doThrow(ConcurrentCacheModificationException.class).when(regionMap)
        .processVersionTag(same(existingRegionEntry), same(event));
    givenEventWithVersionTag();

    Throwable thrown = catchThrowable(() -> doDestroy());

    assertThat(thrown).isInstanceOf(ConcurrentCacheModificationException.class);
    verify(owner, never()).notifyTimestampsToGateways(any());
  }

  @Test
  public void destroyOfExistingTombstoneThatThrowsConcurrentCacheModificationExceptionWithTimeStampUpdatedCallsNotify()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingTombstone();
    givenVersionStampThatDetectsConflict();
    doThrow(ConcurrentCacheModificationException.class).when(regionMap)
        .processVersionTag(same(existingRegionEntry), same(event));
    givenEventWithVersionTag();
    when(event.getVersionTag().isTimeStampUpdated()).thenReturn(true);

    Throwable thrown = catchThrowable(() -> doDestroy());

    assertThat(thrown).isInstanceOf(ConcurrentCacheModificationException.class);
    verify(owner, times(1)).notifyTimestampsToGateways(eq(event));
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndCallsNotifyTimestampsToGateways()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenVersionStampThatDetectsConflict();
    givenEventWithVersionTag();
    when(event.getVersionTag().isTimeStampUpdated()).thenReturn(true);
    when(regionMap.getEntry(event)).thenReturn(null).thenReturn(existingRegionEntry);
    givenEntryDestroyThrows(existingRegionEntry, ConcurrentCacheModificationException.class);

    Throwable thrown = catchThrowable(() -> doDestroy());

    assertThat(thrown).isInstanceOf(ConcurrentCacheModificationException.class);
    verifyNoDestroyInvocationsOnRegion();
    verify(owner, times(1)).notifyTimestampsToGateways(eq(event));
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndNoTagThrowsEntryNotFound() {
    givenConcurrencyChecks(true);
    givenExistingTombstone();

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void evictDestroyOfExistingTombstoneWithConcurrencyChecksReturnsFalse() {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    isEviction = true;

    assertThat(doDestroy()).isFalse();
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndRemoveRecoveredEntryDoesRemove()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    when(existingRegionEntry.isTombstone()).thenReturn(true).thenReturn(false);
    givenRemoveRecoveredEntry();

    assertThat(doDestroy()).isTrue();

    verifyMapDoesNotContainKey();
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndFromRILocalDestroyDoesRemove()
      throws Exception {
    givenConcurrencyChecks(true);
    fromRILocalDestroy = true;
    when(event.isFromRILocalDestroy()).thenReturn(true);
    givenExistingTombstoneAndVersionTag();
    when(existingRegionEntry.isTombstone()).thenReturn(true).thenReturn(false);

    assertThat(doDestroy()).isTrue();

    verifyMapDoesNotContainKey();
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndRemoveRecoveredEntryNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenRemoveRecoveredEntry();

    assertThat(doDestroy()).isTrue();

    verify(owner, never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndRemoveRecoveredEntryDoesRetryAndThrowsEntryNotFound() {
    givenConcurrencyChecks(true);
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    givenRemoveRecoveredEntry();

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksDoesRetryAndThrowsEntryNotFound() {
    givenConcurrencyChecks(true);
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndExpectedValueDoesRetryAndReturnsFalse() {
    givenConcurrencyChecks(true);
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    expectedOldValue = "OLD_VALUE";

    assertThat(doDestroy()).isFalse();
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndInTokenModeDoesRetryAndReturnsFalse() {
    givenConcurrencyChecks(true);
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    inTokenMode = true;

    assertThat(doDestroy()).isFalse();
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndEvictionDoesRetryAndReturnsFalse() {
    givenConcurrencyChecks(true);
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    isEviction = true;

    assertThat(doDestroy()).isFalse();
  }

  @Test
  public void destroyOfExistingRemovePhase2WithoutConcurrencyChecksDoesRetryAndThrowsEntryNotFound() {
    givenConcurrencyChecks(false);
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndOriginRemoteDoesRetryAndDoesRemove()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    givenOriginIsRemote();

    assertThat(doDestroy()).isTrue();

    verify(entryMap, times(1)).remove(KEY, existingRegionEntry);
    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndClientOriginDoesRetryAndDoesRemove()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    givenEventWithClientOrigin();

    assertThat(doDestroy()).isTrue();

    verify(entryMap, times(1)).remove(KEY, existingRegionEntry);
    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
  }

  @Test
  public void destroyOfExistingEntryRemovesEntryFromMapAndDoesNotifications() throws Exception {
    givenConcurrencyChecks(false);
    givenExistingEntry();

    assertThat(doDestroy()).isTrue();

    verifyEntryDestroyed(existingRegionEntry, false);
    verify(regionMap, times(1)).removeEntry(KEY, existingRegionEntry, true, event, owner);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryWithConflictDoesPart3() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    when(event.isConcurrencyConflict()).thenReturn(true);

    assertThat(doDestroy()).isTrue();

    verifyPart3();
  }

  @Test
  public void destroyOfExistingEntryWithConflictAndWANSkipsPart3() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    when(event.isConcurrencyConflict()).thenReturn(true);
    givenEventWithGatewayTag();

    assertThat(doDestroy()).isTrue();

    verifyNoPart3();
  }

  @Test
  public void destroyOfExistingEntryWithRegionClearedExceptionDoesDestroyAndPart2AndPart3()
      throws RegionClearedException {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    when(regionMap.getEntry(event)).thenReturn(null).thenReturn(existingRegionEntry);
    givenEventWithVersionTag();
    givenEntryDestroyThrows(existingRegionEntry, RegionClearedException.class);

    assertThat(doDestroy()).isTrue();

    verifyInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void expireDestroyOfExistingEntry() {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    when(event.getOperation()).thenReturn(Operation.EXPIRE_DESTROY);

    assertThat(doDestroy()).isTrue();
  }

  @Test
  public void expireDestroyOfExistingEntryWithOriginRemote() {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenOriginIsRemote();
    when(event.getOperation()).thenReturn(Operation.EXPIRE_DESTROY);

    assertThat(doDestroy()).isTrue();
  }

  @Test
  public void expireDestroyOfEntryInUseIsCancelled()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenEntryDestroyReturnsTrue(evictableEntry);
    when(evictableEntry.isInUseByTransaction()).thenReturn(true);
    when(regionMap.getEntry(event)).thenReturn(evictableEntry);
    when(event.getOperation()).thenReturn(Operation.EXPIRE_DESTROY);

    assertThat(doDestroy()).isFalse();

    verify(evictableEntry, never()).destroy(any(), any(), anyBoolean(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verifyNoDestroyInvocationsOnRegion();
  }


  @Test
  public void destroyOfExistingEntryCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenExistingEntry();

    assertThat(doDestroy()).isTrue();

    verify(owner, times(1)).updateSizeOnRemove(any(), anyInt());
  }

  /**
   * This might be a bug. It seems like we should have created a tombstone but we have no version
   * tag so that might be the cause of this bug.
   */
  @Test
  public void destroyOfExistingEntryWithConcurrencyChecksAndNoVersionTagDestroysWithoutTombstone()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    when(existingRegionEntry.getVersionStamp()).thenReturn(null);

    assertThat(doDestroy()).isTrue();

    verifyMapDoesNotContainKey();
    verify(existingRegionEntry, times(1)).removePhase2();
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryWithConcurrencyChecksAddsTombstone() throws Exception {
    givenConcurrencyChecks(true);
    givenExistingEntryWithVersionTag();

    assertThat(doDestroy()).isTrue();

    verifyEntryDestroyed(existingRegionEntry, false);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void evictDestroyOfExistingEntryWithConcurrencyChecksAddsTombstone() throws Exception {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntryWithVersionTag();

    assertThat(doDestroy()).isTrue();

    verifyEntryDestroyed(existingRegionEntry, false);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksThrowsException() {
    givenConcurrencyChecks(true);

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventThrowsException() {
    givenConcurrencyChecks(true);
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenOriginIsRemote();

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventAndCacheWriteThrowsException() {
    givenConcurrencyChecks(true);
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenOriginIsRemote();
    cacheWrite = true;
    removeRecoveredEntry = false;

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventAndCacheWriteAndRemoveRecoveredEntryDoesNotThrowException() {
    givenConcurrencyChecks(true);
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenOriginIsRemote();
    cacheWrite = true;
    removeRecoveredEntry = true;

    assertThat(doDestroy()).isFalse();
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventAndCacheWriteAndBridgeWriteBeforeDestroyReturningTrueDoesNotThrowException() {
    givenConcurrencyChecks(true);
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenOriginIsRemote();
    cacheWrite = true;
    removeRecoveredEntry = false;
    when(owner.bridgeWriteBeforeDestroy(eq(event), any())).thenReturn(true);

    assertThat(doDestroy()).isFalse();
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventAndCacheWriteAndBridgeWriteBeforeDestroyThrows_ThrowsException() {
    givenConcurrencyChecks(true);
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenOriginIsRemote();
    cacheWrite = true;
    removeRecoveredEntry = false;
    doThrow(EntryNotFoundException.class).when(owner).bridgeWriteBeforeDestroy(any(),
        any());

    Throwable thrown = catchThrowable(() -> doDestroy());

    assertThat(thrown).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void localDestroyWithEmptyNonReplicateRegionWithConcurrencyChecksThrowsException() {
    givenConcurrencyChecks(true);
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenEventWithVersionTag();
    givenEventWithClientOrigin();
    when(event.getOperation()).thenReturn(Operation.LOCAL_DESTROY);

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndClientTaggedEventAndCacheWriteDoesNotThrowException()
      throws Exception {
    givenConcurrencyChecks(true);
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    cacheWrite = true;
    removeRecoveredEntry = false;
    givenEventWithVersionTag();
    givenEventWithClientOrigin();

    assertThat(doDestroy()).isTrue();

    verifyEntryAddedToMap(newRegionEntry);
    verify(newRegionEntry, times(1)).makeTombstone(any(), any());
    verifyPart3();
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndWANTaggedEventAndCacheWriteDoesNotThrowException()
      throws Exception {
    givenConcurrencyChecks(true);
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    cacheWrite = true;
    removeRecoveredEntry = false;
    givenEventWithGatewayTag();

    assertThat(doDestroy()).isTrue();

    verifyEntryAddedToMap(newRegionEntry);
    verify(newRegionEntry, times(1)).makeTombstone(any(), any());
    verifyPart3();
  }

  /**
   * This seems to be a bug. We should not leave an evictableEntry in the entryMap added by the
   * destroy call if destroy returns false.
   */
  @Test
  public void evictDestroyWithEmptyRegionWithConcurrencyChecksDoesNothing() {
    givenConcurrencyChecks(true);
    givenEviction();

    assertThat(doDestroy()).isFalse();

    // the following verify should be enabled once GEODE-5573 is fixed
    // verifyMapDoesNotContainKey(KEY);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithEmptyNonReplicateRegionWithConcurrencyChecksDoesNothing() {
    givenConcurrencyChecks(true);
    givenEviction();
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);

    assertThat(doDestroy()).isFalse();

    // the following verify should be enabled once GEODE-5573 is fixed
    // verifyMapDoesNotContainKey(KEY);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithEmptyRegionDoesNothing() {
    givenConcurrencyChecks(false);
    givenEviction();

    assertThat(doDestroy()).isFalse();

    verify(regionMap, times(1)).getEntry(event);
    verifyNoMoreInteractions(regionMap);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAddsATombstone() throws Exception {
    givenConcurrencyChecks(true);
    givenRemoteEventWithVersionTag();

    assertThat(doDestroy()).isTrue();

    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndClientOriginEventAddsNewEntryAndCallsDestroy()
      throws Exception {
    givenConcurrencyChecks(true);
    givenEventWithVersionTag();
    givenEventWithClientOrigin();

    assertThat(doDestroy()).isTrue();

    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndWANEventAddsATombstone()
      throws Exception {
    givenConcurrencyChecks(true);
    givenEventWithGatewayTag();

    assertThat(doDestroy()).isTrue();

    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void validateNoDestroyWhenExistingTombstoneAndNewEntryDestroyFails()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingTombstone();
    when(regionMap.putEntryIfAbsent(eq(KEY), any())).thenReturn(null);
    givenEventWithGatewayTag();
    givenEntryDestroyReturnsFalse(newRegionEntry);
    inTokenMode = true;

    assertThat(doDestroy()).isFalse();

    verifyNoDestroyInvocationsOnRegion();
    verify(entryMap, never()).remove(KEY, newRegionEntry); // TODO: this seems like a bug. This
                                                           // should be called once.
  }

  @Test
  public void validateNoDestroyInvocationsOnRegionDoesNotDoDestroyIfEntryDestroyReturnsFalse()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEventWithGatewayTag();
    givenEntryDestroyReturnsFalse(newRegionEntry);

    assertThat(doDestroy()).isFalse();

    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndEventFromServerAddsATombstone()
      throws Exception {
    givenConcurrencyChecks(true);
    givenRemoteEventWithVersionTag();
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenEventFromServer();

    assertThat(doDestroy()).isTrue();

    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndWANEventWithConflictAddsATombstoneButDoesNotDoPart3()
      throws Exception {
    givenConcurrencyChecks(true);
    givenEventWithGatewayTag();
    when(event.isConcurrencyConflict()).thenReturn(true);

    assertThat(doDestroy()).isTrue();

    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyPart2(false);
    verifyNoPart3();
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndEventWithConflictAddsATombstone()
      throws Exception {
    givenConcurrencyChecks(true);
    givenRemoteEventWithVersionTag();
    when(event.isConcurrencyConflict()).thenReturn(true);

    assertThat(doDestroy()).isTrue();

    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksCallsIndexManager() {
    givenConcurrencyChecks(true);
    givenRemoteEventWithVersionTag();
    IndexManager indexManager = mock(IndexManager.class);
    when(owner.getIndexManager()).thenReturn(indexManager);

    assertThat(doDestroy()).isTrue();

    verifyIndexManagerOrder(indexManager);
  }

  /**
   * instead of a TOMBSTONE we leave an evictableEntry whose value is REMOVE_PHASE1 this looks like
   * a bug. It is caused by some code in: AbstractRegionEntry.destroy() that calls removePhase1 when
   * the versionTag is null. It seems like this code path needs to tell the higher levels to call
   * removeEntry
   */
  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndNullVersionTagAddsATombstone()
      throws Exception {
    givenConcurrencyChecks(true);
    givenOriginIsRemote();

    assertThat(doDestroy()).isTrue();

    verifyEntryAddedToMap(newRegionEntry);
    verify(regionMap, never()).removeEntry(eq(KEY), same(newRegionEntry), anyBoolean());
    verify(regionMap, never()).removeEntry(eq(KEY), same(newRegionEntry), anyBoolean(), same(event),
        same(owner));
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }


  private void givenConcurrencyChecks(boolean enabled) {
    withConcurrencyChecks = enabled;
    when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
  }

  private void givenVersionStampThatDetectsConflict() {
    @SuppressWarnings("rawtypes")
    VersionStamp versionStamp = mock(VersionStamp.class);
    when(existingRegionEntry.getVersionStamp()).thenReturn(versionStamp);
    doThrow(ConcurrentCacheModificationException.class).when(versionStamp)
        .processVersionTag(eq(event));
  }

  private void givenEviction() {
    when(owner.getEvictionAttributes()).thenReturn(evictionAttributes);
    isEviction = true;
    when(regionMap.confirmEvictionDestroy(any())).thenReturn(true);
  }

  private void givenExistingEvictableEntry(Object value) throws RegionClearedException {
    when(evictableEntry.getValue()).thenReturn(value);
    when(regionMap.getEntry(event)).thenReturn(value == null ? null : evictableEntry);
    when(regionMap.putEntryIfAbsent(eq(KEY), any())).thenReturn(evictableEntry);
    givenEntryDestroyReturnsTrue(evictableEntry);
  }

  private void givenDestroyThrowsRegionClearedException() throws RegionClearedException {
    givenEntryDestroyThrows(evictableEntry, RegionClearedException.class);
    when(regionMap.getEntry(event)).thenReturn(null);
    when(factory.createEntry(any(), any(), any())).thenReturn(evictableEntry);
  }

  private void givenExistingEntryWithVersionTag(@SuppressWarnings("rawtypes") VersionTag version) {
    // ((VersionStamp) entry).setVersions(version);
    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(owner.getVersionVector()).thenReturn(versionVector);
    when(event.getVersionTag()).thenReturn(version);
  }

  private void givenExistingEntry() {
    when(regionMap.getEntry(event)).thenReturn(existingRegionEntry);
    when(regionMap.putEntryIfAbsent(KEY, newRegionEntry)).thenReturn(existingRegionEntry);
    if (withConcurrencyChecks) {
      when(existingRegionEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    }
  }

  private void givenExistingEntryWithVersionTag() {
    givenExistingEntry();
    givenEventWithVersionTag();
  }

  private void givenExistingEntryWithTokenAndVersionTag(Token token) {
    if (token == Token.REMOVED_PHASE2) {
      when(regionMap.getEntry(event)).thenReturn(existingRegionEntry).thenReturn(null);
      when(regionMap.putEntryIfAbsent(KEY, newRegionEntry)).thenReturn(null);
      when(existingRegionEntry.isRemovedPhase2()).thenReturn(true);
      when(existingRegionEntry.isRemoved()).thenReturn(true);
      when(existingRegionEntry.isDestroyedOrRemoved()).thenReturn(true);
      when(existingRegionEntry.isInvalidOrRemoved()).thenReturn(true);
      when(existingRegionEntry.isDestroyedOrRemovedButNotTombstone()).thenReturn(true);
    } else {
      throw new IllegalArgumentException("unexpected token: " + token);
    }
    givenEventWithVersionTag();
  }

  private void givenExistingTombstone() {
    givenExistingEntry();
    when(existingRegionEntry.isTombstone()).thenReturn(true);
    when(existingRegionEntry.isRemoved()).thenReturn(true);
    when(existingRegionEntry.isDestroyedOrRemoved()).thenReturn(true);
    when(existingRegionEntry.isInvalidOrRemoved()).thenReturn(true);
    when(existingRegionEntry.getValue()).thenReturn(Token.TOMBSTONE);
  }

  private void givenExistingTombstoneAndVersionTag() {
    givenExistingTombstone();
    givenEventWithVersionTag();
  }

  private void givenRemoteEventWithVersionTag() {
    givenOriginIsRemote();
    givenEventWithVersionTag();
  }

  private void givenEventWithVersionTag() {
    when(owner.getVersionVector()).thenReturn(mock(RegionVersionVector.class));
    @SuppressWarnings("rawtypes")
    VersionTag versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    when(event.getVersionTag()).thenReturn(versionTag);
  }

  private void givenInTokenMode() {
    inTokenMode = true;
  }

  private void givenRemoveRecoveredEntry() {
    removeRecoveredEntry = true;
  }

  private void givenEvictableEntryIsInUseByTransaction() {
    when(regionMap.confirmEvictionDestroy(evictableEntry)).thenReturn(false);
    when(evictableEntry.isInUseByTransaction()).thenReturn(true);
  }

  private void givenOriginIsRemote() {
    when(event.isOriginRemote()).thenReturn(true);
  }

  private void givenEventFromServer() {
    when(event.isFromServer()).thenReturn(true);
  }

  private void givenEventWithClientOrigin() {
    when(event.getContext()).thenReturn(mock(ClientProxyMembershipID.class));
    when(event.isBridgeEvent()).thenReturn(true);
    when(event.hasClientOrigin()).thenReturn(true);
    if (event.getVersionTag() != null) {
      when(event.isFromBridgeAndVersioned()).thenReturn(true);
    }
  }

  private void givenEntryDestroyReturnsTrue(RegionEntry entry) throws RegionClearedException {
    when(entry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(true);
  }

  private void givenEntryDestroyReturnsFalse(RegionEntry entry) throws RegionClearedException {
    when(entry.destroy(any(), any(), anyBoolean(), anyBoolean(),
        any(), anyBoolean(), anyBoolean())).thenReturn(false);
  }

  private void givenEntryDestroyThrows(RegionEntry entry, Class<? extends Throwable> classToThrow)
      throws RegionClearedException {
    doThrow(classToThrow).when(entry).destroy(
        any(), any(), anyBoolean(),
        anyBoolean(), any(), anyBoolean(), anyBoolean());
  }

  private void givenEventWithGatewayTag() {
    givenEventWithVersionTag();
    when(event.getVersionTag().isGatewayTag()).thenReturn(true);
    when(event.isFromWANAndVersioned()).thenReturn(true);
  }

  private boolean doDestroy() {
    return regionMapDestroy.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction,
        expectedOldValue, removeRecoveredEntry);
  }

  private void verifyEntryAddedToMap(RegionEntry entry) {
    verify(regionMap, times(1)).putEntryIfAbsent(KEY, entry);
  }

  private void verifyEntryDestroyed(RegionEntry entry, boolean force) throws Exception {
    verify(entry, times(1)).destroy(same(owner), same(event), eq(inTokenMode), eq(cacheWrite),
        same(expectedOldValue), eq(force), eq(removeRecoveredEntry || fromRILocalDestroy));
  }

  private void verifyMapDoesNotContainKey() throws Exception {
    verifyEntryDestroyed(existingRegionEntry, false);
    verify(regionMap, times(1)).removeEntry(KEY, existingRegionEntry, true, event, owner);
  }

  private void verifyNoDestroyInvocationsOnEvictableEntry() throws RegionClearedException {
    verify(evictableEntry, never()).destroy(any(), any(), anyBoolean(), anyBoolean(), any(),
        anyBoolean(), anyBoolean());
  }

  private void verifyInvokedDestroyMethodsOnRegion(boolean conflictWithClear) {
    verifyPart2(conflictWithClear);
    verifyPart3();
  }

  private void verifyPart3() {
    verify(owner, times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(true), eq(expectedOldValue));
  }

  private void verifyPart2(boolean conflictWithClear) {
    verify(owner, times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(conflictWithClear), eq(duringRI), eq(true));
  }

  private void verifyNoDestroyInvocationsOnRegion() {
    verifyNoPart2();
    verifyNoPart3();
  }

  private void verifyNoPart2() {
    verify(owner, never()).basicDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
  }

  private void verifyNoPart3() {
    verify(owner, never()).basicDestroyPart3(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), any());
  }

  private void verifyIndexManagerOrder(IndexManager indexManager) {
    InOrder inOrder = inOrder(indexManager, owner);
    inOrder.verify(indexManager, times(1)).waitForIndexInit();
    inOrder.verify(owner, times(1)).basicDestroyPart2(any(), any(), anyBoolean(),
        anyBoolean(),
        anyBoolean(), anyBoolean());
    inOrder.verify(indexManager, times(1)).countDownIndexUpdaters();
  }

  private void verifyEntryRemoved(RegionEntry entry) {
    int times = 2; // TODO: fix product to only call this once
    verify(regionMap, times(times)).removeEntry(eq(KEY), same(entry), eq(false));
  }
}
