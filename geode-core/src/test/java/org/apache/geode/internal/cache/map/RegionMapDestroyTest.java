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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.Token;
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

  private EntryEventImpl event;
  private Object expectedOldValue;

  private boolean inTokenMode;
  private boolean duringRI;
  private boolean cacheWrite;
  private boolean isEviction;
  private boolean removeRecoveredEntry;
  private boolean fromRILocalDestroy;

  private RegionMapDestroy regionMapDestroy;

  private Throwable doDestroyThrowable;

  private boolean doDestroyResult;

  private Runnable testHook;

  private IndexManager indexManager;

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

  @After
  public void tearDown() {
    RegionMapDestroy.testHookRunnableForConcurrentOperation = null;
  }

  @Test
  public void destroyWithDuplicateVersionInvokesListener() {
    givenConcurrencyChecks(true);
    givenExistingTombstone();
    givenExistingEntryWithVersionTag(mock(VersionTag.class));
    givenEventWithClientOrigin();

    doDestroy();

    verifyDestroyReturnedTrue();
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

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyInvokesTestHook() {
    givenConcurrencyChecks(false);
    givenInTokenMode();
    givenTestHook();

    doDestroy();

    verifyTestHookRun();
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeWithRegionClearedExceptionDoesDestroy()
      throws Exception {
    givenConcurrencyChecks(false);
    givenDestroyThrowsRegionClearedException();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void evictDestroyWithEmptyRegionInTokenModeDoesNothing() {
    givenEviction();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedFalse();
    verifyEntryRemoved(newRegionEntry);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithExistingTombstoneInTokenModeDestroyExistingEntry() throws Exception {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingTombstone();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryDestroyed(existingRegionEntry, false);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void evictDestroyWithExistingTombstoneInTokenModeNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingTombstone();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void evictDestroyWithExistingTombstoneInUseByTransactionInTokenModeDoesNothing()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntryWithValue(Token.TOMBSTONE);
    givenEntryIsInUseByTransaction();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedFalse();
    verifyNoDestroyInvocationsOnEntry(existingRegionEntry);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithConcurrentChangeFromNullToInUseByTransactionInTokenModeDoesNothing()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntryWithValue(null);
    givenEntryIsInUseByTransaction();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedFalse();
    verifyNoDestroyInvocationsOnEntry(existingRegionEntry);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithInUseByTransactionInTokenModeDoesNothing()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntry();
    givenEntryIsInUseByTransaction();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedFalse();
    verifyNoDestroyInvocationsOnEntry(existingRegionEntry);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndDoesDestroy()
      throws Exception {
    givenConcurrencyChecks(true);
    givenMissThenExistingEntry();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryDestroyed(existingRegionEntry, false);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndThrowsConcurrentCacheModificationException()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenMissThenExistingEntry();
    givenEntryDestroyThrows(existingRegionEntry, ConcurrentCacheModificationException.class);

    doDestroyExpectingThrowable();

    verifyThrowableInstanceOf(ConcurrentCacheModificationException.class);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesCallsDestroyWhichReturnsFalseCausingDestroyToNotHappen()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenMissThenExistingEntry();
    givenExistingEntryWithNoVersionStamp();
    givenEntryDestroyReturnsFalse(existingRegionEntry);

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(existingRegionEntry, times(1)).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithInTokenModeAndTombstoneCallsDestroyWhichReturnsFalseCausingDestroyToNotHappen()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntryWithValue(Token.TOMBSTONE);
    givenMissThenExistingEntry();
    givenInTokenMode();
    givenEntryDestroyReturnsFalse(existingRegionEntry);

    doDestroy();

    // TODO since destroy returns false it seems like doDestroy should return false
    verifyDestroyReturnedTrue();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithInTokenModeAndTombstoneCallsDestroyWhichThrowsRegionClearedStillDoesDestroy()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenMissThenExistingTombstone();
    givenInTokenMode();
    givenEntryDestroyThrows(existingRegionEntry, RegionClearedException.class);

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void destroyWithInTokenModeCallsDestroyWhichReturnsFalseCausingDestroyToNotHappen()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenEntryDestroyReturnsFalse(existingRegionEntry);
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedFalse();
    verify(existingRegionEntry, never()).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyExistingEntryWithVersionStampCallsDestroyWhichReturnsFalseCausingDestroyToNotHappenAndDoesNotCallRemovePhase2()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenEntryDestroyReturnsFalse(existingRegionEntry);
    givenOriginIsRemote();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, never()).rescheduleTombstone(any(), any());
    verify(existingRegionEntry, never()).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyTombstoneWithRemoveRecoveredEntryAndVersionStampCallsRescheduleTombstone()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntryWithValue(Token.TOMBSTONE);
    givenEntryDestroyReturnsFalse(existingRegionEntry);
    givenOriginIsRemote();
    givenRemoveRecoveredEntry();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, times(1)).rescheduleTombstone(same(existingRegionEntry), any());
    verify(existingRegionEntry, never()).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyTombstoneWithLocalOriginAndRemoveRecoveredEntryAndVersionStampDoesNotCallRescheduleTombstone()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntryWithValue(Token.TOMBSTONE);
    givenEntryDestroyReturnsFalse(existingRegionEntry);
    givenRemoveRecoveredEntry();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, never()).rescheduleTombstone(any(), any());
    verify(existingRegionEntry, never()).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenMissThenExistingEntry();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, times(1)).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyInTokenModeWithConcurrentChangeFromNullToRemovePhase2RetriesAndDoesDestroy()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenInTokenMode();
    givenRemovePhase2Retry();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(entryMap).remove(eq(KEY), eq(existingRegionEntry));
    verify(regionMap, times(2)).putEntryIfAbsent(eq(KEY), any());
    verifyNoDestroyInvocationsOnEntry(existingRegionEntry);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyInTokenModeWithConcurrentChangeFromNullToRemovePhase2RetriesAndNeverCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenInTokenMode();
    givenRemovePhase2Retry();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingEntryInTokenModeAddsAToken() throws Exception {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryDestroyed(existingRegionEntry, false);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryInTokenModeInhibitsCacheListenerNotification() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(event, times(1)).inhibitCacheListenerNotification(true);
  }

  @Test
  public void destroyOfExistingEntryInTokenModeDuringRegisterInterestDoesNotInhibitCacheListenerNotification() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenInTokenMode();
    givenDuringRegisterInterest();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(event, never()).inhibitCacheListenerNotification(true);
  }

  @Test
  public void destroyOfExistingEntryDoesNotInhibitCacheListenerNotification() {
    givenConcurrencyChecks(false);
    givenExistingEntry();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(event, never()).inhibitCacheListenerNotification(true);
  }

  @Test
  public void destroyOfExistingEntryCallsIndexManager() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenIndexManager();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyIndexManagerOrder();
  }

  @Test
  public void destroyOfExistingEntryInTokenModeCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, times(1)).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingTombstoneInTokenModeWithConcurrencyChecksDoesNothing()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
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
    givenMissThenExistingEntry();
    givenOriginIsRemote();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, times(1)).basicDestroyBeforeRemoval(existingRegionEntry, event);
  }

  @Test
  public void destroyOfExistingFromPutIfAbsentWithTokenModeAndLocalOriginDoesNotCallBasicDestroyBeforeRemoval()
      throws Exception {
    givenConcurrencyChecks(true);
    givenMissThenExistingEntry();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
    // TODO: this seems like a bug in the product. See the comment in:
    // RegionMapDestroy.destroyExistingFromPutIfAbsent(RegionEntry)
    verify(owner, never()).basicDestroyBeforeRemoval(existingRegionEntry, event);
  }

  @Test
  public void destroyOfExistingTombstoneInTokenModeWithConcurrencyChecksNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingTombstoneWillThrowConcurrentCacheModificationException()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenInTokenMode();
    givenEntryDestroyThrows(existingRegionEntry, ConcurrentCacheModificationException.class);

    doDestroyExpectingThrowable();

    verifyThrowableInstanceOf(ConcurrentCacheModificationException.class);
    verify(owner, never()).notifyTimestampsToGateways(event);
  }

  @Test
  public void destroyOfExistingTombstoneWithTimeStampUpdatedWillCallNotifyTimestampsToGateways()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenInTokenMode();
    givenEntryDestroyThrows(existingRegionEntry, ConcurrentCacheModificationException.class);
    givenEventTimeStampUpdated();

    doDestroyExpectingThrowable();

    verifyThrowableInstanceOf(ConcurrentCacheModificationException.class);
    verify(owner, times(1)).notifyTimestampsToGateways(event);
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
    givenEventWithVersionTag();

    doDestroyExpectingThrowable();

    verifyThrowableInstanceOf(ConcurrentCacheModificationException.class);
    verify(owner, never()).notifyTimestampsToGateways(any());
  }

  @Test
  public void destroyOfTombstoneThatBecomesNonTombstoneRetriesAndDoesDestroy() throws Exception {
    givenConcurrencyChecks(true);
    givenExistingTombstone();
    givenEventWithVersionTag();
    givenTombstoneThenAlive();
    givenNotDestroyedOrRemoved();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(regionMap, times(2)).getEntry(any());
    verify(regionMap, times(1)).processVersionTag(existingRegionEntry, event);
    verify(regionMap, times(1)).lruEntryDestroy(existingRegionEntry);
    verifyNoMoreInteractions(regionMap);
    verifyEntryDestroyed(existingRegionEntry, false);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingTombstoneThatThrowsConcurrentCacheModificationExceptionWithTimeStampUpdatedCallsNotify()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingTombstone();
    givenVersionStampThatDetectsConflict();
    givenEventWithVersionTag();
    givenEventTimeStampUpdated();

    doDestroyExpectingThrowable();

    verifyThrowableInstanceOf(ConcurrentCacheModificationException.class);
    verify(owner, times(1)).notifyTimestampsToGateways(eq(event));
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndCallsNotifyTimestampsToGateways()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenMissThenExistingEntry();
    givenVersionStampThatDetectsConflict();
    givenEventWithVersionTag();
    givenEventTimeStampUpdated();
    givenEntryDestroyThrows(existingRegionEntry, ConcurrentCacheModificationException.class);

    doDestroyExpectingThrowable();

    verifyThrowableInstanceOf(ConcurrentCacheModificationException.class);
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
  public void destroyOfExistingTombstoneDoesDestroyAndReschedulesTombstone()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingTombstone();
    givenEventWithVersionTag();
    givenOriginIsRemote();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(owner, times(1)).checkEntryNotFound(any());
    verify(owner, times(1)).recordEvent(event);
    verify(owner, times(1)).rescheduleTombstone(same(existingRegionEntry), any());
    verify(existingRegionEntry, times(1)).setValue(owner, Token.TOMBSTONE);
    verifyPart2(true);
    verifyNoPart3();
  }

  @Test
  public void evictDestroyOfExistingTombstoneWithConcurrencyChecksReturnsFalse() {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenEviction();

    doDestroy();

    verifyDestroyReturnedFalse();
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndRemoveRecoveredEntryDoesRemove()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenTombstoneThenAlive();
    givenRemoveRecoveredEntry();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyMapDoesNotContainKey();
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryThatBecomesTombstoneAfterInitialCheckCallsProcessVersionTagAndMakeTombstoneButDoesNotDoDestroy()
      throws Exception {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenAliveThenTombstone();

    doDestroy();

    verifyDestroyReturnedFalse();
    verify(regionMap, times(1)).getEntry(any());
    verify(regionMap, times(1)).processVersionTag(existingRegionEntry, event);
    verifyNoMoreInteractions(regionMap);
    verify(existingRegionEntry, times(1)).makeTombstone(any(), any());
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndFromRILocalDestroyDoesRemove()
      throws Exception {
    givenConcurrencyChecks(true);
    givenFromRILocalDestroy();
    givenExistingTombstoneAndVersionTag();
    givenTombstoneThenAlive();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyMapDoesNotContainKey();
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndRemoveRecoveredEntryNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenExistingTombstoneAndVersionTag();
    givenRemoveRecoveredEntry();

    doDestroy();

    verifyDestroyReturnedTrue();
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
    givenExpectedOldValue();

    doDestroy();

    verifyDestroyReturnedFalse();
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndInTokenModeDoesRetryAndReturnsFalse() {
    givenConcurrencyChecks(true);
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedFalse();
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndEvictionDoesRetryAndReturnsFalse() {
    givenConcurrencyChecks(true);
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    givenEviction();

    doDestroy();

    verifyDestroyReturnedFalse();
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

    doDestroy();

    verifyDestroyReturnedTrue();
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

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(entryMap, times(1)).remove(KEY, existingRegionEntry);
    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
  }

  @Test
  public void destroyOfExistingEntryRemovesEntryFromMapAndDoesNotifications() throws Exception {
    givenConcurrencyChecks(false);
    givenExistingEntry();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryDestroyed(existingRegionEntry, false);
    verify(regionMap, times(1)).removeEntry(KEY, existingRegionEntry, true, event, owner);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryWithConflictDoesPart3() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenEventWithConflict();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyPart3();
  }

  @Test
  public void destroyOfExistingEntryWithConflictAndWANSkipsPart3() {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenEventWithConflict();
    givenEventWithGatewayTag();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyNoPart3();
  }

  @Test
  public void destroyOfExistingEntryWithRegionClearedExceptionDoesDestroyAndPart2AndPart3()
      throws RegionClearedException {
    givenConcurrencyChecks(false);
    givenMissThenExistingEntry();
    givenEventWithVersionTag();
    givenEntryDestroyThrows(existingRegionEntry, RegionClearedException.class);

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(event, never()).inhibitCacheListenerNotification(true);
    verifyInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void destroyOfExistingEntryWithRegionClearedExceptionInTokenModeCallsInhibitCacheListenerNotification()
      throws RegionClearedException {
    givenConcurrencyChecks(false);
    givenExistingEntry();
    givenEventWithVersionTag();
    givenEntryDestroyThrows(existingRegionEntry, RegionClearedException.class);
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedTrue();
    verify(event, times(1)).inhibitCacheListenerNotification(true);
    verifyInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void expireDestroyOfExistingEntry() {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenExpireDestroy();

    doDestroy();

    verifyDestroyReturnedTrue();
  }

  @Test
  public void expireDestroyOfExistingEntryWithOriginRemote() {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenOriginIsRemote();
    givenExpireDestroy();

    doDestroy();

    verifyDestroyReturnedTrue();
  }

  @Test
  public void expireDestroyOfEntryInUseIsCancelled()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingEntry();
    givenEntryIsInUseByTransaction();
    givenExpireDestroy();

    doDestroy();

    verifyDestroyReturnedFalse();
    verifyNoDestroyInvocationsOnEntry(existingRegionEntry);
    verifyNoDestroyInvocationsOnRegion();
  }


  @Test
  public void destroyOfExistingEntryCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenExistingEntry();

    doDestroy();

    verifyDestroyReturnedTrue();
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
    givenExistingEntryWithNoVersionStamp();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyMapDoesNotContainKey();
    verify(existingRegionEntry, times(1)).removePhase2();
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryWithConcurrencyChecksAddsTombstone() throws Exception {
    givenConcurrencyChecks(true);
    givenExistingEntryWithVersionTag();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryDestroyed(existingRegionEntry, false);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void evictDestroyOfExistingEntryWithConcurrencyChecksAddsTombstone() throws Exception {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntryWithVersionTag();

    doDestroy();

    verifyDestroyReturnedTrue();
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
    givenEmptyDataPolicy();
    givenOriginIsRemote();

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventAndCacheWriteThrowsException() {
    givenConcurrencyChecks(true);
    givenEmptyDataPolicy();
    givenOriginIsRemote();
    givenCacheWrite();

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventAndCacheWriteAndRemoveRecoveredEntryDoesNotThrowException() {
    givenConcurrencyChecks(true);
    givenEmptyDataPolicy();
    givenOriginIsRemote();
    givenCacheWrite();
    givenRemoveRecoveredEntry();

    doDestroy();

    verifyDestroyReturnedFalse();
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventAndCacheWriteAndBridgeWriteBeforeDestroyReturningTrueDoesNotThrowException() {
    givenConcurrencyChecks(true);
    givenEmptyDataPolicy();
    givenOriginIsRemote();
    givenCacheWrite();
    givenBridgeWriteBeforeDestroyReturnsTrue();

    doDestroy();

    verifyDestroyReturnedFalse();
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventAndCacheWriteAndBridgeWriteBeforeDestroyThrows_ThrowsException() {
    givenConcurrencyChecks(true);
    givenEmptyDataPolicy();
    givenOriginIsRemote();
    givenCacheWrite();
    givenBridgeWriteBeforeDestroyThrows();

    doDestroyExpectingThrowable();

    verifyThrowableInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void localDestroyWithEmptyNonReplicateRegionWithConcurrencyChecksThrowsException() {
    givenConcurrencyChecks(true);
    givenEmptyDataPolicy();
    givenEventWithVersionTag();
    givenEventWithClientOrigin();
    givenLocalDestroy();

    doDestroy();

    verify(owner, times(1)).checkEntryNotFound(any());
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndClientTaggedEventAndCacheWriteDoesNotThrowException()
      throws Exception {
    givenConcurrencyChecks(true);
    givenEmptyDataPolicy();
    givenCacheWrite();
    givenEventWithVersionTag();
    givenEventWithClientOrigin();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryAddedToMap(newRegionEntry);
    verify(newRegionEntry, times(1)).makeTombstone(any(), any());
    verifyPart3();
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndWANTaggedEventAndCacheWriteDoesNotThrowException()
      throws Exception {
    givenConcurrencyChecks(true);
    givenEmptyDataPolicy();
    givenCacheWrite();
    givenEventWithGatewayTag();

    doDestroy();

    verifyDestroyReturnedTrue();
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

    doDestroy();

    verifyDestroyReturnedFalse();
    // the following verify should be enabled once GEODE-5573 is fixed
    // verifyMapDoesNotContainKey(KEY);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithEmptyNonReplicateRegionWithConcurrencyChecksDoesNothing() {
    givenConcurrencyChecks(true);
    givenEviction();
    givenEmptyDataPolicy();

    doDestroy();

    verifyDestroyReturnedFalse();
    // the following verify should be enabled once GEODE-5573 is fixed
    // verifyMapDoesNotContainKey(KEY);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithEmptyRegionDoesNothing() {
    givenConcurrencyChecks(false);
    givenEviction();

    doDestroy();

    verifyDestroyReturnedFalse();
    verify(regionMap, times(1)).getEntry(event);
    verifyNoMoreInteractions(regionMap);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAddsATombstone() throws Exception {
    givenConcurrencyChecks(true);
    givenRemoteEventWithVersionTag();

    doDestroy();

    verifyDestroyReturnedTrue();
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

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndWANEventAddsATombstone()
      throws Exception {
    givenConcurrencyChecks(true);
    givenEventWithGatewayTag();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWhoseNewRegionEntryThrowsConcurrentCheckThrowsException() throws Exception {
    givenConcurrencyChecks(true);
    givenEventWithGatewayTag();
    givenEntryDestroyThrows(newRegionEntry, ConcurrentCacheModificationException.class);

    doDestroyExpectingThrowable();

    verifyThrowableInstanceOf(ConcurrentCacheModificationException.class);
    verify(owner, never()).notifyTimestampsToGateways(any());
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWhoseNewRegionEntryThrowsConcurrentCheckAndTimeStampUpdatedThrowsException()
      throws Exception {
    givenConcurrencyChecks(true);
    givenEventWithGatewayTag();
    givenEventTimeStampUpdated();
    givenEntryDestroyThrows(newRegionEntry, ConcurrentCacheModificationException.class);

    doDestroyExpectingThrowable();

    verifyThrowableInstanceOf(ConcurrentCacheModificationException.class);
    verify(owner, times(1)).notifyTimestampsToGateways(any());
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void validateNoDestroyWhenExistingTombstoneAndNewEntryDestroyFails()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenExistingTombstone();
    givenPutEntryIfAbsentReturnsNull();
    givenEventWithGatewayTag();
    givenEntryDestroyReturnsFalse(newRegionEntry);
    givenInTokenMode();

    doDestroy();

    verifyDestroyReturnedFalse();
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

    doDestroy();

    verifyDestroyReturnedFalse();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndEventFromServerAddsATombstone()
      throws Exception {
    givenConcurrencyChecks(true);
    givenRemoteEventWithVersionTag();
    givenEmptyDataPolicy();
    givenEventFromServer();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndWANEventWithConflictAddsATombstoneButDoesNotDoPart3()
      throws Exception {
    givenConcurrencyChecks(true);
    givenEventWithGatewayTag();
    givenEventWithConflict();

    doDestroy();

    verifyDestroyReturnedTrue();
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
    givenEventWithConflict();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryAddedToMap(newRegionEntry);
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksCallsIndexManager() {
    givenConcurrencyChecks(true);
    givenRemoteEventWithVersionTag();
    givenIndexManager();

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyIndexManagerOrder();
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

    doDestroy();

    verifyDestroyReturnedTrue();
    verifyEntryAddedToMap(newRegionEntry);
    verify(regionMap, never()).removeEntry(eq(KEY), same(newRegionEntry), anyBoolean());
    verify(regionMap, never()).removeEntry(eq(KEY), same(newRegionEntry), anyBoolean(), same(event),
        same(owner));
    verifyEntryDestroyed(newRegionEntry, true);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyDoesNotLockGIIClearLockWhenRegionIsInitialized()
      throws Exception {
    DistributedRegion region = mock(DistributedRegion.class, RETURNS_DEEP_STUBS);
    when(region.isInitialized()).thenReturn(true);
    when(region.lockWhenRegionIsInitializing()).thenCallRealMethod();
    RegionMapDestroy mapDestroy =
        new RegionMapDestroy(region, regionMap, mock(CacheModificationLock.class));

    mapDestroy.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction,
        expectedOldValue, removeRecoveredEntry);

    verify(region).lockWhenRegionIsInitializing();
    assertThat(region.lockWhenRegionIsInitializing()).isFalse();
    verify(region, never()).unlockWhenRegionIsInitializing();
  }

  @Test
  public void destroyLockGIIClearLockWhenRegionIsInitializing()
      throws Exception {
    DistributedRegion region = mock(DistributedRegion.class, RETURNS_DEEP_STUBS);
    when(region.isInitialized()).thenReturn(false);
    when(region.lockWhenRegionIsInitializing()).thenCallRealMethod();
    RegionMapDestroy mapDestroy =
        new RegionMapDestroy(region, regionMap, mock(CacheModificationLock.class));

    mapDestroy.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction,
        expectedOldValue, removeRecoveredEntry);

    verify(region).lockWhenRegionIsInitializing();
    assertThat(region.lockWhenRegionIsInitializing()).isTrue();
    verify(region).unlockWhenRegionIsInitializing();
  }

  ///////////////////// given methods /////////////////////////////

  private void givenConcurrencyChecks(boolean enabled) {
    withConcurrencyChecks = enabled;
    when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
  }

  private void givenVersionStampThatDetectsConflict() {
    doThrow(ConcurrentCacheModificationException.class).when(regionMap)
        .processVersionTag(same(existingRegionEntry), same(event));
  }

  private void givenEviction() {
    isEviction = true;
    when(regionMap.confirmEvictionDestroy(any())).thenReturn(true);
  }

  private void givenExistingEntryWithValue(Object value) throws RegionClearedException {
    if (value == Token.TOMBSTONE) {
      givenExistingTombstone();
    } else {
      when(existingRegionEntry.getValue()).thenReturn(value);
    }
    when(regionMap.getEntry(event)).thenReturn(value == null ? null : existingRegionEntry);
    when(regionMap.putEntryIfAbsent(eq(KEY), any())).thenReturn(existingRegionEntry);
    if (withConcurrencyChecks) {
      when(existingRegionEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    }
  }

  private void givenDestroyThrowsRegionClearedException() throws RegionClearedException {
    givenEntryDestroyThrows(existingRegionEntry, RegionClearedException.class);
    when(regionMap.getEntry(event)).thenReturn(null);
    when(factory.createEntry(any(), any(), any())).thenReturn(existingRegionEntry);
  }

  private void givenExistingEntryWithVersionTag(@SuppressWarnings("rawtypes") VersionTag version) {
    // ((VersionStamp) entry).setVersions(version);
    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(owner.getVersionVector()).thenReturn(versionVector);
    when(event.getVersionTag()).thenReturn(version);
  }

  private void givenMissThenExistingEntry() {
    givenExistingEntry(true);
  }

  private void givenExistingEntry() {
    givenExistingEntry(false);
  }

  private void givenExistingEntry(boolean miss) {
    if (miss) {
      when(regionMap.getEntry(event)).thenReturn(null).thenReturn(existingRegionEntry);
    } else {
      when(regionMap.getEntry(event)).thenReturn(existingRegionEntry);
    }
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

  private void givenMissThenExistingTombstone() {
    givenExistingTombstone(true);
  }

  private void givenExistingTombstone() {
    givenExistingTombstone(false);
  }

  private void givenExistingTombstone(boolean miss) {
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

  private void givenDuringRegisterInterest() {
    duringRI = true;
  }

  private void givenRemoveRecoveredEntry() {
    removeRecoveredEntry = true;
  }

  private void givenEntryIsInUseByTransaction() {
    when(regionMap.confirmEvictionDestroy(existingRegionEntry)).thenReturn(false);
    when(existingRegionEntry.isInUseByTransaction()).thenReturn(true);
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

  private void givenTestHook() {
    testHook = mock(Runnable.class);
    RegionMapDestroy.testHookRunnableForConcurrentOperation = testHook;
  }

  private void givenExistingEntryWithNoVersionStamp() {
    when(existingRegionEntry.getVersionStamp()).thenReturn(null);
  }

  private void givenRemovePhase2Retry() {
    when(existingRegionEntry.isRemovedPhase2()).thenReturn(true);
    when(regionMap.getEntry(event)).thenReturn(null);
    when(regionMap.putEntryIfAbsent(eq(KEY), any())).thenReturn(existingRegionEntry)
        .thenReturn(null);
  }

  private void givenEventTimeStampUpdated() {
    when(event.getVersionTag().isTimeStampUpdated()).thenReturn(true);
  }

  private void givenIndexManager() {
    indexManager = mock(IndexManager.class);
    when(owner.getIndexManager()).thenReturn(indexManager);
  }

  private void givenNotDestroyedOrRemoved() {
    when(existingRegionEntry.isRemoved()).thenReturn(false);
    when(existingRegionEntry.isDestroyedOrRemoved()).thenReturn(false);
  }

  private void givenAliveThenTombstone() {
    when(existingRegionEntry.isTombstone()).thenReturn(false).thenReturn(true);
  }

  private void givenTombstoneThenAlive() {
    when(existingRegionEntry.isTombstone()).thenReturn(true).thenReturn(false);
  }

  private void givenFromRILocalDestroy() {
    fromRILocalDestroy = true;
    when(event.isFromRILocalDestroy()).thenReturn(true);
  }

  private void givenExpectedOldValue() {
    expectedOldValue = "OLD_VALUE";
  }

  private void givenExpireDestroy() {
    when(event.getOperation()).thenReturn(Operation.EXPIRE_DESTROY);
  }

  private void givenBridgeWriteBeforeDestroyReturnsTrue() {
    when(owner.bridgeWriteBeforeDestroy(eq(event), any())).thenReturn(true);
  }

  private void givenBridgeWriteBeforeDestroyThrows() {
    doThrow(EntryNotFoundException.class).when(owner).bridgeWriteBeforeDestroy(any(),
        any());
  }

  private void givenLocalDestroy() {
    when(event.getOperation()).thenReturn(Operation.LOCAL_DESTROY);
  }

  private void givenCacheWrite() {
    cacheWrite = true;
  }

  private void givenPutEntryIfAbsentReturnsNull() {
    when(regionMap.putEntryIfAbsent(eq(KEY), any())).thenReturn(null);
  }

  private void givenEmptyDataPolicy() {
    when(owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
  }

  private void givenEventWithConflict() {
    when(event.isConcurrencyConflict()).thenReturn(true);
  }

  ///////////////////// do methods /////////////////////////////

  private void doDestroy() {
    doDestroyResult = regionMapDestroy.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction,
        expectedOldValue, removeRecoveredEntry);
  }

  private void doDestroyExpectingThrowable() {
    doDestroyThrowable = catchThrowable(() -> doDestroy());
  }

  ///////////////////// verify methods /////////////////////////////

  private void verifyThrowableInstanceOf(Class<?> expected) {
    assertThat(doDestroyThrowable).isInstanceOf(expected);
  }

  private void verifyDestroyReturnedTrue() {
    assertThat(doDestroyResult).isTrue();
  }

  private void verifyDestroyReturnedFalse() {
    assertThat(doDestroyResult).isFalse();
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

  private void verifyNoDestroyInvocationsOnEntry(RegionEntry entry) throws RegionClearedException {
    verify(entry, never()).destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean());
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

  private void verifyIndexManagerOrder() {
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

  private void verifyTestHookRun() {
    verify(testHook, times(1)).run();
  }
}
