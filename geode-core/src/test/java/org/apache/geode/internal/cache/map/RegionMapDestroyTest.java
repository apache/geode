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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.AbstractRegionMap;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.RegionMap.Attributes;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VMLRURegionMap;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionMapDestroyTest {

  private static final EvictionAttributes evictionAttributes =
      EvictionAttributes.createLRUEntryAttributes();
  private static final Object KEY = "key";

  private AbstractRegionMap arm;
  private boolean withConcurrencyChecks;
  private CustomEntryConcurrentHashMap<Object, Object> entryMap;
  private RegionEntryFactory factory;
  private LocalRegion owner;
  private EvictableEntry evictableEntry;
  private EvictionController evictionController;
  private Attributes attributes;

  private EntryEventImpl event;
  private Object expectedOldValue;

  private boolean inTokenMode;
  private boolean duringRI;
  private boolean cacheWrite;
  private boolean isEviction;
  private boolean removeRecoveredEntry;
  private boolean invokeCallbacks;

  @Before
  public void setUp() {
    withConcurrencyChecks = true;
    entryMap = null;
    factory = null;

    attributes = new Attributes();

    owner = mock(LocalRegion.class);
    when(owner.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(owner.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
    doThrow(EntryNotFoundException.class).when(owner).checkEntryNotFound(any());

    evictionController = mock(EvictionController.class);
    when(evictionController.getEvictionAlgorithm()).thenReturn(evictionAttributes.getAlgorithm());
    when(evictionController.getCounters()).thenReturn(mock(EvictionCounters.class));

    evictableEntry = mock(EvictableEntry.class);

    event = null;
    inTokenMode = false;
    duringRI = false;
    cacheWrite = false;
    isEviction = false;
    expectedOldValue = null;
    removeRecoveredEntry = false;
    invokeCallbacks = false;
  }

  @After
  public void tearDown() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
  }

  private void givenConcurrencyChecks(boolean enabled) {
    withConcurrencyChecks = enabled;
    when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
  }

  private void givenEmptyRegionMap() {
    arm = new SimpleRegionMap();
    event = createEventForDestroy(arm._getOwner());
  }

  private void givenEmptyRegionMapWithMockedEntryMap() {
    entryMap = mock(CustomEntryConcurrentHashMap.class);
    factory = mock(RegionEntryFactory.class);
    arm = new SimpleRegionMap(entryMap, factory);
    event = createEventForDestroy(arm._getOwner());
  }

  private void givenEviction() {
    when(owner.getEvictionAttributes()).thenReturn(evictionAttributes);
    arm = new EvictableRegionMap();
    event = createEventForDestroy(arm._getOwner());
    isEviction = true;
  }

  private void givenEvictionWithMockedEntryMap() {
    givenEviction();

    entryMap = mock(CustomEntryConcurrentHashMap.class);
    arm = new EvictableRegionMapWithMockedEntryMap();
    event = createEventForDestroy(arm._getOwner());
  }

  private void givenExistingEvictableEntry(Object value) throws RegionClearedException {
    when(evictableEntry.getValue()).thenReturn(value);
    when(entryMap.get(KEY)).thenReturn(value == null ? null : evictableEntry);
    when(entryMap.putIfAbsent(eq(KEY), any())).thenReturn(evictableEntry);
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(true);
  }

  private void givenExistingEvictableEntryWithMockedIsTombstone() throws RegionClearedException {
    givenExistingEvictableEntry("value");
    when(evictableEntry.isTombstone()).thenReturn(true).thenReturn(false);
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(true);
  }

  private void givenDestroyThrowsRegionClearedException() throws RegionClearedException {
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenThrow(RegionClearedException.class);
    when(entryMap.get(KEY)).thenReturn(null);
    when(factory.createEntry(any(), any(), any())).thenReturn(evictableEntry);
  }

  private void givenExistingEntry() {
    RegionEntry entry = arm.getEntryFactory().createEntry(arm._getOwner(), KEY, "value");
    arm.getEntryMap().put(KEY, entry);
  }

  private void givenExistingEntry(Object value) {
    RegionEntry entry = arm.getEntryFactory().createEntry(arm._getOwner(), KEY, value);
    arm.getEntryMap().put(KEY, entry);
  }

  private void givenExistingEntryWithVersionTag() {
    givenExistingEntry();

    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    VersionTag<?> versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
  }

  private void givenExistingEntryWithTokenAndVersionTag(Token token) {
    givenExistingEntry(token);

    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    VersionTag<?> versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
  }

  private void givenRemoteEventWithVersionTag() {
    givenOriginIsRemote();

    RegionVersionVector versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    VersionTag versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
  }

  private void givenInTokenMode() {
    inTokenMode = true;
  }

  private void givenRemoveRecoveredEntry() {
    removeRecoveredEntry = true;
  }

  private void givenEvictableEntryIsInUseByTransaction() {
    when(evictableEntry.isInUseByTransaction()).thenReturn(true);
  }

  private void givenOriginIsRemote() {
    event.setOriginRemote(true);
  }

  @Test
  public void destroyWithEmptyRegionThrowsException() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();

    assertThatThrownBy(() -> arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction,
        expectedOldValue, removeRecoveredEntry)).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeAddsAToken() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateMapContainsTokenValue(Token.DESTROYED);
    validateInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeWithRegionClearedExceptionDoesDestroy()
      throws Exception {
    givenConcurrencyChecks(false);
    givenEmptyRegionMapWithMockedEntryMap();
    givenDestroyThrowsRegionClearedException();
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void evictDestroyWithEmptyRegionInTokenModeDoesNothing() {
    givenEviction();
    givenEmptyRegionMap();
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isFalse();

    validateMapDoesNotContainKey(event.getKey());
    validateNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithExistingTombstoneInTokenModeChangesToDestroyToken() {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntry(Token.TOMBSTONE);
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateMapContainsTokenValue(Token.DESTROYED);
    validateInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void evictDestroyWithExistingTombstoneInTokenModeNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntry(Token.TOMBSTONE);
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void evictDestroyWithExistingTombstoneInUseByTransactionInTokenModeDoesNothing()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry(Token.TOMBSTONE);
    givenEvictableEntryIsInUseByTransaction();
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isFalse();

    validateNoDestroyInvocationsOnEvictableEntry();
    validateNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithConcurrentChangeFromNullToInUseByTransactionInTokenModeDoesNothing()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry(null);
    givenEvictableEntryIsInUseByTransaction();
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isFalse();

    validateNoDestroyInvocationsOnEvictableEntry();
    validateNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndDoesDestroy()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry("value");

    when(entryMap.get(KEY)).thenReturn(null).thenReturn(evictableEntry);

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateInvokedDestroyMethodOnEvictableEntry();
    validateInvokedDestroyMethodsOnRegion(false);
  }


  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry("value");

    when(entryMap.get(KEY)).thenReturn(null).thenReturn(evictableEntry);

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    verify(arm._getOwner(), times(1)).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyInTokenModeWithConcurrentChangeFromNullToRemovePhase2RetriesAndDoesDestroy()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenInTokenMode();

    when(evictableEntry.isRemovedPhase2()).thenReturn(true);
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(true);
    when(entryMap.get(KEY)).thenReturn(null);
    when(entryMap.putIfAbsent(eq(KEY), any())).thenReturn(evictableEntry).thenReturn(null);

    // isEviction is false despite having eviction enabled
    isEviction = false;

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    verify(entryMap).remove(eq(KEY), eq(evictableEntry));
    verify(entryMap, times(2)).putIfAbsent(eq(KEY), any());
    verify(evictableEntry, never()).destroy(eq(arm._getOwner()), eq(event), eq(false), anyBoolean(),
        eq(expectedOldValue), anyBoolean(), anyBoolean());

    validateInvokedDestroyMethodsOnRegion(false);
  }


  @Test
  public void destroyInTokenModeWithConcurrentChangeFromNullToRemovePhase2RetriesAndNeverCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenInTokenMode();

    when(evictableEntry.isRemovedPhase2()).thenReturn(true);
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(true);
    when(entryMap.get(KEY)).thenReturn(null);
    when(entryMap.putIfAbsent(eq(KEY), any())).thenReturn(evictableEntry).thenReturn(null);

    // isEviction is false despite having eviction enabled
    isEviction = false;

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyWithConcurrentChangeFromTombstoneToValidRetriesAndDoesDestroy()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntryWithMockedIsTombstone();

    isEviction = false;

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateInvokedDestroyMethodOnEvictableEntry();
    validateInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithConcurrentChangeFromTombstoneToValidRetriesAndCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntryWithMockedIsTombstone();

    isEviction = false;

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    verify(arm._getOwner(), times(1)).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingEntryInTokenModeAddsAToken() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateMapContainsTokenValue(Token.DESTROYED);
    validateInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryInTokenModeCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    verify(arm._getOwner(), times(1)).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingTombstoneInTokenModeWithConcurrencyChecksDoesNothing() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    // why not DESTROY token? since it was already destroyed why do we do the parts?
    validateMapContainsTokenValue(Token.TOMBSTONE);
    validateInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingTombstoneInTokenModeWithConcurrencyChecksNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);
    givenInTokenMode();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksThrowsEntryNotFound() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);

    assertThatThrownBy(() -> arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction,
        expectedOldValue, removeRecoveredEntry)).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndRemoveRecoveredEntryDoesRemove() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);
    givenRemoveRecoveredEntry();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateMapDoesNotContainKey(event.getKey());
    validateInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndRemoveRecoveredEntryNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);
    givenRemoveRecoveredEntry();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndRemoveRecoveredEntryDoesRetryAndThrowsEntryNotFound() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    givenRemoveRecoveredEntry();

    assertThatThrownBy(() -> arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction,
        expectedOldValue, removeRecoveredEntry)).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyOfExistingEntryRemovesEntryFromMapAndDoesNotifications() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateMapDoesNotContainKey(event.getKey());
    validateInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    verify(arm._getOwner(), times(1)).updateSizeOnRemove(any(), anyInt());
  }

  /**
   * This might be a bug. It seems like we should have created a tombstone but we have no version
   * tag so that might be the cause of this bug.
   */
  @Test
  public void destroyOfExistingEntryWithConcurrencyChecksAndNoVersionTagDestroysWithoutTombstone() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntry();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateMapDoesNotContainKey(event.getKey());
    validateInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryWithConcurrencyChecksAddsTombstone() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithVersionTag();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateMapContainsTokenValue(Token.TOMBSTONE);
    validateInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void evictDestroyOfExistingEntryWithConcurrencyChecksAddsTombstone() {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntryWithVersionTag();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateMapContainsTokenValue(Token.TOMBSTONE);
    validateInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksThrowsException() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();

    assertThatThrownBy(() -> arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction,
        expectedOldValue, removeRecoveredEntry)).isInstanceOf(EntryNotFoundException.class);
  }

  /**
   * This seems to be a bug. We should not leave an evictableEntry in the entryMap added by the
   * destroy call if destroy returns false.
   */
  @Test
  public void evictDestroyWithEmptyRegionWithConcurrencyChecksDoesNothing() {
    givenConcurrencyChecks(true);
    givenEviction();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isFalse();

    validateMapContainsTokenValue(Token.REMOVED_PHASE1);
    validateNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithEmptyRegionDoesNothing() {
    givenConcurrencyChecks(false);
    givenEviction();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isFalse();

    validateMapDoesNotContainKey(event.getKey());
    validateNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAddsATombstone() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenRemoteEventWithVersionTag();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateMapContainsTokenValue(Token.TOMBSTONE);
    validateInvokedDestroyMethodsOnRegion(false);
  }

  /**
   * instead of a TOMBSTONE we leave an evictableEntry whose value is REMOVE_PHASE1 this looks like
   * a bug. It is caused by some code in: AbstractRegionEntry.destroy() that calls removePhase1 when
   * the versionTag is null. It seems like this code path needs to tell the higher levels to call
   * removeEntry
   */
  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndNullVersionTagAddsATombstone() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenOriginIsRemote();

    assertThat(arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry)).isTrue();

    validateMapContainsKey(event.getKey());
    validateMapContainsTokenValue(Token.REMOVED_PHASE1);
    validateInvokedDestroyMethodsOnRegion(false);
  }

  private void validateInvokedDestroyMethodOnEvictableEntry() throws RegionClearedException {
    verify(evictableEntry, times(1)).destroy(eq(arm._getOwner()), eq(event), eq(false),
        anyBoolean(), eq(expectedOldValue), anyBoolean(), anyBoolean());
  }

  private void validateMapContainsKey(Object key) {
    assertThat(arm.getEntryMap()).containsKey(key);
  }

  private void validateMapDoesNotContainKey(Object key) {
    assertThat(arm.getEntryMap()).doesNotContainKey(key);
  }

  private void validateNoDestroyInvocationsOnEvictableEntry() throws RegionClearedException {
    verify(evictableEntry, never()).destroy(any(), any(), anyBoolean(), anyBoolean(), any(),
        anyBoolean(), anyBoolean());
  }

  private void validateMapContainsTokenValue(Token token) {
    assertThat(arm.getEntryMap()).containsKey(event.getKey());
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(token);
  }

  private void validateInvokedDestroyMethodsOnRegion(boolean conflictWithClear) {
    invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(conflictWithClear), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  private void validateNoDestroyInvocationsOnRegion() {
    verify(arm._getOwner(), never()).basicDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).basicDestroyPart3(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), any());
  }

  private EntryEventImpl createEventForDestroy(LocalRegion lr) {
    when(lr.getKeyInfo(KEY)).thenReturn(new KeyInfo(KEY, null, null));
    return EntryEventImpl.create(lr, Operation.DESTROY, KEY, false, null, true, false);
  }

  /**
   * SimpleRegionMap
   */
  private class SimpleRegionMap extends AbstractRegionMap {

    SimpleRegionMap() {
      super(null);
      initialize(owner, attributes, null, false);
    }

    SimpleRegionMap(CustomEntryConcurrentHashMap<Object, Object> entryMap,
        RegionEntryFactory factory) {
      super(null);
      initialize(owner, attributes, null, false);
      setEntryMap(entryMap);
      setEntryFactory(factory);
    }
  }

  /**
   * EvictableRegionMapWithMockedEntryMap
   */
  private class EvictableRegionMapWithMockedEntryMap extends VMLRURegionMap {

    EvictableRegionMapWithMockedEntryMap() {
      super(owner, attributes, null, evictionController);
      setEntryMap(entryMap);
    }
  }

  /**
   * EvictableRegionMap
   */
  private class EvictableRegionMap extends VMLRURegionMap {

    EvictableRegionMap() {
      super(owner, attributes, null, evictionController);
    }
  }

}
