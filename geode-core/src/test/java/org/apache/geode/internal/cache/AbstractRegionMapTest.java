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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.map.RegionMapPut;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionHolder;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.util.concurrent.ConcurrentMapWithReusableEntries;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AbstractRegionMapTest {

  private static final Object KEY = "key";
  private static final EntryEventImpl UPDATEEVENT = mock(EntryEventImpl.class);

  @After
  public void tearDown() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
  }

  @Test
  public void shouldBeMockable() throws Exception {
    AbstractRegionMap mockAbstractRegionMap = mock(AbstractRegionMap.class);
    RegionEntry mockRegionEntry = mock(RegionEntry.class);
    VersionHolder mockVersionHolder = mock(VersionHolder.class);

    when(mockAbstractRegionMap.removeTombstone(eq(mockRegionEntry), eq(mockVersionHolder),
        anyBoolean(), anyBoolean())).thenReturn(true);

    assertThat(
        mockAbstractRegionMap.removeTombstone(mockRegionEntry, mockVersionHolder, true, true))
            .isTrue();
  }

  @Test
  public void invalidateOfNonExistentRegionThrowsEntryNotFound() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm._getOwner());
    when(arm._getOwner().isInitialized()).thenReturn(true);

    assertThatThrownBy(() -> arm.invalidate(event, true, false, false))
        .isInstanceOf(EntryNotFoundException.class);
    verify(arm._getOwner(), never()).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void invalidateOfNonExistentRegionThrowsEntryNotFoundWithForce() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm._getOwner());
    when(arm._getOwner().isInitialized()).thenReturn(true);

    assertThatThrownBy(() -> arm.invalidate(event, true, false, false))
        .isInstanceOf(EntryNotFoundException.class);
    verify(arm._getOwner(), never()).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
    verify(arm._getOwner(), times(1)).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void invalidateOfAlreadyInvalidEntryReturnsFalse() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm._getOwner());

    // invalidate on region that is not initialized should create
    // entry in map as invalid.
    when(arm._getOwner().isInitialized()).thenReturn(false);
    assertThatThrownBy(() -> arm.invalidate(event, true, false, false))
        .isInstanceOf(EntryNotFoundException.class);

    when(arm._getOwner().isInitialized()).thenReturn(true);
    assertFalse(arm.invalidate(event, true, false, false));
    verify(arm._getOwner(), never()).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void invalidateOfAlreadyInvalidEntryReturnsFalseWithForce() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm._getOwner());

    // invalidate on region that is not initialized should create
    // entry in map as invalid.
    when(arm._getOwner().isInitialized()).thenReturn(false);
    assertThatThrownBy(() -> arm.invalidate(event, true, false, false))
        .isInstanceOf(EntryNotFoundException.class);

    when(arm._getOwner().isInitialized()).thenReturn(true);
    assertFalse(arm.invalidate(event, true, false, false));
    verify(arm._getOwner(), never()).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
    verify(arm._getOwner(), times(1)).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void invalidateForceNewEntryOfAlreadyInvalidEntryReturnsFalse() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm._getOwner());

    // invalidate on region that is not initialized should create
    // entry in map as invalid.
    assertTrue(arm.invalidate(event, true, true, false));
    verify(arm._getOwner(), times(1)).basicInvalidatePart2(any(), any(), anyBoolean(),
        anyBoolean());

    when(arm._getOwner().isInitialized()).thenReturn(true);
    assertFalse(arm.invalidate(event, true, true, false));
    verify(arm._getOwner(), times(1)).basicInvalidatePart2(any(), any(), anyBoolean(),
        anyBoolean());
    verify(arm._getOwner(), never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void invalidateForceNewEntryOfAlreadyInvalidEntryReturnsFalseWithForce() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    try {
      TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
      EntryEventImpl event = createEventForInvalidate(arm._getOwner());

      // invalidate on region that is not initialized should create
      // entry in map as invalid.
      when(arm._getOwner().isInitialized()).thenReturn(false);
      assertTrue(arm.invalidate(event, true, true, false));
      verify(arm._getOwner(), times(1)).basicInvalidatePart2(any(), any(), anyBoolean(),
          anyBoolean());
      verify(arm._getOwner(), never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());

      when(arm._getOwner().isInitialized()).thenReturn(true);
      assertFalse(arm.invalidate(event, true, true, false));
      verify(arm._getOwner(), times(1)).basicInvalidatePart2(any(), any(), anyBoolean(),
          anyBoolean());
      verify(arm._getOwner(), times(1)).invokeInvalidateCallbacks(any(), any(), anyBoolean());
    } finally {
      AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
    }
  }

  @Test
  public void destroyWithEmptyRegionThrowsException() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForDestroy(arm._getOwner());
    assertThatThrownBy(() -> arm.destroy(event, false, false, false, false, null, false))
        .isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeAddsAToken() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.DESTROYED);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeWithRegionClearedExceptionDoesDestroy()
      throws Exception {
    CustomEntryConcurrentHashMap<String, EvictableEntry> map =
        mock(CustomEntryConcurrentHashMap.class);
    EvictableEntry entry = mock(EvictableEntry.class);
    when(entry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean()))
        .thenThrow(RegionClearedException.class);
    when(map.get(KEY)).thenReturn(null);
    RegionEntryFactory factory = mock(RegionEntryFactory.class);
    when(factory.createEntry(any(), any(), any())).thenReturn(entry);
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, map, factory);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode), eq(true),
        eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void evictDestroyWithEmptyRegionInTokenModeDoesNothing() {
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    final boolean evict = true;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, evict, expectedOldValue, false))
        .isFalse();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isFalse();
    verify(arm._getOwner(), never()).basicDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).basicDestroyPart3(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), any());
  }

  @Test
  public void evictDestroyWithExistingTombstoneInTokenModeChangesToDestroyToken() {
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true);
    addEntry(arm, Token.TOMBSTONE);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    final boolean evict = true;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, evict, expectedOldValue, false))
        .isTrue();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.DESTROYED);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void evictDestroyWithExistingTombstoneInUseByTransactionInTokenModeDoesNothing()
      throws RegionClearedException {
    CustomEntryConcurrentHashMap<String, EvictableEntry> map =
        mock(CustomEntryConcurrentHashMap.class);
    EvictableEntry entry = mock(EvictableEntry.class);
    when(entry.isInUseByTransaction()).thenReturn(true);
    when(entry.getValue()).thenReturn(Token.TOMBSTONE);
    when(map.get(KEY)).thenReturn(entry);
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true, map);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    final boolean evict = true;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, evict, expectedOldValue, false))
        .isFalse();
    verify(entry, never()).destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean());
    verify(arm._getOwner(), never()).basicDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).basicDestroyPart3(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), any());
  }

  @Test
  public void evictDestroyWithConcurrentChangeFromNullToInUseByTransactionInTokenModeDoesNothing()
      throws RegionClearedException {
    CustomEntryConcurrentHashMap<Object, EvictableEntry> map =
        mock(CustomEntryConcurrentHashMap.class);
    EvictableEntry entry = mock(EvictableEntry.class);
    when(entry.isInUseByTransaction()).thenReturn(true);
    when(map.get(KEY)).thenReturn(null);
    when(map.putIfAbsent(eq(KEY), any())).thenReturn(entry);
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true, map);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    final boolean evict = true;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, evict, expectedOldValue, false))
        .isFalse();
    verify(entry, never()).destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean());
    verify(arm._getOwner(), never()).basicDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).basicDestroyPart3(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), any());
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndDoesDestroy()
      throws RegionClearedException {
    CustomEntryConcurrentHashMap<Object, EvictableEntry> map =
        mock(CustomEntryConcurrentHashMap.class);
    EvictableEntry entry = mock(EvictableEntry.class);
    when(entry.getValue()).thenReturn("value");
    when(entry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean()))
        .thenReturn(true);
    when(map.get(KEY)).thenReturn(null).thenReturn(entry);
    when(map.putIfAbsent(eq(KEY), any())).thenReturn(entry);
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true, map);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    final boolean evict = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, evict, expectedOldValue, false))
        .isTrue();
    verify(entry, times(1)).destroy(eq(arm._getOwner()), eq(event), eq(false), anyBoolean(),
        eq(expectedOldValue), anyBoolean(), anyBoolean());
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyInTokenModeWithConcurrentChangeFromNullToRemovePhase2RetriesAndDoesDestroy()
      throws RegionClearedException {
    CustomEntryConcurrentHashMap<Object, EvictableEntry> map =
        mock(CustomEntryConcurrentHashMap.class);
    EvictableEntry entry = mock(EvictableEntry.class);
    when(entry.isRemovedPhase2()).thenReturn(true);
    when(entry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean()))
        .thenReturn(true);
    when(map.get(KEY)).thenReturn(null);
    when(map.putIfAbsent(eq(KEY), any())).thenReturn(entry).thenReturn(null);
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true, map);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    final boolean evict = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, evict, expectedOldValue, false))
        .isTrue();
    verify(map).remove(eq(KEY), eq(entry));
    verify(map, times(2)).putIfAbsent(eq(KEY), any());
    verify(entry, never()).destroy(eq(arm._getOwner()), eq(event), eq(false), anyBoolean(),
        eq(expectedOldValue), anyBoolean(), anyBoolean());
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyWithConcurrentChangeFromTombstoneToValidRetriesAndDoesDestroy()
      throws RegionClearedException {
    CustomEntryConcurrentHashMap<Object, EvictableEntry> map =
        mock(CustomEntryConcurrentHashMap.class);
    EvictableEntry entry = mock(EvictableEntry.class);
    when(entry.getValue()).thenReturn("value");
    when(entry.isTombstone()).thenReturn(true).thenReturn(false);
    when(entry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean()))
        .thenReturn(true);
    when(map.get(KEY)).thenReturn(entry);
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true, map);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    final boolean evict = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, evict, expectedOldValue, false))
        .isTrue();
    verify(entry, times(1)).destroy(eq(arm._getOwner()), eq(event), eq(false), anyBoolean(),
        eq(expectedOldValue), anyBoolean(), anyBoolean());
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyOfExistingEntryInTokenModeAddsAToken() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    addEntry(arm);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.DESTROYED);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyOfExistingTombstoneInTokenModeWithConcurrencyChecksDoesNothing() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    VersionTag<?> versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
    addEntry(arm, Token.TOMBSTONE);
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    // why not DESTROY token?
    assertThat(re.getValueAsToken()).isEqualTo(Token.TOMBSTONE);
    // since it was already destroyed why do we do the parts?
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksThrowsEntryNotFound() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    VersionTag<?> versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
    addEntry(arm, Token.TOMBSTONE);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThatThrownBy(
        () -> arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
            .isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndRemoveRecoveredEntryDoesRemove() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    VersionTag<?> versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
    addEntry(arm, Token.TOMBSTONE);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    final boolean removeRecoveredEntry = true;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue,
        removeRecoveredEntry)).isTrue();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isFalse();
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndRemoveRecoveredEntryDoesRetryAndThrowsEntryNotFound() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    VersionTag<?> versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
    addEntry(arm, Token.REMOVED_PHASE2);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    final boolean removeRecoveredEntry = true;
    assertThatThrownBy(() -> arm.destroy(event, inTokenMode, duringRI, false, false,
        expectedOldValue, removeRecoveredEntry)).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyOfExistingEntryRemovesEntryFromMapAndDoesNotifications() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    addEntry(arm);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isFalse();
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyOfExistingEntryWithConcurrencyChecksAndNoVersionTagDestroysWithoutTombstone() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    addEntry(arm);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    // This might be a bug. It seems like we should have created a tombstone but we have no
    // version tag so that might be the cause of this bug.
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isFalse();
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyOfExistingEntryWithConcurrencyChecksAddsTombstone() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    RegionVersionVector versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    addEntry(arm);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    VersionTag versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.TOMBSTONE);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void evictDestroyOfExistingEntryWithConcurrencyChecksAddsTombstone() {
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true);
    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    addEntry(arm);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    VersionTag<?> versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    final boolean evict = true;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, evict, expectedOldValue, false))
        .isTrue();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.TOMBSTONE);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksThrowsException() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    EntryEventImpl event = createEventForDestroy(arm._getOwner());
    assertThatThrownBy(() -> arm.destroy(event, false, false, false, false, null, false))
        .isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void evictDestroyWithEmptyRegionWithConcurrencyChecksDoesNothing() {
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true);
    EntryEventImpl event = createEventForDestroy(arm._getOwner());
    assertThat(arm.destroy(event, false, false, false, true, null, false)).isFalse();
    verify(arm._getOwner(), never()).basicDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).basicDestroyPart3(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), any());
    // This seems to be a bug. We should not leave an entry in the map
    // added by the destroy call if destroy returns false.
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.REMOVED_PHASE1);
  }

  @Test
  public void evictDestroyWithEmptyRegionDoesNothing() {
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(false);
    EntryEventImpl event = createEventForDestroy(arm._getOwner());
    assertThat(arm.destroy(event, false, false, false, true, null, false)).isFalse();
    verify(arm._getOwner(), never()).basicDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).basicDestroyPart3(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), any());
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isFalse();
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAddsATombstone() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    RegionVersionVector versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    VersionTag versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
    event.setOriginRemote(true);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.TOMBSTONE);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndNullVersionTagAddsATombstone() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    event.setOriginRemote(true);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm.getEntryMap().containsKey(event.getKey())).isTrue();
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
    // instead of a TOMBSTONE we leave an entry whose value is REMOVE_PHASE1
    // this looks like a bug. It is caused by some code in: AbstractRegionEntry.destroy()
    // that calls removePhase1 when the versionTag is null.
    // It seems like this code path needs to tell the higher levels
    // to call removeEntry
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.REMOVED_PHASE1);
  }

  @Test
  public void txApplyInvalidateDoesNotInvalidateRemovedToken() throws RegionClearedException {
    TxTestableAbstractRegionMap arm = new TxTestableAbstractRegionMap();

    Object newValue = "value";
    arm.txApplyPut(Operation.CREATE, KEY, newValue, false,
        new TXId(mock(InternalDistributedMember.class), 1), mock(TXRmtEvent.class),
        mock(EventID.class), null, null, null, null, null, null, 1);
    RegionEntry re = arm.getEntry(KEY);
    assertNotNull(re);

    Token[] removedTokens =
        {Token.REMOVED_PHASE2, Token.REMOVED_PHASE1, Token.DESTROYED, Token.TOMBSTONE};

    for (Token token : removedTokens) {
      verifyTxApplyInvalidate(arm, KEY, re, token);
    }
  }

  private EntryEventImpl createEventForInvalidate(LocalRegion lr) {
    when(lr.getKeyInfo(KEY)).thenReturn(new KeyInfo(KEY, null, null));
    return EntryEventImpl.create(lr, Operation.INVALIDATE, KEY, false, null, true, false);
  }

  private EntryEventImpl createEventForDestroy(LocalRegion lr) {
    when(lr.getKeyInfo(KEY)).thenReturn(new KeyInfo(KEY, null, null));
    return EntryEventImpl.create(lr, Operation.DESTROY, KEY, false, null, true, false);
  }

  private void addEntry(AbstractRegionMap arm) {
    addEntry(arm, "value");
  }

  private void addEntry(AbstractRegionMap arm, Object value) {
    RegionEntry entry = arm.getEntryFactory().createEntry(arm._getOwner(), KEY, value);
    arm.getEntryMap().put(KEY, entry);
  }

  private void verifyTxApplyInvalidate(TxTestableAbstractRegionMap arm, Object key, RegionEntry re,
      Token token) throws RegionClearedException {
    re.setValue(arm._getOwner(), token);
    arm.txApplyInvalidate(key, Token.INVALID, false,
        new TXId(mock(InternalDistributedMember.class), 1), mock(TXRmtEvent.class), false,
        mock(EventID.class), null, null, null, null, null, null, 1);
    assertEquals(re.getValueAsToken(), token);
  }

  @Test
  public void updateOnEmptyMapReturnsNull() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    final EntryEventImpl event = createEventForCreate(arm._getOwner(), "key");

    RegionEntry result = arm.basicPut(event, 0L, false, true, null, false, false);

    assertThat(result).isNull();
    verify(arm._getOwner(), never()).basicPutPart2(any(), any(), anyBoolean(), anyLong(),
        anyBoolean());
    verify(arm._getOwner(), never()).basicPutPart3(any(), any(), anyBoolean(), anyLong(),
        anyBoolean(), anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void createOnExistingEntryReturnsNull() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    // do a create to get a region entry in the map
    arm.basicPut(createEventForCreate(arm._getOwner(), "key"), 0L, true, false, null, false, false);
    final EntryEventImpl event = createEventForCreate(arm._getOwner(), "key");
    final boolean ifNew = true;
    final boolean ifOld = false;
    final boolean requireOldValue = false;
    final Object expectedOldValue = null;

    RegionEntry result =
        arm.basicPut(event, 0L, ifNew, ifOld, expectedOldValue, requireOldValue, false);
    assertThat(result).isNull();
    verify(arm._getOwner(), never()).basicPutPart2(eq(event), any(), anyBoolean(), anyLong(),
        anyBoolean());
    verify(arm._getOwner(), never()).basicPutPart3(eq(event), any(), anyBoolean(), anyLong(),
        anyBoolean(), anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void createOnEntryReturnedFromPutIfAbsentDoesNothing() {
    CustomEntryConcurrentHashMap<String, RegionEntry> map =
        mock(CustomEntryConcurrentHashMap.class);
    RegionEntry entry = mock(RegionEntry.class);
    when(entry.getValueAsToken()).thenReturn(Token.NOT_A_TOKEN);
    when(map.get(KEY)).thenReturn(null);
    when(map.putIfAbsent((String) eq(KEY), any())).thenReturn(entry);
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, map, null);
    final EntryEventImpl event = createEventForCreate(arm._getOwner(), "key");
    final boolean ifNew = true;
    final boolean ifOld = false;
    final boolean requireOldValue = false;
    final Object expectedOldValue = null;

    RegionEntry result =
        arm.basicPut(event, 0L, ifNew, ifOld, expectedOldValue, requireOldValue, false);
    assertThat(result).isNull();
    verify(arm._getOwner(), never()).basicPutPart2(eq(event), any(), anyBoolean(), anyLong(),
        anyBoolean());
    verify(arm._getOwner(), never()).basicPutPart3(eq(event), any(), anyBoolean(), anyLong(),
        anyBoolean(), anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void createOnExistingEntryWithRemovePhase2DoesCreate() throws RegionClearedException {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    // do a create to get a region entry in the map
    RegionEntry createdEntry = arm.basicPut(createEventForCreate(arm._getOwner(), "key"), 0L, true,
        false, null, false, false);
    createdEntry.setValue(null, Token.REMOVED_PHASE2);
    final EntryEventImpl event = createEventForCreate(arm._getOwner(), "key");
    event.setNewValue("create");
    final boolean ifNew = true;
    final boolean ifOld = false;
    final boolean requireOldValue = false;
    final Object expectedOldValue = null;

    RegionEntry result =
        arm.basicPut(event, 0L, ifNew, ifOld, expectedOldValue, requireOldValue, false);

    assertThat(result).isNotNull();
    assertThat(result).isNotSameAs(createdEntry);
    assertThat(result.getKey()).isEqualTo("key");
    assertThat(result.getValue()).isEqualTo("create");
    verify(arm._getOwner(), times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(arm._getOwner(), times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }


  @Test
  public void updateOnExistingEntryDoesUpdate() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    // do a create to get a region entry in the map
    RegionEntry createdEntry = arm.basicPut(createEventForCreate(arm._getOwner(), "key"), 0L, true,
        false, null, false, false);
    final EntryEventImpl event = createEventForCreate(arm._getOwner(), "key");
    event.setNewValue("update");
    final boolean ifNew = false;
    final boolean ifOld = true;
    final boolean requireOldValue = false;
    final Object expectedOldValue = null;

    RegionEntry result =
        arm.basicPut(event, 0L, ifNew, ifOld, expectedOldValue, requireOldValue, false);

    assertThat(result).isSameAs(createdEntry);
    assertThat(result.getKey()).isEqualTo("key");
    assertThat(result.getValue()).isEqualTo("update");
    verify(arm._getOwner(), times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(arm._getOwner(), times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }

  /**
   * TestableAbstractRegionMap
   */
  private static class TestableAbstractRegionMap extends AbstractRegionMap {

    protected TestableAbstractRegionMap() {
      this(false);
    }

    protected TestableAbstractRegionMap(boolean withConcurrencyChecks) {
      this(withConcurrencyChecks, null, null);
    }

    protected TestableAbstractRegionMap(boolean withConcurrencyChecks,
        ConcurrentMapWithReusableEntries map, RegionEntryFactory factory) {
      super(null);
      LocalRegion owner = mock(LocalRegion.class);
      CachePerfStats cachePerfStats = mock(CachePerfStats.class);
      when(owner.getCachePerfStats()).thenReturn(cachePerfStats);
      when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
      when(owner.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
      when(owner.getScope()).thenReturn(Scope.LOCAL);
      when(owner.isInitialized()).thenReturn(true);
      doThrow(EntryNotFoundException.class).when(owner).checkEntryNotFound(any());
      initialize(owner, new Attributes(), null, false);
      if (map != null) {
        setEntryMap(map);
      }
      if (factory != null) {
        setEntryFactory(factory);
      }
    }
  }


  /**
   * TestableVMLRURegionMap
   */
  private static class TestableVMLRURegionMap extends VMLRURegionMap {
    private static EvictionAttributes evictionAttributes =
        EvictionAttributes.createLRUEntryAttributes();

    protected TestableVMLRURegionMap() {
      this(false);
    }

    private static LocalRegion createOwner(boolean withConcurrencyChecks) {
      LocalRegion owner = mock(LocalRegion.class);
      CachePerfStats cachePerfStats = mock(CachePerfStats.class);
      when(owner.getCachePerfStats()).thenReturn(cachePerfStats);
      when(owner.getEvictionAttributes()).thenReturn(evictionAttributes);
      when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
      when(owner.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
      doThrow(EntryNotFoundException.class).when(owner).checkEntryNotFound(any());
      return owner;
    }

    private static EvictionController createEvictionController() {
      EvictionController result = mock(EvictionController.class);
      when(result.getEvictionAlgorithm()).thenReturn(evictionAttributes.getAlgorithm());
      EvictionCounters evictionCounters = mock(EvictionCounters.class);
      when(result.getCounters()).thenReturn(evictionCounters);
      return result;
    }

    protected TestableVMLRURegionMap(boolean withConcurrencyChecks) {
      super(createOwner(withConcurrencyChecks), new Attributes(), null, createEvictionController());
    }

    protected TestableVMLRURegionMap(boolean withConcurrencyChecks,
        ConcurrentMapWithReusableEntries hashMap) {
      this(withConcurrencyChecks);
      setEntryMap(hashMap);
    }
  }

  /**
   * TxTestableAbstractRegionMap
   */
  private static class TxTestableAbstractRegionMap extends AbstractRegionMap {

    protected TxTestableAbstractRegionMap() {
      super(null);
      LocalRegion owner = mock(LocalRegion.class);
      when(owner.getCache()).thenReturn(mock(InternalCache.class));
      when(owner.isAllEvents()).thenReturn(true);
      when(owner.isInitialized()).thenReturn(true);
      when(owner.shouldNotifyBridgeClients()).thenReturn(true);
      initialize(owner, new Attributes(), null, false);
    }
  }

  @Test
  public void txApplyPutOnSecondaryConstructsPendingCallbacksWhenRegionEntryExists()
      throws Exception {
    AbstractRegionMap arm = new TxRegionEntryTestableAbstractRegionMap();
    List<EntryEventImpl> pendingCallbacks = new ArrayList<>();

    Object newValue = "value";
    arm.txApplyPut(Operation.UPDATE, KEY, newValue, false,
        new TXId(mock(InternalDistributedMember.class), 1), mock(TXRmtEvent.class),
        mock(EventID.class), null, pendingCallbacks, null, null, null, null, 1);

    assertEquals(1, pendingCallbacks.size());
    verify(arm._getOwner(), never()).invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, UPDATEEVENT,
        false);
  }

  @Test
  public void txApplyPutOnSecondaryNotifiesClientsWhenRegionEntryIsRemoved() throws Exception {
    AbstractRegionMap arm = new TxRemovedRegionEntryTestableAbstractRegionMap();
    List<EntryEventImpl> pendingCallbacks = new ArrayList<>();

    Object newValue = "value";
    arm.txApplyPut(Operation.UPDATE, KEY, newValue, false,
        new TXId(mock(InternalDistributedMember.class), 1), mock(TXRmtEvent.class),
        mock(EventID.class), null, pendingCallbacks, null, null, null, null, 1);

    assertEquals(0, pendingCallbacks.size());
    verify(arm._getOwner(), times(1)).invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, UPDATEEVENT,
        false);
  }

  @Test
  public void txApplyPutOnSecondaryNotifiesClientsWhenRegionEntryIsNull() throws Exception {
    AbstractRegionMap arm = new TxNoRegionEntryTestableAbstractRegionMap();
    List<EntryEventImpl> pendingCallbacks = new ArrayList<>();

    Object newValue = "value";
    arm.txApplyPut(Operation.UPDATE, KEY, newValue, false,
        new TXId(mock(InternalDistributedMember.class), 1), mock(TXRmtEvent.class),
        mock(EventID.class), null, pendingCallbacks, null, null, null, null, 1);

    assertEquals(0, pendingCallbacks.size());
    verify(arm._getOwner(), times(1)).invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, UPDATEEVENT,
        false);
  }

  private EntryEventImpl createEventForCreate(LocalRegion lr, String key) {
    when(lr.getKeyInfo(key)).thenReturn(new KeyInfo(key, null, null));
    EntryEventImpl event =
        EntryEventImpl.create(lr, Operation.CREATE, key, false, null, true, false);
    event.setNewValue("create_value");
    return event;
  }

  private static class TxNoRegionEntryTestableAbstractRegionMap
      extends TxTestableAbstractRegionMap {
    @Override
    public RegionEntry getEntry(Object key) {
      return null;
    }

    @Override
    EntryEventImpl createCallBackEvent(final LocalRegion re, Operation op, Object key,
        Object newValue, TransactionId txId, TXRmtEvent txEvent, EventID eventId,
        Object aCallbackArgument, FilterRoutingInfo filterRoutingInfo,
        ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
        long tailKey) {
      return UPDATEEVENT;
    }
  }

  private static class TxRegionEntryTestableAbstractRegionMap extends TxTestableAbstractRegionMap {
    @Override
    public RegionEntry getEntry(Object key) {
      return mock(RegionEntry.class);
    }
  }

  private static class TxRemovedRegionEntryTestableAbstractRegionMap
      extends TxTestableAbstractRegionMap {
    @Override
    public RegionEntry getEntry(Object key) {
      RegionEntry regionEntry = mock(RegionEntry.class);
      when(regionEntry.isRemoved()).thenReturn(true);
      return regionEntry;
    }

    @Override
    EntryEventImpl createCallBackEvent(final LocalRegion re, Operation op, Object key,
        Object newValue, TransactionId txId, TXRmtEvent txEvent, EventID eventId,
        Object aCallbackArgument, FilterRoutingInfo filterRoutingInfo,
        ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
        long tailKey) {
      return UPDATEEVENT;
    }
  }

}
