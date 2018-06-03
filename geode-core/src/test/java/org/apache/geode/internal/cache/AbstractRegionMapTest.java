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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
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

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.entries.DiskEntry.RecoveredEntry;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionHolder;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
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
        mock(EventID.class), null, new ArrayList<EntryEventImpl>(), null, null, null, null, 1);
    RegionEntry re = arm.getEntry(KEY);
    assertNotNull(re);

    Token[] removedTokens =
        {Token.REMOVED_PHASE2, Token.REMOVED_PHASE1, Token.DESTROYED, Token.TOMBSTONE};

    for (Token token : removedTokens) {
      verifyTxApplyInvalidate(arm, KEY, re, token);
    }
  }

  @Test
  public void updateRecoveredEntry_givenExistingDestroyedOrRemovedAndSettingToTombstone_neverCallsUpdateSizeOnRemove() {
    RecoveredEntry recoveredEntry = mock(RecoveredEntry.class);
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(regionEntry.isTombstone()).thenReturn(false).thenReturn(true);
    when(regionEntry.isDestroyedOrRemoved()).thenReturn(true);
    when(regionEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, null, null, regionEntry);

    arm.updateRecoveredEntry(KEY, recoveredEntry);

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void updateRecoveredEntry_givenExistingRemovedNonTombstone_neverCallsUpdateSizeOnRemove() {
    RecoveredEntry recoveredEntry = mock(RecoveredEntry.class);
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(regionEntry.isRemoved()).thenReturn(true);
    when(regionEntry.isTombstone()).thenReturn(false);
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, null, null, regionEntry);

    arm.updateRecoveredEntry(KEY, recoveredEntry);

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void updateRecoveredEntry_givenNoExistingEntry_neverCallsUpdateSizeOnRemove() {
    RecoveredEntry recoveredEntry = mock(RecoveredEntry.class);
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, null, null, null);

    arm.updateRecoveredEntry(KEY, recoveredEntry);

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void updateRecoveredEntry_givenExistingNonTombstoneAndSettingToTombstone_callsUpdateSizeOnRemove() {
    RecoveredEntry recoveredEntry = mock(RecoveredEntry.class);
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(regionEntry.isTombstone()).thenReturn(false).thenReturn(true);
    when(regionEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, null, null, regionEntry);

    arm.updateRecoveredEntry(KEY, recoveredEntry);

    verify(arm._getOwner(), times(1)).updateSizeOnRemove(eq(KEY), anyInt());
  }

  @Test
  public void initialImagePut_givenPutIfAbsentReturningDestroyedOrRemovedEntry_neverCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    ConcurrentMapWithReusableEntries map = mock(ConcurrentMapWithReusableEntries.class);
    RegionEntry entry = mock(RegionEntry.class);
    when(entry.isDestroyedOrRemoved()).thenReturn(true);
    when(entry.initialImagePut(any(), anyLong(), any(), anyBoolean(), anyBoolean()))
        .thenReturn(true);
    VersionStamp versionStamp = mock(VersionStamp.class);
    when(entry.getVersionStamp()).thenReturn(versionStamp);
    when(versionStamp.asVersionTag()).thenReturn(mock(VersionTag.class));
    when(map.putIfAbsent(eq(KEY), any())).thenReturn(entry).thenReturn(null);
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, map, null);
    when(arm._getOwner().getConcurrencyChecksEnabled()).thenReturn(true);
    when(arm._getOwner().getServerProxy()).thenReturn(mock(ServerRegionProxy.class));
    VersionTag versionTag = mock(VersionTag.class);
    when(versionTag.getMemberID()).thenReturn(mock(VersionSource.class));

    arm.initialImagePut(KEY, 0, Token.TOMBSTONE, false, false, versionTag, null, false);

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void initialImagePut_givenPutIfAbsentReturningNonTombstone_callsUpdateSizeOnRemove()
      throws RegionClearedException {
    ConcurrentMapWithReusableEntries map = mock(ConcurrentMapWithReusableEntries.class);
    RegionEntry entry = mock(RegionEntry.class);
    when(entry.isTombstone()).thenReturn(false);
    when(entry.isDestroyedOrRemoved()).thenReturn(false);
    when(entry.initialImagePut(any(), anyLong(), any(), anyBoolean(), anyBoolean()))
        .thenReturn(true);
    VersionStamp versionStamp = mock(VersionStamp.class);
    when(entry.getVersionStamp()).thenReturn(versionStamp);
    when(versionStamp.asVersionTag()).thenReturn(mock(VersionTag.class));
    when(map.putIfAbsent(eq(KEY), any())).thenReturn(entry).thenReturn(null);
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, map, null);
    when(arm._getOwner().getConcurrencyChecksEnabled()).thenReturn(true);
    when(arm._getOwner().getServerProxy()).thenReturn(mock(ServerRegionProxy.class));
    VersionTag versionTag = mock(VersionTag.class);
    when(versionTag.getMemberID()).thenReturn(mock(VersionSource.class));

    arm.initialImagePut(KEY, 0, Token.TOMBSTONE, false, false, versionTag, null, false);

    verify(arm._getOwner(), times(1)).updateSizeOnRemove(eq(KEY), anyInt());
  }

  @Test
  public void txApplyDestroy_givenExistingDestroyedOrRemovedEntry_neverCallsUpdateSizeOnRemove() {
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(regionEntry.isTombstone()).thenReturn(false);
    when(regionEntry.isDestroyedOrRemoved()).thenReturn(true);
    when(regionEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    TXId txId = mock(TXId.class);
    when(txId.getMemberId()).thenReturn(mock(InternalDistributedMember.class));
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, null, null, regionEntry);

    arm.txApplyDestroy(KEY, txId, null, false, false, null, null, null, new ArrayList<>(), null,
        null, false, null, null, 0);

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void txApplyDestroy_givenExistingNonTombstone_callsUpdateSizeOnRemove() {
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(regionEntry.isTombstone()).thenReturn(false);
    when(regionEntry.getVersionStamp()).thenReturn(mock(VersionStamp.class));
    TXId txId = mock(TXId.class);
    when(txId.getMemberId()).thenReturn(mock(InternalDistributedMember.class));
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, null, null, regionEntry);

    arm.txApplyDestroy(KEY, txId, null, false, false, null, null, null, new ArrayList<>(), null,
        null, false, null, null, 0);

    verify(arm._getOwner(), times(1)).updateSizeOnRemove(eq(KEY), anyInt());
  }

  @Test
  public void txApplyDestroy_givenPutIfAbsentReturningDestroyedOrRemovedEntry_neverCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    ConcurrentMapWithReusableEntries map = mock(ConcurrentMapWithReusableEntries.class);
    RegionEntry entry = mock(RegionEntry.class);
    when(entry.isTombstone()).thenReturn(false);
    when(entry.isDestroyedOrRemoved()).thenReturn(true);
    VersionStamp versionStamp = mock(VersionStamp.class);
    when(entry.getVersionStamp()).thenReturn(versionStamp);
    when(versionStamp.asVersionTag()).thenReturn(mock(VersionTag.class));
    when(map.putIfAbsent(eq(KEY), any())).thenReturn(entry).thenReturn(null);
    TXId txId = mock(TXId.class);
    when(txId.getMemberId()).thenReturn(mock(InternalDistributedMember.class));
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, map, null);
    when(arm._getOwner().getConcurrencyChecksEnabled()).thenReturn(true);

    arm.txApplyDestroy(KEY, txId, null, false, false, null, null, null, new ArrayList<>(), null,
        null, false, null, null, 0);

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void txApplyDestroy_givenPutIfAbsentReturningNonTombstone_callsUpdateSizeOnRemove()
      throws RegionClearedException {
    ConcurrentMapWithReusableEntries map = mock(ConcurrentMapWithReusableEntries.class);
    RegionEntry entry = mock(RegionEntry.class);
    when(entry.getKey()).thenReturn(KEY);
    when(entry.isTombstone()).thenReturn(false);
    VersionStamp versionStamp = mock(VersionStamp.class);
    when(entry.getVersionStamp()).thenReturn(versionStamp);
    when(versionStamp.asVersionTag()).thenReturn(mock(VersionTag.class));
    when(map.putIfAbsent(eq(KEY), any())).thenReturn(entry).thenReturn(null);
    TXId txId = mock(TXId.class);
    when(txId.getMemberId()).thenReturn(mock(InternalDistributedMember.class));
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, map, null);
    when(arm._getOwner().getConcurrencyChecksEnabled()).thenReturn(true);

    arm.txApplyDestroy(KEY, txId, null, false, false, null, null, null, new ArrayList<>(), null,
        null, false, null, null, 0);

    verify(arm._getOwner(), times(1)).updateSizeOnRemove(eq(KEY), anyInt());
  }

  @Test
  public void txApplyDestroy_givenFactory_neverCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    ConcurrentMapWithReusableEntries map = mock(ConcurrentMapWithReusableEntries.class);
    RegionEntry entry = mock(RegionEntry.class);
    TXId txId = mock(TXId.class);
    when(txId.getMemberId()).thenReturn(mock(InternalDistributedMember.class));
    RegionEntryFactory factory = mock(RegionEntryFactory.class);
    when(factory.createEntry(any(), any(), any())).thenReturn(entry);
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(false, map, factory);
    when(arm._getOwner().getConcurrencyChecksEnabled()).thenReturn(true);

    arm.txApplyDestroy(KEY, txId, null, false, false, null, null, null, new ArrayList<>(), null,
        null, false, null, null, 0);

    verify(arm._getOwner(), never()).updateSizeOnCreate(any(), anyInt());
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
        mock(EventID.class), null, new ArrayList<EntryEventImpl>(), null, null, null, null, 1);
    assertEquals(re.getValueAsToken(), token);
  }

  /**
   * TestableAbstractRegionMap
   */
  private static class TestableAbstractRegionMap extends AbstractRegionMap {
    private final RegionEntry regionEntryForGetEntry;

    protected TestableAbstractRegionMap() {
      this(false);
    }

    protected TestableAbstractRegionMap(boolean withConcurrencyChecks) {
      this(withConcurrencyChecks, null, null);
    }

    protected TestableAbstractRegionMap(boolean withConcurrencyChecks,
        ConcurrentMapWithReusableEntries map, RegionEntryFactory factory) {
      this(withConcurrencyChecks, map, factory, null);
    }

    protected TestableAbstractRegionMap(boolean withConcurrencyChecks,
        ConcurrentMapWithReusableEntries map, RegionEntryFactory factory,
        RegionEntry regionEntryForGetEntry) {
      super(null);
      this.regionEntryForGetEntry = regionEntryForGetEntry;
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

    @Override
    public RegionEntry getEntry(Object key) {
      if (this.regionEntryForGetEntry != null) {
        return this.regionEntryForGetEntry;
      } else {
        return super.getEntry(key);
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
      KeyInfo keyInfo = mock(KeyInfo.class);
      when(keyInfo.getKey()).thenReturn(KEY);
      when(owner.getKeyInfo(eq(KEY), any(), any())).thenReturn(keyInfo);
      when(owner.getMyId()).thenReturn(mock(InternalDistributedMember.class));
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
    TXId txId = new TXId(mock(InternalDistributedMember.class), 1);
    TXRmtEvent txRmtEvent = mock(TXRmtEvent.class);
    EventID eventId = mock(EventID.class);
    Object newValue = "value";

    arm.txApplyPut(Operation.UPDATE, KEY, newValue, false, txId, txRmtEvent, eventId, null,
        pendingCallbacks, null, null, null, null, 1);

    assertEquals(1, pendingCallbacks.size());
    verify(arm._getOwner(), times(1)).txApplyPutPart2(any(), any(), anyLong(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, UPDATEEVENT,
        false);
  }

  @Test
  public void txApplyPutOnPrimaryConstructsPendingCallbacksWhenPutIfAbsentReturnsExistingEntry()
      throws Exception {
    AbstractRegionMap arm = new TxPutIfAbsentTestableAbstractRegionMap();
    List<EntryEventImpl> pendingCallbacks = new ArrayList<>();
    TXId txId = new TXId(arm._getOwner().getMyId(), 1);
    EventID eventId = mock(EventID.class);
    TXEntryState txEntryState = mock(TXEntryState.class);
    Object newValue = "value";

    arm.txApplyPut(Operation.UPDATE, KEY, newValue, false, txId, null, eventId, null,
        pendingCallbacks, null, null, txEntryState, null, 1);

    assertEquals(1, pendingCallbacks.size());
    verify(arm._getOwner(), times(1)).txApplyPutPart2(any(), any(), anyLong(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, UPDATEEVENT,
        false);
  }

  @Test
  public void txApplyPutOnSecondaryNotifiesClientsWhenRegionEntryIsRemoved() throws Exception {
    AbstractRegionMap arm = new TxRemovedRegionEntryTestableAbstractRegionMap();
    List<EntryEventImpl> pendingCallbacks = new ArrayList<>();
    TXId txId = new TXId(mock(InternalDistributedMember.class), 1);
    TXRmtEvent txRmtEvent = mock(TXRmtEvent.class);
    EventID eventId = mock(EventID.class);
    Object newValue = "value";

    arm.txApplyPut(Operation.UPDATE, KEY, newValue, false, txId, txRmtEvent, eventId, null,
        pendingCallbacks, null, null, null, null, 1);

    assertEquals(0, pendingCallbacks.size());
    verify(arm._getOwner(), never()).txApplyPutPart2(any(), any(), anyLong(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), times(1)).invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, UPDATEEVENT,
        false);
  }

  @Test
  public void txApplyPutOnSecondaryNotifiesClientsWhenRegionEntryIsNull() throws Exception {
    AbstractRegionMap arm = new TxNoRegionEntryTestableAbstractRegionMap();
    List<EntryEventImpl> pendingCallbacks = new ArrayList<>();
    TXId txId = new TXId(mock(InternalDistributedMember.class), 1);
    TXRmtEvent txRmtEvent = mock(TXRmtEvent.class);
    EventID eventId = mock(EventID.class);
    Object newValue = "value";

    arm.txApplyPut(Operation.UPDATE, KEY, newValue, false, txId, txRmtEvent, eventId, null,
        pendingCallbacks, null, null, null, null, 1);

    assertEquals(0, pendingCallbacks.size());
    verify(arm._getOwner(), never()).txApplyPutPart2(any(), any(), anyLong(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), times(1)).invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, UPDATEEVENT,
        false);
  }

  private static class TxNoRegionEntryTestableAbstractRegionMap
      extends TxTestableAbstractRegionMap {
    @Override
    public RegionEntry getEntry(Object key) {
      return null;
    }

    @Override
    EntryEventImpl createTransactionCallbackEvent(final LocalRegion re, Operation op, Object key,
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

  private static class TxPutIfAbsentTestableAbstractRegionMap extends TxTestableAbstractRegionMap {
    @Override
    public RegionEntry putEntryIfAbsent(Object key, RegionEntry newRe) {
      return mock(RegionEntry.class);
    }
  }

  private static class TxRemovedRegionEntryTestableAbstractRegionMap
      extends TxTestableAbstractRegionMap {
    @Override
    public RegionEntry getEntry(Object key) {
      RegionEntry regionEntry = mock(RegionEntry.class);
      when(regionEntry.isDestroyedOrRemoved()).thenReturn(true);
      return regionEntry;
    }

    @Override
    EntryEventImpl createTransactionCallbackEvent(final LocalRegion re, Operation op, Object key,
        Object newValue, TransactionId txId, TXRmtEvent txEvent, EventID eventId,
        Object aCallbackArgument, FilterRoutingInfo filterRoutingInfo,
        ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
        long tailKey) {
      return UPDATEEVENT;
    }
  }

}
