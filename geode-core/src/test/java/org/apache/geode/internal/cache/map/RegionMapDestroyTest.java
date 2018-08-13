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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
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
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;

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
  private boolean fromRILocalDestroy;

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
  }

  @After
  public void tearDown() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
  }

  private void givenConcurrencyChecks(boolean enabled) {
    withConcurrencyChecks = enabled;
    when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
  }

  private void givenMockedTombstone() {
    givenEvictionWithMockedEntryMap();
    when(entryMap.get(KEY)).thenReturn(evictableEntry);
    when(evictableEntry.isTombstone()).thenReturn(true);
    when(evictableEntry.isRemoved()).thenReturn(true);
    // We are not really testing eviction in this test.
    // But since the framework currently has a mocked evictableEntry
    // we use some evict stuff from it but tell the RegionMapDestroy
    // that it is not an eviction.
    isEviction = false;
  }

  private void givenVersionStampThatDetectsConflict() {
    VersionStamp versionStamp = mock(VersionStamp.class);
    when(evictableEntry.getVersionStamp()).thenReturn(versionStamp);
    doThrow(ConcurrentCacheModificationException.class).when(versionStamp)
        .processVersionTag(eq(event));
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

  private void givenExistingEntryWithValueAndVersion(Object value, VersionTag version) {
    RegionEntry entry = arm.getEntryFactory().createEntry(arm._getOwner(), KEY, value);
    ((VersionStamp) entry).setVersions(version);
    arm.getEntryMap().put(KEY, entry);
  }

  private void givenExistingEntryWithValueAndSameVersion(Object value, VersionTag version) {
    givenExistingEntryWithValueAndVersion(value, version);

    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    event.setVersionTag(version);
  }

  private void givenExistingEntry(Object value) {
    RegionEntry entry = arm.getEntryFactory().createEntry(arm._getOwner(), KEY, value);
    arm.getEntryMap().put(KEY, entry);
  }

  private void givenExistingEntryWithVersionTag() {
    givenExistingEntry();
    givenEventWithVersionTag();
  }

  private void givenExistingEntryWithTokenAndVersionTag(Token token) {
    givenExistingEntry(token);
    givenEventWithVersionTag();
  }

  private void givenRemoteEventWithVersionTag() {
    givenOriginIsRemote();
    givenEventWithVersionTag();
  }

  private void givenEventWithVersionTag() {
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

  private void givenEventFromServer() {
    event.setFromServer(true);
  }

  private void givenEventWithClientOrigin() {
    event.setContext(mock(ClientProxyMembershipID.class));
  }

  private boolean doDestroy() {
    return arm.destroy(event, inTokenMode, duringRI, cacheWrite, isEviction, expectedOldValue,
        removeRecoveredEntry);
  }

  @Test
  public void destroyWithDuplicateVersionInvokesListener() {
    givenEmptyRegionMap();
    givenConcurrencyChecks(true);
    VersionTag version = VersionTag.create(new InternalDistributedMember("localhost", 123));
    version.setEntryVersion(1);
    version.setRegionVersion(1);
    givenExistingEntryWithValueAndSameVersion(Token.TOMBSTONE, version);
    // make this a client/server operation
    event.setContext(new ClientProxyMembershipID());

    assertThat(doDestroy()).isTrue();

    assertThat(event.getIsRedestroyedEntry()).isTrue();
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionThrowsException() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeAddsAToken() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.DESTROYED);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeWithRegionClearedExceptionDoesDestroy()
      throws Exception {
    givenConcurrencyChecks(false);
    givenEmptyRegionMapWithMockedEntryMap();
    givenDestroyThrowsRegionClearedException();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verifyInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void evictDestroyWithEmptyRegionInTokenModeDoesNothing() {
    givenEviction();
    givenEmptyRegionMap();
    givenInTokenMode();

    assertThat(doDestroy()).isFalse();

    verifyMapDoesNotContainKey(event.getKey());
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithExistingTombstoneInTokenModeChangesToDestroyToken() {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntry(Token.TOMBSTONE);
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.DESTROYED);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void evictDestroyWithExistingTombstoneInTokenModeNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntry(Token.TOMBSTONE);
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

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

    assertThat(doDestroy()).isFalse();

    verifyNoDestroyInvocationsOnEvictableEntry();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void evictDestroyWithConcurrentChangeFromNullToInUseByTransactionInTokenModeDoesNothing()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry(null);
    givenEvictableEntryIsInUseByTransaction();
    givenInTokenMode();

    assertThat(doDestroy()).isFalse();

    verifyNoDestroyInvocationsOnEvictableEntry();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndDoesDestroy()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry("value");
    when(entryMap.get(KEY)).thenReturn(null).thenReturn(evictableEntry);
    isEviction = false;

    assertThat(doDestroy()).isTrue();

    verifyInvokedDestroyMethodOnEvictableEntry();
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndThrowsConcurrentCacheModificationException()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry("value");
    when(entryMap.get(KEY)).thenReturn(null).thenReturn(evictableEntry);
    doThrow(ConcurrentCacheModificationException.class).when(evictableEntry).destroy(
        eq(arm._getOwner()), eq(event), eq(false),
        anyBoolean(), eq(expectedOldValue), anyBoolean(), anyBoolean());
    isEviction = false;

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(ConcurrentCacheModificationException.class);
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesCallsDestroyWhichReturnsFalseCausingDestroyToNotHappen()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry("value");
    when(entryMap.get(KEY)).thenReturn(null).thenReturn(evictableEntry);
    when(evictableEntry.destroy(eq(arm._getOwner()), eq(event), eq(false), anyBoolean(),
        eq(expectedOldValue), anyBoolean(), anyBoolean())).thenReturn(false);
    isEviction = false;

    assertThat(doDestroy()).isTrue();

    verify(evictableEntry, times(1)).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithInTokenModeAndTombstoneCallsDestroyWhichReturnsFalseCausingDestroyToNotHappen()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry(Token.TOMBSTONE);
    when(entryMap.get(KEY)).thenReturn(null).thenReturn(evictableEntry);
    givenInTokenMode();
    when(evictableEntry.destroy(eq(arm._getOwner()), eq(event), anyBoolean(), anyBoolean(),
        eq(expectedOldValue), anyBoolean(), anyBoolean())).thenReturn(false);

    assertThat(doDestroy()).isTrue(); // TODO since destroy returns false it seems like doDestroy
                                      // should return true

    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithInTokenModeAndTombstoneCallsDestroyWhichThrowsRegionClearedStillDoesDestroy()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry(Token.TOMBSTONE);
    when(entryMap.get(KEY)).thenReturn(null).thenReturn(evictableEntry);
    givenInTokenMode();
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenThrow(RegionClearedException.class);

    assertThat(doDestroy()).isTrue();

    verifyInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void destroyWithInTokenModeCallsDestroyWhichReturnsFalseCausingDestroyToNotHappen()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry("value");
    when(entryMap.get(KEY)).thenReturn(evictableEntry);
    when(evictableEntry.destroy(eq(arm._getOwner()), eq(event), anyBoolean(), anyBoolean(),
        eq(expectedOldValue), anyBoolean(), anyBoolean())).thenReturn(false);
    inTokenMode = true;

    assertThat(doDestroy()).isFalse();

    verify(evictableEntry, never()).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyExistingEntryWithVersionStampCallsDestroyWhichReturnsFalseCausingDestroyToNotHappenAndDoesNotCallRemovePhase2()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry("value");
    when(entryMap.get(KEY)).thenReturn(evictableEntry);
    when(evictableEntry.destroy(eq(arm._getOwner()), eq(event), eq(false), anyBoolean(),
        eq(expectedOldValue), anyBoolean(), anyBoolean())).thenReturn(false);
    VersionStamp versionStamp = mock(VersionStamp.class);
    when(evictableEntry.getVersionStamp()).thenReturn(versionStamp);
    event.setOriginRemote(true);

    assertThat(doDestroy()).isTrue();

    verify(evictableEntry, never()).removePhase2();
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithConcurrentChangeFromNullToValidRetriesAndCallsUpdateSizeOnRemove()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    givenExistingEvictableEntry("value");
    when(entryMap.get(KEY)).thenReturn(null).thenReturn(evictableEntry);
    isEviction = false;

    assertThat(doDestroy()).isTrue();

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

    assertThat(doDestroy()).isTrue();

    verify(entryMap).remove(eq(KEY), eq(evictableEntry));
    verify(entryMap, times(2)).putIfAbsent(eq(KEY), any());
    verify(evictableEntry, never()).destroy(eq(arm._getOwner()), eq(event), eq(false), anyBoolean(),
        eq(expectedOldValue), anyBoolean(), anyBoolean());

    verifyInvokedDestroyMethodsOnRegion(false);
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

    assertThat(doDestroy()).isTrue();

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingEntryInTokenModeAddsAToken() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.DESTROYED);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryInTokenModeInhibitsCacheListenerNotification() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    assertThat(event.inhibitCacheListenerNotification()).isTrue();
  }

  @Test
  public void destroyOfExistingEntryInTokenModeDuringRegisterInterestDoesNotInhibitCacheListenerNotification() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();
    givenInTokenMode();
    duringRI = true;

    assertThat(doDestroy()).isTrue();

    assertThat(event.inhibitCacheListenerNotification()).isFalse();
  }

  @Test
  public void destroyOfExistingEntryDoesNotInhibitCacheListenerNotification() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();

    assertThat(doDestroy()).isTrue();

    assertThat(event.inhibitCacheListenerNotification()).isFalse();
  }

  @Test
  public void destroyOfExistingEntryInTokenModeCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verify(arm._getOwner(), times(1)).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingTombstoneInTokenModeWithConcurrencyChecksDoesNothing() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    // why not DESTROY token? since it was already destroyed why do we do the parts?
    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingTombstoneInTokenModeWithConcurrencyChecksNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);
    givenInTokenMode();

    assertThat(doDestroy()).isTrue();

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksThrowsEntryNotFound() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyOfExistingTombstoneThatThrowsConcurrentCacheModificationExceptionNeverCallsNotify() {
    givenConcurrencyChecks(true);
    givenMockedTombstone();
    givenVersionStampThatDetectsConflict();
    givenEventWithVersionTag();

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(ConcurrentCacheModificationException.class);
    verify(arm._getOwner(), never()).notifyTimestampsToGateways(any());
  }

  @Test
  public void destroyOfExistingTombstoneThatThrowsConcurrentCacheModificationExceptionWithTimeStampUpdatedCallsNotify() {
    givenConcurrencyChecks(true);
    givenMockedTombstone();
    givenVersionStampThatDetectsConflict();
    givenEventWithVersionTag();
    when(event.getVersionTag().isTimeStampUpdated()).thenReturn(true);

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(ConcurrentCacheModificationException.class);
    verify(arm._getOwner(), times(1)).notifyTimestampsToGateways(eq(event));
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndNoTagThrowsEntryNotFound() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntry(Token.TOMBSTONE);

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void evictDestroyOfExistingTombstoneWithConcurrencyChecksReturnsFalse() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);
    this.isEviction = true;

    assertThat(doDestroy()).isFalse();
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndRemoveRecoveredEntryDoesRemove() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);
    givenRemoveRecoveredEntry();

    assertThat(doDestroy()).isTrue();

    verifyMapDoesNotContainKey(event.getKey());
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndFromRILocalDestroyDoesRemove() {
    givenConcurrencyChecks(true);
    fromRILocalDestroy = true;
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);

    assertThat(doDestroy()).isTrue();

    verifyMapDoesNotContainKey(event.getKey());
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingTombstoneWithConcurrencyChecksAndRemoveRecoveredEntryNeverCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.TOMBSTONE);
    givenRemoveRecoveredEntry();

    assertThat(doDestroy()).isTrue();

    verify(arm._getOwner(), never()).updateSizeOnRemove(any(), anyInt());
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndRemoveRecoveredEntryDoesRetryAndThrowsEntryNotFound() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    givenRemoveRecoveredEntry();

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksDoesRetryAndThrowsEntryNotFound() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndExpectedValueDoesRetryAndReturnsFalse() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    this.expectedOldValue = "OLD_VALUE";

    assertThat(doDestroy()).isFalse();
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndInTokenModeDoesRetryAndReturnsFalse() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    this.inTokenMode = true;

    assertThat(doDestroy()).isFalse();
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndEvictionDoesRetryAndReturnsFalse() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    this.isEviction = true;

    assertThat(doDestroy()).isFalse();
  }

  @Test
  public void destroyOfExistingRemovePhase2WithoutConcurrencyChecksDoesRetryAndThrowsEntryNotFound() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndOriginRemoteDoesRetryAndDoesRemove() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    givenOriginIsRemote();

    assertThat(doDestroy()).isTrue();
  }

  @Test
  public void destroyOfExistingRemovePhase2WithConcurrencyChecksAndClientOriginDoesRetryAndDoesRemove() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithTokenAndVersionTag(Token.REMOVED_PHASE2);
    givenEventWithClientOrigin();

    assertThat(doDestroy()).isTrue();
  }

  @Test
  public void destroyOfExistingEntryRemovesEntryFromMapAndDoesNotifications() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();

    assertThat(doDestroy()).isTrue();

    verifyMapDoesNotContainKey(event.getKey());
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryWithConflictDoesPart3() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();
    event.isConcurrencyConflict(true);

    assertThat(doDestroy()).isTrue();

    verifyPart3();
  }

  @Test
  public void destroyOfExistingEntryWithConflictAndWANSkipsPart3() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();
    event.isConcurrencyConflict(true);
    givenEventWithVersionTag();
    when(event.getVersionTag().isGatewayTag()).thenReturn(true);

    assertThat(doDestroy()).isTrue();

    verifyNoPart3();
  }

  @Test
  public void destroyOfExistingEntryWithRegionClearedExceptionDoesDestroyAndPart2AndPart3()
      throws RegionClearedException {
    givenConcurrencyChecks(false);
    givenEvictionWithMockedEntryMap();
    this.isEviction = false;
    givenExistingEvictableEntry("value");
    when(entryMap.get(KEY)).thenReturn(null).thenReturn(evictableEntry);
    givenEventWithVersionTag();
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenThrow(RegionClearedException.class);

    assertThat(doDestroy()).isTrue();

    verifyInvokedDestroyMethodsOnRegion(true);
  }

  @Test
  public void expireDestroyOfExistingEntry() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntry();
    event.setOperation(Operation.EXPIRE_DESTROY);

    assertThat(doDestroy()).isTrue();
  }

  @Test
  public void expireDestroyOfExistingEntryWithOriginRemote() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntry();
    givenOriginIsRemote();
    event.setOperation(Operation.EXPIRE_DESTROY);

    assertThat(doDestroy()).isTrue();
  }

  @Test
  public void expireDestroyOfEntryInUseIsCancelled()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEvictionWithMockedEntryMap();
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(true);
    when(evictableEntry.isInUseByTransaction()).thenReturn(true);
    when(entryMap.get(KEY)).thenReturn(evictableEntry);
    event.setOperation(Operation.EXPIRE_DESTROY);

    assertThat(doDestroy()).isFalse();

    verify(evictableEntry, never()).destroy(any(), any(), anyBoolean(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verifyNoDestroyInvocationsOnRegion();
  }


  @Test
  public void destroyOfExistingEntryCallsUpdateSizeOnRemove() {
    givenConcurrencyChecks(false);
    givenEmptyRegionMap();
    givenExistingEntry();

    assertThat(doDestroy()).isTrue();

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

    assertThat(doDestroy()).isTrue();

    verifyMapDoesNotContainKey(event.getKey());
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyOfExistingEntryWithConcurrencyChecksAddsTombstone() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenExistingEntryWithVersionTag();

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void evictDestroyOfExistingEntryWithConcurrencyChecksAddsTombstone() {
    givenConcurrencyChecks(true);
    givenEviction();
    givenExistingEntryWithVersionTag();

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksThrowsException() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventThrowsException() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    when(this.owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenOriginIsRemote();

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventAndCacheWriteThrowsException() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    when(this.owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenOriginIsRemote();
    this.cacheWrite = true;
    this.removeRecoveredEntry = false;

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndRemoteEventAndCacheWriteAndBridgeWriteBeforeDestroyThrows_ThrowsException() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    when(this.owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenOriginIsRemote();
    this.cacheWrite = true;
    this.removeRecoveredEntry = false;
    doThrow(EntryNotFoundException.class).when(arm._getOwner()).bridgeWriteBeforeDestroy(any(),
        any());

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void localDestroyWithEmptyNonReplicateRegionWithConcurrencyChecksThrowsException() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    when(this.owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenEventWithClientOrigin();
    givenEventWithVersionTag();
    event.setOperation(Operation.LOCAL_DESTROY);

    assertThatThrownBy(() -> doDestroy()).isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndClientTaggedEventAndCacheWriteDoesNotThrowException() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    when(this.owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    this.cacheWrite = true;
    this.removeRecoveredEntry = false;
    givenEventWithClientOrigin();
    givenEventWithVersionTag();

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndWANTaggedEventAndCacheWriteDoesNotThrowException() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    when(this.owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    this.cacheWrite = true;
    this.removeRecoveredEntry = false;
    givenEventWithVersionTag();
    when(event.getVersionTag().isGatewayTag()).thenReturn(true);

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyInvokedDestroyMethodsOnRegion(false);
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
    when(this.owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);

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

    verifyMapDoesNotContainKey(event.getKey());
    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAddsATombstone() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenRemoteEventWithVersionTag();

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndClientOriginEventAddsATombstone() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenEventWithClientOrigin();
    givenEventWithVersionTag();

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndWANEventAddsATombstone() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenEventWithVersionTag();
    when(event.getVersionTag().isGatewayTag()).thenReturn(true);

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void validateNoDestroyWhenExistingTombstoneAndNewEntryDestroyFails()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEmptyRegionMapWithMockedEntryMap();
    RegionEntry existingTombstone = mock(RegionEntry.class);
    when(existingTombstone.isTombstone()).thenReturn(true);
    when(existingTombstone.getValue()).thenReturn(Token.TOMBSTONE);
    when(entryMap.get(KEY)).thenReturn(existingTombstone);
    when(entryMap.putIfAbsent(eq(KEY), any())).thenReturn(null);
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(true);
    when(factory.createEntry(any(), any(), any())).thenReturn(evictableEntry);
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(true);
    givenEventWithVersionTag();
    when(event.getVersionTag().isGatewayTag()).thenReturn(true);
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(false);
    inTokenMode = true;

    assertThat(doDestroy()).isFalse();

    verifyNoDestroyInvocationsOnRegion();
    verify(entryMap, never()).remove(KEY, evictableEntry); // TODO: this seems like a bug. This
                                                           // should be called once.
  }

  @Test
  public void validateNoDestroyInvocationsOnRegionDoesNotDoDestroyIfEntryDestroyReturnsFalse()
      throws RegionClearedException {
    givenConcurrencyChecks(true);
    givenEmptyRegionMapWithMockedEntryMap();
    when(factory.createEntry(any(), any(), any())).thenReturn(evictableEntry);
    givenEventWithVersionTag();
    when(event.getVersionTag().isGatewayTag()).thenReturn(true);
    when(evictableEntry.destroy(any(), any(), anyBoolean(), anyBoolean(), any(), anyBoolean(),
        anyBoolean())).thenReturn(false);

    assertThat(doDestroy()).isFalse();

    verifyNoDestroyInvocationsOnRegion();
  }

  @Test
  public void destroyWithEmptyNonReplicateRegionWithConcurrencyChecksAndEventFromServerAddsATombstone() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenRemoteEventWithVersionTag();
    when(this.owner.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    givenEventFromServer();

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndWANEventWithConflictAddsATombstoneButDoesNotDoPart3() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenEventWithVersionTag();
    when(event.getVersionTag().isGatewayTag()).thenReturn(true);
    event.isConcurrencyConflict(true);

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyPart2(false);
    verifyNoPart3();
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndEventWithConflictAddsATombstone() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenRemoteEventWithVersionTag();
    event.isConcurrencyConflict(true);

    assertThat(doDestroy()).isTrue();

    verifyMapContainsTokenValue(Token.TOMBSTONE);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksCallsIndexManager() {
    givenConcurrencyChecks(true);
    givenEmptyRegionMap();
    givenRemoteEventWithVersionTag();
    IndexManager indexManager = mock(IndexManager.class);
    when(this.owner.getIndexManager()).thenReturn(indexManager);

    assertThat(doDestroy()).isTrue();

    InOrder inOrder = inOrder(indexManager, arm._getOwner());
    inOrder.verify(indexManager, times(1)).waitForIndexInit();
    inOrder.verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), any(), anyBoolean(),
        anyBoolean(),
        anyBoolean(), anyBoolean());
    inOrder.verify(indexManager, times(1)).countDownIndexUpdaters();
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

    assertThat(doDestroy()).isTrue();

    verifyMapContainsKey(event.getKey());
    verifyMapContainsTokenValue(Token.REMOVED_PHASE1);
    verifyInvokedDestroyMethodsOnRegion(false);
  }

  private void verifyInvokedDestroyMethodOnEvictableEntry() throws RegionClearedException {
    verify(evictableEntry, times(1)).destroy(eq(arm._getOwner()), eq(event), eq(false),
        anyBoolean(), eq(expectedOldValue), anyBoolean(), anyBoolean());
  }

  private void verifyMapContainsKey(Object key) {
    assertThat(arm.getEntryMap()).containsKey(key);
  }

  private void verifyMapDoesNotContainKey(Object key) {
    assertThat(arm.getEntryMap()).doesNotContainKey(key);
  }

  private void verifyNoDestroyInvocationsOnEvictableEntry() throws RegionClearedException {
    verify(evictableEntry, never()).destroy(any(), any(), anyBoolean(), anyBoolean(), any(),
        anyBoolean(), anyBoolean());
  }

  private void verifyMapContainsTokenValue(Token token) {
    assertThat(arm.getEntryMap()).containsKey(event.getKey());
    RegionEntry re = (RegionEntry) arm.getEntryMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(token);
  }

  private void verifyInvokedDestroyMethodsOnRegion(boolean conflictWithClear) {
    verifyPart2(conflictWithClear);
    verifyPart3();
  }

  private void verifyPart3() {
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(true), eq(expectedOldValue));
  }

  private void verifyPart2(boolean conflictWithClear) {
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(conflictWithClear), eq(duringRI), eq(true));
  }

  private void verifyNoDestroyInvocationsOnRegion() {
    verifyNoPart2();
    verifyNoPart3();
  }

  private void verifyNoPart2() {
    verify(arm._getOwner(), never()).basicDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
  }

  private void verifyNoPart3() {
    verify(arm._getOwner(), never()).basicDestroyPart3(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), any());
  }

  private EntryEventImpl createEventForDestroy(LocalRegion lr) {
    when(lr.getKeyInfo(KEY)).thenReturn(new KeyInfo(KEY, null, null));
    return EntryEventImpl.create(lr, Operation.DESTROY, KEY, false, null, true, fromRILocalDestroy);
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
