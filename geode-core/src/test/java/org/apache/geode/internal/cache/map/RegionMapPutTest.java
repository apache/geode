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
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntryEventSerialization;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionMapPutTest {

  private InternalRegion internalRegion;
  private FocusedRegionMap focusedRegionMap;
  private CacheModificationLock cacheModificationLock;
  private EntryEventSerialization entryEventSerialization;
  private RegionEntry regionEntry;

  @Before
  public void setup() {
    this.internalRegion = mock(InternalRegion.class);
    this.focusedRegionMap = mock(FocusedRegionMap.class);
    this.cacheModificationLock = mock(CacheModificationLock.class);
    this.entryEventSerialization = mock(EntryEventSerialization.class);
    this.regionEntry = mock(RegionEntry.class);

    RegionEntryFactory regionEntryFactory = mock(RegionEntryFactory.class);

    when(internalRegion.getScope()).thenReturn(Scope.LOCAL);
    when(internalRegion.isInitialized()).thenReturn(true);
    when(internalRegion.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(focusedRegionMap.getEntryMap()).thenReturn(mock(Map.class));
    when(focusedRegionMap.getEntryFactory()).thenReturn(regionEntryFactory);
    when(regionEntryFactory.createEntry(any(), any(), any())).thenReturn(regionEntry);
    when(regionEntry.getValueAsToken()).thenReturn(Token.REMOVED_PHASE1);
  }

  @Test
  public void createOnEmptyMapAddsEntry() {
    final EntryEventImpl event = createEventForCreate(internalRegion, "key");
    final boolean ifNew = true;
    final boolean ifOld = false;
    final boolean requireOldValue = false;
    final Object expectedOldValue = null;
    final boolean overwriteDestroyed = false;
    RegionMapPut regionMapPut = new RegionMapPut(focusedRegionMap, internalRegion,
        cacheModificationLock, entryEventSerialization, event, ifNew, ifOld, overwriteDestroyed,
        requireOldValue, expectedOldValue);

    RegionEntry result = regionMapPut.put();

    assertThat(result).isSameAs(regionEntry);
    verify(internalRegion, times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(internalRegion, times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }

  @Test
  public void updateOnEmptyMapReturnsNull() {
    final EntryEventImpl event = createEventForCreate(internalRegion, "key");
    final boolean ifNew = false;
    final boolean ifOld = true;
    final boolean requireOldValue = false;
    final Object expectedOldValue = null;
    final boolean overwriteDestroyed = false;
    RegionMapPut regionMapPut = new RegionMapPut(focusedRegionMap, internalRegion,
        cacheModificationLock, entryEventSerialization, event, ifNew, ifOld, overwriteDestroyed,
        requireOldValue, expectedOldValue);

    RegionEntry result = regionMapPut.put();

    assertThat(result).isNull();
    verify(internalRegion, never()).basicPutPart2(any(), any(), anyBoolean(), anyLong(),
        anyBoolean());
    verify(internalRegion, never()).basicPutPart3(any(), any(), anyBoolean(), anyLong(),
        anyBoolean(), anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void createOnExistingEntryReturnsNull() {
    final EntryEventImpl event = createEventForCreate(internalRegion, "key");
    final boolean ifNew = true;
    final boolean ifOld = false;
    final boolean requireOldValue = false;
    final Object expectedOldValue = null;
    final boolean overwriteDestroyed = false;
    RegionMapPut regionMapPut = new RegionMapPut(focusedRegionMap, internalRegion,
        cacheModificationLock, entryEventSerialization, event, ifNew, ifOld, overwriteDestroyed,
        requireOldValue, expectedOldValue);
    when(focusedRegionMap.getEntry(event)).thenReturn(mock(RegionEntry.class));

    RegionEntry result = regionMapPut.put();

    assertThat(result).isNull();
    verify(internalRegion, never()).basicPutPart2(any(), any(), anyBoolean(), anyLong(),
        anyBoolean());
    verify(internalRegion, never()).basicPutPart3(any(), any(), anyBoolean(), anyLong(),
        anyBoolean(), anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void createOnEntryReturnedFromPutIfAbsentDoesNothing() {
    final EntryEventImpl event = createEventForCreate(internalRegion, "key");
    final boolean ifNew = true;
    final boolean ifOld = false;
    final boolean requireOldValue = false;
    final Object expectedOldValue = null;
    final boolean overwriteDestroyed = false;
    RegionMapPut regionMapPut = new RegionMapPut(focusedRegionMap, internalRegion,
        cacheModificationLock, entryEventSerialization, event, ifNew, ifOld, overwriteDestroyed,
        requireOldValue, expectedOldValue);
    when(focusedRegionMap.putEntryIfAbsent(event.getKey(), regionEntry))
        .thenReturn(mock(RegionEntry.class));

    RegionEntry result = regionMapPut.put();

    assertThat(result).isNull();
    verify(internalRegion, never()).basicPutPart2(any(), any(), anyBoolean(), anyLong(),
        anyBoolean());
    verify(internalRegion, never()).basicPutPart3(any(), any(), anyBoolean(), anyLong(),
        anyBoolean(), anyBoolean(), anyBoolean(), any(), anyBoolean());
  }

  @Test
  public void createOnExistingEntryWithRemovePhase2DoesCreate() throws RegionClearedException {
    final EntryEventImpl event = createEventForCreate(internalRegion, "key");
    final boolean ifNew = true;
    final boolean ifOld = false;
    final boolean requireOldValue = false;
    final Object expectedOldValue = null;
    final boolean overwriteDestroyed = false;
    RegionMapPut regionMapPut = new RegionMapPut(focusedRegionMap, internalRegion,
        cacheModificationLock, entryEventSerialization, event, ifNew, ifOld, overwriteDestroyed,
        requireOldValue, expectedOldValue);
    RegionEntry existingRegionEntry = mock(RegionEntry.class);
    when(existingRegionEntry.isRemovedPhase2()).thenReturn(true);
    when(focusedRegionMap.putEntryIfAbsent(event.getKey(), regionEntry))
        .thenReturn(existingRegionEntry).thenReturn(null);

    RegionEntry result = regionMapPut.put();

    assertThat(result).isSameAs(regionEntry);
    verify(internalRegion, times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(internalRegion, times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }



  /*
   * @Test
   * public void verifyConcurrentCreateHasCorrectResult() throws Exception {
   * CountDownLatch firstCreateAddedUninitializedEntry = new CountDownLatch(1);
   * CountDownLatch secondCreateFoundFirstCreatesEntry = new CountDownLatch(1);
   * TestableBasicPutMap arm = new TestableBasicPutMap(firstCreateAddedUninitializedEntry,
   * secondCreateFoundFirstCreatesEntry);
   * // The key needs to be long enough to not be stored inline on the region entry.
   * String key1 = "lonGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGkey";
   * String key2 = new String(key1);
   *
   * Future<RegionEntry> future = doFirstCreateInAnotherThread(arm, key1);
   * if (!firstCreateAddedUninitializedEntry.await(5, TimeUnit.SECONDS)) {
   * // something is wrong with the other thread
   * // so count down the latch it may be waiting
   * // on and then call get to see what went wrong with him.
   * secondCreateFoundFirstCreatesEntry.countDown();
   * fail("other thread took too long. It returned " + future.get());
   * }
   * EntryEventImpl event = createEventForCreate(arm._getOwner(), key2);
   * // now do the second create
   * RegionEntry result = arm.basicPut(event, 0L, true, false, null, false, false);
   *
   * RegionEntry resultFromOtherThread = future.get();
   *
   * assertThat(result).isNull();
   * assertThat(resultFromOtherThread).isNotNull();
   * assertThat(resultFromOtherThread.getKey()).isSameAs(key1);
   * }
   */

  private EntryEventImpl createEventForCreate(InternalRegion lr, String key) {
    when(lr.getKeyInfo(key)).thenReturn(new KeyInfo(key, null, null));
    EntryEventImpl event =
        EntryEventImpl.create(lr, Operation.CREATE, key, false, null, true, false);
    event.setNewValue("create_value");
    return event;
  }

  /*
   *
   * private Future<RegionEntry> doFirstCreateInAnotherThread(TestableBasicPutMap arm, String key) {
   * Future<RegionEntry> result = CompletableFuture.supplyAsync(() -> {
   * EntryEventImpl event = createEventForCreate(arm._getOwner(), key);
   * return arm.basicPut(event, 0L, true, false, null, false, false);
   * });
   * return result;
   * }
   *
   *
   * private static class TestableBasicPutMap extends
   * AbstractRegionMapTest.TestableAbstractRegionMap {
   * private final CountDownLatch firstCreateAddedUninitializedEntry;
   * private final CountDownLatch secondCreateFoundFirstCreatesEntry;
   * private boolean alreadyCalledPutIfAbsentNewEntry;
   * private boolean alreadyCalledAddRegionEntryToMapAndDoPut;
   *
   * public TestableBasicPutMap(CountDownLatch removePhase1Completed,
   * CountDownLatch secondCreateFoundFirstCreatesEntry) {
   * super();
   * this.firstCreateAddedUninitializedEntry = removePhase1Completed;
   * this.secondCreateFoundFirstCreatesEntry = secondCreateFoundFirstCreatesEntry;
   * }
   *
   * @Override
   * protected boolean addRegionEntryToMapAndDoPut(final RegionMapPut putInfo) {
   * if (!alreadyCalledAddRegionEntryToMapAndDoPut) {
   * alreadyCalledAddRegionEntryToMapAndDoPut = true;
   * } else {
   * this.secondCreateFoundFirstCreatesEntry.countDown();
   * }
   * return super.addRegionEntryToMapAndDoPut(putInfo);
   * }
   *
   * @Override
   * protected void putIfAbsentNewEntry(final RegionMapPut putInfo) {
   * super.putIfAbsentNewEntry(putInfo);
   * if (!alreadyCalledPutIfAbsentNewEntry) {
   * alreadyCalledPutIfAbsentNewEntry = true;
   * this.firstCreateAddedUninitializedEntry.countDown();
   * try {
   * this.secondCreateFoundFirstCreatesEntry.await(5, TimeUnit.SECONDS);
   * } catch (InterruptedException ignore) {
   * }
   * }
   * }
   * }
   */
}
