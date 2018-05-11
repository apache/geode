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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AbstractRegionMapPutTest {
  private final InternalRegion internalRegion = mock(InternalRegion.class);
  private final FocusedRegionMap focusedRegionMap = mock(FocusedRegionMap.class);
  @SuppressWarnings("rawtypes")
  private final Map entryMap = mock(Map.class);
  private final EntryEventImpl event = mock(EntryEventImpl.class);
  private final RegionEntry createdRegionEntry = mock(RegionEntry.class);
  private final TestableRegionMapPut instance = spy(new TestableRegionMapPut());

  @Before
  public void setup() {
    RegionEntryFactory regionEntryFactory = mock(RegionEntryFactory.class);
    when(regionEntryFactory.createEntry(any(), any(), any())).thenReturn(createdRegionEntry);
    when(focusedRegionMap.getEntryFactory()).thenReturn(regionEntryFactory);
    when(focusedRegionMap.getEntryMap()).thenReturn(entryMap);
    when(internalRegion.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
  }

  @Test
  public void validateOwnerInitialized() {
    when(internalRegion.isInitialized()).thenReturn(true);

    TestableRegionMapPut testableRegionMapPut = new TestableRegionMapPut();

    assertThat(testableRegionMapPut.isOwnerInitialized()).isTrue();
  }

  @Test
  public void validateOwnerUninitialized() {
    when(internalRegion.isInitialized()).thenReturn(false);

    TestableRegionMapPut testableRegionMapPut = new TestableRegionMapPut();

    assertThat(testableRegionMapPut.isOwnerInitialized()).isFalse();
  }

  @Test
  public void validateSetLastModifiedTime() {
    instance.setLastModifiedTime(99L);

    assertThat(instance.getLastModifiedTime()).isEqualTo(99L);
  }

  @Test
  public void validateSetClearOccurred() {
    instance.setClearOccurred(true);

    assertThat(instance.isClearOccurred()).isTrue();
  }

  @Test
  public void putWithUnsatisfiedPreconditionsReturnsNull() {
    instance.checkPreconditions = false;

    RegionEntry result = instance.put();

    assertThat(result).isNull();
    assertThat(instance.getRegionEntry()).isSameAs(createdRegionEntry);
    verify(focusedRegionMap, times(1)).getEntry(eq(event));
    verify(focusedRegionMap, times(1)).putEntryIfAbsent(any(), eq(createdRegionEntry));
    verify(instance, times(1)).isOnlyExisting();
    verify(instance, never()).entryExists(any());
    verify(instance, times(1)).serializeNewValueIfNeeded();
    verify(instance, times(1)).runWhileLockedForCacheModification(any());
    verify(instance, times(1)).setOldValueForDelta();
    verify(instance, times(1)).setOldValueInEvent();
    verify(instance, times(1)).unsetOldValueForDelta();
    verify(instance, times(1)).checkPreconditions();
    verify(instance, never()).invokeCacheWriter();
    verify(instance, never()).createOrUpdateEntry();
    verify(instance, times(1)).shouldCreatedEntryBeRemoved();
    verify(instance, never()).doBeforeCompletionActions();
    verify(instance, times(1)).doAfterCompletionActions();
  }

  @Test
  public void removeEntryCalledIfShouldCreatedEntryBeRemoved() {
    instance.shouldCreatedEntryBeRemoved = true;

    instance.put();

    verify(focusedRegionMap, times(1)).removeEntry(any(), eq(createdRegionEntry), eq(false));
  }

  @Test
  public void oqlIndexManagerInitializedAndCountedDown() {
    IndexManager oqlIndexManager = mock(IndexManager.class);
    when(internalRegion.getIndexManager()).thenReturn(oqlIndexManager);

    instance.put();

    verify(oqlIndexManager, times(1)).waitForIndexInit();
    verify(oqlIndexManager, times(1)).countDownIndexUpdaters();
  }

  @Test
  public void putCallsHandleDiskAccessExceptionWhenThrownDuringPut() {
    instance.checkPreconditions = true;
    doThrow(DiskAccessException.class).when(instance).createOrUpdateEntry();

    assertThatThrownBy(() -> instance.put()).isInstanceOf(DiskAccessException.class);

    verify(internalRegion, times(1)).handleDiskAccessException(any());
  }

  @Test
  public void putWithSatisfiedPreconditionsAndNoExistingEntryReturnsRegionEntryFromFactory() {
    when(focusedRegionMap.getEntry(event)).thenReturn(null);
    instance.checkPreconditions = true;

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(createdRegionEntry);
    assertThat(instance.getRegionEntry()).isSameAs(createdRegionEntry);
    verify(focusedRegionMap, times(1)).getEntry(eq(event));
    verify(focusedRegionMap, times(1)).putEntryIfAbsent(any(), eq(createdRegionEntry));
    verify(instance, times(1)).isOnlyExisting();
    verify(instance, never()).entryExists(any());
    verify(instance, times(1)).serializeNewValueIfNeeded();
    verify(instance, times(1)).runWhileLockedForCacheModification(any());
    verify(instance, times(1)).setOldValueForDelta();
    verify(instance, times(1)).setOldValueInEvent();
    verify(instance, times(1)).unsetOldValueForDelta();
    verify(instance, times(1)).checkPreconditions();
    verify(instance, times(1)).invokeCacheWriter();
    verify(instance, times(1)).createOrUpdateEntry();
    verify(instance, times(1)).shouldCreatedEntryBeRemoved();
    verify(instance, times(1)).doBeforeCompletionActions();
    verify(instance, times(1)).doAfterCompletionActions();
  }

  @Test
  public void regionWithIndexMaintenanceSynchronousCallsSetUpdateInProgress() {
    when(internalRegion.getIndexMaintenanceSynchronous()).thenReturn(true);
    instance.checkPreconditions = true;

    instance.put();

    verify(createdRegionEntry, times(1)).setUpdateInProgress(true);
    verify(createdRegionEntry, times(1)).setUpdateInProgress(false);
  }

  @Test
  public void putWithOnlyExistingTrueReturnsNull() {
    instance.onlyExisting = true;

    RegionEntry result = instance.put();

    assertThat(result).isNull();
    assertThat(instance.getRegionEntry()).isNull();
    verify(focusedRegionMap, times(1)).getEntry(eq(event));
    verify(focusedRegionMap, never()).putEntryIfAbsent(any(), any());
    verify(instance, times(1)).isOnlyExisting();
    verify(instance, times(1)).entryExists(any());
    verify(instance, times(1)).serializeNewValueIfNeeded();
    verify(instance, times(1)).runWhileLockedForCacheModification(any());
    verify(instance, never()).setOldValueForDelta();
    verify(instance, never()).setOldValueInEvent();
    verify(instance, never()).unsetOldValueForDelta();
    verify(instance, never()).checkPreconditions();
    verify(instance, never()).invokeCacheWriter();
    verify(instance, never()).createOrUpdateEntry();
    verify(instance, never()).shouldCreatedEntryBeRemoved();
    verify(instance, never()).doBeforeCompletionActions();
    verify(instance, times(1)).doAfterCompletionActions();
  }

  @Test
  public void putWithExistingEntrySucceeds() {
    instance.checkPreconditions = true;
    instance.onlyExisting = true;
    instance.entryExists = true;
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(focusedRegionMap.getEntry(eq(event))).thenReturn(existingEntry);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(existingEntry);
    assertThat(instance.getRegionEntry()).isSameAs(existingEntry);
    verify(focusedRegionMap, times(1)).getEntry(eq(event));
    verify(focusedRegionMap, never()).getEntryFactory();
    verify(focusedRegionMap, never()).putEntryIfAbsent(any(), eq(createdRegionEntry));
    verify(instance, times(1)).isOnlyExisting();
    verify(instance, times(1)).entryExists(eq(existingEntry));
    verify(instance, times(1)).serializeNewValueIfNeeded();
    verify(instance, times(1)).runWhileLockedForCacheModification(any());
    verify(instance, times(1)).setOldValueForDelta();
    verify(instance, times(1)).setOldValueInEvent();
    verify(instance, times(1)).unsetOldValueForDelta();
    verify(instance, times(1)).checkPreconditions();
    verify(instance, times(1)).invokeCacheWriter();
    verify(instance, times(1)).createOrUpdateEntry();
    verify(instance, never()).shouldCreatedEntryBeRemoved();
    verify(instance, times(1)).doBeforeCompletionActions();
    verify(instance, times(1)).doAfterCompletionActions();
  }

  @Test
  public void putWithExistingEntryFromPutIfAbsentSucceeds() {
    instance.checkPreconditions = true;
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(focusedRegionMap.putEntryIfAbsent(any(), eq(createdRegionEntry)))
        .thenReturn(existingEntry);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(existingEntry);
    assertThat(instance.getRegionEntry()).isSameAs(existingEntry);
    verify(focusedRegionMap, times(1)).getEntry(eq(event));
    verify(focusedRegionMap, times(1)).getEntryFactory();
    verify(instance, times(1)).isOnlyExisting();
    verify(instance, never()).entryExists(any());
    verify(instance, times(1)).serializeNewValueIfNeeded();
    verify(instance, times(1)).runWhileLockedForCacheModification(any());
    verify(instance, times(1)).setOldValueForDelta();
    verify(instance, times(1)).setOldValueInEvent();
    verify(instance, times(1)).unsetOldValueForDelta();
    verify(instance, times(1)).checkPreconditions();
    verify(instance, times(1)).invokeCacheWriter();
    verify(instance, times(1)).createOrUpdateEntry();
    verify(instance, never()).shouldCreatedEntryBeRemoved();
    verify(instance, times(1)).doBeforeCompletionActions();
    verify(instance, times(1)).doAfterCompletionActions();
  }


  @Test
  public void putWithExistingEntryFromPutIfAbsentThatIsRemovedSucceeds() {
    instance.checkPreconditions = true;
    RegionEntry existingEntry = mock(RegionEntry.class);
    when(existingEntry.isRemovedPhase2()).thenReturn(true).thenReturn(false);
    when(focusedRegionMap.putEntryIfAbsent(any(), eq(createdRegionEntry)))
        .thenReturn(existingEntry);

    RegionEntry result = instance.put();

    assertThat(result).isSameAs(existingEntry);
    assertThat(instance.getRegionEntry()).isSameAs(existingEntry);
    verify(focusedRegionMap, times(2)).getEntry(eq(event));
    verify(focusedRegionMap, times(2)).getEntryFactory();
    verify(entryMap, times(1)).remove(any(), eq(existingEntry));
    verify(instance, times(2)).isOnlyExisting();
    verify(instance, never()).entryExists(any());
    verify(instance, times(1)).serializeNewValueIfNeeded();
    verify(instance, times(1)).runWhileLockedForCacheModification(any());
    verify(instance, times(1)).setOldValueForDelta();
    verify(instance, times(1)).setOldValueInEvent();
    verify(instance, times(1)).unsetOldValueForDelta();
    verify(instance, times(1)).checkPreconditions();
    verify(instance, times(1)).invokeCacheWriter();
    verify(instance, times(1)).createOrUpdateEntry();
    verify(instance, never()).shouldCreatedEntryBeRemoved();
    verify(instance, times(1)).doBeforeCompletionActions();
    verify(instance, times(1)).doAfterCompletionActions();
  }

  private class TestableRegionMapPut extends AbstractRegionMapPut {
    public boolean checkPreconditions;
    public boolean onlyExisting;
    public boolean entryExists;
    public boolean shouldCreatedEntryBeRemoved;

    public TestableRegionMapPut() {
      super(focusedRegionMap, internalRegion, event);
    }

    @Override
    protected boolean isOnlyExisting() {
      return onlyExisting;
    }

    @Override
    protected boolean entryExists(RegionEntry regionEntry) {
      return entryExists;
    }

    @Override
    protected void serializeNewValueIfNeeded() {}

    @Override
    protected void runWhileLockedForCacheModification(Runnable r) {
      r.run();
    }

    @Override
    protected void setOldValueForDelta() {}

    @Override
    protected void setOldValueInEvent() {}

    @Override
    protected void unsetOldValueForDelta() {}

    @Override
    protected boolean checkPreconditions() {
      return checkPreconditions;
    }

    @Override
    protected void invokeCacheWriter() {}

    @Override
    protected void createOrUpdateEntry() {}

    @Override
    protected void doBeforeCompletionActions() {}

    @Override
    protected boolean shouldCreatedEntryBeRemoved() {
      return shouldCreatedEntryBeRemoved;
    }

    @Override
    protected void doAfterCompletionActions() {}

  }
}
