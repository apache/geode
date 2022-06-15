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

import static org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.apache.geode.cache.api.DataPolicy;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.event.EventTracker;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.GatewaySenderConfigurationException;


public class DistributedRegionTest {
  private RegionVersionVector<VersionSource<Object>> vector;
  private RegionVersionHolder<VersionSource<Object>> holder;
  private VersionSource<Object> lostMemberVersionID;
  private InternalDistributedMember member;
  private EntryEventImpl event;
  private EventTracker eventTracker;
  private DistributedRegion distributedRegion;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setup() {
    vector = mock(RegionVersionVector.class);
    holder = mock(RegionVersionHolder.class);
    lostMemberVersionID = mock(VersionSource.class);
    member = mock(InternalDistributedMember.class);
    event = mock(EntryEventImpl.class);
    eventTracker = mock(EventTracker.class);
    distributedRegion = mock(DistributedRegion.class);
  }

  @Test
  public void shouldBeMockable() throws Exception {
    EntryEventImpl mockEntryEventImpl = mock(EntryEventImpl.class);
    Object returnValue = new Object();

    when(distributedRegion.validatedDestroy(any(), eq(mockEntryEventImpl)))
        .thenReturn(returnValue);

    assertThat(distributedRegion.validatedDestroy(new Object(), mockEntryEventImpl))
        .isSameAs(returnValue);
  }

  @Test
  public void cleanUpAfterFailedInitialImageHoldsLockForClear() {
    DistributedRegion distributedRegion = mock(DistributedRegion.class, RETURNS_DEEP_STUBS);
    RegionMap regionMap = mock(RegionMap.class);

    doCallRealMethod().when(distributedRegion).cleanUpAfterFailedGII(false);
    when(distributedRegion.getRegionMap()).thenReturn(regionMap);
    when(regionMap.isEmpty()).thenReturn(false);

    distributedRegion.cleanUpAfterFailedGII(false);

    verify(distributedRegion).lockFailedInitialImageWriteLock();
    verify(distributedRegion).closeEntries();
    verify(distributedRegion).unlockFailedInitialImageWriteLock();
  }

  @Test
  public void cleanUpAfterFailedInitialImageDoesNotCloseEntriesIfIsPersistentRegionAndRecoveredFromDisk() {
    DiskRegion diskRegion = mock(DiskRegion.class);

    doCallRealMethod().when(distributedRegion).cleanUpAfterFailedGII(true);
    when(distributedRegion.getDiskRegion()).thenReturn(diskRegion);
    when(diskRegion.isBackup()).thenReturn(true);

    distributedRegion.cleanUpAfterFailedGII(true);

    verify(diskRegion).resetRecoveredEntries(eq(distributedRegion));
    verify(distributedRegion, never()).closeEntries();
  }

  @Test
  public void lockHeldWhenRegionIsNotInitialized() {
    doCallRealMethod().when(distributedRegion).lockWhenRegionIsInitializing();
    when(distributedRegion.isInitialized()).thenReturn(false);

    assertThat(distributedRegion.lockWhenRegionIsInitializing()).isTrue();
    verify(distributedRegion).lockFailedInitialImageReadLock();
  }

  @Test
  public void lockNotHeldWhenRegionIsInitialized() {
    doCallRealMethod().when(distributedRegion).lockWhenRegionIsInitializing();
    when(distributedRegion.isInitialized()).thenReturn(true);

    assertThat(distributedRegion.lockWhenRegionIsInitializing()).isFalse();
    verify(distributedRegion, never()).lockFailedInitialImageReadLock();
  }

  @Test
  public void versionHolderInvokesSetRegionSynchronizeScheduledIfVectorContainsLostMemberID() {
    when(distributedRegion.getVersionVector()).thenReturn(vector);
    when(vector.getHolderForMember(lostMemberVersionID)).thenReturn(holder);
    doCallRealMethod().when(distributedRegion).setRegionSynchronizeScheduled(lostMemberVersionID);

    distributedRegion.setRegionSynchronizeScheduled(lostMemberVersionID);

    verify(holder).setRegionSynchronizeScheduled();
  }

  @Test
  public void versionHolderInvokesSetRegionSynchronizeScheduledOrDoneIfNotIfVectorContainsLostMemberID() {
    when(distributedRegion.getVersionVector()).thenReturn(vector);
    when(vector.getHolderForMember(lostMemberVersionID)).thenReturn(holder);
    doCallRealMethod().when(distributedRegion)
        .setRegionSynchronizedWithIfNotScheduled(lostMemberVersionID);
    when(holder.setRegionSynchronizeScheduledOrDoneIfNot()).thenReturn(true);

    assertThat(distributedRegion.setRegionSynchronizedWithIfNotScheduled(lostMemberVersionID))
        .isTrue();

    verify(holder).setRegionSynchronizeScheduledOrDoneIfNot();
  }

  @Test
  public void setRegionSynchronizedWithIfNotScheduledReturnsFalseIfVectorDoesNotContainLostMemberID() {
    when(distributedRegion.getVersionVector()).thenReturn(vector);
    when(vector.getHolderForMember(lostMemberVersionID)).thenReturn(holder);

    assertThat(distributedRegion.setRegionSynchronizedWithIfNotScheduled(lostMemberVersionID))
        .isFalse();

    verify(holder, never()).setRegionSynchronizeScheduledOrDoneIfNot();
  }

  @Test
  public void regionSyncInvokedInPerformSynchronizeForLostMemberTaskAfterRegionInitialized() {
    when(distributedRegion.getDataPolicyEnum()).thenReturn(DataPolicy.NORMAL);
    when(distributedRegion.isInitializedWithWait()).thenReturn(true);
    doCallRealMethod().when(distributedRegion).performSynchronizeForLostMemberTask(member,
        lostMemberVersionID);
    InOrder inOrder = inOrder(distributedRegion);

    distributedRegion.performSynchronizeForLostMemberTask(member, lostMemberVersionID);

    inOrder.verify(distributedRegion).isInitializedWithWait();
    inOrder.verify(distributedRegion).synchronizeForLostMember(member, lostMemberVersionID);
  }

  @Test
  public void regionSyncNotInvokedInPerformSynchronizeForLostMemberTaskIfRegionNotInitialized() {
    when(distributedRegion.getDataPolicyEnum()).thenReturn(DataPolicy.NORMAL);
    when(distributedRegion.isInitializedWithWait()).thenReturn(false);
    doCallRealMethod().when(distributedRegion).performSynchronizeForLostMemberTask(member,
        lostMemberVersionID);

    distributedRegion.performSynchronizeForLostMemberTask(member, lostMemberVersionID);

    verify(distributedRegion, never()).synchronizeForLostMember(member, lostMemberVersionID);
  }

  @Test
  public void validateAsynchronousEventDispatcherShouldDoNothingWhenDispatcherIdCanNotBeFound() {
    InternalCache internalCache = mock(InternalCache.class);
    when(internalCache.getAllGatewaySenders())
        .thenReturn(Collections.singleton(mock(GatewaySender.class)));
    when(distributedRegion.getCache()).thenReturn(internalCache);
    doCallRealMethod().when(distributedRegion).validateAsynchronousEventDispatcher(anyString());

    distributedRegion.validateAsynchronousEventDispatcher("nonExistingDispatcher");
  }

  @Test
  public void validateAsynchronousEventDispatcherShouldDoNothingWhenFoundDispatcherIsSerial() {
    String senderId = "mySender";
    GatewaySender serialSender = mock(GatewaySender.class);
    when(serialSender.isParallel()).thenReturn(false);
    when(serialSender.getId()).thenReturn(senderId);
    InternalCache internalCache = mock(InternalCache.class);
    when(internalCache.getAllGatewaySenders()).thenReturn(Collections.singleton(serialSender));
    when(distributedRegion.getCache()).thenReturn(internalCache);
    doCallRealMethod().when(distributedRegion).validateAsynchronousEventDispatcher(anyString());

    distributedRegion.validateAsynchronousEventDispatcher(senderId);
  }

  @Test
  public void validateAsynchronousEventDispatcherShouldThrowExceptionWhenDispatcherIdMatchesAnExistingParallelAsyncEventQueue() {
    String senderId = "senderId";
    String regionPath = "thisRegion";
    String internalSenderId = getSenderIdFromAsyncEventQueueId(senderId);
    GatewaySender parallelAsyncEventQueue = mock(GatewaySender.class);
    when(parallelAsyncEventQueue.isParallel()).thenReturn(true);
    when(parallelAsyncEventQueue.getId()).thenReturn(internalSenderId);
    InternalCache internalCache = mock(InternalCache.class);
    when(internalCache.getAllGatewaySenders())
        .thenReturn(Collections.singleton(parallelAsyncEventQueue));
    when(distributedRegion.getCache()).thenReturn(internalCache);
    when(distributedRegion.getFullPath()).thenReturn(regionPath);
    doCallRealMethod().when(distributedRegion).validateAsynchronousEventDispatcher(anyString());

    assertThatThrownBy(
        () -> distributedRegion.validateAsynchronousEventDispatcher(internalSenderId))
            .isInstanceOf(AsyncEventQueueConfigurationException.class)
            .hasMessage("Parallel Async Event Queue " + senderId
                + " can not be used with replicated region " + regionPath);
  }

  @Test
  public void validateAsynchronousEventDispatcherShouldThrowExceptionWhenDispatcherIdMatchesAnExistingParallelGatewaySender() {
    String senderId = "senderId";
    String regionPath = "thisRegion";
    GatewaySender parallelGatewaySender = mock(GatewaySender.class);
    when(parallelGatewaySender.isParallel()).thenReturn(true);
    when(parallelGatewaySender.getId()).thenReturn(senderId);
    InternalCache internalCache = mock(InternalCache.class);
    when(internalCache.getAllGatewaySenders())
        .thenReturn(Collections.singleton(parallelGatewaySender));
    when(distributedRegion.getCache()).thenReturn(internalCache);
    when(distributedRegion.getFullPath()).thenReturn(regionPath);
    doCallRealMethod().when(distributedRegion).validateAsynchronousEventDispatcher(anyString());

    assertThatThrownBy(() -> distributedRegion.validateAsynchronousEventDispatcher(senderId))
        .isInstanceOf(GatewaySenderConfigurationException.class)
        .hasMessage("Parallel Gateway Sender " + senderId
            + " can not be used with replicated region " + regionPath);
  }

  @Test
  public void hasSeenEventDoesNotFindAndSetVersionTagIfFoundInEventTrackerAndVersionTagIsSet() {
    doCallRealMethod().when(distributedRegion).hasSeenEvent(event);
    when(distributedRegion.getEventTracker()).thenReturn(eventTracker);
    when(eventTracker.hasSeenEvent(event)).thenReturn(true);
    when(event.getVersionTag()).thenReturn(mock(VersionTag.class));

    assertThat(distributedRegion.hasSeenEvent(event)).isTrue();
    verify(distributedRegion).markEventAsDuplicate(event);
    verify(distributedRegion, never()).findAndSetVersionTag(event);
  }

  @Test
  public void hasSeenEventDoesNotFindAndSetVersionTagIfFoundInEventTrackerAndConcurrencyChecksNotEnabled() {
    doCallRealMethod().when(distributedRegion).hasSeenEvent(event);
    when(distributedRegion.getEventTracker()).thenReturn(eventTracker);
    when(eventTracker.hasSeenEvent(event)).thenReturn(true);
    when(event.getRegion()).thenReturn(distributedRegion);
    when(distributedRegion.getConcurrencyChecksEnabled()).thenReturn(false);

    assertThat(distributedRegion.hasSeenEvent(event)).isTrue();
    verify(distributedRegion).markEventAsDuplicate(event);
    verify(distributedRegion, never()).findAndSetVersionTag(event);
  }

  @Test
  public void hasSeenEventDoesNotFindAndSetVersionTagIfFoundInEventTrackerAndEventIdIsNotSet() {
    doCallRealMethod().when(distributedRegion).hasSeenEvent(event);
    when(distributedRegion.getEventTracker()).thenReturn(eventTracker);
    when(eventTracker.hasSeenEvent(event)).thenReturn(true);
    when(event.getRegion()).thenReturn(distributedRegion);
    when(distributedRegion.getConcurrencyChecksEnabled()).thenReturn(false);
    when(event.getEventId()).thenReturn(null);

    assertThat(distributedRegion.hasSeenEvent(event)).isTrue();
    verify(distributedRegion).markEventAsDuplicate(event);
    verify(distributedRegion, never()).findAndSetVersionTag(event);
  }

  @Test
  public void hasSeenEventWillFindAndSetVersionTagIfFoundInEventTrackerButValidTagNotSet() {
    doCallRealMethod().when(distributedRegion).hasSeenEvent(event);
    when(distributedRegion.getEventTracker()).thenReturn(eventTracker);
    when(eventTracker.hasSeenEvent(event)).thenReturn(true);
    when(event.getRegion()).thenReturn(distributedRegion);
    when(distributedRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    when(event.getEventId()).thenReturn(mock(EventID.class));

    assertThat(distributedRegion.hasSeenEvent(event)).isTrue();
    verify(distributedRegion).markEventAsDuplicate(event);
    verify(distributedRegion).findAndSetVersionTag(event);
  }

  @Test
  public void hasSeenEventDoesNotFindAndSetVersionTagIfNotFoundEventInEventTrackerAndNotADuplicateEvent() {
    doCallRealMethod().when(distributedRegion).hasSeenEvent(event);
    when(distributedRegion.getEventTracker()).thenReturn(eventTracker);
    when(eventTracker.hasSeenEvent(event)).thenReturn(false);
    when(event.isPossibleDuplicate()).thenReturn(false);

    assertThat(distributedRegion.hasSeenEvent(event)).isFalse();
    verify(distributedRegion, never()).findAndSetVersionTag(event);
  }

  @Test
  public void hasSeenEventDoesNotFindAndSetVersionTagIfNotFoundInEventTrackerAndConcurrencyChecksNotEnabled() {
    doCallRealMethod().when(distributedRegion).hasSeenEvent(event);
    when(distributedRegion.getEventTracker()).thenReturn(eventTracker);
    when(eventTracker.hasSeenEvent(event)).thenReturn(false);
    when(event.isPossibleDuplicate()).thenReturn(true);
    when(event.getRegion()).thenReturn(distributedRegion);
    when(distributedRegion.getConcurrencyChecksEnabled()).thenReturn(false);

    assertThat(distributedRegion.hasSeenEvent(event)).isFalse();
    verify(distributedRegion, never()).findAndSetVersionTag(event);
  }

  @Test
  public void hasSeenEventDoesNotFindAndSetVersionTagIfNotFoundInEventTrackerAndVersionTagIsSet() {
    doCallRealMethod().when(distributedRegion).hasSeenEvent(event);
    when(distributedRegion.getEventTracker()).thenReturn(eventTracker);
    when(eventTracker.hasSeenEvent(event)).thenReturn(false);
    when(event.isPossibleDuplicate()).thenReturn(true);
    when(event.getRegion()).thenReturn(distributedRegion);
    when(distributedRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    when(event.getVersionTag()).thenReturn(mock(VersionTag.class));

    assertThat(distributedRegion.hasSeenEvent(event)).isFalse();
    verify(distributedRegion, never()).findAndSetVersionTag(event);
  }

  @Test
  public void hasSeenEventDoesNotFindAndSetVersionTagIfNotFoundInEventTrackerAndNoEventId() {
    doCallRealMethod().when(distributedRegion).hasSeenEvent(event);
    when(distributedRegion.getEventTracker()).thenReturn(eventTracker);
    when(eventTracker.hasSeenEvent(event)).thenReturn(false);
    when(event.isPossibleDuplicate()).thenReturn(true);
    when(event.getRegion()).thenReturn(distributedRegion);
    when(distributedRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    when(event.getEventId()).thenReturn(null);

    assertThat(distributedRegion.hasSeenEvent(event)).isFalse();
    verify(distributedRegion, never()).findAndSetVersionTag(event);
  }

  @Test
  public void hasSeenEventWillFindAndSetVersionTagIfNotFoundInEventTrackerAndIsPossibleDuplicateWithConcurrencyChecksEnabled() {
    doCallRealMethod().when(distributedRegion).hasSeenEvent(event);
    when(distributedRegion.getEventTracker()).thenReturn(eventTracker);
    when(eventTracker.hasSeenEvent(event)).thenReturn(false);
    when(event.isPossibleDuplicate()).thenReturn(true);
    when(event.getRegion()).thenReturn(distributedRegion);
    when(distributedRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    when(event.getEventId()).thenReturn(mock(EventID.class));

    assertThat(distributedRegion.hasSeenEvent(event)).isFalse();
    verify(distributedRegion).findAndSetVersionTag(event);
  }
}
