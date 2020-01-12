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
package org.apache.geode.internal.cache.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionTag;


public class DistributedEventTrackerTest {

  private LocalRegion region;
  private DistributedEventTracker eventTracker;
  private ClientProxyMembershipID memberId;
  private DistributedMember member;

  @Before
  public void setup() {
    region = mock(LocalRegion.class);
    RegionAttributes<?, ?> regionAttributes = mock(RegionAttributes.class);
    memberId = mock(ClientProxyMembershipID.class);
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getDataPolicy()).thenReturn(mock(DataPolicy.class));
    when(region.getConcurrencyChecksEnabled()).thenReturn(true);

    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getOffHeapStore()).thenReturn(null);

    member = mock(DistributedMember.class);
    eventTracker = new DistributedEventTracker(region.getCache(), mock(CancelCriterion.class),
        region.getName());
  }

  @Test
  public void retriedBulkOpDoesNotRemoveRecordedBulkOpVersionTags() {
    byte[] memId = {1, 2, 3};
    long threadId = 1;
    long retrySeqId = 1;
    ThreadIdentifier tid = new ThreadIdentifier(memId, threadId);
    EventID retryEventID = new EventID(memId, threadId, retrySeqId);
    boolean skipCallbacks = true;
    int size = 5;
    recordPutAllEvents(memId, threadId, skipCallbacks, size);

    ConcurrentMap<ThreadIdentifier, BulkOperationHolder> map =
        eventTracker.getRecordedBulkOpVersionTags();
    BulkOperationHolder holder = map.get(tid);
    int beforeSize = holder.getEntryVersionTags().size();

    eventTracker.recordBulkOpStart(retryEventID, tid);
    map = eventTracker.getRecordedBulkOpVersionTags();
    holder = map.get(tid);
    // Retried bulk op should not remove exiting BulkOpVersionTags
    assertTrue(holder.getEntryVersionTags().size() == beforeSize);
  }

  private void recordPutAllEvents(byte[] memId, long threadId, boolean skipCallbacks, int size) {
    for (int i = 0; i < size; i++) {
      putEvent("key" + i, "value" + i, memId, threadId, skipCallbacks, i + 1);
      EntryEventImpl event = EntryEventImpl.create(region, Operation.PUTALL_CREATE, "key" + i,
          "value" + i, null, false, member, !skipCallbacks, new EventID(memId, threadId, i + 1));
      event.setContext(memberId);
      event.setVersionTag(mock(VersionTag.class));
      eventTracker.recordEvent(event);
    }
  }

  private void putEvent(String key, String value, byte[] memId, long threadId,
      boolean skipCallbacks, int sequenceId) {
    EntryEventImpl event = EntryEventImpl.create(region, Operation.PUTALL_CREATE, key, value, null,
        false, member, !skipCallbacks, new EventID(memId, threadId, sequenceId));
    event.setContext(memberId);
    event.setVersionTag(mock(VersionTag.class));
    eventTracker.recordEvent(event);
  }

  private void putEvent(String key, String value, byte[] memId, long threadId,
      boolean skipCallbacks, int sequenceId, VersionTag tag) {
    EntryEventImpl event = EntryEventImpl.create(region, Operation.PUTALL_CREATE, key, value, null,
        false, member, !skipCallbacks, new EventID(memId, threadId, sequenceId));
    event.setContext(memberId);
    event.setVersionTag(tag);
    eventTracker.recordEvent(event);
  }

  @Test
  public void returnsCorrectNameOfCache() {
    String testName = "testing";
    when(region.getName()).thenReturn(testName);
    eventTracker = new DistributedEventTracker(region.getCache(), mock(CancelCriterion.class),
        region.getName());
    assertEquals("Event Tracker for " + testName, eventTracker.getName());
  }

  @Test
  public void initializationCorrectlyReadiesTheTracker() throws InterruptedException {
    assertFalse(eventTracker.isInitialized());
    eventTracker.setInitialized();
    assertTrue(eventTracker.isInitialized());
    eventTracker.waitOnInitialization();
  }

  @Test
  public void startAndStopAddAndRemoveTrackerFromExpiryTask() {
    EventTrackerExpiryTask task = mock(EventTrackerExpiryTask.class);
    InternalCache cache = mock(InternalCache.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getEventTrackerTask()).thenReturn(task);
    eventTracker = new DistributedEventTracker(region.getCache(), mock(CancelCriterion.class),
        region.getName());
    eventTracker.start();
    verify(task, times(1)).addTracker(eventTracker);
    eventTracker.stop();
    verify(task, times(1)).removeTracker(eventTracker);
  }

  @Test
  public void returnsEmptyMapIfRecordedEventsAreEmpty() {
    assertEquals(0, eventTracker.getState().size());
  }

  @Test
  public void returnsMapContainingSequenceIdHoldersCurrentlyPresent() {
    EventSequenceNumberHolder sequenceIdHolder = new EventSequenceNumberHolder(0L, null);
    ThreadIdentifier threadId = new ThreadIdentifier(new byte[0], 0L);
    eventTracker.recordSequenceNumber(threadId, sequenceIdHolder);
    Map<ThreadIdentifier, EventSequenceNumberHolder> state = eventTracker.getState();
    assertEquals(1, state.size());
    EventSequenceNumberHolder returnedHolder = state.get(threadId);
    assertNotNull(returnedHolder);
    // the version tag is stripped out on purpose, so passed in object and returned one are not
    // equal to each other
    assertNull(returnedHolder.getVersionTag());
    assertEquals(sequenceIdHolder.getLastSequenceNumber(), returnedHolder.getLastSequenceNumber());
  }

  @Test
  public void setToInitializedWhenStateRecorded() {
    eventTracker.recordState(null, Collections.emptyMap());
    assertTrue(eventTracker.isInitialized());
  }

  @Test
  public void setsInitialImageProvidedWhenStateRecorded() {
    InternalDistributedMember distributedMember = mock(InternalDistributedMember.class);
    eventTracker.recordState(distributedMember, Collections.emptyMap());
    assertTrue(eventTracker.isInitialImageProvider(distributedMember));
  }

  @Test
  public void entryInRecordedStateStoredWhenNotInCurrentState() {
    EventSequenceNumberHolder sequenceIdHolder = new EventSequenceNumberHolder(0L, null);
    ThreadIdentifier threadId = new ThreadIdentifier(new byte[0], 0L);
    Map<ThreadIdentifier, EventSequenceNumberHolder> state =
        Collections.singletonMap(threadId, sequenceIdHolder);
    eventTracker.recordState(null, state);
    Map<ThreadIdentifier, EventSequenceNumberHolder> storedState = eventTracker.getState();
    assertEquals(storedState.get(threadId).getLastSequenceNumber(),
        sequenceIdHolder.getLastSequenceNumber());
  }

  @Test
  public void entryInRecordedStateNotStoredIfAlreadyInCurrentState() {
    EventSequenceNumberHolder originalSequenceIdHolder = new EventSequenceNumberHolder(0L, null);
    ThreadIdentifier threadId = new ThreadIdentifier(new byte[0], 0L);
    Map<ThreadIdentifier, EventSequenceNumberHolder> state =
        Collections.singletonMap(threadId, originalSequenceIdHolder);
    eventTracker.recordState(null, state);

    EventSequenceNumberHolder newSequenceIdHolder = new EventSequenceNumberHolder(1L, null);
    Map<ThreadIdentifier, EventSequenceNumberHolder> newState =
        Collections.singletonMap(threadId, newSequenceIdHolder);
    eventTracker.recordState(null, newState);

    Map<ThreadIdentifier, EventSequenceNumberHolder> storedState = eventTracker.getState();
    assertEquals(storedState.get(threadId).getLastSequenceNumber(),
        originalSequenceIdHolder.getLastSequenceNumber());
  }

  @Test
  public void hasSeenEventReturnsFalseForEventWithNoID() {
    InternalCacheEvent event = mock(InternalCacheEvent.class);
    when(event.getEventId()).thenReturn(null);
    assertFalse(eventTracker.hasSeenEvent(event));
  }

  @Test
  public void hasSeenEventReturnsFalseForNullEventID() {
    assertFalse(eventTracker.hasSeenEvent((EventID) null));
    assertFalse(eventTracker.hasSeenEvent(null, null));
  }

  @Test
  public void hasNotSeenEventIDThatIsNotInRecordedEvents() {
    EventID eventID = new EventID(new byte[0], 0L, 0L);
    assertFalse(eventTracker.hasSeenEvent(eventID));
  }

  @Test
  public void hasSeenEventIDThatIsInRecordedEvents() {
    EventID eventID = new EventID(new byte[0], 0L, 0L);
    recordSequence(eventID);
    assertTrue(eventTracker.hasSeenEvent(eventID));
  }

  @Test
  public void hasNotSeenEventIDWhosSequenceIDIsMarkedRemoved() {
    EventID eventID = new EventID(new byte[0], 0L, 0L);
    EventSequenceNumberHolder sequenceIdHolder =
        new EventSequenceNumberHolder(eventID.getSequenceID(), null);
    sequenceIdHolder.setRemoved(true);
    ThreadIdentifier threadId = new ThreadIdentifier(new byte[0], 0L);
    eventTracker.recordSequenceNumber(threadId, sequenceIdHolder);

    assertFalse(eventTracker.hasSeenEvent(eventID));
  }

  @Test
  public void hasNotSeeEventIDWhosSequenceIDIsLargerThanSeen() {
    EventID eventID = new EventID(new byte[0], 0L, 0L);
    recordSequence(eventID);

    EventID higherSequenceID = new EventID(new byte[0], 0L, 1);
    assertFalse(eventTracker.hasSeenEvent(higherSequenceID));
  }

  @Test
  public void returnsNoTagIfNoSequenceForEvent() {
    EventID eventID = new EventID(new byte[0], 0L, 1L);
    assertNull(eventTracker.findVersionTagForSequence(eventID));
  }

  @Test
  public void returnsNoTagIfSequencesDoNotMatchForEvent() {
    EventID eventID = new EventID(new byte[0], 0L, 1);
    recordSequence(eventID);
    assertNull(eventTracker.findVersionTagForSequence(eventID));
  }

  @Test
  public void returnsCorrectTagForEvent() {
    EventID eventID = new EventID(new byte[0], 0L, 0L);
    EventSequenceNumberHolder sequenceIdHolder = recordSequence(eventID);
    assertEquals(sequenceIdHolder.getVersionTag(), eventTracker.findVersionTagForSequence(eventID));
  }

  @Test
  public void returnsNoTagIfNoBulkOpWhenNoEventGiven() {
    assertNull(eventTracker.findVersionTagForBulkOp(null));
  }

  @Test
  public void returnsNoTagIfNoBulkOpForEventWithSequence() {
    EventID eventID = new EventID(new byte[0], 0L, 1L);
    assertNull(eventTracker.findVersionTagForBulkOp(eventID));
  }

  @Test
  public void returnsNoTagIfBulkOpsDoNotMatchForEvent() {
    putEvent("key", "value", new byte[0], 0, false, 0);
    EventID eventIDWithoutBulkOp = new EventID(new byte[0], 0L, 1);
    assertNull(eventTracker.findVersionTagForBulkOp(eventIDWithoutBulkOp));
  }

  @Test
  public void returnsCorrectTagForEventWithBulkOp() {
    EventID eventID = new EventID(new byte[0], 0L, 0L);
    VersionTag tag = mock(VersionTag.class);
    putEvent("key", "value", new byte[0], 0, false, 0, tag);
    assertEquals(tag, eventTracker.findVersionTagForBulkOp(eventID));
  }

  @Test
  public void executesABulkOperations() {
    EventID eventID = new EventID(new byte[0], 0L, 1L);
    Runnable bulkOperation = mock(Runnable.class);
    eventTracker.syncBulkOp(bulkOperation, eventID, false);
    verify(bulkOperation, times(1)).run();
  }

  @Test
  public void executesRunnableIfNotPartOfATransaction() {
    EventID eventID = new EventID(new byte[0], 0L, 1L);
    Runnable bulkOperation = mock(Runnable.class);
    eventTracker.syncBulkOp(bulkOperation, eventID, true);
    verify(bulkOperation, times(1)).run();
  }

  private EventSequenceNumberHolder recordSequence(EventID eventID) {
    EventSequenceNumberHolder sequenceIdHolder =
        new EventSequenceNumberHolder(eventID.getSequenceID(), null);
    ThreadIdentifier threadIdentifier = new ThreadIdentifier(new byte[0], eventID.getThreadID());
    eventTracker.recordSequenceNumber(threadIdentifier, sequenceIdHolder);
    return sequenceIdHolder;
  }
}
