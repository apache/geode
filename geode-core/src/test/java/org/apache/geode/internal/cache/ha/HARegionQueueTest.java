/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.internal.cache.ha;

import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class HARegionQueueTest {
  private HARegionQueue haRegionQueue;
  private final HARegion haRegion = mock(HARegion.class);
  private final InternalCache internalCache = mock(InternalCache.class);
  private final List<EventID> eventIDS = new LinkedList<>();
  private final EventID id1 = mock(EventID.class);
  private final EventID id2 = mock(EventID.class);
  private final EventID id3 = mock(EventID.class);
  private final EventID id4 = mock(EventID.class);
  private final Collection<List<EventID>> chunks = new LinkedList<>();
  private final List<EventID> chunk1 = new LinkedList<>();
  private final List<EventID> chunk2 = new LinkedList<>();
  private final InternalDistributedMember primary = mock(InternalDistributedMember.class);
  private final ThreadIdentifier tid = mock(ThreadIdentifier.class);
  private final HARegionQueue.DispatchedAndCurrentEvents wrapper =
      new HARegionQueue.DispatchedAndCurrentEvents();

  @Before
  public void setup() throws IOException, ClassNotFoundException, InterruptedException {
    StoppableReentrantReadWriteLock giiLock = mock(StoppableReentrantReadWriteLock.class);
    when(giiLock.readLock())
        .thenReturn(mock(StoppableReentrantReadWriteLock.StoppableReadLock.class));

    StoppableReentrantReadWriteLock rwLock = mock(StoppableReentrantReadWriteLock.class);
    when(rwLock.writeLock())
        .thenReturn(mock(StoppableReentrantReadWriteLock.StoppableWriteLock.class));
    when(rwLock.readLock())
        .thenReturn(mock(StoppableReentrantReadWriteLock.StoppableReadLock.class));

    HashMap map = new HashMap();
    when(haRegion.put(any(), any())).then((invocationOnMock) -> {
      return map.put(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1));
    });
    when(haRegion.get(any())).then((invocationOnMock) -> {
      return map.get(invocationOnMock.getArgument(0));
    });
    when(haRegion.getGemFireCache()).thenReturn(internalCache);
    haRegionQueue = new HARegionQueue("haRegion", haRegion, internalCache,
        new HAContainerMap(new ConcurrentHashMap()), null, (byte) 1, true,
        mock(HARegionQueueStats.class), giiLock, rwLock, mock(CancelCriterion.class), false,
        mock(StatisticsClock.class));

    CacheClientNotifier.resetInstance();
  }

  @Test
  public void whenProxyIDisNullThenItIsNotAddedToClientInterestList() {
    ClientUpdateMessageImpl clientUpdateMessage = mock(ClientUpdateMessageImpl.class);
    HAEventWrapper haEventWrapper = mock(HAEventWrapper.class);
    HAContainerWrapper haContainerWrapper = mock(HAContainerWrapper.class);
    String regionName = "mockRegion";
    when(haContainerWrapper.getProxyID(any())).thenReturn(null);
    haRegionQueue.addClientCQsAndInterestList(clientUpdateMessage, haEventWrapper,
        haContainerWrapper, regionName);
    verify(haEventWrapper, times(0)).getClientCqs();
    verify(haEventWrapper, times(0)).getClientUpdateMessage();

  }

  @Test
  public void conflateConflatableEntriesAndDoNotConflateNonConflatableEntries() throws Exception {
    EventID eventId1 = new EventID(new byte[] {1}, 1, 1);
    EventID eventId2 = new EventID(new byte[] {1}, 1, 2);
    EventID eventId3 = new EventID(new byte[] {1}, 1, 3);
    EventID eventId4 = new EventID(new byte[] {1}, 1, 4);
    EventID eventId5 = new EventID(new byte[] {1}, 1, 5);
    EventID eventId6 = new EventID(new byte[] {1}, 1, 6);
    EventID eventId7 = new EventID(new byte[] {1}, 1, 7);

    haRegionQueue.put(new ConflatableObject("key", "value1", eventId1, false, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value2", eventId2, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value3", eventId3, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value4", eventId4, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value5", eventId5, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value6", eventId6, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value7", eventId7, false, "someRegion"));
    assertEquals(3, haRegionQueue.size());
  }

  @Test
  public void queueShouldconflateConflatableEntries() throws Exception {
    EventID eventId1 = new EventID(new byte[] {1}, 1, 1);
    EventID eventId2 = new EventID(new byte[] {1}, 1, 2);
    EventID eventId3 = new EventID(new byte[] {1}, 1, 3);
    EventID eventId4 = new EventID(new byte[] {1}, 1, 4);
    EventID eventId5 = new EventID(new byte[] {1}, 1, 5);
    EventID eventId6 = new EventID(new byte[] {1}, 1, 6);
    EventID eventId7 = new EventID(new byte[] {1}, 1, 7);

    haRegionQueue.put(new ConflatableObject("key", "value1", eventId1, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value2", eventId2, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value3", eventId3, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value4", eventId4, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value5", eventId5, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value6", eventId6, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value7", eventId7, true, "someRegion"));
    assertEquals(1, haRegionQueue.size());
  }

  @Test
  public void queuePutElidesSequenceIdLowerThanOrEqualToLastSeenSequenceId() throws Exception {
    EventID eventId1 = new EventID(new byte[] {1}, 1, 1);
    EventID eventId2 = new EventID(new byte[] {1}, 1, 2);
    EventID eventId3 = new EventID(new byte[] {1}, 1, 0);
    EventID eventId4 = new EventID(new byte[] {1}, 1, 3);

    haRegionQueue.put(new ConflatableObject("key", "value1", eventId1, false, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value2", eventId2, false, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value3", eventId3, false, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value3", eventId4, false, "someRegion"));
    assertEquals(3, haRegionQueue.size());
  }

  @Test
  public void isQueueInitializedWithWaitDoesNotWaitIfInitialized() throws Exception {
    long time = 1;
    HARegionQueue spy = spy(haRegionQueue);
    doReturn(true).when(spy).isQueueInitialized();

    assertThat(spy.isQueueInitializedWithWait(time)).isTrue();

    verify(spy, never()).waitForInitialized(time);
  }

  @Test
  public void isQueueInitializedWithWaitWillWaitIfNotInitialized() throws Exception {
    long time = 1;
    HARegionQueue spy = spy(haRegionQueue);
    doReturn(false).doReturn(true).when(spy).isQueueInitialized();
    doNothing().when(spy).waitForInitialized(time);

    assertThat(spy.isQueueInitializedWithWait(time)).isTrue();

    verify(spy).waitForInitialized(time);
  }

  @Test
  public void isQueueInitializedWithWaitReturnsFalseIfNotInitializedAfterWait() throws Exception {
    long time = 1;
    HARegionQueue spy = spy(haRegionQueue);
    doReturn(false).doReturn(false).when(spy).isQueueInitialized();
    doNothing().when(spy).waitForInitialized(time);

    assertThat(spy.isQueueInitializedWithWait(time)).isFalse();

    verify(spy).waitForInitialized(time);
  }

  @Test
  public void getDispatchedOrRemovedEventsReturnsRemovedEvents() {
    HARegionQueue spy = spy(haRegionQueue);
    List<EventID> removedEvents;
    addEvents();
    doReturn(false).when(spy).isRemoved(id1);
    doReturn(true).when(spy).isRemoved(id2);
    doReturn(true).when(spy).isRemoved(id3);
    doReturn(false).when(spy).isRemoved(id4);

    removedEvents = spy.getDispatchedOrRemovedEvents(eventIDS);

    assertThat(removedEvents.size()).isEqualTo(2);
    assertThat(removedEvents.contains(id2));
    assertThat(removedEvents.contains(id3));
  }

  private void addEvents() {
    eventIDS.add(id1);
    eventIDS.add(id2);
    eventIDS.add(id3);
    eventIDS.add(id4);
  }

  @Test
  public void isRemovedReturnsTrueIfDispatchedAndCurrentEventsAreRemoved() {
    HARegionQueue spy = spy(haRegionQueue);
    doReturn(null).when(spy).getDispatchedAndCurrentEvents(id1);

    assertThat(spy.isRemoved(id1)).isTrue();
  }

  @Test
  public void isRemovedReturnsFalseIfSequenceIdGreaterThanLastDispatched() {
    HARegionQueue spy = spy(haRegionQueue);
    when(id1.getSequenceID()).thenReturn(100L);
    wrapper.lastDispatchedSequenceId = 99L;
    doReturn(wrapper).when(spy).getDispatchedAndCurrentEvents(id1);

    assertThat(spy.isRemoved(id1)).isFalse();
  }

  @Test
  public void isRemovedReturnsTrueIfSequenceIdEqualsLastDispatched() {
    HARegionQueue spy = spy(haRegionQueue);
    when(id1.getSequenceID()).thenReturn(100L);
    wrapper.lastDispatchedSequenceId = 100L;
    doReturn(wrapper).when(spy).getDispatchedAndCurrentEvents(id1);

    assertThat(spy.isRemoved(id1)).isTrue();
  }

  @Test
  public void isRemovedReturnsTrueIfSequenceIdLessThanLastDispatched() {
    HARegionQueue spy = spy(haRegionQueue);
    when(id1.getSequenceID()).thenReturn(90L);
    wrapper.lastDispatchedSequenceId = 100L;
    doReturn(wrapper).when(spy).getDispatchedAndCurrentEvents(id1);

    assertThat(spy.isRemoved(id1)).isTrue();
  }

  @Test
  public void doNotRunSynchronizationWithPrimaryIfHasDoneSynchronization() {
    HARegionQueue spy = spy(haRegionQueue);
    spy.hasSynchronizedWithPrimary.set(true);

    spy.synchronizeQueueWithPrimary(primary, internalCache);
    verify(spy, never()).runSynchronizationWithPrimary(primary, internalCache);
  }

  @Test
  public void doNotRunSynchronizationWithPrimaryIfSynchronizationIsInProgress() {
    HARegionQueue spy = spy(haRegionQueue);
    spy.synchronizeWithPrimaryInProgress.set(true);

    spy.synchronizeQueueWithPrimary(primary, internalCache);
    verify(spy, never()).runSynchronizationWithPrimary(primary, internalCache);
  }

  @Test
  public void doNotRunSynchronizationWithPrimaryIfGIINotFinished() {
    HARegionQueue spy = spy(haRegionQueue);

    spy.synchronizeQueueWithPrimary(primary, internalCache);
    verify(spy, never()).runSynchronizationWithPrimary(primary, internalCache);
  }

  @Test
  public void doNotRunSynchronizationWithPrimaryIfPrimaryHasOlderThanGEODE_1_14_0Version() {
    HARegionQueue spy = spy(haRegionQueue);
    spy.doneGIIQueueing.set(true);
    when(primary.getVersionOrdinal()).thenReturn((short) (KnownVersion.GEODE_1_14_0.ordinal() - 1));

    spy.synchronizeQueueWithPrimary(primary, internalCache);
    verify(spy, never()).runSynchronizationWithPrimary(primary, internalCache);
  }

  @Test
  public void runSynchronizationWithPrimaryIfPrimaryIsGEODE_1_14_0Version() {
    HARegionQueue spy = spy(haRegionQueue);
    spy.doneGIIQueueing.set(true);
    when(primary.getVersionOrdinal()).thenReturn(KnownVersion.GEODE_1_14_0.ordinal());
    doNothing().when(spy).runSynchronizationWithPrimary(primary, internalCache);

    spy.synchronizeQueueWithPrimary(primary, internalCache);
    verify(spy).runSynchronizationWithPrimary(primary, internalCache);
  }

  @Test
  public void runSynchronizationWithPrimaryIfPrimaryIsLaterThanGEODE_1_14_0Version() {
    HARegionQueue spy = spy(haRegionQueue);
    spy.doneGIIQueueing.set(true);
    when(primary.getVersionOrdinal()).thenReturn((short) (KnownVersion.GEODE_1_14_0.ordinal() + 1));
    doNothing().when(spy).runSynchronizationWithPrimary(primary, internalCache);

    spy.synchronizeQueueWithPrimary(primary, internalCache);
    verify(spy).runSynchronizationWithPrimary(primary, internalCache);
  }

  @Test
  public void getGIIEventsReturnsCorrectEvents() {
    HARegionQueue spy = spy(haRegionQueue);
    List<EventID> giiEvents;
    spy.positionBeforeGII = 1;
    spy.positionAfterGII = 4;
    HAEventWrapper wrapper1 = mock(HAEventWrapper.class);
    HAEventWrapper wrapper2 = mock(HAEventWrapper.class);
    when(wrapper1.getEventId()).thenReturn(id1);
    when(wrapper2.getEventId()).thenReturn(id2);
    Region.Entry entry1 = mock(Region.Entry.class);
    Region.Entry entry2 = mock(Region.Entry.class);
    Region.Entry entry3 = mock(Region.Entry.class);
    when(entry1.getValue()).thenReturn(wrapper1);
    when(entry3.getValue()).thenReturn(wrapper2);
    when(entry2.getValue()).thenReturn(id3);

    when(haRegion.getEntry(1L)).thenReturn(entry1);
    when(haRegion.getEntry(2L)).thenReturn(entry2);
    when(haRegion.getEntry(3L)).thenReturn(null);
    when(haRegion.getEntry(4L)).thenReturn(entry3);

    giiEvents = spy.getGIIEvents();

    assertThat(giiEvents.size()).isEqualTo(2);
    assertThat(giiEvents.contains(id1));
    assertThat(giiEvents.contains(id2));
  }

  @Test
  public void doSynchronizationWithPrimaryReturnsIfHasDoneSynchronization() {
    HARegionQueue spy = spy(haRegionQueue);
    spy.hasSynchronizedWithPrimary.set(true);

    spy.doSynchronizationWithPrimary(primary, internalCache);
    verify(spy, never()).getGIIEvents();
  }

  @Test
  public void doSynchronizationWithPrimaryReturnsIfNoGIIEvents() {
    HARegionQueue spy = spy(haRegionQueue);
    int maxChunkSize = 1000;
    spy.hasSynchronizedWithPrimary.set(true);
    doReturn(new LinkedList<EventID>()).when(spy).getGIIEvents();

    spy.doSynchronizationWithPrimary(primary, internalCache);

    verify(spy, never()).getChunks(eventIDS, maxChunkSize);
    verify(spy, never()).removeDispatchedEvents(primary, internalCache, eventIDS);
    assertThat(spy.hasSynchronizedWithPrimary).isTrue();
    assertThat(spy.synchronizeWithPrimaryInProgress).isFalse();
  }

  @Test
  public void doSynchronizationWithPrimaryRemoveDispatchedEvents() {
    HARegionQueue spy = spy(haRegionQueue);
    int maxChunkSize = 1000;
    addEvents();
    doReturn(eventIDS).when(spy).getGIIEvents();
    doReturn(true).when(spy).removeDispatchedEvents(primary, internalCache, eventIDS);

    spy.doSynchronizationWithPrimary(primary, internalCache);

    verify(spy, never()).getChunks(eventIDS, maxChunkSize);
    verify(spy).removeDispatchedEvents(primary, internalCache, eventIDS);
    assertThat(spy.hasSynchronizedWithPrimary).isTrue();
    assertThat(spy.synchronizeWithPrimaryInProgress).isFalse();
  }

  @Test
  public void hasSynchronizedWithPrimaryNotSetIfRemoveDispatchedEventsFails() {
    HARegionQueue spy = spy(haRegionQueue);
    int maxChunkSize = 1000;
    addEvents();
    doReturn(eventIDS).when(spy).getGIIEvents();
    doReturn(false).when(spy).removeDispatchedEvents(primary, internalCache, eventIDS);

    spy.doSynchronizationWithPrimary(primary, internalCache);

    verify(spy, never()).getChunks(eventIDS, maxChunkSize);
    verify(spy).removeDispatchedEvents(primary, internalCache, eventIDS);
    assertThat(spy.hasSynchronizedWithPrimary).isFalse();
    assertThat(spy.synchronizeWithPrimaryInProgress).isFalse();
  }

  @Test
  public void hasSynchronizedWithPrimaryRemoveChunksIfManyGIIEvents() {
    HARegionQueue spy = spy(haRegionQueue);
    int maxChunkSize = 1000;
    for (int i = 0; i < 1100; i++) {
      eventIDS.add(mock(EventID.class));
    }
    createChunks();
    doReturn(eventIDS).when(spy).getGIIEvents();
    doReturn(chunks).when(spy).getChunks(eventIDS, maxChunkSize);
    doReturn(true).when(spy).removeDispatchedEvents(primary, internalCache, chunk1);
    doReturn(true).when(spy).removeDispatchedEvents(primary, internalCache, chunk2);

    spy.doSynchronizationWithPrimary(primary, internalCache);

    verify(spy).getChunks(eventIDS, maxChunkSize);
    verify(spy).removeDispatchedEvents(primary, internalCache, chunk1);
    verify(spy).removeDispatchedEvents(primary, internalCache, chunk2);
    assertThat(spy.hasSynchronizedWithPrimary).isTrue();
    assertThat(spy.synchronizeWithPrimaryInProgress).isFalse();
  }

  private void createChunks() {
    chunk1.add(id1);
    chunk2.add(id2);
    chunks.add(chunk1);
    chunks.add(chunk2);
  }

  @Test
  public void hasSynchronizedWithPrimaryNotSetIfRemoveChunksFails() {
    HARegionQueue spy = spy(haRegionQueue);
    int maxChunkSize = 1000;
    for (int i = 0; i < 1100; i++) {
      eventIDS.add(mock(EventID.class));
    }
    createChunks();
    doReturn(eventIDS).when(spy).getGIIEvents();
    doReturn(chunks).when(spy).getChunks(eventIDS, maxChunkSize);
    doReturn(true).when(spy).removeDispatchedEvents(primary, internalCache, chunk1);
    doReturn(false).when(spy).removeDispatchedEvents(primary, internalCache, chunk2);

    spy.doSynchronizationWithPrimary(primary, internalCache);

    verify(spy).getChunks(eventIDS, maxChunkSize);
    verify(spy).removeDispatchedEvents(primary, internalCache, chunk1);
    verify(spy).removeDispatchedEvents(primary, internalCache, chunk2);
    assertThat(spy.hasSynchronizedWithPrimary).isFalse();
    assertThat(spy.synchronizeWithPrimaryInProgress).isFalse();
  }

  @Test
  public void getChunksReturnsEqualSizedChunks() {
    HARegionQueue spy = spy(haRegionQueue);
    addEvents();
    // add more events
    eventIDS.add(mock(EventID.class));
    eventIDS.add(mock(EventID.class));

    Collection<List<EventID>> myChunks = spy.getChunks(eventIDS, 2);

    assertThat(myChunks.size()).isEqualTo(3);
    for (List<EventID> chunk : myChunks) {
      assertThat(chunk.size()).isEqualTo(2);
    }
  }

  @Test
  public void removeDispatchedEventAfterSyncWithPrimaryRemovesEvents() throws Exception {
    HARegionQueue spy = spy(haRegionQueue);
    doNothing().when(spy).removeDispatchedEvents(id1);

    assertThat(spy.removeDispatchedEventAfterSyncWithPrimary(id1)).isTrue();
    verify(spy).removeDispatchedEvents(id1);
  }

  @Test
  public void removeDispatchedEventReturnsTrueIfRemovalThrowsCacheException() throws Exception {
    HARegionQueue spy = spy(haRegionQueue);
    doThrow(new EntryNotFoundException("")).when(spy).removeDispatchedEvents(id1);

    assertThat(spy.removeDispatchedEventAfterSyncWithPrimary(id1)).isTrue();
    verify(spy).removeDispatchedEvents(id1);
  }

  @Test
  public void removeDispatchedEventReturnsTrueIfRemovalThrowsRegionDestroyedException()
      throws Exception {
    HARegionQueue spy = spy(haRegionQueue);
    doThrow(new RegionDestroyedException("", "")).when(spy).removeDispatchedEvents(id1);

    assertThat(spy.removeDispatchedEventAfterSyncWithPrimary(id1)).isTrue();
    verify(spy).removeDispatchedEvents(id1);
  }

  @Test
  public void removeDispatchedEventReturnsFalseIfRemovalThrowsCancelException() throws Exception {
    HARegionQueue spy = spy(haRegionQueue);
    doThrow(new CacheClosedException()).when(spy).removeDispatchedEvents(id1);

    assertThat(spy.removeDispatchedEventAfterSyncWithPrimary(id1)).isFalse();
    verify(spy).removeDispatchedEvents(id1);
  }

  @Test
  public void removeDispatchedEventsReturnsFalseIfNotGettingEventsFromPrimary() {
    HARegionQueue spy = spy(haRegionQueue);
    doReturn(null).when(spy).getDispatchedEventsFromPrimary(primary, internalCache, eventIDS);

    assertThat(spy.removeDispatchedEvents(primary, internalCache, eventIDS)).isFalse();
  }

  @Test
  public void removeDispatchedEventsReturnsTrueIfRemovedDispatchedEvents() {
    HARegionQueue spy = spy(haRegionQueue);
    List<EventID> dispatched = new LinkedList<>();
    dispatched.add(id1);
    dispatched.add(id3);
    doReturn(dispatched).when(spy).getDispatchedEventsFromPrimary(primary, internalCache, eventIDS);
    doReturn(true).when(spy).removeDispatchedEventAfterSyncWithPrimary(id1);
    doReturn(true).when(spy).removeDispatchedEventAfterSyncWithPrimary(id3);

    assertThat(spy.removeDispatchedEvents(primary, internalCache, eventIDS)).isTrue();
  }

  @Test
  public void removeDispatchedEventsReturnsFalseIfFailedToRemoveDispatchedEvents() {
    HARegionQueue spy = spy(haRegionQueue);
    List<EventID> dispatched = new LinkedList<>();
    dispatched.add(id1);
    dispatched.add(id3);
    doReturn(dispatched).when(spy).getDispatchedEventsFromPrimary(primary, internalCache, eventIDS);
    doReturn(true).when(spy).removeDispatchedEventAfterSyncWithPrimary(id1);
    doReturn(false).when(spy).removeDispatchedEventAfterSyncWithPrimary(id3);

    assertThat(spy.removeDispatchedEvents(primary, internalCache, eventIDS)).isFalse();
  }
}
