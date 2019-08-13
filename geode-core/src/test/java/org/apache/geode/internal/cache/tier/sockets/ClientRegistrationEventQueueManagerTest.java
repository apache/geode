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

package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.FilterRoutingInfo;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.LocalRegion;

public class ClientRegistrationEventQueueManagerTest {
  @Test
  public void messageDeliveredAfterRegisteringOnDrainIfNewFilterIDsIncludesClient() {
    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);

    ClientRegistrationEventQueueManager.ClientRegistrationEventQueue clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(clientProxyMembershipID,
            new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock());

    InternalCacheEvent internalCacheEvent = mock(InternalCacheEvent.class);
    LocalRegion localRegion = mock(LocalRegion.class);
    FilterProfile filterProfile = mock(FilterProfile.class);
    FilterRoutingInfo filterRoutingInfo = mock(FilterRoutingInfo.class);
    FilterRoutingInfo.FilterInfo filterInfo = mock(FilterRoutingInfo.FilterInfo.class);

    when(filterRoutingInfo.getLocalFilterInfo()).thenReturn(
        filterInfo);
    when(filterProfile.getFilterRoutingInfoPart2(null, internalCacheEvent))
        .thenReturn(filterRoutingInfo);
    when(localRegion.getFilterProfile()).thenReturn(filterProfile);
    when(internalCacheEvent.getRegion()).thenReturn(localRegion);
    when(internalCacheEvent.getOperation()).thenReturn(mock(Operation.class));

    ClientUpdateMessageImpl clientUpdateMessage = mock(ClientUpdateMessageImpl.class);

    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);
    Set<ClientProxyMembershipID> recalculatedFilterClientIDs = new HashSet<>();
    recalculatedFilterClientIDs.add(clientProxyMembershipID);
    when(cacheClientNotifier.getFilterClientIDs(internalCacheEvent, filterProfile, filterInfo,
        clientUpdateMessage))
            .thenReturn(recalculatedFilterClientIDs);
    CacheClientProxy cacheClientProxy = mock(CacheClientProxy.class);
    when(cacheClientProxy.getProxyID()).thenReturn(clientProxyMembershipID);
    when(cacheClientNotifier.getClientProxy(clientProxyMembershipID)).thenReturn(cacheClientProxy);

    // Create empty filter client IDs produced by the "normal" put processing path, so we can test
    // that the event is still delivered if the client finished registering and needs the event.
    Set<ClientProxyMembershipID> normalPutFilterClientIDs = new HashSet<>();

    clientRegistrationEventQueueManager
        .add(internalCacheEvent, clientUpdateMessage, normalPutFilterClientIDs,
            cacheClientNotifier);

    clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue, cacheClientNotifier);

    // The client update message should still be delivered because it is now part of the
    // filter clients interested in this event, despite having not been included in the original
    // filter info in the "normal" put processing path.
    verify(cacheClientProxy, times(1)).deliverMessage(clientUpdateMessage);
  }

  @Test
  public void clientRemovedFromFilterClientsListIfEventAddedToRegistrationQueue() {
    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);

    clientRegistrationEventQueueManager.create(clientProxyMembershipID,
        new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock());

    InternalCacheEvent internalCacheEvent = mock(InternalCacheEvent.class);
    when(internalCacheEvent.getRegion()).thenReturn(mock(LocalRegion.class));
    when(internalCacheEvent.getOperation()).thenReturn(mock(Operation.class));

    Conflatable conflatable = mock(Conflatable.class);

    // Add the registering client to the filter clients. This can happen if the filter info is
    // received but the client is not completely registered yet (queue GII has not been completed).
    // In that case, we want to remove the client from the filter IDs set and add the event
    // to the client's registration queue.
    Set<ClientProxyMembershipID> filterClientIDs = new HashSet<>();
    filterClientIDs.add(clientProxyMembershipID);

    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);

    clientRegistrationEventQueueManager.add(internalCacheEvent, conflatable, filterClientIDs,
        cacheClientNotifier);

    // The client should no longer be in the filter clients since the event was queued in the
    // client's registration queue.
    assertThat(filterClientIDs.isEmpty()).isTrue();
  }

  @Test
  public void addAndDrainQueueContentionTest() throws ExecutionException, InterruptedException {
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    ReadWriteLock mockPutDrainLock = mock(ReadWriteLock.class);
    ReadWriteLock actualPutDrainLock = new ReentrantReadWriteLock();

    when(mockPutDrainLock.readLock())
        .thenReturn(actualPutDrainLock.readLock());

    when(mockPutDrainLock.writeLock())
        .thenAnswer(i -> {
          // Force a context switch from drain to put thread so we can ensure the event is not lost
          Thread.sleep(1);
          return actualPutDrainLock.writeLock();
        });

    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    ClientRegistrationEventQueueManager.ClientRegistrationEventQueue clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(clientProxyMembershipID,
            new ConcurrentLinkedQueue<>(), mockPutDrainLock);

    InternalCacheEvent internalCacheEvent = mock(InternalCacheEvent.class);
    when(internalCacheEvent.getRegion()).thenReturn(mock(LocalRegion.class));
    when(internalCacheEvent.getOperation()).thenReturn(mock(Operation.class));

    Conflatable conflatable = mock(Conflatable.class);
    Set<ClientProxyMembershipID> filterClientIDs = new HashSet<>();
    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);
    CacheClientProxy cacheClientProxy = mock(CacheClientProxy.class);
    when(cacheClientNotifier.getClientProxy(clientProxyMembershipID)).thenReturn(cacheClientProxy);

    CompletableFuture<Void> addEventsToQueueTask = CompletableFuture.runAsync(() -> {
      for (int numAdds = 0; numAdds < 100000; ++numAdds) {
        // In thread one, we add events to the queue
        clientRegistrationEventQueueManager
            .add(internalCacheEvent, conflatable, filterClientIDs, cacheClientNotifier);
      }
    });

    CompletableFuture<Void> drainEventsFromQueueTask = CompletableFuture.runAsync(() -> {
      // In thread two, we drain events from the queue
      clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue, cacheClientNotifier);
    });

    CompletableFuture.allOf(addEventsToQueueTask, drainEventsFromQueueTask).get();

    assertThat(clientRegistrationEventQueue.isEmpty()).isTrue();
  }

  @Test
  public void addEventWithOffheapValueCopiedToHeap() {
    EntryEventImpl internalCacheEvent = mock(EntryEventImpl.class);
    when(internalCacheEvent.getRegion()).thenReturn(mock(LocalRegion.class));
    Operation mockOperation = mock(Operation.class);
    when(mockOperation.isEntry()).thenReturn(true);
    when(internalCacheEvent.getOperation()).thenReturn(mockOperation);

    Conflatable conflatable = mock(Conflatable.class);
    Set<ClientProxyMembershipID> filterClientIDs = new HashSet<>();
    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);

    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    clientRegistrationEventQueueManager.create(clientProxyMembershipID,
        new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock());

    clientRegistrationEventQueueManager
        .add(internalCacheEvent, conflatable, filterClientIDs, cacheClientNotifier);

    verify(internalCacheEvent, times(1)).copyOffHeapToHeap();
  }

  @Test
  public void clientWasNeverRegisteredDrainQueueStillRemoved() {
    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);

    ClientRegistrationEventQueueManager.ClientRegistrationEventQueue clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(clientProxyMembershipID,
            new ConcurrentLinkedQueue<>(),
            new ReentrantReadWriteLock());

    clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue, cacheClientNotifier);

    assertEquals(0, clientRegistrationEventQueueManager.getRegisteringProxyEventQueuesSize());
  }

  @Test
  public void drainThrowsExceptionQueueStillRemoved() {
    EntryEventImpl internalCacheEvent = mock(EntryEventImpl.class);
    RuntimeException testException = new RuntimeException();
    when(internalCacheEvent.getRegion()).thenThrow(testException);
    Operation mockOperation = mock(Operation.class);
    when(mockOperation.isEntry()).thenReturn(true);
    when(internalCacheEvent.getOperation()).thenReturn(mockOperation);

    CacheClientProxy cacheClientProxy = mock(CacheClientProxy.class);
    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    when(cacheClientNotifier.getClientProxy(clientProxyMembershipID)).thenReturn(cacheClientProxy);

    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    ClientRegistrationEventQueueManager.ClientRegistrationEventQueue clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(clientProxyMembershipID,
            new ConcurrentLinkedQueue<>(),
            new ReentrantReadWriteLock());

    Conflatable conflatable = mock(Conflatable.class);
    Set<ClientProxyMembershipID> filterClientIDs = new HashSet<>();

    clientRegistrationEventQueueManager.add(internalCacheEvent, conflatable, filterClientIDs,
        cacheClientNotifier);

    assertThatThrownBy(() -> clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue,
        cacheClientNotifier))
            .isEqualTo(testException);

    assertEquals(0, clientRegistrationEventQueueManager.getRegisteringProxyEventQueuesSize());
  }

  @Test
  public void addEventInOriginalFilterIDsButQueueWasRemovedDueToSuccessfulRegistrationSoEventNotRedelivered() {
    InternalCacheEvent internalCacheEvent = mock(InternalCacheEvent.class);
    LocalRegion localRegion = mock(LocalRegion.class);
    FilterProfile filterProfile = mock(FilterProfile.class);
    FilterRoutingInfo filterRoutingInfo = mock(FilterRoutingInfo.class);
    FilterRoutingInfo.FilterInfo filterInfo = mock(FilterRoutingInfo.FilterInfo.class);

    when(filterRoutingInfo.getLocalFilterInfo()).thenReturn(
        filterInfo);
    when(filterProfile.getFilterRoutingInfoPart2(null, internalCacheEvent))
        .thenReturn(filterRoutingInfo);
    when(localRegion.getFilterProfile()).thenReturn(filterProfile);
    when(internalCacheEvent.getRegion()).thenReturn(localRegion);
    when(internalCacheEvent.getOperation()).thenReturn(mock(Operation.class));

    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);
    CacheClientProxy cacheClientProxy = mock(CacheClientProxy.class);
    when(cacheClientNotifier.getClientProxy(clientProxyMembershipID)).thenReturn(cacheClientProxy);
    Set<ClientProxyMembershipID> originalFilterIDs = new HashSet<>();
    originalFilterIDs.add(clientProxyMembershipID);

    ClientUpdateMessageImpl clientUpdateMessage = mock(ClientUpdateMessageImpl.class);

    Set<ClientProxyMembershipID> recalculatedFilterClientIDs = new HashSet<>();
    recalculatedFilterClientIDs.add(clientProxyMembershipID);
    when(cacheClientNotifier.getFilterClientIDs(internalCacheEvent, filterProfile, filterInfo,
        clientUpdateMessage))
            .thenReturn(recalculatedFilterClientIDs);
    when(cacheClientProxy.getProxyID()).thenReturn(clientProxyMembershipID);
    when(cacheClientNotifier.getClientProxy(clientProxyMembershipID)).thenReturn(cacheClientProxy);
    ReentrantReadWriteLock mockReadWriteLock = mock(ReentrantReadWriteLock.class);

    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    ClientRegistrationEventQueueManager.ClientRegistrationEventQueue clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(clientProxyMembershipID,
            new ConcurrentLinkedQueue<>(),
            mockReadWriteLock);

    ReentrantReadWriteLock.ReadLock mockReadLock = mock(ReentrantReadWriteLock.ReadLock.class);
    when(mockReadWriteLock.readLock()).thenReturn(mockReadLock);
    ReentrantReadWriteLock actualPutDrainLock = new ReentrantReadWriteLock();
    when(mockReadWriteLock.writeLock()).thenReturn(actualPutDrainLock.writeLock());
    doAnswer(i -> {
      clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue, cacheClientNotifier);
      actualPutDrainLock.readLock();
      return null;
    }).when(mockReadLock).lock();

    clientRegistrationEventQueueManager.add(internalCacheEvent, clientUpdateMessage,
        originalFilterIDs, cacheClientNotifier);

    verify(cacheClientProxy, times(0)).deliverMessage(clientUpdateMessage);
  }
}
