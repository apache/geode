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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
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
  public void messageDeliveredAfterRegisteringOnDrainIfNewFilterIDsIncludesClient()
      throws ExecutionException, InterruptedException {
    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);

    ReadWriteLock mockPutDrainLock = mock(ReadWriteLock.class);
    ReadWriteLock actualPutDrainLock = new ReentrantReadWriteLock();

    CountDownLatch waitForDrainAndRemoveLatch = new CountDownLatch(1);
    CountDownLatch waitForAddInProgressLatch = new CountDownLatch(1);

    when(mockPutDrainLock.readLock())
        .thenAnswer(i -> {
          waitForAddInProgressLatch.countDown();
          waitForDrainAndRemoveLatch.await();
          return actualPutDrainLock.readLock();
        });

    when(mockPutDrainLock.writeLock())
        .thenAnswer(i -> {
          waitForAddInProgressLatch.await();
          waitForDrainAndRemoveLatch.countDown();
          return actualPutDrainLock.writeLock();
        });

    clientRegistrationEventQueueManager.create(clientProxyMembershipID,
        new ConcurrentLinkedQueue<>(), mockPutDrainLock, new ReentrantLock());

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
    ClientProxyMembershipID originatorId = mock(ClientProxyMembershipID.class);
    CacheClientProxy cacheClientProxy = mock(CacheClientProxy.class);
    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);
    Set<ClientProxyMembershipID> recalculatedFilterClientIDs = new HashSet<>();
    recalculatedFilterClientIDs.add(clientProxyMembershipID);

    when(cacheClientNotifier.getFilterClientIDs(filterProfile, filterInfo,
        originatorId))
            .thenReturn(recalculatedFilterClientIDs);
    when(cacheClientNotifier.getClientProxy(clientProxyMembershipID)).thenReturn(cacheClientProxy);

    // Create empty filter client IDs produced by the "normal" put processing path, so we can test
    // that the event is still delivered if the client finished registering and needs the event.
    Set<ClientProxyMembershipID> normalPutFilterClientIDs = new HashSet<>();
    CompletableFuture<Void> addEventsToQueueTask = CompletableFuture.runAsync(() -> {
      // In thread one, we add and event to the queue
      clientRegistrationEventQueueManager
          .add(internalCacheEvent, clientUpdateMessage, normalPutFilterClientIDs,
              cacheClientNotifier);
    });

    CompletableFuture<Void> drainEventsFromQueueTask = CompletableFuture.runAsync(() -> {
      // In thread two, we drain the event from the queue
      clientRegistrationEventQueueManager.drain(clientProxyMembershipID, cacheClientNotifier);
    });

    CompletableFuture.allOf(addEventsToQueueTask, drainEventsFromQueueTask).get();

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
        new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock(), new ReentrantLock());

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
    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

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

    ClientRegistrationEventQueueManager.ClientRegistrationEventQueue clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(clientProxyMembershipID,
            new ConcurrentLinkedQueue<>(), mockPutDrainLock, new ReentrantLock());

    InternalCacheEvent internalCacheEvent = mock(InternalCacheEvent.class);
    when(internalCacheEvent.getRegion()).thenReturn(mock(LocalRegion.class));
    when(internalCacheEvent.getOperation()).thenReturn(mock(Operation.class));

    Conflatable conflatable = mock(Conflatable.class);
    Set<ClientProxyMembershipID> filterClientIDs = new HashSet<>();
    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);

    CompletableFuture<Void> addEventsToQueueTask = CompletableFuture.runAsync(() -> {
      for (int numAdds = 0; numAdds < 100000; ++numAdds) {
        // In thread one, we add events to the queue
        clientRegistrationEventQueueManager
            .add(internalCacheEvent, conflatable, filterClientIDs, cacheClientNotifier);
      }
    });

    CompletableFuture<Void> drainEventsFromQueueTask = CompletableFuture.runAsync(() -> {
      // In thread two, we drain events from the queue
      clientRegistrationEventQueueManager.drain(clientProxyMembershipID, cacheClientNotifier);
    });

    CompletableFuture.allOf(addEventsToQueueTask, drainEventsFromQueueTask).get();

    assertThat(clientRegistrationEventQueue.isEmpty()).isTrue();
  }

  @Test
  public void twoThreadsRegisteringSameClientNoEventsLost()
      throws ExecutionException, InterruptedException {
    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    InternalCacheEvent internalCacheEvent = mock(InternalCacheEvent.class);
    when(internalCacheEvent.getRegion()).thenReturn(mock(LocalRegion.class));
    when(internalCacheEvent.getOperation()).thenReturn(mock(Operation.class));

    Conflatable conflatable = mock(Conflatable.class);
    Set<ClientProxyMembershipID> filterClientIDs = new HashSet<>();
    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);

    ClientRegistrationEventQueueManager.ClientRegistrationEventQueue clientRegistrationEventQueue =
        clientRegistrationEventQueueManager.create(clientProxyMembershipID,
            new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock(), new ReentrantLock());

    for (int registrationIterations = 0; registrationIterations < 1000; ++registrationIterations) {
      Runnable clientRegistrationSimulation = () -> {
        for (int numAdds = 0; numAdds < getRandomNumberOfAdds(); ++numAdds) {
          // In thread one, we add events to the queue
          clientRegistrationEventQueueManager
              .add(internalCacheEvent, conflatable, filterClientIDs, cacheClientNotifier);
        }
        // In thread two, we drain events from the queue
        clientRegistrationEventQueueManager.drain(clientProxyMembershipID, cacheClientNotifier);
      };

      CompletableFuture<Void> registrationFutureOne =
          CompletableFuture.runAsync(clientRegistrationSimulation);
      CompletableFuture<Void> registrationFutureTwo =
          CompletableFuture.runAsync(clientRegistrationSimulation);

      CompletableFuture.allOf(registrationFutureOne, registrationFutureTwo).get();

      assertThat(clientRegistrationEventQueue.isEmpty()).isTrue();
    }
  }

  @Test
  public void addEventWithOffheapValueCopiedToHeap() {
    ClientRegistrationEventQueueManager clientRegistrationEventQueueManager =
        new ClientRegistrationEventQueueManager();

    EntryEventImpl internalCacheEvent = mock(EntryEventImpl.class);
    when(internalCacheEvent.getRegion()).thenReturn(mock(LocalRegion.class));
    Operation mockOperation = mock(Operation.class);
    when(mockOperation.isEntry()).thenReturn(true);
    when(internalCacheEvent.getOperation()).thenReturn(mockOperation);

    Conflatable conflatable = mock(Conflatable.class);
    Set<ClientProxyMembershipID> filterClientIDs = new HashSet<>();
    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);
    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);

    clientRegistrationEventQueueManager.create(clientProxyMembershipID,
        new ConcurrentLinkedQueue<>(), new ReentrantReadWriteLock(), new ReentrantLock());

    clientRegistrationEventQueueManager
        .add(internalCacheEvent, conflatable, filterClientIDs, cacheClientNotifier);

    verify(internalCacheEvent, times(1)).copyOffHeapToHeap();
  }

  /*
   * This helps to create contention between registration threads during the drain phase
   */
  private static int getRandomNumberOfAdds() {
    int min = 10_000;
    int max = 50_000;
    return ThreadLocalRandom.current().nextInt(min, max + 1);
  }
}
