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

import java.util.Collection;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.FilterRoutingInfo;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This class is responsible for queueing events for clients while they are registering. Once the
 * client is completely registered (filter info retrieved and GII complete), we will drain the
 * client's queued events and deliver them to the cache client proxy if necessary.
 */
public class ClientRegistrationEventQueueManager {
  private static final Logger logger = LogService.getLogger();

  private final Set<ClientRegistrationEventQueue> registeringProxyEventQueues =
      ConcurrentHashMap.newKeySet();

  void add(final InternalCacheEvent event,
      final ClientUpdateMessageImpl clientMessage,
      final Conflatable conflatable,
      final Set<ClientProxyMembershipID> originalFilterClientIDs,
      final CacheClientNotifier cacheClientNotifier) {
    if (registeringProxyEventQueues.isEmpty()) {
      return;
    }

    if (originalFilterClientIDs.isEmpty()
        && event.getOperation().isEntry()
        && !(clientMessage instanceof ClientTombstoneMessage)) {
      EntryEventImpl entryEvent = (EntryEventImpl) event;
      entryEvent.exportNewValue(clientMessage);
    }

    ClientRegistrationEvent clientRegistrationEvent =
        new ClientRegistrationEvent(event, conflatable);

    for (final ClientRegistrationEventQueue registrationEventQueue : registeringProxyEventQueues) {
      registrationEventQueue.lockForPutting();
      try {
        // If this is an HAEventWrapper we need to increment the PutInProgress counter so
        // that the contents of the contents of the HAEventWrapper are preserved when the
        // event is drained and processed. See incrementPutInProgressCounter() and
        // decrementPutInProgressCounter() for more details.
        if (conflatable instanceof HAEventWrapper) {
          ((HAEventWrapper) conflatable).incrementPutInProgressCounter("client registration");
        }

        ClientProxyMembershipID clientProxyMembershipID =
            registrationEventQueue.getClientProxyMembershipID();

        // After taking out the lock, we need to determine if the client is still actually
        // registering since there is a small race where it may have finished registering
        // after we pulled the queue out of the registeringProxyEventQueues collection
        if (registeringProxyEventQueues.contains(registrationEventQueue)) {
          // If the event value is off-heap, copy it to heap so we are guaranteed the value
          // is available when we drain the registration queue
          copyOffHeapToHeapForRegistrationQueue(event);

          registrationEventQueue.add(clientRegistrationEvent);

          // Because this event will be processed and sent when it is drained out of the temporary
          // client registration queue, we do not need to send it to the client at this time.
          // We can prevent the event from being sent to the registering client at this time
          // by removing its client proxy membership ID from the filter clients collection,
          // if it exists.
          originalFilterClientIDs.remove(clientProxyMembershipID);
        } else {
          // The client is no longer registering, but we want to reprocess the filter info on
          // this event and potentially deliver the conflatable to handle the edge case where
          // the original client filter IDs generated in the "normal" put processing path did
          // not include the registering client because the filter info was not yet available.
          CacheClientProxy cacheClientProxy =
              cacheClientNotifier.getClientProxy(clientProxyMembershipID);

          processEventAndDeliverConflatable(cacheClientProxy, cacheClientNotifier, event,
              conflatable, originalFilterClientIDs);
        }
      } finally {
        registrationEventQueue.unlockForPutting();
      }
    }
  }

  void drain(final ClientRegistrationEventQueue clientRegistrationEventQueue,
      final CacheClientNotifier cacheClientNotifier) {
    try {
      ClientProxyMembershipID clientProxyMembershipID =
          clientRegistrationEventQueue.getClientProxyMembershipID();

      if (logger.isDebugEnabled()) {
        logger.debug("Draining events from registration queue for client proxy "
            + clientProxyMembershipID
            + " without synchronization");
      }

      CacheClientProxy cacheClientProxy =
          cacheClientNotifier.getClientProxy(clientProxyMembershipID);

      drainEventsReceivedWhileRegisteringClient(cacheClientProxy, clientRegistrationEventQueue,
          cacheClientNotifier);

      // Prevents additional events from being added to the queue while we process and remove it
      clientRegistrationEventQueue.lockForDraining();

      if (logger.isDebugEnabled()) {
        logger.debug("Draining remaining events from registration queue for client proxy "
            + clientProxyMembershipID
            + " with synchronization");
      }

      drainEventsReceivedWhileRegisteringClient(cacheClientProxy, clientRegistrationEventQueue,
          cacheClientNotifier);
    } finally {
      // The queue must be removed before attempting to release the drain lock
      // so that no additional events can be added from add threads.
      registeringProxyEventQueues.remove(clientRegistrationEventQueue);

      if (clientRegistrationEventQueue.isLockForDrainingHeld()) {
        clientRegistrationEventQueue.unlockForDraining();
      }
    }
  }

  ClientRegistrationEventQueue create(
      final ClientProxyMembershipID clientProxyMembershipID,
      final Queue<ClientRegistrationEvent> eventQueue,
      final ReentrantReadWriteLock eventAddDrainLock) {
    final ClientRegistrationEventQueue clientRegistrationEventQueue =
        new ClientRegistrationEventQueue(clientProxyMembershipID, eventQueue,
            eventAddDrainLock);
    registeringProxyEventQueues.add(clientRegistrationEventQueue);
    return clientRegistrationEventQueue;
  }

  private void processEventAndDeliverConflatable(final CacheClientProxy cacheClientProxy,
      final CacheClientNotifier cacheClientNotifier,
      final InternalCacheEvent internalCacheEvent,
      final Conflatable conflatable,
      final Set<ClientProxyMembershipID> originalFilterClientIDs) {
    try {
      // If the cache client proxy is null, the registration was not successful and the proxy
      // was never added to the initialized proxy collection managed by the cache client notifier.
      // If that is the case, we can just decrement the put in progress counter on the conflatable
      // if it is an HAEventWrapper.
      if (cacheClientProxy != null) {
        // The first step is to repopulate the filter info for the event to determine if
        // the client which was registering has a matching CQ or has registered interest
        // in the key for this event. We need to get the filter profile, filter routing info,
        // and local filter info in order to do so. If any of these are null, then there is
        // no need to proceed as the client is not interested.
        FilterProfile filterProfile =
            ((InternalRegion) internalCacheEvent.getRegion()).getFilterProfile();

        if (filterProfile != null) {
          FilterRoutingInfo filterRoutingInfo =
              filterProfile.getFilterRoutingInfoPart2(null, internalCacheEvent, true);

          if (filterRoutingInfo != null) {
            FilterRoutingInfo.FilterInfo filterInfo = filterRoutingInfo.getLocalFilterInfo();

            if (filterInfo != null) {
              ClientUpdateMessageImpl clientUpdateMessage = conflatable instanceof HAEventWrapper
                  ? (ClientUpdateMessageImpl) ((HAEventWrapper) conflatable)
                      .getClientUpdateMessage()
                  : (ClientUpdateMessageImpl) conflatable;

              internalCacheEvent.setLocalFilterInfo(filterInfo);

              Set<ClientProxyMembershipID> newFilterClientIDs =
                  cacheClientNotifier.getFilterClientIDs(internalCacheEvent, filterProfile,
                      filterInfo,
                      clientUpdateMessage);

              ClientProxyMembershipID proxyID = cacheClientProxy.getProxyID();
              if (eventNotInOriginalFilterClientIDs(proxyID, newFilterClientIDs,
                  originalFilterClientIDs) && newFilterClientIDs.contains(proxyID)) {
                cacheClientProxy.deliverMessage(conflatable);
              }
            }
          }
        }
      }
    } finally {
      // Once we have processed the conflatable, if it is an HAEventWrapper we can
      // decrement the PutInProgress counter, allowing the ClientUpdateMessage to be
      // set to null. See decrementPutInProgressCounter() for more details.
      if (conflatable instanceof HAEventWrapper) {
        ((HAEventWrapper) conflatable).decrementPutInProgressCounter();
      }
    }
  }

  /*
   * This is to handle the edge case where the original filter client IDs
   * calculated by "normal" put processing did not include the registering client
   * because the filter info had not been received yet, but we now found that the client
   * is interested in the event so we should deliver it.
   */
  private boolean eventNotInOriginalFilterClientIDs(final ClientProxyMembershipID proxyID,
      final Collection<ClientProxyMembershipID> newFilterClientIDs,
      final Collection<ClientProxyMembershipID> originalFilterClientIDs) {
    return originalFilterClientIDs == null
        || !originalFilterClientIDs.contains(proxyID)
            && newFilterClientIDs.contains(proxyID);
  }

  /**
   * For simplicity, we will copy off-heap registration queue values to heap to avoid
   * complicated off-heap reference counting. Since the registration queue is only a
   * temporary construct during client registration, the overhead should not be significant.
   *
   * @param event The InternalCacheEvent whose value will be copied to the heap if need be
   */
  private void copyOffHeapToHeapForRegistrationQueue(final CacheEvent event) {
    if (event.getOperation().isEntry()) {
      EntryEventImpl entryEvent = ((EntryEventImpl) event);
      entryEvent.copyOffHeapToHeap();
    }
  }

  private void drainEventsReceivedWhileRegisteringClient(
      final CacheClientProxy cacheClientProxy,
      final ClientRegistrationEventQueue registrationEventQueue,
      final CacheClientNotifier cacheClientNotifier) {
    ClientRegistrationEvent queuedEvent;
    while ((queuedEvent = registrationEventQueue.poll()) != null) {
      InternalCacheEvent internalCacheEvent = queuedEvent.internalCacheEvent;
      Conflatable conflatable = queuedEvent.conflatable;
      processEventAndDeliverConflatable(cacheClientProxy, cacheClientNotifier, internalCacheEvent,
          conflatable, null);
    }
  }

  /**
   * Represents a conflatable and event processed while a client was registering.
   * This needs to be queued and processed after registration is complete. The conflatable
   * is what we will actually be delivering to the MessageDispatcher (and thereby adding
   * to the HARegionQueue). The internal cache event is required to rehydrate the filter
   * info and determine if the client which was registering does have a CQ that matches or
   * has registered interest in the key.
   */
  @VisibleForTesting
  static class ClientRegistrationEvent {

    private final Conflatable conflatable;
    private final InternalCacheEvent internalCacheEvent;

    private ClientRegistrationEvent(final InternalCacheEvent internalCacheEvent,
        final Conflatable conflatable) {
      this.conflatable = conflatable;
      this.internalCacheEvent = internalCacheEvent;
    }

    @Override
    public String toString() {
      return "ClientRegistrationEvent{" +
          "conflatable=" + conflatable +
          ", internalCacheEvent=" + internalCacheEvent +
          '}';
    }
  }

  static class ClientRegistrationEventQueue {

    private final ClientProxyMembershipID clientProxyMembershipID;
    private final Queue<ClientRegistrationEvent> eventQueue;
    private final ReentrantReadWriteLock eventAddDrainLock;

    private ClientRegistrationEventQueue(
        final ClientProxyMembershipID clientProxyMembershipID,
        final Queue<ClientRegistrationEvent> eventQueue,
        final ReentrantReadWriteLock eventAddDrainLock) {
      this.clientProxyMembershipID = clientProxyMembershipID;
      this.eventQueue = eventQueue;
      this.eventAddDrainLock = eventAddDrainLock;
    }

    @Override
    public String toString() {
      return "ClientRegistrationEventQueue{" +
          "clientProxyMembershipID=" + clientProxyMembershipID +
          ", eventQueue=" + eventQueue +
          ", eventAddDrainLock=" + eventAddDrainLock +
          '}';
    }

    public ClientProxyMembershipID getClientProxyMembershipID() {
      return clientProxyMembershipID;
    }

    boolean isEmpty() {
      return eventQueue.isEmpty();
    }

    private void add(final ClientRegistrationEvent clientRegistrationEvent) {
      eventQueue.add(clientRegistrationEvent);
    }

    public ClientRegistrationEvent poll() {
      return eventQueue.poll();
    }

    private void lockForDraining() {
      eventAddDrainLock.writeLock().lock();
    }

    private void unlockForDraining() {
      eventAddDrainLock.writeLock().unlock();
    }

    private boolean isLockForDrainingHeld() {
      return eventAddDrainLock.writeLock().isHeldByCurrentThread();
    }

    private void lockForPutting() {
      eventAddDrainLock.readLock().lock();
    }

    private void unlockForPutting() {
      eventAddDrainLock.readLock().unlock();
    }
  }
}
