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

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.FilterRoutingInfo;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.logging.LogService;

/**
 * This class is responsible for queueing events for clients while they are registering. Once the
 * client is completely registered (filter info retrieved and GII complete), we will drain the
 * client's queued events and deliver them to the cache client proxy if necessary.
 */
class ClientRegistrationEventQueueManager {
  private static final Logger logger = LogService.getLogger();
  private final Map<ClientProxyMembershipID, ClientRegistrationEventQueue> registeringProxyEventQueues =
      new ConcurrentHashMap<>();

  void add(final InternalCacheEvent event,
      final Conflatable conflatable,
      final Set<ClientProxyMembershipID> originalFilterClientIDs,
      final CacheClientNotifier cacheClientNotifier) {
    if (registeringProxyEventQueues.isEmpty())
      return;

    ClientRegistrationEvent clientRegistrationEvent =
        new ClientRegistrationEvent(event, conflatable);

    for (final Map.Entry<ClientProxyMembershipID, ClientRegistrationEventQueue> eventsReceivedWhileRegisteringClient : registeringProxyEventQueues
        .entrySet()) {
      ClientProxyMembershipID clientProxyMembershipID =
          eventsReceivedWhileRegisteringClient.getKey();
      ClientRegistrationEventQueue registrationEventQueue =
          eventsReceivedWhileRegisteringClient.getValue();

      registrationEventQueue.lockForPutting();
      try {
        // If this is an HAEventWrapper we need to increment the PutInProgress counter so
        // that the contents of the contents of the HAEventWrapper are preserved when the
        // event is drained and processed. See incrementPutInProgressCounter() and
        // decrementPutInProgressCounter() for more details.
        if (conflatable instanceof HAEventWrapper) {
          ((HAEventWrapper) conflatable).incrementPutInProgressCounter();
        }

        // After taking out the lock, we need to determine if the client is still actually
        // registering since there is a small race where it may have finished registering
        // after we pulled the queue out of the registeringProxyEventQueues collection
        if (registeringProxyEventQueues.containsKey(clientProxyMembershipID)) {
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
          processEventAndDeliverConflatable(clientProxyMembershipID, cacheClientNotifier, event,
              conflatable, originalFilterClientIDs);
        }
      } finally {
        registrationEventQueue.unlockForPutting();
      }
    }
  }

  void drain(final ClientProxyMembershipID clientProxyMembershipID,
      final CacheClientNotifier cacheClientNotifier) {
    // As an optimization, we drain as many events from the queue as we can
    // before taking out a lock to drain the remaining events
    if (logger.isDebugEnabled()) {
      logger.debug("Draining events from registration queue for client proxy "
          + clientProxyMembershipID
          + " without synchronization");
    }

    drainEventsReceivedWhileRegisteringClient(clientProxyMembershipID, cacheClientNotifier);

    ClientRegistrationEventQueue registrationEventQueue =
        registeringProxyEventQueues.get(clientProxyMembershipID);

    registrationEventQueue.lockForDraining();
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Draining remaining events from registration queue for client proxy "
            + clientProxyMembershipID + " with synchronization");
      }

      drainEventsReceivedWhileRegisteringClient(clientProxyMembershipID, cacheClientNotifier);

      registeringProxyEventQueues.remove(clientProxyMembershipID);
    } finally {
      registrationEventQueue.unlockForDraining();
    }
  }

  private void drainEventsReceivedWhileRegisteringClient(final ClientProxyMembershipID proxyID,
      final CacheClientNotifier cacheClientNotifier) {
    ClientRegistrationEvent queuedEvent;
    ClientRegistrationEventQueue registrationEventQueue = registeringProxyEventQueues.get(proxyID);

    while ((queuedEvent = registrationEventQueue.poll()) != null) {
      InternalCacheEvent internalCacheEvent = queuedEvent.internalCacheEvent;
      Conflatable conflatable = queuedEvent.conflatable;
      processEventAndDeliverConflatable(proxyID, cacheClientNotifier, internalCacheEvent,
          conflatable, null);
    }
  }

  public ClientRegistrationEventQueue create(
      final ClientProxyMembershipID clientProxyMembershipID,
      final Queue<ClientRegistrationEvent> eventQueue,
      final ReadWriteLock putDrainLock) {
    final ClientRegistrationEventQueue clientRegistrationEventQueue =
        new ClientRegistrationEventQueue(eventQueue,
            putDrainLock);
    registeringProxyEventQueues.put(clientProxyMembershipID,
        clientRegistrationEventQueue);
    return clientRegistrationEventQueue;
  }

  class ClientRegistrationEventQueue {
    Queue<ClientRegistrationEvent> eventQueue;
    ReadWriteLock readWriteLock;

    ClientRegistrationEventQueue(
        final Queue<ClientRegistrationEvent> eventQueue, final ReadWriteLock readWriteLock) {
      this.eventQueue = eventQueue;
      this.readWriteLock = readWriteLock;
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
      readWriteLock.writeLock().lock();
    }

    private void unlockForDraining() {
      readWriteLock.writeLock().unlock();
    }

    private void lockForPutting() {
      readWriteLock.readLock().lock();
    }

    private void unlockForPutting() {
      readWriteLock.readLock().unlock();
    }
  }

  private void processEventAndDeliverConflatable(final ClientProxyMembershipID proxyID,
      final CacheClientNotifier cacheClientNotifier,
      final InternalCacheEvent internalCacheEvent,
      final Conflatable conflatable,
      final Set<ClientProxyMembershipID> originalFilterClientIDs) {
    // The first step is to repopulate the filter info for the event to determine if
    // the client which was registering has a matching CQ or has registered interest
    // in the key for this event. We need to get the filter profile, filter routing info,
    // and local filter info in order to do so. If any of these are null, then there is
    // no need to proceed as the client is not interested.
    FilterProfile filterProfile =
        ((LocalRegion) internalCacheEvent.getRegion()).getFilterProfile();

    if (filterProfile != null) {
      FilterRoutingInfo filterRoutingInfo =
          filterProfile.getFilterRoutingInfoPart2(null, internalCacheEvent);

      if (filterRoutingInfo != null) {
        FilterRoutingInfo.FilterInfo filterInfo = filterRoutingInfo.getLocalFilterInfo();

        if (filterInfo != null) {
          ClientUpdateMessageImpl clientUpdateMessage = conflatable instanceof HAEventWrapper
              ? (ClientUpdateMessageImpl) ((HAEventWrapper) conflatable).getClientUpdateMessage()
              : (ClientUpdateMessageImpl) conflatable;

          internalCacheEvent.setLocalFilterInfo(filterInfo);

          Set<ClientProxyMembershipID> newFilterClientIDs =
              cacheClientNotifier.getFilterClientIDs(internalCacheEvent, filterProfile,
                  filterInfo,
                  clientUpdateMessage);

          if (eventNotInOriginalFilterClientIDs(proxyID, newFilterClientIDs,
              originalFilterClientIDs)) {
            CacheClientProxy cacheClientProxy = cacheClientNotifier.getClientProxy(proxyID);

            if (eventShouldBeDelivered(proxyID, newFilterClientIDs, cacheClientProxy)) {
              cacheClientProxy.deliverMessage(conflatable);
            }
          }
        }
      }
    }

    // Once we have processed the conflatable, if it is an HAEventWrapper we can
    // decrement the PutInProgress counter, allowing the ClientUpdateMessage to be
    // set to null. See decrementPutInProgressCounter() for more details.
    if (conflatable instanceof HAEventWrapper) {
      ((HAEventWrapper) conflatable).decrementPutInProgressCounter();
    }
  }

  private boolean eventShouldBeDelivered(final ClientProxyMembershipID proxyID,
      final Set<ClientProxyMembershipID> filterClientIDs,
      final CacheClientProxy cacheClientProxy) {
    return filterClientIDs.contains(proxyID) && cacheClientProxy != null;
  }

  /*
   * This is to handle the edge case where the original filter client IDs
   * calculated by "normal" put processing did not include the registering client
   * because the filter info had not been received yet, but we now found that the client
   * is interested in the event so we should deliver it.
   */
  private boolean eventNotInOriginalFilterClientIDs(final ClientProxyMembershipID proxyID,
      final Set<ClientProxyMembershipID> newFilterClientIDs,
      final Set<ClientProxyMembershipID> originalFilterClientIDs) {
    return originalFilterClientIDs == null
        || (!originalFilterClientIDs.contains(proxyID) && newFilterClientIDs.contains(proxyID));
  }

  /**
   * Represents a conflatable and event processed while a client was registering.
   * This needs to be queued and processed after registration is complete. The conflatable
   * is what we will actually be delivering to the MessageDispatcher (and thereby adding
   * to the HARegionQueue). The internal cache event is required to rehydrate the filter
   * info and determine if the client which was registering does have a CQ that matches or
   * has registered interest in the key.
   */
  private class ClientRegistrationEvent {
    private final Conflatable conflatable;
    private final InternalCacheEvent internalCacheEvent;

    ClientRegistrationEvent(final InternalCacheEvent internalCacheEvent,
        final Conflatable conflatable) {
      this.conflatable = conflatable;
      this.internalCacheEvent = internalCacheEvent;
    }
  }
}
