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
package org.apache.geode.internal.cache.wan.serial;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender.EventWrapper;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventCallbackArgument;
import org.apache.geode.internal.cache.wan.GatewaySenderEventCallbackDispatcher;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.pdx.internal.PeerTypeRegistration;

/**
 * @since GemFire 7.0
 *
 */
public class SerialGatewaySenderEventProcessor extends AbstractGatewaySenderEventProcessor {
  private static final Logger logger = LogService.getLogger();

  private final Object unprocessedEventsLock = new Object();

  protected static final int RANDOM_SLEEP_TIME = 1000;
  /**
   * A <code>Map</code> of events that have not been processed by the primary yet. This map is
   * created and used by a secondary <code>GatewaySender</code> to keep track of events that have
   * been received by the secondary but not yet processed by the primary. Once an event has been
   * processed by the primary, it is removed from this map. This map will only be used in the event
   * that this <code>GatewaySender</code> becomes the primary. Any events contained in this map will
   * need to be sent to other <code>GatewayReceiver</code>s. Note: unprocessedEventsLock MUST be
   * synchronized before using this map.
   */
  private Map<EventID, EventWrapper> unprocessedEvents;

  /**
   * A <code>Map</code> of tokens (i.e. longs) of entries that we have heard of from the primary but
   * not yet the secondary. This map is created and used by a secondary <code>GatewaySender</code>
   * to keep track. Note: unprocessedEventsLock MUST be synchronized before using this map. This is
   * not a cut and paste error. sync unprocessedEventsLock when using unprocessedTokens.
   */
  protected Map<EventID, Long> unprocessedTokens;

  private ExecutorService executor;

  private Object listenerObjectLock = new Object();

  private boolean failoverCompleted = false;

  private final Object failoverCompletedLock = new Object();

  /**
   * When the Number of unchecked events exceeds this threshold and the number of tokens in the map
   * exceeds this threshold then a check will be done for old tokens.
   */
  protected static final int REAP_THRESHOLD = 1000;

  /*
   * How many events have happened without a reap check being done?
   */
  private int uncheckedCount = 0;


  public SerialGatewaySenderEventProcessor(AbstractGatewaySender sender, String id) {
    super(LoggingThreadGroup.createThreadGroup("Event Processor for GatewaySender_" + id, logger),
        "Event Processor for GatewaySender_" + id, sender);

    this.unprocessedEvents = new LinkedHashMap<EventID, EventWrapper>();
    this.unprocessedTokens = new LinkedHashMap<EventID, Long>();

    initializeMessageQueue(id);
    setDaemon(true);
  }

  @Override
  protected void initializeMessageQueue(String id) {
    // Create the region name
    StringBuffer regionNameBuffer = new StringBuffer();
    regionNameBuffer.append(id).append("_SERIAL_GATEWAY_SENDER_QUEUE");
    String regionName = regionNameBuffer.toString();

    CacheListener listener = null;
    if (!this.sender.isPrimary()) {
      listener = new SerialSecondaryGatewayListener(this);
      initializeListenerExecutor();
    }
    // Create the region queue
    this.queue = new SerialGatewaySenderQueue(sender, regionName, listener);

    if (logger.isDebugEnabled()) {
      logger.debug("Created queue: {}", this.queue);
    }
  }

  /**
   * @return false on failure
   */
  protected boolean waitForPrimary() {

    try {
      this.sender.getSenderAdvisor().waitToBecomePrimary(this);
    } catch (InterruptedException e) {
      // No need to set the interrupt bit, we're exiting the thread.
      if (!stopped()) {
        logger.fatal(
            LocalizedMessage.create(
                LocalizedStrings.GatewayImpl_AN_INTERRUPTEDEXCEPTION_OCCURRED_THE_THREAD_WILL_EXIT),
            e);
      }
      shutdownListenerExecutor();
      return false;
    }
    try {
      shutdownListenerExecutor();

      // Don't allow conserve-sockets = false so that ordering is preserved.
      DistributedSystem.setThreadsSocketPolicy(true);

      // Once notification has occurred, handle failover
      if (!stopped()) {
        handleFailover();
      } else {
        return false;
      }
    } catch (RegionDestroyedException e) {
      // This happens during handleFailover
      // because the region on _eventQueue can be closed.
      if (!stopped()) {
        logger.debug("Terminating due to {}", e.getMessage(), e);
      }
      return false;
    } catch (CancelException e) {
      if (!stopped()) {
        logger.debug("Terminating due to {}", e.getMessage(), e);
      }
      return false;
    } finally {
      // This is a good thing even on termination, because it ends waits
      // on the part of other threads.
      completeFailover();
    }
    return true;
  }



  @Override
  public void run() {
    try {
      setRunningStatus();
      // If this is not a primary gateway, wait for notification
      if (!sender.isPrimary()) {
        if (!waitForPrimary()) {
          return;
        }
      } else {
        // we are the primary so mark failover as being completed
        completeFailover();
      }
      // Begin to process the message queue after becoming primary
      if (logger.isDebugEnabled()) {
        logger.debug("Beginning to process the message queue");
      }

      if (!sender.isPrimary()) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.GatewayImpl_ABOUT_TO_PROCESS_THE_MESSAGE_QUEUE_BUT_NOT_THE_PRIMARY));
      }

      // Sleep for a bit. The random is so that if several of these are
      // started at once, they will stagger a bit.
      try {
        Thread.sleep(new Random().nextInt(RANDOM_SLEEP_TIME));
      } catch (InterruptedException e) {
        // no need to set the interrupt bit or throw an exception; just exit.
        return;
      }
      processQueue();
    } catch (CancelException e) {
      if (!this.isStopped()) {
        logger.info(LocalizedMessage
            .create(LocalizedStrings.GatewayImpl_A_CANCELLATION_OCCURRED_STOPPING_THE_DISPATCHER));
        setIsStopped(true);
      }
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.fatal(LocalizedMessage.create(
          LocalizedStrings.GatewayImpl_MESSAGE_DISPATCH_FAILED_DUE_TO_UNEXPECTED_EXCEPTION), e);
    }
  }

  @Override
  protected void rebalance() {
    // No reason to rebalance a serial sender since all connections are to the same server.
    throw new UnsupportedOperationException();
  }

  /**
   * Handle failover. This method is called when a secondary <code>GatewaySender</code> becomes a
   * primary <code>GatewaySender</code>.
   *
   * Once this secondary becomes the primary, it must:
   * <ul>
   * <li>Remove the queue's CacheListener
   * <li>Process the map of unprocessed events (those it has seen but the previous primary had not
   * yet processed before it crashed). These will include both queued and unqueued events. Remove
   * from the queue any events that were already sent
   * <li>Clear the unprocessed events map
   * </ul>
   */
  protected void handleFailover() {
    /*
     * We must hold this lock while we're processing these maps to prevent us from handling a
     * secondary event while failover occurs. See enqueueEvent
     */
    synchronized (this.unprocessedEventsLock) {
      // Remove the queue's CacheListener
      this.queue.removeCacheListener();
      this.unprocessedTokens = null;

      // Process the map of unprocessed events
      logger.info(LocalizedMessage.create(
          LocalizedStrings.GatewayImpl_GATEWAY_FAILOVER_INITIATED_PROCESSING_0_UNPROCESSED_EVENTS,
          this.unprocessedEvents.size()));
      GatewaySenderStats statistics = this.sender.getStatistics();
      if (!this.unprocessedEvents.isEmpty()) {
        // do a reap for bug 37603
        reapOld(statistics, true); // to get rid of timed out events
        // now iterate over the region queue to figure out what unprocessed
        // events are already in the queue
        {
          Iterator it = this.queue.getRegion().values().iterator();
          while (it.hasNext() && !stopped()) {
            Object o = it.next();
            if (o != null && o instanceof GatewaySenderEventImpl) {
              GatewaySenderEventImpl ge = (GatewaySenderEventImpl) o;
              EventWrapper unprocessedEvent = this.unprocessedEvents.remove(ge.getEventId());
              if (unprocessedEvent != null) {
                unprocessedEvent.event.release();
                if (this.unprocessedEvents.isEmpty()) {
                  break;
                }
              }
            }
          }
        }
        // now for every unprocessed event add it to the end of the queue
        {
          Iterator<Map.Entry<EventID, EventWrapper>> it =
              this.unprocessedEvents.entrySet().iterator();
          while (it.hasNext()) {
            if (stopped())
              break;
            Map.Entry<EventID, EventWrapper> me = it.next();
            EventWrapper ew = me.getValue();
            GatewaySenderEventImpl gatewayEvent = ew.event;
            // Initialize each gateway event. This initializes the key,
            // value
            // and callback arg based on the EntryEvent.
            // TODO:wan70, remove dependencies from old code
            gatewayEvent.initialize();
            // Verify that they GatewayEventCallbackArgument is initialized.
            // If not, initialize it. It won't be initialized if a client to
            // this GatewayHub VM was the creator of this event. This Gateway
            // will be the first one to process it. If will be initialized if
            // this event was sent to this Gateway from another GatewayHub
            // (either directly or indirectly).
            GatewaySenderEventCallbackArgument seca = gatewayEvent.getSenderCallbackArgument();
            if (seca.getOriginatingDSId() == GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID) {
              seca.setOriginatingDSId(sender.getMyDSId());
              seca.initializeReceipientDSIds(Collections.singletonList(sender.getRemoteDSId()));
            }
            it.remove();
            boolean queuedEvent = false;
            try {
              queuedEvent = queuePrimaryEvent(gatewayEvent);
            } catch (IOException ex) {
              if (!stopped()) {
                logger.warn(
                    LocalizedMessage.create(
                        LocalizedStrings.GatewayImpl_EVENT_DROPPED_DURING_FAILOVER_0, gatewayEvent),
                    ex);
              }
            } catch (CacheException ex) {
              if (!stopped()) {
                logger.warn(
                    LocalizedMessage.create(
                        LocalizedStrings.GatewayImpl_EVENT_DROPPED_DURING_FAILOVER_0, gatewayEvent),
                    ex);
              }
            } finally {
              if (!queuedEvent) {
                gatewayEvent.release();
              }
            }
          }
        }
        // Clear the unprocessed events map
        statistics.clearUnprocessedMaps();
      }

      // Iterate the entire queue and mark all events as possible
      // duplicate
      logger.info(LocalizedMessage.create(
          LocalizedStrings.GatewayImpl_0__MARKING__1__EVENTS_AS_POSSIBLE_DUPLICATES,
          new Object[] {getSender(), Integer.valueOf(this.queue.size())}));
      Iterator it = this.queue.getRegion().values().iterator();
      while (it.hasNext() && !stopped()) {
        Object o = it.next();
        if (o != null && o instanceof GatewaySenderEventImpl) {
          GatewaySenderEventImpl ge = (GatewaySenderEventImpl) o;
          ge.setPossibleDuplicate(true);
        }
      }

      releaseUnprocessedEvents();
    } // synchronized
  }

  private void releaseUnprocessedEvents() {
    synchronized (this.unprocessedEventsLock) {
      Map<EventID, EventWrapper> m = this.unprocessedEvents;
      if (m != null) {
        for (EventWrapper ew : m.values()) {
          GatewaySenderEventImpl gatewayEvent = ew.event;
          if (logger.isDebugEnabled()) {
            logger.debug("releaseUnprocessedEvents:" + gatewayEvent);
          }
          gatewayEvent.release();
        }
        this.unprocessedEvents = null;
      }
    }
  }

  @Override
  public void closeProcessor() {
    try {
      super.closeProcessor();
    } finally {
      releaseUnprocessedEvents();
    }
  }

  /**
   * Add the input object to the event queue
   */
  @Override
  public void enqueueEvent(EnumListenerEvent operation, EntryEvent event, Object substituteValue)
      throws IOException, CacheException {
    // There is a case where the event is serialized for processing. The
    // region is not
    // serialized along with the event since it is a transient field. I
    // created an
    // intermediate object (GatewayEventImpl) to avoid this since the region
    // name is
    // used in the sendBatch method, and it can't be null. See EntryEventImpl
    // for details.
    GatewaySenderEventImpl senderEvent = null;

    boolean isPrimary = sender.isPrimary();
    if (!isPrimary) {
      // Fix for #40615. We need to check if we've now become the primary
      // while holding the unprocessedEventsLock. This prevents us from failing
      // over while we're processing an event as a secondaryEvent.
      synchronized (unprocessedEventsLock) {
        // Test whether this gateway is the primary.
        if (sender.isPrimary()) {
          isPrimary = true;
        } else {
          // If it is not, create an uninitialized GatewayEventImpl and
          // put it into the map of unprocessed events.
          // 2 Special cases:
          // 1) UPDATE_VERSION_STAMP: only enqueue to primary
          // 2) CME && !originRemote: only enqueue to primary
          if (!(event.getOperation().equals(Operation.UPDATE_VERSION_STAMP)
              || ((EntryEventImpl) event).isConcurrencyConflict() && !event.isOriginRemote())) {
            senderEvent = new GatewaySenderEventImpl(operation, event, substituteValue, false); // OFFHEAP
            handleSecondaryEvent(senderEvent);
          }
        }
      }
    }
    if (isPrimary) {
      Region region = event.getRegion();
      boolean isPDXRegion = (region instanceof DistributedRegion
          && region.getName().equals(PeerTypeRegistration.REGION_NAME));
      if (!isPDXRegion) {
        waitForFailoverCompletion();
      }
      // If it is, create and enqueue an initialized GatewayEventImpl
      senderEvent = new GatewaySenderEventImpl(operation, event, substituteValue); // OFFHEAP ok

      boolean queuedEvent = false;
      try {
        queuedEvent = queuePrimaryEvent(senderEvent);
      } finally {
        // When queuePrimaryEvent() failed with some exception, it could
        // occur after the GatewaySenderEventImpl is put onto the queue.
        // In that case, the GatewaySenderEventImpl could be released here,
        // and IllegalStateException could be thrown if getDeserializedValue is called
        // when the event is accessed through the region queue.
        if (!queuedEvent) {
          GatewaySenderEventImpl.release(senderEvent);
        }
      }
    }
  }

  private boolean queuePrimaryEvent(GatewaySenderEventImpl gatewayEvent)
      throws IOException, CacheException {
    // Queue the event
    GatewaySenderStats statistics = this.sender.getStatistics();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Queueing event ({}): {}", sender.getId(),
          (statistics.getEventsQueued() + 1), gatewayEvent);
    }
    if (!sender.beforeEnqueue(gatewayEvent)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Event {} is not added to queue.", gatewayEvent);
      }
      statistics.incEventsFiltered();
      return false;
    }
    long start = statistics.startTime();
    boolean putDone = false;
    try {
      putDone = this.queue.put(gatewayEvent);
    } catch (InterruptedException e) {
      // Asif Not expected from SingleWriteSingleReadRegionQueue as it does not
      // throw
      // InterruptedException. But since both HARegionQueue and
      // SingleReadSingleWriteRegionQueue
      // extend RegionQueue , it has to handle InterruptedException
      Thread.currentThread().interrupt();
      getSender().getCancelCriterion().checkCancelInProgress(e);
    }
    statistics.endPut(start);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Queued event ({}): {}", sender.getId(), (statistics.getEventsQueued()),
          gatewayEvent);
    }
    // this._logger.warning(getGateway() + ": Queued event (" +
    // (statistics.getEventsQueued()) + "): " + gatewayEvent + " queue size: "
    // + this._eventQueue.size());
    /*
     * FAILOVER TESTING CODE System.out.println(getName() + ": Queued event (" +
     * (statistics.getEventsQueued()) + "): " + gatewayEvent.getId());
     */
    int queueSize = eventQueueSize();
    statistics.incQueueSize(1);
    if (!this.eventQueueSizeWarning && queueSize >= AbstractGatewaySender.QUEUE_SIZE_THRESHOLD) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.GatewayImpl_0_THE_EVENT_QUEUE_SIZE_HAS_REACHED_THE_THRESHOLD_1,
          new Object[] {sender.getId(),
              Integer.valueOf(AbstractGatewaySender.QUEUE_SIZE_THRESHOLD)}));
      this.eventQueueSizeWarning = true;
    }
    return putDone;
  }

  protected void waitForFailoverCompletion() {
    synchronized (this.failoverCompletedLock) {
      if (this.failoverCompleted) {
        return;
      }
      logger.info(LocalizedMessage
          .create(LocalizedStrings.GatewayImpl_0__WAITING_FOR_FAILOVER_COMPLETION, this));
      try {
        while (!this.failoverCompleted) {
          this.failoverCompletedLock.wait();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        this.sender.getCache().getCancelCriterion().checkCancelInProgress(ex);
        logger.info(LocalizedMessage.create(
            LocalizedStrings.GatewayImpl_0_DID_NOT_WAIT_FOR_FAILOVER_COMPLETION_DUE_TO_INTERRUPTION,
            this));
      }
    }
  }

  protected void completeFailover() {
    synchronized (this.failoverCompletedLock) {
      this.failoverCompleted = true;
      this.failoverCompletedLock.notifyAll();
    }
  }

  /**
   * Update an unprocessed event in the unprocessed events map. This method is called by a secondary
   * <code>GatewaySender</code> to store a gateway event until it is processed by a primary
   * <code>GatewaySender</code>. The complexity of this method is the fact that the event could be
   * processed first by either the primary or secondary <code>GatewaySender</code>.
   *
   * If the primary processes the event first, the map will already contain an entry for the event
   * (through
   * {@link org.apache.geode.internal.cache.wan.serial.SerialSecondaryGatewayListener#afterDestroy}
   * ). When the secondary processes the event, it will remove it from the map.
   *
   * If the secondary processes the event first, it will add it to the map. When the primary
   * processes the event (through
   * {@link org.apache.geode.internal.cache.wan.serial.SerialSecondaryGatewayListener#afterDestroy}
   * ), it will then be removed from the map.
   *
   * @param senderEvent The event being processed
   */
  protected void handleSecondaryEvent(GatewaySenderEventImpl senderEvent) {
    basicHandleSecondaryEvent(senderEvent);
  }

  /**
   * Update an unprocessed event in the unprocessed events map. This method is called by a primary
   * <code>Gateway</code> (through
   * {@link org.apache.geode.internal.cache.wan.serial.SerialSecondaryGatewayListener#afterCreate} )
   * to notify the secondary <code>Gateway</code> that an event has been added to the queue. Once an
   * event has been added to the queue, the secondary no longer needs to keep track of it in the
   * unprocessed events map. The complexity of this method is the fact that the event could be
   * processed first by either the primary or secondary <code>Gateway</code>.
   *
   * If the primary processes the event first, the map will not contain an entry for the event. It
   * will be added to the map in this case so that when the secondary processes it, it will know
   * that the primary has already processed it, and it can be safely removed.
   *
   * If the secondary processes the event first, the map will already contain an entry for the
   * event. In this case, the event can be removed from the map.
   *
   * @param gatewayEvent The event being processed
   */
  protected void handlePrimaryEvent(final GatewaySenderEventImpl gatewayEvent) {
    Executor my_executor = this.executor;
    synchronized (listenerObjectLock) {
      if (my_executor == null) {
        // should mean we are now primary
        return;
      }
      my_executor.execute(new Runnable() {
        public void run() {
          basicHandlePrimaryEvent(gatewayEvent);
        }
      });
    }
  }

  /**
   * Called when the primary gets rid of an event from the queue This method added to fix bug 37603
   */
  protected void handlePrimaryDestroy(final GatewaySenderEventImpl gatewayEvent) {
    Executor my_executor = this.executor;
    synchronized (listenerObjectLock) {
      if (my_executor == null) {
        // should mean we are now primary
        return;
      }
      my_executor.execute(new Runnable() {
        public void run() {
          basicHandlePrimaryDestroy(gatewayEvent.getEventId());
        }
      });
    }
  }

  /**
   * Just remove the event from the unprocessed events map if it is present. This method added to
   * fix bug 37603
   */
  protected boolean basicHandlePrimaryDestroy(final EventID eventId) {
    if (this.sender.isPrimary()) {
      // no need to do anything if we have become the primary
      return false;
    }
    GatewaySenderStats statistics = this.sender.getStatistics();
    // Get the event from the map
    synchronized (unprocessedEventsLock) {
      if (this.unprocessedEvents == null)
        return false;
      // now we can safely use the unprocessedEvents field
      EventWrapper ew = this.unprocessedEvents.remove(eventId);
      if (ew != null) {
        ew.event.release();
        statistics.incUnprocessedEventsRemovedByPrimary();
        return true;
      }
    }
    return false;
  }

  protected void basicHandlePrimaryEvent(final GatewaySenderEventImpl gatewayEvent) {
    if (this.sender.isPrimary()) {
      // no need to do anything if we have become the primary
      return;
    }
    GatewaySenderStats statistics = this.sender.getStatistics();
    // Get the event from the map
    synchronized (unprocessedEventsLock) {
      if (this.unprocessedEvents == null)
        return;
      // now we can safely use the unprocessedEvents field
      EventWrapper ew = this.unprocessedEvents.remove(gatewayEvent.getEventId());

      if (ew == null) {
        // first time for the event
        if (logger.isTraceEnabled()) {
          logger.trace("{}: fromPrimary event {} : {}->{} added to unprocessed token map",
              sender.getId(), gatewayEvent.getEventId(), gatewayEvent.getKey(),
              gatewayEvent.getValueAsString(true));
        }
        {
          Long mapValue =
              Long.valueOf(System.currentTimeMillis() + AbstractGatewaySender.TOKEN_TIMEOUT);
          Long oldv = this.unprocessedTokens.put(gatewayEvent.getEventId(), mapValue);
          if (oldv == null) {
            statistics.incUnprocessedTokensAddedByPrimary();
          } else {
            // its ok for oldv to be non-null
            // this shouldn't happen anymore @todo add an assertion here
          }
        }
      } else {
        // already added by secondary (i.e. hub)
        // the secondary
        // gateway has already seen this event, and it can be safely
        // removed (it already was above)
        if (logger.isTraceEnabled()) {
          logger.trace(
              "{}: Primary create/update event {}:{}->{} remove from unprocessed events map",
              sender.getId(), gatewayEvent.getEventId(), gatewayEvent.getKey(),
              gatewayEvent.getValueAsString(true));
        }
        ew.event.release();
        statistics.incUnprocessedEventsRemovedByPrimary();
      }
      reapOld(statistics, false);
    }
  }

  private void basicHandleSecondaryEvent(final GatewaySenderEventImpl gatewayEvent) {
    boolean freeGatewayEvent = true;
    try {
      GatewaySenderStats statistics = this.sender.getStatistics();
      // Get the event from the map

      if (!getSender().getGatewayEventFilters().isEmpty()) {
        try {
          gatewayEvent.initialize();
        } catch (Exception e) {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.GatewayImpl_EVENT_FAILED_TO_BE_INITIALIZED_0, gatewayEvent), e);
        }
        if (!sender.beforeEnqueue(gatewayEvent)) {
          statistics.incEventsFiltered();
          return;
        }
      }
      Assert.assertHoldsLock(unprocessedEventsLock, true);
      Assert.assertTrue(unprocessedEvents != null);
      // @todo add an assertion that !getPrimary()
      // now we can safely use the unprocessedEvents field
      Long v = this.unprocessedTokens.remove(gatewayEvent.getEventId());

      if (v == null) {
        // first time for the event
        if (logger.isTraceEnabled()) {
          logger.trace("{}: fromSecondary event {}:{}->{} added from unprocessed events map",
              sender.getId(), gatewayEvent.getEventId(), gatewayEvent.getKey(),
              gatewayEvent.getValueAsString(true));
        }
        {
          EventWrapper mapValue = new EventWrapper(gatewayEvent);
          EventWrapper oldv = this.unprocessedEvents.put(gatewayEvent.getEventId(), mapValue);
          if (oldv == null) {
            freeGatewayEvent = false;
            statistics.incUnprocessedEventsAddedBySecondary();
          } else {
            // put old one back in
            this.unprocessedEvents.put(gatewayEvent.getEventId(), oldv);
            // already added by secondary (i.e. hub)
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.GatewayImpl_0_THE_UNPROCESSED_EVENTS_MAP_ALREADY_CONTAINED_AN_EVENT_FROM_THE_HUB_1_SO_IGNORING_NEW_EVENT_2,
                new Object[] {sender.getId(), v, gatewayEvent}));
          }
        }
      } else {
        // token already added by primary already removed
        if (logger.isTraceEnabled()) {
          logger.trace("{}: Secondary created event {}:{}->{} removed from unprocessed events map",
              sender.getId(), gatewayEvent.getEventId(), gatewayEvent.getKey(),
              gatewayEvent.getValueAsString(true));
        }
        statistics.incUnprocessedTokensRemovedBySecondary();
      }
      reapOld(statistics, false);
    } finally {
      if (freeGatewayEvent) {
        gatewayEvent.release();
      }
    }
  }

  /**
   * Call to check if a cleanup of tokens needs to be done
   */
  private void reapOld(final GatewaySenderStats statistics, boolean forceEventReap) {
    synchronized (this.unprocessedEventsLock) {
      if (uncheckedCount > REAP_THRESHOLD) { // only check every X events
        uncheckedCount = 0;
        long now = System.currentTimeMillis();
        if (!forceEventReap && this.unprocessedTokens.size() > REAP_THRESHOLD) {
          Iterator<Map.Entry<EventID, Long>> it = this.unprocessedTokens.entrySet().iterator();
          int count = 0;
          while (it.hasNext()) {
            Map.Entry<EventID, Long> me = it.next();
            long meValue = me.getValue().longValue();
            if (meValue <= now) {
              // @todo log fine level message here
              // it has expired so remove it
              it.remove();
              count++;
            } else {
              // all done try again
              break;
            }
          }
          if (count > 0) {
            // statistics.incUnprocessedTokensRemovedByTimeout(count);
          }
        }
        if (forceEventReap || this.unprocessedEvents.size() > REAP_THRESHOLD) {
          Iterator<Map.Entry<EventID, EventWrapper>> it =
              this.unprocessedEvents.entrySet().iterator();
          int count = 0;
          while (it.hasNext()) {
            Map.Entry<EventID, EventWrapper> me = it.next();
            EventWrapper ew = me.getValue();
            if (ew.timeout <= now) {
              // @todo log fine level message here
              // it has expired so remove it
              it.remove();
              ew.event.release();
              count++;
            } else {
              // all done try again
              break;
            }
          }
          if (count > 0) {
            // statistics.incUnprocessedEventsRemovedByTimeout(count);
          }
        }
      } else {
        uncheckedCount++;
      }
    }
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("GatewayEventProcessor[").append("gatewaySenderId=").append(sender.getId())
        .append(";remoteDSId=").append(getSender().getRemoteDSId()).append(";batchSize=")
        .append(getSender().getBatchSize());
    buffer.append("]");
    return buffer.toString();
  }

  /**
   * Initialize the Executor that handles listener events. Only used by non-primary gateway senders
   */
  private void initializeListenerExecutor() {
    // Create the ThreadGroups
    final ThreadGroup loggerGroup =
        LoggingThreadGroup.createThreadGroup("Gateway Listener Group", logger);

    // Create the Executor
    ThreadFactory tf = new ThreadFactory() {
      public Thread newThread(Runnable command) {
        Thread thread = new Thread(loggerGroup, command, "Queued Gateway Listener Thread");
        thread.setDaemon(true);
        return thread;
      }
    };
    LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
    this.executor = new ThreadPoolExecutor(1, 1/* max unused */, 120, TimeUnit.SECONDS, q, tf);
  }

  private void shutdownListenerExecutor() {
    synchronized (listenerObjectLock) {
      if (this.executor != null) {
        this.executor.shutdown();
        this.executor = null;
      }
    }
  }

  @Override
  public void removeCacheListener() {
    this.queue.removeCacheListener();
  }

  @Override
  public void initializeEventDispatcher() {
    if (logger.isDebugEnabled()) {
      logger.debug(" Creating the GatewayEventCallbackDispatcher");
    }
    this.dispatcher = new GatewaySenderEventCallbackDispatcher(this);

  }

  @Override
  protected void enqueueEvent(GatewayQueueEvent event) {
    // @TODO This API hasn't been implemented yet
    throw new UnsupportedOperationException();
  }

  public void sendBatchDestroyOperationForDroppedEvent(EntryEventImpl dropEvent, int index) {
    EntryEventImpl destroyEvent =
        EntryEventImpl.create((LocalRegion) this.queue.getRegion(), Operation.DESTROY, (long) index,
            null/* newValue */, null, false, sender.getCache().getMyId());
    destroyEvent.setEventId(dropEvent.getEventId());
    destroyEvent.disallowOffHeapValues();
    destroyEvent.setTailKey(-1L);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "SerialGatewaySenderEventProcessor sends BatchDestroyOperation to secondary for event {}",
          destroyEvent);
    }

    try {
      BatchDestroyOperation op = new BatchDestroyOperation(destroyEvent);
      op.distribute();
      if (logger.isDebugEnabled()) {
        logger.debug("BatchRemovalThread completed destroy of dropped event {}", dropEvent);
      }
    } catch (Exception ignore) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Exception in sending dropped event could be ignored in order not to interrupt sender starting",
            ignore);
      }
    }
  }

  @Override
  protected void registerEventDroppedInPrimaryQueue(EntryEventImpl droppedEvent) {
    this.getSender().setModifiedEventId(droppedEvent);
    sendBatchDestroyOperationForDroppedEvent(droppedEvent, -1);
  }

  private String printEventIdList(Set<EventID> eventIds) {
    StringBuffer sb = new StringBuffer().append("[").append(
        eventIds.stream().map(entry -> entry.expensiveToString()).collect(Collectors.joining(", ")))
        .append("]");
    return sb.toString();
  }

  public String printUnprocessedEvents() {
    synchronized (this.unprocessedEventsLock) {
      return printEventIdList(this.unprocessedEvents.keySet());
    }
  }

  public String printUnprocessedTokens() {
    synchronized (this.unprocessedEventsLock) {
      return printEventIdList(this.unprocessedTokens.keySet());
    }
  }
}
