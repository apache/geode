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
package org.apache.geode.internal.cache.wan;

import static java.lang.Boolean.TRUE;
import static org.apache.geode.internal.cache.wan.GatewaySenderEventImpl.TransactionMetadataDisposition.EXCLUDE;
import static org.apache.geode.internal.cache.wan.GatewaySenderEventImpl.TransactionMetadataDisposition.INCLUDE;
import static org.apache.geode.internal.cache.wan.GatewaySenderEventImpl.TransactionMetadataDisposition.INCLUDE_LAST_EVENT;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.GemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderQueue;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * EventProcessor responsible for peeking from queue and handling over the events to the dispatcher.
 * The queue could be SerialGatewaySenderQueue or ParallelGatewaySenderQueue or {@link
 * ConcurrentParallelGatewaySenderQueue}. The dispatcher could be either
 * GatewaySenderEventRemoteDispatcher or GatewaySenderEventCallbackDispatcher.
 *
 * @since GemFire 7.0
 */
public abstract class AbstractGatewaySenderEventProcessor extends LoggingThread
    implements GatewaySenderEventProcessor {

  private static final Logger logger = LogService.getLogger();

  protected RegionQueue queue;

  protected GatewaySenderEventDispatcher dispatcher;

  protected final AbstractGatewaySender sender;

  /**
   * An int id used to identify each batch.
   */
  protected int batchId = 0;

  /**
   * A boolean verifying whether this <code>AbstractGatewaySenderEventProcessor</code> is running.
   */
  private volatile boolean isStopped = true;

  /**
   * A boolean verifying whether this <code>AbstractGatewaySenderEventProcessor</code> is paused.
   */
  protected volatile boolean isPaused = false;

  /**
   * A boolean indicating that the dispatcher thread for this
   * <code>AbstractGatewaySenderEventProcessor</code> is now waiting for resuming
   */
  protected boolean isDispatcherWaiting = false;

  /**
   * A lock object used to control pausing this dispatcher
   */
  protected final Object pausedLock = new Object();

  private final Object runningStateLock = new Object();

  /**
   * A boolean verifying whether a warning has already been issued if the event queue has reached a
   * certain threshold.
   */
  protected boolean eventQueueSizeWarning = false;

  private Exception exception;

  private final ThreadsMonitoring threadMonitoring;

  /*
   * The batchIdToEventsMap contains a mapping between batch id and an array of events. The first
   * element of the array is the list of events peeked from the queue. The second element of the
   * array is the list of filtered events. These are the events actually sent.
   */
  private final Map<Integer, List<GatewaySenderEventImpl>[]> batchIdToEventsMap =
      Collections.synchronizedMap(new HashMap<>());
  private final Map<Integer, List<GatewaySenderEventImpl>> batchIdToPDXEventsMap =
      Collections.synchronizedMap(new HashMap<>());

  private final List<GatewaySenderEventImpl> pdxSenderEventsList = new ArrayList<>();
  private final Map<Object, GatewaySenderEventImpl> pdxEventsMap = new HashMap<>();
  private volatile boolean rebuildPdxList = false;

  private volatile boolean resetLastPeekedEvents;

  /**
   * Cumulative count of events dispatched by this event processor.
   */
  private long numEventsDispatched;

  /**
   * The batchSize is the batch size being used by this processor. By default, it is the configured
   * batch size of the GatewaySender. It may be automatically reduced if a MessageTooLargeException
   * occurs.
   */
  private int batchSize;
  private int batchTimeInterval;

  public AbstractGatewaySenderEventProcessor(String string,
      GatewaySender sender, ThreadsMonitoring tMonitoring) {
    super(string);
    this.sender = (AbstractGatewaySender) sender;
    batchSize = sender.getBatchSize();
    batchTimeInterval = sender.getBatchTimeInterval();
    threadMonitoring = tMonitoring;
  }

  public void setExpectedReceiverUniqueId(String uniqueId) {
    sender.setExpectedReceiverUniqueId(uniqueId);
  }

  public String getExpectedReceiverUniqueId() {
    return sender.getExpectedReceiverUniqueId();
  }

  public Object getRunningStateLock() {
    return runningStateLock;
  }

  @Override
  public int getTotalQueueSize() {
    return getQueue().size();
  }

  protected abstract void initializeMessageQueue(String id, boolean cleanQueues);

  public void enqueueEvent(EnumListenerEvent operation, EntryEvent<?, ?> event,
      Object substituteValue) throws IOException, CacheException {
    enqueueEvent(operation, event, substituteValue, false);
  }

  public abstract void enqueueEvent(EnumListenerEvent operation, EntryEvent<?, ?> event,
      Object substituteValue, boolean isLastEventInTransaction) throws IOException, CacheException;


  protected abstract void rebalance();

  public boolean isStopped() {
    return isStopped;
  }

  public void setIsStopped(boolean isStopped) {
    this.isStopped = isStopped;
    if (isStopped) {
      failureLogInterval.clear();
    }
  }

  public boolean isPaused() {
    return isPaused;
  }

  /**
   * @return the queue
   */
  public RegionQueue getQueue() {
    return queue;
  }

  /**
   * Increment the batch id. This method is not synchronized because this dispatcher is the caller
   */
  public void incrementBatchId() {
    // If _batchId + 1 == maximum, then roll over
    if (batchId + 1 == Integer.MAX_VALUE) {
      batchId = -1;
    }
    batchId++;
  }

  /**
   * Reset the batch id. This method is not synchronized because this dispatcher is the caller
   */
  public void resetBatchId() {
    batchId = 0;
    // dont reset first time when first batch is put for dispatch
    // if (this.batchIdToEventsMap.size() == 1) {
    // if (this.batchIdToEventsMap.containsKey(0)) {
    // return;
    // }
    // }
    // this.batchIdToEventsMap.clear();
    resetLastPeekedEvents = true;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    int currentBatchSize = this.batchSize;
    if (batchSize <= 0) {
      this.batchSize = 1;
      logger.warn(
          "Attempting to set the batch size from {} to {} events failed. Instead it was set to 1.",
          new Object[] {currentBatchSize, batchSize});
    } else {
      this.batchSize = batchSize;
      logger.info("Set the batch size from {} to {} events",
          new Object[] {currentBatchSize, this.batchSize});
    }
  }

  protected void setBatchTimeInterval(int batchTimeInterval) {
    this.batchTimeInterval = batchTimeInterval;
  }

  /**
   * Returns the current batch id to be used to identify the next batch.
   *
   * @return the current batch id to be used to identify the next batch
   */
  public int getBatchId() {
    return batchId;
  }

  public boolean isConnectionReset() {
    return resetLastPeekedEvents;
  }

  protected void eventQueueRemove(int size) throws CacheException {
    queue.remove(size);
  }

  public int eventQueueSize() {
    return getQueue() == null ? 0 : getQueue().size();
  }

  public int secondaryEventQueueSize() {
    if (queue == null) {
      return 0;
    }

    // if parallel, get both primary and secondary queues' size, then substract primary queue's size
    if (queue instanceof ConcurrentParallelGatewaySenderQueue) {
      final ConcurrentParallelGatewaySenderQueue concurrentParallelGatewaySenderQueue =
          (ConcurrentParallelGatewaySenderQueue) queue;
      return concurrentParallelGatewaySenderQueue.localSize(true)
          - concurrentParallelGatewaySenderQueue.localSize(false);
    }
    return queue.size();
  }

  protected abstract void registerEventDroppedInPrimaryQueue(EntryEventImpl droppedEvent);

  /**
   * @return the sender
   */
  public AbstractGatewaySender getSender() {
    return sender;
  }

  public void pauseDispatching() {
    if (isPaused) {
      return;
    }
    isPaused = true;
  }

  // merge44957: WHile merging 44957, need this method hence picked up this method from revision
  // 42024.
  public void waitForDispatcherToPause() {
    if (!isPaused) {
      throw new IllegalStateException("Should be trying to pause!");
    }
    boolean interrupted = false;
    synchronized (pausedLock) {
      while (!isDispatcherWaiting && !isStopped() && sender.getSenderAdvisor().isPrimary()) {
        try {
          pausedLock.wait();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  public void resumeDispatching() {
    if (!isPaused) {
      return;
    }
    isPaused = false;

    // Notify thread to resume
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Resumed dispatching", this);
    }
    synchronized (pausedLock) {
      pausedLock.notifyAll();
    }
  }

  protected boolean stopped() {
    if (isStopped) {
      return true;
    }
    return sender.getStopper().isCancelInProgress();
  }

  /**
   * When a batch fails, then this keeps the last time when a failure was logged . We don't want to
   * swamp the logs in retries due to same batch failures.
   */
  private final ConcurrentHashMap<Integer, long[]> failureLogInterval =
      new ConcurrentHashMap<>();

  /**
   * The maximum size of {@link #failureLogInterval} beyond which it will start logging all failure
   * instances. Hopefully this should never happen in practice.
   */
  protected static final int FAILURE_MAP_MAXSIZE = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.FAILURE_MAP_MAXSIZE", 1000000);

  /**
   * The maximum interval for logging failures of the same event in millis.
   */
  protected static final int FAILURE_LOG_MAX_INTERVAL = Integer.getInteger(
      GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.FAILURE_LOG_MAX_INTERVAL", 300000);

  public boolean skipFailureLogging(Integer batchId) {
    boolean skipLogging = false;
    // if map has become large then give up on new events but we don't expect
    // it to become too large in practise
    if (failureLogInterval.size() < FAILURE_MAP_MAXSIZE) {
      // first long in logInterval gives the last time when the log was done,
      // and the second tracks the current log interval to be used which
      // increases exponentially
      // multiple currentTimeMillis calls below may hinder performance
      // but not much to worry about since failures are expected to
      // be an infrequent occurance (and if frequent then we have to skip
      // logging for quite a while in any case)
      long[] logInterval = failureLogInterval.get(batchId);
      if (logInterval == null) {
        logInterval = failureLogInterval.putIfAbsent(batchId,
            new long[] {System.currentTimeMillis(), 1000});
      }
      if (logInterval != null) {
        long currentTime = System.currentTimeMillis();
        if ((currentTime - logInterval[0]) < logInterval[1]) {
          skipLogging = true;
        } else {
          logInterval[0] = currentTime;
          // don't increase logInterval to beyond a limit (5 mins by default)
          if (logInterval[1] <= (FAILURE_LOG_MAX_INTERVAL / 4)) {
            logInterval[1] *= 4;
          }
          // TODO: should the retries be throttled by some sleep here?
        }
      }
    }
    return skipLogging;
  }

  /**
   * After a successful batch execution remove from failure map if present (i.e. if the event had
   * failed on a previous try).
   */
  public boolean removeEventFromFailureMap(Integer batchId) {
    return failureLogInterval.remove(batchId) != null;
  }

  protected void processQueue() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    final boolean isTraceEnabled = logger.isTraceEnabled();

    final GatewaySenderStats statistics = sender.getStatistics();

    if (isDebugEnabled) {
      logger.debug("STARTED processQueue {}", getId());
    }
    // list of the events peeked from queue
    List<GatewaySenderEventImpl> events = null;
    // list of the PDX events which are peeked from pDX region and needs to go acrossthe site
    List<GatewaySenderEventImpl> pdxEventsToBeDispatched = new ArrayList<>();
    // list of filteredList + pdxEventsToBeDispatched events
    List<GatewaySenderEventImpl> eventsToBeDispatched = new ArrayList<>();

    for (;;) {
      if (stopped()) {
        resetLastPeekedEvents = true;
        break;
      }

      try {
        // Check if paused. If so, wait for resumption
        if (isPaused) {
          waitForResumption();
        }

        // Peek a batch
        if (isDebugEnabled) {
          logger.debug("Attempting to peek a batch of {} events", batchSize);
        }
        for (;;) {
          // check before sleeping
          if (stopped()) {
            resetLastPeekedEvents = true;
            if (isDebugEnabled) {
              logger.debug(
                  "GatewaySenderEventProcessor is stopped. Returning without peeking events.");
            }
            break;
          }

          // Check if paused. If so, wait for resumption
          if (isPaused) {
            waitForResumption();
          }

          // sleep a little bit, look for events
          boolean interrupted = Thread.interrupted();
          try {
            if (resetLastPeekedEvents) {
              resetLastPeekedEvents();
              resetLastPeekedEvents = false;
            }

            {
              // Below code was added to consider the case of queue region is
              // destroyed due to userPRs localdestroy or destroy operation.
              // In this case we were waiting for queue region to get created
              // and then only peek from the region queue.
              // With latest change of multiple PR with single ParalleSender, we
              // cant wait for particular regionqueue to get recreated as there
              // will be other region queue from which events can be picked

              /*
               * // Check if paused. If so, wait for resumption if (this.isPaused) {
               * waitForResumption(); }
               *
               * synchronized (this.getQueue()) { // its quite possible that the queue region is //
               * destroyed(userRegion // localdestroy destroys shadow region locally). In this case
               * // better to // wait for shadows region to get recreated instead of keep loop //
               * for peeking events if (this.getQueue().getRegion() == null ||
               * this.getQueue().getRegion().isDestroyed()) { try { this.getQueue().wait();
               * continue; // this continue is important to recheck the // conditions of stop/ pause
               * after the wait of 1 sec } catch (InterruptedException e1) {
               * Thread.currentThread().interrupt(); } } }
               */
            }
            events = queue.peek(batchSize, batchTimeInterval);
          } catch (InterruptedException e) {
            interrupted = true;
            sender.getCancelCriterion().checkCancelInProgress(e);
            continue; // keep trying
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
          if (events.isEmpty()) {
            continue; // nothing to do!
          }

          beforeExecute();
          try {
            // this list is access by ack reader thread so create new every time. #50220

            List<GatewaySenderEventImpl> filteredList = new ArrayList<>(events);

            // If the exception has been set and its cause is an IllegalStateExcetption,
            // remove all events whose serialized value is no longer available
            if (exception != null && exception.getCause() != null
                && exception.getCause() instanceof IllegalStateException) {
              filteredList.removeIf(GatewaySenderEventImpl::isSerializedValueNotAvailable);
              exception = null;
            }

            // Filter the events
            for (GatewayEventFilter filter : sender.getGatewayEventFilters()) {
              Iterator<GatewaySenderEventImpl> itr = filteredList.iterator();
              while (itr.hasNext()) {
                GatewayQueueEvent<?, ?> event = itr.next();

                // This seems right place to prevent transmission of UPDATE_VERSION events if
                // receiver's version is < 7.0.1, especially to prevent another loop over events.
                if (event.getOperation() == Operation.UPDATE_VERSION_STAMP) {
                  if (isTraceEnabled) {
                    logger.trace(
                        "Update Event Version event: {} removed from Gateway Sender queue: {}",
                        event, sender);
                  }

                  itr.remove();
                  statistics.incEventsNotQueued();
                  continue;
                }

                boolean transmit = filter.beforeTransmit(event);
                if (!transmit) {
                  if (isDebugEnabled) {
                    logger.debug("{}: Did not transmit event due to filtering: {}", sender.getId(),
                        event);
                  }
                  itr.remove();
                  statistics.incEventsFiltered();
                }
              }
            }

            // filter out the events with CME
            Iterator<GatewaySenderEventImpl> cmeItr = filteredList.iterator();
            while (cmeItr.hasNext()) {
              GatewaySenderEventImpl event = cmeItr.next();
              if (event.isConcurrencyConflict()) {
                cmeItr.remove();
                logger.debug("The CME event: {} is removed from Gateway Sender queue: {}", event,
                    sender);
                statistics.incEventsNotQueued();
              }
            }

            /*
             * if (filteredList.isEmpty()) { eventQueueRemove(events.size()); continue; }
             */

            // if the bucket becomes secondary after the event is picked from it,
            // check again before dispatching the event. Do this only for
            // AsyncEventQueue since possibleDuplicate flag is not used in WAN.
            if (getSender().isParallel()
                && (getDispatcher() instanceof GatewaySenderEventCallbackDispatcher)) {
              for (final GatewaySenderEventImpl event : filteredList) {
                final PartitionedRegion qpr;
                if (getQueue() instanceof ConcurrentParallelGatewaySenderQueue) {
                  qpr = ((ConcurrentParallelGatewaySenderQueue) getQueue())
                      .getRegion(event.getRegionPath());
                } else {
                  qpr = ((ParallelGatewaySenderQueue) getQueue())
                      .getRegion(event.getRegionPath());
                }
                int bucketId = event.getBucketId();
                // if the bucket from which the event has been picked is no longer
                // primary, then set possibleDuplicate to true on the event
                if (qpr != null) {
                  BucketRegion bucket = qpr.getDataStore().getLocalBucketById(bucketId);
                  if (bucket == null || !bucket.getBucketAdvisor().isPrimary()) {
                    event.setPossibleDuplicate(true);
                    if (isDebugEnabled) {
                      logger.debug(
                          "Bucket id: {} is no longer primary on this node. The event: {} will be dispatched from this node with possibleDuplicate set to true.",
                          bucketId, event);
                    }
                  }
                }
              }
            }

            eventsToBeDispatched.clear();
            if (!(dispatcher instanceof GatewaySenderEventCallbackDispatcher)) {
              // store the batch before dispatching so it can be retrieved by the ack thread.
              List<GatewaySenderEventImpl>[] eventsArr = uncheckedCast(new List[2]);
              eventsArr[0] = events;
              eventsArr[1] = filteredList;
              batchIdToEventsMap.put(getBatchId(), eventsArr);
              // find out PDX event and append it in front of the list
              pdxEventsToBeDispatched = addPDXEvent();
              eventsToBeDispatched.addAll(pdxEventsToBeDispatched);
              if (!pdxEventsToBeDispatched.isEmpty()) {
                batchIdToPDXEventsMap.put(getBatchId(), pdxEventsToBeDispatched);
              }
            }

            eventsToBeDispatched.addAll(filteredList);

            // Conflate the batch. Event conflation only occurs on the queue.
            // Once an event has been peeked into a batch, it won't be
            // conflated. So if events go through the queue quickly (as in the
            // no-ack case), then multiple events for the same key may end up in
            // the batch.
            List<GatewaySenderEventImpl> conflatedEventsToBeDispatched =
                conflate(eventsToBeDispatched);

            if (isDebugEnabled) {
              logBatchFine("During normal processing, dispatching the following ",
                  conflatedEventsToBeDispatched);
            }

            boolean success = dispatcher.dispatchBatch(conflatedEventsToBeDispatched,
                sender.isRemoveFromQueueOnException(), false);
            if (success) {
              if (isDebugEnabled) {
                logger.debug(
                    "During normal processing, successfully dispatched {} events (batch #{})",
                    conflatedEventsToBeDispatched.size(), getBatchId());
              }
              removeEventFromFailureMap(getBatchId());
            } else {
              if (!skipFailureLogging(getBatchId())) {
                logger.warn(
                    "During normal processing, unsuccessfully dispatched {} events (batch #{})",
                    new Object[] {filteredList.size(), getBatchId()});
              }
            }
            // check again, don't do post-processing if we're stopped.
            if (stopped()) {
              resetLastPeekedEvents = true;
              break;
            }

            // If the batch is successfully processed, remove it from the queue.
            if (success) {
              if (dispatcher instanceof GatewaySenderEventCallbackDispatcher) {
                handleSuccessfulBatchDispatch(conflatedEventsToBeDispatched, events);
              } else {
                incrementBatchId();
              }

              // pdx related gateway sender events needs to be updated for
              // isDispatched
              for (GatewaySenderEventImpl pdxGatewaySenderEvent : pdxEventsToBeDispatched) {
                pdxGatewaySenderEvent.isDispatched = true;
              }

              increaseNumEventsDispatched(conflatedEventsToBeDispatched.size());
            } // successful batch
            else { // The batch was unsuccessful.
              if (dispatcher instanceof GatewaySenderEventCallbackDispatcher) {
                handleUnSuccessfulBatchDispatch(events);
                resetLastPeekedEvents = true;
              } else {
                handleUnSuccessfulBatchDispatch(events);
                if (!resetLastPeekedEvents) {
                  while (!dispatcher.dispatchBatch(conflatedEventsToBeDispatched,
                      sender.isRemoveFromQueueOnException(), true)) {
                    if (isDebugEnabled) {
                      logger.debug(
                          "During normal processing, unsuccessfully dispatched {} events (batch #{})",
                          conflatedEventsToBeDispatched.size(), getBatchId());
                    }
                    if (stopped()) {
                      resetLastPeekedEvents = true;
                      break;
                    }
                    if (resetLastPeekedEvents) {
                      break;
                    }
                    try {
                      if (threadMonitoring != null) {
                        threadMonitoring.updateThreadStatus();
                      }
                      Thread.sleep(100);
                    } catch (InterruptedException ie) {
                      Thread.currentThread().interrupt();
                    }
                  }
                  if (!resetLastPeekedEvents) {
                    incrementBatchId();
                  }
                }
              }
            } // unsuccessful batch
            if (logger.isDebugEnabled()) {
              logger.debug("Finished processing events (batch #{})", (getBatchId() - 1));
            }
          } finally {
            afterExecute();
          }
        } // for
      } catch (RegionDestroyedException e) {
        // setting this flag will ensure that already peeked events will make
        // it to the next batch before new events are peeked (fix for #48784)
        resetLastPeekedEvents = true;
        // most possible case is ParallelWan when user PR is locally destroyed
        // shadow PR is also locally destroyed
        if (logger.isDebugEnabled()) {
          logger.debug("Observed RegionDestroyedException on Queue's region.");
        }
      } catch (CancelException e) {
        logger.debug("Caught cancel exception", e);
        setIsStopped(true);
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

        // Well, OK. Some strange nonfatal thing.
        if (stopped()) {
          return; // don't complain, just exit.
        }

        if (events != null) {
          handleUnSuccessfulBatchDispatch(events);
        }
        resetLastPeekedEvents = true;

        if (e instanceof GatewaySenderException) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException || e instanceof GatewaySenderConfigurationException) {
            continue;
          }
        }

        // We'll log it but continue on with the next batch.
        logger.warn("An Exception occurred. The dispatcher will continue.",
            e);
      }
    } // for
  }

  public List<GatewaySenderEventImpl> conflate(List<GatewaySenderEventImpl> events) {
    final List<GatewaySenderEventImpl> conflatedEvents;
    // Conflate the batch if necessary
    if (sender.isBatchConflationEnabled() && events.size() > 1) {
      if (logger.isDebugEnabled()) {
        logEvents("original", events);
      }
      Map<ConflationKey, GatewaySenderEventImpl> conflatedEventsMap =
          new LinkedHashMap<>();
      for (GatewaySenderEventImpl gsEvent : events) {
        // Determine whether the event should be conflated.
        if (gsEvent.shouldBeConflated()) {
          // The event should be conflated. Create the conflation key
          // (comprised of the event's region, key and the operation).
          ConflationKey key = new ConflationKey(gsEvent.getRegionPath(),
              gsEvent.getKeyToConflate(), gsEvent.getOperation());

          // Get the entry at that key
          GatewaySenderEventImpl existingEvent = conflatedEventsMap.get(key);
          if (!gsEvent.equals(existingEvent)) {
            // Attempt to remove the key. If the entry is removed, that means a
            // duplicate key was found. If not, this is a no-op.
            conflatedEventsMap.remove(key);

            // Add the key to the end of the map.
            conflatedEventsMap.put(key, gsEvent);
          }
        } else {
          // The event should not be conflated (create or destroy). Add it to
          // the map.
          ConflationKey key = new ConflationKey(gsEvent.getRegionPath(),
              gsEvent.getKeyToConflate(), gsEvent.getOperation(), gsEvent.getEventId());
          conflatedEventsMap.put(key, gsEvent);
        }
      }

      // Iterate the map and add the events to the conflated events list
      conflatedEvents = new ArrayList<>(conflatedEventsMap.values());

      // Increment the events conflated from batches statistic
      sender.getStatistics()
          .incEventsConflatedFromBatches(events.size() - conflatedEvents.size());
      if (logger.isDebugEnabled()) {
        logEvents("conflated", conflatedEvents);
      }
    } else {
      conflatedEvents = events;
    }
    return conflatedEvents;
  }

  private void logEvents(String message, List<GatewaySenderEventImpl> events) {
    StringBuilder builder = new StringBuilder();
    builder.append("The batch contains the following ").append(events.size()).append(" ")
        .append(message).append(" events:");
    for (GatewaySenderEventImpl event : events) {
      builder.append("\t\n").append(event.toSmallString());
    }
    logger.debug(builder);
  }

  private List<GatewaySenderEventImpl> addPDXEvent() throws IOException {
    List<GatewaySenderEventImpl> pdxEventsToBeDispatched = new ArrayList<>();

    // getPDXRegion
    InternalCache cache = sender.getCache();
    Region<Object, Object> pdxRegion = cache.getRegion(PeerTypeRegistration.REGION_NAME);

    if (rebuildPdxList) {
      pdxEventsMap.clear();
      pdxSenderEventsList.clear();
      rebuildPdxList = false;
    }

    // find out the list of the PDXEvents which needs to be send across remote
    // site
    // these events will be added to list pdxSenderEventsList. I am expecting
    // that PDX events will be only added to PDX region. no deletion happens on
    // PDX region
    if (pdxRegion != null && pdxRegion.size() != pdxEventsMap.size()) {
      for (Map.Entry<Object, Object> typeEntry : pdxRegion.entrySet()) {
        if (!pdxEventsMap.containsKey(typeEntry.getKey())) {
          // event should never be off-heap so it does not need to be released
          EntryEventImpl event = EntryEventImpl.create((LocalRegion) pdxRegion, Operation.UPDATE,
              typeEntry.getKey(), typeEntry.getValue(), null, false, cache.getMyId());
          event.disallowOffHeapValues();
          event.setEventId(new EventID(cache.getInternalDistributedSystem()));
          List<Integer> allRemoteDSIds = new ArrayList<>();
          for (GatewaySender sender : cache.getGatewaySenders()) {
            allRemoteDSIds.add(sender.getRemoteDSId());
          }
          GatewaySenderEventCallbackArgument geCallbackArg = new GatewaySenderEventCallbackArgument(
              event.getRawCallbackArgument(), sender.getMyDSId(), allRemoteDSIds);
          event.setCallbackArgument(geCallbackArg);
          // OFFHEAP: event for pdx type meta data so it should never be off-heap
          GatewaySenderEventImpl pdxSenderEvent =
              new GatewaySenderEventImpl(EnumListenerEvent.AFTER_UPDATE, event, null);

          pdxEventsMap.put(typeEntry.getKey(), pdxSenderEvent);
          pdxSenderEventsList.add(pdxSenderEvent);
        }
      }
    }

    Iterator<GatewaySenderEventImpl> iterator = pdxSenderEventsList.iterator();
    while (iterator.hasNext()) {
      GatewaySenderEventImpl pdxEvent = iterator.next();
      if (pdxEvent.isAcked) {
        // Since this is acked, it means it has reached to remote site.Dont add
        // to pdxEventsToBeDispatched
        iterator.remove();
        continue;
      }
      if (pdxEvent.isDispatched) {
        // Dispacther does not mean that event has reched remote site. We may
        // need to send it agian if there is porblem while receiveing ack
        // containing this event.Dont add to pdxEventsToBeDispatched
        continue;
      }
      pdxEventsToBeDispatched.add(pdxEvent);
    }

    if (!pdxEventsToBeDispatched.isEmpty() && logger.isDebugEnabled()) {
      logger.debug("List of PDX Event to be dispatched : {}", pdxEventsToBeDispatched);
    }

    // add all these pdx events before filtered events
    return pdxEventsToBeDispatched;
  }

  /**
   * Mark all PDX types as requiring dispatch so that they will be sent over the connection again.
   */
  public void checkIfPdxNeedsResend(int remotePdxSize) {
    InternalCache cache = sender.getCache();
    Region<Object, Object> pdxRegion = cache.getRegion(PeerTypeRegistration.REGION_NAME);

    // The peer has not seen all of our PDX types. This may be because
    // they have been lost on the remote side. Resend the PDX types.
    if (pdxRegion != null && pdxRegion.size() > remotePdxSize) {
      rebuildPdxList = true;
    }
  }

  private void resetLastPeekedEvents() {
    batchIdToEventsMap.clear();
    // make sure that when there is problem while receiving ack, pdx gateway
    // sender events isDispatched is set to false so that same events will be
    // dispatched in next batch
    for (Map.Entry<Integer, List<GatewaySenderEventImpl>> entry : batchIdToPDXEventsMap
        .entrySet()) {
      for (GatewaySenderEventImpl event : entry.getValue()) {
        event.isDispatched = false;
      }
    }
    batchIdToPDXEventsMap.clear();
    if (queue instanceof SerialGatewaySenderQueue) {
      ((SerialGatewaySenderQueue) queue).resetLastPeeked();
    } else if (queue instanceof ParallelGatewaySenderQueue) {
      ((ParallelGatewaySenderQueue) queue).resetLastPeeked();
    } else {
      // we will never come here
      throw new RuntimeException("resetLastPeekedEvents : no matching queue found " + this);
    }
  }

  private void handleSuccessfulBatchDispatch(List<?> filteredList,
      List<GatewaySenderEventImpl> events) {
    if (filteredList != null) {
      for (GatewayEventFilter filter : sender.getGatewayEventFilters()) {
        for (Object o : filteredList) {
          if (o instanceof GatewaySenderEventImpl) {
            try {
              filter.afterAcknowledgement((GatewaySenderEventImpl) o);
            } catch (Exception e) {
              logger.fatal(String.format(
                  "Exception occurred while handling call to %s.afterAcknowledgement for event %s:",
                  filter.toString(), o), e);
            }
          }
        }
      }
    }

    filteredList.clear();
    eventQueueRemove(events.size());

    logThresholdExceededAlerts(events);

    int queueSize = eventQueueSize();

    if (eventQueueSizeWarning && queueSize <= AbstractGatewaySender.QUEUE_SIZE_THRESHOLD) {
      logger.info("The event queue size has dropped below {} events.",
          AbstractGatewaySender.QUEUE_SIZE_THRESHOLD);
      eventQueueSizeWarning = false;
    }
    incrementBatchId();

  }

  private void handleUnSuccessfulBatchDispatch(List<?> events) {
    final GatewaySenderStats statistics = sender.getStatistics();
    statistics.incBatchesRedistributed();

    // Set posDup flag on each event in the batch
    Iterator<?> it = events.iterator();
    while (it.hasNext() && !isStopped) {
      Object o = it.next();
      if (o instanceof GatewaySenderEventImpl) {
        GatewaySenderEventImpl ge = (GatewaySenderEventImpl) o;
        ge.setPossibleDuplicate(true);
      }
    }
  }

  /**
   * In case of BatchException we expect that the dispatcher has removed all the events till the
   * event that threw BatchException.
   */
  public void handleException() {
    final GatewaySenderStats statistics = sender.getStatistics();
    statistics.incBatchesRedistributed();
    resetLastPeekedEvents = true;
  }

  public void handleSuccessBatchAck(int batchId) {
    // this is to acknowledge PDX related events
    List<GatewaySenderEventImpl> pdxEvents = batchIdToPDXEventsMap.remove(batchId);
    if (pdxEvents != null) {
      for (GatewaySenderEventImpl senderEvent : pdxEvents) {
        senderEvent.isAcked = true;
      }
    }

    List<GatewaySenderEventImpl>[] eventsArr = batchIdToEventsMap.remove(batchId);
    if (eventsArr != null) {
      List<GatewaySenderEventImpl> filteredEvents = eventsArr[1];
      for (GatewayEventFilter filter : sender.getGatewayEventFilters()) {
        for (GatewaySenderEventImpl event : filteredEvents) {
          try {
            filter.afterAcknowledgement(event);
          } catch (Exception e) {
            logger.fatal(String.format(
                "Exception occurred while handling call to %s.afterAcknowledgement for event %s:",
                filter.toString(), event),
                e);
          }
        }
      }
      List<GatewaySenderEventImpl> events = eventsArr[0];
      if (logger.isDebugEnabled()) {
        logger.debug("Removing events from the queue {}", events.size());
      }
      eventQueueRemove(events.size());

      logThresholdExceededAlerts(events);
    }
  }

  protected void logThresholdExceededAlerts(List<GatewaySenderEventImpl> events) {
    // Log an alert for each event if necessary
    if (getSender().getAlertThreshold() > 0) {
      long currentTime = System.currentTimeMillis();
      for (GatewaySenderEventImpl event : events) {
        try {
          if (event.getCreationTime() + getSender().getAlertThreshold() < currentTime) {
            logger.warn(
                "{} event for region={} key={} value={} was in the queue for {} milliseconds",
                new Object[] {event.getOperation(), event.getRegionPath(), event.getKey(),
                    event.getValueAsString(true), currentTime - event.getCreationTime()});
            getSender().getStatistics().incEventsExceedingAlertThreshold();
          }
        } catch (Exception e) {
          logger.warn("Caught the following exception attempting to log threshold exceeded alert:",
              e);
          getSender().getStatistics().incEventsExceedingAlertThreshold();
        }
      }
    }
  }

  protected void waitForResumption() throws InterruptedException {
    synchronized (pausedLock) {
      if (!isPaused) {
        return;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("GatewaySenderEventProcessor is paused. Waiting for Resumption");
      }
      isDispatcherWaiting = true;
      pausedLock.notifyAll();
      while (isPaused) {
        pausedLock.wait();
      }
      isDispatcherWaiting = false;
    }
  }

  public abstract void initializeEventDispatcher();

  public GatewaySenderEventDispatcher getDispatcher() {
    return dispatcher;
  }

  public Map<Integer, List<GatewaySenderEventImpl>[]> getBatchIdToEventsMap() {
    return batchIdToEventsMap;
  }

  public Map<Integer, List<GatewaySenderEventImpl>> getBatchIdToPDXEventsMap() {
    return batchIdToPDXEventsMap;
  }

  @Override
  public void run() {
    try {
      setRunningStatus();
      processQueue();
    } catch (CancelException e) {
      if (!isStopped()) {
        logger.info("A cancellation occurred. Stopping the dispatcher.");
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
      logger.fatal("Message dispatch failed due to unexpected exception..", e);
    }
  }

  public void setRunningStatus() throws Exception {
    GemFireException ex = null;
    try {
      initializeEventDispatcher();
    } catch (GemFireException e) {
      ex = e;
    }
    synchronized (runningStateLock) {
      if (ex != null) {
        setException(ex);
        setIsStopped(true);
      } else {
        setIsStopped(false);
      }
      runningStateLock.notifyAll();
    }
    if (ex != null) {
      throw ex;
    }
  }

  public void setException(GemFireException ex) {
    exception = ex;
  }

  public Exception getException() {
    return exception;
  }

  /**
   * Stops the dispatcher from dispatching events . The dispatcher will stay alive for a predefined
   * time OR until its queue is empty.
   *
   * @see AbstractGatewaySender#MAXIMUM_SHUTDOWN_WAIT_TIME
   */
  public void stopProcessing() {

    if (!isAlive()) {
      return;
    }
    resumeDispatching();

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Notifying the dispatcher to terminate", this);
    }

    // If this is the primary, stay alive for a predefined time
    // OR until the queue becomes empty
    if (sender.isPrimary()) {
      if (AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME == -1) {
        try {
          while (!(queue.size() == 0)) {
            Thread.sleep(5000);
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Waiting for the queue to get empty.", this);
            }
          }
        } catch (InterruptedException e) {
          // interrupted
        } catch (CancelException e) {
          // cancelled
        }
      } else {
        try {
          Thread.sleep(AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME * 1000L);
        } catch (InterruptedException e) {/* ignore */
          // interrupted
        }
      }
    } else {
      sender.getSenderAdvisor().notifyPrimaryLock();
    }

    setIsStopped(true);
    dispatcher.stop();

    if (isAlive()) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Joining with the dispatcher thread upto limit of 5 seconds", this);
      }
      try {
        join(5000); // wait for our thread to stop
        if (isAlive()) {
          logger.warn("{}:Dispatcher still alive even after join of 5 seconds.",
              this);
          // if the server machine crashed or there was a nic failure, we need
          // to terminate the socket connection now to avoid a hang when closing
          // the connections later
          // try to stop it again
          dispatcher.stop();
          batchIdToEventsMap.clear();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        logger.warn("{}: InterruptedException in joining with dispatcher thread.",
            this);
      }
    }

    closeProcessor();

    if (logger.isDebugEnabled()) {
      logger.debug("Stopped dispatching: {}", this);
    }
  }

  public void closeProcessor() {
    if (logger.isDebugEnabled()) {
      logger.debug("Closing dispatcher");
    }
    try {
      if (sender.isPrimary() && queue.size() > 0) {
        logger.warn("Destroying GatewayEventDispatcher with actively queued data.");
      }
      if (resetLastPeekedEvents) {
        resetLastPeekedEvents();
        resetLastPeekedEvents = false;
      }
    } catch (RegionDestroyedException | CancelException | CacheException ignore) {
    } finally {
      queue.close();
      if (logger.isDebugEnabled()) {
        logger.debug("Closed dispatcher");
      }
    }
  }

  protected GatewaySenderEventImpl.TransactionMetadataDisposition getTransactionMetadataDisposition(
      final boolean isLastEventInTransaction) {
    if (getSender().mustGroupTransactionEvents()) {
      if (isLastEventInTransaction) {
        return INCLUDE_LAST_EVENT;
      }
      return INCLUDE;
    } else {
      return EXCLUDE;
    }
  }

  public void removeCacheListener() {}

  /**
   * Logs a batch of events.
   *
   * @param events The batch of events to log
   **/
  public void logBatchFine(String message, List<GatewaySenderEventImpl> events) {
    if (events != null) {
      StringBuffer buffer = new StringBuffer();
      buffer.append(message);
      buffer.append(events.size()).append(" events");
      buffer.append(" (batch #").append(getBatchId());
      buffer.append("):\n");
      for (GatewaySenderEventImpl ge : events) {
        buffer.append("\tEvent ").append(ge.getEventId()).append(":");
        buffer.append(ge.getKey()).append("->");
        buffer.append(ge.getValueAsString(true)).append(",");
        buffer.append(ge.getShadowKey());
        buffer.append("\n");
      }
      logger.debug(buffer);
    }
  }


  public long getNumEventsDispatched() {
    return numEventsDispatched;
  }

  public void increaseNumEventsDispatched(long newEventsDispatched) {
    numEventsDispatched += newEventsDispatched;
  }

  public void clear(PartitionedRegion pr, int bucketId) {
    ((ParallelGatewaySenderQueue) queue).clear(pr, bucketId);
  }

  public void notifyEventProcessorIfRequired(int bucketId) {
    ((ParallelGatewaySenderQueue) queue).notifyEventProcessorIfRequired();
  }

  public BlockingQueue<GatewaySenderEventImpl> getBucketTmpQueue(int bucketId) {
    return ((ParallelGatewaySenderQueue) queue).getBucketToTempQueueMap().get(bucketId);
  }

  public PartitionedRegion getRegion(String prRegionName) {
    return ((ParallelGatewaySenderQueue) queue).getRegion(prRegionName);
  }

  public void removeShadowPR(String prRegionName) {
    ((ParallelGatewaySenderQueue) queue).removeShadowPR(prRegionName);
  }

  public void conflateEvent(Conflatable conflatableObject, int bucketId, Long tailKey) {
    ((ParallelGatewaySenderQueue) queue).conflateEvent(conflatableObject, bucketId, tailKey);
  }

  public void addShadowPartitionedRegionForUserPR(PartitionedRegion pr) {
    ((ParallelGatewaySenderQueue) queue).addShadowPartitionedRegionForUserPR(pr);
  }

  public void addShadowPartitionedRegionForUserRR(DistributedRegion userRegion) {
    ((ParallelGatewaySenderQueue) queue).addShadowPartitionedRegionForUserRR(userRegion);
  }

  protected void beforeExecute() {
    if (threadMonitoring != null) {
      threadMonitoring.startMonitor(ThreadsMonitoring.Mode.AGSExecutor);
    }
  }

  protected void afterExecute() {
    if (threadMonitoring != null) {
      threadMonitoring.endMonitor();
    }
  }

  protected abstract void enqueueEvent(GatewayQueueEvent<?, ?> event);

  protected static class SenderStopperCallable implements Callable<Boolean> {
    private final AbstractGatewaySenderEventProcessor processor;

    /**
     * Need the processor to stop.
     */
    public SenderStopperCallable(AbstractGatewaySenderEventProcessor processor) {
      this.processor = processor;
    }

    @Override
    public Boolean call() {
      processor.stopProcessing();
      return TRUE;
    }
  }

  private static class ConflationKey {
    private final Object key;

    private final Operation operation;

    private final String regionName;

    private final EventID eventId;

    private ConflationKey(String region, Object key, Operation operation) {
      this(region, key, operation, null);
    }

    private ConflationKey(String region, Object key, Operation operation, EventID eventId) {
      this.key = key;
      this.operation = operation;
      regionName = region;
      this.eventId = eventId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + key.hashCode();
      result = prime * result + operation.hashCode();
      result = prime * result + regionName.hashCode();
      result = prime * result + (eventId == null ? 0 : eventId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ConflationKey that = (ConflationKey) obj;
      if (!regionName.equals(that.regionName)) {
        return false;
      }
      if (!key.equals(that.key)) {
        return false;
      }
      if (!operation.equals(that.operation)) {
        return false;
      }
      return Objects.equals(eventId, that.eventId);
    }
  }

  public String printUnprocessedEvents() {
    return null;
  }

  public String printUnprocessedTokens() {
    return null;
  }

}
