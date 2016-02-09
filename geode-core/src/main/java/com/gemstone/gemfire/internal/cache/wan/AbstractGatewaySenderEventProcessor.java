/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.pooling.ConnectionDestroyedException;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderQueue;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.pdx.internal.PeerTypeRegistration;

/**
 * EventProcessor responsible for peeking from queue and handling over the events
 * to the dispatcher.
 * The queue could be SerialGatewaySenderQueue or ParallelGatewaySenderQueue or
 *  {@link ConcurrentParallelGatewaySenderQueue}.
 * The dispatcher could be either GatewaySenderEventRemoteDispatcher or GatewaySenderEventCallbackDispatcher.
 * 
 * @author Suranjan Kumar
 * @since 7.0
 * 
 */
public abstract class AbstractGatewaySenderEventProcessor extends Thread {

  private static final Logger logger = LogService.getLogger();
  
  public static boolean TEST_HOOK = false;

  protected RegionQueue queue;
  
  protected GatewaySenderEventDispatcher dispatcher;

  protected final AbstractGatewaySender sender;

  /**
   * An int id used to identify each batch.
   */
  protected int batchId = 0;

  /**
   * A boolean verifying whether this <code>AbstractGatewaySenderEventProcessor</code>
   * is running.
   */
  private volatile boolean isStopped = true;

  /**
   * A boolean verifying whether this <code>AbstractGatewaySenderEventProcessor</code>
   * is paused.
   */
  protected volatile boolean isPaused = false;

  /**
   * A boolean indicating that the dispatcher thread for
   * this <code>AbstractGatewaySenderEventProcessor</code>
   * is now waiting for resuming
   */
  protected boolean isDispatcherWaiting = false;
  
  /**
   * A lock object used to control pausing this dispatcher
   */
  protected final Object pausedLock = new Object();

  public final Object runningStateLock = new Object();

  /**
   * A boolean verifying whether a warning has already been issued if the event
   * queue has reached a certain threshold.
   */
  protected boolean eventQueueSizeWarning = false;

  private Exception exception;
  
  /*
   * The batchIdToEventsMap contains a mapping between batch id and an array of
   * events. The first element of the array is the list of events peeked from
   * the queue. The second element of the array is the list of filtered events.
   * These are the events actually sent.
   */
  private Map<Integer, List<GatewaySenderEventImpl>[]> batchIdToEventsMap = Collections
      .synchronizedMap(new HashMap<Integer, List<GatewaySenderEventImpl>[]>());
  private Map<Integer, List<GatewaySenderEventImpl>> batchIdToPDXEventsMap = Collections
      .synchronizedMap(new HashMap<Integer, List<GatewaySenderEventImpl>>());

  private List<GatewaySenderEventImpl> pdxSenderEventsList = new ArrayList<GatewaySenderEventImpl>();
  private Map<Object, GatewaySenderEventImpl> pdxEventsMap = new HashMap<Object,GatewaySenderEventImpl>();
  private volatile boolean rebuildPdxList = false;
  
  private volatile boolean resetLastPeekedEvents;
  
  private long numEventsDispatched;
  
  /**
   * @param createThreadGroup
   * @param string
   */
  public AbstractGatewaySenderEventProcessor(LoggingThreadGroup createThreadGroup,
      String string, GatewaySender sender) {
    super(createThreadGroup, string);
    this.sender = (AbstractGatewaySender)sender;
  }

  abstract protected void initializeMessageQueue(String id);
  
  public abstract void enqueueEvent(EnumListenerEvent operation,
      EntryEvent event, Object substituteValue) throws IOException,
      CacheException;
  
  protected abstract void rebalance();

  public boolean isStopped() {
    return this.isStopped;
  }

  protected void setIsStopped(boolean isStopped) {
    if (isStopped) {
      this.isStopped = true;
      this.failureLogInterval.clear();
    } else {
      this.isStopped = isStopped;
    }
  }

  public boolean isPaused() {
    return this.isPaused;
  }

  /**
   * @return the queue
   */
  public RegionQueue getQueue() {
    return this.queue;
  }

  /**
   * Increment the batch id. This method is not synchronized because this
   * dispatcher is the caller
   */
  public void incrementBatchId() {
    // If _batchId + 1 == maximum, then roll over
    if (this.batchId + 1 == Integer.MAX_VALUE) {
      this.batchId = -1;
    }
    this.batchId++;
  }

  /**
   * Reset the batch id. This method is not synchronized because this dispatcher
   * is the caller
   */
  protected void resetBatchId() {
    this.batchId = 0;
    // dont reset first time when first batch is put for dispatch
    //if (this.batchIdToEventsMap.size() == 1) {
    //  if (this.batchIdToEventsMap.containsKey(0)) {
    //    return;
    //  }
    //}
    //this.batchIdToEventsMap.clear();
    this.resetLastPeekedEvents = true;
  }

  /**
   * Returns the current batch id to be used to identify the next batch.
   * 
   * @return the current batch id to be used to identify the next batch
   */
  protected int getBatchId() {
    return this.batchId;
  }

  protected boolean isConnectionReset() {
    return this.resetLastPeekedEvents;
  }
  
  protected void eventQueueRemove() throws CacheException,
      InterruptedException {
    this.queue.remove();
  }

  protected void eventQueueRemove(int size) throws CacheException {
    this.queue.remove(size);
  }

  protected Object eventQueueTake() throws CacheException, InterruptedException {
    throw new UnsupportedOperationException();
    // No code currently calls this method.
    // To implement it we need to make sure that the callers
    // call freeOffHeapResources on the returned GatewaySenderEventImpl.
    //return this.queue.take();
  }

  public int eventQueueSize() {
    if(queue == null) {
      return 0;
    }
    
    // This should be local size instead of pr size
    if (this.queue instanceof ParallelGatewaySenderQueue) {
      return ((ParallelGatewaySenderQueue) queue).localSize();
    }
    if (this.queue instanceof ConcurrentParallelGatewaySenderQueue) {
      return ((ConcurrentParallelGatewaySenderQueue) queue).localSize();
    } 
    return this.queue.size();
  }
  
  /**
   * @return the sender
   */
  public AbstractGatewaySender getSender() {
    return this.sender;
  }

  public void pauseDispatching() {
    if (this.isPaused) {
      return;
    }
    this.isPaused = true;
  }

  //merge44957: WHile merging 44957, need this method hence picked up this method from revision 42024.
  public void waitForDispatcherToPause() {
    if (!this.isPaused) {
      throw new IllegalStateException("Should be trying to pause!");
    }
    boolean interrupted=false;
    synchronized(this.pausedLock) {
      while(!isDispatcherWaiting && !isStopped()
            && sender.getSenderAdvisor().isPrimary()) {
        try {
          this.pausedLock.wait();
        } catch(InterruptedException e) {
          interrupted = true;
        }
      }
    }
    if(interrupted) {
      Thread.currentThread().interrupt();
    }
  }
  
  public void resumeDispatching() {
    if (!this.isPaused) {
      return;
    }
    this.isPaused = false;

    // Notify thread to resume
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Resumed dispatching", this);
    }
    synchronized (this.pausedLock) {
      this.pausedLock.notifyAll();
    }
  }

  protected boolean stopped() {
    if (this.isStopped) {
      return true;
    }
    if (sender.getStopper().cancelInProgress() != null) {
      return true;
    }
    return false;
  }

  /**
   * When a batch fails, then this keeps the last time when a failure was
   * logged . We don't want to swamp the logs in retries due to same batch failures.
   */
  private final ConcurrentHashMap<Integer, long[]> failureLogInterval =
      new ConcurrentHashMap<Integer, long[]>();
      
  /**
   * The maximum size of {@link #failureLogInterval} beyond which it will start
   * logging all failure instances. Hopefully this should never happen in
   * practice.
   */
  protected static final int FAILURE_MAP_MAXSIZE = Integer.getInteger(
      "gemfire.GatewaySender.FAILURE_MAP_MAXSIZE", 1000000);

  /**
   * The maximum interval for logging failures of the same event in millis.
   */
  protected static final int FAILURE_LOG_MAX_INTERVAL = Integer.getInteger(
      "gemfire.GatewaySender.FAILURE_LOG_MAX_INTERVAL", 300000);
  
  public final boolean skipFailureLogging(Integer batchId) {
    boolean skipLogging = false;
    // if map has become large then give up on new events but we don't expect
    // it to become too large in practise
    if (this.failureLogInterval.size() < FAILURE_MAP_MAXSIZE) {
      // first long in logInterval gives the last time when the log was done,
      // and the second tracks the current log interval to be used which
      // increases exponentially
      // multiple currentTimeMillis calls below may hinder performance
      // but not much to worry about since failures are expected to
      // be an infrequent occurance (and if frequent then we have to skip
      // logging for quite a while in any case)
      long[] logInterval = this.failureLogInterval.get(batchId);
      if (logInterval == null) {
        logInterval = this.failureLogInterval.putIfAbsent(batchId,
            new long[] { System.currentTimeMillis(), 1000 });
      }
      if (logInterval != null) {
        long currentTime = System.currentTimeMillis();
        if ((currentTime - logInterval[0]) < logInterval[1]) {
          skipLogging = true;
        }
        else {
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
   * After a successful batch execution remove from failure map if present (i.e.
   * if the event had failed on a previous try).
   */
  public final boolean removeEventFromFailureMap(Integer batchId) {
    return this.failureLogInterval.remove(batchId) != null;
  }
  
  protected void processQueue() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    final boolean isTraceEnabled = logger.isTraceEnabled();
    
    final int batchSize = sender.getBatchSize();
    final int batchTimeInterval = sender.getBatchTimeInterval();
    final GatewaySenderStats statistics = this.sender.getStatistics();
    
    if (isDebugEnabled) {
      logger.debug("STARTED processQueue {}", this.getId());
    }
    //list of the events peeked from queue
    List<GatewaySenderEventImpl> events = null;
    // list of the above peeked events which are filtered through the filters attached 
    List<GatewaySenderEventImpl> filteredList = new ArrayList<GatewaySenderEventImpl>();
    //list of the PDX events which are peeked from pDX region and needs to go acrossthe site 
    List<GatewaySenderEventImpl> pdxEventsToBeDispatched = new ArrayList<GatewaySenderEventImpl>();
    // list of filteredList + pdxEventsToBeDispatched events
    List<GatewaySenderEventImpl> eventsToBeDispatched = new ArrayList<GatewaySenderEventImpl>();
    
    
    for (;;) {
      if (stopped()) {
        break;
      }

      try {
        // Check if paused. If so, wait for resumption
        if (this.isPaused) {
          waitForResumption();
        }

        // Peek a batch
        if (isDebugEnabled) {
          logger.debug("Attempting to peek a batch of {} events", batchSize);
        }
        for (;;) {
          // check before sleeping
          if (stopped()) {
            if (isDebugEnabled) {
              logger.debug("GatewaySenderEventProcessor is stopped. Returning without peeking events.");
            }
            break;
          }

          // Check if paused. If so, wait for resumption
          if (this.isPaused) {
            waitForResumption();
          }
          // We need to initialize connection in dispatcher before sending first
          // batch here ONLY, because we need GatewayReceiver's version for
          // filtering VERSION_ACTION events from being sent.
          boolean sendUpdateVersionEvents = shouldSendVersionEvents(this.dispatcher);

          
          // sleep a little bit, look for events
          boolean interrupted = Thread.interrupted();
          try {
            if(resetLastPeekedEvents) {
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
              
            /*// Check if paused. If so, wait for resumption
            if (this.isPaused) {
              waitForResumption();
            }

            synchronized (this.getQueue()) {
              // its quite possible that the queue region is
              // destroyed(userRegion
              // localdestroy destroys shadow region locally). In this case
              // better to
              // wait for shadows region to get recreated instead of keep loop
              // for peeking events
              if (this.getQueue().getRegion() == null
                  || this.getQueue().getRegion().isDestroyed()) {
                try {
                  this.getQueue().wait();
                  continue; // this continue is important to recheck the
                            // conditions of stop/ pause after the wait of 1 sec
                }
                catch (InterruptedException e1) {
                  Thread.currentThread().interrupt();
                }
              }
            }*/
            }
            events = this.queue.peek(batchSize, batchTimeInterval);
          } catch (InterruptedException e) {
            interrupted = true;
            this.sender.getCancelCriterion().checkCancelInProgress(e);
            continue; // keep trying
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
          if (events.isEmpty()) {
            continue; // nothing to do!
          }

          //this list is access by ack reader thread so create new every time. #50220
          filteredList = new ArrayList<GatewaySenderEventImpl>();
          
          filteredList.addAll(events);
          for (GatewayEventFilter filter : sender.getGatewayEventFilters()) {
            Iterator<GatewaySenderEventImpl> itr = filteredList.iterator();
            while (itr.hasNext()) {
              GatewayQueueEvent event = itr.next();

              // This seems right place to prevent transmission of UPDATE_VERSION events if receiver's
              // version is < 7.0.1, especially to prevent another loop over events.
              if (!sendUpdateVersionEvents && event.getOperation() == Operation.UPDATE_VERSION_STAMP) {
                if (isTraceEnabled) {
                  logger.trace("Update Event Version event: {} removed from Gateway Sender queue: {}", event, sender);
                }
                
                itr.remove();
                statistics.incEventsNotQueued();
                continue;
              }
              
              boolean transmit = filter.beforeTransmit(event);
              if (!transmit) {
                if (isDebugEnabled) {
                  logger.debug("{}: Did not transmit event due to filtering: {}", sender.getId(), event);
                }
                itr.remove();
                statistics.incEventsFiltered();
              }
            }
          }
          /*if (filteredList.isEmpty()) {
            eventQueueRemove(events.size());
            continue;
          }*/
          
          // if the bucket becomes secondary after the event is picked from it,
          // check again before dispatching the event. Do this only for
          // AsyncEventQueue since possibleDuplicate flag is not used in WAN.
          if (this.getSender().isParallel()
              && (this.getDispatcher() instanceof GatewaySenderEventCallbackDispatcher)) {
            Iterator<GatewaySenderEventImpl> itr = filteredList.iterator();
            while (itr.hasNext()) {
              GatewaySenderEventImpl event = (GatewaySenderEventImpl)itr.next();
              PartitionedRegion qpr = null;
              if (this.getQueue() instanceof ConcurrentParallelGatewaySenderQueue) {
                qpr = ((ConcurrentParallelGatewaySenderQueue)this.getQueue())
                    .getRegion(event.getRegionPath());
              }
              else {
                qpr = ((ParallelGatewaySenderQueue)this.getQueue())
                    .getRegion(event.getRegionPath());
              }
              int bucketId = event.getBucketId();
              // if the bucket from which the event has been picked is no longer
              // primary, then set possibleDuplicate to true on the event
              if (qpr != null) {
                BucketRegion bucket = qpr.getDataStore().getLocalBucketById(
                    bucketId);
                if (bucket == null || !bucket.getBucketAdvisor().isPrimary()) {
                  event.setPossibleDuplicate(true);
                }
              }
              if (isDebugEnabled) {
                logger.debug("Bucket id: {} is no longer primary on this node. The event {} will be dispatched from this node with possibleDuplicate set to true.",
                    bucketId, event);
              }
            }
          }
          
          eventsToBeDispatched.clear();
          if (!(this.dispatcher instanceof GatewaySenderEventCallbackDispatcher)) {
            // store the batch before dispatching so it can be retrieved by the ack thread.
            List<GatewaySenderEventImpl>[] eventsArr = (List<GatewaySenderEventImpl>[]) new List[2];
            eventsArr[0] = events;
            eventsArr[1] = filteredList;
            this.batchIdToEventsMap.put(getBatchId(), eventsArr);
            // find out PDX event and append it in front of the list
            pdxEventsToBeDispatched = addPDXEvent();
            eventsToBeDispatched.addAll(pdxEventsToBeDispatched);
            if (!pdxEventsToBeDispatched.isEmpty()) {
              this.batchIdToPDXEventsMap.put(getBatchId(),
                  pdxEventsToBeDispatched);
            }
          }
          
          eventsToBeDispatched.addAll(filteredList);
          
          // Conflate the batch. Event conflation only occurs on the queue.
          // Once an event has been peeked into a batch, it won't be
          // conflated. So if events go through the queue quickly (as in the
          // no-ack case), then multiple events for the same key may end up in
          // the batch.
          List conflatedEventsToBeDispatched = conflate(eventsToBeDispatched);          
          
          if (isDebugEnabled) {
            logBatchFine("During normal processing, dispatching the following ", conflatedEventsToBeDispatched);
          }
          
          boolean success = this.dispatcher.dispatchBatch(conflatedEventsToBeDispatched,
              sender.isRemoveFromQueueOnException(), false);
          if (success) {
            if (isDebugEnabled) {
              logger.debug("During normal processing, successfully dispatched {} events (batch #{})",
                  conflatedEventsToBeDispatched.size(), getBatchId());
            }
                            removeEventFromFailureMap(getBatchId());
          }
          else {
            if (!skipFailureLogging(getBatchId())) { 
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.GatewayImpl_EVENT_QUEUE_DISPATCH_FAILED, new Object[] { filteredList.size(), getBatchId() }));       
            }
          }
          // check again, don't do post-processing if we're stopped.
          if (stopped()) {
            break;
          }

          // If the batch is successfully processed, remove it from the queue.
          if (success) {
            if (this.dispatcher instanceof GatewaySenderEventCallbackDispatcher) {
              handleSuccessfulBatchDispatch(conflatedEventsToBeDispatched, events);
            } else {
              incrementBatchId();
            }
            
            // pdx related gateway sender events needs to be updated for
            // isDispatched
            for (GatewaySenderEventImpl pdxGatewaySenderEvent : pdxEventsToBeDispatched) {
              pdxGatewaySenderEvent.isDispatched = true;
            }
            if (TEST_HOOK) {
              this.numEventsDispatched += conflatedEventsToBeDispatched.size();
            }
          } // successful batch
          else { // The batch was unsuccessful.
            if (this.dispatcher instanceof GatewaySenderEventCallbackDispatcher) {
              handleUnSuccessfulBatchDispatch(events);
              this.resetLastPeekedEvents = true;
            } else {
              handleUnSuccessfulBatchDispatch(events);
              if (!resetLastPeekedEvents) {
                while (!this.dispatcher.dispatchBatch(conflatedEventsToBeDispatched,
                    sender.isRemoveFromQueueOnException(), true)) {
                  if (isDebugEnabled) {
                    logger.debug("During normal processing, unsuccessfully dispatched {} events (batch #{})",
                        conflatedEventsToBeDispatched.size(), getBatchId());
                  }
                  if (stopped() || resetLastPeekedEvents) {
                    break;
                  }
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                  }
                }
                incrementBatchId();
              }
            }
          } // unsuccessful batch
          if (logger.isDebugEnabled()) {
            logger.debug("Finished processing events (batch #{})", (getBatchId()-1));
          }
        } // for
      } catch (RegionDestroyedException e) {
        //setting this flag will ensure that already peeked events will make 
        //it to the next batch before new events are peeked (fix for #48784)
        this.resetLastPeekedEvents = true;
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
        this.resetLastPeekedEvents = true;
        
        if (e instanceof GatewaySenderException) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException
              || e instanceof GatewaySenderConfigurationException) {
            continue;
          }
        }
        
        // We'll log it but continue on with the next batch.
        logger.warn(LocalizedMessage.create(LocalizedStrings.GatewayImpl_AN_EXCEPTION_OCCURRED_THE_DISPATCHER_WILL_CONTINUE), e);
      }
    } // for
  }
  
  private boolean shouldSendVersionEvents(
      GatewaySenderEventDispatcher dispatcher) {
    // onyly in case of remote dispatcher we send versioned events
    return false;
  }
    
  private List conflate(List<GatewaySenderEventImpl> events) {
    List<GatewaySenderEventImpl> conflatedEvents = null;
    // Conflate the batch if necessary
    if (this.sender.isBatchConflationEnabled() && events.size() > 1) {
      Map<ConflationKey, GatewaySenderEventImpl> conflatedEventsMap = new LinkedHashMap<ConflationKey, GatewaySenderEventImpl>();
      conflatedEvents = new ArrayList<GatewaySenderEventImpl>();
      for (GatewaySenderEventImpl gsEvent : events) {
        // Determine whether the event should be conflated.
        if (gsEvent.shouldBeConflated()) {
          // The event should be conflated. Create the conflation key
          // (comprised of the event's region, key and the operation).
          ConflationKey key = new ConflationKey(gsEvent.getRegion().getFullPath(), gsEvent.getKeyToConflate(),
              gsEvent.getOperation());

          // Attempt to remove the key. If the entry is removed, that means a
          // duplicate key was found. If not, this is a no-op.
          conflatedEventsMap.remove(key);

          // Add the key to the end of the map.
          conflatedEventsMap.put(key, gsEvent);
        }
        else {
          // The event should not be conflated (create or destroy). Add it to
          // the map.
          ConflationKey key = new ConflationKey(gsEvent.getRegion().getFullPath(), gsEvent.getKeyToConflate(),
              gsEvent.getOperation());
          conflatedEventsMap.put(key, gsEvent);
        }
      }

      // Iterate the map and add the events to the conflated events list
      for (GatewaySenderEventImpl gei : conflatedEventsMap.values()) {
        conflatedEvents.add(gei);
      }

      // Increment the events conflated from batches statistic
      this.sender.getStatistics().incEventsConflatedFromBatches(
          events.size() - conflatedEvents.size());
    }
    else {
      conflatedEvents = events;
    }
    return conflatedEvents;
  } 
  
  private List<GatewaySenderEventImpl> addPDXEvent() throws IOException {
    List<GatewaySenderEventImpl> pdxEventsToBeDispatched = new ArrayList<GatewaySenderEventImpl>();
    
    //getPDXRegion
    GemFireCacheImpl cache = (GemFireCacheImpl) this.sender.getCache();
    Region<Object, Object> pdxRegion = cache
        .getRegion(PeerTypeRegistration.REGION_NAME);
    
    if(rebuildPdxList) {
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
        if(!pdxEventsMap.containsKey(typeEntry.getKey())){
          EntryEventImpl event = EntryEventImpl.create(
              (LocalRegion) pdxRegion, Operation.UPDATE,
              typeEntry.getKey(), typeEntry.getValue(), null, false,
              cache.getMyId());
          event.setEventId(new EventID(cache.getSystem()));
          List<Integer> allRemoteDSIds = new ArrayList<Integer>();
          for (GatewaySender sender : cache.getGatewaySenders()) {
            allRemoteDSIds.add(sender.getRemoteDSId());
          }
          GatewaySenderEventCallbackArgument geCallbackArg = new GatewaySenderEventCallbackArgument(
              event.getRawCallbackArgument(), this.sender.getMyDSId(),
              allRemoteDSIds, true);
          event.setCallbackArgument(geCallbackArg);
          GatewaySenderEventImpl pdxSenderEvent = new GatewaySenderEventImpl(
              EnumListenerEvent.AFTER_UPDATE, event, null); // OFFHEAP: event for pdx type meta data so it should never be off-heap
          pdxEventsMap.put(typeEntry.getKey(), pdxSenderEvent);
          pdxSenderEventsList.add(pdxSenderEvent);
        }
      }
    }
    
    
    Iterator<GatewaySenderEventImpl> iterator = pdxSenderEventsList.iterator();
    while(iterator.hasNext()){
      GatewaySenderEventImpl pdxEvent = iterator.next();
      if (pdxEvent.isAcked) {
        // Since this is acked, it means it has reached to remote site.Dont add
        // to pdxEventsToBeDispatched
        iterator.remove();
        continue;
      }
      if (pdxEvent.isDispatched) {
        // Dispacther does not mean that event has reched remote site. We may
        // need to send it agian if there is porblem while recieveing ack
        // containing this event.Dont add to pdxEventsToBeDispatched
        continue;
      }
      pdxEventsToBeDispatched.add(pdxEvent);
    }

    if(!pdxEventsToBeDispatched.isEmpty() && logger.isDebugEnabled()){
      logger.debug("List of PDX Event to be dispatched : {}", pdxEventsToBeDispatched);  
    }
    
    // add all these pdx events before filtered events
    return pdxEventsToBeDispatched;
  }
  
  /**
   * Mark all PDX types as requiring dispatch so that they will be
   * sent over the connection again.
   * @param remotePdxSize 
   */
  public void checkIfPdxNeedsResend(int remotePdxSize) {
    GemFireCacheImpl cache = (GemFireCacheImpl) this.sender.getCache();
    Region<Object, Object> pdxRegion = cache
        .getRegion(PeerTypeRegistration.REGION_NAME);
    
    //The peer has not seen all of our PDX types. This may be because
    //they have been lost on the remote side. Resend the PDX types.
    if(pdxRegion != null && pdxRegion.size() > remotePdxSize) {
      rebuildPdxList = true;
    }
  } 

  private void resetLastPeekedEvents() {
    this.batchIdToEventsMap.clear();
    // make sure that when there is problem while receiving ack, pdx gateway
    // sender events isDispatched is set to false so that same events will be
    // dispatched in next batch
    for(Map.Entry<Integer, List<GatewaySenderEventImpl>> entry : this.batchIdToPDXEventsMap.entrySet()){
      for(GatewaySenderEventImpl event : entry.getValue()){
        event.isDispatched = false;
      }
    }
    this.batchIdToPDXEventsMap.clear();
    if(this.queue instanceof SerialGatewaySenderQueue)
      ((SerialGatewaySenderQueue)this.queue).resetLastPeeked();
    else if (this.queue instanceof ParallelGatewaySenderQueue){
      ((ParallelGatewaySenderQueue)this.queue).resetLastPeeked();
    }else{
      //we will never come here
      throw new RuntimeException("resetLastPeekedEvents : no matching queue found " + this);
    }
  }

  private void handleSuccessfulBatchDispatch(List filteredList, List events) {
    if (filteredList != null) {
      for (GatewayEventFilter filter : sender.getGatewayEventFilters()) {
        for (Iterator i = filteredList.iterator(); i.hasNext();) {
          Object o = i.next();
          if (o != null && o instanceof GatewaySenderEventImpl) {
            try {
              filter.afterAcknowledgement((GatewaySenderEventImpl)o);
            } catch (Exception e) {
              logger
              .fatal(
                  LocalizedMessage
                      .create(
                          LocalizedStrings.GatewayEventFilter_EXCEPTION_OCCURED_WHILE_HANDLING_CALL_TO_0_AFTER_ACKNOWLEDGEMENT_FOR_EVENT_1,
                          new Object[] { filter.toString(), o }), e);
            }
          }
        }
      }
    }

    filteredList.clear();
    eventQueueRemove(events.size());
    final GatewaySenderStats statistics = this.sender.getStatistics();
    int queueSize = eventQueueSize();

    // Log an alert for each event if necessary
    if (this.sender.getAlertThreshold() > 0) {
      Iterator it = events.iterator();
      long currentTime = System.currentTimeMillis();
      while (it.hasNext()) {
        Object o = it.next();
        if (o != null && o instanceof GatewaySenderEventImpl) {
          GatewaySenderEventImpl ge = (GatewaySenderEventImpl)o;
          if (ge.getCreationTime() + this.sender.getAlertThreshold() < currentTime) {
            logger.warn(LocalizedMessage.create(LocalizedStrings.GatewayImpl_EVENT_QUEUE_ALERT_OPERATION_0_REGION_1_KEY_2_VALUE_3_TIME_4,
                new Object[] { ge.getOperation(), ge.getRegionPath(), ge.getKey(),
                ge.getValueAsString(true), currentTime - ge.getCreationTime() }));
            statistics.incEventsExceedingAlertThreshold();
          }
        }
      }
    }

    if (this.eventQueueSizeWarning
        && queueSize <= AbstractGatewaySender.QUEUE_SIZE_THRESHOLD) {
      logger.info(LocalizedMessage.create(
              LocalizedStrings.GatewayImpl_THE_EVENT_QUEUE_SIZE_HAS_DROPPED_BELOW_THE_THRESHOLD_0,
              AbstractGatewaySender.QUEUE_SIZE_THRESHOLD));
      this.eventQueueSizeWarning = false;
    }
    incrementBatchId();
  
  }
  
  private void handleUnSuccessfulBatchDispatch(List events) {
    final GatewaySenderStats statistics = this.sender.getStatistics();
    statistics.incBatchesRedistributed();

    // Set posDup flag on each event in the batch
    Iterator it = events.iterator();
    while (it.hasNext() && !this.isStopped) {
      Object o = it.next();
      if (o != null && o instanceof GatewaySenderEventImpl) {
        GatewaySenderEventImpl ge = (GatewaySenderEventImpl)o;
        ge.setPossibleDuplicate(true);
      }
    }
  }
  
  /**
   * In case of BatchException we expect that the dispatcher has removed all
   * the events till the event that threw BatchException.
   */
  public void handleException() {
    final GatewaySenderStats statistics = this.sender.getStatistics();
    statistics.incBatchesRedistributed();
    this.resetLastPeekedEvents = true;
  }

  public void handleSuccessBatchAck(int batchId) {
    // this is to acknowledge PDX related events
    List<GatewaySenderEventImpl> pdxEvents = this.batchIdToPDXEventsMap
        .remove(batchId);
    if (pdxEvents != null) {
      for (GatewaySenderEventImpl senderEvent : pdxEvents) {
        senderEvent.isAcked = true;
      }
    }
    
    List<GatewaySenderEventImpl>[] eventsArr = this.batchIdToEventsMap
        .remove(batchId);
    if (eventsArr != null) {
      List<GatewaySenderEventImpl> filteredEvents = eventsArr[1];
      for (GatewayEventFilter filter : sender.getGatewayEventFilters()) {
        for (GatewaySenderEventImpl event : filteredEvents) {
          try {
            filter.afterAcknowledgement(event);
          } catch (Exception e) {
            logger
                .fatal(
                    LocalizedMessage
                        .create(
                            LocalizedStrings.GatewayEventFilter_EXCEPTION_OCCURED_WHILE_HANDLING_CALL_TO_0_AFTER_ACKNOWLEDGEMENT_FOR_EVENT_1,
                            new Object[] { filter.toString(), event }), e);
          }
        }
      }
      List<GatewaySenderEventImpl> events = eventsArr[0];
      if (logger.isDebugEnabled()) {
        logger.debug("Removing events from the queue {}", events.size());
      }
      eventQueueRemove(events.size());
    }
    
  }
  
  public void handleUnSuccessBatchAck(int bId) {
    this.sender.getStatistics().incBatchesRedistributed();
    // Set posDup flag on each event in the batch
    List<GatewaySenderEventImpl>[] eventsArr = this.batchIdToEventsMap.get(bId);
    if(eventsArr!=null){
      List<GatewaySenderEventImpl> events = eventsArr[0];
      Iterator it = events.iterator();
      while (it.hasNext() && !this.isStopped) {
        Object o = it.next();
        if (o != null && o instanceof GatewaySenderEventImpl) {
          GatewaySenderEventImpl ge = (GatewaySenderEventImpl)o;
          ge.setPossibleDuplicate(true);
        }
      }
    }
  }
  
  //merge44957: While merging 44957, changed this method as per revision 42024.
  protected void waitForResumption() throws InterruptedException {
    synchronized (this.pausedLock) {
      if (!this.isPaused) {
        return;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("GatewaySenderEventProcessor is paused. Waiting for Resumption");
      }
      this.isDispatcherWaiting = true;
      this.pausedLock.notifyAll();
      while (this.isPaused) {
        this.pausedLock.wait();
      }
      this.isDispatcherWaiting = false;
    }
  }

  
  public abstract void initializeEventDispatcher();
  
  public GatewaySenderEventDispatcher getDispatcher(){
    return this.dispatcher;
  }

  public Map<Integer, List<GatewaySenderEventImpl>[]> getBatchIdToEventsMap() {
    return this.batchIdToEventsMap;
  }
  
  public Map<Integer, List<GatewaySenderEventImpl>> getBatchIdToPDXEventsMap() {
    return this.batchIdToPDXEventsMap;
  }
  @Override
  public void run() {
    try {
      setRunningStatus();
      processQueue();
    } catch (CancelException e) {
      if (!this.isStopped()) {
        logger.info(LocalizedMessage.create(LocalizedStrings.GatewayImpl_A_CANCELLATION_OCCURRED_STOPPING_THE_DISPATCHER));
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
      logger.fatal(LocalizedMessage.create(LocalizedStrings.GatewayImpl_MESSAGE_DISPATCH_FAILED_DUE_TO_UNEXPECTED_EXCEPTION), e);
    }
  }

  public void setRunningStatus() throws Exception {
    GemFireException ex = null;
    try {
      this.initializeEventDispatcher();
    }
    catch (GemFireException e) {
      ex = e;
    }
    synchronized (this.runningStateLock) {
      if (ex != null) {
        this.setException(ex);
        setIsStopped(true);
      }
      else {
        setIsStopped(false);
      }
      this.runningStateLock.notifyAll();
    }
    if (ex != null) {
      throw ex;
    }
  }

  public void setException(GemFireException ex) {
    this.exception = ex;
  }
  
  public Exception getException(){
    return this.exception;
  }
  /**
   * Stops the dispatcher from dispatching events . The dispatcher will stay
   * alive for a predefined time OR until its queue is empty.
   * 
   * @see AbstractGatewaySender#MAXIMUM_SHUTDOWN_WAIT_TIME
   */
  public void stopProcessing() {
    
    if (!this.isAlive()) {
      return;
    }
    resumeDispatching();

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Notifying the dispatcher to terminate", this);
    }

    // If this is the primary, stay alive for a predefined time
    // OR until the queue becomes empty
    if (this.sender.isPrimary()) {
      if (AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME == -1) {
        try {
          while (!(this.queue.size() == 0)) {
            Thread.sleep(5000);
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Waiting for the queue to get empty.", this);
            }
          }
        }
        catch (InterruptedException e) {
          // interrupted
        }
        catch (CancelException e) {
          // cancelled
        }
      } else {
        try {
          Thread.sleep(AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME * 1000);
        } catch (InterruptedException e) {/* ignore */
          // interrupted
        }
      }
    }
   
    dispatcher.stop();
    //set isStopped to true
    setIsStopped(true);

    if (this.isAlive()) {
      this.interrupt();
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Joining with the dispatcher thread upto limit of 5 seconds", this);
      }
      try {
        this.join(5000); // wait for our thread to stop
        if (this.isAlive()) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.GatewayImpl_0_DISPATCHER_STILL_ALIVE_EVEN_AFTER_JOIN_OF_5_SECONDS, this));
          // if the server machine crashed or there was a nic failure, we need
          // to terminate the socket connection now to avoid a hang when closing
          // the connections later
          // try to stop it again
          dispatcher.stop();
          this.batchIdToEventsMap.clear();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        logger.warn(LocalizedMessage.create(LocalizedStrings.GatewayImpl_0_INTERRUPTEDEXCEPTION_IN_JOINING_WITH_DISPATCHER_THREAD, this));
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
      if (this.sender.isPrimary() && this.queue.size() > 0) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.GatewayImpl_DESTROYING_GATEWAYEVENTDISPATCHER_WITH_ACTIVELY_QUEUED_DATA));
      }
    } catch (RegionDestroyedException ignore) {
    } catch (CancelException ignore) {
    } catch (CacheException ignore) {
      // just checking in case we should log a warning
    } finally {
      this.queue.close();
      if (logger.isDebugEnabled()) {
        logger.debug("Closed dispatcher");
      }
    }
  }

  protected void destroyProcessor() {
    if (logger.isDebugEnabled()) {
      logger.debug("Destroying dispatcher");
    }
    try {
      try {
        if (this.queue.peek() != null) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.GatewayImpl_DESTROYING_GATEWAYEVENTDISPATCHER_WITH_ACTIVELY_QUEUED_DATA));
        }
      } catch (InterruptedException e) {
        /*
         * ignore, 
         */
        // TODO if this won't be thrown, assert it.
      }
    } catch (CacheException ignore) {
      // just checking in case we should log a warning
    } finally {
      this.queue.getRegion().localDestroyRegion();
      if (logger.isDebugEnabled()) {
        logger.debug("Destroyed dispatcher");
      }
    }
  }
  
  public void removeCacheListener(){
    
  }

  /**
   * Logs a batch of events.
   * 
   * @param events
   *          The batch of events to log
   **/
  public void logBatchFine(String message, List<GatewaySenderEventImpl> events) {
    if (events != null) {
      StringBuffer buffer = new StringBuffer();
      buffer.append(message);
      buffer.append(events.size()).append(" events");
      buffer.append(" (batch #" + getBatchId());
      buffer.append("):\n");
      for (GatewaySenderEventImpl ge : events) {
        buffer.append("\tEvent ").append(ge.getEventId()).append(":");
        buffer.append(ge.getKey()).append("->");
        // TODO:wan70 remove old code
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
  public void clear(PartitionedRegion pr, int bucketId) {
    ((ParallelGatewaySenderQueue)this.queue).clear(pr, bucketId);
}

/*public int size(PartitionedRegion pr, int bucketId)
  throws ForceReattemptException {
    return ((ParallelGatewaySenderQueue)this.queue).size(pr, bucketId);
}*/

  public void notifyEventProcessorIfRequired(int bucketId) {
    ((ParallelGatewaySenderQueue) this.queue).notifyEventProcessorIfRequired();
  }

  public BlockingQueue<GatewaySenderEventImpl> getBucketTmpQueue(int bucketId) {
    return ((ParallelGatewaySenderQueue) this.queue).getBucketToTempQueueMap()
        .get(bucketId);
  }

  public PartitionedRegion getRegion(String prRegionName) {
    return ((ParallelGatewaySenderQueue) this.queue).getRegion(prRegionName);
  }

  public void removeShadowPR(String prRegionName) {
    ((ParallelGatewaySenderQueue) this.queue).removeShadowPR(prRegionName);
  }

  public void conflateEvent(Conflatable conflatableObject, int bucketId,
      Long tailKey) {
    ((ParallelGatewaySenderQueue) this.queue).conflateEvent(conflatableObject,
        bucketId, tailKey);
  }

  public void addShadowPartitionedRegionForUserPR(PartitionedRegion pr) {
    ((ParallelGatewaySenderQueue) this.queue)
        .addShadowPartitionedRegionForUserPR(pr);
  }

  public void addShadowPartitionedRegionForUserRR(DistributedRegion userRegion) {
    ((ParallelGatewaySenderQueue) this.queue)
        .addShadowPartitionedRegionForUserRR(userRegion);
  }
  
  protected class SenderStopperCallable implements Callable<Boolean> {
    private final AbstractGatewaySenderEventProcessor p;

    /**
     * Need the processor to stop.
     */
    public SenderStopperCallable(AbstractGatewaySenderEventProcessor processor) {
      this.p = processor;
    }

    public Boolean call () {
      this.p.stopProcessing();
      return true;
    }
  }
  
  private static class ConflationKey {
    private Object key;

    private Operation operation;
    
    private String regionName;

    private ConflationKey(String region, Object key, Operation operation) {
      this.key = key;
      this.operation = operation;
      this.regionName = region;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + key.hashCode();
      result = prime * result + operation.hashCode();
      result = prime * result + regionName.hashCode();
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
      ConflationKey that = (ConflationKey)obj;
      if (!this.regionName.equals(that.regionName)) {
        return false;
      }
      if (!this.key.equals(that.key)) {
        return false;
      }
      if (!this.operation.equals(that.operation)) {
        return false;
      }
      return true;
    }
  }   
}
