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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.wan.serial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.ha.ThreadIdentifier;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventDispatcher;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.offheap.annotations.Released;

/**
 * 
 * 
 */
public class ConcurrentSerialGatewaySenderEventProcessor extends
    AbstractGatewaySenderEventProcessor {
  
  private static final Logger logger = LogService.getLogger();

  protected final List<SerialGatewaySenderEventProcessor> processors = new ArrayList<SerialGatewaySenderEventProcessor>();

  protected final AbstractGatewaySender sender;

  private GemFireException ex = null;

  private final Set<RegionQueue> queues;
  /**
   * @param sender
   */
  public ConcurrentSerialGatewaySenderEventProcessor(
      AbstractGatewaySender sender) {
    super(LoggingThreadGroup.createThreadGroup("Event Processor for GatewaySender_"
        + sender.getId(), logger),
        "Event Processor for GatewaySender_" + sender.getId(), sender);
    this.sender = sender;
    
    initializeMessageQueue(sender.getId());
    queues = new HashSet<RegionQueue>();
    for (SerialGatewaySenderEventProcessor processor : processors) {
      queues.add(processor.getQueue());
    }
    setDaemon(true);
  }

  @Override
  protected void initializeMessageQueue(String id) {
    for (int i = 0; i < sender.getDispatcherThreads(); i++) {
      processors.add(new SerialGatewaySenderEventProcessor(this.sender, id
          + "." + i));
      if (logger.isDebugEnabled()) {
        logger.debug("Created the SerialGatewayEventProcessor_{}->{}", i, processors.get(i));
      }
    }
  }
  
  @Override
  public int eventQueueSize() {
    int size = 0;
    for (RegionQueue queue : queues) {
      size += queue.size();
    }
    return size;
  }

  //based on the fix for old wan Bug#46992 .revision is 39437  
  @Override
  public void enqueueEvent(EnumListenerEvent operation, EntryEvent event,
      Object substituteValue) throws IOException, CacheException {
    // Get the appropriate index into the gateways
    int index = Math.abs(getHashCode(((EntryEventImpl)event))
        % this.processors.size());
    // Distribute the event to the gateway
      enqueueEvent(operation, event, substituteValue, index);

  }
  
  public void enqueueEvent(EnumListenerEvent operation, EntryEvent event,
      Object substituteValue, int index) throws CacheException, IOException {
    // Get the appropriate gateway
    SerialGatewaySenderEventProcessor serialProcessor = this.processors
        .get(index);

    if (sender.getOrderPolicy() == OrderPolicy.KEY || sender.getOrderPolicy() == OrderPolicy.PARTITION) {
      // Create copy since the event id will be changed, otherwise the same
      // event will be changed for multiple gateways. Fix for bug 44471.
      @Released EntryEventImpl clonedEvent = new EntryEventImpl((EntryEventImpl)event);
      try {
      EventID originalEventId = clonedEvent.getEventId();
      if (logger.isDebugEnabled()) {
        logger.debug("The original EventId is {}", originalEventId);
      }
      // PARALLEL_THREAD_BUFFER * (index +1) + originalEventId.getThreadID();
      // generating threadId by the algorithm explained above used to clash with
      // fakeThreadId generated by putAll
      // below is new way to generate threadId so that it doesn't clash with
      // any.
      long newThreadId = ThreadIdentifier.createFakeThreadIDForParallelGateway(
          index, originalEventId.getThreadID(), 0 /*gateway sender event id index has already been applied in SerialGatewaySenderImpl.setModifiedEventId*/);
      EventID newEventId = new EventID(originalEventId.getMembershipID(),
          newThreadId, originalEventId.getSequenceID());
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Generated event id for event with key={}, index={}, original event id={}, threadId={}, new event id={}, newThreadId={}",
            this, event.getKey(), index, originalEventId, originalEventId.getThreadID(), newEventId, newThreadId);
      }
      clonedEvent.setEventId(newEventId);
      serialProcessor.enqueueEvent(operation, clonedEvent, substituteValue);
      } finally {
        clonedEvent.release();
      }
    } else {
      serialProcessor.enqueueEvent(operation, event, substituteValue);
    }

  }

  @Override
  public void run() {
    for(int i = 0; i < this.processors.size(); i++){
      if (logger.isDebugEnabled()) {
        logger.debug("Starting the serialProcessor {}", i);
      }
      this.processors.get(i).start();
    }
    try {
      waitForRunningStatus();
    } catch (GatewaySenderException e) {
      this.ex = e;
    }

    synchronized (this.runningStateLock) {
      if (ex != null) {
        this.setException(ex);
        setIsStopped(true);
      } else {
        setIsStopped(false);
      }
      this.runningStateLock.notifyAll();
    }
    
    for (SerialGatewaySenderEventProcessor serialProcessor : this.processors) {
      try {
        serialProcessor.join();
      } catch (InterruptedException e) {
        if(logger.isDebugEnabled()) {
          logger.debug("Got InterruptedException while waiting for child threads to finish.");
          Thread.currentThread().interrupt();
        }  
      }
    }
  }
  
  @Override
  protected void rebalance() {
    // No reason to rebalance a serial sender since all connections are to the same server.
    throw new UnsupportedOperationException();
  }
  
  private void waitForRunningStatus() {
    for (SerialGatewaySenderEventProcessor serialProcessor : this.processors) {
      synchronized (serialProcessor.runningStateLock) {
        while (serialProcessor.getException() == null
            && serialProcessor.isStopped()) {
          try {
            serialProcessor.runningStateLock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        Exception ex = serialProcessor.getException();
        if (ex != null) {
          throw new GatewaySenderException(
              LocalizedStrings.Sender_COULD_NOT_START_GATEWAYSENDER_0_BECAUSE_OF_EXCEPTION_1
                  .toLocalizedString(new Object[] { this.getId(),
                      ex.getMessage() }), ex.getCause());
        }
      }
    }
  }
  
  private int getHashCode(EntryEventImpl event) {
    // Get the hash code for the event based on the configured order policy
    int eventHashCode = 0;
    switch (this.sender.getOrderPolicy()) {
    case KEY:
      // key ordering
      eventHashCode = event.getKey().hashCode();
      break;
    case THREAD:
      // member id, thread id ordering
      // requires a lot of threads to achieve parallelism
      EventID eventId = event.getEventId();
      byte[] memberId = eventId.getMembershipID();
      long threadId = eventId.getThreadID();
      int memberIdHashCode = Arrays.hashCode(memberId);
      int threadIdHashCode = (int)(threadId ^ (threadId >>> 32));
      eventHashCode = memberIdHashCode + threadIdHashCode;
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Generated hashcode for event with key={}, memberId={}, threadId={}: {}",
            this, event.getKey(), Arrays.toString(memberId), threadId, eventHashCode);
      }
      break;
    case PARTITION:
      eventHashCode = PartitionRegionHelper.isPartitionedRegion(event
          .getRegion()) ? PartitionedRegionHelper.getHashKey(event)
      // Get the partition for the event
          : event.getKey().hashCode();
      // Fall back to key ordering if the region is not partitioned
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Generated partition hashcode for event with key={}: {}", this, event.getKey(), eventHashCode);
      }
      break;

    }
    return eventHashCode;
  }
  
  @Override
  public void stopProcessing() {
    if (!this.isAlive()) {
      return;
    }

    setIsStopped(true);

    final LoggingThreadGroup loggingThreadGroup = LoggingThreadGroup
        .createThreadGroup(
            "ConcurrentSerialGatewaySenderEventProcessor Logger Group",
            logger);

    ThreadFactory threadFactory = new ThreadFactory() {
      public Thread newThread(final Runnable task) {
        final Thread thread = new Thread(loggingThreadGroup, task,
            "ConcurrentSerialGatewaySenderEventProcessor Stopper Thread");
        thread.setDaemon(true);
        return thread;
      }
    };

    List<SenderStopperCallable> stopperCallables = new ArrayList<SenderStopperCallable>();
    for (SerialGatewaySenderEventProcessor serialProcessor : this.processors) {
      stopperCallables.add(new SenderStopperCallable(serialProcessor));
    }

    ExecutorService stopperService = Executors.newFixedThreadPool(
        processors.size(), threadFactory);
    try {
      List<Future<Boolean>> futures = stopperService
          .invokeAll(stopperCallables);
      for (Future<Boolean> f : futures) {
        try {
          boolean b = f.get();
          if (logger.isDebugEnabled()) {
            logger.debug("ConcurrentSerialGatewaySenderEventProcessor: {} stopped dispatching: {}",
                (b ? "Successfully" : "Unsuccesfully"), this);
          }     
        } catch (ExecutionException e) {
          // we don't expect any exception but if caught then eat it and log
          // warning
          logger.warn(LocalizedMessage.create(LocalizedStrings.GatewaySender_0_CAUGHT_EXCEPTION_WHILE_STOPPING_1, new Object[] { sender, e.getCause() }));
        }
      }
    } catch (InterruptedException e) {
      throw new InternalGemFireException(e.getMessage());
    } catch (RejectedExecutionException rejectedExecutionEx) {
      throw rejectedExecutionEx;
    }
    //shutdown the stopperService. This will release all the stopper threads
    stopperService.shutdown();

    closeProcessor();
    
    if (logger.isDebugEnabled()) {
      logger.debug("ConcurrentSerialGatewaySenderEventProcessor: Stopped dispatching: {}", this);
    }
  }
  
  @Override
  public void closeProcessor() {
    for (SerialGatewaySenderEventProcessor processor : processors) {
      processor.closeProcessor();
    }
  }
  
  @Override
  public void pauseDispatching(){
    for (SerialGatewaySenderEventProcessor serialProcessor : this.processors) {
      serialProcessor.pauseDispatching();
    }
    super.pauseDispatching();
    if (logger.isDebugEnabled()) {
      logger.debug("ConcurrentSerialGatewaySenderEventProcessor: Paused dispatching: {}", this);
    }
  }
  
  @Override
  public void resumeDispatching() {
    for (SerialGatewaySenderEventProcessor serialProcessor : this.processors) {
      serialProcessor.resumeDispatching();
    }
    super.resumeDispatching();
    if (logger.isDebugEnabled()) {
      logger.debug("ConcurrentSerialGatewaySenderEventProcessor: Resumed dispatching: {}", this);
    }
  }

  /**
   * @return the queues
   */
  public Set<RegionQueue> getQueues() {
    return queues;
  }
  
  @Override
  public void removeCacheListener() {
    for(SerialGatewaySenderEventProcessor processor: processors) {
      processor.removeCacheListener();
    }
  }
  
  @Override
  public void waitForDispatcherToPause() {
    for (SerialGatewaySenderEventProcessor serialProcessor : this.processors) {
      serialProcessor.waitForDispatcherToPause();
    }
   // super.waitForDispatcherToPause();
  }
  
  @Override
  public GatewaySenderEventDispatcher getDispatcher() {
    return this.processors.get(0).getDispatcher();//Suranjan is that fine??
  }

  @Override
  public void initializeEventDispatcher() {
    //no op for concurrent 
    
  }

}
