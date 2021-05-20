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
package org.apache.geode.internal.cache.wan.parallel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventDispatcher;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Parallel processor which constitutes of multiple {@link ParallelGatewaySenderEventProcessor}.
 * Each of the {@link ParallelGatewaySenderEventProcessor} is responsible of dispatching events from
 * a set of shadowPR or buckets. Once the buckets/shadowPRs are assigned to a processor it should
 * not change to avoid any event ordering issue.
 *
 * The {@link ParallelGatewaySenderQueue} should be shared among all the
 * {@link ParallelGatewaySenderEventProcessor}s.
 */
public class ConcurrentParallelGatewaySenderEventProcessor
    extends AbstractGatewaySenderEventProcessor {

  protected static final Logger logger = LogService.getLogger();

  protected ParallelGatewaySenderEventProcessor[] processors;

  private GemFireException ex = null;

  final int nDispatcher;

  public ConcurrentParallelGatewaySenderEventProcessor(AbstractGatewaySender sender,
      ThreadsMonitoring tMonitoring, boolean cleanQueues) {
    super("Event Processor for GatewaySender_" + sender.getId(), sender, tMonitoring);
    logger.info("ConcurrentParallelGatewaySenderEventProcessor: dispatcher threads {}",
        sender.getDispatcherThreads());

    nDispatcher = sender.getDispatcherThreads();
    /*
     * We have to divide the buckets/shadowPRs here. So that the individual processors can start
     * with a set of events to deal with In case of shadowPR getting created it will have to attach
     * itself to one of the processors when they are created.
     */
    // We have to do static partitioning of buckets and region attached.
    // We should remember that this partitioning may change in future as new shadowPRs
    // get created.
    // Static partitioning is as follows
    // for each of the shadowPR:
    // each of the processor gets : 0 .. totalNumBuckets/totalDispatcherThreads and last processor
    // gets the remaining
    // bucket
    Set<Region<?, ?>> targetRs = new HashSet<>();
    for (InternalRegion pr : sender.getCache().getApplicationRegions()) {
      if (pr.getAllGatewaySenderIds().contains(sender.getId())) {
        targetRs.add(pr);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("The target PRs are {} Dispatchers: {}", targetRs, nDispatcher);
    }

    createProcessors(sender.getDispatcherThreads(), targetRs, cleanQueues);

    // this.queue = parallelQueue;
    queue = new ConcurrentParallelGatewaySenderQueue(sender, processors);
  }

  protected void createProcessors(int dispatcherThreads, Set<Region<?, ?>> targetRs,
      boolean cleanQueues) {
    processors = new ParallelGatewaySenderEventProcessor[sender.getDispatcherThreads()];
    if (logger.isDebugEnabled()) {
      logger.debug("Creating AsyncEventProcessor");
    }
    for (int i = 0; i < sender.getDispatcherThreads(); i++) {
      processors[i] = new ParallelGatewaySenderEventProcessor(sender, i,
          sender.getDispatcherThreads(), getThreadMonitorObj(), cleanQueues);
    }
  }

  @Override
  protected void initializeMessageQueue(String id, boolean cleanQueues) {
    // nothing
  }

  @Override
  public int eventQueueSize() {
    ConcurrentParallelGatewaySenderQueue queue = (ConcurrentParallelGatewaySenderQueue) getQueue();
    return queue == null ? 0 : queue.localSize();
  }

  @Override
  public void enqueueEvent(EnumListenerEvent operation, EntryEvent<?, ?> event,
      Object substituteValue)
      throws CacheException, IOException {
    enqueueEvent(operation, event, substituteValue, false);
  }

  @Override
  public void enqueueEvent(EnumListenerEvent operation, EntryEvent<?, ?> event,
      Object substituteValue,
      boolean isLastEventInTransaction) throws CacheException, IOException {
    int bucketId = ((EntryEventImpl) event).getEventId().getBucketID();
    if (bucketId < 0) {
      return;
    }
    int pId = bucketId % nDispatcher;
    processors[pId].enqueueEvent(operation, event, substituteValue, isLastEventInTransaction);
  }

  @Override
  protected void registerEventDroppedInPrimaryQueue(EntryEventImpl droppedEvent) {
    if (queue == null) {
      return;
    }
    ConcurrentParallelGatewaySenderQueue cpgsq = (ConcurrentParallelGatewaySenderQueue) queue;
    PartitionedRegion prQ = cpgsq.getRegion(droppedEvent.getRegion().getFullPath());
    if (prQ == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("shadow partitioned region " + droppedEvent.getRegion().getFullPath()
            + " is not created yet.");
      }
      return;
    }
    int bucketId = PartitionedRegionHelper.getHashKey(droppedEvent);
    long shadowKey = droppedEvent.getTailKey();

    ParallelGatewaySenderQueue pgsq = (ParallelGatewaySenderQueue) cpgsq.getQueueByBucket(bucketId);
    boolean isPrimary = prQ.getRegionAdvisor().getBucketAdvisor(bucketId).isPrimary();
    if (isPrimary) {
      pgsq.sendQueueRemovalMesssageForDroppedEvent(prQ, bucketId, shadowKey);
      sender.getStatistics().incEventsDroppedDueToPrimarySenderNotRunning();
      if (logger.isDebugEnabled()) {
        logger.debug("register dropped event for primary queue. BucketId is " + bucketId
            + ", shadowKey is " + shadowKey + ", prQ is " + prQ.getFullPath());
      }
    }
  }

  @Override
  public void run() {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    for (int i = 0; i < processors.length; i++) {
      if (isDebugEnabled) {
        logger.debug("Starting the ParallelProcessors {}", i);
      }
      processors[i].start();
    }
    try {
      waitForRunningStatus();
    } catch (GatewaySenderException e) {
      ex = e;
    }

    synchronized (getRunningStateLock()) {
      if (ex != null) {
        setException(ex);
        setIsStopped(true);
      } else {
        setIsStopped(false);
      }
      getRunningStateLock().notifyAll();
    }

    for (ParallelGatewaySenderEventProcessor parallelProcessor : processors) {
      try {
        parallelProcessor.join();
      } catch (InterruptedException e) {
        if (isDebugEnabled) {
          logger.debug("Got InterruptedException while waiting for child threads to finish.");
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private void waitForRunningStatus() {
    for (ParallelGatewaySenderEventProcessor parallelProcessor : processors) {
      synchronized (parallelProcessor.getRunningStateLock()) {
        while (parallelProcessor.getException() == null && parallelProcessor.isStopped()) {
          try {
            parallelProcessor.getRunningStateLock().wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        Exception ex = parallelProcessor.getException();
        if (ex != null) {
          throw new GatewaySenderException(
              String.format("Could not start a gateway sender %s because of exception %s",
                  sender.getId(), ex.getMessage()),
              ex.getCause());
        }
      }
    }
  }

  @Override
  public void stopProcessing() {
    if (!isAlive()) {
      return;
    }

    setIsStopped(true);

    List<SenderStopperCallable> stopperCallables = new ArrayList<>();
    for (ParallelGatewaySenderEventProcessor parallelProcessor : processors) {
      stopperCallables.add(new SenderStopperCallable(parallelProcessor));
    }

    ExecutorService stopperService = LoggingExecutors.newFixedThreadPool(
        processors.length, "ConcurrentParallelGatewaySenderEventProcessor Stopper Thread",
        true);
    try {
      List<Future<Boolean>> futures = stopperService.invokeAll(stopperCallables);
      for (Future<Boolean> f : futures) {
        try {
          Boolean b = f.get();
          if (logger.isDebugEnabled()) {
            logger.debug(
                "ConcurrentParallelGatewaySenderEventProcessor: {} stopped dispatching: {}",
                (b ? "Successfully" : "Unsuccessfully"), this);
          }
        } catch (ExecutionException e) {
          // we don't expect any exception but if caught then eat it and log warning
          logger.warn(String.format("GatewaySender %s caught exception while stopping: %s", sender,
              e.getCause()));
        }
      }
    } catch (InterruptedException e) {
      throw new InternalGemFireException(e);
    }

    stopperService.shutdown();
    closeProcessor();
    if (logger.isDebugEnabled()) {
      logger.debug("ConcurrentParallelGatewaySenderEventProcessor: Stopped dispatching: {}", this);
    }
  }

  @Override
  public void closeProcessor() {
    for (ParallelGatewaySenderEventProcessor parallelProcessor : processors) {
      parallelProcessor.closeProcessor();
    }
  }

  @Override
  public void pauseDispatching() {
    for (ParallelGatewaySenderEventProcessor parallelProcessor : processors) {
      parallelProcessor.pauseDispatching();
    }
    super.pauseDispatching();
    if (logger.isDebugEnabled()) {
      logger.debug("ConcurrentParallelGatewaySenderEventProcessor: Paused dispatching: {}", this);
    }
  }

  @Override
  public void waitForDispatcherToPause() {
    for (ParallelGatewaySenderEventProcessor parallelProcessor : processors) {
      parallelProcessor.waitForDispatcherToPause();
    }
  }

  @Override
  public void resumeDispatching() {
    for (ParallelGatewaySenderEventProcessor parallelProcessor : processors) {
      parallelProcessor.resumeDispatching();
    }
    super.resumeDispatching();
    if (logger.isDebugEnabled()) {
      logger.debug("ConcurrentParallelGatewaySenderEventProcessor: Resumed dispatching: {}", this);
    }
  }

  @Override
  protected void waitForResumption() throws InterruptedException {
    // TODO Auto-generated method stub
    super.waitForResumption();
  }

  public List<ParallelGatewaySenderEventProcessor> getProcessors() {
    return new LinkedList<>(Arrays.asList(processors));
  }

  @Override
  public RegionQueue getQueue() {
    return queue;
  }

  @Override
  public GatewaySenderEventDispatcher getDispatcher() {
    return processors[0].getDispatcher();
  }

  @Override
  protected void rebalance() {
    // no op for AsyncEventProcessor
  }

  @Override
  public void initializeEventDispatcher() {
    // no op for AsyncEventProcessor
  }

  @Override
  protected void enqueueEvent(GatewayQueueEvent<?, ?> event) {
    int pId = ((GatewaySenderEventImpl) event).getBucketId() % nDispatcher;
    processors[pId].enqueueEvent(event);
  }

  private ThreadsMonitoring getThreadMonitorObj() {
    DistributionManager distributionManager = sender.getDistributionManager();
    if (distributionManager != null) {
      return distributionManager.getThreadMonitoring();
    } else {
      return null;
    }
  }
}
