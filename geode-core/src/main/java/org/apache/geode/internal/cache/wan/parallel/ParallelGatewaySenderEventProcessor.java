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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventCallbackDispatcher;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ParallelGatewaySenderEventProcessor extends AbstractGatewaySenderEventProcessor {

  private static final Logger logger = LogService.getLogger();

  final int index;
  final int nDispatcher;

  protected ParallelGatewaySenderEventProcessor(AbstractGatewaySender sender,
      ThreadsMonitoring tMonitoring, boolean cleanQueues) {
    super("Event Processor for GatewaySender_" + sender.getId(), sender, tMonitoring);
    index = 0;
    nDispatcher = 1;
    initializeMessageQueue(sender.getId(), cleanQueues);
  }

  /**
   * use in concurrent scenario where queue is to be shared among all the processors.
   */
  protected ParallelGatewaySenderEventProcessor(AbstractGatewaySender sender, int index,
      int nDispatcher, ThreadsMonitoring tMonitoring, boolean cleanQueues) {
    super("Event Processor for GatewaySender_" + sender.getId() + "_" + index, sender, tMonitoring);
    this.index = index;
    this.nDispatcher = nDispatcher;
    initializeMessageQueue(sender.getId(), cleanQueues);
  }

  @Override
  protected void initializeMessageQueue(String id, boolean cleanQueues) {
    Set<Region<?, ?>> targetRs = new HashSet<>();
    for (InternalRegion region : sender.getCache().getApplicationRegions()) {
      if (region.getAllGatewaySenderIds().contains(id)) {
        targetRs.add(region);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("The target Regions are(PGSEP) {}", targetRs);
    }

    ParallelGatewaySenderQueue queue =
        new ParallelGatewaySenderQueue(sender, targetRs, index, nDispatcher, cleanQueues);

    queue.start();
    this.queue = queue;

    if (queue.localSize() > 0) {
      queue.notifyEventProcessorIfRequired();
    }
  }

  @Override
  public int eventQueueSize() {
    ParallelGatewaySenderQueue queue = (ParallelGatewaySenderQueue) getQueue();
    return queue == null ? 0 : queue.localSize();
  }

  @Override
  public void enqueueEvent(EnumListenerEvent operation, EntryEvent<?, ?> event,
      Object substituteValue, boolean isLastEventInTransaction)
      throws IOException, CacheException {
    Region<?, ?> region = event.getRegion();

    if (!(region instanceof DistributedRegion) && ((EntryEventImpl) event).getTailKey() == -1) {
      // In case of parallel sender, we don't expect the key to be not set.
      // If it is the case then the event must be coming from notificationOnly message.
      // Don't enqueue the event and return from here only.
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ParallelGatewaySenderEventProcessor not enqueuing the following event since tailKey is not set. {}",
            event);
      }
      return;
    }

    // TODO: Looks like for PDX region bucket id is set to -1.
    EventID eventID = ((EntryEventImpl) event).getEventId();

    final GatewaySenderEventImpl gatewayQueueEvent =
        new GatewaySenderEventImpl(operation, event, substituteValue, true, eventID.getBucketID(),
            isLastEventInTransaction);

    enqueueEvent(gatewayQueueEvent);
  }

  @Override
  protected void enqueueEvent(GatewayQueueEvent<?, ?> gatewayQueueEvent) {
    boolean queuedEvent = false;
    try {
      if (getSender().beforeEnqueue(gatewayQueueEvent)) {
        long start = getSender().getStatistics().startTime();
        try {
          queuedEvent = queue.put(gatewayQueueEvent);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        getSender().getStatistics().endPut(start);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("The Event {} is filtered.", gatewayQueueEvent);
        }
        getSender().getStatistics().incEventsFiltered();
      }
    } finally {
      if (!queuedEvent) {
        // it was not queued for some reason
        ((GatewaySenderEventImpl) gatewayQueueEvent).release();
      }
    }
  }

  @Override
  protected void registerEventDroppedInPrimaryQueue(EntryEventImpl droppedEvent) {
    logger.info("ParallelGatewaySenderEventProcessor should not process dropped event {}",
        droppedEvent);
  }

  @Override
  public void clear(PartitionedRegion pr, int bucketId) {
    ((ParallelGatewaySenderQueue) queue).clear(pr, bucketId);
  }

  @Override
  public void notifyEventProcessorIfRequired(int bucketId) {
    ((ParallelGatewaySenderQueue) queue).notifyEventProcessorIfRequired();
  }

  @Override
  public BlockingQueue<GatewaySenderEventImpl> getBucketTmpQueue(int bucketId) {
    return ((ParallelGatewaySenderQueue) queue).getBucketToTempQueueMap().get(bucketId);
  }

  @Override
  public PartitionedRegion getRegion(String prRegionName) {
    return ((ParallelGatewaySenderQueue) queue).getRegion(prRegionName);
  }

  @Override
  public void removeShadowPR(String prRegionName) {
    ((ParallelGatewaySenderQueue) queue).removeShadowPR(prRegionName);
  }

  @Override
  public void conflateEvent(Conflatable conflatableObject, int bucketId, Long tailKey) {
    ((ParallelGatewaySenderQueue) queue).conflateEvent(conflatableObject, bucketId, tailKey);
  }

  @Override
  public void addShadowPartitionedRegionForUserPR(PartitionedRegion pr) {
    ((ParallelGatewaySenderQueue) queue).addShadowPartitionedRegionForUserPR(pr);
  }

  @Override
  public void addShadowPartitionedRegionForUserRR(DistributedRegion userRegion) {
    ((ParallelGatewaySenderQueue) queue).addShadowPartitionedRegionForUserRR(userRegion);
  }

  @Override
  protected void rebalance() {
    // No operation for AsyncEventQueuerProcessor
  }

  @Override
  public void initializeEventDispatcher() {
    if (logger.isDebugEnabled()) {
      logger.debug(" Creating the GatewayEventCallbackDispatcher");
    }
    dispatcher = new GatewaySenderEventCallbackDispatcher(this);
  }
}
