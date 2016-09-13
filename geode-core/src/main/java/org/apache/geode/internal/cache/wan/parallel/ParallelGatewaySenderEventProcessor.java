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
package com.gemstone.gemfire.internal.cache.wan.parallel;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackDispatcher;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.size.SingleObjectSizer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;


/**
 * 
 */
public class ParallelGatewaySenderEventProcessor extends
    AbstractGatewaySenderEventProcessor {
	
  private static final Logger logger = LogService.getLogger();
  
  final int index; 
  final int nDispatcher;

  protected ParallelGatewaySenderEventProcessor(AbstractGatewaySender sender) {
    super(LoggingThreadGroup.createThreadGroup("Event Processor for GatewaySender_"
        + sender.getId(), logger),
        "Event Processor for GatewaySender_" + sender.getId(), sender);
    this.index = 0;
    this.nDispatcher = 1;
    initializeMessageQueue(sender.getId());
    setDaemon(true);
  }
  
  /**
   * use in concurrent scenario where queue is to be shared among all the processors.
   */
  protected ParallelGatewaySenderEventProcessor(AbstractGatewaySender sender,  Set<Region> userRegions, int id, int nDispatcher) {
    super(LoggingThreadGroup.createThreadGroup("Event Processor for GatewaySender_"
        + sender.getId(), logger),
        "Event Processor for GatewaySender_" + sender.getId()+"_"+ id, sender);
    this.index = id;
    this.nDispatcher = nDispatcher;
    //this.queue = new ParallelGatewaySenderQueue(sender, userRegions, id, nDispatcher);
    initializeMessageQueue(sender.getId());
    setDaemon(true);
  }
  
  @Override
  protected void initializeMessageQueue(String id ) {
    Set<Region> targetRs = new HashSet<Region>();
    for (LocalRegion region : ((GemFireCacheImpl)((AbstractGatewaySender)sender).getCache())
        .getApplicationRegions()) {
      if (region.getAllGatewaySenderIds().contains(id)) {
        targetRs.add(region);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("The target Regions are(PGSEP) {}", targetRs);
    }
    
    ParallelGatewaySenderQueue queue;
    queue = new ParallelGatewaySenderQueue(this.sender, targetRs, this.index, this.nDispatcher);
    
    queue.start();
    this.queue = queue;
    
    if(((ParallelGatewaySenderQueue)queue).localSize() > 0) {
      ((ParallelGatewaySenderQueue)queue).notifyEventProcessorIfRequired();
    }
  }
  
  @Override
  public void enqueueEvent(EnumListenerEvent operation, EntryEvent event,
      Object substituteValue) throws IOException, CacheException {
    GatewaySenderEventImpl gatewayQueueEvent = null;
    Region region = event.getRegion();
    
    if (!(region instanceof DistributedRegion) && ((EntryEventImpl)event).getTailKey() == -1) {
      // In case of parallel sender, we don't expect the key to be not set. 
      // If it is the case then the event must be coming from notificationOnly message.
      // Don't enqueue the event and return from here only. 
      // Fix for #49081 and EntryDestroyedException in #49367. 
      if (logger.isDebugEnabled()) {
        logger.debug("ParallelGatewaySenderEventProcessor not enqueing the following event since tailKey is not set. {}", event);
      }
      return;
    }
    
    //TODO : Kishor : Looks like for PDX region bucket id is set to -1.
//    int bucketId = -1;
//    if (!(region instanceof DistributedRegion && ((DistributedRegion)region)
//        .isPdxTypesRegion())) {
//      bucketId = PartitionedRegionHelper.getHashKey(event);
//    }
    boolean queuedEvent = false;
    try {
      EventID eventID = ((EntryEventImpl)event).getEventId();

      // while merging 42004, kept substituteValue as it is(it is barry's
      // change 42466). bucketID is merged with eventID.getBucketID
      gatewayQueueEvent = new GatewaySenderEventImpl(operation, event,
          substituteValue, true, eventID.getBucketID());


      if (getSender().beforeEnqueue(gatewayQueueEvent)) {
        long start = getSender().getStatistics().startTime();
        try {
          queuedEvent = this.queue.put(gatewayQueueEvent);
        }
        catch (InterruptedException e) {
          e.printStackTrace();
        }
        getSender().getStatistics().endPut(start);
      }
      else {
        if (logger.isDebugEnabled()) {
          logger.debug("The Event {} is filtered.", gatewayQueueEvent);
        }
        getSender().getStatistics().incEventsFiltered();
      }
    }
    finally {
      if (!queuedEvent) {
        // it was not queued for some reason
        gatewayQueueEvent.release();
      }
    }
  }

  public void clear(PartitionedRegion pr, int bucketId) {
  	((ParallelGatewaySenderQueue)this.queue).clear(pr, bucketId);
  }
  
  /*public int size(PartitionedRegion pr, int bucketId)
      throws ForceReattemptException {
  	return ((ParallelGatewaySenderQueue)this.queue).size(pr, bucketId);
  }*/
  
  public void notifyEventProcessorIfRequired(int bucketId) {
    ((ParallelGatewaySenderQueue)this.queue).notifyEventProcessorIfRequired();
  }
  
  public BlockingQueue<GatewaySenderEventImpl> getBucketTmpQueue(int bucketId) {
    return ((ParallelGatewaySenderQueue)this.queue).getBucketToTempQueueMap().get(bucketId);
  }
  
  public PartitionedRegion getRegion(String prRegionName) {
    return ((ParallelGatewaySenderQueue)this.queue).getRegion(prRegionName);
  }
  
  public void removeShadowPR(String prRegionName) {
  	((ParallelGatewaySenderQueue)this.queue).removeShadowPR(prRegionName);
  }
  
  public void conflateEvent(Conflatable conflatableObject, int bucketId,
      Long tailKey) {
  	((ParallelGatewaySenderQueue)this.queue).conflateEvent(conflatableObject, bucketId, tailKey);
  }
  
  public void addShadowPartitionedRegionForUserPR(PartitionedRegion pr) {
	// TODO Auto-generated method stub
	((ParallelGatewaySenderQueue)this.queue).addShadowPartitionedRegionForUserPR(pr);
  }
  
  public void addShadowPartitionedRegionForUserRR(DistributedRegion userRegion) {
	// TODO Auto-generated method stub
	((ParallelGatewaySenderQueue)this.queue).addShadowPartitionedRegionForUserRR(userRegion);
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
    this.dispatcher = new GatewaySenderEventCallbackDispatcher(this);
  }
}
