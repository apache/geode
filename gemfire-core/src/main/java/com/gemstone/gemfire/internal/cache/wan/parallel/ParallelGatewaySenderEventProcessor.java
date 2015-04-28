/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackDispatcher;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;


/**
 * @author Suranjan Kumar
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
    
      this.queue = new ParallelGatewaySenderQueue(this.sender, targetRs, this.index, this.nDispatcher);
    
    if(((ParallelGatewaySenderQueue)queue).localSize() > 0) {
      ((ParallelGatewaySenderQueue)queue).notifyEventProcessorIfRequired();
    }
  }
  
  @Override
  public void enqueueEvent(EnumListenerEvent operation, EntryEvent event,
      Object substituteValue) throws IOException, CacheException {
    GatewayQueueEvent gatewayQueueEvent = null;
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
    try {
      EventID eventID = ((EntryEventImpl)event).getEventId();

      // while merging 42004, kept substituteValue as it is(it is barry's
      // change 42466). bucketID is merged with eventID.getBucketID
      gatewayQueueEvent = new GatewaySenderEventImpl(operation, event,
          substituteValue, true, eventID.getBucketID());

      if (getSender().beforeEnque(gatewayQueueEvent)) {
        long start = getSender().getStatistics().startTime();
        try {
          this.queue.put(gatewayQueueEvent);
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
      //merge44012: this try finally has came from cheetah. this change is related to offheap.
//      if (gatewayQueueEvent != null) {
//        // it was not queued for some reason
//         gatewayQueueEvent.release();
//      }
    }
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
