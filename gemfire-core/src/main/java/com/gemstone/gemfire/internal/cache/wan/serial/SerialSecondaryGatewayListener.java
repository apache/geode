/*=========================================================================
 * Copyright (c) 2002-2014, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan.serial;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.logging.LogService;
/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @since 7.0
 *
 */
public class SerialSecondaryGatewayListener extends CacheListenerAdapter
{

  private static final Logger logger = LogService.getLogger();
  
 private final SerialGatewaySenderEventProcessor processor;
 
 private final AbstractGatewaySender sender;

 protected SerialSecondaryGatewayListener(SerialGatewaySenderEventProcessor eventProcessor) {
   this.processor = eventProcessor;
   this.sender = eventProcessor.getSender();
 }

 @Override
 public void afterCreate(EntryEvent event)
 {
   if (this.sender.isPrimary()) {
     // The secondary has failed over to become the primary. There is a small
     // window where the secondary has become the primary, but the listener
     // is
     // still set. Ignore any updates to the map at this point. It is unknown
     // what the state of the map is. This may result in duplicate events
     // being sent.
     return;
   }
   // There is a small window where queue has not been created fully yet. 
   // The underlying region of the queue is created, and it receives afterDestroy callback
   if (this.sender.getQueues() != null && !this.sender.getQueues().isEmpty()) {
//     int size = 0;
//     for(RegionQueue q: this.sender.getQueues()) {
//       size += q.size();
//     }
     this.sender.getStatistics().incQueueSize();
   }
   // fix bug 35730

   // Send event to the event dispatcher
   GatewaySenderEventImpl senderEvent = (GatewaySenderEventImpl)event.getNewValue();
   this.processor.handlePrimaryEvent(senderEvent);
 }

 @Override
 public void afterDestroy(EntryEvent event) {
   if (this.sender.isPrimary()) {
     return;
   }
    // fix bug 37603
    // There is a small window where queue has not been created fully yet. The region is created, and it receives afterDestroy callback.
   
   if (this.sender.getQueues() != null && !this.sender.getQueues().isEmpty()) {
//     int size = 0;
//     for(RegionQueue q: this.sender.getQueues()) {
//       size += q.size();
//     }
     this.sender.getStatistics().decQueueSize();
   }

   // Send event to the event dispatcher
   if (event.getOldValue() instanceof GatewaySenderEventImpl) {
     GatewaySenderEventImpl senderEvent = (GatewaySenderEventImpl)event.getOldValue();
     if(logger.isDebugEnabled()) {
        logger.debug("Received after Destroy for Secondary event {} the key was {}", senderEvent, event.getKey());
     }
     this.processor.handlePrimaryDestroy(senderEvent);
   }
 }
}
