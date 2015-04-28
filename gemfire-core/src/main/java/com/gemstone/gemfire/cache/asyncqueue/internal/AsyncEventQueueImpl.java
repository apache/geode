/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.asyncqueue.internal;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayEventSubstitutionFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.serial.ConcurrentSerialGatewaySenderEventProcessor;

public class AsyncEventQueueImpl implements AsyncEventQueue {

  private GatewaySender sender = null;
  
  private AsyncEventListener asyncEventListener = null;
  
  public static final String ASYNC_EVENT_QUEUE_PREFIX = "AsyncEventQueue_";
  
  public AsyncEventQueueImpl(GatewaySender sender, AsyncEventListener eventListener) {
    this.sender = sender;
    this.asyncEventListener = eventListener;
  }
 
  @Override
  public String getId() {
    return getAsyncEventQueueIdFromSenderId(this.sender.getId());
  }
  
  @Override
  public AsyncEventListener getAsyncEventListener() {
    return asyncEventListener;
  }
  
  @Override
  public List<GatewayEventFilter> getGatewayEventFilters() {
    return sender.getGatewayEventFilters();
  }

  @Override
  public GatewayEventSubstitutionFilter getGatewayEventSubstitutionFilter() {
    return sender.getGatewayEventSubstitutionFilter();
  }

  @Override
  public int getBatchSize() {
    return sender.getBatchSize();
  }

  @Override
  public String getDiskStoreName() {
    return sender.getDiskStoreName();
  }
  
  @Override
  public int getBatchTimeInterval() {
    return sender.getBatchTimeInterval();
  }
  
  @Override
  public boolean isBatchConflationEnabled() {
    return sender.isBatchConflationEnabled();
  }

  @Override
  public int getMaximumQueueMemory() {
    return sender.getMaximumQueueMemory();
  }

  @Override
  public boolean isPersistent() {
    return sender.isPersistenceEnabled();
  }

  @Override
  public boolean isDiskSynchronous() {
    return sender.isDiskSynchronous();
  }
  
  @Override
  public int getDispatcherThreads() {
    return sender.getDispatcherThreads();
  }
  
  @Override
  public OrderPolicy getOrderPolicy() {
    return sender.getOrderPolicy();
  }
  
  @Override
  public boolean isPrimary() {
    return ((AbstractGatewaySender) sender).isPrimary();
  }
  
  @Override
  public int size() {
	AbstractGatewaySenderEventProcessor eventProcessor = 
	  ((AbstractGatewaySender) sender).getEventProcessor();
	    
	int size = 0;
	if (eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor) {
	  Set<RegionQueue> queues = 
		((ConcurrentSerialGatewaySenderEventProcessor) eventProcessor).getQueues();
	  Iterator<RegionQueue> itr = queues.iterator();
	  while (itr.hasNext()) {
		size = size + itr.next().size();
	  }
	} else {
	  size = eventProcessor.getQueue().size();
	}
	return size;
  }
  
  public GatewaySender getSender() {
    return this.sender;
  }
  
  public AsyncEventQueueStats getStatistics() {
     AbstractGatewaySender abstractSender =  (AbstractGatewaySender) this.sender;
     return ((AsyncEventQueueStats) abstractSender.getStatistics());
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AsyncEventQueue)) {
      return false;
    }
    AsyncEventQueueImpl asyncEventQueue = (AsyncEventQueueImpl) obj;
    if (asyncEventQueue.getId().equals(this.getId())) {
      return true;
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  public static String getSenderIdFromAsyncEventQueueId(String asyncQueueId) {
    StringBuilder builder = new StringBuilder();
    builder.append(ASYNC_EVENT_QUEUE_PREFIX);
    builder.append(asyncQueueId);
    return builder.toString();
  }

  public static String getAsyncEventQueueIdFromSenderId(String senderId) {
    if (!senderId.startsWith(ASYNC_EVENT_QUEUE_PREFIX)) {
      return senderId;
    }
    else {
      return senderId.substring(ASYNC_EVENT_QUEUE_PREFIX.length());
    }
  }

  public static boolean isAsyncEventQueue(String senderId) {
    return senderId.startsWith(ASYNC_EVENT_QUEUE_PREFIX);
  }

  public boolean isParallel() {
    return sender.isParallel();
  }

   public boolean isBucketSorted() {
    // TODO Auto-generated method stub
    return false;
  }
  
}
