/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.asyncqueue;

import java.util.List;

import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayEventSubstitutionFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;

/**
 * Interface of AsyncEventQueue. 
 * This represents the channel over which the events are delivered to the <code>AsyncEventListener</code>. 
 * 
 * @author pdeole
 * @since 7.0
 */
public interface AsyncEventQueue {

  /**
   * @return String  Id of the AsyncEventQueue  
   */
  public String getId();
  
  /**
   * The Disk store that is required for overflow and persistence
   * @return    String
   */
  public String getDiskStoreName();//for overflow and persistence
  
  /**
   * The maximum memory after which the data needs to be overflowed to disk.
   * Default is 100 MB.
   * @return    int
   */
  public int getMaximumQueueMemory();//for overflow
  
  /**
   * Represents the size of a batch that gets delivered over the AsyncEventQueue.
   * Default batchSize is 100. 
   * @return    int
   */
  public int getBatchSize();
  
  /**
   * Represents the maximum time interval that can elapse before a batch is sent 
   * from <code>AsyncEventQueue</code>.
   * Default batchTimeInterval is 5 ms.
   * 
   * @return    int
   */
  public int getBatchTimeInterval();
  
  /**
   * Represents whether batch conflation is enabled for batches sent 
   * from <code>AsyncEventQueue</code>.
   * Default is false.
   * @return    boolean
   */
  public boolean isBatchConflationEnabled();
  
  /**
   * Represents whether the AsyncEventQueue is configured to be persistent or non-persistent.
   * Default is false.
   * @return    boolean
   */
  public boolean isPersistent();
  
  /**
   * Represents whether writing to disk is synchronous or not.
   * Default is true.
   * @return    boolean
   */
  public boolean isDiskSynchronous();
  
  /**
   * Represents whether the queue is primary or secondary. 
   * Events get delivered only by the primary queue. 
   * If the primary queue goes down then the secondary queue first becomes primary 
   * and then starts delivering the events.  
   * @return    boolean
   */
  public boolean isPrimary();
  
  /**
   * The <code>AsyncEventListener</code> that is attached to the queue. 
   * All the event passing over the queue are delivered to attached listener.
   * @return    AsyncEventListener      Implementation of AsyncEventListener
   */
  public AsyncEventListener getAsyncEventListener();
  
  /**
   * Represents whether this queue is parallel (higher throughput) or serial.
   * @return    boolean    True if the queue is parallel, false otherwise.
   */
  public boolean isParallel();
  
  /**
   * Returns the number of dispatcher threads working for this <code>AsyncEventQueue</code>.
   * Default number of dispatcher threads is 5.
   * 
   * @return the number of dispatcher threads working for this <code>AsyncEventQueue</code>
   */
  public int getDispatcherThreads();
  
  /**
   * Returns the order policy followed while dispatching the events to AsyncEventListener.
   * Order policy is set only when dispatcher threads are > 1.
   * Default order policy is KEY.
   * @return the order policy followed while dispatching the events to AsyncEventListener.
   */
  public OrderPolicy getOrderPolicy();
  
  /**
   * Returns the number of entries in this <code>AsyncEventQueue</code>.
   * @return the number of entries in this <code>AsyncEventQueue</code>.
   */
  public int size();
  
  /**
   * Returns the <code>GatewayEventFilters</code> for this
   * <code>AsyncEventQueue</code>
   * 
   * @return the <code>GatewayEventFilters</code> for this
   *         <code>AsyncEventQueue</code>
   */
  public List<GatewayEventFilter> getGatewayEventFilters();
  
  /**
   * Returns the <code>GatewayEventSubstitutionFilter</code> for this
   * <code>AsyncEventQueue</code>
   * 
   * @return the <code>GatewayEventSubstitutionFilter</code> for this
   *         <code>AsyncEventQueue</code>
   */
  public GatewayEventSubstitutionFilter getGatewayEventSubstitutionFilter();
}
