/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;

/**
 * MBean that provides access to an {@link AsyncEventQueue}.
 * 
 * @author rishim
 * @since 7.0
 * 
 */
public interface AsyncEventQueueMXBean {

  /**
   * Returns the ID of the AsyncEventQueue.
   */
  public String getId();

  /**
   * Returns the name of the disk store that is used for persistence.
   */
  public String getOverflowDiskStoreName();

  /**
   * Returns the maximum memory after which the data needs to be overflowed to disk.
   */
  public int getMaximumQueueMemory();

  /**
   * Returns the size of a batch that gets delivered over the AsyncEventQueue.
   */
  public int getBatchSize();
  
  /**
   * Returns the interval between transmissions by the AsyncEventQueue.
   */
  public long getBatchTimeInterval();

  /**
   * Returns whether batch conflation for the AsyncEventQueue is enabled
   * 
   * @return True if batch conflation is enabled, false otherwise.
   */
  public boolean isBatchConflationEnabled();

  /**
   * Returns whether the AsyncEventQueue is configured to be persistent or
   * non-persistent.
   * 
   * @return True if the queue is persistent, false otherwise.
   */
  public boolean isPersistent();

  /**
   * Returns whether the queue is primary or secondary. Events get delivered
   * only by the primary queue. If the primary queue goes down then the secondary
   * queue first becomes primary and then starts delivering the events. 
   * 
   * @return True if this is the primary queue, false otherwise.
   */
  public boolean isPrimary();
  
  /**
   * Returns the number of dispatcher threads working for this <code>AsyncEventQueue</code>.
   */
  public int getDispatcherThreads();
  
  /**
   * Returns the order policy followed while dispatching the events to remote
   * distributed system. Order policy is only relevant when the number of dispatcher
   * threads is greater than one.
   */
  
  public String getOrderPolicy();
 
  /**
   * Returns whether the isDiskSynchronous property is set for this AsyncEventQueue.
   * 
   * @return True if the property is set, false otherwise.
   */
  public boolean isDiskSynchronous();

  /**
   * Returns whether the isParallel property is set for this AsyncEventQueue.
   * 
   * @return True if the property is set, false otherwise.
   */
  public boolean isParallel();

  /**
   * Returns the class name of the AsyncEventListener that is attached to the queue.
   */
  public String getAsyncEventListener();
  
  /**
   * Returns the Size of the event queue
   * 
   * @return integer
   */
  public int getEventQueueSize();

}
