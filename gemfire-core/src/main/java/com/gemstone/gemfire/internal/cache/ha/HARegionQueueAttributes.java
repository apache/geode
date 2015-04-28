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
package com.gemstone.gemfire.internal.cache.ha;

/**
 * 
 * This class defines the user specified attributes of the HARegion which are
 * configurable.
 * 
 * @author Mitul Bid
 *  
 */
public class HARegionQueueAttributes
{
  /**
   * default expiry time for region entries in seconds
   */

  private static final int DEFAULT_EXPIRY_TIME = 180;
  private static final int DEFAULT_BLOCKING_QUEUE_CAPACITY = 230000;

  /**
   * String storing the System Property key representing the blocking queue capacity 
   */
  private static final String BLOCKING_QUEUE_CAPACITY = "gemfire.Capacity";

  /**
   * expiry time for region entries in seconds
   */

  private int expiryTime = DEFAULT_EXPIRY_TIME;
  
  /**
   * 
   */
  private int blockingQueueCapacity = Integer.getInteger(BLOCKING_QUEUE_CAPACITY,DEFAULT_BLOCKING_QUEUE_CAPACITY).intValue();
  
  //TODO:Asif: We shoudl prevent modification of this object by using
  // HARegionAttributesFactory instead of directly
  // providing getter/setter in HARegionAttributes. HAregionAttributes should be
  // immutable
  static final HARegionQueueAttributes DEFAULT_HARQ_ATTRIBUTES = new HARegionQueueAttributes();

  /**
   * Default constructor
   */
  public HARegionQueueAttributes() {
   // this.blockingQueueCapacity = Integer.getInteger(BLOCKING_QUEUE_CAPACITY,DEFAULT_BLOCKING_QUEUE_CAPACITY).intValue();
  }

  /**
   * Gets the expiration time for the region entries
   * 
   * @return the expiry time in seconds
   */
  public int getExpiryTime()
  {
    return expiryTime;
  }

  /**
   * Sets the expiration time for the region entries
   * 
   * @param expiryTime
   *          expiry time in seconds
   */
  public void setExpiryTime(int expiryTime)
  {
    this.expiryTime = expiryTime;
  }
  /**
   * Gets the blocking queue capacity
   * 
   * @return the blocking queue capacity
   */
  public int getBlockingQueueCapacity()
  {
    return this.blockingQueueCapacity;
  }

  /**
   * Sets the capacity of the queue
   * 
   * @param cap
   *          number of items allowed in the queue
   */
  public void setBlockingQueueCapacity(int cap)
  {
     this.blockingQueueCapacity = cap;
  }
  
}
