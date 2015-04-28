/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.*;

import java.util.List;

/**
 * Interface <code>RegionQueue</code> is an interface for queue
 * implementations backed by regions.
 * 
 * @author Barry Oglesby
 * 
 * @since 4.2
 */
public interface RegionQueue
{

  /**
   * A token used to signify this is a WAN queue. It is set in the callback
   * argument.
   */
  public static final String WAN_QUEUE_TOKEN = "WAN_QUEUE_TOKEN";

  /**
   * Puts an object onto the tail of the queue
   * 
   * @param object
   *          The object to put onto the queue
   * 
   * @throws InterruptedException
   * @throws CacheException
   */
  public void put(Object object) throws InterruptedException, CacheException;

  /**
   * Returns the underlying region that backs this queue.
   */
  public Region getRegion();

  /**
   * Takes the first object from the head of the queue. This method returns null
   * if there are no objects on the queue.
   * 
   * @return The object taken
   * 
   * @throws CacheException
   * @throws InterruptedException
   */
  public Object take() throws CacheException, InterruptedException;

  /**
   * Takes up to batchSize number of objects from the head of the queue. As soon
   * as it gets a null response from a take, it stops taking.
   * 
   * @param batchSize
   *          The number of objects to take from the queue
   * 
   * @return the <code>List</code> of objects taken from the queue
   * 
   * @throws CacheException
   * @throws InterruptedException
   */
  public List take(int batchSize) throws CacheException, InterruptedException;

  /**
   * Removes a single object from the head of the queue without returning it.
   * This method assumes that the queue contains at least one object.
   * 
   * @throws InterruptedException
   * @throws CacheException
   */
  public void remove() throws InterruptedException, CacheException;

  /**
   * Peeks the first object from the head of the queue without removing it. This
   * method returns null if there are no objects on the queue.
   * 
   * @return The object peeked
   * @throws InterruptedException
   * @throws CacheException
   */
  public Object peek() throws InterruptedException, CacheException;

  /**
   * Peeks up to batchSize number of objects from the head of the queue without
   * removing them. As soon as it gets a null response from a peek, it stops
   * peeking.
   * 
   * @param batchSize
   *          The number of objects to peek from the queue
   * 
   * @return The list of objects peeked
   * @throws InterruptedException
   * @throws CacheException
   */
  public List peek(int batchSize) throws InterruptedException, CacheException;

  /**
   * Peeks either a batchSize number of elements from the queue or until
   * timeToWait milliseconds have elapsed (whichever comes first). If it has
   * peeked batchSize number of elements from the queue before timeToWait
   * milliseconds have elapsed, it stops peeking. If timeToWait milliseconds
   * elapse before batchSize number of elements has been peeked, it stops.
   * 
   * @param batchSize
   *          The number of objects to peek from the queue
   * @param timeToWait
   *          The number of milliseconds to attempt to peek
   * 
   * @return The list of objects peeked
   * @throws InterruptedException
   * @throws CacheException
   * 
   */
  public List peek(int batchSize, int timeToWait) throws  InterruptedException, CacheException;

  /**
   * Returns the size of the queue
   * 
   * @return the size of the queue
   */
  public int size();

  /**
   * Add a <code>CacheListener</code> to the queue
   * 
   * @param listener
   *          The <code>CacheListener</code> to add
   */
  public void addCacheListener(CacheListener listener);

  /**
   * Remove the <code>CacheListener</code> from the queue
   */
  public void removeCacheListener();

  //TODO:Asif: Remove this method. Added this justto make it compilable
  public void remove(int top) throws CacheException;
}
