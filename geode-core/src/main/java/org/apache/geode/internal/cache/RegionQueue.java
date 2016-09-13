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

package org.apache.geode.internal.cache;

import org.apache.geode.cache.*;

import java.util.List;

/**
 * Interface <code>RegionQueue</code> is an interface for queue
 * implementations backed by regions.
 * 
 * 
 * @since GemFire 4.2
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
   * @return boolean whether object was successfully put onto the queue
   */
  public boolean put(Object object) throws InterruptedException, CacheException;

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

  public void close();
}
