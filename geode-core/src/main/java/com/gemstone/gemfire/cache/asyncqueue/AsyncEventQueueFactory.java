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
package com.gemstone.gemfire.cache.asyncqueue;

import java.util.List;

import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayEventSubstitutionFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;

/**
 * Factory to create the <code>AsyncEventQueue</code>. 
 * Below example illustrates how to get the instance of factory and create the 
 * <code>AsyncEventQueue</code>.
<PRE>
  Cache c = new CacheFactory().create();
  // get AsyncEventQueueFactory from cache
  AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
  
  // set the attributes on factory
  factory.setBatchSize(batchSize);
  factory.setBatchConflationEnabled(isConflation);
  factory.setMaximumQueueMemory(maxMemory);
  factory.setParallel(isParallel);
             .
             .
  // create instance of AsyncEventListener           
  AsyncEventListener asyncEventListener = new <AsyncEventListener class>;
  // create AsyncEventQueue by providing the id and instance of AsyncEventListener
  AsyncEventQueue asyncQueue = factory.create(asyncQueueId, asyncEventListener);
</PRE>
 * 
 * @since 7.0
 */
public interface AsyncEventQueueFactory {

  /**
   * Sets the disk store name for overflow or persistence.
   * 
   * @param name
   */
  public AsyncEventQueueFactory setDiskStoreName(String name);

  /**
   * Sets the maximum amount of memory (in MB) for an
   * <code>AsyncEventQueue</code>'s queue.
   * Default is 100 MB.
   * 
   * @param memory
   *          The maximum amount of memory (in MB) for an
   *          <code>AsyncEventQueue</code>'s queue
   */
  public AsyncEventQueueFactory setMaximumQueueMemory(int memory);
  
  /**
   * Sets whether or not the writing to the disk is synchronous.
   * Default is true.
   * 
   * @param isSynchronous
   *          boolean if true indicates synchronous writes
   *  
   */
  public AsyncEventQueueFactory setDiskSynchronous(boolean isSynchronous);

  /**
   * Sets the batch size for an <code>AsyncEventQueue</code>'s queue.
   * Default is 100.
   * 
   * @param size
   *          The size of batches sent to its <code>AsyncEventListener</code>
   */
  public AsyncEventQueueFactory setBatchSize(int size);
  
  /**
   * Sets the batch time interval (in milliseconds) for a <code>AsyncEventQueue</code>.
   * Default is 5 ms.
   * 
   * @param interval
   *          The maximum time interval that can elapse before a partial batch
   *          is sent from a <code>AsyncEventQueue</code>.
   */
  public AsyncEventQueueFactory setBatchTimeInterval(int interval);

  /**
   * Sets whether the <code>AsyncEventQueue</code> is persistent or not.
   * Default is false.
   * 
   * @param isPersistent
   *          Whether to enable persistence for an <code>AsyncEventQueue</code>.
   */
  public AsyncEventQueueFactory setPersistent(boolean isPersistent);

  /**
   * Indicates whether all VMs need to distribute events to remote site. In this
   * case only the events originating in a particular VM will be in dispatched
   * in order.
   * Default is false.
   * 
   * @param isParallel
   *          boolean to indicate whether distribution policy is parallel
   */

  public AsyncEventQueueFactory setParallel(boolean isParallel);
  
  /**
   * Sets whether to enable batch conflation for <code>AsyncEventQueue</code>.
   * Default is false.
   * 
   * @param     isConflation        
   *              Whether or not to enable batch conflation for batches sent from a 
   *              <code>AsyncEventQueue</code>
   */
  public AsyncEventQueueFactory setBatchConflationEnabled(boolean isConflation);
  
  /**
   * Sets the number of dispatcher thread.
   * Default is 5.
   * 
   * @param numThreads
   */
  public AsyncEventQueueFactory setDispatcherThreads(int numThreads);

  /**
   * Adds a <code>GatewayEventFilter</code> to the attributes of
   * AsyncEventQueue being created by factory.
   * 
   * @param filter
   *          GatewayEventFilter
   */
  public AsyncEventQueueFactory addGatewayEventFilter(GatewayEventFilter filter);

  /**
   * Removes the provided <code>GatewayEventFilter</code> from the attributes of
   * AsyncEventQueue being created by factory.
   * 
   * @param filter
   */
  public AsyncEventQueueFactory removeGatewayEventFilter(
      GatewayEventFilter filter);
  
  /**
   * Sets the order policy for multiple dispatchers.
   * Default is KEY.
   * 
   * @param policy
   */
  public AsyncEventQueueFactory setOrderPolicy(OrderPolicy policy);
  
  /**
   * Sets the <code>GatewayEventSubstitutionFilter</code>.
   * 
   * @param filter
   *          The <code>GatewayEventSubstitutionFilter</code>
   */
  public AsyncEventQueueFactory setGatewayEventSubstitutionListener(
      GatewayEventSubstitutionFilter filter);

  /**
   * Ignores the eviction and expiration events.
   * By default its set to ignore eviction and expiration events (true), by
   * setting it to false, the AEQ will receive destroy events due to eviction
   * and expiration action.
   * Note, setting this to false doesn't propagate invalidate events due to
   * expiration action.
   *
   * @param ignore 
   *        boolean to indicate whether to ignore eviction and expiration events. 
   */
  public AsyncEventQueueFactory setIgnoreEvictionAndExpiration(boolean ignore);

  
  /**
   * Creates the <code>AsyncEventQueue</code>. It accepts Id of AsyncEventQueue
   * and instance of AsyncEventListener. Multiple queues can be created using
   * Same listener instance. So, the instance of <code>AsyncEventListener</code>
   * should be thread safe in that case. The <code>AsyncEventListener</code>
   * will start receiving events when the <code>AsyncEventQueue</code> is
   * created.
   * 
   * 
   * @param id
   *          Id of AsyncEventQueue
   * @param listener
   *          <code>AsyncEventListener</code> to be added to the regions that
   *          are configured to use this queue.
   */
  public AsyncEventQueue create(String id, AsyncEventListener listener);
}
