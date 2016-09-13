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
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to an {@link AsyncEventQueue}.
 * 
 * @since GemFire 7.0
 * 
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
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
