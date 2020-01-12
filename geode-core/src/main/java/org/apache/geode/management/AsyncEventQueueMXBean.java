/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management;

import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.management.internal.security.ResourceOperation;
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
  String getId();

  /**
   * Returns the name of the disk store that is used for persistence.
   */
  String getOverflowDiskStoreName();

  /**
   * Returns the maximum memory after which the data needs to be overflowed to disk.
   */
  int getMaximumQueueMemory();

  /**
   * Returns the size of a batch that gets delivered over the AsyncEventQueue.
   */
  int getBatchSize();

  /**
   * Returns the interval between transmissions by the AsyncEventQueue.
   */
  long getBatchTimeInterval();

  /**
   * Returns whether batch conflation for the AsyncEventQueue is enabled
   *
   * @return True if batch conflation is enabled, false otherwise.
   */
  boolean isBatchConflationEnabled();

  /**
   * Returns whether the AsyncEventQueue is configured to be persistent or non-persistent.
   *
   * @return True if the queue is persistent, false otherwise.
   */
  boolean isPersistent();

  /**
   * Returns whether the queue is primary or secondary. Events get delivered only by the primary
   * queue. If the primary queue goes down then the secondary queue first becomes primary and then
   * starts delivering the events.
   *
   * @return True if this is the primary queue, false otherwise.
   */
  boolean isPrimary();

  /**
   * Returns the number of dispatcher threads working for this <code>AsyncEventQueue</code>.
   */
  int getDispatcherThreads();

  /**
   * Returns the order policy followed while dispatching the events to remote distributed system.
   * Order policy is only relevant when the number of dispatcher threads is greater than one.
   */

  String getOrderPolicy();

  /**
   * Returns whether the isDiskSynchronous property is set for this AsyncEventQueue.
   *
   * @return True if the property is set, false otherwise.
   */
  boolean isDiskSynchronous();

  /**
   * Returns whether the isParallel property is set for this AsyncEventQueue.
   *
   * @return True if the property is set, false otherwise.
   */
  boolean isParallel();

  /**
   * Returns the class name of the AsyncEventListener that is attached to the queue.
   */
  String getAsyncEventListener();

  /**
   * Returns the Size of the event queue
   *
   */
  int getEventQueueSize();

  /**
   * Returns the rate of LRU evictions per second by this Sender.
   */
  float getLRUEvictionsRate();

  /**
   * Returns the number of entries overflowed to disk for this Sender.
   */
  long getEntriesOverflowedToDisk();

  /**
   * Returns the number of bytes overflowed to disk for this Sender.
   */
  long getBytesOverflowedToDisk();

  /**
   * Returns the state of the event dispatcher.
   *
   * @return True if the dispatcher is paused, false otherwise.
   */
  boolean isDispatchingPaused();

}
