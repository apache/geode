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

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;

/**
 * MBean that provides access to information and management functionality for a
 * {@link GatewaySender}.
 *
 * @since GemFire 7.0
 *
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface GatewaySenderMXBean {

  /**
   * Returns the ID of the GatewaySender.
   */
  String getSenderId();

  /**
   * Returns the id of the remote <code>GatewayReceiver</code>'s DistributedSystem.
   */
  int getRemoteDSId();

  /**
   * Returns the configured buffer size of the socket connection between this GatewaySender and its
   * receiving <code>GatewayReceiver</code>.
   */
  int getSocketBufferSize();

  /**
   * Returns the amount of time (in milliseconds) that a socket read between a sending GatewaySender
   * and its receiving <code>GatewayReceiver</code> is allowed to block.
   */
  long getSocketReadTimeout();

  /**
   * Returns the name of the disk store that is used for persistence.
   */
  String getOverflowDiskStoreName();

  /**
   * Returns the maximum memory after which the data needs to be overflowed to disk.
   */
  int getMaximumQueueMemory();

  /**
   * Returns the size of a batch that gets delivered by the GatewaySender.
   */
  int getBatchSize();

  /**
   * Returns the interval between transmissions by the GatewaySender.
   */
  long getBatchTimeInterval();

  /**
   * Returns whether batch conflation for the GatewaySender's queue is enabled
   *
   * @return True if batch conflation is enabled, false otherwise.
   */
  boolean isBatchConflationEnabled();

  /**
   * Returns whether the GatewaySender is configured to be persistent or non-persistent.
   *
   * @return True if the sender is persistent, false otherwise.
   */

  boolean isPersistenceEnabled();

  /**
   * Returns the alert threshold for entries in a GatewaySender's queue.The default value is 0
   * milliseconds in which case no alert will be logged if events are delayed in Queue.
   */
  int getAlertThreshold();

  /**
   * Returns a list of <code>GatewayEventFilter</code>s added to this GatewaySender.
   */
  String[] getGatewayEventFilters();

  /**
   * Returns a list of <code>GatewayTransportFilter</code>s added to this GatewaySender.
   */
  String[] getGatewayTransportFilters();

  /**
   * Returns whether the GatewaySender is configured for manual start.
   *
   * @return True if the GatewaySender is configured for manual start, false otherwise.
   */
  boolean isManualStart();

  /**
   * Returns whether or not this GatewaySender is running.
   *
   * @return True if the GatewaySender is running, false otherwise.
   */
  boolean isRunning();

  /**
   * Returns whether or not this GatewaySender is paused.
   *
   * @return True of the GatewaySender is paused, false otherwise.
   */
  boolean isPaused();

  /**
   * Returns the rate of events received per second by this Sender.
   */
  float getEventsReceivedRate();

  /**
   * Returns the rate of events being queued.
   */
  float getEventsQueuedRate();

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
   * Returns the current size of the event queue.
   */
  int getEventQueueSize();

  /**
   * Returns the number of events received, but not added to the event queue, because the queue
   * already contains an event with the same key.
   */
  int getTotalEventsConflated();


  /**
   * Returns the average number of batches sent per second.
   */
  float getBatchesDispatchedRate();

  /**
   * Returns the average time taken to send a batch of events.
   */
  long getAverageDistributionTimePerBatch();

  /**
   * Returns the total number of batches of events that were resent.
   */
  int getTotalBatchesDistributed();

  /**
   * Returns the total number of batches of events that were resent.
   */
  int getTotalBatchesRedistributed();

  /**
   * Returns the total number of batches sent with incomplete transactions.
   * Only relevant if group-transaction-events is enabled.
   */
  int getTotalBatchesWithIncompleteTransactions();

  /**
   * Returns the total number of bytes in heap occupied by the event queue.
   */
  long getTotalQueueSizeBytesInUse();

  /**
   * Starts this GatewaySender. Once the GatewaySender is running its configuration cannot be
   * changed.
   *
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.GATEWAY)
  void start();

  /**
   * Starts this GatewaySender and cleans previous queue content.
   * Once the GatewaySender is running its configuration cannot be changed.
   *
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.GATEWAY)
  void startWithCleanQueue();

  /**
   * Stops this GatewaySender.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.GATEWAY)
  void stop();

  /**
   * Pauses this GatewaySender.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.GATEWAY)
  void pause();

  /**
   * Resumes this paused GatewaySender.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.GATEWAY)
  void resume();

  /**
   * Rebalances this GatewaySender.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.GATEWAY)
  void rebalance();

  /**
   * Returns whether this GatewaySender is primary or secondary.
   *
   * @return True if this is the primary, false otherwise.
   */
  boolean isPrimary();

  /**
   * Returns the number of dispatcher threads working for this <code>GatewaySender</code>.
   */
  int getDispatcherThreads();

  /**
   * Returns the order policy followed while dispatching the events to remote distributed system.
   * Order policy is only relevant when the number of dispatcher threads is greater than one.
   */

  String getOrderPolicy();

  /**
   * Returns whether the isDiskSynchronous property is set for this GatewaySender.
   *
   * @return True if the property is set, false otherwise.
   */
  boolean isDiskSynchronous();

  /**
   * Returns whether the isParallel property is set for this GatewaySender.
   *
   * @return True if the property is set, false otherwise.
   */
  boolean isParallel();

  boolean mustGroupTransactionEvents();

  /**
   * Returns the host and port information of GatewayReceiver to which this gateway sender is
   * connected.
   */
  String getGatewayReceiver();

  /**
   * Returns whether this GatewaySender is connected and sending data to a GatewayReceiver.
   */
  boolean isConnected();

  /**
   * Returns number of events which have exceeded the configured alert threshold.
   */
  int getEventsExceedingAlertThreshold();



}
