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

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

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
  public String getSenderId();

  /**
   * Returns the id of the remote <code>GatewayReceiver</code>'s
   * DistributedSystem.
   */
  public int getRemoteDSId();

  /**
   * Returns the configured buffer size of the socket connection between this
   * GatewaySender and its receiving <code>GatewayReceiver</code>.
   */
  public int getSocketBufferSize();

  /**
   * Returns the amount of time (in milliseconds) that a socket read between a
   * sending GatewaySender and its receiving <code>GatewayReceiver</code> is
   * allowed to block.
   */
  public long getSocketReadTimeout();

  /**
   * Returns the name of the disk store that is used for persistence.
   */
  public String getOverflowDiskStoreName();

  /**
   * Returns the maximum memory after which the data needs to be overflowed to disk.
   */
  public int getMaximumQueueMemory();

  /**
   * Returns the size of a batch that gets delivered by the GatewaySender.
   */
  public int getBatchSize();

  /**
   * Returns the interval between transmissions by the GatewaySender.
   */
  public long getBatchTimeInterval();

  /**
   * Returns whether batch conflation for the GatewaySender's queue is enabled
   * 
   * @return True if batch conflation is enabled, false otherwise.
   */
  public boolean isBatchConflationEnabled();

  /**
   * Returns whether the GatewaySender is configured to be persistent or
   * non-persistent.
   * 
   * @return True if the sender is persistent, false otherwise.
   */

  public boolean isPersistenceEnabled();

  /**
   * Returns the alert threshold for entries in a GatewaySender's queue.The
   * default value is 0 milliseconds in which case no alert will be logged if
   * events are delayed in Queue.
   */
  public int getAlertThreshold();

  /**
   * Returns a list of <code>GatewayEventFilter</code>s added to this
   * GatewaySender.
   */
  public String[] getGatewayEventFilters();

  /**
   * Returns a list of <code>GatewayTransportFilter</code>s added to this
   * GatewaySender.
   */
  public String[] getGatewayTransportFilters();

  /**
   * Returns whether the GatewaySender is configured for manual start.
   * 
   * @return True if the GatewaySender is configured for manual start, false otherwise.
   */
  public boolean isManualStart();

  /**
   * Returns whether or not this GatewaySender is running.
   * 
   * @return True if the GatewaySender is running, false otherwise.
   */
  public boolean isRunning();

  /**
   * Returns whether or not this GatewaySender is paused.
   * 
   * @return True of the GatewaySender is paused, false otherwise.
   */
  public boolean isPaused();

  /**
   * Returns the rate of events received per second by this Sender if it's a
   * serial-wan.
   */
  public float getEventsReceivedRate();

  /**
   * Returns the rate of events being queued.
   */
  public float getEventsQueuedRate();

  /**
   * Returns the current size of the event queue.
   */
  public int getEventQueueSize();

  /**
   * Returns the number of events received, but not added to the event queue, because
   * the queue already contains an event with the same key.
   */
  public int getTotalEventsConflated();


  /**
   * Returns the average number of batches sent per second.
   */
  public float getBatchesDispatchedRate();

  /**
   * Returns the average time taken to send a batch of events.
   */
  public long getAverageDistributionTimePerBatch();

  /**
   * Returns the total number of batches of events that were resent.
   */
  public int getTotalBatchesRedistributed();

  /**
   * Starts this GatewaySender. Once the GatewaySender is running its
   * configuration cannot be changed.
   * 
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void start();

  /**
   * Stops this GatewaySender.
   */
  @ResourceOperation(resource=Resource.DATA, operation=Operation.MANAGE)
  public void stop();

  /**
   * Pauses this GatewaySender.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void pause();

  /**
   * Resumes this paused GatewaySender.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void resume();

  /**
   * Rebalances this GatewaySender.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void rebalance();
  
  /**
   * Returns whether this GatewaySender is primary or secondary.
   * 
   * @return True if this is the primary, false otherwise.
   */
  public boolean isPrimary();
  
  /**
   * Returns the number of dispatcher threads working for this <code>GatewaySender</code>.
   */
  public int getDispatcherThreads();
  
  /**
   * Returns the order policy followed while dispatching the events to remote
   * distributed system. Order policy is only relevant when the number of dispatcher
   * threads is greater than one.
   */
  
  public String getOrderPolicy();
 
  /**
   * Returns whether the isDiskSynchronous property is set for this GatewaySender.
   * 
   * @return True if the property is set, false otherwise.
   */
  public boolean isDiskSynchronous();

  /**
   * Returns whether the isParallel property is set for this GatewaySender.
   * 
   * @return True if the property is set, false otherwise.
   */
  public boolean isParallel();

  /**
   * Returns the host and port information of GatewayReceiver to which this
   * gateway sender is connected.
   */
  public String getGatewayReceiver();
  
  /**
   * Returns whether this GatewaySender is connected and sending data to a
   * GatewayReceiver.
   */
  public boolean isConnected();
  
  /**
   * Returns number of events which have exceeded the configured alert threshold.
   */
  public int getEventsExceedingAlertThreshold();

  

}
