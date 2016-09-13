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

package org.apache.geode.cache.wan;

import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;

/**
 * Factory to create SerialGatewaySender
 * 
 * 
 * @since GemFire 7.0
 * @see GatewaySender
 * 
 */
public interface GatewaySenderFactory {

  /**
   * Indicates whether all VMs need to distribute events to remote site.
   * In this case only the events originating in a particular VM will be in dispatched in order.
   *
   * @param isParallel
   *          boolean to indicate whether distribution policy is parallel
   */
  public GatewaySenderFactory setParallel(boolean isParallel);
  
  /**
   * Adds a <code>GatewayEventFilter</code>
   * 
   * @param filter
   *          GatewayEventFilter
   */
  public GatewaySenderFactory addGatewayEventFilter(
      GatewayEventFilter filter);

  /**
   * Adds a <code>GatewayTransportFilter</code>
   * 
   * @param filter
   *          GatewayTransportFilter
   */
  public GatewaySenderFactory addGatewayTransportFilter(
      GatewayTransportFilter filter);

  /**
   * Sets the buffer size in bytes of the socket connection for this
   * <code>GatewaySender</code>. The default is 32768 bytes.
   * 
   * @param size
   *          The size in bytes of the socket buffer
   */
  public GatewaySenderFactory setSocketBufferSize(int size);

  /**
   * Sets the number of milliseconds to wait for a response from a
   * <code>GatewayReceiver</code> before timing out the operation and trying
   * another <code>GatewayReceiver</code> (if any are available).
   * Default is 0 which means infinite timeout.
   * 
   * @param timeout
   *          number of milliseconds to wait for a response from a
   *          GatewayReceiver
   * @throws IllegalArgumentException
   *           if <code>timeout</code> is less than <code>0</code>.
   */
  public GatewaySenderFactory setSocketReadTimeout(int timeout);

  /**
   * Sets the disk store name for overflow or persistence
   * 
   * @param name
   */
  public GatewaySenderFactory setDiskStoreName(String name);

  /**
   * Sets the number of dispatcher thread.
   * Default number of dispatcher threads is 5.
   * 
   * @param numThreads
   */
  public GatewaySenderFactory setDispatcherThreads(int numThreads);

  /**
   * Sets <code>OrderPolicy</code> for this GatewaySender.
   * Default order policy is KEY.
   * 
   * @param policy
   */
  public GatewaySenderFactory setOrderPolicy(OrderPolicy policy);
  
  /**
   * Sets the maximum amount of memory (in MB) for a
   * <code>GatewaySender</code>'s queue.
   * Default is 100.
   * 
   * @param maxQueueMemory
   *           The maximum amount of memory (in MB) for a
   *          <code>GatewaySender</code>'s queue.
   */
  public GatewaySenderFactory setMaximumQueueMemory(int maxQueueMemory);

  /**
   * Sets the batch size to be picked at the time of dispatching from a <code>GatewaySender</code>'s queue.
   * Default batchSize is 100.
   * 
   * @param size
   *          The size of batches sent from a <code>GatewaySender</code> to its
   *          corresponding <code>GatewayReceiver</code>.
   */
  public GatewaySenderFactory setBatchSize(int size);

  /**
   * Sets a time interval in milliseconds to wait to form a batch to be dispatched from a <code>GatewaySender</code>'s
   * queue.
   * Default is 1000.
   * 
   * @param interval
   *          The maximum time interval (in milliseconds) that can elapse before a partial batch
   *          is sent from a <code>GatewaySender</code> to its corresponding
   *          <code>GatewayReceiver</code>.
   */
  public GatewaySenderFactory setBatchTimeInterval(int interval);

  /**
   * Sets whether to enable batch conflation for a
   * <code>GatewaySender</code>'s queue.
   * Default is false.
   * 
   * @param isConflation
   *          Whether or not to enable batch conflation for batches sent from a
   *          <code>GatewaySender</code> to its corresponding
   *          <code>GatewayReceiver</code>.
   */
  public GatewaySenderFactory setBatchConflationEnabled(
      boolean isConflation);

  /**
   * Sets whether to enable persistence for a <code>GatewaySender</code>'s
   * queue.
   * Default is false.
   * 
   * @param isPersistence
   *          Whether to enable persistence for a
   *          <code>GatewaySender</code>'s queue
   */
  public GatewaySenderFactory setPersistenceEnabled(boolean isPersistence);

  /**
   * Sets the alert threshold in milliseconds for entries in a <code>GatewaySender</code>
   * 's queue.
   * Default value is 0.
   * 
   * @param threshold
   *          the alert threshold for entries in a
   *          <code>GatewaySender</code>'s queue
   */
  public GatewaySenderFactory setAlertThreshold(int threshold);

  /**
   * Sets the manual start boolean property for this
   * <code>GatewaySender</code>.
   * Default is false i.e. the <code>GatewaySender</code> will automatically start once created.
   * 
   * @param start
   *          the manual start boolean property for this
   *          <code>GatewaySender</code>
   */
  public GatewaySenderFactory setManualStart(boolean start);
  
  /**
   * Sets whether or not the writing to the disk is synchronous.
   * Default is true.
   * 
   * @param isSynchronous
   *          boolean if true indicates synchronous writes
   *  
   */
  public GatewaySenderFactory setDiskSynchronous(boolean isSynchronous);
  
  /**
   * Removes the provided <code>GatewayEventFilter</code> from this
   * GatewaySender.
   * 
   * @param filter
   */
  public GatewaySenderFactory removeGatewayEventFilter(GatewayEventFilter filter);

  /**
   * Removes the provided <code>GatewayTransportFilter</code> from this
   * GatewaySender.
   * 
   * @param filter
   */
  public GatewaySenderFactory removeGatewayTransportFilter(GatewayTransportFilter filter);

  public GatewaySenderFactory setParallelFactorForReplicatedRegion(int parallel);
  
  /**
   * Sets the provided <code>GatewayEventSubstitutionFilter</code> in this
   * GatewaySenderFactory.
   * 
   * @param filter The <code>GatewayEventSubstitutionFilter</code>
   */
  public GatewaySenderFactory setGatewayEventSubstitutionFilter(
      GatewayEventSubstitutionFilter filter);

  /**
   * Creates a <code>GatewaySender</code> to communicate with remote
   * distributed system
   * 
   * @param id
   *          unique id for this SerialGatewaySender
   * @param remoteDSId
   *          unique id representing the remote distributed system
   * @return instance of SerialGatewaySender
   * @throws IllegalStateException 
   *           If the GatewaySender creation fails during validation due to mismatch of 
   *           attributes of GatewaySender created on other nodes with same id 
   */ 
  public GatewaySender create(String id, int remoteDSId);

}
