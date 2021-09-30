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
package org.apache.geode.cache.wan;

import java.util.List;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 *
 *
 * @since GemFire 7.0
 */
public interface GatewaySender {

  /**
   * The default value (false) for manually starting a <code>GatewaySender</code>.
   *
   * @deprecated - Manual start of senders is deprecated and will be removed in a later release.
   */
  @Deprecated
  boolean DEFAULT_MANUAL_START = false;

  /**
   * The default value ( true) for writing to disk synchronously in case of persistence.
   */
  boolean DEFAULT_DISK_SYNCHRONOUS = true;
  /**
   * The default buffer size for socket buffers from a sending GatewaySender to its receiving
   * <code>GatewayReceiver</code>.
   */
  int DEFAULT_SOCKET_BUFFER_SIZE = 524288;

  /**
   * The default amount of time in milliseconds that a socket read between a sending
   * <code>Gateway</code> and its receiving <code>Gateway</code> will block.
   */
  int DEFAULT_SOCKET_READ_TIMEOUT = Integer
      .getInteger(
          GeodeGlossary.GEMFIRE_PREFIX + "cache.gatewaySender.default-socket-read-timeout", 0)
      .intValue();

  /**
   * The default minimum socket read timeout.
   */
  int MINIMUM_SOCKET_READ_TIMEOUT = 30000;

  /**
   * Size of the oplog file used for the persistent queue in bytes
   */
  int QUEUE_OPLOG_SIZE =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "cache.gatewaySender.queueOpLogSize",
          1024 * 1024 * 100).intValue();


  /**
   * The default value (false)of whether to persist queue data to disk or not.
   */
  boolean DEFAULT_PERSISTENCE_ENABLED = false;


  /**
   * The default batch conflation
   */
  boolean DEFAULT_BATCH_CONFLATION = false;

  /**
   * The default batch size
   */
  int DEFAULT_BATCH_SIZE = 100;

  /**
   * The default batch time interval in milliseconds
   */
  int DEFAULT_BATCH_TIME_INTERVAL = 1000;

  /**
   * The default alert threshold in milliseconds
   */
  int DEFAULT_ALERT_THRESHOLD = 0;

  int DEFAULT_PARALLELISM_REPLICATED_REGION = Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX
      + "cache.gatewaySender.defaultParallelismForReplicatedRegion", 113).intValue();

  int DEFAULT_DISTRIBUTED_SYSTEM_ID = -1;

  int DEFAULT_DISPATCHER_THREADS = 5;

  boolean DEFAULT_FORWARD_EXPIRATION_DESTROY = false;

  @Immutable
  OrderPolicy DEFAULT_ORDER_POLICY = OrderPolicy.KEY;
  /**
   * The default maximum amount of memory (MB) to allow in the queue before overflowing entries to
   * disk
   */
  int DEFAULT_MAXIMUM_QUEUE_MEMORY = 100;

  /**
   * Time, in seconds, that we allow before a <code>GatewaySender is considered dead and should be
   * aborted
   */
  long GATEWAY_SENDER_TIMEOUT = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "GATEWAY_SENDER_TIMEOUT", 30).intValue();


  /**
   * The obsolete socket read timeout java system property. Since customers have been given this
   * property, it is used to log a warning.
   */
  String GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.GATEWAY_CONNECTION_READ_TIMEOUT";

  int GATEWAY_CONNECTION_IDLE_TIMEOUT = Integer
      .getInteger(
          GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.GATEWAY_CONNECTION_IDLE_TIMEOUT", -1)
      .intValue();

  /**
   * If the System property is set, use it. Otherwise, set default to 'true'.
   */
  boolean REMOVE_FROM_QUEUE_ON_EXCEPTION = (System.getProperty(
      GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION") != null)
          ? Boolean.getBoolean(
              GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION")
          : true;

  boolean EARLY_ACK =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.EARLY_ACK");

  boolean DEFAULT_IS_PARALLEL = false;

  boolean DEFAULT_MUST_GROUP_TRANSACTION_EVENTS = false;

  boolean DEFAULT_IS_FOR_INTERNAL_USE = false;

  boolean DEFAULT_ENFORCE_THREADS_CONNECT_SAME_RECEIVER = false;

  /**
   * Retry a connection from sender to receiver after specified time interval (in milliseconds) in
   * case receiver is not up and running. Default is set to 1000 milliseconds i.e. 1 second.
   */
  int CONNECTION_RETRY_INTERVAL = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "gateway-connection-retry-interval", 1000)
      .intValue();

  /**
   * Number of times to retry to get events for a transaction from the gateway sender queue when
   * group-transaction-events is set to true.
   * When group-transaction-events is set to true and a batch ready to be sent does not contain
   * all the events for all the transactions to which the events belong, the gateway sender will try
   * to get the missing events of the transactions from the queue to add them to the batch
   * before sending it.
   * If the missing events are not in the queue when the gateway sender tries to get them
   * it will retry for a maximum of times equal to the value set in this parameter before
   * delivering the batch without the missing events and logging an error.
   * Setting this parameter to a very low value could cause that under heavy load and
   * group-transaction-events set to true, batches are sent with incomplete transactions. Setting it
   * to a high value could cause that under heavy load and group-transaction-events set to true,
   * batches are held for some time before being sent.
   */
  int GET_TRANSACTION_EVENTS_FROM_QUEUE_RETRIES =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "get-transaction-events-from-queue-retries",
          10);
  /**
   * Milliseconds to wait before retrying to get events for a transaction from the
   * gateway sender queue when group-transaction-events is true.
   */
  int GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS =
      SystemPropertyHelper.getProductIntegerProperty(
          SystemPropertyHelper.GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS).orElse(1);

  /**
   * The order policy. This enum is applicable only when concurrency-level is > 1.
   *
   * @since GemFire 6.5.1
   */
  enum OrderPolicy {
    /**
     * Indicates that events will be parallelized based on the event's originating member and thread
     */
    THREAD,
    /**
     * Indicates that events will be parallelized based on the event's key
     */
    KEY,
    /**
     * Indicates that events will be parallelized based on the event's: - partition (using the
     * PartitionResolver) in the case of a partitioned region event - key in the case of a
     * replicated region event
     */
    PARTITION
  }

  /**
   * Starts this GatewaySender. Once the GatewaySender is running, its configuration cannot be
   * changed.
   */
  void start();

  /**
   * Starts this GatewaySender and discards previous queue content. Once the GatewaySender is
   * running, its configuration cannot be changed.
   */
  void startWithCleanQueue();

  /**
   * Stops this GatewaySender. The scope of this operation is the VM on which it is invoked. In case
   * the GatewaySender is parallel, the GatewaySender will be stopped on individual node where this
   * API is called. If the GatewaySender is not parallel, then the GatewaySender will stop on this
   * VM and the secondary GatewaySender will become primary and start dispatching events.
   *
   * The GatewaySender will wait for GatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME seconds before
   * stopping itself. If the system property is set to -1 then it will wait until all the events are
   * dispatched from the queue.
   *
   * @see GatewaySender#isParallel()
   */
  void stop();

  /**
   * Pauses the dispatching of the events from the underlying queue. It should be kept in mind that
   * the events will still be getting queued into the queue. The scope of this operation is the VM
   * on which it is invoked. In case the GatewaySender is parallel, the GatewaySender will be paused
   * on individual node where this API is called and the GatewaySender on other VM's can still
   * dispatch events. In case the GatewaySender is not parallel, and the running GatewaySender on
   * which this API is invoked is not primary then PRIMARY GatewaySender will still continue
   * dispatching events.
   *
   * The batch of events that are in the process of being dispatched are dispatched irrespective of
   * the state of pause operation. We can expect maximum of one batch of events being received at
   * the GatewayReceiver even after the GatewaySenders were paused.
   *
   * @see GatewaySender#isParallel()
   * @see GatewaySender#getBatchSize()
   * @see GatewaySender#resume()
   */
  void pause();

  /**
   * Resumes this paused GatewaySender.
   */
  void resume();

  /**
   * Rebalances this GatewaySender.
   */
  void rebalance();

  /**
   * Returns whether or not this GatewaySender is running.
   */
  boolean isRunning();

  /**
   * Returns whether or not this GatewaySender is paused.
   *
   */
  boolean isPaused();

  /**
   * Adds the provided <code>GatewayEventFilter</code> to this GatewaySender.
   *
   */
  void addGatewayEventFilter(GatewayEventFilter filter);

  /**
   * Removes the provided <code>GatewayEventFilter</code> from this GatewaySender.
   *
   */
  void removeGatewayEventFilter(GatewayEventFilter filter);

  /**
   * Returns this <code>GatewaySender's</code> <code>GatewayEventSubstitutionFilter</code>.
   *
   * @return this <code>GatewaySender's</code> <code>GatewayEventSubstitutionFilter</code>
   */
  GatewayEventSubstitutionFilter getGatewayEventSubstitutionFilter();

  /**
   * Returns the id of this GatewaySender.
   *
   * @return the id of this GatewaySender.
   */
  String getId();

  /**
   * Returns the id of the remote <code>GatewayReceiver</code>'s DistributedSystem.
   *
   * @return the id of the remote <code>GatewayReceiver</code>'s DistributedSystem.
   */
  int getRemoteDSId();

  /**
   * Returns the configured buffer size of the socket connection between this GatewaySender and its
   * receiving <code>GatewayReceiver</code>. The default is 32768 bytes.
   *
   * @return the configured buffer size of the socket connection between this GatewaySender and its
   *         receiving <code>GatewayReceiver</code>
   */
  int getSocketBufferSize();

  /**
   * Returns the amount of time in milliseconds that a socket read between a sending GatewaySender
   * and its receiving <code>GatewayReceiver</code> will block. The default value is 0 which is
   * interpreted as infinite timeout.
   *
   * @return the amount of time in milliseconds that a socket read between a sending GatewaySender
   *         and its receiving <code>GatewayReceiver</code> will block
   */
  int getSocketReadTimeout();

  /**
   * Gets the disk store name for overflow or persistence.
   *
   * @return disk store name
   */
  String getDiskStoreName();

  /**
   * Returns the maximum amount of memory (in MB) for a GatewaySender's queue. The default is 100.
   *
   * @return maximum amount of memory (in MB) for a GatewaySender's queue
   */
  int getMaximumQueueMemory();

  /**
   * Returns the batch size for this GatewaySender. Default batchSize is 100.
   *
   * @return the batch size for this GatewaySender.
   */
  int getBatchSize();

  /**
   * Returns the batch time interval for this GatewaySender. Default value of batchTimeInterval is
   * 1000.
   *
   * @return the batch time interval for this GatewaySender
   */
  int getBatchTimeInterval();

  /**
   * Answers whether to enable batch conflation for a GatewaySender 's queue. The default value is
   * false.
   *
   * @return whether to enable batch conflation for batches sent from a GatewaySender to its
   *         corresponding <code>GatewayReceiver</code>.
   */
  boolean isBatchConflationEnabled();

  /**
   * Returns true if persistence is enabled for this GatewaySender, otherwise returns false. Default
   * is false if not set explicitly.
   *
   * @return true if persistence is enabled for this GatewaySender
   */
  boolean isPersistenceEnabled();

  /**
   * Returns the alert threshold in milliseconds for entries in a GatewaySender's queue. Default
   * value is 0.
   *
   * @return the alert threshold for entries in a GatewaySender's queue
   *
   */
  int getAlertThreshold();

  /**
   * Returns the list of <code>GatewayEventFilter</code> added to this GatewaySender.
   *
   * @return the list of <code>GatewayEventFilter</code> added to this GatewaySender.
   */
  List<GatewayEventFilter> getGatewayEventFilters();

  /**
   * Returns the list of <code>GatewayTransportFilter</code> added to this GatewaySender.
   *
   * @return the list of <code>GatewayTransportFilter</code> added to this GatewaySender.
   */

  List<GatewayTransportFilter> getGatewayTransportFilters();

  /**
   * Returns isDiskSynchronous boolean property for this GatewaySender. Default value is true.
   *
   * @return isDiskSynchronous boolean property for this GatewaySender
   *
   */
  boolean isDiskSynchronous();

  /**
   * Returns the manual start boolean property for this GatewaySender. Default is false i.e. the
   * GatewaySender will automatically start once created.
   *
   * @return the manual start boolean property for this GatewaySender
   *
   * @deprecated - Manual start of senders is deprecated and will be removed in a later release.
   */
  @Deprecated
  boolean isManualStart();

  /**
   * Returns isParallel boolean property for this GatewaySender.
   *
   * @return isParallel boolean property for this GatewaySender
   *
   */
  boolean isParallel();

  /**
   * Returns groupTransactionEvents boolean property for this GatewaySender.
   *
   * @return groupTransactionEvents boolean property for this GatewaySender
   *
   */
  boolean mustGroupTransactionEvents();

  /**
   * Returns retriesToGetTransactionEventsFromQueue int property for this GatewaySender.
   *
   * @return retriesToGetTransactionEventsFromQueue int property for this GatewaySender
   *
   */
  int getRetriesToGetTransactionEventsFromQueue();

  /**
   * Returns the number of dispatcher threads working for this <code>GatewaySender</code>. Default
   * number of dispatcher threads is 5.
   *
   * @return the number of dispatcher threads working for this <code>GatewaySender</code>
   */
  int getDispatcherThreads();

  /**
   * Returns the order policy followed while dispatching the events to remote ds. Order policy is
   * set only when dispatcher threads are > 1. Default value of order policy is KEY.
   *
   * @return the order policy followed while dispatching the events to remote ds.
   */

  OrderPolicy getOrderPolicy();

  int getMaxParallelismForReplicatedRegion();

  /**
   * Destroys the GatewaySender.
   * <p>
   * In case of ParallelGatewaySender, the destroy operation does distributed destroy of the Queue
   * Region. In case of SerialGatewaySender, the Queue Region is destroyed locally.
   *
   * @since Geode 1.1
   *
   */
  void destroy();

  /**
   * Returns enforceThreadsConnectSameReceiver boolean property for this GatewaySender.
   *
   * @return enforceThreadsConnectSameReceiver boolean property for this GatewaySender
   *
   */
  boolean getEnforceThreadsConnectSameReceiver();


  /**
   * Set AlertThreshold for this GatewaySender.
   *
   * @since Geode 1.14
   *
   */
  void setAlertThreshold(int alertThreshold);

  /**
   * Set BatchSize for this GatewaySender.
   *
   * @since Geode 1.14
   *
   */
  void setBatchSize(int batchSize);

  /**
   * Set BatchTimeInterval for this GatewaySender.
   *
   * @since Geode 1.14
   *
   */
  void setBatchTimeInterval(int batchTimeInterval);

  /**
   * Set GroupTransactionEvents for this GatewaySender.
   *
   * @since Geode 1.14
   *
   */
  void setGroupTransactionEvents(boolean groupTransactionEvents);

  /**
   * Set GatewayEventFilters for this GatewaySender.
   *
   * @since Geode 1.14
   *
   */
  void setGatewayEventFilters(List<GatewayEventFilter> filters);

}
