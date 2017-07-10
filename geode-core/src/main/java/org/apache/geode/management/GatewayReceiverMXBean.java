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

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;

/**
 * MBean that provides access to information and management functionality for a
 * {@link GatewayReceiver}.
 * 
 * @since GemFire 7.0
 * 
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface GatewayReceiverMXBean {

  /**
   * Returns the port the receiver is listening on.
   */
  int getPort();

  /**
   * Returns the configured buffer size of the socket connection.
   */
  int getSocketBufferSize();

  /**
   * Returns the bind address on the host.
   */
  String getBindAddress();

  /**
   * Returns the maximum amount of time between client pings.
   */
  int getMaximumTimeBetweenPings();

  /**
   * Returns whether the receiver is in running state.
   * 
   * @return True if the receiver is in a running state, false otherwise.
   */
  boolean isRunning();

  /**
   * Returns the instantaneous rate of events received.
   */
  float getEventsReceivedRate();

  /**
   * Returns the rate of create requests received.
   */
  float getCreateRequestsRate();

  /**
   * Returns the rate of update requests received.
   */
  float getUpdateRequestsRate();

  /**
   * Returns the rate of destroy requests received.
   */
  float getDestroyRequestsRate();

  /**
   * Returns the number of duplicate batches which have been received.
   */
  int getDuplicateBatchesReceived();

  /**
   * Returns the number of batches which have been received out of order.
   */
  int getOutoforderBatchesReceived();

  /**
   * Starts the gateway receiver.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.GATEWAY)
  void start() throws Exception;

  /**
   * Stops the gateway receiver.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.GATEWAY)
  void stop() throws Exception;

  /**
   * Returns the configured start port.
   */
  int getStartPort();

  /**
   * Returns the configured end port.
   */
  int getEndPort();

  /**
   * Returns a list of names for the transport filters in use.
   */
  String[] getGatewayTransportFilters();

  /**
   * Returns the number of sockets accepted and used for client to server messaging.
   */
  int getClientConnectionCount();

  /**
   * Returns the number of client virtual machines connected and acting as a gateway.
   */
  int getNumGateways();

  /**
   * Returns the average get request latency.
   */
  long getGetRequestAvgLatency();

  /**
   * Returns the average put request latency.
   */
  long getPutRequestAvgLatency();

  /**
   * Returns the total number of client connections that timed out and were closed.
   */
  int getTotalConnectionsTimedOut();

  /**
   * Returns the total number of client connection requests that failed.
   */
  int getTotalFailedConnectionAttempts();

  /**
   * Returns the current number of connections waiting for a thread to start processing their
   * message.
   */
  int getThreadQueueSize();

  /**
   * Returns the current number of threads handling a client connection.
   */
  int getConnectionThreads();

  /**
   * Returns the load from client to server connections as reported by the load probe installed in
   * this server.
   */
  double getConnectionLoad();

  /**
   * Returns the estimate of how much load is added for each new connection as reported by the load
   * probe installed in this server.
   */
  double getLoadPerConnection();

  /**
   * Returns the load from queues as reported by the load probe installed in this server.
   */
  double getQueueLoad();

  /**
   * Returns the estimate of how much load is added for each new queue as reported by the load probe
   * installed in this server.
   */
  double getLoadPerQueue();

  /**
   * Returns the rate of get requests.
   */
  float getGetRequestRate();

  /**
   * Returns the rate of put requests.
   */
  float getPutRequestRate();

  /**
   * Returns the total number of bytes sent to clients.
   */
  long getTotalSentBytes();

  /**
   * Returns the total number of bytes received from clients.
   */
  long getTotalReceivedBytes();

  /**
   * Returns a list of the host and port information for gateway senders connected to this gateway
   * receiver.
   */
  String[] getConnectedGatewaySenders();

  /**
   * Returns the average batch processing time (in milliseconds).
   */
  long getAverageBatchProcessingTime();

}
