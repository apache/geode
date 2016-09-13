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

import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

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
  public int getPort();

  /**
   * Returns the configured buffer size of the socket connection.
   */
  public int getSocketBufferSize();

  /**
   * Returns the bind address on the host.
   */
  public String getBindAddress();

  /**
   * Returns the maximum amount of time between client pings.
   */
  public int getMaximumTimeBetweenPings();

  /**
   * Returns whether the receiver is in running state.
   * 
   * @return True if the receiver is in a running state, false otherwise.
   */
  public boolean isRunning();

  /**
   * Returns the instantaneous rate of events received.
   */
  public float getEventsReceivedRate();

  /**
   * Returns the rate of create requests received.
   */
  public float getCreateRequestsRate();

  /**
   * Returns the rate of update requests received.
   */
  public float getUpdateRequestsRate();

  /**
   * Returns the rate of destroy requests received.
   */
  public float getDestroyRequestsRate();

  /**
   * Returns the number of duplicate batches which have been received.
   */
  public int getDuplicateBatchesReceived();

  /**
   * Returns the number of batches which have been received out of order.
   */
  public int getOutoforderBatchesReceived();

  /**
   * Starts the gateway receiver.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void start() throws Exception;

  /**
   * Stops the gateway receiver.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void stop() throws Exception;

  /**
   * Returns the configured start port.
   */
  public int getStartPort();

  /**
   * Returns the configured end port.
   */
  public int getEndPort();
  
  /**
   * Returns a list of names for the transport filters in use.
   */
  public String[] getGatewayTransportFilters();
  
  /**
   * Returns the number of sockets accepted and used for client to server messaging.
   */
  public int getClientConnectionCount();
  
  /**
   * Returns the number of client virtual machines connected and acting as a gateway.
   */
  public int getNumGateways();

  /**
   * Returns the average get request latency.
   */
  public long getGetRequestAvgLatency();

  /**
   * Returns the average put request latency.
   */
  public long getPutRequestAvgLatency();

  /**
   * Returns the total number of client connections that timed out and were
   * closed.
   */
  public int getTotalConnectionsTimedOut();

  /**
   * Returns the total number of client connection requests that failed.
   */
  public int getTotalFailedConnectionAttempts();

  /**
   * Returns the current number of connections waiting for a thread to start
   * processing their message.
   */
  public int getThreadQueueSize();

  /**
   * Returns the current number of threads handling a client connection.
   */
  public int getConnectionThreads();

  /**
   * Returns the load from client to server connections as reported by the load
   * probe installed in this server.
   */
  public double getConnectionLoad();

  /**
   * Returns the estimate of how much load is added for each new connection as
   * reported by the load probe installed in this server.
   */
  public double getLoadPerConnection();

  /**
   * Returns the load from queues as reported by the load probe installed in
   * this server.
   */
  public double getQueueLoad();

  /**
   * Returns the estimate of how much load is added for each new queue as
   * reported by the load probe installed in this server.
   */
  public double getLoadPerQueue();

  /**
   * Returns the rate of get requests.
   */
  public float getGetRequestRate();

  /**
   * Returns the rate of put requests.
   */
  public float getPutRequestRate();
  
  /**
   * Returns the total number of bytes sent to clients.
   */
  public long getTotalSentBytes();

  /**
   * Returns the total number of bytes received from clients.
   */
  public long getTotalReceivedBytes();

  /**
   * Returns a list of the host and port information for gateway senders connected to
   * this gateway receiver.
   */
  public String[] getConnectedGatewaySenders();  
  
  /**
   * Returns the average batch processing time (in milliseconds).
   */
  public long getAverageBatchProcessingTime();

}
