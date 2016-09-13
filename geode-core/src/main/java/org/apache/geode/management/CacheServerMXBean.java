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

import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to information and management functionality for a
 * {@link CacheServer}.
 * 
 * <p>The will be one CacheServermBean per {@link CacheServer} started in GemFire node.
 * 
 * <p> ObjectName for this MBean is GemFire:service=CacheServer,port={0},type=Member,member={1}
 * <p>
 * <table border="1">
 * <tr>
 * <th>Notification Type</th>
 * <th>Notification Source</th>
 * <th>Message</th>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cacheserver.client.joined</td>
 * <td>CacheServer MBean Name</td>
 * <td>Client joined with Id &ltClient ID&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cacheserver.client.left</td>
 * <td>CacheServer MBean Name</td>
 * <td>Client crashed with Id &ltClient ID&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cacheserver.client.crashed</td>
 * <td>CacheServer MBean Name</td>
 * <td>Client left with Id &ltClient ID&gt</td>
 * </tr>
 * </table>
 * 
 * @since GemFire 7.0
 * 
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface CacheServerMXBean {

  /**
   * Returns the port on which this CacheServer listens for clients.
   */
  public int getPort();

  /**
   * Returns a string representing the IP address or host name that this
   * CacheServer will listen on.
   */
  public String getBindAddress();
  
  /**
   * Returns the configured buffer size of the socket connection for this CacheServer.
   */
  public int getSocketBufferSize();

  /**
   * Returns the maximum amount of time between client pings. This value is used to determine the 
   * health of client's attached to the server.
   */
  public int getMaximumTimeBetweenPings();
  
  /**
   * Returns the maximum allowed client connections.
   */
  public int getMaxConnections();

  /**
   * Returns the maxium number of threads allowed in this CacheServer to service client requests.
   */
  public int getMaxThreads();
  
  /**
   * Returns the maximum number of messages that can be enqueued in a client-queue.
   */
  public int getMaximumMessageCount();

  /**
   * Returns the time (in seconds) after which a message in the client queue will expire.
   */
  public int getMessageTimeToLive();
  
  /**
   * Returns the frequency (in milliseconds) to poll the load probe on this CacheServer.
   */
  public long getLoadPollInterval();
  
  /**
   * Returns the name or IP address to pass to the client as the location
   * where the server is listening. When the server connects to the locator it tells
   * the locator the host and port where it is listening for client connections. If
   * the host the server uses by default is one that the client can’t translate into
   * an IP address, the client will have no route to the server’s host and won’t be
   * able to find the server. For this situation, you must supply the server’s
   * alternate hostname for the locator to pass to the client.
   */
  public String getHostNameForClients();
  
  /**
   * Returns the load probe for this CacheServer.
   */
  public ServerLoadData fetchLoadProbe();

  /**
   * Returns whether or not this CacheServer is running.
   * 
   * @return True of the server is running, false otherwise.
   */
  public boolean isRunning();

  /**
   * Returns the capacity (in megabytes) of the client queue.
   */
  public int getCapacity();

  /**
   * Returns the eviction policy that is executed when the capacity of the client
   * queue is reached.
   */
  public String getEvictionPolicy();

  /**
   * Returns the name of the disk store that is used for persistence.
   */
  public String getDiskStoreName();

  /**
   * Returns the number of sockets accepted and used for client to server messaging.
   */
  public int getClientConnectionCount();
  
  /**
   * Returns the number of client virtual machines connected.
   */
  public int getCurrentClients();

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
   * Returns the number of cache client notification requests.
   */
  public int getNumClientNotificationRequests();

  /**
   * Returns the average latency for processing client notifications.
   */
  public long getClientNotificationAvgLatency();

  /**
   * Returns the rate of client notifications.
   */
  public float getClientNotificationRate();

  /**
   * Returns the number of registered CQs.
   */
  public long getRegisteredCQCount();

  /**
   * Returns the number of active (currently executing) CQs.
   */
  public long getActiveCQCount();

  /**
   * Returns the rate of queries.
   */
  public float getQueryRequestRate();

  /**
   * Returns the total number of indexes in use by the member.
   */
  public int getIndexCount();

  /**
   * Returns a list of names for all indexes.
   */
  public String[] getIndexList();

  /**
   * Returns the total time spent updating indexes due to changes in the data.
   */
  public long getTotalIndexMaintenanceTime();

  /**
   * Remove an index.
   * 
   * @param indexName
   *          Name of the index to be removed.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void removeIndex(String indexName) throws Exception;

  /**
   * Returns a list of names for all registered CQs.
   */
  public String[] getContinuousQueryList();

  /**
   * Execute an ad-hoc CQ on the server
   * 
   * @param queryName
   *          Name of the CQ to execute.
   * @deprecated This method is dangerous because it only modifies the
   * target cache server - other copies of the CQ on other servers are
   * not affected. Using the client side CQ methods to modify a CQ.
   */
  @Deprecated
  @ResourceOperation(resource = Resource.DATA, operation = Operation.READ)
  public void executeContinuousQuery(String queryName) throws Exception;

  /**
   * Stop (pause) a CQ from executing
   * 
   * @param queryName
   *          Name of the CQ to stop.
   *          
   * @deprecated This method is dangerous because it only modifies the
   * target cache server - other copies of the CQ on other servers are
   * not affected. Using the client side CQ methods to modify a CQ.         
   */
  @Deprecated
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void stopContinuousQuery(String queryName) throws Exception;

  /**
   * Unregister all CQs from a region
   * 
   * @param regionName
   *          Name of the region from which to remove CQs.
   * @deprecated This method is dangerous because it only modifies the
   * target cache server - other copies of the CQ on other servers are
   * not affected. Using the client side CQ methods to modify a CQ.         
   */
  @Deprecated
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void closeAllContinuousQuery(String regionName) throws Exception;
  
  
  /**
   * Unregister a CQ
   * 
   * @param queryName
   *          Name of the CQ to unregister.
   * @deprecated This method is dangerous because it only modifies the
   * target cache server - other copies of the CQ on other servers are
   * not affected. Using the client side CQ methods to modify a CQ.         
   */
  @Deprecated
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void closeContinuousQuery(String queryName) throws Exception;


  /**
   * Returns a list of IDs for all connected clients.
   * 
   * @return A list of IDs or a length 0 array if no clients are registered.
   */
  public String[] getClientIds() throws Exception;

  /**
   * Returns health and statistic information for the give client id. Some of the
   * information (CPUs, NumOfCacheListenerCalls, NumOfGets,NumOfMisses,
   * NumOfPuts,NumOfThreads, ProcessCpuTime) only available for clients which
   * have set a "StatisticsInterval".
   * 
   * @param clientId
   *          ID of the client for which to retrieve information.
   */
  public ClientHealthStatus showClientStats(String clientId) throws Exception;
  
  /**
   * Returns the number of clients who have existing subscriptions.
   */
  public int getNumSubscriptions();

  /**
   * Returns health and statistic information for all clients. Some of the
   * information (CPUs, NumOfCacheListenerCalls, NumOfGets,NumOfMisses,
   * NumOfPuts,NumOfThreads, ProcessCpuTime) only available for clients which
   * have set a "StatisticsInterval".
   */
  public ClientHealthStatus[] showAllClientStats() throws Exception;
  
  /**
   * Shows a list of client with their queue statistics. Client queue statistics
   * shown in this method are the following
   * 
   * eventsEnqued,eventsRemoved , eventsConflated ,markerEventsConflated ,
   * eventsExpired, eventsRemovedByQrm , eventsTaken , numVoidRemovals
   * 
   * @return an array of ClientQueueDetail
   * @throws Exception
   */
  public ClientQueueDetail[] showClientQueueDetails() throws Exception;
  
  /**
   * 
   * Shows queue statistics of the given client. Client queue statistics
   * shown in this method are the following
   * 
   * eventsEnqued,eventsRemoved , eventsConflated ,markerEventsConflated ,
   * eventsExpired, eventsRemovedByQrm , eventsTaken , numVoidRemovals
   * 
   * @param clientId the ID of client which is returned by the attribute ClientIds 
   * @return ClientQueueDetail
   * @throws Exception
   */
  public ClientQueueDetail showClientQueueDetails(String clientId) throws Exception;

}
