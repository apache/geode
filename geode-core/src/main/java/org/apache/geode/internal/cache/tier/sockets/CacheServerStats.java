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
package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.distributed.internal.PoolStatHelper;

/**
 * Cache Server statistic definitions
 */
public class CacheServerStats implements MessageStats {

  private static final String typeName = "CacheServerStats";

  protected Statistics stats;

  // Get request / response statistics
  private int getRequestsId;

  private int readGetRequestTimeId;

  private int processGetTimeId;

  private int getResponsesId;

  private int writeGetResponseTimeId;

  // PutAll request / response statistics
  private int putAllRequestsId;
  private int readPutAllRequestTimeId;
  private int processPutAllTimeId;
  private int putAllResponsesId;
  private int writePutAllResponseTimeId;

  // RemoveAll request / response statistics
  private int removeAllRequestsId;
  private int readRemoveAllRequestTimeId;
  private int processRemoveAllTimeId;
  private int removeAllResponsesId;
  private int writeRemoveAllResponseTimeId;

  // GetAll request / response statistics
  private int getAllRequestsId;
  private int readGetAllRequestTimeId;
  private int processGetAllTimeId;
  private int getAllResponsesId;
  private int writeGetAllResponseTimeId;

  // Put request / response statistics
  private int putRequestsId;

  private int readPutRequestTimeId;

  private int processPutTimeId;

  private int putResponsesId;

  private int writePutResponseTimeId;

  // Destroy request / response statistics
  private int destroyRequestsId;
  private int readDestroyRequestTimeId;
  private int processDestroyTimeId;
  private int destroyResponsesId;
  private int writeDestroyResponseTimeId;

  // Query request / response statistics
  private int queryRequestsId;

  private int readQueryRequestTimeId;

  private int processQueryTimeId;

  private int queryResponsesId;

  private int writeQueryResponseTimeId;

  // Destroy region request / response statistics
  private int destroyRegionRequestsId;

  private int readDestroyRegionRequestTimeId;

  private int processDestroyRegionTimeId;

  private int destroyRegionResponsesId;

  private int writeDestroyRegionResponseTimeId;

  // ContainsKey request / response statistics
  private int containsKeyRequestsId;
  private int readContainsKeyRequestTimeId;
  private int processContainsKeyTimeId;
  private int containsKeyResponsesId;
  private int writeContainsKeyResponseTimeId;

  // Clear region request / response statistics
  private int clearRegionRequestsId;

  private int readClearRegionRequestTimeId;

  private int processClearRegionTimeId;

  private int clearRegionResponsesId;

  private int writeClearRegionResponseTimeId;


  // Batch processing statistics
  private int processBatchRequestsId;

  private int readProcessBatchRequestTimeId;

  private int processBatchTimeId;

  private int processBatchResponsesId;

  private int writeProcessBatchResponseTimeId;

  private int batchSizeId;

  // Client notification request statistics
  private int clientNotificationRequestsId;

  private int readClientNotificationRequestTimeId;

  private int processClientNotificationTimeId;

  // Update client notification request statistics
  private int updateClientNotificationRequestsId;

  private int readUpdateClientNotificationRequestTimeId;

  private int processUpdateClientNotificationTimeId;

  // Close connection request statistics
  private int closeConnectionRequestsId;

  private int readCloseConnectionRequestTimeId;

  private int processCloseConnectionTimeId;

  // Client ready request / response statistics
  private int clientReadyRequestsId;

  private int readClientReadyRequestTimeId;

  private int processClientReadyTimeId;

  private int clientReadyResponsesId;

  private int writeClientReadyResponseTimeId;

  // Connection statistics
  private int currentClientConnectionsId;
  private int currentQueueConnectionsId;

  private int currentClientsId;

  private int failedConnectionAttemptsId;

  private int receivedBytesId;
  private int sentBytesId;

  private int outOfOrderBatchIdsId;
  private int abandonedWriteRequestsId;
  private int abandonedReadRequestsId;

  private int messagesBeingReceivedId;
  private int messageBytesBeingReceivedId;

  private int connectionsTimedOutId;
  private int threadQueueSizeId;
  private int acceptsInProgressId;
  private int acceptThreadStartsId;
  private int connectionThreadStartsId;
  private int connectionThreadsId;

  // Load callback stats
  private int connectionLoadId;
  private int queueLoadId;
  private int loadPerConnectionId;
  private int loadPerQueueId;

  protected StatisticsType statType;

  public CacheServerStats(StatisticsFactory statisticsFactory, String ownerName) {
    this(statisticsFactory, ownerName, typeName, null);
  }

  /**
   * Add a convinience method to pass in a StatisticsFactory for Statistics construction. Helpful
   * for local Statistics operations
   *
   */
  public CacheServerStats(StatisticsFactory statisticsFactory, String ownerName, String typeName,
      StatisticDescriptor[] descriptors) {
    if (statisticsFactory == null) {
      // Create statistics later when needed
      return;
    }
    StatisticDescriptor[] serverStatDescriptors = new StatisticDescriptor[] {
        statisticsFactory.createLongCounter("getRequests", "Number of cache client get requests.",
            "operations"),
        statisticsFactory.createLongCounter("readGetRequestTime",
            "Total time spent in reading get requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processGetTime",
            "Total time spent in processing a cache client get request, including the time to get an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("getResponses",
            "Number of get responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeGetResponseTime",
            "Total time spent in writing get responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("putRequests", "Number of cache client put requests.",
            "operations"),
        statisticsFactory.createLongCounter("readPutRequestTime",
            "Total time spent in reading put requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processPutTime",
            "Total time spent in processing a cache client put request, including the time to put an object into the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("putResponses",
            "Number of put responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writePutResponseTime",
            "Total time spent in writing put responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("putAllRequests",
            "Number of cache client putAll requests.", "operations"),
        statisticsFactory.createLongCounter("readPutAllRequestTime",
            "Total time spent in reading putAll requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processPutAllTime",
            "Total time spent in processing a cache client putAll request, including the time to put all objects into the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("putAllResponses",
            "Number of putAll responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writePutAllResponseTime",
            "Total time spent in writing putAll responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("removeAllRequests",
            "Number of cache client removeAll requests.", "operations"),
        statisticsFactory.createLongCounter("readRemoveAllRequestTime",
            "Total time spent in reading removeAll requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processRemoveAllTime",
            "Total time spent in processing a cache client removeAll request, including the time to remove all objects from the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("removeAllResponses",
            "Number of removeAll responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeRemoveAllResponseTime",
            "Total time spent in writing removeAll responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("getAllRequests",
            "Number of cache client getAll requests.", "operations"),
        statisticsFactory.createLongCounter("readGetAllRequestTime",
            "Total time spent in reading getAll requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processGetAllTime",
            "Total time spent in processing a cache client getAll request.", "nanoseconds"),
        statisticsFactory.createLongCounter("getAllResponses",
            "Number of getAll responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeGetAllResponseTime",
            "Total time spent in writing getAll responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("destroyRequests",
            "Number of cache client destroy requests.", "operations"),
        statisticsFactory.createLongCounter("readDestroyRequestTime",
            "Total time spent in reading destroy requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processDestroyTime",
            "Total time spent in processing a cache client destroy request, including the time to destroy an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("destroyResponses",
            "Number of destroy responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeDestroyResponseTime",
            "Total time spent in writing destroy responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("invalidateRequests",
            "Number of cache client invalidate requests.", "operations"),
        statisticsFactory.createLongCounter("readInvalidateRequestTime",
            "Total time spent in reading invalidate requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processInvalidateTime",
            "Total time spent in processing a cache client invalidate request, including the time to invalidate an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("invalidateResponses",
            "Number of invalidate responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeInvalidateResponseTime",
            "Total time spent in writing invalidate responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("sizeRequests", "Number of cache client size requests.",
            "operations"),
        statisticsFactory.createLongCounter("readSizeRequestTime",
            "Total time spent in reading size requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processSizeTime",
            "Total time spent in processing a cache client size request, including the time to size an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("sizeResponses",
            "Number of size responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeSizeResponseTime",
            "Total time spent in writing size responses.", "nanoseconds"),


        statisticsFactory.createLongCounter("queryRequests",
            "Number of cache client query requests.", "operations"),
        statisticsFactory.createLongCounter("readQueryRequestTime",
            "Total time spent in reading query requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processQueryTime",
            "Total time spent in processing a cache client query request, including the time to destroy an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("queryResponses",
            "Number of query responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeQueryResponseTime",
            "Total time spent in writing query responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("destroyRegionRequests",
            "Number of cache client destroyRegion requests.", "operations"),
        statisticsFactory.createLongCounter("readDestroyRegionRequestTime",
            "Total time spent in reading destroyRegion requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processDestroyRegionTime",
            "Total time spent in processing a cache client destroyRegion request, including the time to destroy the region from the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("destroyRegionResponses",
            "Number of destroyRegion responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeDestroyRegionResponseTime",
            "Total time spent in writing destroyRegion responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("containsKeyRequests",
            "Number of cache client containsKey requests.", "operations"),
        statisticsFactory.createLongCounter("readContainsKeyRequestTime",
            "Total time spent reading containsKey requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processContainsKeyTime",
            "Total time spent processing a containsKey request.", "nanoseconds"),
        statisticsFactory.createLongCounter("containsKeyResponses",
            "Number of containsKey responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeContainsKeyResponseTime",
            "Total time spent writing containsKey responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("processBatchRequests",
            "Number of cache client processBatch requests.", "operations"),
        statisticsFactory.createLongCounter("readProcessBatchRequestTime",
            "Total time spent in reading processBatch requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processBatchTime",
            "Total time spent in processing a cache client processBatch request.", "nanoseconds"),
        statisticsFactory.createLongCounter("processBatchResponses",
            "Number of processBatch responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeProcessBatchResponseTime",
            "Total time spent in writing processBatch responses.", "nanoseconds"),
        statisticsFactory.createLongCounter("batchSize", "The size of the batches received.",
            "bytes"),
        statisticsFactory.createLongCounter("clearRegionRequests",
            "Number of cache client clearRegion requests.", "operations"),
        statisticsFactory.createLongCounter("readClearRegionRequestTime",
            "Total time spent in reading clearRegion requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processClearRegionTime",
            "Total time spent in processing a cache client clearRegion request, including the time to clear the region from the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("clearRegionResponses",
            "Number of clearRegion responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeClearRegionResponseTime",
            "Total time spent in writing clearRegion responses.", "nanoseconds"),
        statisticsFactory.createLongCounter("clientNotificationRequests",
            "Number of cache client notification requests.", "operations"),
        statisticsFactory.createLongCounter("readClientNotificationRequestTime",
            "Total time spent in reading client notification requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processClientNotificationTime",
            "Total time spent in processing a cache client notification request.", "nanoseconds"),

        statisticsFactory.createLongCounter("updateClientNotificationRequests",
            "Number of cache client notification update requests.", "operations"),
        statisticsFactory.createLongCounter("readUpdateClientNotificationRequestTime",
            "Total time spent in reading client notification update requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processUpdateClientNotificationTime",
            "Total time spent in processing a client notification update request.", "nanoseconds"),

        statisticsFactory.createLongCounter("clientReadyRequests",
            "Number of cache client ready requests.", "operations"),
        statisticsFactory.createLongCounter("readClientReadyRequestTime",
            "Total time spent in reading cache client ready requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processClientReadyTime",
            "Total time spent in processing a cache client ready request, including the time to destroy an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("clientReadyResponses",
            "Number of client ready responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeClientReadyResponseTime",
            "Total time spent in writing client ready responses.", "nanoseconds"),

        statisticsFactory.createLongCounter("closeConnectionRequests",
            "Number of cache client close connection requests.", "operations"),
        statisticsFactory.createLongCounter("readCloseConnectionRequestTime",
            "Total time spent in reading close connection requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processCloseConnectionTime",
            "Total time spent in processing a cache client close connection request.",
            "nanoseconds"),
        statisticsFactory.createLongCounter("failedConnectionAttempts",
            "Number of failed connection attempts.", "attempts"),
        statisticsFactory.createLongGauge("currentClientConnections",
            "Number of sockets accepted and used for client to server messaging.", "sockets"),
        statisticsFactory.createLongGauge("currentQueueConnections",
            "Number of sockets accepted and used for server to client messaging.", "sockets"),
        statisticsFactory.createLongGauge("currentClients",
            "Number of client virtual machines connected.", "clients"),
        statisticsFactory.createLongCounter("outOfOrderGatewayBatchIds",
            "Number of Out of order batch IDs.", "batches"),
        statisticsFactory.createLongCounter("abandonedWriteRequests",
            "Number of write opertations abandond by clients", "requests"),
        statisticsFactory.createLongCounter("abandonedReadRequests",
            "Number of read opertations abandond by clients", "requests"),
        statisticsFactory.createLongCounter("receivedBytes",
            "Total number of bytes received from clients.", "bytes"),
        statisticsFactory.createLongCounter("sentBytes", "Total number of bytes sent to clients.",
            "bytes"),
        statisticsFactory.createLongGauge("messagesBeingReceived",
            "Current number of message being received off the network or being processed after reception.",
            "messages"),
        statisticsFactory.createLongGauge("messageBytesBeingReceived",
            "Current number of bytes consumed by messages being received or processed.", "bytes"),
        statisticsFactory.createLongCounter("connectionsTimedOut",
            "Total number of connections that have been timed out by the server because of client inactivity",
            "connections"),
        statisticsFactory.createLongGauge("threadQueueSize",
            "Current number of connections waiting for a thread to start processing their message.",
            "connections"),
        statisticsFactory.createLongGauge("acceptsInProgress",
            "Current number of server accepts that are attempting to do the initial handshake with the client.",
            "accepts"),
        statisticsFactory.createLongCounter("acceptThreadStarts",
            "Total number of threads created to deal with an accepted socket. Note that this is not the current number of threads.",
            "starts"),
        statisticsFactory.createLongCounter("connectionThreadStarts",
            "Total number of threads created to deal with a client connection. Note that this is not the current number of threads.",
            "starts"),
        statisticsFactory.createLongGauge("connectionThreads",
            "Current number of threads dealing with a client connection.", "threads"),
        statisticsFactory.createDoubleGauge("connectionLoad",
            "The load from client to server connections as reported by the load probe installed in this server",
            "load"),
        statisticsFactory.createDoubleGauge("loadPerConnection",
            "The estimate of how much load is added for each new connection as reported by the load probe installed in this server",
            "load"),
        statisticsFactory.createDoubleGauge("queueLoad",
            "The load from queues as reported by the load probe installed in this server", "load"),
        statisticsFactory.createDoubleGauge("loadPerQueue",
            "The estimate of how much load is added for each new connection as reported by the load probe installed in this server",
            "load")};
    StatisticDescriptor[] alldescriptors = serverStatDescriptors;
    if (descriptors != null) {
      alldescriptors = new StatisticDescriptor[descriptors.length + serverStatDescriptors.length];
      System.arraycopy(descriptors, 0, alldescriptors, 0, descriptors.length);
      System.arraycopy(serverStatDescriptors, 0, alldescriptors, descriptors.length,
          serverStatDescriptors.length);
    }
    statType = statisticsFactory.createType(typeName, typeName, alldescriptors);
    this.stats = statisticsFactory.createAtomicStatistics(statType, ownerName);

    getRequestsId = this.stats.nameToId("getRequests");
    readGetRequestTimeId = this.stats.nameToId("readGetRequestTime");
    processGetTimeId = this.stats.nameToId("processGetTime");
    getResponsesId = this.stats.nameToId("getResponses");
    writeGetResponseTimeId = this.stats.nameToId("writeGetResponseTime");

    putRequestsId = this.stats.nameToId("putRequests");
    readPutRequestTimeId = this.stats.nameToId("readPutRequestTime");
    processPutTimeId = this.stats.nameToId("processPutTime");
    putResponsesId = this.stats.nameToId("putResponses");
    writePutResponseTimeId = this.stats.nameToId("writePutResponseTime");

    putAllRequestsId = this.stats.nameToId("putAllRequests");
    readPutAllRequestTimeId = this.stats.nameToId("readPutAllRequestTime");
    processPutAllTimeId = this.stats.nameToId("processPutAllTime");
    putAllResponsesId = this.stats.nameToId("putAllResponses");
    writePutAllResponseTimeId = this.stats.nameToId("writePutAllResponseTime");

    removeAllRequestsId = this.stats.nameToId("removeAllRequests");
    readRemoveAllRequestTimeId = this.stats.nameToId("readRemoveAllRequestTime");
    processRemoveAllTimeId = this.stats.nameToId("processRemoveAllTime");
    removeAllResponsesId = this.stats.nameToId("removeAllResponses");
    writeRemoveAllResponseTimeId = this.stats.nameToId("writeRemoveAllResponseTime");

    getAllRequestsId = this.stats.nameToId("getAllRequests");
    readGetAllRequestTimeId = this.stats.nameToId("readGetAllRequestTime");
    processGetAllTimeId = this.stats.nameToId("processGetAllTime");
    getAllResponsesId = this.stats.nameToId("getAllResponses");
    writeGetAllResponseTimeId = this.stats.nameToId("writeGetAllResponseTime");

    destroyRequestsId = this.stats.nameToId("destroyRequests");
    readDestroyRequestTimeId = this.stats.nameToId("readDestroyRequestTime");
    processDestroyTimeId = this.stats.nameToId("processDestroyTime");
    destroyResponsesId = this.stats.nameToId("destroyResponses");
    writeDestroyResponseTimeId = this.stats.nameToId("writeDestroyResponseTime");

    queryRequestsId = this.stats.nameToId("queryRequests");
    readQueryRequestTimeId = this.stats.nameToId("readQueryRequestTime");
    processQueryTimeId = this.stats.nameToId("processQueryTime");
    queryResponsesId = this.stats.nameToId("queryResponses");
    writeQueryResponseTimeId = this.stats.nameToId("writeQueryResponseTime");

    destroyRegionRequestsId = this.stats.nameToId("destroyRegionRequests");
    readDestroyRegionRequestTimeId = this.stats.nameToId("readDestroyRegionRequestTime");
    processDestroyRegionTimeId = this.stats.nameToId("processDestroyRegionTime");
    destroyRegionResponsesId = this.stats.nameToId("destroyRegionResponses");
    writeDestroyRegionResponseTimeId = this.stats.nameToId("writeDestroyRegionResponseTime");

    clearRegionRequestsId = this.stats.nameToId("clearRegionRequests");
    readClearRegionRequestTimeId = this.stats.nameToId("readClearRegionRequestTime");
    processClearRegionTimeId = this.stats.nameToId("processClearRegionTime");
    clearRegionResponsesId = this.stats.nameToId("clearRegionResponses");
    writeClearRegionResponseTimeId = this.stats.nameToId("writeClearRegionResponseTime");

    containsKeyRequestsId = this.stats.nameToId("containsKeyRequests");
    readContainsKeyRequestTimeId = this.stats.nameToId("readContainsKeyRequestTime");
    processContainsKeyTimeId = this.stats.nameToId("processContainsKeyTime");
    containsKeyResponsesId = this.stats.nameToId("containsKeyResponses");
    writeContainsKeyResponseTimeId = this.stats.nameToId("writeContainsKeyResponseTime");

    processBatchRequestsId = this.stats.nameToId("processBatchRequests");
    readProcessBatchRequestTimeId = this.stats.nameToId("readProcessBatchRequestTime");
    processBatchTimeId = this.stats.nameToId("processBatchTime");
    processBatchResponsesId = this.stats.nameToId("processBatchResponses");
    writeProcessBatchResponseTimeId = this.stats.nameToId("writeProcessBatchResponseTime");
    batchSizeId = this.stats.nameToId("batchSize");

    clientNotificationRequestsId = this.stats.nameToId("clientNotificationRequests");
    readClientNotificationRequestTimeId = this.stats.nameToId("readClientNotificationRequestTime");
    processClientNotificationTimeId = this.stats.nameToId("processClientNotificationTime");

    updateClientNotificationRequestsId = this.stats.nameToId("updateClientNotificationRequests");
    readUpdateClientNotificationRequestTimeId =
        this.stats.nameToId("readUpdateClientNotificationRequestTime");
    processUpdateClientNotificationTimeId =
        this.stats.nameToId("processUpdateClientNotificationTime");

    clientReadyRequestsId = this.stats.nameToId("clientReadyRequests");
    readClientReadyRequestTimeId = this.stats.nameToId("readClientReadyRequestTime");
    processClientReadyTimeId = this.stats.nameToId("processClientReadyTime");
    clientReadyResponsesId = this.stats.nameToId("clientReadyResponses");
    writeClientReadyResponseTimeId = this.stats.nameToId("writeClientReadyResponseTime");

    closeConnectionRequestsId = this.stats.nameToId("closeConnectionRequests");
    readCloseConnectionRequestTimeId = this.stats.nameToId("readCloseConnectionRequestTime");
    processCloseConnectionTimeId = this.stats.nameToId("processCloseConnectionTime");

    currentClientConnectionsId = this.stats.nameToId("currentClientConnections");
    currentQueueConnectionsId = this.stats.nameToId("currentQueueConnections");
    currentClientsId = this.stats.nameToId("currentClients");
    failedConnectionAttemptsId = this.stats.nameToId("failedConnectionAttempts");

    outOfOrderBatchIdsId = this.stats.nameToId("outOfOrderGatewayBatchIds");

    abandonedWriteRequestsId = this.stats.nameToId("abandonedWriteRequests");
    abandonedReadRequestsId = this.stats.nameToId("abandonedReadRequests");

    receivedBytesId = this.stats.nameToId("receivedBytes");
    sentBytesId = this.stats.nameToId("sentBytes");

    messagesBeingReceivedId = this.stats.nameToId("messagesBeingReceived");
    messageBytesBeingReceivedId = this.stats.nameToId("messageBytesBeingReceived");
    connectionsTimedOutId = this.stats.nameToId("connectionsTimedOut");
    threadQueueSizeId = this.stats.nameToId("threadQueueSize");
    acceptsInProgressId = this.stats.nameToId("acceptsInProgress");
    acceptThreadStartsId = this.stats.nameToId("acceptThreadStarts");
    connectionThreadStartsId = this.stats.nameToId("connectionThreadStarts");
    connectionThreadsId = this.stats.nameToId("connectionThreads");

    connectionLoadId = this.stats.nameToId("connectionLoad");
    queueLoadId = this.stats.nameToId("queueLoad");
    loadPerConnectionId = this.stats.nameToId("loadPerConnection");
    loadPerQueueId = this.stats.nameToId("loadPerQueue");
  }

  void incAcceptThreadsCreated() {
    this.stats.incLong(acceptThreadStartsId, 1);
  }

  void incConnectionThreadsCreated() {
    this.stats.incLong(connectionThreadStartsId, 1);
  }

  void incAcceptsInProgress() {
    this.stats.incLong(acceptsInProgressId, 1);
  }

  void decAcceptsInProgress() {
    this.stats.incLong(acceptsInProgressId, -1);
  }

  public void incConnectionThreads() {
    this.stats.incLong(connectionThreadsId, 1);
  }

  private void decConnectionThreads() {
    this.stats.incLong(connectionThreadsId, -1);
  }

  void incAbandonedWriteRequests() {
    this.stats.incLong(abandonedWriteRequestsId, 1);
  }

  void incAbandonedReadRequests() {
    this.stats.incLong(abandonedReadRequestsId, 1);
  }

  public void incFailedConnectionAttempts() {
    this.stats.incLong(failedConnectionAttemptsId, 1);
  }

  public void incConnectionsTimedOut() {
    this.stats.incLong(connectionsTimedOutId, 1);
  }

  public void incCurrentClientConnections() {
    this.stats.incLong(currentClientConnectionsId, 1);
  }

  void decCurrentClientConnections() {
    this.stats.incLong(currentClientConnectionsId, -1);
  }

  public int getCurrentClientConnections() {
    return (int) this.stats.getLong(currentClientConnectionsId);
  }

  public void incCurrentQueueConnections() {
    this.stats.incLong(currentQueueConnectionsId, 1);
  }

  void decCurrentQueueConnections() {
    this.stats.incLong(currentQueueConnectionsId, -1);
  }

  public void incCurrentClients() {
    this.stats.incLong(currentClientsId, 1);
  }

  void decCurrentClients() {
    this.stats.incLong(currentClientsId, -1);
  }

  public void incThreadQueueSize() {
    this.stats.incLong(threadQueueSizeId, 1);
  }

  public void decThreadQueueSize() {
    this.stats.incLong(threadQueueSizeId, -1);
  }

  public void incReadGetRequestTime(long delta) {
    this.stats.incLong(readGetRequestTimeId, delta);
    this.stats.incLong(getRequestsId, 1);
  }

  public void incProcessGetTime(long delta) {
    this.stats.incLong(processGetTimeId, delta);
  }

  public void incWriteGetResponseTime(long delta) {
    this.stats.incLong(writeGetResponseTimeId, delta);
    this.stats.incLong(getResponsesId, 1);
  }

  public void incReadPutAllRequestTime(long delta) {
    this.stats.incLong(readPutAllRequestTimeId, delta);
    this.stats.incLong(putAllRequestsId, 1);
  }

  public void incProcessPutAllTime(long delta) {
    this.stats.incLong(processPutAllTimeId, delta);
  }

  public void incWritePutAllResponseTime(long delta) {
    this.stats.incLong(writePutAllResponseTimeId, delta);
    this.stats.incLong(putAllResponsesId, 1);
  }

  public void incReadRemoveAllRequestTime(long delta) {
    this.stats.incLong(readRemoveAllRequestTimeId, delta);
    this.stats.incLong(removeAllRequestsId, 1);
  }

  public void incProcessRemoveAllTime(long delta) {
    this.stats.incLong(processRemoveAllTimeId, delta);
  }

  public void incWriteRemoveAllResponseTime(long delta) {
    this.stats.incLong(writeRemoveAllResponseTimeId, delta);
    this.stats.incLong(removeAllResponsesId, 1);
  }

  public void incReadPutRequestTime(long delta) {
    this.stats.incLong(readPutRequestTimeId, delta);
    this.stats.incLong(putRequestsId, 1);
  }

  public void incProcessPutTime(long delta) {
    this.stats.incLong(processPutTimeId, delta);
  }

  public void incWritePutResponseTime(long delta) {
    this.stats.incLong(writePutResponseTimeId, delta);
    this.stats.incLong(putResponsesId, 1);
  }

  public void incReadDestroyRequestTime(long delta) {
    this.stats.incLong(readDestroyRequestTimeId, delta);
    this.stats.incLong(destroyRequestsId, 1);
  }

  public void incProcessDestroyTime(long delta) {
    this.stats.incLong(processDestroyTimeId, delta);
  }

  public void incWriteDestroyResponseTime(long delta) {
    this.stats.incLong(writeDestroyResponseTimeId, delta);
    this.stats.incLong(destroyResponsesId, 1);
  }

  public void incReadQueryRequestTime(long delta) {
    this.stats.incLong(readQueryRequestTimeId, delta);
    this.stats.incLong(queryRequestsId, 1);
  }

  void incProcessQueryTime(long delta) {
    this.stats.incLong(processQueryTimeId, delta);
  }

  void incWriteQueryResponseTime(long delta) {
    this.stats.incLong(writeQueryResponseTimeId, delta);
    this.stats.incLong(queryResponsesId, 1);
  }

  public void incReadDestroyRegionRequestTime(long delta) {
    this.stats.incLong(readDestroyRegionRequestTimeId, delta);
    this.stats.incLong(destroyRegionRequestsId, 1);
  }

  public void incProcessDestroyRegionTime(long delta) {
    this.stats.incLong(processDestroyRegionTimeId, delta);
  }

  public void incWriteDestroyRegionResponseTime(long delta) {
    this.stats.incLong(writeDestroyRegionResponseTimeId, delta);
    this.stats.incLong(destroyRegionResponsesId, 1);
  }

  public void incReadContainsKeyRequestTime(long delta) {
    this.stats.incLong(readContainsKeyRequestTimeId, delta);
    this.stats.incLong(containsKeyRequestsId, 1);
  }

  public void incProcessContainsKeyTime(long delta) {
    this.stats.incLong(processContainsKeyTimeId, delta);
  }

  public void incWriteContainsKeyResponseTime(long delta) {
    this.stats.incLong(writeContainsKeyResponseTimeId, delta);
    this.stats.incLong(containsKeyResponsesId, 1);
  }

  public void incReadClearRegionRequestTime(long delta) {
    this.stats.incLong(readClearRegionRequestTimeId, delta);
    this.stats.incLong(clearRegionRequestsId, 1);
  }

  public void incProcessClearRegionTime(long delta) {
    this.stats.incLong(processClearRegionTimeId, delta);
  }

  public void incWriteClearRegionResponseTime(long delta) {
    this.stats.incLong(writeClearRegionResponseTimeId, delta);
    this.stats.incLong(clearRegionResponsesId, 1);
  }

  public void incReadProcessBatchRequestTime(long delta) {
    this.stats.incLong(readProcessBatchRequestTimeId, delta);
    this.stats.incLong(processBatchRequestsId, 1);
  }

  public void incWriteProcessBatchResponseTime(long delta) {
    this.stats.incLong(writeProcessBatchResponseTimeId, delta);
    this.stats.incLong(processBatchResponsesId, 1);
  }

  public void incProcessBatchTime(long delta) {
    this.stats.incLong(processBatchTimeId, delta);
  }

  public void incBatchSize(long size) {
    this.stats.incLong(batchSizeId, size);
  }

  public void incReadUpdateClientNotificationRequestTime(long delta) {
    this.stats.incLong(readUpdateClientNotificationRequestTimeId, delta);
    this.stats.incLong(updateClientNotificationRequestsId, 1);
  }

  public void incProcessUpdateClientNotificationTime(long delta) {
    this.stats.incLong(processUpdateClientNotificationTimeId, delta);
  }

  public void incReadCloseConnectionRequestTime(long delta) {
    this.stats.incLong(readCloseConnectionRequestTimeId, delta);
    this.stats.incLong(closeConnectionRequestsId, 1);
  }

  public void incProcessCloseConnectionTime(long delta) {
    this.stats.incLong(processCloseConnectionTimeId, delta);
  }

  void incOutOfOrderBatchIds() {
    this.stats.incLong(outOfOrderBatchIdsId, 1);
  }

  @Override
  public void incReceivedBytes(long v) {
    this.stats.incLong(receivedBytesId, v);
  }

  @Override
  public void incSentBytes(long v) {
    this.stats.incLong(sentBytesId, v);
  }

  @Override
  public void incMessagesBeingReceived(int bytes) {
    stats.incLong(messagesBeingReceivedId, 1);
    if (bytes > 0) {
      stats.incLong(messageBytesBeingReceivedId, bytes);
    }
  }

  @Override
  public void decMessagesBeingReceived(int bytes) {
    stats.incLong(messagesBeingReceivedId, -1);
    if (bytes > 0) {
      stats.incLong(messageBytesBeingReceivedId, -bytes);
    }
  }

  public void incReadClientReadyRequestTime(long delta) {
    this.stats.incLong(readClientReadyRequestTimeId, delta);
    this.stats.incLong(clientReadyRequestsId, 1);
  }

  public void incProcessClientReadyTime(long delta) {
    this.stats.incLong(processClientReadyTimeId, delta);
  }

  public void incWriteClientReadyResponseTime(long delta) {
    this.stats.incLong(writeClientReadyResponseTimeId, delta);
    this.stats.incLong(clientReadyResponsesId, 1);
  }

  public void setLoad(ServerLoad load) {
    this.stats.setDouble(connectionLoadId, load.getConnectionLoad());
    this.stats.setDouble(queueLoadId, load.getSubscriptionConnectionLoad());
    this.stats.setDouble(loadPerConnectionId, load.getLoadPerConnection());
    this.stats.setDouble(loadPerQueueId, load.getLoadPerSubscriptionConnection());
  }

  public double getConnectionLoad() {
    return this.stats.getDouble(connectionLoadId);
  }

  public double getLoadPerConnection() {
    return this.stats.getDouble(loadPerConnectionId);
  }

  public int getProcessBatchRequests() {
    return (int) this.stats.getLong(processBatchRequestsId);
  }

  public void close() {
    this.stats.close();
  }

  PoolStatHelper getCnxPoolHelper() {
    return new PoolStatHelper() {
      @Override
      public void startJob() {
        incConnectionThreads();
      }

      @Override
      public void endJob() {
        decConnectionThreads();
      }
    };
  }

  public Statistics getStats() {
    return stats;
  }
}
