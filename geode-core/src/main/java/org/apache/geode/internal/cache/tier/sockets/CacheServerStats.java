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
  int getRequestsId;

  int readGetRequestTimeId;

  int processGetTimeId;

  int getResponsesId;

  int writeGetResponseTimeId;

  // PutAll request / response statistics
  int putAllRequestsId;
  int readPutAllRequestTimeId;
  int processPutAllTimeId;
  int putAllResponsesId;
  int writePutAllResponseTimeId;

  // RemoveAll request / response statistics
  int removeAllRequestsId;
  int readRemoveAllRequestTimeId;
  int processRemoveAllTimeId;
  int removeAllResponsesId;
  int writeRemoveAllResponseTimeId;

  // GetAll request / response statistics
  int getAllRequestsId;
  int readGetAllRequestTimeId;
  int processGetAllTimeId;
  int getAllResponsesId;
  int writeGetAllResponseTimeId;

  // Put request / response statistics
  int putRequestsId;

  int readPutRequestTimeId;

  int processPutTimeId;

  int putResponsesId;

  int writePutResponseTimeId;

  // Destroy request / response statistics
  int destroyRequestsId;
  int readDestroyRequestTimeId;
  int processDestroyTimeId;
  int destroyResponsesId;
  int writeDestroyResponseTimeId;

  // Invalidate request / response statistics
  // int invalidateRequestsId;
  // int readInvalidateRequestTimeId;
  // int processInvalidateTimeId;
  // int invalidateResponsesId;
  // int writeInvalidateResponseTimeId;

  // size request / response statistics
  // int sizeRequestsId;
  // int readSizeRequestTimeId;
  // int processSizeTimeId;
  // int sizeResponsesId;
  // int writeSizeResponseTimeId;


  // Query request / response statistics
  int queryRequestsId;

  int readQueryRequestTimeId;

  int processQueryTimeId;

  int queryResponsesId;

  int writeQueryResponseTimeId;

  // CQ commands request / response statistics
  // int processCreateCqTimeId;
  // int processExecuteCqWithIRCqTimeId;
  // int processStopCqTimeId;
  // int processCloseCqTimeId;
  // int processCloseClientCqsTimeId;
  // int processGetCqStatsTimeId;


  // Destroy region request / response statistics
  int destroyRegionRequestsId;

  int readDestroyRegionRequestTimeId;

  int processDestroyRegionTimeId;

  int destroyRegionResponsesId;

  int writeDestroyRegionResponseTimeId;

  // ContainsKey request / response statistics
  int containsKeyRequestsId;
  int readContainsKeyRequestTimeId;
  int processContainsKeyTimeId;
  int containsKeyResponsesId;
  int writeContainsKeyResponseTimeId;

  // Clear region request / response statistics
  int clearRegionRequestsId;

  int readClearRegionRequestTimeId;

  int processClearRegionTimeId;

  int clearRegionResponsesId;

  int writeClearRegionResponseTimeId;


  // Batch processing statistics
  int processBatchRequestsId;

  int readProcessBatchRequestTimeId;

  int processBatchTimeId;

  int processBatchResponsesId;

  int writeProcessBatchResponseTimeId;

  int batchSizeId;

  // Client notification request statistics
  int clientNotificationRequestsId;

  int readClientNotificationRequestTimeId;

  int processClientNotificationTimeId;

  // Update client notification request statistics
  int updateClientNotificationRequestsId;

  int readUpdateClientNotificationRequestTimeId;

  int processUpdateClientNotificationTimeId;

  // Close connection request statistics
  int closeConnectionRequestsId;

  int readCloseConnectionRequestTimeId;

  int processCloseConnectionTimeId;

  // Client ready request / response statistics
  int clientReadyRequestsId;

  int readClientReadyRequestTimeId;

  int processClientReadyTimeId;

  int clientReadyResponsesId;

  int writeClientReadyResponseTimeId;

  // Connection statistics
  int currentClientConnectionsId;
  int currentQueueConnectionsId;

  int currentClientsId;

  int failedConnectionAttemptsId;

  int receivedBytesId;
  int sentBytesId;

  int outOfOrderBatchIdsId;
  int abandonedWriteRequestsId;
  int abandonedReadRequestsId;

  int messagesBeingReceivedId;
  int messageBytesBeingReceivedId;

  int connectionsTimedOutId;
  int threadQueueSizeId;
  int acceptsInProgressId;
  int acceptThreadStartsId;
  int connectionThreadStartsId;
  int connectionThreadsId;

  // Load callback stats
  int connectionLoadId;
  int queueLoadId;
  int loadPerConnectionId;
  int loadPerQueueId;

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
        statisticsFactory.createIntCounter("getRequests", "Number of cache client get requests.",
            "operations"),
        statisticsFactory.createLongCounter("readGetRequestTime",
            "Total time spent in reading get requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processGetTime",
            "Total time spent in processing a cache client get request, including the time to get an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("getResponses",
            "Number of get responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeGetResponseTime",
            "Total time spent in writing get responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("putRequests", "Number of cache client put requests.",
            "operations"),
        statisticsFactory.createLongCounter("readPutRequestTime",
            "Total time spent in reading put requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processPutTime",
            "Total time spent in processing a cache client put request, including the time to put an object into the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("putResponses",
            "Number of put responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writePutResponseTime",
            "Total time spent in writing put responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("putAllRequests",
            "Number of cache client putAll requests.", "operations"),
        statisticsFactory.createLongCounter("readPutAllRequestTime",
            "Total time spent in reading putAll requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processPutAllTime",
            "Total time spent in processing a cache client putAll request, including the time to put all objects into the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("putAllResponses",
            "Number of putAll responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writePutAllResponseTime",
            "Total time spent in writing putAll responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("removeAllRequests",
            "Number of cache client removeAll requests.", "operations"),
        statisticsFactory.createLongCounter("readRemoveAllRequestTime",
            "Total time spent in reading removeAll requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processRemoveAllTime",
            "Total time spent in processing a cache client removeAll request, including the time to remove all objects from the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("removeAllResponses",
            "Number of removeAll responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeRemoveAllResponseTime",
            "Total time spent in writing removeAll responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("getAllRequests",
            "Number of cache client getAll requests.", "operations"),
        statisticsFactory.createLongCounter("readGetAllRequestTime",
            "Total time spent in reading getAll requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processGetAllTime",
            "Total time spent in processing a cache client getAll request.", "nanoseconds"),
        statisticsFactory.createIntCounter("getAllResponses",
            "Number of getAll responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeGetAllResponseTime",
            "Total time spent in writing getAll responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("destroyRequests",
            "Number of cache client destroy requests.", "operations"),
        statisticsFactory.createLongCounter("readDestroyRequestTime",
            "Total time spent in reading destroy requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processDestroyTime",
            "Total time spent in processing a cache client destroy request, including the time to destroy an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("destroyResponses",
            "Number of destroy responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeDestroyResponseTime",
            "Total time spent in writing destroy responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("invalidateRequests",
            "Number of cache client invalidate requests.", "operations"),
        statisticsFactory.createLongCounter("readInvalidateRequestTime",
            "Total time spent in reading invalidate requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processInvalidateTime",
            "Total time spent in processing a cache client invalidate request, including the time to invalidate an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("invalidateResponses",
            "Number of invalidate responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeInvalidateResponseTime",
            "Total time spent in writing invalidate responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("sizeRequests", "Number of cache client size requests.",
            "operations"),
        statisticsFactory.createLongCounter("readSizeRequestTime",
            "Total time spent in reading size requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processSizeTime",
            "Total time spent in processing a cache client size request, including the time to size an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("sizeResponses",
            "Number of size responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeSizeResponseTime",
            "Total time spent in writing size responses.", "nanoseconds"),


        statisticsFactory.createIntCounter("queryRequests",
            "Number of cache client query requests.", "operations"),
        statisticsFactory.createLongCounter("readQueryRequestTime",
            "Total time spent in reading query requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processQueryTime",
            "Total time spent in processing a cache client query request, including the time to destroy an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("queryResponses",
            "Number of query responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeQueryResponseTime",
            "Total time spent in writing query responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("destroyRegionRequests",
            "Number of cache client destroyRegion requests.", "operations"),
        statisticsFactory.createLongCounter("readDestroyRegionRequestTime",
            "Total time spent in reading destroyRegion requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processDestroyRegionTime",
            "Total time spent in processing a cache client destroyRegion request, including the time to destroy the region from the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("destroyRegionResponses",
            "Number of destroyRegion responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeDestroyRegionResponseTime",
            "Total time spent in writing destroyRegion responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("containsKeyRequests",
            "Number of cache client containsKey requests.", "operations"),
        statisticsFactory.createLongCounter("readContainsKeyRequestTime",
            "Total time spent reading containsKey requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processContainsKeyTime",
            "Total time spent processing a containsKey request.", "nanoseconds"),
        statisticsFactory.createIntCounter("containsKeyResponses",
            "Number of containsKey responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeContainsKeyResponseTime",
            "Total time spent writing containsKey responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("processBatchRequests",
            "Number of cache client processBatch requests.", "operations"),
        statisticsFactory.createLongCounter("readProcessBatchRequestTime",
            "Total time spent in reading processBatch requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processBatchTime",
            "Total time spent in processing a cache client processBatch request.", "nanoseconds"),
        statisticsFactory.createIntCounter("processBatchResponses",
            "Number of processBatch responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeProcessBatchResponseTime",
            "Total time spent in writing processBatch responses.", "nanoseconds"),
        statisticsFactory.createLongCounter("batchSize", "The size of the batches received.",
            "bytes"),
        statisticsFactory.createIntCounter("clearRegionRequests",
            "Number of cache client clearRegion requests.", "operations"),
        statisticsFactory.createLongCounter("readClearRegionRequestTime",
            "Total time spent in reading clearRegion requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processClearRegionTime",
            "Total time spent in processing a cache client clearRegion request, including the time to clear the region from the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("clearRegionResponses",
            "Number of clearRegion responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeClearRegionResponseTime",
            "Total time spent in writing clearRegion responses.", "nanoseconds"),
        statisticsFactory.createIntCounter("clientNotificationRequests",
            "Number of cache client notification requests.", "operations"),
        statisticsFactory.createLongCounter("readClientNotificationRequestTime",
            "Total time spent in reading client notification requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processClientNotificationTime",
            "Total time spent in processing a cache client notification request.", "nanoseconds"),

        statisticsFactory.createIntCounter("updateClientNotificationRequests",
            "Number of cache client notification update requests.", "operations"),
        statisticsFactory.createLongCounter("readUpdateClientNotificationRequestTime",
            "Total time spent in reading client notification update requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processUpdateClientNotificationTime",
            "Total time spent in processing a client notification update request.", "nanoseconds"),

        statisticsFactory.createIntCounter("clientReadyRequests",
            "Number of cache client ready requests.", "operations"),
        statisticsFactory.createLongCounter("readClientReadyRequestTime",
            "Total time spent in reading cache client ready requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processClientReadyTime",
            "Total time spent in processing a cache client ready request, including the time to destroy an object from the cache.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("clientReadyResponses",
            "Number of client ready responses written to the cache client.", "operations"),
        statisticsFactory.createLongCounter("writeClientReadyResponseTime",
            "Total time spent in writing client ready responses.", "nanoseconds"),

        statisticsFactory.createIntCounter("closeConnectionRequests",
            "Number of cache client close connection requests.", "operations"),
        statisticsFactory.createLongCounter("readCloseConnectionRequestTime",
            "Total time spent in reading close connection requests.", "nanoseconds"),
        statisticsFactory.createLongCounter("processCloseConnectionTime",
            "Total time spent in processing a cache client close connection request.",
            "nanoseconds"),
        statisticsFactory.createIntCounter("failedConnectionAttempts",
            "Number of failed connection attempts.", "attempts"),
        statisticsFactory.createIntGauge("currentClientConnections",
            "Number of sockets accepted and used for client to server messaging.", "sockets"),
        statisticsFactory.createIntGauge("currentQueueConnections",
            "Number of sockets accepted and used for server to client messaging.", "sockets"),
        statisticsFactory.createIntGauge("currentClients",
            "Number of client virtual machines connected.", "clients"),
        statisticsFactory.createIntCounter("outOfOrderGatewayBatchIds",
            "Number of Out of order batch IDs.", "batches"),
        statisticsFactory.createIntCounter("abandonedWriteRequests",
            "Number of write opertations abandond by clients", "requests"),
        statisticsFactory.createIntCounter("abandonedReadRequests",
            "Number of read opertations abandond by clients", "requests"),
        statisticsFactory.createLongCounter("receivedBytes",
            "Total number of bytes received from clients.", "bytes"),
        statisticsFactory.createLongCounter("sentBytes", "Total number of bytes sent to clients.",
            "bytes"),
        statisticsFactory.createIntGauge("messagesBeingReceived",
            "Current number of message being received off the network or being processed after reception.",
            "messages"),
        statisticsFactory.createLongGauge("messageBytesBeingReceived",
            "Current number of bytes consumed by messages being received or processed.", "bytes"),
        statisticsFactory.createIntCounter("connectionsTimedOut",
            "Total number of connections that have been timed out by the server because of client inactivity",
            "connections"),
        statisticsFactory.createIntGauge("threadQueueSize",
            "Current number of connections waiting for a thread to start processing their message.",
            "connections"),
        statisticsFactory.createIntGauge("acceptsInProgress",
            "Current number of server accepts that are attempting to do the initial handshake with the client.",
            "accepts"),
        statisticsFactory.createIntCounter("acceptThreadStarts",
            "Total number of threads created to deal with an accepted socket. Note that this is not the current number of threads.",
            "starts"),
        statisticsFactory.createIntCounter("connectionThreadStarts",
            "Total number of threads created to deal with a client connection. Note that this is not the current number of threads.",
            "starts"),
        statisticsFactory.createIntGauge("connectionThreads",
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
    stats = statisticsFactory.createAtomicStatistics(statType, ownerName);

    getRequestsId = stats.nameToId("getRequests");
    readGetRequestTimeId = stats.nameToId("readGetRequestTime");
    processGetTimeId = stats.nameToId("processGetTime");
    getResponsesId = stats.nameToId("getResponses");
    writeGetResponseTimeId = stats.nameToId("writeGetResponseTime");

    putRequestsId = stats.nameToId("putRequests");
    readPutRequestTimeId = stats.nameToId("readPutRequestTime");
    processPutTimeId = stats.nameToId("processPutTime");
    putResponsesId = stats.nameToId("putResponses");
    writePutResponseTimeId = stats.nameToId("writePutResponseTime");

    putAllRequestsId = stats.nameToId("putAllRequests");
    readPutAllRequestTimeId = stats.nameToId("readPutAllRequestTime");
    processPutAllTimeId = stats.nameToId("processPutAllTime");
    putAllResponsesId = stats.nameToId("putAllResponses");
    writePutAllResponseTimeId = stats.nameToId("writePutAllResponseTime");

    removeAllRequestsId = stats.nameToId("removeAllRequests");
    readRemoveAllRequestTimeId = stats.nameToId("readRemoveAllRequestTime");
    processRemoveAllTimeId = stats.nameToId("processRemoveAllTime");
    removeAllResponsesId = stats.nameToId("removeAllResponses");
    writeRemoveAllResponseTimeId = stats.nameToId("writeRemoveAllResponseTime");

    getAllRequestsId = stats.nameToId("getAllRequests");
    readGetAllRequestTimeId = stats.nameToId("readGetAllRequestTime");
    processGetAllTimeId = stats.nameToId("processGetAllTime");
    getAllResponsesId = stats.nameToId("getAllResponses");
    writeGetAllResponseTimeId = stats.nameToId("writeGetAllResponseTime");

    destroyRequestsId = stats.nameToId("destroyRequests");
    readDestroyRequestTimeId = stats.nameToId("readDestroyRequestTime");
    processDestroyTimeId = stats.nameToId("processDestroyTime");
    destroyResponsesId = stats.nameToId("destroyResponses");
    writeDestroyResponseTimeId = stats.nameToId("writeDestroyResponseTime");

    queryRequestsId = stats.nameToId("queryRequests");
    readQueryRequestTimeId = stats.nameToId("readQueryRequestTime");
    processQueryTimeId = stats.nameToId("processQueryTime");
    queryResponsesId = stats.nameToId("queryResponses");
    writeQueryResponseTimeId = stats.nameToId("writeQueryResponseTime");

    destroyRegionRequestsId = stats.nameToId("destroyRegionRequests");
    readDestroyRegionRequestTimeId = stats.nameToId("readDestroyRegionRequestTime");
    processDestroyRegionTimeId = stats.nameToId("processDestroyRegionTime");
    destroyRegionResponsesId = stats.nameToId("destroyRegionResponses");
    writeDestroyRegionResponseTimeId = stats.nameToId("writeDestroyRegionResponseTime");

    clearRegionRequestsId = stats.nameToId("clearRegionRequests");
    readClearRegionRequestTimeId = stats.nameToId("readClearRegionRequestTime");
    processClearRegionTimeId = stats.nameToId("processClearRegionTime");
    clearRegionResponsesId = stats.nameToId("clearRegionResponses");
    writeClearRegionResponseTimeId = stats.nameToId("writeClearRegionResponseTime");

    containsKeyRequestsId = stats.nameToId("containsKeyRequests");
    readContainsKeyRequestTimeId = stats.nameToId("readContainsKeyRequestTime");
    processContainsKeyTimeId = stats.nameToId("processContainsKeyTime");
    containsKeyResponsesId = stats.nameToId("containsKeyResponses");
    writeContainsKeyResponseTimeId = stats.nameToId("writeContainsKeyResponseTime");

    processBatchRequestsId = stats.nameToId("processBatchRequests");
    readProcessBatchRequestTimeId = stats.nameToId("readProcessBatchRequestTime");
    processBatchTimeId = stats.nameToId("processBatchTime");
    processBatchResponsesId = stats.nameToId("processBatchResponses");
    writeProcessBatchResponseTimeId = stats.nameToId("writeProcessBatchResponseTime");
    batchSizeId = stats.nameToId("batchSize");

    clientNotificationRequestsId = stats.nameToId("clientNotificationRequests");
    readClientNotificationRequestTimeId = stats.nameToId("readClientNotificationRequestTime");
    processClientNotificationTimeId = stats.nameToId("processClientNotificationTime");

    updateClientNotificationRequestsId = stats.nameToId("updateClientNotificationRequests");
    readUpdateClientNotificationRequestTimeId =
        stats.nameToId("readUpdateClientNotificationRequestTime");
    processUpdateClientNotificationTimeId =
        stats.nameToId("processUpdateClientNotificationTime");

    clientReadyRequestsId = stats.nameToId("clientReadyRequests");
    readClientReadyRequestTimeId = stats.nameToId("readClientReadyRequestTime");
    processClientReadyTimeId = stats.nameToId("processClientReadyTime");
    clientReadyResponsesId = stats.nameToId("clientReadyResponses");
    writeClientReadyResponseTimeId = stats.nameToId("writeClientReadyResponseTime");

    closeConnectionRequestsId = stats.nameToId("closeConnectionRequests");
    readCloseConnectionRequestTimeId = stats.nameToId("readCloseConnectionRequestTime");
    processCloseConnectionTimeId = stats.nameToId("processCloseConnectionTime");

    currentClientConnectionsId = stats.nameToId("currentClientConnections");
    currentQueueConnectionsId = stats.nameToId("currentQueueConnections");
    currentClientsId = stats.nameToId("currentClients");
    failedConnectionAttemptsId = stats.nameToId("failedConnectionAttempts");

    outOfOrderBatchIdsId = stats.nameToId("outOfOrderGatewayBatchIds");

    abandonedWriteRequestsId = stats.nameToId("abandonedWriteRequests");
    abandonedReadRequestsId = stats.nameToId("abandonedReadRequests");

    receivedBytesId = stats.nameToId("receivedBytes");
    sentBytesId = stats.nameToId("sentBytes");

    messagesBeingReceivedId = stats.nameToId("messagesBeingReceived");
    messageBytesBeingReceivedId = stats.nameToId("messageBytesBeingReceived");
    connectionsTimedOutId = stats.nameToId("connectionsTimedOut");
    threadQueueSizeId = stats.nameToId("threadQueueSize");
    acceptsInProgressId = stats.nameToId("acceptsInProgress");
    acceptThreadStartsId = stats.nameToId("acceptThreadStarts");
    connectionThreadStartsId = stats.nameToId("connectionThreadStarts");
    connectionThreadsId = stats.nameToId("connectionThreads");

    connectionLoadId = stats.nameToId("connectionLoad");
    queueLoadId = stats.nameToId("queueLoad");
    loadPerConnectionId = stats.nameToId("loadPerConnection");
    loadPerQueueId = stats.nameToId("loadPerQueue");
  }

  public void incAcceptThreadsCreated() {
    stats.incInt(acceptThreadStartsId, 1);
  }

  public void incConnectionThreadsCreated() {
    stats.incInt(connectionThreadStartsId, 1);
  }

  public void incAcceptsInProgress() {
    stats.incInt(acceptsInProgressId, 1);
  }

  public void decAcceptsInProgress() {
    stats.incInt(acceptsInProgressId, -1);
  }

  public void incConnectionThreads() {
    stats.incInt(connectionThreadsId, 1);
  }

  public void decConnectionThreads() {
    stats.incInt(connectionThreadsId, -1);
  }

  public void incAbandonedWriteRequests() {
    stats.incInt(abandonedWriteRequestsId, 1);
  }

  public void incAbandonedReadRequests() {
    stats.incInt(abandonedReadRequestsId, 1);
  }

  public void incFailedConnectionAttempts() {
    stats.incInt(failedConnectionAttemptsId, 1);
  }

  public void incConnectionsTimedOut() {
    stats.incInt(connectionsTimedOutId, 1);
  }

  public void incCurrentClientConnections() {
    stats.incInt(currentClientConnectionsId, 1);
  }

  public void decCurrentClientConnections() {
    stats.incInt(currentClientConnectionsId, -1);
  }

  public int getCurrentClientConnections() {
    return stats.getInt(currentClientConnectionsId);
  }

  public void incCurrentQueueConnections() {
    stats.incInt(currentQueueConnectionsId, 1);
  }

  public void decCurrentQueueConnections() {
    stats.incInt(currentQueueConnectionsId, -1);
  }

  public int getCurrentQueueConnections() {
    return stats.getInt(currentQueueConnectionsId);
  }

  public void incCurrentClients() {
    stats.incInt(currentClientsId, 1);
  }

  public void decCurrentClients() {
    stats.incInt(currentClientsId, -1);
  }

  public void incThreadQueueSize() {
    stats.incInt(threadQueueSizeId, 1);
  }

  public void decThreadQueueSize() {
    stats.incInt(threadQueueSizeId, -1);
  }

  public void incReadGetRequestTime(long delta) {
    stats.incLong(readGetRequestTimeId, delta);
    stats.incInt(getRequestsId, 1);
  }

  public void incProcessGetTime(long delta) {
    stats.incLong(processGetTimeId, delta);
  }

  public void incWriteGetResponseTime(long delta) {
    stats.incLong(writeGetResponseTimeId, delta);
    stats.incInt(getResponsesId, 1);
  }

  public void incReadPutAllRequestTime(long delta) {
    stats.incLong(readPutAllRequestTimeId, delta);
    stats.incInt(putAllRequestsId, 1);
  }

  public void incProcessPutAllTime(long delta) {
    stats.incLong(processPutAllTimeId, delta);
  }

  public void incWritePutAllResponseTime(long delta) {
    stats.incLong(writePutAllResponseTimeId, delta);
    stats.incInt(putAllResponsesId, 1);
  }

  public void incReadRemoveAllRequestTime(long delta) {
    stats.incLong(readRemoveAllRequestTimeId, delta);
    stats.incInt(removeAllRequestsId, 1);
  }

  public void incProcessRemoveAllTime(long delta) {
    stats.incLong(processRemoveAllTimeId, delta);
  }

  public void incWriteRemoveAllResponseTime(long delta) {
    stats.incLong(writeRemoveAllResponseTimeId, delta);
    stats.incInt(removeAllResponsesId, 1);
  }

  public void incReadGetAllRequestTime(long delta) {
    stats.incLong(readGetAllRequestTimeId, delta);
    stats.incInt(getAllRequestsId, 1);
  }

  public void incProcessGetAllTime(long delta) {
    stats.incLong(processGetAllTimeId, delta);
  }

  public void incWriteGetAllResponseTime(long delta) {
    stats.incLong(writeGetAllResponseTimeId, delta);
    stats.incInt(getAllResponsesId, 1);
  }

  public void incReadPutRequestTime(long delta) {
    stats.incLong(readPutRequestTimeId, delta);
    stats.incInt(putRequestsId, 1);
  }

  public void incProcessPutTime(long delta) {
    stats.incLong(processPutTimeId, delta);
  }

  public void incWritePutResponseTime(long delta) {
    stats.incLong(writePutResponseTimeId, delta);
    stats.incInt(putResponsesId, 1);
  }

  public void incReadDestroyRequestTime(long delta) {
    stats.incLong(readDestroyRequestTimeId, delta);
    stats.incInt(destroyRequestsId, 1);
  }

  public void incProcessDestroyTime(long delta) {
    stats.incLong(processDestroyTimeId, delta);
  }

  public void incWriteDestroyResponseTime(long delta) {
    stats.incLong(writeDestroyResponseTimeId, delta);
    stats.incInt(destroyResponsesId, 1);
  }



  public void incReadInvalidateRequestTime(long delta) {
    // this.stats.incLong(readInvalidateRequestTimeId, delta);
    // this.stats.incInt(invalidateRequestsId, 1);
  }

  public void incProcessInvalidateTime(long delta) {
    // this.stats.incLong(processInvalidateTimeId, delta);
  }

  public void incWriteInvalidateResponseTime(long delta) {
    // this.stats.incLong(writeInvalidateResponseTimeId, delta);
    // this.stats.incInt(invalidateResponsesId, 1);
  }



  public void incReadSizeRequestTime(long delta) {
    // this.stats.incLong(readSizeRequestTimeId, delta);
    // this.stats.incInt(sizeRequestsId, 1);
  }

  public void incProcessSizeTime(long delta) {
    // this.stats.incLong(processSizeTimeId, delta);
  }

  public void incWriteSizeResponseTime(long delta) {
    // this.stats.incLong(writeSizeResponseTimeId, delta);
    // this.stats.incInt(sizeResponsesId, 1);
  }



  public void incReadQueryRequestTime(long delta) {
    stats.incLong(readQueryRequestTimeId, delta);
    stats.incInt(queryRequestsId, 1);
  }

  public void incProcessQueryTime(long delta) {
    stats.incLong(processQueryTimeId, delta);
  }

  public void incWriteQueryResponseTime(long delta) {
    stats.incLong(writeQueryResponseTimeId, delta);
    stats.incInt(queryResponsesId, 1);
  }

  public void incProcessCreateCqTime(long delta) {
    // this.stats.incLong(processCreateCqTimeId, delta);
  }

  public void incProcessCloseCqTime(long delta) {
    // this.stats.incLong(processCloseCqTimeId, delta);
  }

  public void incProcessExecuteCqWithIRTime(long delta) {
    // this.stats.incLong(processExecuteCqWithIRCqTimeId, delta);
  }

  public void incProcessStopCqTime(long delta) {
    // this.stats.incLong(processStopCqTimeId, delta);
  }

  public void incProcessCloseClientCqsTime(long delta) {
    // this.stats.incLong(processCloseClientCqsTimeId, delta);
  }

  public void incProcessGetCqStatsTime(long delta) {
    // this.stats.incLong(processGetCqStatsTimeId, delta);
  }

  public void incReadDestroyRegionRequestTime(long delta) {
    stats.incLong(readDestroyRegionRequestTimeId, delta);
    stats.incInt(destroyRegionRequestsId, 1);
  }

  public void incProcessDestroyRegionTime(long delta) {
    stats.incLong(processDestroyRegionTimeId, delta);
  }

  public void incWriteDestroyRegionResponseTime(long delta) {
    stats.incLong(writeDestroyRegionResponseTimeId, delta);
    stats.incInt(destroyRegionResponsesId, 1);
  }

  public void incReadContainsKeyRequestTime(long delta) {
    stats.incLong(readContainsKeyRequestTimeId, delta);
    stats.incInt(containsKeyRequestsId, 1);
  }

  public void incProcessContainsKeyTime(long delta) {
    stats.incLong(processContainsKeyTimeId, delta);
  }

  public void incWriteContainsKeyResponseTime(long delta) {
    stats.incLong(writeContainsKeyResponseTimeId, delta);
    stats.incInt(containsKeyResponsesId, 1);
  }

  public void incReadClearRegionRequestTime(long delta) {
    stats.incLong(readClearRegionRequestTimeId, delta);
    stats.incInt(clearRegionRequestsId, 1);
  }

  public void incProcessClearRegionTime(long delta) {
    stats.incLong(processClearRegionTimeId, delta);
  }

  public void incWriteClearRegionResponseTime(long delta) {
    stats.incLong(writeClearRegionResponseTimeId, delta);
    stats.incInt(clearRegionResponsesId, 1);
  }

  public void incReadProcessBatchRequestTime(long delta) {
    stats.incLong(readProcessBatchRequestTimeId, delta);
    stats.incInt(processBatchRequestsId, 1);
  }

  public void incWriteProcessBatchResponseTime(long delta) {
    stats.incLong(writeProcessBatchResponseTimeId, delta);
    stats.incInt(processBatchResponsesId, 1);
  }

  public void incProcessBatchTime(long delta) {
    stats.incLong(processBatchTimeId, delta);
  }

  public void incBatchSize(long size) {
    stats.incLong(batchSizeId, size);
  }

  public void incReadClientNotificationRequestTime(long delta) {
    stats.incLong(readClientNotificationRequestTimeId, delta);
    stats.incInt(clientNotificationRequestsId, 1);
  }

  public void incProcessClientNotificationTime(long delta) {
    stats.incLong(processClientNotificationTimeId, delta);
  }

  public void incReadUpdateClientNotificationRequestTime(long delta) {
    stats.incLong(readUpdateClientNotificationRequestTimeId, delta);
    stats.incInt(updateClientNotificationRequestsId, 1);
  }

  public void incProcessUpdateClientNotificationTime(long delta) {
    stats.incLong(processUpdateClientNotificationTimeId, delta);
  }

  public void incReadCloseConnectionRequestTime(long delta) {
    stats.incLong(readCloseConnectionRequestTimeId, delta);
    stats.incInt(closeConnectionRequestsId, 1);
  }

  public void incProcessCloseConnectionTime(long delta) {
    stats.incLong(processCloseConnectionTimeId, delta);
  }

  public void incOutOfOrderBatchIds() {
    stats.incInt(outOfOrderBatchIdsId, 1);
  }

  @Override
  public void incReceivedBytes(long v) {
    stats.incLong(receivedBytesId, v);
  }

  @Override
  public void incSentBytes(long v) {
    stats.incLong(sentBytesId, v);
  }

  @Override
  public void incMessagesBeingReceived(int bytes) {
    stats.incInt(messagesBeingReceivedId, 1);
    if (bytes > 0) {
      stats.incLong(messageBytesBeingReceivedId, bytes);
    }
  }

  @Override
  public void decMessagesBeingReceived(int bytes) {
    stats.incInt(messagesBeingReceivedId, -1);
    if (bytes > 0) {
      stats.incLong(messageBytesBeingReceivedId, -bytes);
    }
  }

  public void incReadClientReadyRequestTime(long delta) {
    stats.incLong(readClientReadyRequestTimeId, delta);
    stats.incInt(clientReadyRequestsId, 1);
  }

  public void incProcessClientReadyTime(long delta) {
    stats.incLong(processClientReadyTimeId, delta);
  }

  public void incWriteClientReadyResponseTime(long delta) {
    stats.incLong(writeClientReadyResponseTimeId, delta);
    stats.incInt(clientReadyResponsesId, 1);
  }

  public void setLoad(ServerLoad load) {
    stats.setDouble(connectionLoadId, load.getConnectionLoad());
    stats.setDouble(queueLoadId, load.getSubscriptionConnectionLoad());
    stats.setDouble(loadPerConnectionId, load.getLoadPerConnection());
    stats.setDouble(loadPerQueueId, load.getLoadPerSubscriptionConnection());
  }

  public double getQueueLoad() {
    return stats.getDouble(queueLoadId);
  }

  public double getLoadPerQueue() {
    return stats.getDouble(loadPerQueueId);
  }

  public double getConnectionLoad() {
    return stats.getDouble(connectionLoadId);
  }

  public double getLoadPerConnection() {
    return stats.getDouble(loadPerConnectionId);
  }

  public long getProcessBatchRequests() {
    return stats.getLong(processBatchRequestsId);
  }

  public void close() {
    stats.close();
  }

  public PoolStatHelper getCnxPoolHelper() {
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
