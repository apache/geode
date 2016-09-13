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
package org.apache.geode.internal.cache.wan;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;

public class GatewayReceiverStats extends CacheServerStats {

  private static final String typeName = "GatewayReceiverStatistics";

  // ////////////////// Statistic "Id" Fields ////////////////////

//  /** Name of the events queued statistic */
//  private static final String FAILOVER_BATCHES_RECEIVED = "failoverBatchesReceived";

  /** Name of the events not queued because conflated statistic */
  private static final String DUPLICATE_BATCHES_RECEIVED = "duplicateBatchesReceived";

  /** Name of the event queue time statistic */
  private static final String OUT_OF_ORDER_BATCHES_RECEIVED = "outoforderBatchesReceived";

  /** Name of the event queue size statistic */
  private static final String EARLY_ACKS = "earlyAcks";

  /** Name of the events distributed statistic */
  private static final String EVENTS_RECEIVED = "eventsReceived";

  /** Name of the events exceeding alert threshold statistic */
  private static final String CREAT_REQUESTS = "createRequests";

  /** Name of the batch distribution time statistic */
  private static final String UPDATE_REQUESTS = "updateRequest";

  /** Name of the batches distributed statistic */
  private static final String DESTROY_REQUESTS = "destroyRequest";

  /** Name of the batches redistributed statistic */
  private static final String UNKNOWN_OPERATIONS_RECEIVED = "unknowsOperationsReceived";

  /** Name of the unprocessed events added by primary statistic */
  private static final String EXCEPTIONS_OCCURED = "exceptionsOccured";

//  /** Id of the events queued statistic */
//  private int failoverBatchesReceivedId;

  /** Id of the events not queued because conflated statistic */
  private int duplicateBatchesReceivedId;

  /** Id of the event queue time statistic */
  private int outoforderBatchesReceivedId;

  /** Id of the event queue size statistic */
  private int earlyAcksId;

  /** Id of the events distributed statistic */
  private int eventsReceivedId;

  /** Id of the events exceeding alert threshold statistic */
  private int createRequestId;

  /** Id of the batch distribution time statistic */
  private int updateRequestId;

  /** Id of the batches distributed statistic */
  private int destroyRequestId;

  /** Id of the batches redistributed statistic */
  private int unknowsOperationsReceivedId;

  /** Id of the unprocessed events added by primary statistic */
  private int exceptionsOccuredId;

  // ///////////////////// Constructors ///////////////////////

  public static GatewayReceiverStats createGatewayReceiverStats(String ownerName) {
    StatisticsFactory f = InternalDistributedSystem.getAnyInstance();
    StatisticDescriptor[] descriptors = new StatisticDescriptor[] {
        f
            .createIntCounter(
                DUPLICATE_BATCHES_RECEIVED,
                "number of batches which have already been seen by this GatewayReceiver",
                "nanoseconds"),
        f.createIntCounter(OUT_OF_ORDER_BATCHES_RECEIVED,
            "number of batches which are out of order on this GatewayReceiver",
            "operations"),
        f.createIntCounter(EARLY_ACKS,
            "number of early acknowledgements sent to gatewaySenders",
            "operations"),
        f
            .createIntCounter(
                EVENTS_RECEIVED,
                "total number events across the batched received by this GatewayReceiver",
                "operations"),
        f
            .createIntCounter(
                CREAT_REQUESTS,
                "total number of create operations received by this GatewayReceiver",
                "operations"),
        f
            .createIntCounter(
                UPDATE_REQUESTS,
                "total number of update operations received by this GatewayReceiver",
                "operations"),
        f
            .createIntCounter(
                DESTROY_REQUESTS,
                "total number of destroy operations received by this GatewayReceiver",
                "operations"),
        f
            .createIntCounter(
                UNKNOWN_OPERATIONS_RECEIVED,
                "total number of unknown operations received by this GatewayReceiver",
                "operations"),
        f.createIntCounter(EXCEPTIONS_OCCURED,
            "number of exceptions occured while porcessing the batches",
            "operations") };
    return new GatewayReceiverStats(f, ownerName, typeName, descriptors);

  }

  public GatewayReceiverStats(StatisticsFactory f, String ownerName,
      String typeName, StatisticDescriptor[] descriptiors) {
    super(f, ownerName, typeName, descriptiors);
    // Initialize id fields
    //failoverBatchesReceivedId = statType.nameToId(FAILOVER_BATCHES_RECEIVED);
    duplicateBatchesReceivedId = statType.nameToId(DUPLICATE_BATCHES_RECEIVED);
    outoforderBatchesReceivedId = statType
        .nameToId(OUT_OF_ORDER_BATCHES_RECEIVED);
    earlyAcksId = statType.nameToId(EARLY_ACKS);
    eventsReceivedId = statType.nameToId(EVENTS_RECEIVED);
    createRequestId = statType.nameToId(CREAT_REQUESTS);
    updateRequestId = statType.nameToId(UPDATE_REQUESTS);
    destroyRequestId = statType.nameToId(DESTROY_REQUESTS);
    unknowsOperationsReceivedId = statType
        .nameToId(UNKNOWN_OPERATIONS_RECEIVED);
    exceptionsOccuredId = statType.nameToId(EXCEPTIONS_OCCURED);
  }

  // /////////////////// Instance Methods /////////////////////

//  /**
//   * Increments the number of failover batches received by 1.
//   */
//  public void incFailoverBatchesReceived() {
//    this.stats.incInt(failoverBatchesReceivedId, 1);
//  }
//
//  public int getFailoverBatchesReceived() {
//    return this.stats.getInt(failoverBatchesReceivedId);
//  }

  /**
   * Increments the number of duplicate batches received by 1.
   */
  public void incDuplicateBatchesReceived() {
    this.stats.incInt(duplicateBatchesReceivedId, 1);
  }

  public int getDuplicateBatchesReceived() {
    return this.stats.getInt(duplicateBatchesReceivedId);
  }

  /**
   * Increments the number of out of order batches received by 1.
   */
  public void incOutoforderBatchesReceived() {
    this.stats.incInt(outoforderBatchesReceivedId, 1);
  }

  public int getOutoforderBatchesReceived() {
    return this.stats.getInt(outoforderBatchesReceivedId);
  }

  /**
   * Increments the number of early acks by 1.
   */
  public void incEarlyAcks() {
    this.stats.incInt(earlyAcksId, 1);
  }

  public int getEarlyAcks() {
    return this.stats.getInt(earlyAcksId);
  }

  /**
   * Increments the number of events received by 1.
   */
  public void incEventsReceived(int delta) {
    this.stats.incInt(eventsReceivedId, delta);
  }

  public int getEventsReceived() {
    return this.stats.getInt(eventsReceivedId);
  }

  /**
   * Increments the number of create requests by 1.
   */
  public void incCreateRequest() {
    this.stats.incInt(createRequestId, 1);
  }

  public int getCreateRequest() {
    return this.stats.getInt(createRequestId);
  }

  /**
   * Increments the number of update requests by 1.
   */
  public void incUpdateRequest() {
    this.stats.incInt(updateRequestId, 1);
  }

  public int getUpdateRequest() {
    return this.stats.getInt(updateRequestId);
  }

  /**
   * Increments the number of destroy request received by 1.
   */
  public void incDestroyRequest() {
    this.stats.incInt(destroyRequestId, 1);
  }

  public int getDestroyRequest() {
    return this.stats.getInt(destroyRequestId);
  }

  /**
   * Increments the number of unknown operations received by 1.
   */
  public void incUnknowsOperationsReceived() {
    this.stats.incInt(unknowsOperationsReceivedId, 1);
  }

  public int getUnknowsOperationsReceived() {
    return this.stats.getInt(unknowsOperationsReceivedId);
  }

  /**
   * Increments the number of exceptions occured by 1.
   */
  public void incExceptionsOccured() {
    this.stats.incInt(exceptionsOccuredId, 1);
  }

  public int getExceptionsOccured() {
    return this.stats.getInt(exceptionsOccuredId);
  }

  /**
   * Returns the current time (ns).
   * 
   * @return the current time (ns)
   */
  public long startTime() {
    return DistributionStats.getStatTime();
  }

}
