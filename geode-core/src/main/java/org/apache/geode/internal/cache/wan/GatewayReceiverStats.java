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
package org.apache.geode.internal.cache.wan;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.statistics.meters.LegacyStatCounter;

public class GatewayReceiverStats extends CacheServerStats {

  private static final String typeName = "GatewayReceiverStatistics";

  /**
   * Name of the events not queued because conflated statistic
   */
  private static final String DUPLICATE_BATCHES_RECEIVED = "duplicateBatchesReceived";

  /**
   * Name of the event queue time statistic
   */
  private static final String OUT_OF_ORDER_BATCHES_RECEIVED = "outoforderBatchesReceived";

  /**
   * Name of the event queue size statistic
   */
  private static final String EARLY_ACKS = "earlyAcks";

  /**
   * Name of the events distributed statistic
   */
  private static final String EVENTS_RECEIVED = "eventsReceived";

  /**
   * Name of the events exceeding alert threshold statistic
   */
  private static final String CREAT_REQUESTS = "createRequests";

  /**
   * Name of the batch distribution time statistic
   */
  private static final String UPDATE_REQUESTS = "updateRequest";

  /**
   * Name of the batches distributed statistic
   */
  private static final String DESTROY_REQUESTS = "destroyRequest";

  /**
   * Name of the batches redistributed statistic
   */
  private static final String UNKNOWN_OPERATIONS_RECEIVED = "unknowsOperationsReceived";

  /**
   * Name of the unprocessed events added by primary statistic
   */
  private static final String EXCEPTIONS_OCCURRED = "exceptionsOccurred";

  /**
   * Name of the events retried
   */
  private static final String EVENTS_RETRIED = "eventsRetried";
  private final MeterRegistry meterRegistry;

  // /** Id of the events queued statistic */
  // private int failoverBatchesReceivedId;

  /**
   * Id of the events not queued because conflated statistic
   */
  private final int duplicateBatchesReceivedId;

  /**
   * Id of the event queue time statistic
   */
  private final int outoforderBatchesReceivedId;

  /**
   * Id of the event queue size statistic
   */
  private final int earlyAcksId;

  private final Counter eventsReceivedCounter;
  private static final String EVENTS_RECEIVED_COUNTER_NAME =
      "geode.gateway.receiver.events";
  private static final String EVENTS_RECEIVED_COUNTER_DESCRIPTION =
      "total number events across the batched received by this GatewayReceiver";
  private static final String EVENTS_RECEIVED_COUNTER_UNITS = "operations";

  /**
   * Id of the events exceeding alert threshold statistic
   */
  private final int createRequestId;

  /**
   * Id of the batch distribution time statistic
   */
  private final int updateRequestId;

  /**
   * Id of the batches distributed statistic
   */
  private final int destroyRequestId;

  /**
   * Id of the batches redistributed statistic
   */
  private final int unknowsOperationsReceivedId;

  /**
   * Id of the unprocessed events added by primary statistic
   */
  private final int exceptionsOccurredId;

  /**
   * Id of the events retried statistic
   */
  private final int eventsRetriedId;

  // ///////////////////// Constructors ///////////////////////

  public static GatewayReceiverStats createGatewayReceiverStats(StatisticsFactory f,
      String ownerName, MeterRegistry meterRegistry) {
    StatisticDescriptor[] descriptors = new StatisticDescriptor[] {
        f.createLongCounter(DUPLICATE_BATCHES_RECEIVED,
            "number of batches which have already been seen by this GatewayReceiver",
            "nanoseconds"),
        f.createLongCounter(OUT_OF_ORDER_BATCHES_RECEIVED,
            "number of batches which are out of order on this GatewayReceiver", "operations"),
        f.createLongCounter(EARLY_ACKS, "number of early acknowledgements sent to gatewaySenders",
            "operations"),
        f.createLongCounter(EVENTS_RECEIVED,
            EVENTS_RECEIVED_COUNTER_DESCRIPTION,
            EVENTS_RECEIVED_COUNTER_UNITS),
        f.createLongCounter(CREAT_REQUESTS,
            "total number of create operations received by this GatewayReceiver", "operations"),
        f.createLongCounter(UPDATE_REQUESTS,
            "total number of update operations received by this GatewayReceiver", "operations"),
        f.createLongCounter(DESTROY_REQUESTS,
            "total number of destroy operations received by this GatewayReceiver", "operations"),
        f.createLongCounter(UNKNOWN_OPERATIONS_RECEIVED,
            "total number of unknown operations received by this GatewayReceiver", "operations"),
        f.createLongCounter(EXCEPTIONS_OCCURRED,
            "number of exceptions occurred while porcessing the batches", "operations"),
        f.createLongCounter(EVENTS_RETRIED,
            "total number events retried by this GatewayReceiver due to exceptions", "operations")};
    return new GatewayReceiverStats(f, ownerName, typeName, descriptors, meterRegistry);

  }

  public GatewayReceiverStats(StatisticsFactory f, String ownerName, String typeName,
      StatisticDescriptor[] descriptiors, MeterRegistry meterRegistry) {
    super(f, ownerName, typeName, descriptiors);
    // Initialize id fields
    duplicateBatchesReceivedId = statType.nameToId(DUPLICATE_BATCHES_RECEIVED);
    outoforderBatchesReceivedId = statType.nameToId(OUT_OF_ORDER_BATCHES_RECEIVED);
    earlyAcksId = statType.nameToId(EARLY_ACKS);
    final int eventsReceivedId = statType.nameToId(EVENTS_RECEIVED);
    createRequestId = statType.nameToId(CREAT_REQUESTS);
    updateRequestId = statType.nameToId(UPDATE_REQUESTS);
    destroyRequestId = statType.nameToId(DESTROY_REQUESTS);
    unknowsOperationsReceivedId = statType.nameToId(UNKNOWN_OPERATIONS_RECEIVED);
    exceptionsOccurredId = statType.nameToId(EXCEPTIONS_OCCURRED);
    eventsRetriedId = statType.nameToId(EVENTS_RETRIED);

    this.meterRegistry = meterRegistry;
    eventsReceivedCounter = LegacyStatCounter.builder(EVENTS_RECEIVED_COUNTER_NAME)
        .longStatistic(stats, eventsReceivedId)
        .description(EVENTS_RECEIVED_COUNTER_DESCRIPTION)
        .baseUnit(EVENTS_RECEIVED_COUNTER_UNITS)
        .register(meterRegistry);
  }

  /**
   * Increments the number of duplicate batches received by 1.
   */
  public void incDuplicateBatchesReceived() {
    stats.incLong(duplicateBatchesReceivedId, 1);
  }

  public long getDuplicateBatchesReceived() {
    return stats.getLong(duplicateBatchesReceivedId);
  }

  /**
   * Increments the number of out of order batches received by 1.
   */
  public void incOutoforderBatchesReceived() {
    stats.incLong(outoforderBatchesReceivedId, 1);
  }

  public long getOutoforderBatchesReceived() {
    return stats.getLong(outoforderBatchesReceivedId);
  }

  /**
   * Increments the number of early acks by 1.
   */
  public void incEarlyAcks() {
    stats.incLong(earlyAcksId, 1);
  }

  public long getEarlyAcks() {
    return stats.getLong(earlyAcksId);
  }

  /**
   * Increments the number of events received by 1.
   */
  public void incEventsReceived(int delta) {
    eventsReceivedCounter.increment(delta);
  }

  public long getEventsReceived() {
    return (long) eventsReceivedCounter.count();
  }

  /**
   * Increments the number of create requests by 1.
   */
  public void incCreateRequest() {
    stats.incLong(createRequestId, 1);
  }

  public long getCreateRequest() {
    return stats.getLong(createRequestId);
  }

  /**
   * Increments the number of update requests by 1.
   */
  public void incUpdateRequest() {
    stats.incLong(updateRequestId, 1);
  }

  public long getUpdateRequest() {
    return stats.getLong(updateRequestId);
  }

  /**
   * Increments the number of destroy request received by 1.
   */
  public void incDestroyRequest() {
    stats.incLong(destroyRequestId, 1);
  }

  public long getDestroyRequest() {
    return stats.getLong(destroyRequestId);
  }

  /**
   * Increments the number of unknown operations received by 1.
   */
  public void incUnknowsOperationsReceived() {
    stats.incLong(unknowsOperationsReceivedId, 1);
  }

  public long getUnknowsOperationsReceived() {
    return stats.getLong(unknowsOperationsReceivedId);
  }

  public void incExceptionsOccurred(int delta) {
    stats.incLong(exceptionsOccurredId, delta);
  }

  public long getExceptionsOccurred() {
    return stats.getLong(exceptionsOccurredId);
  }

  /**
   * Increments the number of events received by 1.
   */
  public void incEventsRetried() {
    stats.incLong(eventsRetriedId, 1);
  }

  public long getEventsRetried() {
    return stats.getLong(eventsRetriedId);
  }

  /**
   * Returns the current time (ns).
   *
   * @return the current time (ns)
   */
  public long startTime() {
    return DistributionStats.getStatTime();
  }

  @Override
  public void close() {
    meterRegistry.remove(eventsReceivedCounter);
    super.close();
  }
}
