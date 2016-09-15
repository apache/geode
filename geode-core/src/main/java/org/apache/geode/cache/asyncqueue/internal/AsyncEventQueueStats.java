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
package org.apache.geode.cache.asyncqueue.internal;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;

public class AsyncEventQueueStats extends GatewaySenderStats {

  public static final String typeName = "AsyncEventQueueStatistics";
  
  /** The <code>StatisticsType</code> of the statistics */
  private static final StatisticsType type;
  
  
  static {

  StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

  type = f.createType(typeName, "Stats for activity in the AsyncEventQueue",
     new StatisticDescriptor[] {
      f.createIntCounter
      (EVENTS_RECEIVED,
       "Number of events received by this queue.",
       "operations"),
      f.createIntCounter
      (EVENTS_QUEUED,
       "Number of events added to the event queue.",
       "operations"),
      f.createLongCounter
       (EVENT_QUEUE_TIME,
        "Total time spent queueing events.",
        "nanoseconds"),
      f.createIntGauge
       (EVENT_QUEUE_SIZE,
        "Size of the event queue.",
        "operations", false),
      f.createIntGauge
        (TMP_EVENT_QUEUE_SIZE,
         "Size of the temporary events queue.",
         "operations", false),
      f.createIntCounter
       (EVENTS_NOT_QUEUED_CONFLATED,
        "Number of events received but not added to the event queue because the queue already contains an event with the event's key.",
        "operations"),
      f.createIntCounter
        (EVENTS_CONFLATED_FROM_BATCHES,
         "Number of events conflated from batches.",
         "operations"),
      f.createIntCounter
       (EVENTS_DISTRIBUTED,
        "Number of events removed from the event queue and sent.",
        "operations"),
      f.createIntCounter
       (EVENTS_EXCEEDING_ALERT_THRESHOLD,
        "Number of events exceeding the alert threshold.",
        "operations", false),
      f.createLongCounter
       (BATCH_DISTRIBUTION_TIME,
        "Total time spent distributing batches of events to receivers.",
        "nanoseconds"),
      f.createIntCounter
       (BATCHES_DISTRIBUTED,
        "Number of batches of events removed from the event queue and sent.",
        "operations"),
      f.createIntCounter
       (BATCHES_REDISTRIBUTED,
        "Number of batches of events removed from the event queue and resent.",
        "operations", false),
      f.createIntCounter
       (UNPROCESSED_TOKENS_ADDED_BY_PRIMARY,
        "Number of tokens added to the secondary's unprocessed token map by the primary (though a listener).",
        "tokens"),
      f.createIntCounter
       (UNPROCESSED_EVENTS_ADDED_BY_SECONDARY,
        "Number of events added to the secondary's unprocessed event map by the secondary.",
        "events"),
      f.createIntCounter
       (UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY,
        "Number of events removed from the secondary's unprocessed event map by the primary (though a listener).",
        "events"),
      f.createIntCounter
       (UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY,
        "Number of tokens removed from the secondary's unprocessed token map by the secondary.",
        "tokens"),
      f.createIntCounter
       (UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT,
        "Number of events removed from the secondary's unprocessed event map by a timeout.",
        "events"),
      f.createIntCounter
       (UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT,
        "Number of tokens removed from the secondary's unprocessed token map by a timeout.",
        "tokens"),
      f.createIntGauge
       (UNPROCESSED_EVENT_MAP_SIZE,
        "Current number of entries in the secondary's unprocessed event map.",
        "events", false),
      f.createIntGauge
       (UNPROCESSED_TOKEN_MAP_SIZE,
        "Current number of entries in the secondary's unprocessed token map.",
        "tokens", false),
      f.createIntGauge
       (CONFLATION_INDEXES_MAP_SIZE,
        "Current number of entries in the conflation indexes map.",
        "events"),
      f.createIntCounter
        (NOT_QUEUED_EVENTS,
         "Number of events not added to queue.",
         "events"),
      f.createIntCounter
        (EVENTS_FILTERED,
         "Number of events filtered through GatewayEventFilter.",
         "events"),
      f.createIntCounter
        (LOAD_BALANCES_COMPLETED,
         "Number of load balances completed",
         "operations"),
      f.createIntGauge
        (LOAD_BALANCES_IN_PROGRESS,
         "Number of load balances in progress",
         "operations"),
      f.createLongCounter
        (LOAD_BALANCE_TIME,
         "Total time spent load balancing this sender",
         "nanoseconds"),
  });

  // Initialize id fields
  eventsReceivedId = type.nameToId(EVENTS_RECEIVED);
  eventsQueuedId = type.nameToId(EVENTS_QUEUED);
  eventsNotQueuedConflatedId = type.nameToId(EVENTS_NOT_QUEUED_CONFLATED);
  eventQueueTimeId = type.nameToId(EVENT_QUEUE_TIME);
  eventQueueSizeId = type.nameToId(EVENT_QUEUE_SIZE);
  eventTmpQueueSizeId = type.nameToId(TMP_EVENT_QUEUE_SIZE);
  eventsDistributedId = type.nameToId(EVENTS_DISTRIBUTED);
  eventsExceedingAlertThresholdId = type.nameToId(EVENTS_EXCEEDING_ALERT_THRESHOLD);
  batchDistributionTimeId = type.nameToId(BATCH_DISTRIBUTION_TIME);
  batchesDistributedId = type.nameToId(BATCHES_DISTRIBUTED);
  batchesRedistributedId = type.nameToId(BATCHES_REDISTRIBUTED);
  unprocessedTokensAddedByPrimaryId = type.nameToId(UNPROCESSED_TOKENS_ADDED_BY_PRIMARY);
  unprocessedEventsAddedBySecondaryId = type.nameToId(UNPROCESSED_EVENTS_ADDED_BY_SECONDARY);
  unprocessedEventsRemovedByPrimaryId = type.nameToId(UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY);
  unprocessedTokensRemovedBySecondaryId = type.nameToId(UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY);
  unprocessedEventsRemovedByTimeoutId = type.nameToId(UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT);
  unprocessedTokensRemovedByTimeoutId = type.nameToId(UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT);
  unprocessedEventMapSizeId = type.nameToId(UNPROCESSED_EVENT_MAP_SIZE);
  unprocessedTokenMapSizeId = type.nameToId(UNPROCESSED_TOKEN_MAP_SIZE); 
  conflationIndexesMapSizeId = type.nameToId(CONFLATION_INDEXES_MAP_SIZE); 
  notQueuedEventsId = type.nameToId(NOT_QUEUED_EVENTS);
  eventsFilteredId = type.nameToId(EVENTS_FILTERED);
  eventsConflatedFromBatchesId = type.nameToId(EVENTS_CONFLATED_FROM_BATCHES);
  loadBalancesCompletedId = type.nameToId(LOAD_BALANCES_COMPLETED);
  loadBalancesInProgressId = type.nameToId(LOAD_BALANCES_IN_PROGRESS);
  loadBalanceTimeId = type.nameToId(LOAD_BALANCE_TIME);
  }
  
  /**
   * Constructor.
   *
   * @param f The <code>StatisticsFactory</code> which creates the
   * <code>Statistics</code> instance
   * @param asyncQueueId The id of the <code>AsyncEventQueue</code> used to
   * generate the name of the <code>Statistics</code>
   */
  public AsyncEventQueueStats(StatisticsFactory f, String asyncQueueId) {
    super(f, asyncQueueId, type);
  }
}
