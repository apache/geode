/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.asyncqueue.internal;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderStats;

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
