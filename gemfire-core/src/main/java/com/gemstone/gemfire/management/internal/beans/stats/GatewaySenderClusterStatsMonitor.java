/*
 * ========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans.stats;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.management.internal.FederationComponent;

/**
 *
 * @author rishim
 *
 */
public class GatewaySenderClusterStatsMonitor {

  private static final String AVERAGE_DISTRIBUTION_TIME_PER_BATCH = "AverageDistributionTimePerBatch";

  private static final String BATCHES_DISPATCHED_RATE = "BatchesDispatchedRate";

  private static final String EVENT_QUEUE_SIZE = "EventQueueSize";

  private static final String EVENTS_QUEUED_RATE = "EventsQueuedRate";

  private static final String TOTAL_BATCHES_REDISTRIBUTED = "TotalBatchesRedistributed";

  private static final String TOTAL_EVENTS_CONFLATED = "TotalEventsConflated";


  private StatsAggregator aggregator;

  private Map<String, Class<?>> typeMap;

  public void aggregate(FederationComponent newState,
      FederationComponent oldState) {
    aggregator.aggregate(newState, oldState);
  }

  public GatewaySenderClusterStatsMonitor() {
    this.typeMap = new HashMap<String, Class<?>>();
    intTypeMap();
    this.aggregator = new StatsAggregator(typeMap);
  }

  private void intTypeMap() {
    typeMap.put(AVERAGE_DISTRIBUTION_TIME_PER_BATCH, Long.TYPE);
    typeMap.put(BATCHES_DISPATCHED_RATE, Float.TYPE);
    typeMap.put(EVENT_QUEUE_SIZE, Integer.TYPE);
    typeMap.put(EVENTS_QUEUED_RATE, Float.TYPE);
    typeMap.put(TOTAL_BATCHES_REDISTRIBUTED, Integer.TYPE);
    typeMap.put(TOTAL_EVENTS_CONFLATED, Integer.TYPE);

  }

  public long getGatewaySenderAverageDistributionTimePerBatch() {
    return aggregator.getLongValue(AVERAGE_DISTRIBUTION_TIME_PER_BATCH);
  }

  public float getGatewaySenderBatchesDispatchedRate() {
    return aggregator.getFloatValue(BATCHES_DISPATCHED_RATE);
  }

  public int getGatewaySenderEventQueueSize() {
    return aggregator.getIntValue(EVENT_QUEUE_SIZE);
  }

  public float getGatewaySenderEventsQueuedRate() {
    return aggregator.getFloatValue(EVENTS_QUEUED_RATE);
  }

  public int getGatewaySenderTotalBatchesRedistributed() {
    return aggregator.getIntValue(TOTAL_BATCHES_REDISTRIBUTED);
  }

  public int getGatewaySenderTotalEventsConflated() {
    return aggregator.getIntValue(TOTAL_EVENTS_CONFLATED);
  }

}
