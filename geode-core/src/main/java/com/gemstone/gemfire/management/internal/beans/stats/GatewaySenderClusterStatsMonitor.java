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
package com.gemstone.gemfire.management.internal.beans.stats;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.management.internal.FederationComponent;

/**
 *
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
