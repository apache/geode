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
public class ServerClusterStatsMonitor {

  private static final String NUM_CLIENTS = "CurrentClients";

  private static final String ACTIVE_QUERY_COUNT = "ActiveCQCount";

  private static final String REGISTERED_QUERY_COUNT = "RegisteredCQCount";

  private static final String QUERY_REQUEST_RATE = "QueryRequestRate";

  private static final String NUM_SUBSCRIPTIONS = "NumSubscriptions";

  private StatsAggregator aggregator;

  private Map<String, Class<?>> typeMap;

  public void aggregate(FederationComponent newState,
      FederationComponent oldState) {
    aggregator.aggregate(newState, oldState);

  }

  public ServerClusterStatsMonitor() {
    this.typeMap = new HashMap<String, Class<?>>();
    intTypeMap();
    this.aggregator = new StatsAggregator(typeMap);
  }

  private void intTypeMap() {
    typeMap.put(NUM_CLIENTS, Integer.TYPE);
    typeMap.put(ACTIVE_QUERY_COUNT, Long.TYPE);
    typeMap.put(QUERY_REQUEST_RATE, Float.TYPE);
    typeMap.put(REGISTERED_QUERY_COUNT, Long.TYPE);
    typeMap.put(NUM_SUBSCRIPTIONS, Integer.TYPE);

  }

  public int getNumClients() {
    return aggregator.getIntValue(NUM_CLIENTS);
  }

  public long getActiveCQCount() {
    return aggregator.getLongValue(ACTIVE_QUERY_COUNT);
  }

  public long getRegisteredCQCount() {
    return aggregator.getLongValue(REGISTERED_QUERY_COUNT);
  }

  public float getQueryRequestRate() {
    return aggregator.getFloatValue(QUERY_REQUEST_RATE);
  }

  public int getNumSubscriptions() {
    return aggregator.getIntValue(NUM_SUBSCRIPTIONS);
  }

}
