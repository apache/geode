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
public class GatewayReceiverClusterStatsMonitor {

  private static final String CREATE_REQUEST_RATE = "CreateRequestsRate";

  private static final String DESTROY_REQUEST_RATE = "DestroyRequestsRate";

  private static final String UPDATE_REQUEST_RATE = "UpdateRequestsRate";

  private static final String EVENTS_RECEIVED_RATE = "EventsReceivedRate";



  private StatsAggregator aggregator;

  private Map<String, Class<?>> typeMap;

  public void aggregate(FederationComponent newState,
      FederationComponent oldState) {
    aggregator.aggregate(newState, oldState);
  }

  public GatewayReceiverClusterStatsMonitor() {
    this.typeMap = new HashMap<String, Class<?>>();
    intTypeMap();
    this.aggregator = new StatsAggregator(typeMap);
  }

  private void intTypeMap() {
    typeMap.put(CREATE_REQUEST_RATE, Float.TYPE);
    typeMap.put(DESTROY_REQUEST_RATE, Float.TYPE);
    typeMap.put(UPDATE_REQUEST_RATE, Float.TYPE);
    typeMap.put(EVENTS_RECEIVED_RATE, Float.TYPE);

  }


  public float getGatewayReceiverCreateRequestsRate() {
    return aggregator.getFloatValue(CREATE_REQUEST_RATE);
  }

  public float getGatewayReceiverDestroyRequestsRate() {
    return aggregator.getFloatValue(DESTROY_REQUEST_RATE);
  }

  public float getGatewayReceiverUpdateRequestsRate() {
    return aggregator.getFloatValue(UPDATE_REQUEST_RATE);
  }
  public float getGatewayReceiverEventsReceivedRate() {
    return aggregator.getFloatValue(EVENTS_RECEIVED_RATE);
  }

}
