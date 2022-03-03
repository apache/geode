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
package org.apache.geode.management.internal.beans.stats;

import java.util.HashMap;
import java.util.Map;

import org.apache.geode.management.internal.FederationComponent;

public class GatewayReceiverClusterStatsMonitor {

  private static final String CREATE_REQUEST_RATE = "CreateRequestsRate";

  private static final String DESTROY_REQUEST_RATE = "DestroyRequestsRate";

  private static final String UPDATE_REQUEST_RATE = "UpdateRequestsRate";

  private static final String EVENTS_RECEIVED_RATE = "EventsReceivedRate";



  private final StatsAggregator aggregator;

  private final Map<String, Class<?>> typeMap;

  public void aggregate(FederationComponent newState, FederationComponent oldState) {
    aggregator.aggregate(newState, oldState);
  }

  public GatewayReceiverClusterStatsMonitor() {
    typeMap = new HashMap<>();
    intTypeMap();
    aggregator = new StatsAggregator(typeMap);
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
