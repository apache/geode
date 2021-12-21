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

public class ServerClusterStatsMonitor {

  private static final String NUM_CLIENTS = "CurrentClients";

  private static final String ACTIVE_QUERY_COUNT = "ActiveCQCount";

  private static final String REGISTERED_QUERY_COUNT = "RegisteredCQCount";

  private static final String QUERY_REQUEST_RATE = "QueryRequestRate";

  private static final String NUM_SUBSCRIPTIONS = "NumSubscriptions";

  private final StatsAggregator aggregator;

  private final Map<String, Class<?>> typeMap;

  public void aggregate(FederationComponent newState, FederationComponent oldState) {
    aggregator.aggregate(newState, oldState);

  }

  public ServerClusterStatsMonitor() {
    typeMap = new HashMap<String, Class<?>>();
    intTypeMap();
    aggregator = new StatsAggregator(typeMap);
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
