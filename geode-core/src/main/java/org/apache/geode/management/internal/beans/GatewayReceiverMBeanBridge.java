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
package org.apache.geode.management.internal.beans;

import java.util.List;
import java.util.Set;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.wan.GatewayReceiverStats;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.beans.stats.StatType;
import org.apache.geode.management.internal.beans.stats.StatsKey;
import org.apache.geode.management.internal.beans.stats.StatsRate;

public class GatewayReceiverMBeanBridge extends ServerBridge {

  private final GatewayReceiver gatewayReceiver;

  private StatsRate createRequestRate;
  private StatsRate updateRequestRate;
  private StatsRate destroyRequestRate;
  private StatsRate eventsReceivedRate;

  @VisibleForTesting
  public GatewayReceiverMBeanBridge(GatewayReceiver gatewayReceiver) {
    this.gatewayReceiver = gatewayReceiver;
    initializeReceiverStats();
  }

  @Override
  public int getClientConnectionCount() {
    // we can't rely on ServerBridge as the HostStatSampler might not have ran between the last
    // statistical update and the time at which this method is called.
    return !isRunning() ? 0 : getReceiverServer().getAcceptor().getClientServerConnectionCount();
  }

  @Override
  public void stopMonitor() {
    monitor.stopListener();
  }

  public void addGatewayReceiverStats(GatewayReceiverStats stats) {
    monitor.addStatisticsToMonitor(stats.getStats());
  }

  public String getBindAddress() {
    return gatewayReceiver.getBindAddress();
  }

  public int getPort() {
    return gatewayReceiver.getPort();
  }

  public int getSocketBufferSize() {
    return gatewayReceiver.getSocketBufferSize();
  }

  public boolean isRunning() {
    return gatewayReceiver.isRunning();
  }

  public void start() throws Exception {
    try {
      gatewayReceiver.start();
    } catch (Exception e) {
      throw new Exception(e.getMessage());
    }
  }

  public void stop() throws Exception {
    try {
      gatewayReceiver.stop();
    } catch (Exception e) {
      throw new Exception(e.getMessage());
    }
  }

  public int getEndPort() {
    return gatewayReceiver.getEndPort();
  }

  public String[] getGatewayTransportFilters() {
    List<GatewayTransportFilter> transportFilters = gatewayReceiver.getGatewayTransportFilters();
    String[] transportFiltersStringArray = null;
    if (transportFilters != null && !transportFilters.isEmpty()) {
      transportFiltersStringArray = new String[transportFilters.size()];
    } else {
      return transportFiltersStringArray;
    }
    int j = 0;
    for (GatewayTransportFilter filter : transportFilters) {
      transportFiltersStringArray[j] = filter.toString();
      j++;
    }
    return transportFiltersStringArray;
  }

  public int getStartPort() {
    return gatewayReceiver.getStartPort();
  }

  public int getMaximumTimeBetweenPings() {
    return gatewayReceiver.getMaximumTimeBetweenPings();
  }

  public float getCreateRequestsRate() {
    return createRequestRate.getRate();
  }

  public float getDestroyRequestsRate() {
    return destroyRequestRate.getRate();
  }

  public int getDuplicateBatchesReceived() {
    return getStatistic(StatsKey.DUPLICATE_BATCHES_RECEIVED).intValue();
  }

  public int getOutoforderBatchesReceived() {
    return getStatistic(StatsKey.OUT_OF_ORDER_BATCHES_RECEIVED).intValue();
  }

  public float getUpdateRequestsRate() {
    return updateRequestRate.getRate();
  }

  public float getEventsReceivedRate() {
    return eventsReceivedRate.getRate();
  }

  protected void startServer() {
    addServer(getReceiverServer());
  }

  protected void stopServer() {
    removeServer();
  }

  void destroyServer() {
    removeServer();
  }

  public String[] getConnectedGatewaySenders() {
    Acceptor acceptor = getReceiverServer().getAcceptor();
    Set<ServerConnection> serverConnections = acceptor.getAllServerConnections();
    if (serverConnections == null || serverConnections.isEmpty()) {
      return new String[0];
    }
    return serverConnections.stream().map(ServerConnection::getMembershipID).toArray(String[]::new);
  }

  long getAverageBatchProcessingTime() {
    if (getStatistic(StatsKey.TOTAL_BATCHES).longValue() != 0) {
      long processTimeInNano = getStatistic(StatsKey.BATCH_PROCESS_TIME).longValue()
          / getStatistic(StatsKey.TOTAL_BATCHES).longValue();

      return ManagementConstants.nanoSeconds.toMillis(processTimeInNano);
    }
    return 0;
  }

  private void initializeReceiverStats() {
    createRequestRate = new StatsRate(StatsKey.CREAT_REQUESTS, StatType.INT_TYPE, monitor);
    updateRequestRate = new StatsRate(StatsKey.UPDATE_REQUESTS, StatType.INT_TYPE, monitor);
    destroyRequestRate = new StatsRate(StatsKey.DESTROY_REQUESTS, StatType.INT_TYPE, monitor);
    eventsReceivedRate = new StatsRate(StatsKey.EVENTS_RECEIVED, StatType.INT_TYPE, monitor);
  }

  private InternalCacheServer getReceiverServer() {
    return (InternalCacheServer) gatewayReceiver.getServer();
  }
}
