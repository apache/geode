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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.wan.GatewayReceiverStats;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.beans.stats.StatType;
import org.apache.geode.management.internal.beans.stats.StatsKey;
import org.apache.geode.management.internal.beans.stats.StatsRate;

public class GatewayReceiverMBeanBridge extends ServerBridge {

  private GatewayReceiver rcv;

  private StatsRate createRequestRate;

  private StatsRate updateRequestRate;

  private StatsRate destroyRequestRate;

  private StatsRate eventsReceivedRate;

  public GatewayReceiverMBeanBridge(GatewayReceiver rcv) {
    super();
    this.rcv = rcv;
    initializeReceiverStats();
  }

  void destroyServer() {
    removeServer();
  }

  protected void startServer() {
    CacheServer server = rcv.getServer();
    addServer(server);
  }

  protected void stopServer() {
    removeServer();
  }

  public GatewayReceiverMBeanBridge() {
    super();
    initializeReceiverStats();
  }

  public void addGatewayReceiverStats(GatewayReceiverStats stats) {
    monitor.addStatisticsToMonitor(stats.getStats());
  }


  public void stopMonitor() {
    monitor.stopListener();
  }

  public String getBindAddress() {
    return rcv.getBindAddress();
  }


  public int getPort() {
    return rcv.getPort();
  }

  public int getSocketBufferSize() {
    return rcv.getSocketBufferSize();
  }


  public boolean isRunning() {
    return rcv.isRunning();
  }


  public void start() throws Exception {
    try {
      rcv.start();
    } catch (Exception e) {
      throw new Exception(e.getMessage());
    }
  }


  public void stop() throws Exception {
    try {
      rcv.stop();
    } catch (Exception e) {
      throw new Exception(e.getMessage());
    }
  }


  public int getEndPort() {
    return rcv.getEndPort();
  }


  public String[] getGatewayTransportFilters() {
    List<GatewayTransportFilter> transPortfilters = rcv.getGatewayTransportFilters();
    String[] filtersStr = null;
    if (transPortfilters != null && transPortfilters.size() > 0) {
      filtersStr = new String[transPortfilters.size()];
    } else {
      return filtersStr;
    }
    int j = 0;
    for (GatewayTransportFilter filter : transPortfilters) {
      filtersStr[j] = filter.toString();
      j++;
    }
    return filtersStr;

  }


  public int getStartPort() {
    return rcv.getEndPort();
  }


  public int getMaximumTimeBetweenPings() {
    return rcv.getMaximumTimeBetweenPings();
  }


  /** Statistics Related Counters **/


  private void initializeReceiverStats() {
    createRequestRate = new StatsRate(StatsKey.CREAT_REQUESTS, StatType.INT_TYPE, monitor);
    updateRequestRate = new StatsRate(StatsKey.UPDATE_REQUESTS, StatType.INT_TYPE, monitor);
    destroyRequestRate = new StatsRate(StatsKey.DESTROY_REQUESTS, StatType.INT_TYPE, monitor);
    eventsReceivedRate = new StatsRate(StatsKey.EVENTS_RECEIVED, StatType.INT_TYPE, monitor);
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

  @Override
  public int getClientConnectionCount() {
    // See GEODE-5248: we can't rely on ServerBridge as the HostStatSampler might not have ran
    // between the last statistical update and the time at which this method is called.
    return (!isRunning()) ? 0
        : ((CacheServerImpl) rcv.getServer()).getAcceptor().getClientServerCnxCount();
  }

  String[] getConnectedGatewaySenders() {
    Set<String> uniqueIds;
    AcceptorImpl acceptor = ((CacheServerImpl) rcv.getServer()).getAcceptor();
    Set<ServerConnection> serverConnections = acceptor.getAllServerConnections();
    if (serverConnections != null && serverConnections.size() > 0) {
      uniqueIds = new HashSet<>();
      for (ServerConnection conn : serverConnections) {
        uniqueIds.add(conn.getMembershipID());
      }
      String[] allConnectedClientStr = new String[uniqueIds.size()];
      return uniqueIds.toArray(allConnectedClientStr);
    }
    return new String[0];
  }

  long getAverageBatchProcessingTime() {
    if (getStatistic(StatsKey.TOTAL_BATCHES).longValue() != 0) {
      long processTimeInNano = getStatistic(StatsKey.BATCH_PROCESS_TIME).longValue()
          / getStatistic(StatsKey.TOTAL_BATCHES).longValue();

      return ManagementConstants.nanoSeconds.toMillis(processTimeInNano);
    } else {
      return 0;
    }

  }

}
