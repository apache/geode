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

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.geode.management.internal.beans.stats.StatType;
import org.apache.geode.management.internal.beans.stats.StatsAverageLatency;
import org.apache.geode.management.internal.beans.stats.StatsKey;
import org.apache.geode.management.internal.beans.stats.StatsRate;

public class ServerBridge {

  protected MBeanStatsMonitor monitor;

  protected StatsRate getRequestRate;

  protected StatsRate putRequestRate;

  protected StatsAverageLatency getRequestAvgLatency;

  protected StatsAverageLatency putRequestAvgLatency;

  protected AcceptorImpl acceptor;

  public ServerBridge(final CacheServer cacheServer) {
    this((CacheServerImpl) cacheServer,
        new MBeanStatsMonitor("ServerMXBeanMonitor"));
  }

  public ServerBridge(final CacheServerImpl cacheServer, final MBeanStatsMonitor monitor) {
    this(cacheServer.getAcceptor(), monitor);
  }

  public ServerBridge(final AcceptorImpl acceptor, final MBeanStatsMonitor monitor) {
    this.monitor = monitor;
    this.acceptor = acceptor;
    initializeStats();
    startMonitor();
  }

  public void addCacheServerStats(CacheServerStats cacheServerStats) {
    monitor.addStatisticsToMonitor(cacheServerStats.getStats());
  }

  protected void addServer(CacheServer cacheServer) {
    this.acceptor = ((CacheServerImpl) cacheServer).getAcceptor();
    startMonitor();
  }

  protected void removeServer() {
    this.acceptor = null;
    stopMonitor();
  }

  /**
   * While starting the cache server stats can be monitored.
   */
  private void startMonitor() {
    CacheServerStats stats = acceptor.getStats();
    addCacheServerStats(stats);

  }

  public void stopMonitor() {
    monitor.removeStatisticsFromMonitor(null);
    monitor.stopListener();
  }

  private void initializeStats() {
    getRequestRate = new StatsRate(StatsKey.GET_REQUESTS, StatType.INT_TYPE, monitor);

    putRequestRate = new StatsRate(StatsKey.PUT_REQUESTS, StatType.INT_TYPE, monitor);

    getRequestAvgLatency = new StatsAverageLatency(StatsKey.GET_REQUESTS, StatType.INT_TYPE,
        StatsKey.PROCESS_GET_TIME, monitor);

    putRequestAvgLatency = new StatsAverageLatency(StatsKey.PUT_REQUESTS, StatType.INT_TYPE,
        StatsKey.PROCESS_PUT_TIME, monitor);

  }

  public ServerBridge() {
    this.monitor = new MBeanStatsMonitor("ServerMXBeanMonitor");

    initializeStats();
  }

  public double getConnectionLoad() {
    return getStatistic(StatsKey.CONNECTION_LOAD).intValue();
  }

  public int getConnectionThreads() {
    return getStatistic(StatsKey.CONNECTION_THREADS).intValue();
  }

  public long getGetRequestAvgLatency() {
    return getRequestAvgLatency.getAverageLatency();
  }

  public float getGetRequestRate() {
    return getRequestRate.getRate();
  }

  public long getPutRequestAvgLatency() {
    return putRequestAvgLatency.getAverageLatency();
  }

  public float getPutRequestRate() {
    return putRequestRate.getRate();
  }


  public double getLoadPerConnection() {
    return getStatistic(StatsKey.LOAD_PER_CONNECTION).intValue();
  }

  public double getLoadPerQueue() {
    return getStatistic(StatsKey.LOAD_PER_QUEUE).intValue();
  }

  public double getQueueLoad() {
    return getStatistic(StatsKey.QUEUE_LOAD).doubleValue();
  }

  public int getThreadQueueSize() {
    return getStatistic(StatsKey.THREAD_QUEUE_SIZE).intValue();
  }

  public int getTotalConnectionsTimedOut() {
    return getStatistic(StatsKey.CONNECTIONS_TIMED_OUT).intValue();
  }

  public int getTotalFailedConnectionAttempts() {
    return getStatistic(StatsKey.FAILED_CONNECTION_ATTEMPT).intValue();
  }

  public long getTotalSentBytes() {
    return getStatistic(StatsKey.SERVER_SENT_BYTES).longValue();
  }

  public long getTotalReceivedBytes() {
    return getStatistic(StatsKey.SERVER_RECEIVED_BYTES).longValue();
  }

  public int getClientConnectionCount() {
    return getStatistic(StatsKey.CURRENT_CLIENT_CONNECTIONS).intValue();
  }

  public int getCurrentClients() {
    return getStatistic(StatsKey.CURRENT_CLIENTS).intValue();
  }



  protected Number getStatistic(String statName) {
    if (monitor != null) {
      return monitor.getStatistic(statName);
    } else {
      return 0;
    }


  }



}
