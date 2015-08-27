/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.management.internal.ManagementStrings;
import com.gemstone.gemfire.management.internal.beans.stats.MBeanStatsMonitor;
import com.gemstone.gemfire.management.internal.beans.stats.StatType;
import com.gemstone.gemfire.management.internal.beans.stats.StatsAverageLatency;
import com.gemstone.gemfire.management.internal.beans.stats.StatsKey;
import com.gemstone.gemfire.management.internal.beans.stats.StatsRate;

public class ServerBridge {
  
  protected MBeanStatsMonitor monitor;
  

  protected StatsRate getRequestRate;
  
  protected StatsRate putRequestRate;

  protected StatsAverageLatency getRequestAvgLatency;
  
  protected StatsAverageLatency putRequestAvgLatency;
  
  
  protected AcceptorImpl acceptor;

  
  public ServerBridge(CacheServer cacheServer){
    this.monitor = new MBeanStatsMonitor(ManagementStrings.SERVER_MONITOR
        .toLocalizedString());
    this.acceptor =  ((CacheServerImpl) cacheServer).getAcceptor();
    initializeStats();
    startMonitor();
  }
  
  public void addCacheServerStats(CacheServerStats cacheServerStats) {
    monitor.addStatisticsToMonitor(cacheServerStats.getStats());
  }
  
  protected void addServer(CacheServer cacheServer){
    this.acceptor =  ((CacheServerImpl) cacheServer).getAcceptor();
    startMonitor();
  }
  
  protected void removeServer(){
    this.acceptor =  null;
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

    getRequestAvgLatency = new StatsAverageLatency(StatsKey.GET_REQUESTS,
        StatType.INT_TYPE, StatsKey.PROCESS_GET_TIME, monitor);
    
    putRequestAvgLatency = new StatsAverageLatency(StatsKey.PUT_REQUESTS,
        StatType.INT_TYPE, StatsKey.PROCESS_PUT_TIME, monitor);

  }

  public ServerBridge() {
    this.monitor = new MBeanStatsMonitor(ManagementStrings.SERVER_MONITOR
        .toLocalizedString());

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
  


  protected Number getStatistic(String statName){
    if(monitor != null){
      return monitor.getStatistic(statName);
    }else{
      return 0;
    }
    
    
  }
 


  

}
