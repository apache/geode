/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.wan.GatewayReceiverStats;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.beans.stats.StatType;
import com.gemstone.gemfire.management.internal.beans.stats.StatsKey;
import com.gemstone.gemfire.management.internal.beans.stats.StatsRate;

/**
 * 
 * @author rishim
 *
 */
public class GatewayReceiverMBeanBridge extends ServerBridge{

  private GatewayReceiver rcv;  

  private StatsRate createRequestRate;
  
  private StatsRate updateRequestRate;
  
  private StatsRate destroyRequestRate;
  
  private StatsRate eventsReceivedRate;
    
  public GatewayReceiverMBeanBridge(GatewayReceiver rcv){
    super();
    this.rcv = rcv;
    initializeReceiverStats();
  }
  
  protected void startServer(){
    CacheServer server =  rcv.getServer();
    addServer(server);
  }
  
  protected void stopServer(){
    removeServer();
  }
  
  public GatewayReceiverMBeanBridge() {
    super();
    initializeReceiverStats();
  }

  public void addGatewayReceiverStats(GatewayReceiverStats stats) {
    monitor.addStatisticsToMonitor(stats.getStats());
  }
  
  
  public void stopMonitor(){
    monitor.stopListener();
  }
  
  public String getBindAddress() {
   return rcv.getBindAddress();
  }
 
  
  public int getPort() {
    return rcv.getPort();
  }

  
  public String getReceiverId() {
    return null;
  }

  
  public int getSocketBufferSize() {
    return rcv.getSocketBufferSize();
  }

  
  public boolean isRunning() {
    return rcv.isRunning();
  }

  
  public void start() throws Exception{
    try {
      rcv.start();
    } catch (Exception e) {
      throw new Exception(e.getMessage());
    }
  }

  
  public void stop() throws Exception{
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
    createRequestRate = new StatsRate(StatsKey.CREAT_REQUESTS,
        StatType.INT_TYPE, monitor);
    updateRequestRate = new StatsRate(StatsKey.UPDATE_REQUESTS,
        StatType.INT_TYPE, monitor);
    destroyRequestRate = new StatsRate(StatsKey.DESTROY_REQUESTS,
        StatType.INT_TYPE, monitor);
    eventsReceivedRate = new StatsRate(StatsKey.EVENTS_RECEIVED,
        StatType.INT_TYPE, monitor);
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


  public String[] getConnectedGatewaySenders() {
    Set<String> uniqueIds = null;
    AcceptorImpl acceptor = ((CacheServerImpl)rcv.getServer()).getAcceptor();
    Set<ServerConnection> serverConnections = acceptor.getAllServerConnections();
    if(serverConnections !=null && serverConnections.size() >0){
      uniqueIds = new HashSet<String>();
      for(ServerConnection conn : serverConnections){
        uniqueIds.add(conn.getMembershipID());
      }
      String[] allConnectedClientStr = new String[uniqueIds.size()];
      return uniqueIds.toArray(allConnectedClientStr);
    }
    return new String[0];
  }
  
  public long getAverageBatchProcessingTime() {
    if (getStatistic(StatsKey.TOTAL_BATCHES).longValue() != 0) {
      long processTimeInNano = getStatistic(StatsKey.BATCH_PROCESS_TIME)
          .longValue()
          / getStatistic(StatsKey.TOTAL_BATCHES).longValue();

      return ManagementConstants.nanoSeconds.toMillis(processTimeInNano);
    } else {
      return 0;
    }

  }

}
