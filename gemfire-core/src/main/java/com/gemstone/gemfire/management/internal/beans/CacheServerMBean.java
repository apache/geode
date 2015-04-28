/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import javax.management.NotificationBroadcasterSupport;

import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.management.ClientHealthStatus;
import com.gemstone.gemfire.management.ClientQueueDetail;
import com.gemstone.gemfire.management.ServerLoadData;

/**
 * Represents the GemFire CacheServer . Provides data and notifications about
 * server, subscriptions,durable queues and indices
 * 
 * @author rishim
 * 
 */
public class CacheServerMBean extends NotificationBroadcasterSupport implements CacheServerMXBean {

  private CacheServerBridge bridge;

  public CacheServerMBean(CacheServerBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  @Deprecated
  public void closeAllContinuousQuery(String regionName) throws Exception {
    bridge.closeContinuousQuery(regionName);
  }

  @Override
  @Deprecated
  public void executeContinuousQuery(String queryId) throws Exception {
    bridge.executeContinuousQuery(queryId);
  }

  @Override
  public int getCapacity() {
    return bridge.getCapacity();
  }

  @Override
  public int getClientConnectionCount() {
    return bridge.getClientConnectionCount();
  }

  @Override
  public String[] getClientIds() throws Exception{
    return bridge.listClientIds();
  }

  @Override
  public ClientHealthStatus showClientStats(String clientId) throws Exception{
    return bridge.showClientStats(clientId);
  }

  @Override
  public double getConnectionLoad() {
    return bridge.getConnectionLoad();
  }

  @Override
  public int getConnectionThreads() {
    return bridge.getConnectionThreads();
  }

  @Override
  public String[] getContinuousQueryList() {
    return bridge.getContinuousQueryList();
  }

  @Override
  public String getDiskStoreName() {
    return bridge.getDiskStoreName();
  }


  @Override
  public String getEvictionPolicy() {
    return bridge.getEvictionPolicy();
  }

  @Override
  public long getGetRequestAvgLatency() {
    return bridge.getGetRequestAvgLatency();
  }

  @Override
  public float getGetRequestRate() {
    return bridge.getGetRequestRate();
  }

  @Override
  public String getHostNameForClients() {
    return bridge.getHostnameForClients();
  }

  @Override
  public int getIndexCount() {
    return bridge.getIndexCount();
  }

  @Override
  public String[] getIndexList() {
    return bridge.getIndexList();
  }

  @Override
  public double getLoadPerConnection() {
    return bridge.getLoadPerConnection();
  }

  @Override
  public double getLoadPerQueue() {
    return bridge.getLoadPerQueue();
  }

  @Override
  public long getLoadPollInterval() {
    return bridge.getLoadPollInterval();
  }

  @Override
  public ServerLoadData fetchLoadProbe() {
    return bridge.fetchLoadProbe();
  }

  @Override
  public int getMaxConnections() {
    return bridge.getMaxConnections();
  }

  @Override
  public int getMaxThreads() {
    return bridge.getMaxThreads();
  }

  @Override
  public int getMaximumMessageCount() {
    return bridge.getMaximumMessageCount();
  }

  @Override
  public int getMaximumTimeBetweenPings() {

    return bridge.getMaximumTimeBetweenPings();
  }

  @Override
  public int getMessageTimeToLive() {

    return bridge.getMessageTimeToLive();
  }

  
  @Override
  public int getPort() {
    return bridge.getPort();
  }

  @Override
  public long getPutRequestAvgLatency() {
    return bridge.getPutRequestAvgLatency();
  }

  @Override
  public float getPutRequestRate() {
    return bridge.getPutRequestRate();
  }

  @Override
  public float getQueryRequestRate() {
    return bridge.getQueryRequestRate();
  }

  @Override
  public double getQueueLoad() {
    return bridge.getQueueLoad();
  }

  @Override
  public int getSocketBufferSize() {
    return bridge.getSocketBufferSize();
  }

  @Override
  public int getThreadQueueSize() {
    return bridge.getThreadQueueSize();
  }

  @Override
  public int getTotalConnectionsTimedOut() {
    return bridge.getTotalConnectionsTimedOut();
  }


  @Override
  public int getTotalFailedConnectionAttempts() {
    return bridge.getTotalFailedConnectionAttempts();
  }

  @Override
  public long getTotalIndexMaintenanceTime() {
    return bridge.getTotalIndexMaintenanceTime();
  }

  @Override
  public boolean isRunning() {
    return bridge.isRunning();
  }

  @Override
  public void removeIndex(String indexName) throws Exception{
    bridge.removeIndex(indexName);
  }

  @Override
  @Deprecated
  public void stopContinuousQuery(String queryName) throws Exception{
    bridge.stopContinuousQuery(queryName);
  }

  @Override
  public String getBindAddress() {
    return bridge.getBindAddress();
  }

  @Override
  @Deprecated
  public void closeContinuousQuery(String queryName) throws Exception {
    bridge.closeContinuousQuery(queryName);
  }

  @Override
  public int getCurrentClients() {
    return bridge.getCurrentClients();
  }

  @Override
  public long getTotalReceivedBytes() {
    return bridge.getTotalReceivedBytes();
  }

  @Override
  public long getTotalSentBytes() {
    return bridge.getTotalSentBytes();
  }

  @Override
  public long getClientNotificationAvgLatency() {
    return bridge.getClientNotificationAvgLatency();
  }

  @Override
  public float getClientNotificationRate() {
    return bridge.getClientNotificationRate();
  }

  @Override
  public int getNumClientNotificationRequests() {
   return bridge.getNumClientNotificationRequests();
  }
  
  public CacheServerBridge getBridge() {
    return bridge;
  }
  
  public void stopMonitor(){
    bridge.stopMonitor();
  }

  @Override
  public long getActiveCQCount() {
    return bridge.getActiveCQCount();
  }

  @Override
  public long getRegisteredCQCount() {
    return bridge.getRegisteredCQCount();
  }

  @Override
  public int getNumSubscriptions() {
    return bridge.getNumSubscriptions();
  }

  @Override
  public ClientHealthStatus[] showAllClientStats() throws Exception {
    return bridge.showAllClientStats();
  }

  @Override
  public ClientQueueDetail[] showClientQueueDetails() throws Exception{
    return bridge.getClientQueueDetails();
  }
  
  @Override
  public ClientQueueDetail showClientQueueDetails(String clientId) throws Exception{
    return bridge.getClientQueueDetail(clientId);
  }
}
