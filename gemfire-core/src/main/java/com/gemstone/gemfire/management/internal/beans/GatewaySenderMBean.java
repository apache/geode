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

import com.gemstone.gemfire.management.GatewaySenderMXBean;

/**
 * 
 * @author rishim
 * 
 */
public class GatewaySenderMBean extends NotificationBroadcasterSupport
    implements GatewaySenderMXBean {

  private GatewaySenderMBeanBridge bridge;

  public GatewaySenderMBean(GatewaySenderMBeanBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public int getAlertThreshold() {
    return bridge.getAlertThreshold();
  }

  @Override
  public long getAverageDistributionTimePerBatch() {
    return bridge.getAverageDistributionTimePerBatch();
  }

  @Override
  public int getBatchSize() {
    return bridge.getBatchSize();
  }

  @Override
  public long getBatchTimeInterval() {
    return bridge.getBatchTimeInterval();
  }

  @Override
  public float getBatchesDispatchedRate() {
    return bridge.getBatchesDispatchedRate();
  }

  @Override
  public String getOverflowDiskStoreName() {
    return bridge.getOverflowDiskStoreName();
  }

  @Override
  public int getEventQueueSize() {
    return bridge.getEventQueueSize();
  }


  @Override
  public float getEventsQueuedRate() {
    return bridge.getEventsQueuedRate();
  }

  @Override
  public float getEventsReceivedRate() {
    return bridge.getEventsReceivedRate();
  }

  @Override
  public String[] getGatewayEventFilters() {
    return bridge.getGatewayEventFilters();
  }

  @Override
  public String[] getGatewayTransportFilters() {
    return bridge.getGatewayTransportFilters();
  }

  @Override
  public int getMaximumQueueMemory() {
    return bridge.getMaximumQueueMemory();
  }

  @Override
  public int getRemoteDSId() {
    return bridge.getRemoteDSId();
  }

  @Override
  public String getSenderId() {
    return bridge.getSenderId();
  }

  @Override
  public int getSocketBufferSize() {
    return bridge.getSocketBufferSize();
  }

  @Override
  public long getSocketReadTimeout() {
    return bridge.getSocketReadTimeout();
  }

  @Override
  public int getTotalBatchesRedistributed() {
    return bridge.getTotalBatchesRedistributed();
  }

  @Override
  public int getTotalEventsConflated() {
    return bridge.getTotalEventsConflated();
  }

  @Override
  public boolean isBatchConflationEnabled() {
    return bridge.isBatchConflationEnabled();
  }

  @Override
  public boolean isManualStart() {
    return bridge.isManualStart();
  }

  @Override
  public boolean isPaused() {
    return bridge.isPaused();
  }

  @Override
  public boolean isPersistenceEnabled() {
    return bridge.isPersistenceEnabled();
  }

  @Override
  public boolean isRunning() {
    return bridge.isRunning();
  }

  @Override
  public void pause() {
    bridge.pause();
  }

  @Override
  public void resume() {
    bridge.resume();
  }

  @Override
  public void start() {
    bridge.start();
  }

  @Override
  public void stop() {
    bridge.stop();

  }

  @Override
  public void rebalance() {
    bridge.rebalance();
  }
  
  @Override
  public boolean isPrimary() {
   return bridge.isPrimary();
  }

  @Override
  public int getDispatcherThreads() {
    return bridge.getDispatcherThreads();
  }

  @Override
  public String getOrderPolicy() {
    return bridge.getOrderPolicy();
  }

  @Override
  public boolean isDiskSynchronous() {
    return bridge.isDiskSynchronous();
  }

  @Override
  public boolean isParallel() {
    return bridge.isParallel();
  }

  public String getGatewayReceiver(){
    return bridge.getGatewayReceiver();
  }
  
  public GatewaySenderMBeanBridge getBridge() {
    return bridge;
  }
  
  public void stopMonitor(){
    bridge.stopMonitor();
  }

  @Override
  public boolean isConnected() {
    return bridge.isConnected();
  }

  @Override
  public int getEventsExceedingAlertThreshold() {
    return 0;
  }
}
