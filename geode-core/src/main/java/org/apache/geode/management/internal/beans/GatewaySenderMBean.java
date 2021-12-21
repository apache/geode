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

import javax.management.NotificationBroadcasterSupport;

import org.apache.geode.management.GatewaySenderMXBean;

public class GatewaySenderMBean extends NotificationBroadcasterSupport
    implements GatewaySenderMXBean {

  private final GatewaySenderMBeanBridge bridge;

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
  public float getLRUEvictionsRate() {
    return bridge.getLRUEvictionsRate();
  }

  @Override
  public long getEntriesOverflowedToDisk() {
    return bridge.getEntriesOverflowedToDisk();
  }

  @Override
  public long getBytesOverflowedToDisk() {
    return bridge.getBytesOverflowedToDisk();
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
  public int getTotalBatchesDistributed() {
    return bridge.getTotalBatchesDistributed();
  }

  @Override
  public int getTotalBatchesRedistributed() {
    return bridge.getTotalBatchesRedistributed();
  }

  @Override
  public int getTotalBatchesWithIncompleteTransactions() {
    return bridge.getTotalBatchesWithIncompleteTransactions();
  }

  @Override
  public int getTotalEventsConflated() {
    return bridge.getTotalEventsConflated();
  }

  @Override
  public long getTotalQueueSizeBytesInUse() {
    return bridge.getTotalQueueSizeBytesInUse();
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
  public void startWithCleanQueue() {
    bridge.startWithCleanQueue();
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

  @Override
  public boolean mustGroupTransactionEvents() {
    return bridge.mustGroupTransactionEvents();
  }

  @Override
  public String getGatewayReceiver() {
    return bridge.getGatewayReceiver();
  }

  public GatewaySenderMBeanBridge getBridge() {
    return bridge;
  }

  public void stopMonitor() {
    bridge.stopMonitor();
  }

  @Override
  public boolean isConnected() {
    return bridge.isConnected();
  }

  @Override
  public int getEventsExceedingAlertThreshold() {
    return bridge.getEventsExceedingAlertThreshold();
  }
}
