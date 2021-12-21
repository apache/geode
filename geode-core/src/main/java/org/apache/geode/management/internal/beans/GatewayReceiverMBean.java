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

import org.apache.geode.management.GatewayReceiverMXBean;

public class GatewayReceiverMBean extends NotificationBroadcasterSupport
    implements GatewayReceiverMXBean {

  private final GatewayReceiverMBeanBridge bridge;

  public GatewayReceiverMBean(GatewayReceiverMBeanBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public String getBindAddress() {
    return bridge.getBindAddress();
  }

  @Override
  public float getCreateRequestsRate() {
    return bridge.getCreateRequestsRate();
  }

  @Override
  public float getDestroyRequestsRate() {
    return bridge.getDestroyRequestsRate();
  }

  @Override
  public int getDuplicateBatchesReceived() {
    return bridge.getDuplicateBatchesReceived();
  }

  @Override
  public int getOutoforderBatchesReceived() {
    return bridge.getOutoforderBatchesReceived();
  }

  @Override
  public float getUpdateRequestsRate() {
    return bridge.getUpdateRequestsRate();
  }

  @Override
  public float getEventsReceivedRate() {
    return bridge.getEventsReceivedRate();
  }

  @Override
  public int getMaximumTimeBetweenPings() {
    return bridge.getMaximumTimeBetweenPings();
  }

  @Override
  public int getPort() {
    return bridge.getPort();
  }

  @Override
  public int getSocketBufferSize() {
    return bridge.getSocketBufferSize();
  }

  @Override
  public boolean isRunning() {
    return bridge.isRunning();
  }

  @Override
  public void start() throws Exception {
    bridge.start();

  }

  @Override
  public void stop() throws Exception {
    bridge.stop();

  }

  @Override
  public int getEndPort() {
    return bridge.getEndPort();
  }

  @Override
  public int getStartPort() {
    return bridge.getStartPort();
  }

  @Override
  public String[] getGatewayTransportFilters() {
    return bridge.getGatewayTransportFilters();
  }

  @Override
  public int getClientConnectionCount() {
    return bridge.getClientConnectionCount();
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
  public int getNumGateways() {
    return bridge.getCurrentClients();
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
  public double getLoadPerConnection() {
    return bridge.getLoadPerConnection();
  }

  @Override
  public double getLoadPerQueue() {
    return bridge.getLoadPerQueue();
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
  public double getQueueLoad() {
    return bridge.getQueueLoad();
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
  public long getTotalReceivedBytes() {
    return bridge.getTotalReceivedBytes();
  }

  @Override
  public long getTotalSentBytes() {
    return bridge.getTotalSentBytes();
  }

  @Override
  public String[] getConnectedGatewaySenders() {
    return bridge.getConnectedGatewaySenders();
  }

  public GatewayReceiverMBeanBridge getBridge() {
    return bridge;
  }

  public void stopMonitor() {
    bridge.stopMonitor();
  }

  @Override
  public long getAverageBatchProcessingTime() {
    return bridge.getAverageBatchProcessingTime();
  }

}
