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
package org.apache.geode.management.bean.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.internal.cache.wan.GatewayReceiverStats;
import org.apache.geode.management.internal.beans.GatewayReceiverMBeanBridge;
import org.apache.geode.test.junit.categories.JMXTest;

@Category({JMXTest.class})
public class GatewayReceiverStatsJUnitTest extends MBeanStatsTestCase {

  private GatewayReceiverMBeanBridge bridge;

  private GatewayReceiverStats receiverStats;

  public void init() {
    receiverStats = GatewayReceiverStats.createGatewayReceiverStats("Test Sock Name");

    bridge = new GatewayReceiverMBeanBridge();
    bridge.addGatewayReceiverStats(receiverStats);
  }

  @Test
  public void testServerStats() throws InterruptedException {
    long startTime = System.currentTimeMillis();

    receiverStats.incCurrentClients();
    receiverStats.incConnectionThreads();
    receiverStats.incThreadQueueSize();
    receiverStats.incCurrentClientConnections();
    receiverStats.incFailedConnectionAttempts();
    receiverStats.incConnectionsTimedOut();
    receiverStats.incReadQueryRequestTime(startTime);
    receiverStats.incSentBytes(20);
    receiverStats.incReceivedBytes(20);
    receiverStats.incReadGetRequestTime(startTime);
    receiverStats.incProcessGetTime(startTime);
    receiverStats.incReadPutRequestTime(startTime);
    receiverStats.incProcessPutTime(startTime);

    ServerLoad load = new ServerLoad(1, 1, 1, 1);
    receiverStats.setLoad(load);

    sample();

    assertEquals(1, getCurrentClients());
    assertEquals(1, getConnectionThreads());
    assertEquals(1, getThreadQueueSize());
    assertEquals(1, getTotalFailedConnectionAttempts());
    assertEquals(1, getTotalConnectionsTimedOut());
    assertEquals(20, getTotalSentBytes());
    assertEquals(20, getTotalReceivedBytes());

    assertEquals(1.0, getConnectionLoad(), 0.01);
    assertEquals(1.0, getLoadPerConnection(), 0.01);
    assertEquals(1.0, getLoadPerQueue(), 0.01);
    assertEquals(1.0, getQueueLoad(), 0.01);

    assertTrue(getGetRequestRate() > 0);
    assertTrue(getGetRequestAvgLatency() > 0);
    assertTrue(getPutRequestRate() > 0);
    assertTrue(getPutRequestAvgLatency() > 0);

    bridge.stopMonitor();

    // TODO:FAIL: assertIndexDetailsEquals(0, getCurrentClients());
    // TODO:FAIL: assertIndexDetailsEquals(0, getConnectionThreads());
  }

  @Test
  public void testReceiverStats() throws InterruptedException {
    receiverStats.incEventsReceived(100);
    receiverStats.incUpdateRequest();
    receiverStats.incCreateRequest();
    receiverStats.incDestroyRequest();
    receiverStats.incDuplicateBatchesReceived();
    receiverStats.incOutoforderBatchesReceived();

    sample();

    assertTrue(getEventsReceivedRate() > 0);
    assertTrue(getCreateRequestsRate() > 0);
    assertTrue(getDestroyRequestsRate() > 0);
    assertTrue(getUpdateRequestsRate() > 0);

    assertEquals(1, getDuplicateBatchesReceived());
    assertEquals(1, getOutoforderBatchesReceived());
    bridge.stopMonitor();

    // TODO:FAIL: assertIndexDetailsEquals(0, getOutoforderBatchesReceived());
    // TODO:FAIL: assertIndexDetailsEquals(0, getDuplicateBatchesReceived());
  }

  private float getCreateRequestsRate() {
    return bridge.getCreateRequestsRate();
  }

  private float getDestroyRequestsRate() {
    return bridge.getDestroyRequestsRate();
  }

  private int getDuplicateBatchesReceived() {
    return bridge.getDuplicateBatchesReceived();
  }

  private int getOutoforderBatchesReceived() {
    return bridge.getOutoforderBatchesReceived();
  }

  private float getUpdateRequestsRate() {
    return bridge.getUpdateRequestsRate();
  }

  private float getEventsReceivedRate() {
    return bridge.getEventsReceivedRate();
  }

  private double getConnectionLoad() {
    return bridge.getConnectionLoad();
  }

  private int getConnectionThreads() {
    return bridge.getConnectionThreads();
  }

  private long getGetRequestAvgLatency() {
    return bridge.getGetRequestAvgLatency();
  }

  private float getGetRequestRate() {
    return bridge.getGetRequestRate();
  }

  private long getPutRequestAvgLatency() {
    return bridge.getPutRequestAvgLatency();
  }

  private float getPutRequestRate() {
    return bridge.getPutRequestRate();
  }

  private double getLoadPerConnection() {
    return bridge.getLoadPerConnection();
  }

  private double getLoadPerQueue() {
    return bridge.getLoadPerQueue();
  }

  private double getQueueLoad() {
    return bridge.getQueueLoad();
  }

  private int getThreadQueueSize() {
    return bridge.getThreadQueueSize();
  }

  private int getTotalConnectionsTimedOut() {
    return bridge.getTotalConnectionsTimedOut();
  }

  private int getTotalFailedConnectionAttempts() {
    return bridge.getTotalFailedConnectionAttempts();
  }

  private long getTotalSentBytes() {
    return bridge.getTotalSentBytes();
  }

  private long getTotalReceivedBytes() {
    return bridge.getTotalReceivedBytes();
  }

  private int getCurrentClients() {
    return bridge.getCurrentClients();
  }
}
