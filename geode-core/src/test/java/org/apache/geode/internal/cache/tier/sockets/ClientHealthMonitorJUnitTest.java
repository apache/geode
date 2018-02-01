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
package org.apache.geode.internal.cache.tier.sockets;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * This is a functional-test for <code>ClientHealthMonitor</code>.
 */
@Category(UnitTest.class)
public class ClientHealthMonitorJUnitTest {
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  private int pingIntervalMillis = 100;
  private int monitorIntervalMillis = 20;
  private ClientHealthMonitor clientHealthMonitor;

  @Before
  public void setUp() {
    System.setProperty(ClientHealthMonitor.CLIENT_HEALTH_MONITOR_INTERVAL_PROPERTY,
        String.valueOf(Integer.toString(monitorIntervalMillis)));

    InternalCache mockCache = mock(InternalCache.class);
    CacheClientNotifierStats mockStats = mock(CacheClientNotifierStats.class);
    clientHealthMonitor = ClientHealthMonitor.getInstance(mockCache, pingIntervalMillis, mockStats);
  }

  @Test
  public void idleServerConnectionTerminatedByHealthMonitor() throws Exception {
    ClientProxyMembershipID mockId = mock(ClientProxyMembershipID.class);
    ServerConnection mockConnection = mock(ServerConnection.class);

    clientHealthMonitor.addConnection(mockId, mockConnection);
    clientHealthMonitor.receivedPing(mockId);

    Awaitility.await()
        .atMost(2 * (monitorIntervalMillis + pingIntervalMillis), TimeUnit.MILLISECONDS)
        .until(() -> verify(mockConnection).handleTermination(true));
  }


  @Test
  public void activeServerConnectionNotTerminatedByHealthMonitor() throws Exception {
    ClientProxyMembershipID mockId = mock(ClientProxyMembershipID.class);
    ServerConnection mockConnection = mock(ServerConnection.class);

    clientHealthMonitor.addConnection(mockId, mockConnection);
    clientHealthMonitor.receivedPing(mockId);

    for (int i = 0; i < 10; ++i) {
      Thread.sleep(pingIntervalMillis / 2);
      verify(mockConnection, times(0)).handleTermination(true);
      clientHealthMonitor.receivedPing(mockId);
    }
  }
}
