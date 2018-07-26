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
package org.apache.geode.cache.server.internal;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.cache.server.ServerLoadProbe;
import org.apache.geode.internal.cache.tier.CommunicationMode;

public class LoadMonitorTest {

  @Test
  public void protobufConnectionIsIncludedInLoadMetrics() throws Exception {
    ServerLoadProbe probe = mock(ServerLoadProbe.class);
    when(probe.getLoad(any())).thenReturn(null);
    LoadMonitor loadMonitor = new LoadMonitor(probe, 10000, 0, 0, null);
    loadMonitor.connectionOpened(true, CommunicationMode.ProtobufClientServerProtocol);
    assertEquals(1, loadMonitor.metrics.getClientCount());
    assertEquals(1, loadMonitor.metrics.getConnectionCount());
  }

  @Test
  public void protobufConnectionIsRemovedFromLoadMetrics() throws Exception {
    ServerLoadProbe probe = mock(ServerLoadProbe.class);
    when(probe.getLoad(any())).thenReturn(null);
    LoadMonitor loadMonitor = new LoadMonitor(probe, 10000, 0, 0, null);
    loadMonitor.connectionOpened(true, CommunicationMode.ProtobufClientServerProtocol);
    loadMonitor.connectionClosed(true, CommunicationMode.ProtobufClientServerProtocol);
    assertEquals(0, loadMonitor.metrics.getClientCount());
    assertEquals(0, loadMonitor.metrics.getConnectionCount());
  }
}
