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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.test.junit.categories.JMXTest;

@Category(JMXTest.class)
public class GatewayReceiverMBeanBridgeTest {
  private GatewayReceiver gatewayReceiver;
  private Acceptor acceptor;
  private GatewayReceiverMBeanBridge bridge;

  @Before
  public void before() throws Exception {
    gatewayReceiver = mock(GatewayReceiver.class);
    InternalCacheServer server = mock(InternalCacheServer.class);
    when(gatewayReceiver.getServer()).thenReturn(server);
    acceptor = mock(Acceptor.class);
    when(server.getAcceptor()).thenReturn(acceptor);
    bridge = new GatewayReceiverMBeanBridge(gatewayReceiver);
  }

  @Test
  public void getStartPortDelegatesToGatewayReceiver() {
    int startPort = 42;
    when(gatewayReceiver.getStartPort()).thenReturn(startPort);
    int value = bridge.getStartPort();

    assertThat(value).isEqualTo(startPort);
  }

  @Test
  public void getEndPortDelegatesToGatewayReceiver() {
    int endPort = 84;
    when(gatewayReceiver.getEndPort()).thenReturn(endPort);
    int value = bridge.getEndPort();

    assertThat(value).isEqualTo(endPort);
  }

  @Test
  public void getConnectedGatewaySenders() {
    when(acceptor.getAllServerConnections()).thenReturn(null);
    assertThat(bridge.getConnectedGatewaySenders()).isNotNull().hasSize(0);

    when(acceptor.getAllServerConnections()).thenReturn(Collections.emptySet());
    assertThat(bridge.getConnectedGatewaySenders()).isNotNull().hasSize(0);

    ServerConnection connection = mock(ServerConnection.class);
    when(connection.getMembershipID()).thenReturn("testId");
    when(acceptor.getAllServerConnections()).thenReturn(Collections.singleton(connection));
    assertThat(bridge.getConnectedGatewaySenders()).containsExactly("testId");

  }
}
