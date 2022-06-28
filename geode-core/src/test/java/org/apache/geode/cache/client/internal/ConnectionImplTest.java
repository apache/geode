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
package org.apache.geode.cache.client.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.ServerLocationAndMemberId;
import org.apache.geode.distributed.internal.ServerLocationExtension;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.tcpserver.ClientSocketCreator;
import org.apache.geode.internal.cache.tier.ClientSideHandshake;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.net.SocketCreator;

public class ConnectionImplTest {

  private ConnectionImpl connection;
  private EndpointManagerImpl endpointManager;

  private Endpoint endPoint1;
  private ServerLocationAndMemberId serverLocationAndMemberId1;

  private Endpoint endPoint2;
  private ServerLocationAndMemberId serverLocationAndMemberId2;

  private Endpoint endPoint3;
  private ServerLocationAndMemberId serverLocationAndMemberId3;

  private ServerLocationAndMemberId serverLocationAndMemberId4;

  private Map<ServerLocationAndMemberId, Endpoint> endpointMap = new HashMap<>();

  @BeforeEach
  public void init() throws Exception {
    connection = new ConnectionImpl(mock(InternalDistributedSystem.class));
    endpointManager = mock(EndpointManagerImpl.class);
    when(endpointManager.getEndpointMap()).thenReturn(endpointMap);

    ServerLocation serverLocation1 = new ServerLocation("localhost", 1);
    InternalDistributedMember distributedMember1 = new InternalDistributedMember("localhost", 1);
    distributedMember1.setVmViewId(1);
    String uniqueId1 = distributedMember1.getUniqueId();

    serverLocationAndMemberId1 =
        new ServerLocationAndMemberId(serverLocation1, uniqueId1);
    endPoint1 = new Endpoint(endpointManager, mock(DistributedSystem.class), serverLocation1,
        mock(ConnectionStats.class), distributedMember1);

    endpointMap.put(serverLocationAndMemberId1, endPoint1);

    ServerLocation serverLocation2 = new ServerLocation("localhost", 2);
    InternalDistributedMember distributedMember2 = new InternalDistributedMember("localhost", 2);
    distributedMember2.setVmViewId(2);
    String uniqueId2 = distributedMember1.getUniqueId();

    serverLocationAndMemberId2 =
        new ServerLocationAndMemberId(serverLocation2, uniqueId2);
    endPoint2 = new Endpoint(endpointManager, mock(DistributedSystem.class), serverLocation2,
        mock(ConnectionStats.class), distributedMember2);

    endpointMap.put(serverLocationAndMemberId2, endPoint2);

    InternalDistributedMember distributedMember3 = new InternalDistributedMember("localhost", 1);
    distributedMember3.setVmViewId(3);
    String uniqueId3 = distributedMember3.getUniqueId();

    serverLocationAndMemberId3 =
        new ServerLocationAndMemberId(serverLocation1, uniqueId3);
    endPoint3 = new Endpoint(endpointManager, mock(DistributedSystem.class), serverLocation1,
        mock(ConnectionStats.class), distributedMember3);


    InternalDistributedMember distributedMember4 = new InternalDistributedMember("localhost", 1);
    distributedMember4.setVmViewId(4);
    String uniqueId4 = distributedMember4.getUniqueId();

    serverLocationAndMemberId4 =
        new ServerLocationAndMemberId(serverLocation1, uniqueId4);

    endpointMap.put(serverLocationAndMemberId3, endPoint3);

  }

  @Test
  public void testGetEndpoint1() throws Exception {
    ServerLocationExtension serverLocationExtension1 =
        new ServerLocationExtension(serverLocationAndMemberId1);

    assertThat(connection.getEndpoint(endpointManager, serverLocationExtension1))
        .isEqualTo(endPoint1);
  }

  @Test
  public void testGetTwoEndpointsForDifferentServerLocations() throws Exception {
    ServerLocationExtension serverLocationExtension1 =
        new ServerLocationExtension(serverLocationAndMemberId1);

    ServerLocationExtension serverLocationExtension2 =
        new ServerLocationExtension(serverLocationAndMemberId2);

    assertThat(serverLocationExtension1).isNotEqualTo(serverLocationExtension2);

    assertThat(connection.getEndpoint(endpointManager, serverLocationExtension1))
        .isEqualTo(endPoint1);
    assertThat(connection.getEndpoint(endpointManager, serverLocationExtension2))
        .isEqualTo(endPoint2);

    assertThat(endPoint1).isNotEqualTo(endPoint2);
  }


  @Test
  public void testGetTwoEndpointsForEqualServerLocations() throws Exception {
    ServerLocationExtension serverLocationExtension1 =
        new ServerLocationExtension(serverLocationAndMemberId1);

    ServerLocationExtension serverLocationExtension3 =
        new ServerLocationExtension(serverLocationAndMemberId3);

    assertThat(serverLocationExtension1).isEqualTo(serverLocationExtension3);

    assertThat(connection.getEndpoint(endpointManager, serverLocationExtension1))
        .isEqualTo(endPoint1);
    assertThat(connection.getEndpoint(endpointManager, serverLocationExtension3))
        .isEqualTo(endPoint3);

    assertThat(endPoint1).isNotEqualTo(endPoint3);
  }

  @Test
  public void testGetEndpointsForEqualServerLocationsBtOnlyOneExist() throws Exception {
    ServerLocationExtension serverLocationExtension3 =
        new ServerLocationExtension(serverLocationAndMemberId3);

    ServerLocationExtension serverLocationExtension4 =
        new ServerLocationExtension(serverLocationAndMemberId4);

    assertThat(serverLocationExtension3).isEqualTo(serverLocationExtension4);

    assertThat(connection.getEndpoint(endpointManager, serverLocationExtension3))
        .isEqualTo(endPoint3);
    assertThat(connection.getEndpoint(endpointManager, serverLocationExtension4))
        .isNull();

  }


  @Test
  public void testConnectConnectionForPingTask() throws Exception {

    ServerLocationExtension serverLocationExtension1 =
        new ServerLocationExtension(serverLocationAndMemberId1);
    ClientSideHandshake handshake = mock(ClientSideHandshake.class);
    SocketCreator socketCreator = mock(SocketCreator.class);
    SocketFactory socketFactory = mock(SocketFactory.class);

    ClientSocketCreator clientSocketCreator = mock(ClientSocketCreator.class);
    when(socketCreator.forClient()).thenReturn(clientSocketCreator);

    Socket socket = mock(Socket.class);
    when(clientSocketCreator.connect(any(), anyInt(), anyInt(), any())).thenReturn(socket);

    connection.connect(endpointManager, serverLocationExtension1, handshake, 4096, 0, 0,
        CommunicationMode.ClientToServer, null, socketCreator, socketFactory);

    verify(endpointManager, times(0)).referenceEndpoint(any(), any());

  }


  @Test
  public void testConnectConnectionForNotPingTask() throws Exception {
    ServerLocation serverLocation1 = new ServerLocation("localhost", 1);

    ClientSideHandshake handshake = mock(ClientSideHandshake.class);
    SocketCreator socketCreator = mock(SocketCreator.class);
    SocketFactory socketFactory = mock(SocketFactory.class);

    ServerQueueStatus status = mock(ServerQueueStatus.class);
    when(handshake.handshakeWithServer(any(), any(), any())).thenReturn(status);

    ClientSocketCreator clientSocketCreator = mock(ClientSocketCreator.class);
    when(socketCreator.forClient()).thenReturn(clientSocketCreator);

    Socket socket = mock(Socket.class);
    when(clientSocketCreator.connect(any(), anyInt(), anyInt(), any())).thenReturn(socket);
    when(endpointManager.referenceEndpoint(any(), any())).thenReturn(endPoint1);

    connection.connect(endpointManager, serverLocation1, handshake, 4096, 0, 0,
        CommunicationMode.ClientToServer, null, socketCreator, socketFactory);

    verify(endpointManager, times(1)).referenceEndpoint(any(), any());

  }

  @Test
  public void testConnectConnectionForPingTaskWhileEndpointIsClosed() throws Exception {
    ServerLocationExtension serverLocationExtension1 =
        new ServerLocationExtension(serverLocationAndMemberId1);

    ClientSideHandshake handshake = mock(ClientSideHandshake.class);
    SocketCreator socketCreator = mock(SocketCreator.class);
    SocketFactory socketFactory = mock(SocketFactory.class);

    ServerQueueStatus status = mock(ServerQueueStatus.class);
    when(handshake.handshakeWithServer(any(), any(), any())).thenReturn(status);

    ClientSocketCreator clientSocketCreator = mock(ClientSocketCreator.class);
    when(socketCreator.forClient()).thenReturn(clientSocketCreator);

    Socket socket = mock(Socket.class);
    when(clientSocketCreator.connect(any(), anyInt(), anyInt(), any())).thenReturn(socket);
    endPoint1.close();

    when(endpointManager.referenceEndpoint(any(), any())).thenReturn(endPoint2);

    connection.connect(endpointManager, serverLocationExtension1, handshake, 4096, 0, 0,
        CommunicationMode.ClientToServer, null, socketCreator, socketFactory);

    verify(endpointManager, times(1)).referenceEndpoint(any(), any());

  }
}
