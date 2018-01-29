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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;

import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.client.protocol.exception.ServiceLoadingFailureException;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * We don't test the path where the service providing protobufProtocolHandler is actually present,
 * because it lives outside this module, and all the integration tests from that module will test
 * the newclient protocol happy path.
 * <p>
 * What we are concerned with is making sure that everything stays the same when the feature flag
 * isn't set, and that we at least try to load the service when the feature flag is true.
 */
@Category(UnitTest.class)
public class ServerConnectionFactoryTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  /**
   * Safeguard that we won't create the new client protocol object unless the feature flag is
   * enabled.
   */
  @Test(expected = IOException.class)
  public void newClientProtocolFailsWithoutSystemPropertySet() throws IOException {
    ServerConnection serverConnection = serverConnectionMockedExceptForCommunicationMode(
        CommunicationMode.ProtobufClientServerProtocol.getModeNumber());

  }

  /**
   * @throws IOException caused by ServiceLoadingFailureException because the service is implemented
   *         in a different module, and when this unit test is run, that module won't be present.
   */
  @Test
  public void newClientProtocolFailsWithSystemPropertySet() throws IOException {
    Assertions.assertThatThrownBy(() -> {
      System.setProperty("geode.feature-protobuf-protocol", "true");
      ServerConnection serverConnection = serverConnectionMockedExceptForCommunicationMode(
          CommunicationMode.ProtobufClientServerProtocol.getModeNumber());
    }).hasRootCauseInstanceOf(ServiceLoadingFailureException.class);
  }

  @Test
  public void makeServerConnection() throws Exception {
    CommunicationMode[] communicationModes = new CommunicationMode[] {
        CommunicationMode.ClientToServer, CommunicationMode.PrimaryServerToClient,
        CommunicationMode.SecondaryServerToClient, CommunicationMode.GatewayToGateway,
        CommunicationMode.MonitorToServer, CommunicationMode.SuccessfulServerToClient,
        CommunicationMode.UnsuccessfulServerToClient, CommunicationMode.ClientToServer,};

    for (CommunicationMode communicationMode : communicationModes) {
      ServerConnection serverConnection =
          serverConnectionMockedExceptForCommunicationMode(communicationMode.getModeNumber());
      assertTrue(serverConnection instanceof OriginalServerConnection);
    }
  }

  @Test
  public void makeServerConnectionForOldProtocolWithFeatureFlagEnabled() throws IOException {
    System.setProperty("geode.feature-protobuf-protocol", "true");
    CommunicationMode[] communicationModes = new CommunicationMode[] {
        CommunicationMode.ClientToServer, CommunicationMode.PrimaryServerToClient,
        CommunicationMode.SecondaryServerToClient, CommunicationMode.GatewayToGateway,
        CommunicationMode.MonitorToServer, CommunicationMode.SuccessfulServerToClient,
        CommunicationMode.UnsuccessfulServerToClient, CommunicationMode.ClientToServer,};

    for (CommunicationMode communicationMode : communicationModes) {
      ServerConnection serverConnection =
          serverConnectionMockedExceptForCommunicationMode(communicationMode.getModeNumber());
      assertTrue(serverConnection instanceof OriginalServerConnection);
    }
  }

  private ServerConnection serverConnectionMockedExceptForCommunicationMode(byte communicationMode)
      throws IOException {
    Socket socketMock = mock(Socket.class);
    when(socketMock.getInetAddress()).thenReturn(InetAddress.getByName("localhost"));
    InputStream streamMock = mock(InputStream.class);
    when(streamMock.read()).thenReturn(1);
    when(socketMock.getInputStream()).thenReturn(streamMock);

    return new ServerConnectionFactory().makeServerConnection(socketMock, mock(InternalCache.class),
        mock(CachedRegionHelper.class), mock(CacheServerStats.class), 0, 0, "", communicationMode,
        mock(AcceptorImpl.class), mock(SecurityService.class));
  }

}
