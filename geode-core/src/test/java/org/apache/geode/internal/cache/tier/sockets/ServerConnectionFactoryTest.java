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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.client.protocol.exception.ServiceLoadingFailureException;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * We don't test the path where the service providing protobufProtocolHandler is actually present,
 * because it lives outside this module, and all the integration tests from that module will test
 * the newclient protocol happy path.
 * <p>
 * What we are concerned with is making sure that everything stays the same when the feature flag
 * isn't set, and that we at least try to load the service when the feature flag is true.
 */
@Category(ClientServerTest.class)
@RunWith(JUnitParamsRunner.class)
public class ServerConnectionFactoryTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  /**
   * Safeguard that we won't create the new client protocol object unless the feature flag is
   * enabled.
   */
  @Test
  public void newClientProtocolFailsWithoutSystemPropertySet() {
    Throwable thrown = catchThrowable(
        () -> new ServerConnectionFactory(new ServiceLoaderModuleService(LogService.getLogger()))
            .makeServerConnection(
                mock(Socket.class),
                mock(InternalCache.class), mock(CachedRegionHelper.class),
                mock(CacheServerStats.class),
                0, 0, "", CommunicationMode.ProtobufClientServerProtocol.getModeNumber(),
                mock(AcceptorImpl.class), mock(SecurityService.class)));

    assertThat(thrown).isInstanceOf(IOException.class);
  }

  @Test
  public void newClientProtocolFailsWithSystemPropertySet() {
    System.setProperty("geode.feature-protobuf-protocol", "true");
    Throwable thrown = catchThrowable(
        () -> new ServerConnectionFactory(new ServiceLoaderModuleService(LogService.getLogger()))
            .makeServerConnection(
                mock(Socket.class),
                mock(InternalCache.class), mock(CachedRegionHelper.class),
                mock(CacheServerStats.class),
                0, 0, "", CommunicationMode.ProtobufClientServerProtocol.getModeNumber(),
                mock(AcceptorImpl.class), mock(SecurityService.class)));

    assertThat(thrown).hasRootCauseInstanceOf(ServiceLoadingFailureException.class);
  }

  @Test
  @Parameters({"ClientToServer", "PrimaryServerToClient", "SecondaryServerToClient",
      "GatewayToGateway", "MonitorToServer", "SuccessfulServerToClient",
      "UnsuccessfulServerToClient", "ClientToServer"})
  @TestCaseName("{method}({params})")
  public void makeServerConnection(CommunicationMode communicationMode) throws Exception {
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getByName("localhost"));
    when(socket.getInputStream()).thenReturn(mock(InputStream.class));

    ServerConnection serverConnection =
        new ServerConnectionFactory(new ServiceLoaderModuleService(LogService.getLogger()))
            .makeServerConnection(socket,
                mock(InternalCache.class),
                mock(CachedRegionHelper.class), mock(CacheServerStats.class), 0, 0, "",
                communicationMode.getModeNumber(),
                mock(AcceptorImpl.class), mock(SecurityService.class));

    assertThat(serverConnection).isInstanceOf(OriginalServerConnection.class);
  }

  @Test
  @Parameters({"ClientToServer", "PrimaryServerToClient", "SecondaryServerToClient",
      "GatewayToGateway", "MonitorToServer", "SuccessfulServerToClient",
      "UnsuccessfulServerToClient", "ClientToServer"})
  @TestCaseName("{method}({params})")
  public void makeServerConnectionForOldProtocolWithFeatureFlagEnabled(
      CommunicationMode communicationMode) throws IOException {
    System.setProperty("geode.feature-protobuf-protocol", "true");
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getByName("localhost"));
    when(socket.getInputStream()).thenReturn(mock(InputStream.class));

    ServerConnection serverConnection =
        new ServerConnectionFactory(new ServiceLoaderModuleService(LogService.getLogger()))
            .makeServerConnection(socket,
                mock(InternalCache.class),
                mock(CachedRegionHelper.class), mock(CacheServerStats.class), 0, 0, "",
                communicationMode.getModeNumber(),
                mock(AcceptorImpl.class), mock(SecurityService.class));

    assertThat(serverConnection).isInstanceOf(OriginalServerConnection.class);
  }

}
