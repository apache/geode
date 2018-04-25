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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolProcessor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.UnitTest;


@Category({UnitTest.class, ClientServerTest.class})
public class OutputCapturingServerConnectionTest {

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Test
  public void testEOFDoesNotCauseWarningMessage() throws IOException, IncompatibleVersionException {
    Socket socketMock = mock(Socket.class);
    when(socketMock.getInetAddress()).thenReturn(InetAddress.getByName("localhost"));
    when(socketMock.isClosed()).thenReturn(true);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    when(socketMock.getOutputStream()).thenReturn(outputStream);

    AcceptorImpl acceptorStub = mock(AcceptorImpl.class);
    ClientProtocolProcessor clientProtocolProcessor = mock(ClientProtocolProcessor.class);
    doThrow(new IOException()).when(clientProtocolProcessor).processMessage(any(), any());

    ServerConnection serverConnection =
        getServerConnection(socketMock, clientProtocolProcessor, acceptorStub);

    String expectedMessage = "invoking doOneMessage";
    String unexpectedMessage = "IOException";

    // Create some stdout content so we can tell that the capture worked.
    System.out.println(expectedMessage);

    serverConnection.doOneMessage();

    // verify that an IOException wasn't logged
    String stdoutCapture = systemOutRule.getLog();
    assertTrue(stdoutCapture.contains(expectedMessage));
    assertFalse(stdoutCapture.contains(unexpectedMessage));
  }

  private ProtobufServerConnection getServerConnection(Socket socketMock,
      ClientProtocolProcessor clientProtocolProcessorMock, AcceptorImpl acceptorStub)
      throws IOException {
    ClientHealthMonitor clientHealthMonitorMock = mock(ClientHealthMonitor.class);
    when(acceptorStub.getClientHealthMonitor()).thenReturn(clientHealthMonitorMock);
    InetSocketAddress inetSocketAddressStub = InetSocketAddress.createUnresolved("localhost", 9071);
    InetAddress inetAddressStub = mock(InetAddress.class);
    when(socketMock.getInetAddress()).thenReturn(InetAddress.getByName("localhost"));
    when(socketMock.getRemoteSocketAddress()).thenReturn(inetSocketAddressStub);
    when(socketMock.getInetAddress()).thenReturn(inetAddressStub);

    InternalCache cache = mock(InternalCache.class);
    CachedRegionHelper cachedRegionHelper = mock(CachedRegionHelper.class);
    when(cachedRegionHelper.getCache()).thenReturn(cache);
    return new ProtobufServerConnection(socketMock, cache, cachedRegionHelper,
        mock(CacheServerStats.class), 0, 1024, "",
        CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), acceptorStub,
        clientProtocolProcessorMock, mock(SecurityService.class));
  }

}
