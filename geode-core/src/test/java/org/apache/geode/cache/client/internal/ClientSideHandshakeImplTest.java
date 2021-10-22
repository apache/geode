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


import static org.apache.geode.distributed.ConfigurationProperties.CONFLATE_EVENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.internal.cache.tier.CommunicationMode.ClientToServer;
import static org.apache.geode.internal.cache.tier.CommunicationMode.GatewayToGateway;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.DataSerializer;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.BufferDataOutputStream;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.tcp.ByteBufferInputStream;

public class ClientSideHandshakeImplTest {
  private ClientProxyMembershipID proxyId;
  private InternalDistributedSystem system;
  private SecurityService securityService;

  private Connection connection;
  private Properties properties;
  private ServerLocation member;
  private Properties securityproperties;
  private Socket socket;
  private BufferDataOutputStream outputStream;
  private ByteBufferInputStream inputStream;
  private DistributionManager distributionManager;
  private InternalDistributedMember internalDistributedMember;

  private static final byte REPLY_REFUSED = (byte) 60;
  private static final byte REPLY_INVALID = (byte) 61;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() throws Exception {
    proxyId = mock(ClientProxyMembershipID.class);
    system = mock(InternalDistributedSystem.class);
    securityService = mock(SecurityService.class);
    connection = mock(Connection.class);
    properties = mock(Properties.class);
    member = mock(ServerLocation.class);
    securityproperties = mock(Properties.class);
    socket = mock(Socket.class);
    when(system.getProperties()).thenReturn(properties);

    distributionManager = mock(DistributionManager.class);
    when(system.getDistributionManager()).thenReturn(distributionManager);
    internalDistributedMember = mock(InternalDistributedMember.class);
    when(distributionManager.getDistributionManagerId()).thenReturn(internalDistributedMember);
    when(internalDistributedMember.getMembershipPort()).thenReturn(80);
    when(proxyId.getDSFID()).thenReturn(1);

    when(connection.getSocket()).thenReturn(socket);
    outputStream = new BufferDataOutputStream(Version.getCurrentVersion());
  }

  @Test
  public void testClientSideHandshakeRefused() throws IOException {
    when(properties.getProperty(anyString())).thenReturn("false");
    when(system.getSecurityLogWriter()).thenReturn(mock(LogWriter.class));

    inputStream = returnRefuseHandshake();

    when(socket.getOutputStream()).thenReturn(outputStream);
    when(socket.getInputStream()).thenReturn(inputStream);

    ClientSideHandshakeImpl handshake =
        new ClientSideHandshakeImpl(proxyId, system, securityService, false);
    assertThatThrownBy(() -> handshake.handshakeWithServer(connection, member, ClientToServer))
        .isInstanceOf(ServerRefusedConnectionException.class);
  }

  @Test
  public void testClientSideHandshakeRefusedbyGWReceiver() throws IOException {
    when(system.getSecurityProperties()).thenReturn(securityproperties);

    when(properties.getProperty(eq(CONFLATE_EVENTS))).thenReturn("false");
    when(properties.getProperty(eq(SECURITY_CLIENT_AUTH_INIT))).thenReturn("");

    when(system.getSecurityLogWriter()).thenReturn(mock(InternalLogWriter.class));

    inputStream = returnRefuseHandshake();

    when(socket.getOutputStream()).thenReturn(outputStream);
    when(socket.getInputStream()).thenReturn(inputStream);

    ClientSideHandshakeImpl handshake =
        new ClientSideHandshakeImpl(proxyId, system, securityService, false);
    assertThatThrownBy(() -> handshake.handshakeWithServer(connection, member, GatewayToGateway))
        .isInstanceOf(ServerRefusedConnectionException.class);
  }

  @Test
  public void testClientSideInvalidHandshakebyGWReceiver() throws IOException {
    when(system.getSecurityProperties()).thenReturn(securityproperties);

    when(properties.getProperty(eq(CONFLATE_EVENTS))).thenReturn("false");
    when(properties.getProperty(eq(SECURITY_CLIENT_AUTH_INIT))).thenReturn("");

    when(system.getSecurityLogWriter()).thenReturn(mock(InternalLogWriter.class));

    inputStream = returnInvalidHandshake();

    when(socket.getOutputStream()).thenReturn(outputStream);
    when(socket.getInputStream()).thenReturn(inputStream);

    ClientSideHandshakeImpl handshake =
        new ClientSideHandshakeImpl(proxyId, system, securityService, false);
    assertThatThrownBy(() -> handshake.handshakeWithServer(connection, member, GatewayToGateway))
        .isInstanceOf(ServerRefusedConnectionException.class);
  }


  private ByteBufferInputStream returnRefuseHandshake() throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
    // Write refused reply
    hdos.writeByte(REPLY_REFUSED);
    // write dummy endpointType
    hdos.writeByte(0);
    // write dummy queueSize
    hdos.writeInt(0);

    // Write the server's member
    DistributedMember member =
        new InternalDistributedMember(InetAddress.getByName("localhost"), 50505, false,
            false);

    HeapDataOutputStream memberDos = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(member, memberDos);
    DataSerializer.writeByteArray(memberDos.toByteArray(), hdos);
    memberDos.close();

    String message = "exceeded max-connections 800.";
    hdos.writeUTF(message);
    hdos.writeBoolean(Boolean.TRUE);
    hdos.toByteArray();

    ByteBufferInputStream bbis = new ByteBufferInputStream(hdos.toByteBuffer());
    hdos.close();
    return bbis;
  }

  private ByteBufferInputStream returnInvalidHandshake() throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
    // Write refused reply
    hdos.writeByte(REPLY_INVALID);
    // write dummy endpointType
    hdos.writeByte(0);
    // write dummy queueSize
    hdos.writeInt(0);

    // Write the server's member
    DistributedMember member =
        new InternalDistributedMember(InetAddress.getByName("localhost"), 50505, false,
            false);

    HeapDataOutputStream memberDos = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(member, memberDos);
    DataSerializer.writeByteArray(memberDos.toByteArray(), hdos);
    memberDos.close();

    String message = "Received Unknown handshake reply code.";
    hdos.writeUTF(message);
    hdos.writeBoolean(Boolean.TRUE);
    hdos.toByteArray();

    ByteBufferInputStream bbis = new ByteBufferInputStream(hdos.toByteBuffer());
    hdos.close();
    return bbis;
  }
}
