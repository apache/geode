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
import static org.apache.geode.management.internal.security.ResourceConstants.USER_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.tcp.ByteBufferInputStream;

public class ClientSideHandshakeImplTest {
  private ClientProxyMembershipID proxyId;
  private InternalDistributedSystem ds;
  private SecurityService securityService;

  private static final byte REPLY_OK = (byte) 59;

  private static final byte REPLY_REFUSED = (byte) 60;

  private static final byte REPLY_INVALID = (byte) 61;

  @Before
  public void setUp() throws Exception {
    proxyId = mock(ClientProxyMembershipID.class);
    ds = mock(InternalDistributedSystem.class);
    securityService = mock(SecurityService.class);
  }

  @After
  public void tearDown() throws Exception {}


  @Test
  public void testClientSideHandshakeRefused() throws IOException {
    Connection conn = mock(Connection.class);
    Properties props = mock(Properties.class);
    ServerLocation location = mock(ServerLocation.class);
    when(ds.getProperties()).thenReturn(props);
    when(props.getProperty(anyString())).thenReturn("false");
    when(ds.getSecurityLogWriter()).thenReturn(mock(LogWriter.class));

    Socket sock = mock(Socket.class);
    when(conn.getSocket()).thenReturn(sock);

    DistributionManager dm = mock(DistributionManager.class);
    when(ds.getDistributionManager()).thenReturn(dm);
    InternalDistributedMember idm = mock(InternalDistributedMember.class);
    when(dm.getDistributionManagerId()).thenReturn(idm);
    when(idm.getMembershipPort()).thenReturn(80);
    when(proxyId.getDSFID()).thenReturn(1);

    BufferDataOutputStream out =
        new BufferDataOutputStream(KnownVersion.getCurrentVersion());

    ByteBufferInputStream bbis = returnRefuseHandshake();

    when(sock.getOutputStream()).thenReturn(out);
    when(sock.getInputStream()).thenReturn(bbis);

    ClientSideHandshakeImpl handshake =
        new ClientSideHandshakeImpl(proxyId, ds, securityService, false);
    assertThatThrownBy(() -> handshake.handshakeWithServer(conn, location, ClientToServer))
        .isInstanceOf(ServerRefusedConnectionException.class);
  }

  @Test
  public void testClientSideHandshakeRefusedbyGWReceiver() throws IOException {
    Connection conn = mock(Connection.class);
    Properties props = mock(Properties.class);
    Properties securityprops = mock(Properties.class);

    ServerLocation location = mock(ServerLocation.class);
    when(ds.getProperties()).thenReturn(props);
    when(ds.getSecurityProperties()).thenReturn(securityprops);

    when(props.getProperty(eq(CONFLATE_EVENTS))).thenReturn("false");
    when(props.getProperty(eq(SECURITY_CLIENT_AUTH_INIT))).thenReturn("");
    when(securityprops.contains(eq(USER_NAME))).thenReturn(false);

    when(ds.getSecurityLogWriter()).thenReturn(mock(InternalLogWriter.class));

    Socket sock = mock(Socket.class);
    when(conn.getSocket()).thenReturn(sock);

    DistributionManager dm = mock(DistributionManager.class);
    when(ds.getDistributionManager()).thenReturn(dm);
    InternalDistributedMember idm = mock(InternalDistributedMember.class);
    when(dm.getDistributionManagerId()).thenReturn(idm);
    when(idm.getMembershipPort()).thenReturn(80);
    when(proxyId.getDSFID()).thenReturn(1);

    BufferDataOutputStream out =
        new BufferDataOutputStream(KnownVersion.getCurrentVersion());

    ByteBufferInputStream bbis = returnRefuseHandshake();

    when(sock.getOutputStream()).thenReturn(out);
    when(sock.getInputStream()).thenReturn(bbis);

    ClientSideHandshakeImpl handshake =
        new ClientSideHandshakeImpl(proxyId, ds, securityService, false);
    assertThatThrownBy(() -> handshake.handshakeWithServer(conn, location, GatewayToGateway))
        .isInstanceOf(ServerRefusedConnectionException.class);
  }

  @Test
  public void testClientSideInvalidHandshakebyGWReceiver() throws IOException {
    Connection conn = mock(Connection.class);
    Properties props = mock(Properties.class);
    Properties securityprops = mock(Properties.class);

    ServerLocation location = mock(ServerLocation.class);
    when(ds.getProperties()).thenReturn(props);
    when(ds.getSecurityProperties()).thenReturn(securityprops);

    when(props.getProperty(eq(CONFLATE_EVENTS))).thenReturn("false");
    when(props.getProperty(eq(SECURITY_CLIENT_AUTH_INIT))).thenReturn("");

    when(securityprops.contains(eq(USER_NAME))).thenReturn(false);

    when(ds.getSecurityLogWriter()).thenReturn(mock(InternalLogWriter.class));

    Socket sock = mock(Socket.class);
    when(conn.getSocket()).thenReturn(sock);

    DistributionManager dm = mock(DistributionManager.class);
    when(ds.getDistributionManager()).thenReturn(dm);
    InternalDistributedMember idm = mock(InternalDistributedMember.class);
    when(dm.getDistributionManagerId()).thenReturn(idm);
    when(idm.getMembershipPort()).thenReturn(80);
    when(proxyId.getDSFID()).thenReturn(1);

    BufferDataOutputStream out =
        new BufferDataOutputStream(KnownVersion.getCurrentVersion());

    ByteBufferInputStream bbis = returnInvalidHandshake();

    when(sock.getOutputStream()).thenReturn(out);
    when(sock.getInputStream()).thenReturn(bbis);

    ClientSideHandshakeImpl handshake =
        new ClientSideHandshakeImpl(proxyId, ds, securityService, false);
    assertThatThrownBy(() -> handshake.handshakeWithServer(conn, location, GatewayToGateway))
        .isInstanceOf(ServerRefusedConnectionException.class);
  }


  ByteBufferInputStream returnRefuseHandshake() throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(32, KnownVersion.CURRENT);
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

    HeapDataOutputStream memberDos = new HeapDataOutputStream(KnownVersion.CURRENT);
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

  ByteBufferInputStream returnInvalidHandshake() throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(32, KnownVersion.CURRENT);
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

    HeapDataOutputStream memberDos = new HeapDataOutputStream(KnownVersion.CURRENT);
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
