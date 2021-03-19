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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.Properties;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.ByteBufferOutputStream;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.Encryptor;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.net.ByteBufferSharing;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.internal.serialization.VersionedDataStream;
import org.apache.geode.internal.serialization.VersioningIO;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.security.AuthenticationRequiredException;

public class ServerSideHandshakeImpl extends Handshake implements ServerSideHandshake {
  @Immutable
  private static final KnownVersion currentServerVersion =
      ServerSideHandshakeFactory.currentServerVersion;
  private final KnownVersion clientVersion;

  private final byte replyCode;
  private ServerConnection connection;

  @Override
  protected byte getReplyCode() {
    return replyCode;
  }

  /**
   * HandShake Constructor used by server side connection
   */
  public ServerSideHandshakeImpl(Socket sock, int timeout, DistributedSystem sys,
      KnownVersion clientVersion, CommunicationMode communicationMode,
      SecurityService securityService, ServerConnection connection)
      throws IOException, AuthenticationRequiredException {

    this.clientVersion = clientVersion;
    system = sys;
    this.securityService = securityService;
    encryptor = new EncryptorImpl(sys.getSecurityLogWriter());
    this.connection = connection;

    int soTimeout = -1;
    InputStream inputStream = null;

    try {
      soTimeout = sock.getSoTimeout();
      sock.setSoTimeout(timeout);
      if (connection.getIOFilter() == null) {
        inputStream = sock.getInputStream();
      } else {
        inputStream = connection.getIOFilter().getInputStream(sock);
      }
      int valRead = inputStream.read();
      if (valRead == -1) {
        throw new EOFException(
            "HandShake: EOF reached before client code could be read");
      }
      replyCode = (byte) valRead;
      if (replyCode != REPLY_OK) {
        throw new IOException(
            "HandShake reply code is not ok");
      }
      try {
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        DataOutputStream dataOutputStream = new DataOutputStream(sock.getOutputStream());
        clientReadTimeout = dataInputStream.readInt();
        if (clientVersion.isOlderThan(KnownVersion.CURRENT)) {
          // versioned streams allow object serialization code to deal with older clients
          dataInputStream = new VersionedDataInputStream(dataInputStream, clientVersion);
          dataOutputStream =
              new VersionedDataOutputStream(dataOutputStream, clientVersion);
        }
        id = ClientProxyMembershipID.readCanonicalized(dataInputStream);
        setOverrides(new byte[] {dataInputStream.readByte()});
        // Note: credentials should always be the last piece in handshake for
        // Diffie-Hellman key exchange to work
        if (communicationMode.isWAN()) {
          credentials =
              readCredentials(dataInputStream, dataOutputStream, sys, this.securityService);
        } else {
          credentials = readCredential(dataInputStream, dataOutputStream, sys);
        }
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(
            "ClientProxyMembershipID class could not be found while deserializing the object");
      }
    } finally {
      if (connection.getIOFilter() != null) {
        try (
            final ByteBufferSharing sharedBuffer = connection.getIOFilter().getUnwrappedBuffer()) {
          ByteBuffer iobuff = sharedBuffer.getBuffer();
          if (iobuff.capacity() > 0) {
            connection.getIOFilter().doneReading(iobuff);
          }
        }
        connection.getIOFilter().closeInputStream(inputStream);
      }
      if (soTimeout != -1) {
        try {
          sock.setSoTimeout(soTimeout);
        } catch (IOException ignore) {
        }
      }
    }
  }

  public KnownVersion getClientVersion() {
    return clientVersion;
  }

  @Override
  public KnownVersion getVersion() {
    return clientVersion;
  }

  @Override
  public void handshakeWithClient(OutputStream out, InputStream in, byte endpointType,
      int queueSize, CommunicationMode communicationMode, Principal principal) throws IOException {

    DataOutputStream dos;
    ByteBufferOutputStream bbos = null;

    if (connection.getIOFilter() != null) {
      bbos = new ByteBufferOutputStream(
          connection.getIOFilter().getPacketBufferSize());
      dos = new DataOutputStream(bbos);
    } else {
      dos = new DataOutputStream(out);
    }

    DataInputStream dis;
    if (clientVersion.isOlderThan(KnownVersion.CURRENT)) {
      dis = new VersionedDataInputStream(in, clientVersion);
      dos = new VersionedDataOutputStream(dos, clientVersion);
    } else {
      dis = new DataInputStream(in);
    }
    // Write ok reply
    if (communicationMode.isWAN() && principal != null) {
      dos.writeByte(REPLY_WAN_CREDENTIALS);
    } else {
      dos.writeByte(REPLY_OK);// byte 59
    }


    // additional byte of wan site needs to send for Gateway BC
    if (communicationMode.isWAN()) {
      VersioningIO.writeOrdinal(dos, currentServerVersion.ordinal(), true);
    }

    dos.writeByte(endpointType);
    dos.writeInt(queueSize);

    // Write the server's member
    DistributedMember member = system.getDistributedMember();

    KnownVersion v = KnownVersion.CURRENT;
    if (dos instanceof VersionedDataStream) {
      v = ((VersionedDataStream) dos).getVersion();
    }
    HeapDataOutputStream hdos = new HeapDataOutputStream(v);
    DataSerializer.writeObject(member, hdos);
    DataSerializer.writeByteArray(hdos.toByteArray(), dos);
    hdos.close();

    // Write no message
    dos.writeUTF("");

    // Write delta-propagation property value if this is not WAN.
    if (!communicationMode.isWAN()) {
      dos.writeBoolean(((InternalDistributedSystem) system).getConfig().getDeltaPropagation());
    }

    if (communicationMode.isWAN()) {
      if (principal != null) {
        sendCredentialsForWan(dos, dis);
      }

      dos.writeByte(
          ((InternalDistributedSystem) system).getDistributionManager().getDistributedSystemId());

      if (clientVersion.isNotOlderThan(KnownVersion.GFE_80)
          && currentServerVersion.isNotOlderThan(KnownVersion.GFE_80)) {
        int pdxSize = PeerTypeRegistration.getPdxRegistrySize();
        dos.writeInt(pdxSize);
      }
    }

    // Flush
    dos.flush();

    if (bbos != null) {
      bbos.flush();
      ByteBuffer buffer = bbos.getContentBuffer();
      try (final ByteBufferSharing outputSharing = connection.getIOFilter().wrap(buffer)) {
        final ByteBuffer wrappedBuffer = outputSharing.getBuffer();
        connection.getSocket().getChannel().write(wrappedBuffer);
      }
      bbos.close();
    }
  }

  @Override
  public Encryptor getEncryptor() {
    return encryptor;
  }

  private void sendCredentialsForWan(OutputStream out, InputStream in) {
    try {
      Properties wanCredentials = getCredentials(id.getDistributedMember());
      DataOutputStream dos = new DataOutputStream(out);
      DataInputStream dis = new DataInputStream(in);
      writeCredentials(dos, dis, wanCredentials, false, system.getDistributedMember());
    }
    // The exception while getting the credentials is just logged as severe
    catch (Exception e) {
      system.getSecurityLogWriter().severe(
          String.format("An exception was thrown while sending wan credentials: %s",
              e.getLocalizedMessage()));
    }
  }

  @Override
  public int getClientReadTimeout() {
    return clientReadTimeout;
  }
}
