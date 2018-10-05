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
import java.security.Principal;
import java.util.Properties;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;
import org.apache.geode.internal.VersionedDataOutputStream;
import org.apache.geode.internal.VersionedDataStream;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.Encryptor;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.security.AuthenticationRequiredException;

public class ServerSideHandshakeImpl extends Handshake implements ServerSideHandshake {
  private static final Version currentServerVersion =
      ServerSideHandshakeFactory.currentServerVersion;
  private Version clientVersion;

  private final byte replyCode;

  @Override
  protected byte getReplyCode() {
    return replyCode;
  }

  /**
   * HandShake Constructor used by server side connection
   */
  public ServerSideHandshakeImpl(Socket sock, int timeout, DistributedSystem sys,
      Version clientVersion, CommunicationMode communicationMode, SecurityService securityService)
      throws IOException, AuthenticationRequiredException {

    this.clientVersion = clientVersion;
    this.system = sys;
    this.securityService = securityService;
    this.encryptor = new EncryptorImpl(sys.getSecurityLogWriter());

    int soTimeout = -1;
    try {
      soTimeout = sock.getSoTimeout();
      sock.setSoTimeout(timeout);
      InputStream inputStream = sock.getInputStream();
      int valRead = inputStream.read();
      if (valRead == -1) {
        throw new EOFException(
            "HandShake: EOF reached before client code could be read");
      }
      this.replyCode = (byte) valRead;
      if (replyCode != REPLY_OK) {
        throw new IOException(
            "HandShake reply code is not ok");
      }
      try {
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        DataOutputStream dataOutputStream = new DataOutputStream(sock.getOutputStream());
        this.clientReadTimeout = dataInputStream.readInt();
        if (clientVersion.compareTo(Version.CURRENT) < 0) {
          // versioned streams allow object serialization code to deal with older clients
          dataInputStream = new VersionedDataInputStream(dataInputStream, clientVersion);
          dataOutputStream = new VersionedDataOutputStream(dataOutputStream, clientVersion);
        }
        this.id = ClientProxyMembershipID.readCanonicalized(dataInputStream);
        // Note: credentials should always be the last piece in handshake for
        // Diffie-Hellman key exchange to work
        if (clientVersion.compareTo(Version.GFE_603) >= 0) {
          setOverrides(new byte[] {dataInputStream.readByte()});
        } else {
          setClientConflation(dataInputStream.readByte());
        }
        if (this.clientVersion.compareTo(Version.GFE_65) < 0 || communicationMode.isWAN()) {
          this.credentials =
              readCredentials(dataInputStream, dataOutputStream, sys, this.securityService);
        } else {
          this.credentials = this.readCredential(dataInputStream, dataOutputStream, sys);
        }
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(
            "ClientProxyMembershipID class could not be found while deserializing the object");
      }
    } finally {
      if (soTimeout != -1) {
        try {
          sock.setSoTimeout(soTimeout);
        } catch (IOException ignore) {
        }
      }
    }
  }

  public Version getClientVersion() {
    return this.clientVersion;
  }

  public Version getVersion() {
    return this.clientVersion;
  }

  @Override
  public void handshakeWithClient(OutputStream out, InputStream in, byte endpointType,
      int queueSize, CommunicationMode communicationMode, Principal principal) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);
    DataInputStream dis;
    if (clientVersion.compareTo(Version.CURRENT) < 0) {
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
      Version.writeOrdinal(dos, currentServerVersion.ordinal(), true);
    }

    dos.writeByte(endpointType);
    dos.writeInt(queueSize);

    // Write the server's member
    DistributedMember member = this.system.getDistributedMember();

    Version v = Version.CURRENT;
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
    if (!communicationMode.isWAN() && this.clientVersion.compareTo(Version.GFE_61) >= 0) {
      dos.writeBoolean(((InternalDistributedSystem) this.system).getConfig().getDeltaPropagation());
    }

    // Neeraj: Now if the communication mode is GATEWAY_TO_GATEWAY
    // and principal not equal to null then send the credentials also
    if (communicationMode.isWAN() && principal != null) {
      sendCredentialsForWan(dos, dis);
    }

    // Write the distributed system id if this is a 6.6 or greater client
    // on the remote side of the gateway
    if (communicationMode.isWAN() && this.clientVersion.compareTo(Version.GFE_66) >= 0
        && currentServerVersion.compareTo(Version.GFE_66) >= 0) {
      dos.writeByte(((InternalDistributedSystem) this.system).getDistributionManager()
          .getDistributedSystemId());
    }

    if ((communicationMode.isWAN()) && this.clientVersion.compareTo(Version.GFE_80) >= 0
        && currentServerVersion.compareTo(Version.GFE_80) >= 0) {
      int pdxSize = PeerTypeRegistration.getPdxRegistrySize();
      dos.writeInt(pdxSize);
    }

    // Flush
    dos.flush();
  }

  @Override
  public Encryptor getEncryptor() {
    return encryptor;
  }

  private void sendCredentialsForWan(OutputStream out, InputStream in) {
    try {
      Properties wanCredentials = getCredentials(this.id.getDistributedMember());
      DataOutputStream dos = new DataOutputStream(out);
      DataInputStream dis = new DataInputStream(in);
      writeCredentials(dos, dis, wanCredentials, false, this.system.getDistributedMember());
    }
    // The exception while getting the credentials is just logged as severe
    catch (Exception e) {
      this.system.getSecurityLogWriter().severe(
          String.format("An exception was thrown while sending wan credentials: %s",
              e.getLocalizedMessage()));
    }
  }

  public int getClientReadTimeout() {
    return this.clientReadTimeout;
  }
}
