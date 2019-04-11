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
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;
import org.apache.geode.internal.VersionedDataOutputStream;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.logging.LogService;

class ClientRegistrationMetadata {
  private final ClientProxyMembershipID clientProxyMembershipID;
  private final byte clientConflation;
  private final Properties clientCredentials;
  private final Version clientVersion;
  private final DataInputStream dataInputStream;
  private final DataOutputStream dataOutputStream;
  private final SocketMessageWriter socketMessageWriter;
  private static final Logger logger = LogService.getLogger();

  ClientRegistrationMetadata(final InternalCache cache, final Socket socket)
      throws IOException, ClassNotFoundException {
    DataInputStream unversionedDataInputStream = new DataInputStream(socket.getInputStream());
    DataOutputStream unversionedDataOutputStream = new DataOutputStream(socket.getOutputStream());
    socketMessageWriter = new SocketMessageWriter();

    clientVersion = getAndValidateClientVersion(socket, unversionedDataInputStream,
        unversionedDataOutputStream);

    if (oldClientRequiresVersionedStreams(clientVersion)) {
      dataInputStream = new VersionedDataInputStream(unversionedDataInputStream, clientVersion);
      dataOutputStream =
          new VersionedDataOutputStream(unversionedDataOutputStream, clientVersion);
    } else {
      dataInputStream = unversionedDataInputStream;
      dataOutputStream = unversionedDataOutputStream;
    }

    // Read and ignore the reply code. This is used on the client to server
    // handshake.
    dataInputStream.readByte(); // replyCode

    // Read the ports and throw them away. We no longer need them
    int numberOfPorts = dataInputStream.readInt();
    for (int i = 0; i < numberOfPorts; i++) {
      dataInputStream.readInt();
    }

    clientProxyMembershipID = getAndValidateClientProxyMembershipID();

    clientConflation = getAndValidateClientConflation();

    clientCredentials =
        Handshake.readCredentials(dataInputStream, dataOutputStream,
            cache.getDistributedSystem(), cache.getSecurityService());
  }

  ClientProxyMembershipID getClientProxyMembershipID() {
    return clientProxyMembershipID;
  }

  byte getClientConflation() {
    return clientConflation;
  }

  Properties getClientCredentials() {
    return clientCredentials;
  }

  Version getClientVersion() {
    return clientVersion;
  }

  DataOutputStream getDataOutputStream() {
    return dataOutputStream;
  }

  private Version getAndValidateClientVersion(final Socket socket,
      final DataInputStream dataInputStream, final DataOutputStream dataOutputStream)
      throws IOException {
    short clientVersionOrdinal = Version.readOrdinal(dataInputStream);
    final Version clientVersion;

    try {
      clientVersion = Version.fromOrdinal(clientVersionOrdinal, true);
      if (isVersion57orOlder(clientVersion)) {
        throw new UnsupportedVersionException(clientVersionOrdinal);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Registering client with version: {}", this, clientVersion);
      }
    } catch (UnsupportedVersionException e) {
      UnsupportedVersionException unsupportedVersionException = e;
      SocketAddress socketAddress = socket.getRemoteSocketAddress();

      if (socketAddress != null) {
        String sInfo = " Client: " + socketAddress.toString() + ".";
        unsupportedVersionException = new UnsupportedVersionException(e.getMessage() + sInfo);
      }

      logger.warn(
          "CacheClientNotifier: Registering client version is unsupported.  Error details: ",
          unsupportedVersionException);

      socketMessageWriter.writeException(dataOutputStream,
          CommunicationMode.UnsuccessfulServerToClient.getModeNumber(),
          unsupportedVersionException, null);

      throw new IllegalArgumentException(unsupportedVersionException);
    }

    return clientVersion;
  }

  private boolean doesClientSupportExtractOverrides() {
    return clientVersion.compareTo(Version.GFE_603) >= 0;
  }

  private boolean oldClientRequiresVersionedStreams(final Version clientVersion) {
    return Version.CURRENT.compareTo(clientVersion) > 0;
  }

  private boolean isVersion57orOlder(final Version clientVersion) {
    return Version.GFE_57.compareTo(clientVersion) > 0;
  }

  private ClientProxyMembershipID getAndValidateClientProxyMembershipID()
      throws IOException, ClassNotFoundException {
    ClientProxyMembershipID clientProxyMembershipID =
        ClientProxyMembershipID.readCanonicalized(dataInputStream);

    return clientProxyMembershipID;
  }

  private byte getAndValidateClientConflation()
      throws IOException {
    final byte clientConflation;
    if (doesClientSupportExtractOverrides()) {
      byte[] overrides =
          Handshake.extractOverrides(new byte[] {(byte) dataInputStream.read()});
      clientConflation = overrides[0];
    } else {
      clientConflation = (byte) dataInputStream.read();
    }

    switch (clientConflation) {
      case Handshake.CONFLATION_DEFAULT:
      case Handshake.CONFLATION_OFF:
      case Handshake.CONFLATION_ON:
        break;
      default:
        IllegalArgumentException illegalArgumentException =
            new IllegalArgumentException("Invalid conflation byte");
        socketMessageWriter.writeException(dataOutputStream, Handshake.REPLY_INVALID,
            illegalArgumentException, clientVersion);
        throw new IOException(illegalArgumentException.toString());
    }

    return clientConflation;
  }
}
