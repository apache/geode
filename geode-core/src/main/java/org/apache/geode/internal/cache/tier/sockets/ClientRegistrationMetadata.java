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
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.SerializationVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;

class ClientRegistrationMetadata {
  private static final Logger logger = LogService.getLogger();
  private final InternalCache cache;
  private final Socket socket;
  private final SocketMessageWriter socketMessageWriter;
  private ClientProxyMembershipID clientProxyMembershipID;
  private byte clientConflation;
  private Properties clientCredentials;
  private Version clientVersion;
  private DataInputStream dataInputStream;
  private DataOutputStream dataOutputStream;

  ClientRegistrationMetadata(final InternalCache cache, final Socket socket) {
    this.cache = cache;
    this.socket = socket;
    this.socketMessageWriter = new SocketMessageWriter();
  }

  boolean initialize() throws IOException {
    DataInputStream unversionedDataInputStream = new DataInputStream(socket.getInputStream());
    DataOutputStream unversionedDataOutputStream = new DataOutputStream(socket.getOutputStream());

    if (getAndValidateClientVersion(socket, unversionedDataInputStream,
        unversionedDataOutputStream)) {
      if (oldClientRequiresVersionedStreams(clientVersion)) {
        dataInputStream =
            new VersionedDataInputStream(unversionedDataInputStream, clientVersion);
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

      getAndValidateClientProxyMembershipID();

      if (getAndValidateClientConflation()) {
        clientCredentials =
            Handshake.readCredentials(dataInputStream, dataOutputStream,
                cache.getDistributedSystem(), cache.getSecurityService());

        return true;
      }
    }

    return false;
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

  private boolean getAndValidateClientVersion(final Socket socket,
      final DataInputStream dataInputStream, final DataOutputStream dataOutputStream)
      throws IOException {
    short clientVersionOrdinal = SerializationVersion.readOrdinal(dataInputStream);

    try {
      clientVersion = Version.fromOrdinal(clientVersionOrdinal, true);
      if (isVersionOlderThan57(clientVersion)) {
        throw new IOException(new UnsupportedVersionException(clientVersionOrdinal));
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

      return false;
    }

    return true;
  }

  private boolean doesClientSupportExtractOverrides() {
    return clientVersion.compareTo(Version.GFE_603) >= 0;
  }

  private boolean oldClientRequiresVersionedStreams(final Version clientVersion) {
    return Version.CURRENT.compareTo(clientVersion) > 0;
  }

  private boolean isVersionOlderThan57(final Version clientVersion) {
    return Version.GFE_57.compareTo(clientVersion) > 0;
  }

  private void getAndValidateClientProxyMembershipID()
      throws IOException {
    try {
      clientProxyMembershipID = ClientProxyMembershipID.readCanonicalized(dataInputStream);
    } catch (ClassNotFoundException ex) {
      throw new IOException(ex);
    }
  }

  private boolean getAndValidateClientConflation()
      throws IOException {
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
        socketMessageWriter.writeException(dataOutputStream, Handshake.REPLY_INVALID,
            new IllegalArgumentException("Invalid conflation byte"), clientVersion);

        return false;
    }

    return true;
  }
}
