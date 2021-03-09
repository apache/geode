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
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.net.NioFilter;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.internal.serialization.Versioning;
import org.apache.geode.internal.serialization.VersioningIO;
import org.apache.geode.logging.internal.log4j.api.LogService;

class ClientRegistrationMetadata {
  private static final Logger logger = LogService.getLogger();
  private final InternalCache cache;
  private final Socket socket;
  private final SocketMessageWriter socketMessageWriter;
  private ClientProxyMembershipID clientProxyMembershipID;
  private byte clientConflation;
  private Properties clientCredentials;
  private KnownVersion clientVersion;
  private DataInputStream dataInputStream;
  private DataOutputStream dataOutputStream;
  private final NioFilter ioFilter;

  ClientRegistrationMetadata(final InternalCache cache, final Socket socket,
      final NioFilter ioFilter) {
    this.cache = cache;
    this.socket = socket;
    socketMessageWriter = new SocketMessageWriter();
    this.ioFilter = ioFilter;
  }

  boolean initialize() throws IOException {
    InputStream inputStream = null;
    try {
      if (ioFilter == null) {
        inputStream = socket.getInputStream();
      } else {
        inputStream = ioFilter.getInputStream(socket);
      }
      DataInputStream unversionedDataInputStream = new DataInputStream(inputStream);

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
    } finally {
      if (ioFilter != null) {
        ioFilter.closeInputStream(inputStream);
      }
    }
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

  KnownVersion getClientVersion() {
    return clientVersion;
  }

  DataOutputStream getDataOutputStream() {
    return dataOutputStream;
  }

  private boolean getAndValidateClientVersion(final Socket socket,
      final DataInputStream dataInputStream, final DataOutputStream dataOutputStream)
      throws IOException {
    short clientVersionOrdinal = VersioningIO.readOrdinal(dataInputStream);

    clientVersion = Versioning.getKnownVersionOrDefault(
        Versioning.getVersion(clientVersionOrdinal), null);

    final String message;
    if (clientVersion == null) {
      message = KnownVersion.unsupportedVersionMessage(clientVersionOrdinal);
    } else {
      final Map<Integer, Command> commands =
          CommandInitializer.getDefaultInstance().get(clientVersion);
      if (commands == null) {
        message = "No commands registered for version " + clientVersion + ".";
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Registering client with version: {}", this, clientVersion);
        }
        return true;
      }
    }

    UnsupportedVersionException unsupportedVersionException =
        new UnsupportedVersionException(message);
    SocketAddress socketAddress = socket.getRemoteSocketAddress();

    if (socketAddress != null) {
      String sInfo = " Client: " + socketAddress.toString() + ".";
      unsupportedVersionException = new UnsupportedVersionException(message + sInfo);
    }

    logger.warn(
        "CacheClientNotifier: Registering client version is unsupported.  Error details: ",
        unsupportedVersionException);

    socketMessageWriter.writeException(dataOutputStream,
        CommunicationMode.UnsuccessfulServerToClient.getModeNumber(),
        unsupportedVersionException, null, ioFilter, socket);

    return false;
  }

  private boolean oldClientRequiresVersionedStreams(final KnownVersion clientVersion) {
    return KnownVersion.CURRENT.compareTo(clientVersion) > 0;
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
    final byte[] overrides = Handshake.extractOverrides(new byte[] {(byte) dataInputStream.read()});
    clientConflation = overrides[0];

    switch (clientConflation) {
      case Handshake.CONFLATION_DEFAULT:
      case Handshake.CONFLATION_OFF:
      case Handshake.CONFLATION_ON:
        break;
      default:
        socketMessageWriter.writeException(dataOutputStream, Handshake.REPLY_INVALID,
            new IllegalArgumentException("Invalid conflation byte"), clientVersion, ioFilter,
            socket);

        return false;
    }

    return true;
  }

  public NioFilter getIOFilter() {
    return this.ioFilter;
  }

  public Socket getSocket() {
    return socket;
  }
}
