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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.cache.VersionException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.Versioning;
import org.apache.geode.internal.serialization.VersioningIO;
import org.apache.geode.logging.internal.log4j.api.LogService;

class ServerSideHandshakeFactory {
  private static final Logger logger = LogService.getLogger();

  @Immutable
  static final KnownVersion currentServerVersion = KnownVersion.CURRENT;

  ServerSideHandshake readHandshake(Socket socket, int timeout, CommunicationMode communicationMode,
      DistributedSystem system, SecurityService securityService) throws Exception {
    // Read the version byte from the socket
    KnownVersion clientVersion = readClientVersion(socket, timeout, communicationMode.isWAN());

    if (logger.isDebugEnabled()) {
      logger.debug("Client version: {}", clientVersion);
    }

    if (clientVersion.isOlderThan(KnownVersion.OLDEST)) {
      throw new UnsupportedVersionException("Unsupported version " + clientVersion
          + "Server's current version " + currentServerVersion);
    }

    return new ServerSideHandshakeImpl(socket, timeout, system, clientVersion, communicationMode,
        securityService);
  }

  private KnownVersion readClientVersion(Socket socket, int timeout, boolean isWan)
      throws IOException, VersionException {
    int soTimeout = -1;
    try {
      soTimeout = socket.getSoTimeout();
      socket.setSoTimeout(timeout);
      InputStream is = socket.getInputStream();
      short clientVersionOrdinal = VersioningIO.readOrdinalFromInputStream(is);
      if (clientVersionOrdinal == -1) {
        throw new EOFException(
            "HandShakeReader: EOF reached before client version could be read");
      }
      // don't do this check until after we've read the ordinal off the wire
      if (isWan) {
        return currentServerVersion;
      }
      final KnownVersion clientVersion = Versioning.getKnownVersionOrDefault(
          Versioning.getVersion(clientVersionOrdinal), KnownVersion.TEST_VERSION);
      if (clientVersion != KnownVersion.TEST_VERSION) {
        final Map<Integer, Command> commands =
            CommandInitializer.getDefaultInstance().get(clientVersion);
        if (commands != null) {
          return clientVersion;
        }
      }
      SocketAddress sa = socket.getRemoteSocketAddress();
      String sInfo = "";
      if (sa != null) {
        sInfo = " Client: " + sa.toString() + ".";
      }
      throw new UnsupportedVersionException(
          KnownVersion.unsupportedVersionMessage(clientVersionOrdinal) + sInfo);
    } finally {
      if (soTimeout != -1) {
        try {
          socket.setSoTimeout(soTimeout);
        } catch (IOException ignore) {
        }
      }
    }
  }
}
