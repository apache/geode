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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.cache.VersionException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;

class ServerSideHandshakeFactory {
  private static final Logger logger = LogService.getLogger();
  static final Version currentServerVersion = Version.CURRENT;

  ServerSideHandshake readHandshake(Socket socket, int timeout, CommunicationMode communicationMode,
      DistributedSystem system, SecurityService securityService) throws Exception {
    // Read the version byte from the socket
    Version clientVersion = readClientVersion(socket, timeout, communicationMode.isWAN());

    if (logger.isDebugEnabled()) {
      logger.debug("Client version: {}", clientVersion);
    }

    if (clientVersion.compareTo(Version.GFE_57) < 0) {
      throw new UnsupportedVersionException("Unsupported version " + clientVersion
          + "Server's current version " + currentServerVersion);
    }

    return new ServerSideHandshakeImpl(socket, timeout, system, clientVersion, communicationMode,
        securityService);
  }

  private Version readClientVersion(Socket socket, int timeout, boolean isWan)
      throws IOException, VersionException {
    int soTimeout = -1;
    try {
      soTimeout = socket.getSoTimeout();
      socket.setSoTimeout(timeout);
      InputStream is = socket.getInputStream();
      short clientVersionOrdinal = Version.readOrdinalFromInputStream(is);
      if (clientVersionOrdinal == -1) {
        throw new EOFException(
            "HandShakeReader: EOF reached before client version could be read");
      }
      Version clientVersion = null;
      try {
        clientVersion = Version.fromOrdinal(clientVersionOrdinal, true);
      } catch (UnsupportedVersionException uve) {
        // Allows higher version of wan site to connect to server
        if (isWan) {
          return currentServerVersion;
        } else {
          SocketAddress sa = socket.getRemoteSocketAddress();
          String sInfo = "";
          if (sa != null) {
            sInfo = " Client: " + sa.toString() + ".";
          }
          throw new UnsupportedVersionException(uve.getMessage() + sInfo);
        }
      }

      if (!clientVersion.compatibleWith(currentServerVersion)) {
        throw new IncompatibleVersionException(clientVersion, currentServerVersion);
      }
      return clientVersion;
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
