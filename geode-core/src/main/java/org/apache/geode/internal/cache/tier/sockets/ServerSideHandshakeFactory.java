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
import java.net.SocketException;
import java.net.SocketTimeoutException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.cache.VersionException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;

class ServerSideHandshakeFactory {
  private static byte REPLY_REFUSED = AcceptorImpl.REPLY_REFUSED;

  private static final Logger logger = LogService.getLogger();
  static final Version currentServerVersion = Acceptor.VERSION;

  static boolean readHandshake(ServerConnection connection, SecurityService securityService,
      AcceptorImpl acceptorImpl) {
    try {
      // Read the version byte from the socket
      Version clientVersion = readClientVersion(connection);

      if (logger.isDebugEnabled()) {
        logger.debug("Client version: {}", clientVersion);
      }

      // Read the appropriate handshake
      if (clientVersion.compareTo(Version.GFE_57) >= 0) {
        return readGFEHandshake(connection, clientVersion, securityService, acceptorImpl);
      } else {
        connection.refuseHandshake("Unsupported version " + clientVersion
            + "Server's current version " + currentServerVersion, REPLY_REFUSED);
        return false;
      }
    } catch (IOException e) {
      // Only log an exception if the server is still running.
      if (connection.getAcceptor().isRunning()) {
        // Server logging
        logger.warn("{} {}", connection.getName(), e.getMessage(), e);
      }
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
    } catch (UnsupportedVersionException uve) {
      // Server logging
      logger.warn("{} {}", connection.getName(), uve.getMessage(), uve);
      // Client logging
      connection.refuseHandshake(uve.getMessage(), REPLY_REFUSED);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
    } catch (Exception e) {
      // Server logging
      logger.warn("{} {}", connection.getName(), e.getMessage(), e);
      // Client logging
      connection.refuseHandshake(
          LocalizedStrings.ServerHandShakeProcessor_0_SERVERS_CURRENT_VERSION_IS_1
              .toLocalizedString(new Object[] {e.getMessage(), currentServerVersion.toString()}),
          REPLY_REFUSED);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
    }

    return false;
  }

  static boolean readGFEHandshake(ServerConnection connection, Version clientVersion,
      SecurityService securityService, AcceptorImpl acceptorImpl) {
    int handshakeTimeout = connection.getHandShakeTimeout();
    InternalLogWriter securityLogWriter = connection.getSecurityLogWriter();
    try {
      Socket socket = connection.getSocket();
      DistributedSystem system = connection.getDistributedSystem();
      // hitesh:it will set credentials and principals
      ServerSideHandshake handshake = new ServerSideHandshakeImpl(socket, handshakeTimeout, system,
          clientVersion, connection.getCommunicationMode(), securityService);
      connection.setHandshake(handshake);
      ClientProxyMembershipID proxyId = handshake.getMembershipId();
      connection.setProxyId(proxyId);
      // hitesh: it gets principals
      // Hitesh:for older version we should set this
      if (clientVersion.compareTo(Version.GFE_65) < 0
          || connection.getCommunicationMode().isWAN()) {
        connection.setAuthAttributes();
      }
    } catch (SocketTimeoutException timeout) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.ServerHandShakeProcessor_0_HANDSHAKE_REPLY_CODE_TIMEOUT_NOT_RECEIVED_WITH_IN_1_MS,
          new Object[] {connection.getName(), Integer.valueOf(handshakeTimeout)}));
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      return false;
    } catch (EOFException | SocketException e) {
      // no need to warn client just gave up on this server before we could
      // handshake
      logger.info("{} {}", connection.getName(), e);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      return false;
    } catch (IOException e) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.ServerHandShakeProcessor_0_RECEIVED_NO_HANDSHAKE_REPLY_CODE,
          connection.getName()), e);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      return false;
    } catch (AuthenticationRequiredException noauth) {
      String exStr = noauth.getLocalizedMessage();
      if (noauth.getCause() != null) {
        exStr += " : " + noauth.getCause().getLocalizedMessage();
      }
      if (securityLogWriter.warningEnabled()) {
        securityLogWriter.warning(LocalizedStrings.ONE_ARG,
            connection.getName() + ": Security exception: " + exStr);
      }
      connection.stats.incFailedConnectionAttempts();
      connection.refuseHandshake(noauth.getMessage(),
          Handshake.REPLY_EXCEPTION_AUTHENTICATION_REQUIRED);
      connection.cleanup();
      return false;
    } catch (AuthenticationFailedException failed) {
      String exStr = failed.getLocalizedMessage();
      if (failed.getCause() != null) {
        exStr += " : " + failed.getCause().getLocalizedMessage();
      }
      if (securityLogWriter.warningEnabled()) {
        securityLogWriter.warning(LocalizedStrings.ONE_ARG,
            connection.getName() + ": Security exception: " + exStr);
      }
      connection.stats.incFailedConnectionAttempts();
      connection.refuseHandshake(failed.getMessage(),
          Handshake.REPLY_EXCEPTION_AUTHENTICATION_FAILED);
      connection.cleanup();
      return false;
    } catch (Exception ex) {
      logger.warn("{} {}", connection.getName(), ex.getLocalizedMessage());
      connection.stats.incFailedConnectionAttempts();
      connection.refuseHandshake(ex.getMessage(), REPLY_REFUSED);
      connection.cleanup();
      return false;
    }
    return true;
  }

  private static Version readClientVersion(ServerConnection connection)
      throws IOException, VersionException {

    Socket socket = connection.getSocket();
    int timeout = connection.getHandShakeTimeout();

    int soTimeout = -1;
    try {
      soTimeout = socket.getSoTimeout();
      socket.setSoTimeout(timeout);
      InputStream is = socket.getInputStream();
      short clientVersionOrdinal = Version.readOrdinalFromInputStream(is);
      if (clientVersionOrdinal == -1) {
        throw new EOFException(
            LocalizedStrings.ServerHandShakeProcessor_HANDSHAKEREADER_EOF_REACHED_BEFORE_CLIENT_VERSION_COULD_BE_READ
                .toLocalizedString());
      }
      Version clientVersion = null;
      try {
        clientVersion = Version.fromOrdinal(clientVersionOrdinal, true);
      } catch (UnsupportedVersionException uve) {
        // Allows higher version of wan site to connect to server
        if (connection.getCommunicationMode().isWAN()) {
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
