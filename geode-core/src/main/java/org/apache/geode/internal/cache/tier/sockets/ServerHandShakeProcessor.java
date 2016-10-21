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

import static org.apache.geode.distributed.ConfigurationProperties.*;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.Principal;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.cache.VersionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataStream;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;

/**
 * A <code>ServerHandShakeProcessor</code> verifies the client's version compatibility with server.
 *
 * @since GemFire 5.7
 */


public class ServerHandShakeProcessor {
  private static final Logger logger = LogService.getLogger();

  protected static final byte REPLY_REFUSED = (byte) 60;

  protected static final byte REPLY_INVALID = (byte) 61;

  public static Version currentServerVersion = Acceptor.VERSION;

  /**
   * Test hook for server version support
   * 
   * @since GemFire 5.7
   */
  public static void setSeverVersionForTesting(short ver) {
    currentServerVersion = Version.fromOrdinalOrCurrent(ver);
  }

  public static boolean readHandShake(ServerConnection connection) {
    boolean validHandShake = false;
    Version clientVersion = null;
    try {
      // Read the version byte from the socket
      clientVersion = readClientVersion(connection);
    } catch (IOException e) {
      // Only log an exception if the server is still running.
      if (connection.getAcceptor().isRunning()) {
        // Server logging
        logger.warn("{} {}", connection.getName(), e.getMessage(), e);
      }
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      validHandShake = false;
    } catch (UnsupportedVersionException uve) {
      // Server logging
      logger.warn("{} {}", connection.getName(), uve.getMessage(), uve);
      // Client logging
      connection.refuseHandshake(uve.getMessage(), REPLY_REFUSED);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      validHandShake = false;
    } catch (Exception e) {
      // Server logging
      logger.warn("{} {}", connection.getName(), e.getMessage(), e);
      // Client logging
      connection.refuseHandshake(
          LocalizedStrings.ServerHandShakeProcessor_0_SERVERS_CURRENT_VERSION_IS_1
              .toLocalizedString(new Object[] {e.getMessage(), Acceptor.VERSION.toString()}),
          REPLY_REFUSED);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      validHandShake = false;
    }

    if (clientVersion != null) {

      if (logger.isDebugEnabled())
        logger.debug("Client version: {}", clientVersion);

      // Read the appropriate handshake
      if (clientVersion.compareTo(Version.GFE_57) >= 0) {
        validHandShake = readGFEHandshake(connection, clientVersion);
      } else {
        connection.refuseHandshake(
            "Unsupported version " + clientVersion + "Server's current version " + Acceptor.VERSION,
            REPLY_REFUSED);
      }
    }

    return validHandShake;
  }

  /**
   * Refuse a received handshake.
   * 
   * @param out the Stream to the waiting greeter.
   * @param message providing details about the refusal reception, mainly for client logging.
   * @throws IOException
   */
  public static void refuse(OutputStream out, String message) throws IOException {
    refuse(out, message, REPLY_REFUSED);
  }

  /**
   * Refuse a received handshake.
   * 
   * @param out the Stream to the waiting greeter.
   * @param message providing details about the refusal reception, mainly for client logging.
   * @param exception providing details about exception occurred.
   * @throws IOException
   */
  public static void refuse(OutputStream out, String message, byte exception) throws IOException {

    HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
    DataOutputStream dos = new DataOutputStream(hdos);
    // Write refused reply
    dos.writeByte(exception);

    // write dummy epType
    dos.writeByte(0);
    // write dummy qSize
    dos.writeInt(0);

    // Write the server's member
    DistributedMember member = InternalDistributedSystem.getAnyInstance().getDistributedMember();
    writeServerMember(member, dos);

    // Write the refusal message
    if (message == null) {
      message = "";
    }
    dos.writeUTF(message);

    // Write dummy delta-propagation property value. This will never be read at
    // receiver because the exception byte above will cause the receiver code
    // throw an exception before the below byte could be read.
    dos.writeBoolean(Boolean.TRUE);

    out.write(hdos.toByteArray());
    out.flush();
  }

  // Keep the writeServerMember/readServerMember compatible with C++ native
  // client
  protected static void writeServerMember(DistributedMember member, DataOutputStream dos)
      throws IOException {

    Version v = Version.CURRENT;
    if (dos instanceof VersionedDataStream) {
      v = ((VersionedDataStream) dos).getVersion();
    }
    HeapDataOutputStream hdos = new HeapDataOutputStream(v);
    DataSerializer.writeObject(member, hdos);
    DataSerializer.writeByteArray(hdos.toByteArray(), dos);
    hdos.close();
  }

  private static boolean readGFEHandshake(ServerConnection connection, Version clientVersion) {
    int handShakeTimeout = connection.getHandShakeTimeout();
    InternalLogWriter securityLogWriter = connection.getSecurityLogWriter();
    try {
      Socket socket = connection.getSocket();
      DistributedSystem system = connection.getDistributedSystem();
      // hitesh:it will set credentials and principals
      HandShake handshake = new HandShake(socket, handShakeTimeout, system, clientVersion,
          connection.getCommunicationMode());
      connection.setHandshake(handshake);
      ClientProxyMembershipID proxyId = handshake.getMembership();
      connection.setProxyId(proxyId);
      // hitesh: it gets principals
      // Hitesh:for older version we should set this
      if (clientVersion.compareTo(Version.GFE_65) < 0
          || connection.getCommunicationMode() == Acceptor.GATEWAY_TO_GATEWAY) {
        long uniqueId = setAuthAttributes(connection);
        connection.setUserAuthId(uniqueId);// for older clients < 6.5
      }
    } catch (SocketTimeoutException timeout) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.ServerHandShakeProcessor_0_HANDSHAKE_REPLY_CODE_TIMEOUT_NOT_RECEIVED_WITH_IN_1_MS,
          new Object[] {connection.getName(), Integer.valueOf(handShakeTimeout)}));
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      return false;
    } catch (EOFException e) {
      // no need to warn client just gave up on this server before we could
      // handshake
      logger.info("{} {}", connection.getName(), e);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      return false;
    } catch (SocketException e) { // no need to warn client just gave up on this
      // server before we could handshake
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
          HandShake.REPLY_EXCEPTION_AUTHENTICATION_REQUIRED);
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
          HandShake.REPLY_EXCEPTION_AUTHENTICATION_FAILED);
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

  public static long setAuthAttributes(ServerConnection connection) throws Exception {
    try {
      logger.debug("setAttributes()");
      Object principal = ((HandShake) connection.getHandshake()).verifyCredentials();

      long uniqueId;
      if (principal instanceof Subject) {
        uniqueId =
            connection.getClientUserAuths(connection.getProxyID()).putSubject((Subject) principal);
      } else {
        // this sets principal in map as well....
        uniqueId = getUniqueId(connection, (Principal) principal);
        connection.setPrincipal((Principal) principal);// TODO:hitesh is this require now ???
      }
      return uniqueId;
    } catch (Exception ex) {
      throw ex;
    }
  }

  public static long getUniqueId(ServerConnection connection, Principal principal)
      throws Exception {
    try {
      InternalLogWriter securityLogWriter = connection.getSecurityLogWriter();
      DistributedSystem system = connection.getDistributedSystem();
      Properties systemProperties = system.getProperties();
      // hitesh:auth callbacks
      String authzFactoryName = systemProperties.getProperty(SECURITY_CLIENT_ACCESSOR);
      String postAuthzFactoryName = systemProperties.getProperty(SECURITY_CLIENT_ACCESSOR_PP);
      AuthorizeRequest authzRequest = null;
      AuthorizeRequestPP postAuthzRequest = null;

      if (authzFactoryName != null && authzFactoryName.length() > 0) {
        if (securityLogWriter.fineEnabled())
          securityLogWriter.fine(connection.getName()
              + ": Setting pre-process authorization callback to: " + authzFactoryName);
        if (principal == null) {
          if (securityLogWriter.warningEnabled()) {
            securityLogWriter.warning(
                LocalizedStrings.ServerHandShakeProcessor_0_AUTHORIZATION_ENABLED_BUT_AUTHENTICATION_CALLBACK_1_RETURNED_WITH_NULL_CREDENTIALS_FOR_PROXYID_2,
                new Object[] {connection.getName(), SECURITY_CLIENT_AUTHENTICATOR,
                    connection.getProxyID()});
          }
        }
        authzRequest = new AuthorizeRequest(authzFactoryName, connection.getProxyID(), principal,
            connection.getCache());
        // connection.setAuthorizeRequest(authzRequest);
      }
      if (postAuthzFactoryName != null && postAuthzFactoryName.length() > 0) {
        if (securityLogWriter.fineEnabled())
          securityLogWriter.fine(connection.getName()
              + ": Setting post-process authorization callback to: " + postAuthzFactoryName);
        if (principal == null) {
          if (securityLogWriter.warningEnabled()) {
            securityLogWriter.warning(
                LocalizedStrings.ServerHandShakeProcessor_0_POSTPROCESS_AUTHORIZATION_ENABLED_BUT_NO_AUTHENTICATION_CALLBACK_2_IS_CONFIGURED,
                new Object[] {connection.getName(), SECURITY_CLIENT_AUTHENTICATOR});
          }
        }
        postAuthzRequest = new AuthorizeRequestPP(postAuthzFactoryName, connection.getProxyID(),
            principal, connection.getCache());
        // connection.setPostAuthorizeRequest(postAuthzRequest);
      }
      return connection.setUserAuthorizeAndPostAuthorizeRequest(authzRequest, postAuthzRequest);
    } catch (Exception ex) {
      throw ex;
    }
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
        if (connection.getCommunicationMode() == Acceptor.GATEWAY_TO_GATEWAY
            && !(clientVersionOrdinal == Version.NOT_SUPPORTED_ORDINAL)) {
          return Acceptor.VERSION;
        } else {
          SocketAddress sa = socket.getRemoteSocketAddress();
          String sInfo = "";
          if (sa != null) {
            sInfo = " Client: " + sa.toString() + ".";
          }
          throw new UnsupportedVersionException(uve.getMessage() + sInfo);
        }
      }

      if (!clientVersion.compatibleWith(Acceptor.VERSION)) {
        throw new IncompatibleVersionException(clientVersion, Acceptor.VERSION);// we can throw this
                                                                                // to restrict
      } // Backward Compatibilty Support to limited no of versions
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
