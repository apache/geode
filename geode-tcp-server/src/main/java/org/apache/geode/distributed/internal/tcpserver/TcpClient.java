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

package org.apache.geode.distributed.internal.tcpserver;

import static java.util.Objects.requireNonNull;
import static org.apache.geode.internal.lang.utils.JavaWorkarounds.computeIfAbsent;
import static org.apache.geode.internal.lang.utils.function.Checked.rethrowFunction;
import static org.apache.geode.internal.serialization.Versioning.getKnownVersionOrDefault;
import static org.apache.geode.internal.serialization.Versioning.getVersion;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Client for the TcpServer component of the Locator.
 *
 * @see TcpServer#TcpServer(int, InetAddress, TcpHandler, String, ProtocolChecker,
 *      LongSupplier, Supplier, TcpSocketCreator, ObjectSerializer, ObjectDeserializer, String,
 *      String)
 */
public class TcpClient {

  private static final Logger logger = LogService.getLogger();

  private static final int DEFAULT_REQUEST_TIMEOUT = 60 * 2 * 1000;

  private final ConcurrentMap<HostAndPort, Short> serverVersions = new ConcurrentHashMap<>();

  private final TcpSocketCreator socketCreator;
  private final ObjectSerializer objectSerializer;
  private final ObjectDeserializer objectDeserializer;
  private final TcpSocketFactory socketFactory;

  /**
   * Constructs a new TcpClient
   *
   * @param socketCreator the SocketCreator to use in communicating with the Locator
   * @param objectSerializer serializer for messages sent to the TcpServer
   * @param objectDeserializer deserializer for responses from the TcpServer
   */
  public TcpClient(TcpSocketCreator socketCreator, final ObjectSerializer objectSerializer,
      final ObjectDeserializer objectDeserializer, TcpSocketFactory socketFactory) {
    this.socketCreator = socketCreator;
    this.objectSerializer = objectSerializer;
    this.objectDeserializer = objectDeserializer;
    this.socketFactory = socketFactory;
  }

  /**
   * Stops the TcpServer running on a given host and port
   */
  public void stop(HostAndPort hostAndPort) throws java.net.ConnectException {
    try {
      ShutdownRequest request = new ShutdownRequest();
      requestToServer(hostAndPort, request, DEFAULT_REQUEST_TIMEOUT, true);
    } catch (java.net.ConnectException ce) {
      // must not be running, rethrow so the caller can handle.
      // In most cases this Exception should be ignored.
      throw ce;
    } catch (Exception ex) {
      logger.error(
          "TcpClient.stop(): exception connecting to locator " + hostAndPort + ex);
    }
  }

  /**
   * Contacts the Locator running on the given host, and port and gets information about it. Two
   * <code>String</code>s are returned: the first string is the working directory of the locator
   * and the second string is the product directory of the locator.
   *
   * @deprecated this was created for the deprecated Admin API
   */
  @Deprecated
  public String[] getInfo(HostAndPort hostAndPort) {
    try {
      InfoRequest request = new InfoRequest();
      InfoResponse response =
          (InfoResponse) requireNonNull(
              requestToServer(hostAndPort, request, DEFAULT_REQUEST_TIMEOUT, true));
      return response.getInfo();
    } catch (java.net.ConnectException ignore) {
      return null;
    } catch (Exception ex) {
      logger.error(
          "TcpClient.getInfo(): exception connecting to locator " + hostAndPort + ": " + ex);
      return null;
    }
  }

  /**
   * Send a request to a Locator
   *
   * @param address The locator's address
   * @param request The request message
   * @param timeout Timeout for sending the message and receiving a reply
   * @param replyExpected Whether to wait for a reply
   * @return The reply, or null if no reply is expected. This may also return a null
   *         if we're unable to form a connection to the TcpServer before the given timeout elapses
   * @throws ClassNotFoundException if the deserializer throws this exception
   * @throws IOException if there is a problem interacting with the server
   */
  public @Nullable Object requestToServer(final @NotNull HostAndPort address,
      @NotNull Object request, final int timeout, final boolean replyExpected)
      throws IOException, ClassNotFoundException {
    final long expirationTime = System.currentTimeMillis() + timeout;

    // Get the GemFire version of the TcpServer first, before sending any other request.
    final short serverVersionShort = getServerVersion(address, timeout);
    KnownVersion serverVersion = getKnownVersionOrDefault(getVersion(serverVersionShort), null);
    final String debugVersionMessage;
    if (serverVersion == null) {
      serverVersion = KnownVersion.CURRENT;
      debugVersionMessage =
          "Remote TcpServer version: " + serverVersionShort + " is higher than local version: "
              + KnownVersion.CURRENT_ORDINAL + ". This is never expected as remoteVersion";
    } else {
      debugVersionMessage = null;
    }

    final int newTimeout = (int) (expirationTime - System.currentTimeMillis());
    if (newTimeout <= 0) {
      return null;
    }

    return requestToServer(address, request, newTimeout, replyExpected, serverVersionShort,
        serverVersion, debugVersionMessage);
  }

  Object requestToServer(final @NotNull HostAndPort address, final @NotNull Object request,
      final int timeout, final boolean replyExpected, final short serverVersionOrdinal,
      final @NotNull KnownVersion serverVersion, final @Nullable String debugVersionMessage)
      throws IOException, ClassNotFoundException {
    logger.debug("TcpClient sending {} to {}", request, address);

    final Socket sock = socketCreator.forCluster().connect(address, timeout, null, socketFactory);
    try {
      sock.setSoTimeout(timeout);
      try (final DataOutputStream out = createDataOutputStream(sock, serverVersion)) {
        sendRequest(out, serverVersionOrdinal, request);
        if (replyExpected) {
          try (final DataInputStream in = createDataInputStream(sock, serverVersion)) {
            if (debugVersionMessage != null && logger.isDebugEnabled()) {
              logger.debug(debugVersionMessage);
            }
            return receiveResponse(in, address);
          }
        } else {
          return null;
        }
      }
    } finally {
      try {
        if (replyExpected) {
          // Since we've read a response we know that the Locator is finished
          // with the socket and is closing it. Aborting the connection by
          // setting SO_LINGER to zero will clean up the TIME_WAIT socket on
          // the locator's machine.
          if (!sock.isClosed() && !socketCreator.forCluster().useSSL()) {
            sock.setSoLinger(true, 0);
          }
        }
        sock.close();
      } catch (Exception e) {
        logger.error("Error closing socket ", e);
      }
    }
  }

  @Nullable
  Object receiveResponse(final @NotNull DataInputStream in, final @NotNull HostAndPort address)
      throws IOException, ClassNotFoundException {
    try {
      final Object response = objectDeserializer.readObject(in);
      logger.debug("received response: {}", response);
      return response;
    } catch (EOFException ex) {
      logger.debug("requestToServer EOFException ", ex);
      throw createEOFException(address, ex);
    }
  }

  static @NotNull EOFException createEOFException(final @NotNull HostAndPort address,
      final @NotNull Exception cause) {
    final EOFException exception = new EOFException("Locator at " + address
        + " did not respond. This is normal if the locator was shutdown. If it wasn't check its log for exceptions.");
    exception.initCause(cause);
    return exception;
  }

  static @NotNull DataInputStream createDataInputStream(final @NotNull Socket sock,
      final @NotNull KnownVersion version)
      throws IOException {
    DataInputStream in = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
    if (version.isOlderThan(KnownVersion.CURRENT)) {
      in = new VersionedDataInputStream(in, version);
    }
    return in;
  }

  static @NotNull DataOutputStream createDataOutputStream(final @NotNull Socket socket,
      final @NotNull KnownVersion version) throws IOException {
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    if (version.isOlderThan(KnownVersion.CURRENT)) {
      out = new VersionedDataOutputStream(out, version);
    }
    return out;
  }

  @SuppressWarnings("RedundantThrows") // Thrown through rethrowFunction
  short getServerVersion(final @NotNull HostAndPort address, final int timeout) throws IOException {
    return computeIfAbsent(serverVersions, address, rethrowFunction((k) -> {
      final Socket socket =
          socketCreator.forCluster().connect(address, timeout, null, socketFactory);
      try {
        socket.setSoTimeout(timeout);
        return getServerVersion(socket);
      } finally {
        resetSocketAndLogExceptions(socket);
      }
    }));
  }

  @NotNull
  Short getServerVersion(final @NotNull Socket socket)
      throws IOException {
    try (
        final DataInputStream in = createDataInputStream(socket, KnownVersion.OLDEST);
        final DataOutputStream out = createDataOutputStream(socket, KnownVersion.OLDEST)) {
      return getServerVersion(in, out);
    }
  }

  @NotNull
  Short getServerVersion(final @NotNull DataInputStream in, final @NotNull DataOutputStream out)
      throws IOException {
    sendRequest(out, KnownVersion.OLDEST.ordinal(), new VersionRequest());
    try {
      assertNotSslAlert(in);
      final VersionResponse versionResponse = objectDeserializer.readObject(in);
      return versionResponse.getVersionOrdinal();
    } catch (EOFException ignored) {
      // old locators will not recognize the version request and will close the connection
      return KnownVersion.OLDEST.ordinal();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Server version response invalid.", e);
    }
  }

  /**
   * If this client is not expecting to use SSL then SSL messages won't be decoded by the SSL layer,
   * which will result in SSL messages being decoded here. In that even the server side will have
   * sent us an alert message that we didn't send an SSL message to it.
   *
   * @param in stream to check for SSL alert message
   * @throws IOException if stream appears to start with SSL alert message.
   */
  static void assertNotSslAlert(final @NotNull DataInputStream in) throws IOException {
    in.mark(1);
    try {
      if (in.read() == 0x15) {
        throw new SSLHandshakeException("Server expecting SSL handshake.");
      }
    } finally {
      in.reset();
    }
  }

  /**
   * Writes and flushes the header and request object to the server.
   */
  void sendRequest(final @NotNull DataOutputStream out, final short ordinalVersion,
      final @NotNull Object request) throws IOException {
    out.writeInt(TcpServer.GOSSIPVERSION);
    out.writeShort(ordinalVersion);
    objectSerializer.writeObject(request, out);
    out.flush();
  }

  /**
   * Forces a socket reset to avoid TIME_WAIT state.
   *
   * @param socket to reset
   */
  static void resetSocketAndLogExceptions(final @NotNull Socket socket) {
    if (!socket.isClosed()) {
      try {
        // initiate an abort on close to shut down the server's socket
        socket.setSoLinger(true, 0);
      } catch (Exception e) {
        logger.error("Error aborting socket ", e);
      }
      try {
        socket.close();
      } catch (Exception e) {
        logger.error("Error closing socket ", e);
      }
    }
  }
}
