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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;
import org.apache.geode.internal.serialization.UnsupportedSerializationVersionException;
import org.apache.geode.internal.serialization.Version;
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

  private final Map<HostAndPort, Short> serverVersions =
      new HashMap<>();

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
  public void stop(HostAndPort addr) throws java.net.ConnectException {
    try {
      ShutdownRequest request = new ShutdownRequest();
      requestToServer(addr, request, DEFAULT_REQUEST_TIMEOUT);
    } catch (java.net.ConnectException ce) {
      // must not be running, rethrow so the caller can handle.
      // In most cases this Exception should be ignored.
      throw ce;
    } catch (Exception ex) {
      logger.error(
          "TcpClient.stop(): exception connecting to locator " + addr + ex);
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
  public String[] getInfo(HostAndPort addr) {
    try {
      InfoRequest request = new InfoRequest();
      InfoResponse response =
          (InfoResponse) requestToServer(addr, request, DEFAULT_REQUEST_TIMEOUT);
      return response.getInfo();
    } catch (java.net.ConnectException ignore) {
      return null;
    } catch (Exception ex) {
      logger.error(
          "TcpClient.getInfo(): exception connecting to locator " + addr + ": " + ex);
      return null;
    }

  }

  /**
   * Send a request to a Locator and expect a reply
   *
   * @param addr The locator's address
   * @param request The request message
   * @param timeout Timeout for sending the message and receiving a reply
   * @return the reply. This may return a null
   *         if we're unable to form a connection to the TcpServer before the given timeout elapses
   */
  public Object requestToServer(HostAndPort addr, Object request, int timeout)
      throws IOException, ClassNotFoundException {

    return requestToServer(addr, request, timeout, true);
  }

  /**
   * Send a request to a Locator
   *
   * @param addr The locator's address
   * @param request The request message
   * @param timeout Timeout for sending the message and receiving a reply
   * @param replyExpected Whether to wait for a reply
   * @return The reply, or null if no reply is expected. This may also return a null
   *         if we're unable to form a connection to the TcpServer before the given timeout elapses
   * @throws ClassNotFoundException if the deserializer throws this exception
   * @throws IOException if there is a problem interacting with the server
   */
  public Object requestToServer(HostAndPort addr, Object request, int timeout,
      boolean replyExpected) throws IOException, ClassNotFoundException {
    long giveupTime = System.currentTimeMillis() + timeout;

    // Get the GemFire version of the TcpServer first, before sending any other request.
    short serverVersion = getServerVersion(addr, timeout);

    if (serverVersion > Version.CURRENT_ORDINAL) {
      serverVersion = Version.CURRENT_ORDINAL;
    }

    // establish the old GossipVersion for the server
    int gossipVersion = TcpServer.getCurrentGossipVersion();

    if (Version.GFE_71.compareTo(serverVersion) > 0) {
      gossipVersion = TcpServer.getOldGossipVersion();
    }

    long newTimeout = giveupTime - System.currentTimeMillis();
    if (newTimeout <= 0) {
      return null;
    }

    logger.debug("TcpClient sending {} to {}", request, addr);

    Socket sock =
        socketCreator.forCluster().connect(addr, (int) newTimeout, null, socketFactory);
    sock.setSoTimeout((int) newTimeout);
    DataOutputStream out = null;
    try {

      out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));

      if (serverVersion < Version.CURRENT_ORDINAL) {
        out = new VersionedDataOutputStream(out, Version.fromOrdinalNoThrow(serverVersion, false));
      }

      out.writeInt(gossipVersion);
      if (gossipVersion > TcpServer.getOldGossipVersion()) {
        out.writeShort(serverVersion);
      }

      objectSerializer.writeObject(request, out);
      out.flush();

      if (replyExpected) {
        DataInputStream in = new DataInputStream(sock.getInputStream());
        in = new VersionedDataInputStream(in, Version.fromOrdinal(serverVersion));
        try {
          Object response = objectDeserializer.readObject(in);
          logger.debug("received response: {}", response);
          return response;
        } catch (EOFException ex) {
          logger.debug("requestToServer EOFException ", ex);
          EOFException eof = new EOFException("Locator at " + addr
              + " did not respond. This is normal if the locator was shutdown. If it wasn't check its log for exceptions.");
          eof.initCause(ex);
          throw eof;
        }
      } else {
        return null;
      }
    } catch (UnsupportedSerializationVersionException ex) {
      if (logger.isDebugEnabled()) {
        logger
            .debug("Remote TcpServer version: " + serverVersion + " is higher than local version: "
                + Version.CURRENT_ORDINAL + ". This is never expected as remoteVersion");
      }
      return null;
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
      if (out != null) {
        out.close();
      }
    }
  }

  private Short getServerVersion(HostAndPort addr, int timeout)
      throws IOException, ClassNotFoundException {

    int gossipVersion;
    Short serverVersion;
    Socket sock;

    // Get GemFire version of TcpServer first, before sending any other request.
    synchronized (serverVersions) {
      serverVersion = serverVersions.get(addr);
    }

    if (serverVersion != null) {
      return serverVersion;
    }

    gossipVersion = TcpServer.getOldGossipVersion();

    try {
      sock = socketCreator.forCluster().connect(addr, timeout, null, socketFactory);
      sock.setSoTimeout(timeout);
    } catch (SSLHandshakeException e) {
      if ((e.getCause() instanceof EOFException)
          && (e.getCause().getMessage().contains("SSL peer shut down incorrectly"))) {
        throw new IOException("Remote host terminated the handshake", e);
      } else {
        throw new IllegalStateException("Unable to form SSL connection", e);
      }
    } catch (SSLException e) {
      throw new IllegalStateException("Unable to form SSL connection", e);
    }

    try {
      OutputStream outputStream = new BufferedOutputStream(sock.getOutputStream());
      DataOutputStream out =
          new VersionedDataOutputStream(new DataOutputStream(outputStream), Version.GFE_57);

      out.writeInt(gossipVersion);

      VersionRequest verRequest = new VersionRequest();
      objectSerializer.writeObject(verRequest, out);
      out.flush();

      InputStream inputStream = sock.getInputStream();
      DataInputStream in = new DataInputStream(inputStream);
      in = new VersionedDataInputStream(in, Version.GFE_57);
      try {
        Object readObject = objectDeserializer.readObject(in);
        if (!(readObject instanceof VersionResponse)) {
          throw new IllegalThreadStateException(
              "Server version response invalid: "
                  + "This could be the result of trying to connect a non-SSL-enabled client to an SSL-enabled locator.");
        }

        VersionResponse response = (VersionResponse) readObject;
        serverVersion = response.getVersionOrdinal();
        synchronized (serverVersions) {
          serverVersions.put(addr, serverVersion);
        }

        return serverVersion;

      } catch (EOFException ex) {
        // old locators will not recognize the version request and will close the connection
      }
    } finally {
      if (!sock.isClosed()) {
        try {
          sock.setSoLinger(true, 0); // initiate an abort on close to shut down the server's socket
        } catch (Exception e) {
          logger.error("Error aborting socket ", e);
        }
        try {
          sock.close();
        } catch (Exception e) {
          logger.error("Error closing socket ", e);
        }
      }
    }
    synchronized (serverVersions) {
      serverVersions.put(addr, Version.GFE_57.ordinal());
    }
    return Version.GFE_57.ordinal();
  }
}
