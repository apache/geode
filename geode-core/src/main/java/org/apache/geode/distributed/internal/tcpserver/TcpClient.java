/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.distributed.internal.tcpserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;
import org.apache.geode.internal.VersionedDataOutputStream;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.security.SecurableComponent;

/**
 * <p>Client for the TcpServer component of the Locator.
 * </p>
 * @since GemFire 5.7
 */
public class TcpClient {

  private static final Logger logger = LogService.getLogger();

  private static final int DEFAULT_REQUEST_TIMEOUT = 60 * 2 * 1000;

  private static Map<InetSocketAddress, Short> serverVersions = new HashMap<InetSocketAddress, Short>();

  private final SocketCreator socketCreator;

  public TcpClient(DistributionConfig distributionConfig) {
    this(SocketCreatorFactory.setDistributionConfig(distributionConfig).getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR));
  }

  /**
   * Constructs a new TcpClient using the default (Locator) SocketCreator.
   * SocketCreatorFactory should be initialized before invoking this method.
   */
  public TcpClient() {
    this(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR));
  }

  /**
   * Constructs a new TcpClient
   * @param socketCreator the SocketCreator to use in communicating with the Locator
   */
  public TcpClient(SocketCreator socketCreator) {
    this.socketCreator = socketCreator;
  }

  /**
   * Stops the Locator running on a given host and port
   */
  public void stop(InetAddress addr, int port) throws java.net.ConnectException {
    try {
      ShutdownRequest request = new ShutdownRequest();
      requestToServer(addr, port, request, DEFAULT_REQUEST_TIMEOUT);
    } catch (java.net.ConnectException ce) {
      // must not be running, rethrow so the caller can handle. 
      // In most cases this Exception should be ignored.
      throw ce;
    } catch (Exception ex) {
      logger.error("TcpClient.stop(): exception connecting to locator " + addr + ":" + port + ": " + ex);
    }
  }

  /**
   * Contacts the Locator running on the given host,
   * and port and gets information about it.  Two <code>String</code>s
   * are returned: the first string is the working directory of the
   * locator and the second string is the product directory of the
   * locator.
   */
  public String[] getInfo(InetAddress addr, int port) {
    try {
      InfoRequest request = new InfoRequest();
      InfoResponse response = (InfoResponse) requestToServer(addr, port, request, DEFAULT_REQUEST_TIMEOUT);
      return response.getInfo();
    } catch (java.net.ConnectException ignore) {
      return null;
    } catch (Exception ex) {
      logger.error("TcpClient.getInfo(): exception connecting to locator " + addr + ":" + port + ": " + ex);
      return null;
    }

  }

  /**
   * Send a request to a Locator and expect a reply
   * @param addr The locator's address
   * @param port The locator's tcp/ip port
   * @param request The request message
   * @param timeout Timeout for sending the message and receiving a reply
   *
   * @return the reply
   *
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public Object requestToServer(InetAddress addr, int port, Object request, int timeout) throws IOException, ClassNotFoundException {
    return requestToServer(addr, port, request, timeout, true);
  }

  /**
   * Send a request to a Locator
   * @param addr The locator's address
   * @param port The locator's tcp/ip port
   * @param request The request message
   * @param timeout Timeout for sending the message and receiving a reply
   * @param replyExpected Whether to wait for a reply
   *
   * @return The reply, or null if no reply is expected
   *
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public Object requestToServer(InetAddress addr, int port, Object request, int timeout, boolean replyExpected) throws IOException, ClassNotFoundException {
    InetSocketAddress ipAddr;
    if (addr == null) {
      ipAddr = new InetSocketAddress(port);
    } else {
      ipAddr = new InetSocketAddress(addr, port); // fix for bug 30810
    }

    long giveupTime = System.currentTimeMillis() + timeout;

    // Get the GemFire version of the TcpServer first, before sending any other request.
    short serverVersion = getServerVersion(ipAddr, timeout).shortValue();

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

    logger.debug("TcpClient sending {} to {}", request, ipAddr);

    Socket sock = socketCreator.connect(ipAddr.getAddress(), ipAddr.getPort(), (int) newTimeout, null, false);
    sock.setSoTimeout((int) newTimeout);
    DataOutputStream out = null;
    try {
      out = new DataOutputStream(sock.getOutputStream());

      if (serverVersion < Version.CURRENT_ORDINAL) {
        out = new VersionedDataOutputStream(out, Version.fromOrdinalNoThrow(serverVersion, false));
      }

      out.writeInt(gossipVersion);
      if (gossipVersion > TcpServer.getOldGossipVersion()) {
        out.writeShort(serverVersion);
      }
      DataSerializer.writeObject(request, out);
      out.flush();

      if (replyExpected) {
        DataInputStream in = new DataInputStream(sock.getInputStream());
        in = new VersionedDataInputStream(in, Version.fromOrdinal(serverVersion, false));
        try {
          Object response = DataSerializer.readObject(in);
          logger.debug("received response: {}", response);
          return response;
        } catch (EOFException ex) {
          throw new EOFException("Locator at " + ipAddr + " did not respond. This is normal if the locator was shutdown. If it wasn't check its log for exceptions.");
        }
      } else {
        return null;
      }
    } catch (UnsupportedVersionException ex) {
      if (logger.isDebugEnabled()) {
        logger.debug("Remote TcpServer version: " + serverVersion + " is higher than local version: " + Version.CURRENT_ORDINAL + ". This is never expected as remoteVersion");
      }
      return null;
    } finally {
      try {
        if (replyExpected) {
          // Since we've read a response we know that the Locator is finished
          // with the socket and is closing it.  Aborting the connection by
          // setting SO_LINGER to zero will clean up the TIME_WAIT socket on
          // the locator's machine.
          if (!sock.isClosed() && !socketCreator.useSSL()) {
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

  private Short getServerVersion(InetSocketAddress ipAddr, int timeout) throws IOException, ClassNotFoundException {

    int gossipVersion = TcpServer.getCurrentGossipVersion();
    Short serverVersion = null;

    // Get GemFire version of TcpServer first, before sending any other request.
    synchronized (serverVersions) {
      serverVersion = serverVersions.get(ipAddr);
    }
    if (serverVersion != null) {
      return serverVersion;
    }

    gossipVersion = TcpServer.getOldGossipVersion();

    Socket sock = null;
    try {
      sock = socketCreator.connect(ipAddr.getAddress(), ipAddr.getPort(), timeout, null, false);
      sock.setSoTimeout(timeout);
    } catch (SSLHandshakeException e) {
      throw new LocatorCancelException("Unrecognisable response received");
    }

    try {
      DataOutputStream out = new DataOutputStream(sock.getOutputStream());
      out = new VersionedDataOutputStream(out, Version.GFE_57);

      out.writeInt(gossipVersion);

      VersionRequest verRequest = new VersionRequest();
      DataSerializer.writeObject(verRequest, out);
      out.flush();

      InputStream inputStream = sock.getInputStream();
      DataInputStream in = new DataInputStream(inputStream);
      in = new VersionedDataInputStream(in, Version.GFE_57);
      try {
        Object readObject = DataSerializer.readObject(in);
        if (!(readObject instanceof VersionResponse)) {
          throw new LocatorCancelException("Unrecognisable response received");
          //          throw new IOException("Unexpected response received from locator");
        }
        VersionResponse response = (VersionResponse) readObject;
        if (response != null) {
          serverVersion = Short.valueOf(response.getVersionOrdinal());
          synchronized (serverVersions) {
            serverVersions.put(ipAddr, serverVersion);
          }
          return serverVersion;
        }
      } catch (EOFException ex) {
        // old locators will not recognize the version request and will close the connection
      }
    } finally {
      try {
          sock.setSoLinger(true, 0); // initiate an abort on close to shut down the server's socket
          sock.close();
      } catch (Exception e) {
        logger.error("Error closing socket ", e);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Locator " + ipAddr + " did not respond to a request for its version.  I will assume it is using v5.7 for safety.");
    }
    synchronized (serverVersions) {
      serverVersions.put(ipAddr, Version.GFE_57.ordinal());
    }
    return Short.valueOf(Version.GFE_57.ordinal());
  }

  /**
   * Clear static class information concerning Locators.
   * This is used in unit tests.  It will force TcpClient to
   * send version-request messages to locators to reestablish
   * knowledge of their communication protocols.
   */
  public static void clearStaticData() {
    synchronized (serverVersions) {
      serverVersions.clear();
    }
  }

}
