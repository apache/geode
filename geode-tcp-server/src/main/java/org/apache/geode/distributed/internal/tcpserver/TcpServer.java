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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import javax.net.ssl.SSLException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;
import org.apache.geode.internal.serialization.UnsupportedSerializationVersionException;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * TcpServer listens on a port and delegates requests to one or more request handlers. Use
 * TcpClient to send messages to a TcpServer. Messages should all implement DataSerializableFixedID.
 * <p>
 * TcpServer accepts a connection, reads one request, passes the request to a handler,
 * sends the reply and then closes the connection.
 * <p>
 * TcpServer is used in the Geode Locator service.
 */
public class TcpServer {

  /**
   * GOSSIPVERSION is a remnant of the pre-open-source TcpServer. It was used to designate
   * the on-wire protocol for TcpServer communications prior to the introduction of Geode's
   * Version class. It should not be changed and exists for backward-compatibility.
   */
  public static final int GOSSIPVERSION = 1002;

  /**
   * Version 1001 was the on-wire protocol version prior to the introduction of the use of
   * Geode's Version class to designate the on-wire protocol.
   */
  public static final int OLDGOSSIPVERSION = 1001;

  @MutableForTesting("The map used here is mutable, because some tests modify it")
  private static final Map<Integer, Short> GOSSIP_TO_GEMFIRE_VERSION_MAP =
      createGossipToVersionMap();
  public static final int GOSSIP_BYTE = 0;
  private static final String P2P_BACKLOG_PROPERTY_NAME = "p2p.backlog";

  public static final long SHUTDOWN_WAIT_TIME = 60 * 1000;

  private static final Logger logger = LogService.getLogger();

  // no longer static so that tests can test this system property
  private final int readTimeout;
  private final int backlogLimit;
  private final ProtocolChecker protocolChecker;
  private final ObjectDeserializer objectDeserializer;
  private final ObjectSerializer objectSerializer;

  private int port;
  private ServerSocket srv_sock = null;
  private InetAddress bind_address;
  private volatile boolean shuttingDown = false;
  private final TcpHandler handler;

  private ExecutorService executor;
  private final Supplier<ExecutorService> executorServiceSupplier;

  private final String threadName;
  private volatile Thread serverThread;

  protected TcpSocketCreator socketCreator;

  private final LongSupplier nanoTimeSupplier;


  /*
   * Old on-wire protocol map. This should be removed in a release that breaks all backward
   * compatibility since it has been replaced with Geode's Version class.
   */
  private static Map<Integer, Short> createGossipToVersionMap() {
    HashMap<Integer, Short> map = new HashMap<>();
    map.put(GOSSIPVERSION, Version.GFE_71.ordinal());
    map.put(OLDGOSSIPVERSION, Version.GFE_57.ordinal());
    return map;
  }

  /**
   * The constructor for TcpServer
   *
   * @param port The port to listen on
   * @param bind_address The bind-address to use (may be null)
   * @param handler The TcpHandler that will process messages
   * @param threadName The name to use in the listening thread
   * @param protocolChecker A cut point for inserting a message handler with different serialization
   * @param nanoTimeSupplier A time supplier
   * @param executorServiceSupplier A provider of the executor to be used by handlers
   * @param socketCreator The socket-creator that TcpServer should use. If null a default socket
   *        creator is constructed
   * @param objectSerializer The serializer
   * @param objectDeserializer The deserializer
   * @param readTimeoutPropertyName A system property name used to look up read timeout millis
   * @param backlogLimitPropertyName A system property name used to establish the server socket
   *        backlog
   * @see #start()
   */
  public TcpServer(int port, InetAddress bind_address, TcpHandler handler,
      String threadName, ProtocolChecker protocolChecker,
      final LongSupplier nanoTimeSupplier,
      final Supplier<ExecutorService> executorServiceSupplier,
      final TcpSocketCreator socketCreator,
      final ObjectSerializer objectSerializer,
      final ObjectDeserializer objectDeserializer,
      final String readTimeoutPropertyName, final String backlogLimitPropertyName) {
    this.port = port;
    this.bind_address = bind_address;
    this.handler = handler;
    this.protocolChecker = protocolChecker;
    this.executorServiceSupplier = executorServiceSupplier;
    this.executor = executorServiceSupplier.get();
    this.threadName = threadName;
    this.nanoTimeSupplier = nanoTimeSupplier;
    if (socketCreator == null) {
      this.socketCreator = new TcpSocketCreatorImpl();
    } else {
      this.socketCreator = socketCreator;
    }
    this.objectSerializer = objectSerializer;
    this.objectDeserializer = objectDeserializer;
    readTimeout = Integer.getInteger(readTimeoutPropertyName, 60 * 1000);
    final int p2pBacklog = Integer.getInteger(P2P_BACKLOG_PROPERTY_NAME, 1000);
    backlogLimit = Integer.getInteger(backlogLimitPropertyName, p2pBacklog);
  }

  /**
   * This method is used during a Geode auto-reconnect to restart the server-socket thread
   */
  public void restarting() throws IOException {
    this.shuttingDown = false;
    startServerThread();
    if (this.executor == null || this.executor.isShutdown()) {
      this.executor = executorServiceSupplier.get();
    }
    logger.info("TcpServer@" + System.identityHashCode(this)
        + " restarting: completed.  Server thread=" + this.serverThread + '@'
        + System.identityHashCode(this.serverThread) + ";alive=" + this.serverThread.isAlive());
  }

  /**
   * After constructing a TcpServer use this method to start its server-socket listening thread.
   * A TcpServer should be stopped via a ShutdownRequest made through a TcpClient.
   *
   * @see TcpClient#stop(HostAndPort)
   */
  public void start() throws IOException {
    this.shuttingDown = false;
    startServerThread();
    handler.init(this);
  }

  private void startServerThread() throws IOException {
    initializeServerSocket();
    if (serverThread == null || !serverThread.isAlive()) {
      serverThread = new LoggingThread(threadName, this::run);
      serverThread.start();
    }
  }

  private void initializeServerSocket() throws IOException {
    if (srv_sock == null || srv_sock.isClosed()) {
      if (bind_address == null) {
        srv_sock = socketCreator.forCluster().createServerSocket(port, backlogLimit);
        bind_address = srv_sock.getInetAddress();
      } else {
        srv_sock = socketCreator.forCluster().createServerSocket(port, backlogLimit, bind_address);
      }
      // GEODE-4176 - set the port from a wild-card bind so that handlers know the correct value

      if (this.port <= 0) {
        this.port = srv_sock.getLocalPort();
      }
      if (logger.isInfoEnabled()) {
        logger.info("Locator was created at " + new Date());
        logger.info("Listening on port " + getPort() + " bound on address " + bind_address);
      }
      srv_sock.setReuseAddress(true);
    }
  }

  /**
   * Wait on the server-socket thread using {@link Thread#join(long)}
   *
   * @param millis how long to wait
   */
  public void join(long millis) throws InterruptedException {
    if (isAlive()) {
      serverThread.join(millis);
    }
  }

  /**
   * Wait on the server-socket thread using {@link Thread#join()}
   */
  public void join() throws InterruptedException {
    if (isAlive()) {
      serverThread.join();
    }
  }

  /**
   * Check to see if the server-socket thread is alive
   */
  public boolean isAlive() {
    return serverThread != null && serverThread.isAlive();
  }

  /**
   * Check to see if we've requested that the server-socket thread has been requested
   * to shut down
   */
  public boolean isShuttingDown() {
    return this.shuttingDown;
  }

  /**
   * Returns the server-socket's local socket address
   *
   * @see ServerSocket#getLocalSocketAddress()
   */
  public SocketAddress getSocketAddress() {
    return srv_sock.getLocalSocketAddress();
  }

  /**
   * Returns the value of the bound port. If the server was initialized with a port of 0 indicating
   * that any ephemeral port should be used, this method will return the actual bound port.
   *
   * @return the locator's tcp/ip port. This will be zero if the TcpServer hasn't been started.
   */
  public int getPort() {
    return port;
  }

  protected void run() {
    Socket sock = null;

    while (!shuttingDown) {
      if (srv_sock.isClosed()) {
        shuttingDown = true;
        break;
      }
      try {
        try {
          sock = srv_sock.accept();
        } catch (SSLException ex) {
          // SW: This is the case when there is a problem in locator
          // SSL configuration, so need to exit otherwise goes into an
          // infinite loop just filling the logs
          logger.error("Locator stopping due to SSL configuration problem.", ex);
          shuttingDown = true;
          continue;
        }

        processRequest(sock);
      } catch (Exception ex) {
        if (!shuttingDown) {
          logger.error("exception=", ex);
        }
        continue;
      }
    }

    if (!srv_sock.isClosed()) {
      try {
        srv_sock.close();
      } catch (java.io.IOException ex) {
        logger.warn("exception closing server socket during shutdown", ex);
      }
    }

    if (shuttingDown) {
      logger.info("locator shutting down");
      executor.shutdown();
      try {
        executor.awaitTermination(SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      handler.shutDown();
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  /**
   * fix for bug 33711 - client requests are spun off to another thread for processing. Requests are
   * synchronized in processGossip.
   */
  private void processRequest(final Socket socket) {
    executor.execute(() -> {

      final long startTime = nanoTimeSupplier.getAsLong();
      DataInputStream input = null;
      try {
        socket.setSoTimeout(readTimeout);
        socketCreator.forCluster().handshakeIfSocketIsSSL(socket, readTimeout);

        try {
          input = new DataInputStream(socket.getInputStream());
        } catch (StreamCorruptedException e) {
          // Some garbage can be left on the socket stream
          // if a peer disappears at exactly the wrong moment.
          logger.debug("Discarding illegal request from "
              + (socket.getInetAddress().getHostAddress() + ":" + socket.getPort()), e);
          return;
        }
        // read the first byte & check for an improperly configured client pool trying
        // to contact a cache server
        int firstByte = input.readUnsignedByte();

        boolean handled = protocolChecker.checkProtocol(socket, input, firstByte);

        if (!handled) {
          if (firstByte == GOSSIP_BYTE) {
            processOneConnection(socket, startTime, input);
          } else {
            rejectUnknownProtocolConnection(socket, firstByte);
          }
        }
      } catch (EOFException | SocketException ignore) {
        // client went away - ignore
      } catch (SocketTimeoutException ex) {
        String sender = null;
        if (socket != null) {
          sender = socket.getInetAddress().getHostAddress();
        }
        // Do not want the full stack trace to fill up the logs
        logger.info("Exception in processing request from " + sender + ": " + ex.getMessage());
      } catch (ClassNotFoundException ex) {
        String sender = null;
        if (socket != null) {
          sender = socket.getInetAddress().getHostAddress();
        }
        logger.info("Unable to process request from " + sender + " exception=" + ex.getMessage());
      } catch (Exception ex) {
        String sender = null;
        if (socket != null) {
          sender = socket.getInetAddress().getHostAddress();
        }
        if (ex instanceof IOException) {
          // IOException could be caused by a client failure. Don't
          // log with severe.
          if (!socket.isClosed()) {
            logger.info("Exception in processing request from " + sender, ex);
          }
        } else {
          logger.fatal("Exception in processing request from " + sender, ex);
        }

      } catch (Throwable ex) {
        String sender = null;
        if (socket != null) {
          sender = socket.getInetAddress().getHostAddress();
        }
        try {
          logger.fatal("Exception in processing request from " + sender, ex);
        } catch (Throwable t) {
          t.printStackTrace();
        }
      } finally {
        try {
          socket.close();
        } catch (IOException ignore) {
          // ignore
        }
      }
    });
  }

  private void processOneConnection(Socket socket, final long startTime, DataInputStream input)
      throws IOException, UnsupportedSerializationVersionException, ClassNotFoundException {
    // At this point we've read the leading byte of the gossip version and found it to be 0,
    // continue reading the next three bytes
    int gossipVersion = 0;
    for (int i = 0; i < 3; i++) {
      gossipVersion = (gossipVersion << 8) + (0xff & input.readUnsignedByte());
    }

    Object request;
    Object response;
    short versionOrdinal;
    if (gossipVersion <= getCurrentGossipVersion()
        && GOSSIP_TO_GEMFIRE_VERSION_MAP.containsKey(gossipVersion)) {
      // Create a versioned stream to remember sender's GemFire version
      versionOrdinal = (short) GOSSIP_TO_GEMFIRE_VERSION_MAP.get(gossipVersion);

      if (Version.GFE_71.compareTo(versionOrdinal) <= 0) {
        // Recent versions of TcpClient will send the version ordinal
        versionOrdinal = input.readShort();
      }

      if (logger.isDebugEnabled() && versionOrdinal != Version.CURRENT_ORDINAL) {
        logger.debug("Locator reading request from " + socket.getInetAddress() + " with version "
            + Version.fromOrdinal(versionOrdinal));
      }
      input = new VersionedDataInputStream(input, Version.fromOrdinal(versionOrdinal));
      request = objectDeserializer.readObject(input);
      if (logger.isDebugEnabled()) {
        logger.debug("Locator received request " + request + " from " + socket.getInetAddress());
      }
      if (request instanceof ShutdownRequest) {
        shuttingDown = true;
        // Don't call shutdown from within the worker thread, see java bug #6576792.
        // Closing the socket will cause our acceptor thread to shutdown the executor
        srv_sock.close();
        response = new ShutdownResponse();
      } else if (request instanceof VersionRequest) {
        response = handleVersionRequest(request);
      } else {
        response = handler.processRequest(request);
      }

      handler.endRequest(request, startTime);

      final long startTime2 = nanoTimeSupplier.getAsLong();
      if (response != null) {
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        if (versionOrdinal != Version.CURRENT_ORDINAL) {
          output =
              new VersionedDataOutputStream(output, Version.fromOrdinal(versionOrdinal));
        }
        objectSerializer.writeObject(response, output);
        output.flush();
      }

      handler.endResponse(request, startTime2);
    } else {
      // Close the socket. We can not accept requests from a newer version
      rejectUnknownProtocolConnection(socket, gossipVersion);
    }
  }

  private void rejectUnknownProtocolConnection(Socket socket, int gossipVersion) {
    try {
      socket.getOutputStream().write("unknown protocol version".getBytes());
      socket.getOutputStream().flush();
      socket.close();
    } catch (IOException e) {
      logger
          .debug("exception in sending reply to process using unknown protocol " + gossipVersion,
              e);
    }
  }



  protected Object handleVersionRequest(Object request) {
    VersionResponse response = new VersionResponse();
    response.setVersionOrdinal(Version.CURRENT_ORDINAL);
    return response;
  }

  public static int getCurrentGossipVersion() {
    return GOSSIPVERSION;
  }

  public static int getOldGossipVersion() {
    return OLDGOSSIPVERSION;
  }

}
