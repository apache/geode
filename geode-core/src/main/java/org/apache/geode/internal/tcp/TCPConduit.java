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
package org.apache.geode.internal.tcp;

import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_SOCKET_BUFFER_SIZE;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_SOCKET_LEASE_TIME;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.alerting.internal.spi.AlertingAction;
import org.apache.geode.alerting.internal.spi.AlertingIOException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.distributed.internal.direct.DirectChannel;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberShunnedException;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.net.BufferPool;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * <p>
 * TCPConduit manages a server socket and a collection of connections to other systems. Connections
 * are identified by DistributedMember IDs. These types of messages are currently supported:
 *
 * <p>
 * DistributionMessage - message is delivered to the server's ServerDelegate
 *
 * <p>
 * In the current implementation, ServerDelegate is the DirectChannel used by the GemFire
 * DistributionManager to send and receive messages.
 *
 * <p>
 * If the ServerDelegate is null, DistributionMessages are ignored by the TCPConduit.
 *
 * @since GemFire 2.0
 */

public class TCPConduit implements Runnable {
  private static final Logger logger = LogService.getLogger();

  /**
   * max amount of time (ms) to wait for listener threads to stop
   */
  @MakeNotStatic
  private static int LISTENER_CLOSE_TIMEOUT;

  /**
   * BACKLOG is the "accept" backlog configuration parameter for the conduits server socket.
   * In most operating systems this is limited to 128 by default and you must change
   * the OS setting somaxconn to go beyond that limit. Note that setting this too high
   * can have ramifications when disconnecting from the distributed system if a thread
   * is trying to connect to another member that is not accepting connections quickly
   * enough. Setting it too low can also have adverse effects because backlog overflows
   * aren't handled well by most tcp/ip implementations, causing connect timeouts instead
   * of expected ServerRefusedConnection exceptions.
   *
   * <p>
   * Normally the backlog isn't that important because if it's full of connection requests
   * a SYN "cookie" mechanism is used to bypass the backlog queue. If this is turned off
   * though connection requests are dropped when the queue is full.
   */
  @MakeNotStatic
  private static int BACKLOG;

  /**
   * use javax.net.ssl.SSLServerSocketFactory?
   */
  private final boolean useSSL;

  /**
   * The socket producer used by the cluster
   */
  private final SocketCreator socketCreator;

  private final Membership<InternalDistributedMember> membership;

  static {
    init();
  }

  /**
   * the size of OS TCP/IP buffers, not set by default
   */
  int tcpBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;
  int idleConnectionTimeout = DEFAULT_SOCKET_LEASE_TIME;

  /**
   * port is the tcp/ip port that this conduit binds to. If it is zero, a port from
   * membership-port-range is selected to bind to. The actual port number this conduit is listening
   * on will be in the "id" instance variable
   */
  private int port;

  private final int[] tcpPortRange =
      new int[] {DEFAULT_MEMBERSHIP_PORT_RANGE[0], DEFAULT_MEMBERSHIP_PORT_RANGE[1]};

  /**
   * The java groups address that this conduit is associated with
   */
  private InternalDistributedMember localAddr;

  /**
   * address is the InetAddress that this conduit uses for identity
   */
  private final InetAddress address;

  /**
   * isBindAddress is true if we should bind to the address
   */
  private final boolean isBindAddress;

  /**
   * the object that receives DistributionMessage messages received by this conduit.
   */
  private final DirectChannel directChannel;

  private DMStats stats;

  /**
   * Config from the delegate
   *
   * @since GemFire 4.2.1
   */
  private DistributionConfig config;

  /**
   * server socket address
   */
  private InetSocketAddress id;

  private volatile boolean stopped;

  /**
   * the listener thread
   */
  private Thread thread;

  /**
   * if using NIO, this is the object used for accepting connections
   */
  private ServerSocketChannel channel;

  /**
   * the server socket
   */
  private ServerSocket socket;

  /**
   * a table of Connections from this conduit to others
   */
  private ConnectionTable conTable;

  /**
   * the reason for a shutdown, if abnormal
   */
  private volatile Exception shutdownCause;

  private final Stopper stopper = new Stopper();

  /**
   * <p>
   * creates a new TCPConduit bound to the given InetAddress and port. The given ServerDelegate will
   * receive any DistributionMessages passed to the conduit.
   *
   * <p>
   * This constructor forces the conduit to ignore the following system properties and look for them
   * only in the <i>props</i> argument:
   *
   * <pre>
   * p2p.tcpBufferSize
   * p2p.idleConnectionTimeout
   * </pre>
   */
  public TCPConduit(Membership mgr, int port, InetAddress address, boolean isBindAddress,
      DirectChannel receiver, Properties props) throws ConnectionException {
    this(mgr, port, address, isBindAddress, receiver, props, ConnectionTable::create,
        SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER),
        () -> {
          try {
            LocalHostUtil.getLocalHost();
          } catch (UnknownHostException e) {
            throw new ConnectionException("Unable to resolve localHost address", e);
          }
        },
        true);
  }

  @VisibleForTesting
  TCPConduit(Membership mgr, int port, InetAddress address, boolean isBindAddress,
      DirectChannel receiver, Properties props,
      Function<TCPConduit, ConnectionTable> connectionTableFactory, SocketCreator socketCreator,
      Runnable localHostValidation, boolean startAcceptor) throws ConnectionException {
    parseProperties(props);

    this.address = address;
    this.isBindAddress = isBindAddress;
    this.port = port;
    directChannel = receiver;
    stats = null;
    config = null;
    membership = mgr;
    if (directChannel != null) {
      stats = directChannel.getDMStats();
      config = directChannel.getDMConfig();
    }
    if (getStats() == null) {
      stats = new LonerDistributionManager.DummyDMStats();
    }

    conTable = connectionTableFactory.apply(this);

    this.socketCreator = socketCreator;
    useSSL = socketCreator.useSSL();

    if (address == null) {
      localHostValidation.run();
    }

    if (startAcceptor) {
      startAcceptor();
    }
  }

  public static void init() {
    // only use direct buffers if we are using nio
    LISTENER_CLOSE_TIMEOUT = Integer.getInteger("p2p.listenerCloseTimeout", 60000);
    BACKLOG = Integer.getInteger("p2p.backlog", 1280);
    if (Boolean.getBoolean("p2p.oldIO")) {
      logger.warn("detected use of p2p.oldIO setting - this is no longer supported");
    }
  }

  public Membership<InternalDistributedMember> getMembership() {
    return membership;
  }

  public static int getBackLog() {
    return BACKLOG;
  }

  /**
   * parse instance-level properties from the given object
   */
  private void parseProperties(Properties p) {
    if (p != null) {
      String s = p.getProperty("p2p.tcpBufferSize", String.valueOf(tcpBufferSize));
      try {
        tcpBufferSize = Integer.parseInt(s);
      } catch (Exception e) {
        logger.warn("exception parsing p2p.tcpBufferSize", e);
      }
      if (tcpBufferSize < Connection.SMALL_BUFFER_SIZE) {
        // enforce minimum
        tcpBufferSize = Connection.SMALL_BUFFER_SIZE;
      }
      s = p.getProperty("p2p.idleConnectionTimeout", String.valueOf(idleConnectionTimeout));
      try {
        idleConnectionTimeout = Integer.parseInt(s);
      } catch (Exception e) {
        logger.warn("exception parsing p2p.idleConnectionTimeout", e);
      }

      s = p.getProperty("membership_port_range_start");
      try {
        tcpPortRange[0] = Integer.parseInt(s);
      } catch (Exception e) {
        logger.warn("Exception parsing membership-port-range start port.", e);
      }

      s = p.getProperty("membership_port_range_end");
      try {
        tcpPortRange[1] = Integer.parseInt(s);
      } catch (Exception e) {
        logger.warn("Exception parsing membership-port-range end port.", e);
      }
    }
  }

  /**
   * binds the server socket and gets threads going
   */
  private void startAcceptor() throws ConnectionException {
    int p = port;

    createServerSocket();

    int localPort;
    try {
      localPort = socket.getLocalPort();

      id = new InetSocketAddress(socket.getInetAddress(), localPort);
      stopped = false;
      thread = new LoggingThread("P2P Listener Thread " + id, this);
      try {
        thread.setPriority(Thread.MAX_PRIORITY);
      } catch (Exception e) {
        logger.info("unable to set listener priority: {}", e.getMessage());
      }
      if (!Boolean.getBoolean("p2p.test.inhibitAcceptor")) {
        thread.start();
      } else {
        logger.fatal(
            "p2p.test.inhibitAcceptor was found to be set, inhibiting incoming tcp/ip connections");
        socket.close();
      }
    } catch (IOException io) {
      String s = "While creating ServerSocket on port " + p;
      throw new ConnectionException(s, io);
    }
    port = localPort;
  }

  /**
   * creates the server sockets. This can be used to recreate the socket using this.port and
   * this.bindAddress, which must be set before invoking this method.
   */
  private void createServerSocket() {
    int serverPort = port;
    int connectionRequestBacklog = BACKLOG;
    InetAddress bindAddress = address;

    try {
      if (serverPort <= 0) {
        socket = socketCreator.createServerSocketUsingPortRange(bindAddress,
            connectionRequestBacklog, isBindAddress, true, 0, tcpPortRange);

      } else {
        ServerSocketChannel channel = ServerSocketChannel.open();
        socket = channel.socket();

        InetSocketAddress inetSocketAddress =
            new InetSocketAddress(isBindAddress ? bindAddress : null, serverPort);
        socket.bind(inetSocketAddress, connectionRequestBacklog);
      }

      try {
        // set these buffers early so that large buffers will be allocated
        // on accepted sockets (see java.net.ServerSocket.setReceiverBufferSize javadocs)
        socket.setReceiveBufferSize(tcpBufferSize);
        int newSize = socket.getReceiveBufferSize();
        if (newSize != tcpBufferSize) {
          logger.info("{} is {} instead of the requested {}",
              "Listener receiverBufferSize", newSize, tcpBufferSize);
        }
      } catch (SocketException ex) {
        logger.warn("Failed to set listener receiverBufferSize to {}",
            tcpBufferSize);
      }
      channel = socket.getChannel();
      port = socket.getLocalPort();
    } catch (IOException io) {
      throw new ConnectionException(
          String.format("While creating ServerSocket on port %s with address %s",
              serverPort, bindAddress),
          io);
    }
  }

  /**
   * Close the ServerSocketChannel, ServerSocket, and the ConnectionTable.
   *
   * @see SystemFailure#emergencyClose()
   */
  public void emergencyClose() {
    // stop(); // Causes grief
    if (stopped) {
      return;
    }

    stopped = true;

    try {
      if (channel != null) {
        channel.close();
        // NOTE: do not try to interrupt the listener thread at this point.
        // Doing so interferes with the channel's socket logic.
      } else {
        if (socket != null) {
          socket.close();
        }
      }
    } catch (IOException e) {
      // ignore, please!
    }

    // this.conTable.close(); not safe against deadlocks
    ConnectionTable.emergencyClose();

    socket = null;
    thread = null;
    conTable = null;
  }

  /**
   * stops the conduit, closing all tcp/ip connections
   */
  public void stop(Exception cause) {
    if (!stopped) {
      stopped = true;
      shutdownCause = cause;

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "Shutting down conduit");
      }
      try {
        // set timeout endpoint here since interrupt() has been known to hang
        long timeout = System.currentTimeMillis() + LISTENER_CLOSE_TIMEOUT;
        Thread t = thread;
        if (channel != null) {
          channel.close();
          // NOTE: do not try to interrupt the listener thread at this point;
          // doing so interferes with the channel's socket logic.
        } else {
          ServerSocket s = socket;
          if (s != null) {
            s.close();
          }
          if (t != null) {
            t.interrupt();
          }
        }

        do {
          t = thread;
          if (t == null || !t.isAlive()) {
            break;
          }
          t.join(200);
        } while (timeout > System.currentTimeMillis());

        if (t != null && t.isAlive()) {
          logger.warn(
              "Unable to shut down listener within {}ms.  Unable to interrupt socket.accept() due to JDK bug. Giving up.",
              LISTENER_CLOSE_TIMEOUT);
        }
      } catch (IOException | InterruptedException e) {
        // we're already trying to shutdown, ignore
      }

      // close connections after shutting down acceptor to fix bug 30695
      conTable.close();

      socket = null;
      thread = null;
      conTable = null;
    }
  }

  /**
   * starts the conduit again after it's been stopped. This will clear the server map if the
   * conduit's port is zero (wildcard bind)
   */
  public void restart() throws ConnectionException {
    if (!stopped) {
      return;
    }
    stats = null;
    if (directChannel != null) {
      stats = directChannel.getDMStats();
    }
    if (getStats() == null) {
      stats = new LonerDistributionManager.DummyDMStats();
    }
    conTable = ConnectionTable.create(this);
    startAcceptor();
  }

  /**
   * this is the server socket listener thread's run loop
   */
  @Override
  public void run() {
    ConnectionTable.threadWantsSharedResources();
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "Starting P2P Listener on  {}", id);
    }
    for (;;) {
      SystemFailure.checkFailure();
      if (stopper.isCancelInProgress()) {
        break;
      }
      if (stopped) {
        break;
      }
      if (Thread.currentThread().isInterrupted()) {
        break;
      }

      Socket othersock = null;
      try {
        SocketChannel otherChannel = channel.accept();
        othersock = otherChannel.socket();
        if (stopped) {
          try {
            if (othersock != null) {
              othersock.close();
            }
          } catch (Exception e) {
            // ignored
          }
          continue;
        }

        acceptConnection(othersock);

      } catch (ClosedByInterruptException cbie) {
        // safe to ignore
      } catch (ClosedChannelException | CancelException e) {
        break;
      } catch (IOException e) {
        getStats().incFailedAccept();

        try {
          if (othersock != null) {
            othersock.close();
          }
        } catch (IOException ignore) {

        }

        if (!stopped) {
          if (e instanceof SocketException && "Socket closed".equalsIgnoreCase(e.getMessage())) {
            // safe to ignore
            if (!socket.isClosed()) {
              logger.warn("ServerSocket threw 'socket closed' exception but says it is not closed",
                  e);
              try {
                socket.close();
                createServerSocket();
              } catch (IOException ioe) {
                logger.fatal("Unable to close and recreate server socket", ioe);
                // post 5.1.0x, this should force shutdown
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException ie) {
                  // Don't reset; we're just exiting the thread
                  logger.info("Interrupted and exiting while trying to recreate listener sockets");
                  return;
                }
              }
            }
          } else if ("Too many open files".equals(e.getMessage())) {
            getConTable().fileDescriptorsExhausted();
          } else {
            logger.warn(e.getMessage(), e);
          }

        }
      } catch (Exception e) {
        logger.warn(e.getMessage(), e);
      }

      if (!stopped && socket.isClosed()) {
        // NOTE: do not check for distributed system closing here.
        // Messaging may need to occur during the closing of the DS or cache
        logger.warn("ServerSocket closed - reopening");
        try {
          createServerSocket();
        } catch (ConnectionException ex) {
          logger.warn(ex.getMessage(), ex);
        }
      }
    }

    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace("Stopped P2P Listener on  {}", id);
    }
  }

  private ConnectionTable getConTable() {
    ConnectionTable result = conTable;
    if (result == null) {
      stopper.checkCancelInProgress(null);
      throw new DistributedSystemDisconnectedException("tcp layer has been shutdown");
    }
    return result;
  }

  private void acceptConnection(Socket otherSocket) {
    try {
      getConTable().acceptConnection(otherSocket, new PeerConnectionFactory());
    } catch (IOException | ConnectionException io) {
      // exception is logged by the Connection
      if (!stopped) {
        getStats().incFailedAccept();
      }
    } catch (CancelException e) {
      // ignored
    } catch (Exception e) {
      if (!stopped) {
        getStats().incFailedAccept();
        logger.warn("Failed to accept connection from {} because {}",
            otherSocket.getInetAddress(), e);
      }
    }
  }

  /**
   * records the current outgoing message count on all thread-owned ordered connections
   *
   * @since GemFire 5.1
   */
  public void getThreadOwnedOrderedConnectionState(DistributedMember member, Map result) {
    getConTable().getThreadOwnedOrderedConnectionState(member, result);
  }

  /**
   * wait for the incoming connections identified by the keys in the argument to receive and
   * dispatch the number of messages associated with the key
   *
   * @since GemFire 5.1
   */
  public void waitForThreadOwnedOrderedConnectionState(DistributedMember member, Map channelState)
      throws InterruptedException {
    getConTable().waitForThreadOwnedOrderedConnectionState(member, channelState);
  }

  /**
   * connections send messageReceived when a message object has been read.
   *
   * @param bytesRead number of bytes read off of network to get this message
   */
  void messageReceived(Connection receiver, DistributionMessage message, int bytesRead)
      throws MemberShunnedException {
    if (logger.isTraceEnabled()) {
      logger.trace("{} received {} from {}", id, message, receiver);
    }

    if (directChannel != null) {
      message.setBytesRead(bytesRead);
      message.setSender(receiver.getRemoteAddress());
      message.setSharedReceiver(receiver.isSharedResource());
      directChannel.receive(message, bytesRead);
    }
  }

  /**
   * gets the address of this conduit's ServerSocket endpoint
   */
  public InetSocketAddress getSocketId() {
    return id;
  }

  /**
   * gets the actual port to which this conduit's ServerSocket is bound
   */
  public int getPort() {
    return id.getPort();
  }

  /**
   * Gets the local member ID that identifies this conduit
   */
  public InternalDistributedMember getMemberId() {
    return localAddr;
  }

  public void setMemberId(InternalDistributedMember addr) {
    localAddr = addr;
  }

  public DistributionConfig getConfig() {
    return config;
  }

  /**
   * Return a connection to the given member. This method must continue to attempt to create a
   * connection to the given member as long as that member is in the membership view and the system
   * is not shutting down.
   *
   * @param memberAddress the IDS associated with the remoteId
   * @param preserveOrder whether this is an ordered or unordered connection
   * @param retry false if this is the first attempt
   * @param startTime the time this operation started
   * @param ackTimeout the ack-wait-threshold * 1000 for the operation to be transmitted (or zero)
   * @param ackSATimeout the ack-severe-alert-threshold * 1000 for the operation to be transmitted
   *        (or zero)
   *
   * @return the connection
   */
  public Connection getConnection(InternalDistributedMember memberAddress,
      final boolean preserveOrder, boolean retry, long startTime, long ackTimeout,
      long ackSATimeout) throws IOException, DistributedSystemDisconnectedException {
    if (stopped) {
      throw new DistributedSystemDisconnectedException("The conduit is stopped");
    }

    InternalDistributedMember memberInTrouble = null;
    Connection conn = null;
    for (boolean breakLoop = false;;) {
      stopper.checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        // If this is the second time through this loop, we had problems.
        // Tear down the connection so that it gets rebuilt.
        if (retry || conn != null) { // not first time in loop
          if (!membership.memberExists(memberAddress)
              || membership.isShunned(memberAddress)
              || membership.shutdownInProgress()) {
            throw new IOException("TCP/IP connection lost and member is not in view");
          }
          // Member is still in view; we MUST NOT give up!

          // Pause just a tiny bit...
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            interrupted = true;
            stopper.checkCancelInProgress(e);
          }

          // try again after sleep
          if (!membership.memberExists(memberAddress)
              || membership.isShunned(memberAddress)) {
            // OK, the member left. Just register an error.
            throw new IOException("TCP/IP connection lost and member is not in view");
          }

          // Print a warning (once)
          if (memberInTrouble == null) {
            memberInTrouble = memberAddress;
            logger.warn("Attempting TCP/IP reconnect to  {}", memberInTrouble);
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("Attempting TCP/IP reconnect to {}", memberInTrouble);
            }
          }

          // Close the connection (it will get rebuilt later).
          getStats().incReconnectAttempts();
          if (conn != null) {
            try {
              if (logger.isDebugEnabled()) {
                logger.debug("Closing old connection.  conn={} before retrying. memberInTrouble={}",
                    conn, memberInTrouble);
              }
              conn.closeForReconnect("closing before retrying");
            } catch (CancelException ex) {
              throw ex;
            } catch (Exception ex) {
              // ignored
            }
          }
        } // not first time in loop

        Exception problem = null;
        try {
          // Get (or regenerate) the connection
          // this could generate a ConnectionException, so it must be caught and retried
          boolean retryForOldConnection;
          boolean debugRetry = false;
          do {
            retryForOldConnection = false;
            conn = getConTable().get(memberAddress, preserveOrder, startTime, ackTimeout,
                ackSATimeout);
            if (conn == null) {
              // conduit may be closed - otherwise an ioexception would be thrown
              problem = new IOException(
                  String.format("Unable to reconnect to server; possible shutdown: %s",
                      memberAddress));
            } else if (conn.isClosing() || !conn.getRemoteAddress().equals(memberAddress)) {
              if (logger.isDebugEnabled()) {
                logger.debug("Got an old connection for {}: {}@{}", memberAddress, conn,
                    conn.hashCode());
              }
              conn.closeOldConnection("closing old connection");
              conn = null;
              retryForOldConnection = true;
              debugRetry = true;
            }
          } while (retryForOldConnection);
          if (debugRetry && logger.isDebugEnabled()) {
            logger.debug("Done removing old connections");
          }

          // we have a connection; fall through and return it
        } catch (ConnectionException e) {
          // Race condition between acquiring the connection and attempting
          // to use it: another thread closed it.
          problem = e;
          // No need to retry since Connection.createSender has already
          // done retries and now member is really unreachable for some reason
          // even though it may be in the view
          breakLoop = true;
        } catch (IOException e) {
          problem = e;
          // don't keep trying to connect to an alert listener
          if (AlertingAction.isThreadAlerting()) {
            if (logger.isDebugEnabled()) {
              logger.debug("Giving up connecting to alert listener {}", memberAddress);
            }
            breakLoop = true;
          }
        }

        if (problem != null) {
          // Some problems are not recoverable; check and error out early.
          if (!membership.memberExists(memberAddress)
              || membership.isShunned(memberAddress)) { // left the view
            // Bracket our original warning
            if (memberInTrouble != null) {
              // make this msg info to bracket warning
              logger.info("Ending reconnect attempt because {} has disappeared.", memberInTrouble);
            }
            throw new IOException(
                String.format("Peer has disappeared from view: %s", memberAddress));
          } // left the view

          if (membership.shutdownInProgress()) { // shutdown in progress
            // Bracket our original warning
            if (memberInTrouble != null) {
              // make this msg info to bracket warning
              logger.info("Ending reconnect attempt to {} because shutdown has started.",
                  memberInTrouble);
            }
            stopper.checkCancelInProgress(null);
            throw new DistributedSystemDisconnectedException(
                "Abandoned because shutdown is in progress");
          } // shutdown in progress

          // Log the warning. We wait until now, because we want
          // to have m defined for a nice message...
          if (memberInTrouble == null) {
            logger.warn("Error sending message to {} (will reattempt): {}", memberAddress, problem);
            memberInTrouble = memberAddress;
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("Error sending message to {}", memberAddress, problem);
            }
          }

          if (breakLoop) {
            if (problem instanceof IOException) {
              if (problem.getMessage().startsWith("Cannot form connection to alert listener")) {
                throw new AlertingIOException((IOException) problem);
              }
              throw (IOException) problem;
            }
            throw new IOException(
                String.format("Problem connecting to %s", memberAddress), problem);
          }
          // Retry the operation (indefinitely)
          continue;
        }
        // Success!

        // Make sure our logging is bracketed if there was a problem
        if (memberInTrouble != null) {
          logger.info("Successfully reconnected to member {}", memberInTrouble);
          if (logger.isTraceEnabled()) {
            logger.trace("new connection is {} memberAddress={}", conn, memberAddress);
          }
        }
        return conn;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Override
  public String toString() {
    return String.valueOf(id);
  }

  /**
   * Returns the distribution manager of the direct channel
   */
  public DistributionManager getDM() {
    return directChannel.getDM();
  }

  public void removeEndpoint(DistributedMember mbr, String reason, boolean notifyDisconnect) {
    ConnectionTable ct = conTable;
    if (ct == null) {
      return;
    }
    ct.removeEndpoint(mbr, reason, notifyDisconnect);
  }

  /**
   * check to see if there are still any receiver threads for the given end-point
   */
  public boolean hasReceiversFor(DistributedMember endPoint) {
    ConnectionTable ct = conTable;
    return ct != null && ct.hasReceiversFor(endPoint);
  }

  /**
   * Stats from the delegate
   */
  public DMStats getStats() {
    return stats;
  }

  public boolean useSSL() {
    return useSSL;
  }

  public BufferPool getBufferPool() {
    return conTable.getBufferPool();
  }

  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

  /**
   * if the conduit is disconnected due to an abnormal condition, this will describe the reason
   *
   * @return exception that caused disconnect
   */
  Exception getShutdownCause() {
    return shutdownCause;
  }

  /**
   * returns the SocketCreator that should be used to produce sockets for TCPConduit connections.
   */
  protected SocketCreator getSocketCreator() {
    return socketCreator;
  }

  /**
   * Called by Connection before handshake reply is sent. Returns true if member is part of
   * view, false if membership is not confirmed before timeout.
   */
  boolean waitForMembershipCheck(InternalDistributedMember remoteId) {
    return membership.waitForNewMember(remoteId);
  }

  private class Stopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      DistributionManager dm = getDM();
      if (dm == null) {
        return "no distribution manager";
      }
      if (stopped) {
        return "Conduit has been stopped";
      }
      return null;
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      }
      DistributionManager dm = getDM();
      if (dm == null) {
        return new DistributedSystemDisconnectedException("no distribution manager");
      }
      RuntimeException result = dm.getCancelCriterion().generateCancelledException(e);
      if (result != null) {
        return result;
      }
      // We know we've been stopped; generate the exception
      result = new DistributedSystemDisconnectedException("Conduit has been stopped", e);
      return result;
    }
  }
}
