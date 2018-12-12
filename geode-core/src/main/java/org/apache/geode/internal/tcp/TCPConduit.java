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

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import javax.net.ssl.SSLException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.distributed.internal.direct.DirectChannel;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.logging.log4j.AlertAppender;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

/**
 * <p>
 * TCPConduit manages a server socket and a collection of connections to other systems. Connections
 * are identified by DistributedMember IDs. These types of messages are currently supported:
 * </p>
 *
 * <pre>
 * <p>
 * DistributionMessage - message is delivered to the server's
 * ServerDelegate
 * <p>
 * </pre>
 * <p>
 * In the current implementation, ServerDelegate is the DirectChannel used by the GemFire
 * DistributionManager to send and receive messages.
 * <p>
 * If the ServerDelegate is null, DistributionMessages are ignored by the TCPConduit.
 * </p>
 *
 * @since GemFire 2.0
 */

public class TCPConduit implements Runnable {

  private static final Logger logger = LogService.getLogger();

  /**
   * max amount of time (ms) to wait for listener threads to stop
   */
  private static int LISTENER_CLOSE_TIMEOUT;

  /**
   * backlog is the "accept" backlog configuration parameter all conduits server socket
   */
  private static int BACKLOG;

  /**
   * use javax.net.ssl.SSLServerSocketFactory?
   */
  static boolean useSSL;

  /**
   * Force use of Sockets rather than SocketChannels (NIO). Note from Bruce: due to a bug in the
   * java VM, NIO cannot be used with IPv6 addresses on Windows. When that condition holds, the
   * useNIO flag must be disregarded.
   */
  private static boolean USE_NIO;

  /**
   * use direct ByteBuffers instead of heap ByteBuffers for NIO operations
   */
  static boolean useDirectBuffers;

  /**
   * The socket producer used by the cluster
   */
  private final SocketCreator socketCreator;


  private MembershipManager membershipManager;

  /**
   * true if NIO can be used for the server socket
   */
  private boolean useNIO;

  static {
    init();
  }

  public MembershipManager getMembershipManager() {
    return membershipManager;
  }

  public static int getBackLog() {
    return BACKLOG;
  }

  public static void init() {
    useSSL = Boolean.getBoolean("p2p.useSSL");
    // only use nio if not SSL
    USE_NIO = !useSSL && !Boolean.getBoolean("p2p.oldIO");
    // only use direct buffers if we are using nio
    useDirectBuffers = USE_NIO && !Boolean.getBoolean("p2p.nodirectBuffers");
    LISTENER_CLOSE_TIMEOUT = Integer.getInteger("p2p.listenerCloseTimeout", 60000).intValue();
    // fix for bug 37730
    BACKLOG = Integer.getInteger("p2p.backlog", HANDSHAKE_POOL_SIZE + 1).intValue();
  }

  ///////////////// permanent conduit state

  /**
   * the size of OS TCP/IP buffers, not set by default
   */
  public int tcpBufferSize = DistributionConfig.DEFAULT_SOCKET_BUFFER_SIZE;
  public int idleConnectionTimeout = DistributionConfig.DEFAULT_SOCKET_LEASE_TIME;

  /**
   * port is the tcp/ip port that this conduit binds to. If it is zero, a port from
   * membership-port-range is selected to bind to. The actual port number this conduit is listening
   * on will be in the "id" instance variable
   */
  private int port;

  private int[] tcpPortRange = new int[] {DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[0],
      DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[1]};

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
  DistributionConfig config;

  ////////////////// runtime state that is re-initialized on a restart

  /**
   * server socket address
   */
  private InetSocketAddress id;

  protected volatile boolean stopped;

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
   * <p>
   * creates a new TCPConduit bound to the given InetAddress and port. The given ServerDelegate will
   * receive any DistributionMessages passed to the conduit.
   * </p>
   * <p>
   * This constructor forces the conduit to ignore the following system properties and look for them
   * only in the <i>props</i> argument:
   * </p>
   *
   * <pre>
   * p2p.tcpBufferSize
   * p2p.idleConnectionTimeout
   * </pre>
   */
  public TCPConduit(MembershipManager mgr, int port, InetAddress address, boolean isBindAddress,
      DirectChannel receiver, Properties props) throws ConnectionException {
    parseProperties(props);

    this.address = address;
    this.isBindAddress = isBindAddress;
    this.port = port;
    this.directChannel = receiver;
    this.stats = null;
    this.config = null;
    this.membershipManager = mgr;
    if (directChannel != null) {
      this.stats = directChannel.getDMStats();
      this.config = directChannel.getDMConfig();
    }
    if (this.getStats() == null) {
      this.stats = new LonerDistributionManager.DummyDMStats();
    }

    try {
      this.conTable = ConnectionTable.create(this);
    } catch (IOException io) {
      throw new ConnectionException(
          "Unable to initialize connection table",
          io);
    }

    this.socketCreator =
        SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER);

    this.useNIO = USE_NIO;
    if (this.useNIO) {
      InetAddress addr = address;
      if (addr == null) {
        try {
          addr = SocketCreator.getLocalHost();
        } catch (java.net.UnknownHostException e) {
          throw new ConnectionException("Unable to resolve localHost address", e);
        }
      }
      // JDK bug 6230761 - NIO can't be used with IPv6 on Windows
      if (addr instanceof Inet6Address) {
        String os = System.getProperty("os.name");
        if (os != null) {
          if (os.indexOf("Windows") != -1) {
            this.useNIO = false;
          }
        }
      }
    }

    startAcceptor();
  }


  /**
   * parse instance-level properties from the given object
   */
  private void parseProperties(Properties p) {
    if (p != null) {
      String s;
      s = p.getProperty("p2p.tcpBufferSize", "" + tcpBufferSize);
      try {
        tcpBufferSize = Integer.parseInt(s);
      } catch (Exception e) {
        logger.warn("exception parsing p2p.tcpBufferSize",
            e);
      }
      if (tcpBufferSize < Connection.SMALL_BUFFER_SIZE) {
        // enforce minimum
        tcpBufferSize = Connection.SMALL_BUFFER_SIZE;
      }
      s = p.getProperty("p2p.idleConnectionTimeout", "" + idleConnectionTimeout);
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
        logger.warn("Exception parsing membership-port-range end port.",
            e);
      }

    }
  }

  private ExecutorService hsPool;

  /**
   * the reason for a shutdown, if abnormal
   */
  private volatile Exception shutdownCause;

  private static final int HANDSHAKE_POOL_SIZE =
      Integer.getInteger("p2p.HANDSHAKE_POOL_SIZE", 10).intValue();
  private static final long HANDSHAKE_POOL_KEEP_ALIVE_TIME =
      Long.getLong("p2p.HANDSHAKE_POOL_KEEP_ALIVE_TIME", 60).longValue();

  /**
   * added to fix bug 40436
   */
  public void setMaximumHandshakePoolSize(int maxSize) {
    if (this.hsPool != null) {
      ThreadPoolExecutor handshakePool = (ThreadPoolExecutor) this.hsPool;
      if (maxSize > handshakePool.getMaximumPoolSize()) {
        handshakePool.setMaximumPoolSize(maxSize);
      }
    }
  }

  /**
   * binds the server socket and gets threads going
   */
  private void startAcceptor() throws ConnectionException {
    int localPort;
    int p = this.port;
    InetAddress ba = this.address;

    {
      ExecutorService tmp_hsPool = null;
      String threadName = "P2P-Handshaker " + ba + ":" + p + " Thread ";
      try {
        tmp_hsPool =
            LoggingExecutors.newThreadPoolWithSynchronousFeedThatHandlesRejection(threadName, null,
                null, 1, HANDSHAKE_POOL_SIZE, HANDSHAKE_POOL_KEEP_ALIVE_TIME);
      } catch (IllegalArgumentException poolInitException) {
        throw new ConnectionException(
            "while creating handshake pool",
            poolInitException);
      }
      this.hsPool = tmp_hsPool;
    }
    createServerSocket();
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
        this.hsPool.shutdownNow();
      }
    } catch (IOException io) {
      String s = "While creating ServerSocket on port " + p;
      throw new ConnectionException(s, io);
    }
    this.port = localPort;
  }

  /**
   * creates the server sockets. This can be used to recreate the socket using this.port and
   * this.bindAddress, which must be set before invoking this method.
   */
  private void createServerSocket() {
    int p = this.port;
    int b = BACKLOG;
    InetAddress bindAddress = this.address;

    try {
      if (this.useNIO) {
        if (p <= 0) {

          socket = socketCreator.createServerSocketUsingPortRange(bindAddress, b, isBindAddress,
              this.useNIO, 0, tcpPortRange);
        } else {
          ServerSocketChannel channel = ServerSocketChannel.open();
          socket = channel.socket();

          InetSocketAddress inetSocketAddress =
              new InetSocketAddress(isBindAddress ? bindAddress : null, p);
          socket.bind(inetSocketAddress, b);
        }

        if (useNIO) {
          try {
            // set these buffers early so that large buffers will be allocated
            // on accepted sockets (see java.net.ServerSocket.setReceiverBufferSize javadocs)
            socket.setReceiveBufferSize(tcpBufferSize);
            int newSize = socket.getReceiveBufferSize();
            if (newSize != tcpBufferSize) {
              logger.info("{} is {} instead of the requested {}",
                  "Listener receiverBufferSize", Integer.valueOf(newSize),
                  Integer.valueOf(tcpBufferSize));
            }
          } catch (SocketException ex) {
            logger.warn("Failed to set listener receiverBufferSize to {}",
                tcpBufferSize);
          }
        }
        channel = socket.getChannel();
      } else {
        try {
          if (p <= 0) {
            socket = socketCreator.createServerSocketUsingPortRange(bindAddress, b, isBindAddress,
                this.useNIO, this.tcpBufferSize, tcpPortRange);
          } else {
            socket = socketCreator.createServerSocket(p, b, isBindAddress ? bindAddress : null,
                this.tcpBufferSize);
          }
          int newSize = socket.getReceiveBufferSize();
          if (newSize != this.tcpBufferSize) {
            logger.info("Listener receiverBufferSize is {} instead of the requested {}",
                Integer.valueOf(newSize),
                Integer.valueOf(this.tcpBufferSize));
          }
        } catch (SocketException ex) {
          logger.warn("Failed to set listener receiverBufferSize to {}",
              this.tcpBufferSize);

        }
      }
      port = socket.getLocalPort();
    } catch (IOException io) {
      throw new ConnectionException(
          String.format("While creating ServerSocket on port %s with address %s",
              new Object[] {Integer.valueOf(p), bindAddress}),
          io);
    }
  }

  /**
   * Ensure that the ConnectionTable class gets loaded.
   *
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    ConnectionTable.loadEmergencyClasses();
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

    // this.hsPool.shutdownNow(); // I don't trust this not to allocate objects or to synchronize
    // this.conTable.close(); not safe against deadlocks
    ConnectionTable.emergencyClose();

    socket = null;
    thread = null;
    conTable = null;
  }

  /* stops the conduit, closing all tcp/ip connections */
  public void stop(Exception cause) {
    if (!stopped) {
      stopped = true;
      shutdownCause = cause;

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "Shutting down conduit");
      }
      try {
        // set timeout endpoint here since interrupt() has been known
        // to hang
        long timeout = System.currentTimeMillis() + LISTENER_CLOSE_TIMEOUT;
        Thread t = this.thread;;
        if (channel != null) {
          channel.close();
          // NOTE: do not try to interrupt the listener thread at this point.
          // Doing so interferes with the channel's socket logic.
        } else {
          ServerSocket s = this.socket;
          if (s != null) {
            s.close();
          }
          if (t != null) {
            t.interrupt();
          }
        }

        do {
          t = this.thread;
          if (t == null || !t.isAlive()) {
            break;
          }
          t.join(200);
        } while (timeout > System.currentTimeMillis());

        if (t != null && t.isAlive()) {
          logger.warn(
              "Unable to shut down listener within {}ms.  Unable to interrupt socket.accept() due to JDK bug. Giving up.",
              Integer.valueOf(LISTENER_CLOSE_TIMEOUT));
        }
      } catch (IOException | InterruptedException e) {
        // we're already trying to shutdown, ignore
      } finally {
        this.hsPool.shutdownNow();
      }

      // close connections after shutting down acceptor to fix bug 30695
      this.conTable.close();

      socket = null;
      thread = null;
      conTable = null;
    }
  }

  /**
   * Returns whether or not this conduit is stopped
   *
   * @since GemFire 3.0
   */
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * starts the conduit again after it's been stopped. This will clear the server map if the
   * conduit's port is zero (wildcard bind)
   */
  public void restart() throws ConnectionException {
    if (!stopped) {
      return;
    }
    this.stats = null;
    if (directChannel != null) {
      this.stats = directChannel.getDMStats();
    }
    if (this.getStats() == null) {
      this.stats = new LonerDistributionManager.DummyDMStats();
    }
    try {
      this.conTable = ConnectionTable.create(this);
    } catch (IOException io) {
      throw new ConnectionException(
          "Unable to initialize connection table",
          io);
    }
    startAcceptor();
  }

  /**
   * this is the server socket listener thread's run loop
   */
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
      if (stopper.isCancelInProgress()) {
        break; // part of bug 37271
      }

      Socket othersock = null;
      try {
        if (this.useNIO) {
          SocketChannel otherChannel = channel.accept();
          othersock = otherChannel.socket();
        } else {
          try {
            othersock = socket.accept();
          } catch (SSLException ex) {
            // SW: This is the case when there is a problem in P2P
            // SSL configuration, so need to exit otherwise goes into an
            // infinite loop just filling the logs
            logger.warn("Stopping P2P listener due to SSL configuration problem.",
                ex);
            break;
          }
          othersock.setSoTimeout(0);
          socketCreator.handshakeIfSocketIsSSL(othersock, idleConnectionTimeout);
        }
        if (stopped) {
          try {
            if (othersock != null) {
              othersock.close();
            }
          } catch (Exception e) {
          }
          continue;
        }

        acceptConnection(othersock);

      } catch (ClosedByInterruptException cbie) {
        // safe to ignore
      } catch (ClosedChannelException | CancelException e) {
        break;
      } catch (IOException e) {
        this.getStats().incFailedAccept();

        try {
          if (othersock != null) {
            othersock.close();
          }
        } catch (IOException ignore) {

        }

        if (!stopped) {
          if (e instanceof SocketException && "Socket closed".equalsIgnoreCase(e.getMessage())) {
            // safe to ignore; see bug 31156
            if (!socket.isClosed()) {
              logger.warn("ServerSocket threw 'socket closed' exception but says it is not closed",
                  e);
              try {
                socket.close();
                createServerSocket();
              } catch (IOException ioe) {
                logger.fatal("Unable to close and recreate server socket",
                    ioe);
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
        // NOTE: do not check for distributed system closing here. Messaging
        // may need to occur during the closing of the DS or cache
        logger.warn("ServerSocket closed - reopening");
        try {
          createServerSocket();
        } catch (ConnectionException ex) {
          logger.warn(ex.getMessage(), ex);
        }
      }
    } // for

    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace("Stopped P2P Listener on  {}", id);
    }
  }

  private void acceptConnection(final Socket othersock) {
    try {
      this.hsPool.execute(new Runnable() {
        public void run() {
          basicAcceptConnection(othersock);
        }
      });
    } catch (RejectedExecutionException rejected) {
      try {
        othersock.close();
      } catch (IOException ignore) {
      }
    }
  }

  private ConnectionTable getConTable() {
    ConnectionTable result = this.conTable;
    if (result == null) {
      stopper.checkCancelInProgress(null);
      throw new DistributedSystemDisconnectedException(
          "tcp layer has been shutdown");
    }
    return result;
  }

  protected void basicAcceptConnection(Socket othersock) {
    try {
      getConTable().acceptConnection(othersock, new PeerConnectionFactory());
    } catch (IOException io) {
      // exception is logged by the Connection
      if (!stopped) {
        this.getStats().incFailedAccept();
      }
    } catch (ConnectionException ex) {
      // exception is logged by the Connection
      if (!stopped) {
        this.getStats().incFailedAccept();
      }
    } catch (CancelException e) {
    } catch (Exception e) {
      if (!stopped) {
        this.getStats().incFailedAccept();
        logger.warn("Failed to accept connection from {} because {}",
            othersock.getInetAddress(), e);
      }
    }
  }

  /**
   * return true if "new IO" classes are being used for the server socket
   */
  protected boolean useNIO() {
    return this.useNIO;
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
  protected void messageReceived(Connection receiver, DistributionMessage message, int bytesRead) {
    if (logger.isTraceEnabled()) {
      logger.trace("{} received {} from {}", id, message, receiver);
    }

    if (directChannel != null) {
      DistributionMessage msg = message;
      msg.setBytesRead(bytesRead);
      msg.setSender(receiver.getRemoteAddress());
      msg.setSharedReceiver(receiver.isSharedResource());
      directChannel.receive(msg, bytesRead);
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
    return this.localAddr;
  }

  public void setMemberId(InternalDistributedMember addr) {
    localAddr = addr;
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
      long ackSATimeout) throws java.io.IOException, DistributedSystemDisconnectedException {
    if (stopped) {
      throw new DistributedSystemDisconnectedException(
          "The conduit is stopped");
    }

    Connection conn = null;
    InternalDistributedMember memberInTrouble = null;
    boolean breakLoop = false;
    for (;;) {
      stopper.checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        // If this is the second time through this loop, we had
        // problems. Tear down the connection so that it gets
        // rebuilt.
        if (retry || conn != null) { // not first time in loop
          if (!membershipManager.memberExists(memberAddress)
              || membershipManager.isShunned(memberAddress)
              || membershipManager.shutdownInProgress()) {
            throw new IOException(
                "TCP/IP connection lost and member is not in view");
          }
          // bug35953: Member is still in view; we MUST NOT give up!

          // Pause just a tiny bit...
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            interrupted = true;
            stopper.checkCancelInProgress(e);
          }

          // try again after sleep
          if (!membershipManager.memberExists(memberAddress)
              || membershipManager.isShunned(memberAddress)) {
            // OK, the member left. Just register an error.
            throw new IOException(
                "TCP/IP connection lost and member is not in view");
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
          this.getStats().incReconnectAttempts();
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
            }
          }
        } // not first time in loop

        Exception problem = null;
        try {
          // Get (or regenerate) the connection
          // bug36202: this could generate a ConnectionException, so it
          // must be caught and retried
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
          // [sumedh] No need to retry since Connection.createSender has already
          // done retries and now member is really unreachable for some reason
          // even though it may be in the view
          breakLoop = true;
        } catch (IOException e) {
          problem = e;
          // bug #43962 don't keep trying to connect to an alert listener
          if (AlertAppender.isThreadAlerting()) {
            if (logger.isDebugEnabled()) {
              logger.debug("Giving up connecting to alert listener {}", memberAddress);
            }
            breakLoop = true;
          }
        }

        if (problem != null) {
          // Some problems are not recoverable; check and error out early.
          if (!membershipManager.memberExists(memberAddress)
              || membershipManager.isShunned(memberAddress)) { // left the view
            // Bracket our original warning
            if (memberInTrouble != null) {
              // make this msg info to bracket warning
              logger.info("Ending reconnect attempt because {} has disappeared.",
                  memberInTrouble);
            }
            throw new IOException(String.format("Peer has disappeared from view: %s",
                memberAddress));
          } // left the view

          if (membershipManager.shutdownInProgress()) { // shutdown in progress
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
            logger.warn("Error sending message to {} (will reattempt): {}",
                memberAddress, problem);
            memberInTrouble = memberAddress;
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("Error sending message to {}", memberAddress, problem);
            }
          }

          if (breakLoop) {
            if (!problem.getMessage().startsWith("Cannot form connection to alert listener")) {
              logger.warn("Throwing IOException after finding breakLoop=true",
                  problem);
            }
            if (problem instanceof IOException) {
              throw (IOException) problem;
            } else {
              IOException ioe = new IOException(String.format("Problem connecting to %s",
                  memberAddress));
              ioe.initCause(problem);
              throw ioe;
            }
          }
          // Retry the operation (indefinitely)
          continue;
        } // problem != null
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
    } // for(;;)
  }

  @Override
  public String toString() {
    return "" + id;
  }

  /**
   * Returns the distribution manager of the direct channel
   */
  public DistributionManager getDM() {
    return directChannel.getDM();
  }

  public void removeEndpoint(DistributedMember mbr, String reason, boolean notifyDisconnect) {
    ConnectionTable ct = this.conTable;
    if (ct == null) {
      return;
    }
    ct.removeEndpoint(mbr, reason, notifyDisconnect);
  }

  /**
   * check to see if there are still any receiver threads for the given end-point
   */
  public boolean hasReceiversFor(DistributedMember endPoint) {
    ConnectionTable ct = this.conTable;
    return (ct != null) && ct.hasReceiversFor(endPoint);
  }

  /**
   * Stats from the delegate
   */
  public DMStats getStats() {
    return stats;
  }

  protected class Stopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      DistributionManager dm = getDM();
      if (dm == null) {
        return "no distribution manager";
      }
      if (TCPConduit.this.stopped) {
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
      result = new DistributedSystemDisconnectedException("Conduit has been stopped");
      result.initCause(e);
      return result;
    }
  }

  private final Stopper stopper = new Stopper();

  public CancelCriterion getCancelCriterion() {
    return stopper;
  }


  /**
   * if the conduit is disconnected due to an abnormal condition, this will describe the reason
   *
   * @return exception that caused disconnect
   */
  public Exception getShutdownCause() {
    return this.shutdownCause;
  }

  /**
   * returns the SocketCreator that should be used to produce sockets for TCPConduit connections.
   */
  protected SocketCreator getSocketCreator() {
    return socketCreator;
  }

  /**
   * ARB: Called by Connection before handshake reply is sent. Returns true if member is part of
   * view, false if membership is not confirmed before timeout.
   */
  public boolean waitForMembershipCheck(InternalDistributedMember remoteId) {
    return membershipManager.waitForNewMember(remoteId);
  }
}
