/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

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
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.LonerDistributionManager;
import com.gemstone.gemfire.distributed.internal.direct.DirectChannel;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/** <p>TCPConduit manages a server socket and a collection of connections to
    other systems.  Connections are identified by host/port Stubs.
    These types of messages are currently supported:</p><pre>

       DistributionMessage - message is delivered to the server's
                             ServerDelegate

    </pre>
    <p>In the current implementation, ServerDelegate is the DirectChannel
    used by the GemFire DistributionManager to send and receive messages.<p>
    If the ServerDelegate is null, DistributionMessages are ignored by
    the TCPConduit.</p>

    @author Bruce Schuchardt
    @since 2.0
   
*/

public class TCPConduit implements Runnable {
  private static final Logger logger = LogService.getLogger();

  /** max amount of time (ms) to wait for listener threads to stop */
  private static int LISTENER_CLOSE_TIMEOUT;

  /** backlog is the "accept" backlog configuration parameter all
      conduits server socket */
  private static int BACKLOG;
  
  /** use javax.net.ssl.SSLServerSocketFactory? */
  static boolean useSSL;

//   public final static boolean USE_SYNC_WRITES = Boolean.getBoolean("p2p.useSyncWrites");

  /**
   * Force use of Sockets rather than SocketChannels (NIO).  Note from Bruce: due to
   * a bug in the java VM, NIO cannot be used with IPv6 addresses on Windows.
   * When that condition holds, the useNIO flag must be disregarded.
   */
  private static boolean USE_NIO;

  /** use direct ByteBuffers instead of heap ByteBuffers for NIO operations */
  static boolean useDirectBuffers;
  
  private volatile boolean inhibitNewConnections;

//  private transient DistributedMembershipListener messageReceiver;
  
  private MembershipManager membershipManager;
  
  /** true if NIO can be used for the server socket */
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
    BACKLOG = Integer.getInteger("p2p.backlog", HANDSHAKE_POOL_SIZE+1).intValue();
  }

  ///////////////// permanent conduit state

  /** the size of OS TCP/IP buffers, not set by default */
  public int tcpBufferSize = DistributionConfig.DEFAULT_SOCKET_BUFFER_SIZE;
  public int idleConnectionTimeout = DistributionConfig.DEFAULT_SOCKET_LEASE_TIME;

  /** port is the tcp/ip port that this conduit binds to. If it is zero, a port
      from membership-port-range is selected to bind to. The actual port number this
      conduit is listening on will be in the "id" instance variable */
  private int port;

  private int[] tcpPortRange = new int[] { 1024, 65535 };

  /** The java groups address that this conduit is associated with */
  private InternalDistributedMember localAddr;
  
  /** address is the InetAddress that this conduit uses for identity */
  private final InetAddress address;
  
  /** isBindAddress is true if we should bind to the address */
  private final boolean isBindAddress;

  /** the object that receives DistributionMessage messages
      received by this conduit. */
  private final DirectChannel directChannel;
  /**
   * Stats from the delegate
   */
  DMStats stats;

  /**
   * Config from the delegate
   * @since 4.2.1
   */
  DistributionConfig config;
  
  ////////////////// runtime state that is re-initialized on a restart

  /** id is an endpoint Stub representing this server.  It holds the
      actual port the server is listening on */
  private Stub id;

  protected volatile boolean stopped;

  /** the listener thread */
  private Thread thread;

  /** if using NIO, this is the object used for accepting connections */
  private ServerSocketChannel channel;

  /** the server socket */
  private ServerSocket socket;

  /** a table of Connections from this conduit to others
   */
  private ConnectionTable conTable;

  /** <p>creates a new TCPConduit bound to the given InetAddress and port.
      The given ServerDelegate will receive any DistributionMessages
      passed to the conduit.</p>
      <p>This constructor forces the conduit to ignore the following
      system properties and look for them only in the <i>props</i> argument:</p>
      <pre>
      p2p.tcpBufferSize
      p2p.idleConnectionTimeout
      </pre>
  */
  public TCPConduit(MembershipManager mgr, int port,
      InetAddress address, boolean isBindAddress,
      DirectChannel receiver, Properties props)
    throws ConnectionException
  {
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
    if (this.stats == null) {
      this.stats = new LonerDistributionManager.DummyDMStats();
    }

    try {
      this.conTable = ConnectionTable.create(this);
    }
    catch (IOException io) {
      throw new ConnectionException(LocalizedStrings.TCPConduit_UNABLE_TO_INITIALIZE_CONNECTION_TABLE.toLocalizedString(), io);
    }
    this.useNIO = USE_NIO;
    if (this.useNIO) {
      InetAddress addr = address;
      if (addr == null) {
        try {
          addr = SocketCreator.getLocalHost();
        }
        catch (java.net.UnknownHostException e) {
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


  /** parse instance-level properties from the given object */
  private void parseProperties(Properties p) {
    if (p != null) {
      String s;
      s = p.getProperty("p2p.tcpBufferSize", ""+tcpBufferSize);
      try { tcpBufferSize = Integer.parseInt(s); } catch (Exception e) { logger.warn(LocalizedMessage.create(LocalizedStrings.TCPConduit_EXCEPTION_PARSING_P2PTCPBUFFERSIZE), e); }
      if (tcpBufferSize < Connection.SMALL_BUFFER_SIZE) {
        // enforce minimum
        tcpBufferSize = Connection.SMALL_BUFFER_SIZE;
      }
      s = p.getProperty("p2p.idleConnectionTimeout", ""+idleConnectionTimeout);
      try { idleConnectionTimeout = Integer.parseInt(s); } catch (Exception e) { logger.warn(LocalizedMessage.create(LocalizedStrings.TCPConduit_EXCEPTION_PARSING_P2PIDLECONNECTIONTIMEOUT), e); }
      
      s = p.getProperty("membership_port_range_start");
      try { tcpPortRange[0] = Integer.parseInt(s); } catch (Exception e) { logger.warn(LocalizedMessage.create(LocalizedStrings.TCPConduit_EXCEPTION_PARSING_TCPPORTRANGESTART), e); }
      
      s = p.getProperty("membership_port_range_end");
      try { tcpPortRange[1] = Integer.parseInt(s); } catch (Exception e) { logger.warn(LocalizedMessage.create(LocalizedStrings.TCPConduit_EXCEPTION_PARSING_TCPPORTRANGEEND), e); }
      
    }
  }

  private ThreadPoolExecutor hsPool;

  /** the reason for a shutdown, if abnormal */
  private volatile Exception shutdownCause;

  private final static int HANDSHAKE_POOL_SIZE = Integer.getInteger("p2p.HANDSHAKE_POOL_SIZE", 10).intValue();
  private final static long HANDSHAKE_POOL_KEEP_ALIVE_TIME = Long.getLong("p2p.HANDSHAKE_POOL_KEEP_ALIVE_TIME", 60).longValue();

  /** added to fix bug 40436 */
  public void setMaximumHandshakePoolSize(int maxSize) {
    if (this.hsPool != null && maxSize > HANDSHAKE_POOL_SIZE) {
      this.hsPool.setMaximumPoolSize(maxSize);
    }
  }

  /** binds the server socket and gets threads going
   *
   * */
  private void startAcceptor() throws ConnectionException {
    int localPort;
    int p = this.port;
    InetAddress ba = this.address;
    
    {
      ThreadPoolExecutor tmp_hsPool = null;
      String gName = "P2P-Handshaker " + ba + ":" + p;
      final ThreadGroup socketThreadGroup
        = LoggingThreadGroup.createThreadGroup(gName, logger);
                                          
      ThreadFactory socketThreadFactory = new ThreadFactory() {
          int connNum = -1;

          public Thread newThread(Runnable command) {
            int tnum;
            synchronized (this) {
              tnum = ++connNum;
            }
            String tName = socketThreadGroup.getName() + " Thread " + tnum;
            return new Thread(socketThreadGroup, command, tName);
          }
        };
      try {
        final BlockingQueue bq = new SynchronousQueue();
        final RejectedExecutionHandler reh = new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor pool) {
              try {
                bq.put(r);
              }
              catch (InterruptedException ex) {
                Thread.currentThread().interrupt(); // preserve the state
                throw new RejectedExecutionException(LocalizedStrings.TCPConduit_INTERRUPTED.toLocalizedString(), ex);
              }
            }
          };
        tmp_hsPool = new ThreadPoolExecutor(1,
                                            HANDSHAKE_POOL_SIZE,
                                            HANDSHAKE_POOL_KEEP_ALIVE_TIME,
                                            TimeUnit.SECONDS,
                                            bq,
                                            socketThreadFactory,
                                            reh);
      }
      catch (IllegalArgumentException poolInitException) {
        throw new ConnectionException(LocalizedStrings.TCPConduit_WHILE_CREATING_HANDSHAKE_POOL.toLocalizedString(), poolInitException);
      }
      this.hsPool = tmp_hsPool;
    }
    createServerSocket();
    try {
      localPort = socket.getLocalPort();

      id = new Stub(socket.getInetAddress(), localPort, 0);
      stopped = false;
      ThreadGroup group =
        LoggingThreadGroup.createThreadGroup("P2P Listener Threads", logger);
      thread = new Thread(group, this, "P2P Listener Thread " + id);
      thread.setDaemon(true);
      try { thread.setPriority(thread.getThreadGroup().getMaxPriority()); }
      catch (Exception e) {
        logger.info(LocalizedMessage.create(LocalizedStrings.TCPConduit_UNABLE_TO_SET_LISTENER_PRIORITY__0, e.getMessage()));
      }
      if (!Boolean.getBoolean("p2p.test.inhibitAcceptor")) {
        thread.start();
      }
      else {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.TCPConduit_INHIBITACCEPTOR));
        socket.close();
        this.hsPool.shutdownNow();
      }
    }
    catch (IOException io) {
      String s = "While creating ServerSocket and Stub on port " + p;
      throw new ConnectionException(s, io);
    }
    this.port = localPort;
  }
  
  /**
   * After startup we install the view ID into the conduit stub to avoid
   * confusion during overlapping shutdown/startup from the same member.
   * 
   * @param viewID
   */
  public void setVmViewID(int viewID) {
    this.id.setViewID(viewID);
  }


  /** creates the server sockets.  This can be used to recreate the
   *  socket using this.port and this.bindAddress, which must be set
   *  before invoking this method.
   */
  private void createServerSocket() {
    int p = this.port;
    int b = BACKLOG;
    InetAddress ba = this.address;
    
    try {
      if (this.useNIO) {
        if (p <= 0) {
          socket = SocketCreator.getDefaultInstance().createServerSocketUsingPortRange(ba, b, isBindAddress,
                    this.useNIO, 0, tcpPortRange);
        } else {
          ServerSocketChannel channl = ServerSocketChannel.open();
          socket = channl.socket();

          InetSocketAddress addr = new InetSocketAddress(isBindAddress ? ba : null, p);
          socket.bind(addr, b);
        }

        if (useNIO) {
          try {
            // set these buffers early so that large buffers will be allocated
            // on accepted sockets (see java.net.ServerSocket.setReceiverBufferSize javadocs)
            socket.setReceiveBufferSize(tcpBufferSize);
            int newSize = socket.getReceiveBufferSize();
            if (newSize != tcpBufferSize) {
              logger.info(LocalizedMessage.create(LocalizedStrings.TCPConduit_0_IS_1_INSTEAD_OF_THE_REQUESTED_2,
                  new Object[] {"Listener receiverBufferSize", Integer.valueOf(newSize), Integer.valueOf(tcpBufferSize)}));
            }
          } catch (SocketException ex) {
            logger.warn(LocalizedMessage.create(LocalizedStrings.TCPConduit_FAILED_TO_SET_LISTENER_RECEIVERBUFFERSIZE_TO__0, tcpBufferSize));
          }
        }
        channel = socket.getChannel();
      }
      else {
        try {
          if (p <= 0) {
            socket = SocketCreator.getDefaultInstance().createServerSocketUsingPortRange(ba, b, isBindAddress,
                      this.useNIO, this.tcpBufferSize, tcpPortRange);
          } else {
            socket = SocketCreator.getDefaultInstance().createServerSocket(p, b, isBindAddress? ba : null, this.tcpBufferSize);
          }
          int newSize = socket.getReceiveBufferSize();
          if (newSize != this.tcpBufferSize) {
            logger.info(LocalizedMessage.create(LocalizedStrings.TCPConduit_0_IS_1_INSTEAD_OF_THE_REQUESTED_2,
                new Object[] {"Listener receiverBufferSize", Integer.valueOf(newSize), Integer.valueOf(this.tcpBufferSize)}));
          }
        } catch (SocketException ex) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.TCPConduit_FAILED_TO_SET_LISTENER_RECEIVERBUFFERSIZE_TO__0, this.tcpBufferSize));
          
        }
      }
      port = socket.getLocalPort();
    }
    catch (IOException io) {
      throw new ConnectionException( LocalizedStrings.TCPConduit_EXCEPTION_CREATING_SERVERSOCKET.toLocalizedString(
              new Object[] {Integer.valueOf(p), ba}), io);
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
   * Close the ServerSocketChannel, ServerSocket, and the
   * ConnectionTable.
   * 
   * @see SystemFailure#emergencyClose()
   */
  public void emergencyClose() {
//    stop(); // Causes grief
    if (stopped) {
      return;
    }
    
    stopped = true;

//    System.err.println("DEBUG: TCPConduit emergencyClose");
    try {
      if (channel != null) {
        channel.close();
        // NOTE: do not try to interrupt the listener thread at this point.
        // Doing so interferes with the channel's socket logic.
      }
      else {
        if (socket != null) {
          socket.close();
        }
      }
    }
    catch (IOException e) {
      // ignore, please!
    }

    // this.hsPool.shutdownNow(); // I don't trust this not to allocate objects or to synchronize
//  this.conTable.close(); not safe against deadlocks
    ConnectionTable.emergencyClose();
    
    socket = null;
    thread = null;
    conTable = null;
    
//    System.err.println("DEBUG: end of TCPConduit emergencyClose");
  }
  
  /* stops the conduit, closing all tcp/ip connections */
  public void stop(Exception cause) {
    if (!stopped) {
      stopped = true;
      shutdownCause = cause;

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "Shutting down conduit");
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
        }
        else {
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
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.TCPConduit_UNABLE_TO_SHUT_DOWN_LISTENER_WITHIN_0_MS_UNABLE_TO_INTERRUPT_SOCKET_ACCEPT_DUE_TO_JDK_BUG_GIVING_UP,
              Integer.valueOf(LISTENER_CLOSE_TIMEOUT)));
        }
      } catch (IOException e) {
      } catch (InterruptedException e) {
        // Ignore, we're trying to stop already.
      }
      finally {
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
   * @since 3.0
   */
  public boolean isStopped() {
    return this.stopped;
  }

  /** starts the conduit again after it's been stopped.  This will clear the
      server map if the conduit's port is zero (wildcard bind) */
  public void restart() throws ConnectionException {
    if (!stopped)
      return;
    this.stats = null;
    if (directChannel != null) {
      this.stats = directChannel.getDMStats();
    }
    if (this.stats == null) {
      this.stats = new LonerDistributionManager.DummyDMStats();
    }
    try {
      this.conTable = ConnectionTable.create(this);
    }
    catch (IOException io) {
      throw new ConnectionException(LocalizedStrings.TCPConduit_UNABLE_TO_INITIALIZE_CONNECTION_TABLE.toLocalizedString(), io);
    }
    startAcceptor();
  }

  /** this is the server socket listener thread's run loop */
  public void run() {
    ConnectionTable.threadWantsSharedResources();
    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.trace(LogMarker.DM, "Starting P2P Listener on  {}", this.getId());
    }
    for(;;) {
      SystemFailure.checkFailure();
      if (stopper.cancelInProgress() != null) {
        break;
      }
      if (stopped) {
        break;
      }
      if (Thread.currentThread().isInterrupted()) {
        break;
      }
      if (stopper.cancelInProgress() != null) {
        break; // part of bug 37271
      }

      Socket othersock = null;
      try {
        if (this.useNIO) {
          SocketChannel otherChannel = channel.accept();          
          othersock = otherChannel.socket();
        }
        else {
          try {
            othersock = socket.accept();
          }
          catch (SSLException ex) {
            // SW: This is the case when there is a problem in P2P
            // SSL configuration, so need to exit otherwise goes into an
            // infinite loop just filling the logs
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.TCPConduit_STOPPING_P2P_LISTENER_DUE_TO_SSL_CONFIGURATION_PROBLEM), ex);
            break;
          }
          SocketCreator.getDefaultInstance().configureServerSSLSocket(
              othersock);
        }
        if (stopped) {
          try {
            if (othersock != null) {
              othersock.close();
            }
          }
          catch (Exception e) {
          }
          continue;
        }
        if (inhibitNewConnections) {
          if (logger.isTraceEnabled(LogMarker.QA)) {
            logger.trace(LogMarker.QA, "Test hook: inhibiting acceptance of connection {}", othersock);
          }
          othersock.close();
          while (inhibitNewConnections && !stopped) {
            this.stopper.checkCancelInProgress(null);
            boolean interrupted = Thread.interrupted();
            try { 
              Thread.sleep(2000); 
            }
            catch (InterruptedException e) {
              interrupted = true;
            }
            finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          } // while
          if (logger.isTraceEnabled(LogMarker.QA)) {
            logger.trace(LogMarker.QA, "Test hook: finished inhibiting acceptance of connections");
          }
        }
        else {
          acceptConnection(othersock);
        }
      }
      catch (ClosedByInterruptException cbie) {
        //safe to ignore
      }
      catch (ClosedChannelException e) {
        break; // we're dead
      }
      catch (CancelException e) {
        break;
      }
      catch (Exception e) {
        if (!stopped) {
          if (e instanceof SocketException && "Socket closed".equalsIgnoreCase(e.getMessage())) {
            // safe to ignore; see bug 31156
            if (!socket.isClosed()) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.TCPConduit_SERVERSOCKET_THREW_SOCKET_CLOSED_EXCEPTION_BUT_SAYS_IT_IS_NOT_CLOSED), e);
              try {
                socket.close();
                createServerSocket();
              }
              catch (IOException ioe) {
                logger.fatal(LocalizedMessage.create(LocalizedStrings.TCPConduit_UNABLE_TO_CLOSE_AND_RECREATE_SERVER_SOCKET), ioe);
                // post 5.1.0x, this should force shutdown
                try {
                  Thread.sleep(5000);
                }
                catch (InterruptedException ie) {
                  // Don't reset; we're just exiting the thread
                  logger.info(LocalizedMessage.create(
                      LocalizedStrings.TCPConduit_INTERRUPTED_AND_EXITING_WHILE_TRYING_TO_RECREATE_LISTENER_SOCKETS));
                  return;
                }
              }
            }
          } else {
            this.stats.incFailedAccept();
            if (e instanceof IOException && "Too many open files".equals(e.getMessage())) {
              getConTable().fileDescriptorsExhausted();
            } else {
              logger.warn(e.getMessage(), e);
            }
          }
        }
        //connections.cleanupLowWater();
      }
      if (!stopped && socket.isClosed()) {
        // NOTE: do not check for distributed system closing here.  Messaging
        // may need to occur during the closing of the DS or cache
        logger.warn(LocalizedMessage.create(LocalizedStrings.TCPConduit_SERVERSOCKET_CLOSED_REOPENING));
        try {
          createServerSocket();
        }
        catch (ConnectionException ex) {
          logger.warn(ex.getMessage(), ex);
        }
      }
    } // for

    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.debug("Stopped P2P Listener on  {}", this.getId());
    }
  }

  private void acceptConnection(final Socket othersock) {
    try {
      this.hsPool.execute(new Runnable() {
          public void run() {
            basicAcceptConnection(othersock);
          }
        });
    }
    catch (RejectedExecutionException rejected) {
      try {
        othersock.close();
      }
      catch (IOException ignore) {
      }
    }
  }
  private ConnectionTable getConTable() {
    ConnectionTable result = this.conTable;
    if (result == null) {
      stopper.checkCancelInProgress(null);
      throw new DistributedSystemDisconnectedException(LocalizedStrings.TCPConduit_TCP_LAYER_HAS_BEEN_SHUTDOWN.toLocalizedString());
    }
    return result;
  }
  protected void basicAcceptConnection(Socket othersock) {
    try {
      getConTable().acceptConnection(othersock);
    }
    catch (IOException io) {
      // exception is logged by the Connection
      if (!stopped) {
        this.stats.incFailedAccept();
      }
    }
    catch (ConnectionException ex) {
      // exception is logged by the Connection
      if (!stopped) {
        this.stats.incFailedAccept();
      }
    }
    catch (CancelException e) {
    }
    catch (Exception e) {
      if (!stopped) {
//        if (e instanceof SocketException
//            && "Socket closed".equals(e.getMessage())) {
//          // safe to ignore; see bug 31156
//        } 
//        else 
        {
          this.stats.incFailedAccept();
          logger.warn(LocalizedMessage.create(LocalizedStrings.TCPConduit_FAILED_TO_ACCEPT_CONNECTION_FROM_0_BECAUSE_1,
            new Object[] {othersock.getInetAddress(), e}), e);
        }
      }
      //connections.cleanupLowWater();
    }
  }
  
  /**
   * return true if "new IO" classes are being used for the server socket
   */
  protected boolean useNIO() {
    return this.useNIO;
  }

  /**
   * records the current outgoing message count on all thread-owned
   * ordered connections
   * @since 5.1
   */
  public void getThreadOwnedOrderedConnectionState(
    Stub member,
    HashMap result)
  {
    getConTable().getThreadOwnedOrderedConnectionState(member, result);
  }
  
  /**
   * wait for the incoming connections identified by the keys in the
   * argument to receive and dispatch the number of messages associated
   * with the key
   * @since 5.1
   */
  public void waitForThreadOwnedOrderedConnectionState(Stub member, HashMap channelState)
    throws InterruptedException
  {
    // if (Thread.interrupted()) throw new InterruptedException(); not necessary done in waitForThreadOwnedOrderedConnectionState
    getConTable().waitForThreadOwnedOrderedConnectionState(member, channelState);
  }

  /**
   * connections send messageReceived when a message object has been
   * read.
   * @param bytesRead number of bytes read off of network to get this message
  */
  protected void messageReceived(Connection receiver, DistributionMessage message,
                                 int bytesRead) {
    if (logger.isTraceEnabled()) {
      logger.trace("{} received {} from {}", id, message, receiver);
    }

    if (directChannel != null) {
      DistributionMessage msg = message;
      msg.setBytesRead(bytesRead);
      msg.setSender(receiver.getRemoteAddress());
      directChannel.receive(msg, bytesRead, receiver.getRemoteId());
    }
  }

  /** gets the Stub representing this conduit's ServerSocket endpoint.  This
      is used to generate other stubs containing endpoint information. */
  public Stub getId() {
    return id;
  }

  /** gets the actual port to which this conduit's ServerSocket is bound */
  public int getPort() {
    return id.getPort();
  }

  /**
   * Gets the local java groups address that identifies this conduit
   */
  public InternalDistributedMember getLocalAddress() {
    return this.localAddr;
  }
  /** gets the requested port that this TCPConduit bound to.  This could
      be zero if a wildcard bind was done */
  public int getBindPort() {
    return port;
  }

  
  /** gets the channel that is used to process non-Stub messages */
  public DirectChannel getDirectChannel() {
    return directChannel;
  }

  public InternalDistributedMember getMemberForStub(Stub s, boolean validate) {
    return membershipManager.getMemberForStub(s, validate);
  }
  
  public void setLocalAddr(InternalDistributedMember addr) {
    localAddr = addr;
  }
  
  public InternalDistributedMember getLocalId() {
    return localAddr;
  }

  /**
   * Return a connection to the given member.   This method must continue
   * to attempt to create a connection to the given member as long as that
   * member is in the membership view and the system is not shutting down.
   * 
   * @param memberAddress the IDS associated with the remoteId
   * @param remoteId the TCPConduit stub for this member
   * @param preserveOrder whether this is an ordered or unordered connection
   * @param retry false if this is the first attempt
   * @param startTime the time this operation started
   * @param ackTimeout the ack-wait-threshold * 1000 for the operation to be transmitted (or zero)
   * @param ackSATimeout the ack-severe-alert-threshold * 1000 for the operation to be transmitted (or zero)
   * @return the connection
   */
  public Connection getConnection(InternalDistributedMember memberAddress, Stub remoteId, final boolean preserveOrder, boolean retry,
      long startTime, long ackTimeout, long ackSATimeout)
    throws java.io.IOException, DistributedSystemDisconnectedException
  {
    //final boolean preserveOrder = (processorType == DistributionManager.SERIAL_EXECUTOR )|| (processorType == DistributionManager.PARTITIONED_REGION_EXECUTOR);
    if (stopped) {
      throw new DistributedSystemDisconnectedException(LocalizedStrings.TCPConduit_THE_CONDUIT_IS_STOPPED.toLocalizedString());
    }

    Connection conn = null;
    InternalDistributedMember memberInTrouble = null;
    boolean breakLoop = false;
    for (;;) {
      stopper.checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
      // If this is the second time through this loop, we had
      // problems.  Tear down the connection so that it gets
      // rebuilt.
      if (retry || conn != null) { // not first time in loop
        // Consult with the membership manager; if member has gone away,
        // there will not be an entry for this stub.
        InternalDistributedMember m = this.membershipManager.getMemberForStub(remoteId, true);
        if (m == null) {
          // OK, the member left.  Just register an error.
          throw new IOException(LocalizedStrings.TCPConduit_TCPIP_CONNECTION_LOST_AND_MEMBER_IS_NOT_IN_VIEW.toLocalizedString());
        }
        // bug35953: Member is still in view; we MUST NOT give up!
        
        // Pause just a tiny bit...
        try {
          Thread.sleep(100);
        }
        catch (InterruptedException e) {
          interrupted = true;
          stopper.checkCancelInProgress(e);
        }
        
        // try again after sleep
        m = this.membershipManager.getMemberForStub(remoteId, true);
        if (m == null) {
          // OK, the member left.  Just register an error.
          throw new IOException(LocalizedStrings.TCPConduit_TCPIP_CONNECTION_LOST_AND_MEMBER_IS_NOT_IN_VIEW.toLocalizedString());
        }
        
        // Print a warning (once)
        if (memberInTrouble == null) {
          memberInTrouble = m;
          logger.warn(LocalizedMessage.create(LocalizedStrings.TCPConduit_ATTEMPTING_TCPIP_RECONNECT_TO__0, memberInTrouble));
        }
        else {
          if (logger.isDebugEnabled()) {
            logger.debug("Attempting TCP/IP reconnect to {}", memberInTrouble);
          }
        }
        
        // Close the connection (it will get rebuilt later).
        this.stats.incReconnectAttempts();
        if (conn != null) {
          try { 
            if (logger.isDebugEnabled()) {
              logger.debug("Closing old connection.  conn={} before retrying. remoteID={} memberInTrouble={}",
                  conn, remoteId,  memberInTrouble);
            }
            conn.closeForReconnect("closing before retrying"); 
          } 
          catch (CancelException ex) {
            throw ex;
          }
          catch (Exception ex) {
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
          conn = getConTable().get(remoteId, preserveOrder, startTime, ackTimeout, ackSATimeout);
          if (conn == null) {
            // conduit may be closed - otherwise an ioexception would be thrown
            problem = new IOException(LocalizedStrings.TCPConduit_UNABLE_TO_RECONNECT_TO_SERVER_POSSIBLE_SHUTDOWN_0.toLocalizedString(remoteId));
          } else if (conn.isClosing() || !conn.getRemoteAddress().equals(memberAddress)) {
            if (logger.isDebugEnabled()) {
              logger.debug("Got an old connection for {}: {}@{}", memberAddress, conn, conn.hashCode());
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
      }
      catch (ConnectionException e) {
        // Race condition between acquiring the connection and attempting
        // to use it: another thread closed it.
        problem = e;
        // [sumedh] No need to retry since Connection.createSender has already
        // done retries and now member is really unreachable for some reason
        // even though it may be in the view
        breakLoop = true;
      }
      catch (IOException e) {
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
        InternalDistributedMember m = this.membershipManager.getMemberForStub(remoteId, true);
        if (m == null) { // left the view
          // Bracket our original warning
          if (memberInTrouble != null) {
            // make this msg info to bracket warning
            logger.info(LocalizedMessage.create(
                LocalizedStrings.TCPConduit_ENDING_RECONNECT_ATTEMPT_BECAUSE_0_HAS_DISAPPEARED, memberInTrouble));
          }
          throw new IOException(LocalizedStrings.TCPConduit_PEER_HAS_DISAPPEARED_FROM_VIEW.toLocalizedString(remoteId));
        } // left the view

        if (membershipManager.shutdownInProgress()) { // shutdown in progress
          // Bracket our original warning
          if (memberInTrouble != null) {
            // make this msg info to bracket warning
            logger.info(LocalizedMessage.create(
                LocalizedStrings.TCPConduit_ENDING_RECONNECT_ATTEMPT_TO_0_BECAUSE_SHUTDOWN_HAS_STARTED, memberInTrouble));
          }
          stopper.checkCancelInProgress(null);
          throw new DistributedSystemDisconnectedException(LocalizedStrings.TCPConduit_ABANDONED_BECAUSE_SHUTDOWN_IS_IN_PROGRESS.toLocalizedString());
        } // shutdown in progress
        
        // Log the warning.  We wait until now, because we want
        // to have m defined for a nice message...
        if (memberInTrouble == null) {
          logger.warn(LocalizedMessage.create(
          LocalizedStrings.TCPConduit_ERROR_SENDING_MESSAGE_TO_0_WILL_REATTEMPT_1,
          new Object[] {m, problem}));
          memberInTrouble = m;
        }
        else {
          if (logger.isDebugEnabled()) {
            logger.debug("Error sending message to {}", m, problem);
          }
        }

        if (breakLoop) {
          if (!problem.getMessage().startsWith("Cannot form connection to alert listener")) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.TCPConduit_THROWING_IOEXCEPTION_AFTER_FINDING_BREAKLOOP_TRUE),
                  problem);
          }
          if (problem instanceof IOException) {
            throw (IOException)problem;
          }
          else {
            IOException ioe = new IOException( LocalizedStrings.TCPConduit_PROBLEM_CONNECTING_TO_0.toLocalizedString(remoteId));
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
        logger.info(LocalizedMessage.create(
            LocalizedStrings.TCPConduit_SUCCESSFULLY_RECONNECTED_TO_MEMBER_0,
            memberInTrouble));
        if (logger.isTraceEnabled()) {
          logger.trace("new connection is {} remoteId={} memberAddress={}", conn, remoteId, memberAddress);
      }
      }
      return conn;
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // for(;;)
  }

//   /**
//    * Send a message.
//    * @return the connection used to send the message
//    * @throws IOException if peer departed view or shutdown in progress
//    */
//   private Connection send(Stub remoteId, ByteBuffer bb, boolean preserveOrder, DistributionMessage msg)
//     throws java.io.IOException
//   {
//     if (stopped) {
//       throw new ConduitStoppedException("The conduit is stopped");
//     }

//     if (!QUIET) {
//       LogWriterI18n l = getLogger();
//       if (l.finerEnabled()) {
//         l.finer(id.toString() + " sending " + bb
//                 + " to " + remoteId);
//       }
//     }

//     Connection conn = null;
//     InternalDistributedMember memberInTrouble = null;
//     for (;;) {
//       // If this is the second time through this loop, we had
//       // problems.  Tear down the connection so that it gets
//       // rebuilt.
//       if (conn != null) { // not first time in loop
//         // Consult with the membership manager; if member has gone away,
//         // there will not be an entry for this stub.
//         InternalDistributedMember m = membershipManager.getMemberForStub(remoteId);
//         if (m == null) {
//           // OK, the member left.  Just register an error.
//           throw new IOException("TCP/IP connection lost and member no longer in view");
//         }
//         // bug35953: Member is still in view; we MUST NOT give up!
        
//         // Pause just a tiny bit...
//         try {
//           Thread.sleep(5000);
//         }
//         catch (InterruptedException e) {
//           Thread.currentThread().interrupt();
//           if (membershipManager.shutdownInProgress()) { // shutdown in progress
//             // Bracket our original warning
//             if (memberInTrouble != null) {
//               logger.info("Ending retry attempt because shutdown has started.");
//             }
//             throw new IOException("Abandoned because shutdown is in progress");
//           } // shutdown in progress
          
//           // Strange random interrupt intercepted?
//           logger.warning("Thread has been interrupted but no shutdown in progress", e);
//           throw new DistributedSystemDisconnectedException(e);
//         }
        
//         // Print a warning (once)
//         if (memberInTrouble == null) {
//           memberInTrouble = m;
//           getLogger().warning("Attempting TCP/IP reconnect to " + memberInTrouble); 
//         }
//         else {
//           getLogger().fine("Attempting TCP/IP reconnect to " + memberInTrouble);
//         }
        
//         // Close the connection (it will get rebuilt later).
//         this.stats.incReconnectAttempts();
//         try { 
//           conn.closeForReconnect("closing before retrying"); 
//           } 
//         catch (CancelException ex) {
//           // In general we ignore close problems, but if the system
//           // is shutting down, we should just quit.
//           throw ex;
//         }
//         catch (Exception ex) {
//           }
//       } // not first time in loop
      
//       // Do the send
//       Exception problem = null;
//       try {
//         // Get (or regenerate) the connection
//         // bug36202: this could generate a ConnectionException, so it
//         // must be caught and retried
//         conn = getConTable().get(remoteId, preserveOrder);
//         //      getLogger().info ("connections returned " + conn);
//         if (conn == null) {
//           // conduit may be closed - otherwise an ioexception would be thrown
//           throw new IOException("Unable to reconnect to server; possible shutdown: " 
//               + remoteId);
//         }

//         conn.sendPreserialized(bb, msg);
//       }
//       catch (ConnectionException e) {
//         // Race condition between acquiring the connection and attempting
//         // to use it: another thread closed it.
//         problem = e;
//       }
//       catch (IOException e) {
//         problem = e;
//       }

//       if (problem != null) {
//         // Some problems are not recoverable; check an error out early.
//         InternalDistributedMember m = membershipManager.getMemberForStub(remoteId);
//         if (m == null) { // left the view
//           // Bracket our original warning
//           if (memberInTrouble != null) {
//             logger.info("Ending retry attempt because " + memberInTrouble 
//                 + " has disappeared.");
//           }
//           throw new IOException("Peer has disappeared from view");
//         } // left the view
        
//         if (membershipManager.shutdownInProgress()) { // shutdown in progress
//           // Bracket our original warning
//           if (memberInTrouble != null) {
//             logger.info("Ending retry attempt because shutdown has started.");
//           }
//           throw new IOException("Abandoned because shutdown is in progress");
//         } // shutdown in progress
        
//         if (endpointRemoved(remoteId)) { // endpoint removed
//           // TODO what does this mean?
//           // Bracket our original warning
//           if (memberInTrouble != null) {
//             logger.info("Ending retry attempt because " + memberInTrouble 
//                 + " has lost its endpoint.");
//           }
//           throw new IOException("Endpoint was removed");
//         } // endpoint removed
        
//         // Log the warning.  We wait until now, because we want
//         // to have m defined for a nice message...
//         if (memberInTrouble == null) {
//           logger.warning(
//               "Error sending message to " + m + " (will reattempt): " 
//                   + problem.toString(), 
//               logger.finerEnabled() ? problem : null);
//           memberInTrouble = m;
//         }
//         else {
//           logger.fine("Error sending message to " + m, problem);
//         }
        
//         // Retry the operation (indefinitely)
//         continue;
//       } // problem != null
//       // Success!
      
//       // Make sure our logging is bracketed if there was a problem
//       if (memberInTrouble != null) {
//         logger.info("Successfully reestablished connection to server " 
//             + memberInTrouble);
//       }
//       return conn;
//     } // while retry
//   }

//   /**
//    * Sends an already serialized message in a byte buffer
//    * to the given endpoint. Waits for the send to complete
//    * before returning.
//    * @return the connection used to send the message
//    */
//   public Connection sendSync(Stub remoteId, ByteBuffer bb, int processorType, DistributionMessage msg)
//     throws java.io.IOException
//   {
//     return send(remoteId, bb,
//                 processorType == DistributionManager.SERIAL_EXECUTOR,
//                 msg);
//   }

  @Override
  public String toString() {
    return "" + id;
  }

  public boolean threadOwnsResources() {
    ConnectionTable ct = this.conTable;
    if (ct == null) {
      return false;
    } else {
      DM d = getDM();
      if (d != null) {
        return d.getSystem().threadOwnsResources();
      } else {
        return false;
      }
    }
  }
  /**
   * Returns the distribution manager of the direct channel
   */
  public DM getDM() {
    return directChannel.getDM();
  }
  /**
   * Closes any connections used to communicate with the given stub
   */
  public void removeEndpoint(Stub stub, String reason) {
    removeEndpoint(stub, reason, true);
  }
  
  public void removeEndpoint(Stub stub, String reason, boolean notifyDisconnect) {
    ConnectionTable ct = this.conTable;
    if (ct == null) {
      return;
    }
    ct.removeEndpoint(stub, reason, notifyDisconnect);
  }
  
  /** check to see if there are still any receiver threads for the given end-point */
  public boolean hasReceiversFor(Stub endPoint) {
    ConnectionTable ct = this.conTable;
    return (ct != null) && ct.hasReceiversFor(endPoint);
  }
  
  protected class Stopper extends CancelCriterion {

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.CancelCriterion#cancelInProgress()
     */
    @Override
    public String cancelInProgress() {
      DM dm = getDM();
      if (dm == null) {
        return "no distribution manager";
      }
      if (TCPConduit.this.stopped) {
        return "Conduit has been stopped";
      }
      return null;
    }
    
    /* (non-Javadoc)
     * @see com.gemstone.gemfire.CancelCriterion#generateCancelledException(java.lang.Throwable)
     */
    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      }
      DM dm = getDM();
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
   * if the conduit is disconnected due to an abnormal condition, this
   * will describe the reason
   * @return exception that caused disconnect
   */
  public Exception getShutdownCause() {
    return this.shutdownCause;
  }
  
  /**
   * ARB: Called by Connection before handshake reply is sent.
   * Returns true if member is part of view, false if membership is not confirmed before timeout.
   */
  public boolean waitForMembershipCheck(InternalDistributedMember remoteId) {
    return membershipManager.waitForMembershipCheck(remoteId);
  }
  
  /**
   * simulate being sick
   */
  public void beSick() {
    this.inhibitNewConnections = true;
    this.conTable.closeReceivers(true);
  }
  
  /**
   * simulate being healthy
   */
  public void beHealthy() {
    this.inhibitNewConnections = false;
  }

}

