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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR_PP;
import static org.apache.geode.internal.cache.tier.CommunicationMode.ClientToServerForQueue;
import static org.apache.geode.internal.cache.tier.sockets.Handshake.REPLY_REFUSED;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.SystemFailure;
import org.apache.geode.ToDataException;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketAdvisor.BucketProfile;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.AllBucketProfilesUpdateMessage;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.cache.wan.GatewayReceiverStats;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.logging.CoreLoggingExecutors;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsClockFactory;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.internal.util.ArrayUtils;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory.CommandWrapper;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory.ThreadInitializer;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Implements the acceptor thread on the cache server. Accepts connections from the edge and starts
 * up threads to process requests from these.
 *
 * @since GemFire 2.0.2
 */
@SuppressWarnings("deprecation")
public class AcceptorImpl implements Acceptor, Runnable {
  private static final Logger logger = LogService.getLogger();

  private static final boolean isJRockit = System.getProperty("java.vm.name").contains("JRockit");
  private static final int HANDSHAKER_DEFAULT_POOL_SIZE = 4;
  private static final int CLIENT_QUEUE_INITIALIZATION_POOL_SIZE = 16;

  private final CacheServerStats stats;
  private final int maxConnections;
  private final int maxThreads;

  private final ExecutorService pool;
  /**
   * A pool used to process handshakes.
   */
  private final ExecutorService hsPool;

  /**
   * A pool used to process client-queue-initializations.
   */
  private final ExecutorService clientQueueInitPool;

  /**
   * The port on which this acceptor listens for client connections
   */
  private final int localPort;

  /**
   * The server socket that handles requests for connections
   */
  private ServerSocket serverSock;

  /**
   * The GemFire cache served up by this acceptor
   */
  private final InternalCache cache;

  /**
   * Caches region information
   */
  private final CachedRegionHelper crHelper;

  /**
   * A lock to prevent close from occurring while creating a ServerConnection
   */
  private final Object syncLock = new Object();

  /**
   * THE selector for the cache server; null if no selector.
   */
  private final Selector selector;

  /**
   * Used for managing direct byte buffer for client comms; null if no selector.
   */
  private final LinkedBlockingQueue<ByteBuffer> commBufferQueue;
  /**
   * Used to timeout accepted sockets that we are waiting for the handshake packet
   */
  private final SystemTimer hsTimer;
  /**
   * A queue used to feed register requests to the selector; null if no selector.
   */
  private final LinkedBlockingQueue<ServerConnection> selectorQueue;
  /**
   * All the objects currently registered with selector.
   */
  private final Set<ServerConnection> selectorRegistrations;
  /**
   * tcpNoDelay setting for outgoing sockets
   */
  private final boolean tcpNoDelay;

  /**
   * The name of a system property that sets the hand shake timeout (in milliseconds). This is how
   * long a client will wait to hear back from a server.
   */
  private static final String HANDSHAKE_TIMEOUT_PROPERTY_NAME = "BridgeServer.handShakeTimeout";

  /**
   * The default value of the {@link #HANDSHAKE_TIMEOUT_PROPERTY_NAME} system property.
   */
  private static final int DEFAULT_HANDSHAKE_TIMEOUT_MS = 59000;

  /**
   * Test value for handshake timeout
   */
  private static final int handshakeTimeout =
      Integer.getInteger(HANDSHAKE_TIMEOUT_PROPERTY_NAME, DEFAULT_HANDSHAKE_TIMEOUT_MS);

  /**
   * The name of a system property that sets the accept timeout (in milliseconds). This is how long
   * a server will wait to get its first byte from a client it has just accepted.
   */
  public static final String ACCEPT_TIMEOUT_PROPERTY_NAME = "BridgeServer.acceptTimeout";

  /**
   * The default value of the {@link #ACCEPT_TIMEOUT_PROPERTY_NAME} system property.
   */
  private static final int DEFAULT_ACCEPT_TIMEOUT_MS = 9900;

  /**
   * Test value for accept timeout
   */
  private final int acceptTimeout =
      Integer.getInteger(ACCEPT_TIMEOUT_PROPERTY_NAME, DEFAULT_ACCEPT_TIMEOUT_MS);

  /**
   * The minimum value of max-connections
   */
  static final int MINIMUM_MAX_CONNECTIONS = 16;

  /**
   * The buffer size for server-side sockets.
   */
  private final int socketBufferSize;

  /**
   * Notifies clients of updates
   */
  private final CacheClientNotifier clientNotifier;

  /**
   * The default value of the {@link ServerSocket} {@link #BACKLOG_PROPERTY_NAME}system property
   */
  private static final int DEFAULT_BACKLOG = 1280;

  /**
   * The system property name for setting the {@link ServerSocket}backlog
   */
  private static final String BACKLOG_PROPERTY_NAME = "BridgeServer.backlog";

  /**
   * The name of a system property that Defines the time interval (in nano-seconds) with which
   * checkRegisteredKeys function can be called.
   */
  private static final String CHECK_REGISTERED_KEYS_INTERVAL_NAME =
      "check-registered-keys-interval-ns";

  /**
   * The default value of {@link #CHECK_REGISTERED_KEYS_INTERVAL_NAME} system property.
   */
  private static final int DEFAULT_CHECK_REGISTERED_KEYS_INTERVAL_NS = 0;

  /**
   * Set value of check registered keys interval
   */
  private final long checkRegisteredKeysInterval = Long
      .getLong(CHECK_REGISTERED_KEYS_INTERVAL_NAME, DEFAULT_CHECK_REGISTERED_KEYS_INTERVAL_NS);

  /**
   * Current number of ServerConnection instances that are CLIENT_TO_SERVER cons.
   */
  private final AtomicInteger clientServerCnxCount = new AtomicInteger();

  /**
   * Has this acceptor been shut down
   */
  private volatile boolean shutdownStarted;

  /**
   * The thread that runs the acceptor
   */
  private Thread thread;

  /**
   * The thread that runs the selector loop if any
   */
  private Thread selectorThread;

  /**
   * Controls updates to {@link #allSCs}
   */
  private final Object allSCsLock = new Object();

  /**
   * List of ServerConnection.
   *
   * Instances added when constructed; removed when terminated.
   *
   * guarded.By {@link #allSCsLock}
   */
  private final Set<ServerConnection> allSCs = new HashSet<>();

  /**
   * List of ServerConnections, for {@link #emergencyClose()}
   *
   * guarded.By {@link #allSCsLock}
   */
  private volatile ServerConnection[] allSCList = new ServerConnection[0];

  /**
   * The ip address or host name this acceptor is to bind to; {@code null} or "" indicates it
   * will listen on all local addresses.
   *
   * @since GemFire 5.7
   */
  private final String bindHostName;

  /**
   * A listener for connect/disconnect events
   */
  private final ConnectionListener connectionListener;

  /**
   * The client health monitor tracking connections for this acceptor
   */
  private final ClientHealthMonitor healthMonitor;

  /**
   * bridge's setting of notifyBySubscription
   */
  private final boolean notifyBySubscription;

  /**
   * The AcceptorImpl identifier, used to identify the clients connected to this Acceptor.
   */
  private long acceptorId;

  @MakeNotStatic
  private static boolean isAuthenticationRequired;

  @MakeNotStatic
  private static boolean isPostAuthzCallbackPresent;

  private final boolean isGatewayReceiver;

  private final List<GatewayTransportFilter> gatewayTransportFilters;
  private final StatisticsClock statisticsClock;

  private final SocketCreator socketCreator;

  private final SecurityService securityService;

  private final ServerConnectionFactory serverConnectionFactory;

  /**
   * Constructs an AcceptorImpl for use within a CacheServer.
   *
   * <p>
   * Initializes this acceptor thread to listen for connections on the given port.
   *
   * @param port The port on which this acceptor listens for connections. If {@code 0}, a
   *        random port will be chosen.
   * @param bindHostName The ip address or host name this acceptor listens on for connections. If
   *        {@code null} or "" then all local addresses are used
   * @param socketBufferSize The buffer size for server-side sockets
   * @param maximumTimeBetweenPings The maximum time between client pings. This value is used by the
   *        {@code ClientHealthMonitor} to monitor the health of this server's clients.
   * @param internalCache The GemFire cache whose contents is served to clients
   * @param maxConnections the maximum number of connections allowed in the server pool
   * @param maxThreads the maximum number of threads allowed in the server pool
   * @param securityService the SecurityService to use for authentication and authorization
   *
   * @see ClientHealthMonitor
   *
   * @since GemFire 5.7
   */
  AcceptorImpl(final int port, final String bindHostName, final boolean notifyBySubscription,
      final int socketBufferSize, final int maximumTimeBetweenPings,
      final InternalCache internalCache, final int maxConnections, final int maxThreads,
      final int maximumMessageCount, final int messageTimeToLive,
      final ConnectionListener connectionListener, final OverflowAttributes overflowAttributes,
      final boolean tcpNoDelay, final ServerConnectionFactory serverConnectionFactory,
      final long timeLimitMillis, final SecurityService securityService,
      final Supplier<SocketCreator> socketCreatorSupplier,
      final CacheClientNotifierProvider cacheClientNotifierProvider,
      final ClientHealthMonitorProvider clientHealthMonitorProvider) throws IOException {
    this(port, bindHostName, notifyBySubscription, socketBufferSize, maximumTimeBetweenPings,
        internalCache, maxConnections, maxThreads, maximumMessageCount, messageTimeToLive,
        connectionListener, overflowAttributes, tcpNoDelay, serverConnectionFactory,
        timeLimitMillis, securityService, socketCreatorSupplier, cacheClientNotifierProvider,
        clientHealthMonitorProvider, false, Collections.emptyList(),
        StatisticsClockFactory.disabledClock());
  }

  /**
   * Constructs an AcceptorImpl for use within a GatewayReceiver.
   *
   * <p>
   * Initializes this acceptor thread to listen for connections on the given port.
   *
   * @param port The port on which this acceptor listens for connections. If {@code 0}, a
   *        random port will be chosen.
   * @param notifyBySubscription the bridges setting whether if it to be notified by subscription
   * @param maximumMessageCount maximum message count setting in the Cache Client Notifier
   * @param messageTimeToLive message time to live setting in the Cache Client Notifier
   * @param connectionListener listener to detect if connect or disconnect events
   * @param overflowAttributes overflow attributes of Cache Client Notifier
   * @param tcpNoDelay TCP delay for the outgoing sockets
   * @param serverConnectionFactory server connection factory for the client
   * @param timeLimitMillis time limit to wait attemping to bind to a server socket
   * @param socketCreatorSupplier socket creator for the server connection
   * @param cacheClientNotifierProvider collection of cache client notifiers
   * @param clientHealthMonitorProvider collection of clinet health monitors
   * @param isGatewayReceiver flag to determine if member is gateway receiver
   * @param statisticsClock maintains the JVM's clock
   * @param bindHostName The ip address or host name this acceptor listens on for connections. If
   *        {@code null} or "" then all local addresses are used
   * @param socketBufferSize The buffer size for server-side sockets
   * @param maximumTimeBetweenPings The maximum time between client pings. This value is used by the
   *        {@code ClientHealthMonitor} to monitor the health of this server's clients.
   * @param internalCache The GemFire cache whose contents is served to clients
   * @param maxConnections the maximum number of connections allowed in the server pool
   * @param maxThreads the maximum number of threads allowed in the server pool
   * @param securityService the SecurityService to use for authentication and authorization
   * @param gatewayTransportFilters List of GatewayTransportFilters
   */
  AcceptorImpl(final int port, final String bindHostName, final boolean notifyBySubscription,
      final int socketBufferSize, final int maximumTimeBetweenPings,
      final InternalCache internalCache, final int maxConnections, final int maxThreads,
      final int maximumMessageCount, final int messageTimeToLive,
      final ConnectionListener connectionListener,
      final OverflowAttributes overflowAttributes,
      final boolean tcpNoDelay, final ServerConnectionFactory serverConnectionFactory,
      final long timeLimitMillis, final SecurityService securityService,
      final Supplier<SocketCreator> socketCreatorSupplier,
      final CacheClientNotifierProvider cacheClientNotifierProvider,
      final ClientHealthMonitorProvider clientHealthMonitorProvider,
      final boolean isGatewayReceiver,
      final List<GatewayTransportFilter> gatewayTransportFilters,
      final StatisticsClock statisticsClock) throws IOException {
    this.securityService = securityService;
    this.statisticsClock = statisticsClock;

    this.isGatewayReceiver = isGatewayReceiver;
    this.gatewayTransportFilters = gatewayTransportFilters;

    this.bindHostName = calcBindHostName(internalCache, bindHostName);
    this.connectionListener =
        connectionListener == null ? new ConnectionListenerAdapter() : connectionListener;
    this.notifyBySubscription = notifyBySubscription;
    this.serverConnectionFactory = serverConnectionFactory;

    {
      int tmp_maxConnections = maxConnections;
      if (tmp_maxConnections < MINIMUM_MAX_CONNECTIONS) {
        tmp_maxConnections = MINIMUM_MAX_CONNECTIONS;
      }
      this.maxConnections = tmp_maxConnections;
    }
    {
      int tmp_maxThreads = maxThreads;
      if (maxThreads == CacheServer.DEFAULT_MAX_THREADS) {
        // consult system properties for 5.0.2 backwards compatibility
        if (DEPRECATED_SELECTOR) {
          tmp_maxThreads = DEPRECATED_SELECTOR_POOL_SIZE;
        }
      }
      if (tmp_maxThreads < 0) {
        tmp_maxThreads = 0;
      } else if (tmp_maxThreads > this.maxConnections) {
        tmp_maxThreads = this.maxConnections;
      }
      boolean isWindows = false;
      String os = System.getProperty("os.name");
      if (os != null) {
        if (os.contains("Windows")) {
          isWindows = true;
        }
      }
      if (tmp_maxThreads > 0 && isWindows) {
        // bug #40472 and JDK bug 6230761 - NIO can't be used with IPv6 on Windows
        if (getBindAddress() instanceof Inet6Address) {
          logger.warn(
              "Ignoring max-threads setting and using zero instead due to JRockit NIO bugs.  See GemFire bug #40198");
          tmp_maxThreads = 0;
        }
        // bug #40198 - Selector.wakeup() hangs if VM starts to exit
        if (isJRockit) {
          logger.warn(
              "Ignoring max-threads setting and using zero instead due to Java bug 6230761: NIO does not work with IPv6 on Windows.  See GemFire bug #40472");
          tmp_maxThreads = 0;
        }
      }
      this.maxThreads = tmp_maxThreads;
    }
    {
      Selector tmp_s = null;
      LinkedBlockingQueue<ServerConnection> tmp_q = null;
      LinkedBlockingQueue<ByteBuffer> tmp_commQ = null;
      Set<ServerConnection> tmp_hs = null;
      SystemTimer tmp_timer = null;
      if (isSelector()) {
        tmp_s = Selector.open(); // no longer catch ex to fix bug 36907
        tmp_q = new LinkedBlockingQueue<>();
        tmp_commQ = new LinkedBlockingQueue<>();
        tmp_hs = new HashSet<>(512);
        tmp_timer = new SystemTimer(internalCache.getDistributedSystem(), true);
      }
      selector = tmp_s;
      selectorQueue = tmp_q;
      commBufferQueue = tmp_commQ;
      selectorRegistrations = tmp_hs;
      hsTimer = tmp_timer;
      this.tcpNoDelay = tcpNoDelay;
    }

    {
      socketCreator = socketCreatorSupplier.get();

      final InternalCache gc;
      if (getCachedRegionHelper() != null) {
        gc = getCachedRegionHelper().getCache();
      } else {
        gc = null;
      }
      final int backLog = Integer.getInteger(BACKLOG_PROPERTY_NAME, DEFAULT_BACKLOG);
      final long tilt = System.currentTimeMillis() + timeLimitMillis;

      if (isSelector()) {
        if (socketCreator.forCluster().useSSL()) {
          throw new IllegalArgumentException(
              "Selector thread pooling can not be used with client/server SSL. The selector can be disabled by setting max-threads=0.");
        }
        ServerSocketChannel channel = ServerSocketChannel.open();
        serverSock = channel.socket();
        serverSock.setReuseAddress(true);

        // Set the receive buffer size before binding the socket so that large
        // buffers will be allocated on accepted sockets (see
        // java.net.ServerSocket.setReceiverBufferSize javadocs)
        serverSock.setReceiveBufferSize(socketBufferSize);

        // fix for bug 36617. If BindException is thrown, retry after
        // sleeping. The server may have been stopped and then
        // immediately restarted, which sometimes results in a bind exception
        for (;;) {
          try {
            serverSock.bind(new InetSocketAddress(getBindAddress(), port), backLog);
            break;
          } catch (SocketException b) {
            if (System.currentTimeMillis() > tilt) {
              throw b;
            }
          }

          boolean interrupted = Thread.interrupted();
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            interrupted = true;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
          if (gc != null) {
            gc.getCancelCriterion().checkCancelInProgress(null);
          }
        } // for
      } // isSelector
      else { // !isSelector
        // fix for bug 36617. If BindException is thrown, retry after
        // sleeping. The server may have been stopped and then
        // immediately restarted, which sometimes results in a bind exception
        for (;;) {
          try {
            serverSock = socketCreator.createServerSocket(port, backLog, getBindAddress(),
                this.gatewayTransportFilters, socketBufferSize);
            break;
          } catch (SocketException e) {
            if (System.currentTimeMillis() > tilt) {
              throw e;
            }
          }

          boolean interrupted = Thread.interrupted();
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            interrupted = true;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
          if (gc != null) {
            gc.getCancelCriterion().checkCancelInProgress(null);
          }
        } // for
      } // !isSelector

      if (port == 0) {
        localPort = serverSock.getLocalPort();
      } else {
        localPort = port;
      }
      {
        InternalDistributedSystem ds = internalCache.getInternalDistributedSystem();
        if (ds != null) {
          DistributionManager dm = ds.getDistributionManager();
          if (dm != null && dm.getDistributionManagerId().getMembershipPort() == 0
              && dm instanceof LonerDistributionManager) {
            // a server with a loner distribution manager - update it's port number
            ((LonerDistributionManager) dm).updateLonerPort(localPort);
          }
        }
      }
      String sockName = getServerName();
      logger.info("Cache server connection listener bound to address {} with backlog {}.",
          sockName, backLog);
      StatisticsFactory statisticsFactory =
          internalCache.getInternalDistributedSystem().getStatisticsManager();
      if (isGatewayReceiver()) {
        MeterRegistry meterRegistry = internalCache.getMeterRegistry();
        stats = GatewayReceiverStats.createGatewayReceiverStats(statisticsFactory, sockName,
            meterRegistry);
      } else {
        stats = new CacheServerStats(statisticsFactory, sockName);
      }
    }

    cache = internalCache;
    crHelper = new CachedRegionHelper(cache);

    clientNotifier =
        cacheClientNotifierProvider.get(internalCache, new ClientRegistrationEventQueueManager(),
            statisticsClock, stats, maximumMessageCount,
            messageTimeToLive, this.connectionListener, overflowAttributes, isGatewayReceiver());

    this.socketBufferSize = socketBufferSize;

    // Create the singleton ClientHealthMonitor
    healthMonitor = clientHealthMonitorProvider.get(internalCache, maximumTimeBetweenPings,
        clientNotifier.getStats());

    pool = initializeServerConnectionThreadPool();
    hsPool = initializeHandshakerThreadPool();
    clientQueueInitPool = initializeClientQueueInitializerThreadPool();

    isAuthenticationRequired = securityService.isClientSecurityRequired();

    String postAuthzFactoryName =
        cache.getDistributedSystem().getProperties().getProperty(SECURITY_CLIENT_ACCESSOR_PP);

    isPostAuthzCallbackPresent =
        postAuthzFactoryName != null && !postAuthzFactoryName.isEmpty();
  }

  private ExecutorService initializeHandshakerThreadPool() throws IOException {
    String threadName =
        "Handshaker " + serverSock.getInetAddress() + ":" + localPort + " Thread ";
    try {
      logger.warn("Handshaker max Pool size: " + HANDSHAKE_POOL_SIZE);
      return CoreLoggingExecutors.newThreadPoolWithSynchronousFeedThatHandlesRejection(threadName,
          thread -> getStats().incAcceptThreadsCreated(), null, 1, HANDSHAKE_POOL_SIZE, 60);
    } catch (IllegalArgumentException poolInitException) {
      stats.close();
      serverSock.close();
      pool.shutdown();
      throw poolInitException;
    }
  }

  private ExecutorService initializeClientQueueInitializerThreadPool() {
    return CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(
        "Client Queue Initialization Thread ",
        command -> {
          try {
            command.run();
          } catch (CancelException e) {
            logger.debug("Client Queue Initialization was canceled.", e);
          }
        }, CLIENT_QUEUE_INITIALIZATION_POOL_SIZE, getStats().getCnxPoolHelper(), 60000,
        getThreadMonitorObj());
  }

  private ExecutorService initializeServerConnectionThreadPool() throws IOException {
    ThreadInitializer threadInitializer = thread -> getStats().incConnectionThreadsCreated();
    CommandWrapper commandWrapper = command -> {
      try {
        command.run();
      } catch (CancelException e) { // bug 39463
        // ignore
      } finally {
        ConnectionTable.releaseThreadsSockets();
      }
    };
    try {
      String threadName = "ServerConnection on port " + localPort + " Thread ";
      if (isSelector()) {
        return CoreLoggingExecutors.newThreadPoolWithUnlimitedFeed(threadName, threadInitializer,
            commandWrapper, maxThreads,
            getStats().getCnxPoolHelper(), Integer.MAX_VALUE, getThreadMonitorObj());
      }
      return CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(threadName, threadInitializer,
          commandWrapper,
          MINIMUM_MAX_CONNECTIONS, maxConnections, 0L);
    } catch (IllegalArgumentException poolInitException) {
      stats.close();
      serverSock.close();
      throw poolInitException;
    }
  }

  private ThreadsMonitoring getThreadMonitorObj() {
    DistributionManager distributionManager = cache.getDistributionManager();
    if (distributionManager != null) {
      return distributionManager.getThreadMonitoring();
    }
    return null;
  }

  @Override
  public long getAcceptorId() {
    return acceptorId;
  }

  @Override
  public CacheServerStats getStats() {
    return stats;
  }

  /**
   * Returns true if this acceptor is using a selector to detect client events.
   */
  @Override
  public boolean isSelector() {
    return maxThreads > 0;
  }

  /**
   * This system property is only used if max-threads == 0. This is for 5.0.2 backwards
   * compatibility.
   *
   * @deprecated since 5.1 use cache-server max-threads instead
   */
  @Deprecated
  private static final boolean DEPRECATED_SELECTOR = Boolean.getBoolean("BridgeServer.SELECTOR");

  /**
   * This system property is only used if max-threads == 0. This is for 5.0.2 backwards
   * compatibility.
   *
   * @deprecated since 5.1 use cache-server max-threads instead
   */
  @Deprecated
  private final int DEPRECATED_SELECTOR_POOL_SIZE =
      Integer.getInteger("BridgeServer.SELECTOR_POOL_SIZE", 16);
  private final int HANDSHAKE_POOL_SIZE = Integer
      .getInteger("BridgeServer.HANDSHAKE_POOL_SIZE", HANDSHAKER_DEFAULT_POOL_SIZE);

  @Override
  public void start() {
    // This thread should not be a daemon to keep BridgeServers created
    // in code from exiting immediately.
    thread =
        new LoggingThread("Cache Server Acceptor " + serverSock.getInetAddress() + ":"
            + localPort + " local port: " + serverSock.getLocalPort(), false, this);
    acceptorId = thread.getId();
    thread.start();

    if (isSelector()) {
      selectorThread =
          new LoggingThread("Cache Server Selector " + serverSock.getInetAddress() + ":"
              + localPort + " local port: " + serverSock.getLocalPort(),
              false,
              this::runSelectorLoop);
      selectorThread.start();
    }
    Set<PartitionedRegion> prs = cache.getPartitionedRegions();
    for (PartitionedRegion pr : prs) {
      Map<Integer, BucketAdvisor.BucketProfile> profiles =
          new HashMap<>();
      // get all local real bucket advisors
      Map<Integer, BucketAdvisor> advisors = pr.getRegionAdvisor().getAllBucketAdvisors();
      for (Map.Entry<Integer, BucketAdvisor> entry : advisors.entrySet()) {
        BucketAdvisor advisor = entry.getValue();
        // addLocally
        BucketProfile bp = (BucketProfile) advisor.createProfile();
        advisor.updateServerBucketProfile(bp);
        // advisor.basicAddClientProfile(bp);
        profiles.put(entry.getKey(), bp);
      }
      Set recipients = pr.getRegionAdvisor().adviseAllPRNodes();
      // send it to all in one message
      ReplyProcessor21 reply = AllBucketProfilesUpdateMessage.send(recipients,
          pr.getDistributionManager(), pr.getPRId(), profiles);
      if (reply != null) {
        reply.waitForRepliesUninterruptibly();
      }
    }
  }

  @Override
  public void registerServerConnection(ServerConnection serverConnection) {
    synchronized (syncLock) {
      if (!isRunning()) {
        finishCon(serverConnection);
        return;
      }
    }
    getSelectorQueue().offer(serverConnection);
    wakeupSelector();
  }

  /**
   * wake up the selector thread
   */
  private void wakeupSelector() {
    Selector s = getSelector();
    if (s != null && s.isOpen()) {
      selector.wakeup();
    }
  }

  @Override
  public void unregisterServerConnection(ServerConnection serverConnection) {
    // removed syncLock synchronization to fix bug 37104
    synchronized (allSCsLock) {
      allSCs.remove(serverConnection);
      Iterator it = allSCs.iterator();
      ServerConnection[] again = new ServerConnection[allSCs.size()];
      for (int i = 0; i < again.length; i++) {
        again[i] = (ServerConnection) it.next();
      }
      allSCList = again;
    }
    if (!isRunning()) {
      return;
    }
    // just need to wake the selector up so it will notice our socket was closed
    wakeupSelector();
  }

  private void finishCon(ServerConnection sc) {
    if (sc != null) {
      sc.handleTermination();
    }
  }

  private void drainSelectorQueue() {
    ServerConnection sc = selectorQueue.poll();
    CancelException cce = null;
    while (sc != null) {
      try {
        finishCon(sc);
      } catch (CancelException e) {
        if (cce == null) {
          cce = e;
        }
      }
      sc = selectorQueue.poll();
    }
    for (ServerConnection selectorRegistration : selectorRegistrations) {
      try {
        finishCon(selectorRegistration);
      } catch (CancelException e) {
        if (cce == null) {
          cce = e;
        }
      }
    } // while
    if (cce != null) {
      throw cce;
    }
  }

  /**
   * @see SystemFailure#emergencyClose()
   */
  @Override
  public void emergencyClose() {
    ServerSocket ss = serverSock;
    if (ss != null) {
      try {
        ss.close();
      } catch (IOException e) {
        // ignore
      }
    }
    // this.selector.close(); might NOT be safe
    crHelper.setShutdown(true);

    // TODO I'm worried about a fat lock to acquire this synchronization
    // synchronized (this.allSCsLock)
    {
      ServerConnection[] snap = allSCList;
      for (ServerConnection serverConnection : snap) {
        serverConnection.emergencyClose(); // part of cleanup()
      }
    }
  }

  private boolean isRegisteredObjectClosed(ServerConnection sc) {
    return sc.isClosed();
  }

  private int checkRegisteredKeys(int count) {
    int result = count;
    CancelException cce = null;
    if (count > 0) {
      Iterator it = selectorRegistrations.iterator();
      while (it.hasNext()) {
        ServerConnection sc = (ServerConnection) it.next();
        if (isRegisteredObjectClosed(sc)) {
          result--;
          it.remove();
          try {
            finishCon(sc);
          } catch (CancelException e) {
            if (cce == null) {
              cce = e;
            }
          }
        }
      } // while
    }
    if (cce != null) {
      throw cce;
    }
    return result;
  }

  private static final boolean WORKAROUND_SELECTOR_BUG =
      Boolean.getBoolean("CacheServer.NIO_SELECTOR_WORKAROUND");

  private Selector tmpSel;

  private void checkForStuckKeys() {
    if (!WORKAROUND_SELECTOR_BUG) {
      return;
    }
    if (tmpSel == null) {
      try {
        tmpSel = Selector.open();
      } catch (IOException ignore) {
        logger.warn("Could not check for stuck keys.", ignore);
        return;
      }

    }
    // logger.info("DEBUG: checking for stuck keys");
    for (SelectionKey sk : new ArrayList<>(selector.keys())) {
      ServerConnection sc = (ServerConnection) sk.attachment();
      if (sc == null) {
        continue;
      }
      try {
        sk.cancel();
        selector.selectNow(); // clear the cancelled key
        SelectionKey tmpsk = sc.getSelectableChannel().register(tmpSel,
            SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        try {
          // it should always be writable
          int events = tmpSel.selectNow();
          if (events == 0) {
            logger.info("stuck selection key detected on {}", sc);
            tmpsk.cancel();
            tmpSel.selectNow(); // clear canceled key
            sc.registerWithSelector2(selector);
          } else {
            if (tmpsk.isValid() && tmpsk.isReadable()) {
              try {
                tmpsk.cancel();
                tmpSel.selectNow(); // clear canceled key
                selectorRegistrations.remove(sc);
                registeredKeys--;
                sc.makeBlocking();
                // we need to say we are processing a message
                // so that that client health monitor will not
                // kill us while we wait for a thread in the thread pool.
                // This is also be used to determine how long we are
                // in the thread pool queue and to cancel operations that
                // have waited too long in the queue.
                sc.setProcessingMessage();
              } catch (ClosedChannelException ignore) {
                finishCon(sc);
                continue;
              } catch (IOException ex) {
                finishCon(sc);
                if (isRunning()) {
                  logger.warn("Unexpected Exception:", ex);
                }
                continue;
              }
              try {
                stats.incThreadQueueSize();
                pool.execute(sc);
              } catch (RejectedExecutionException rejected) {
                finishCon(sc);
                stats.decThreadQueueSize();
                if (!isRunning()) {
                  break;
                }
                logger.warn("Unexpected Exception:", rejected);
              }
            } else if (tmpsk.isValid() && tmpsk.isWritable()) {
              // this is expected
              tmpsk.cancel();
              tmpSel.selectNow(); // clear canceled key
              sc.registerWithSelector2(selector);
            } else if (!tmpsk.isValid()) {
              tmpsk.cancel();
              tmpSel.selectNow(); // clear canceled key
              sc.registerWithSelector2(selector);
            }
          }
        } catch (IOException ex) {
          if (isRunning() && selector.isOpen() && tmpSel.isOpen()) {
            logger.warn("Unexpected Exception:", ex);
            try {
              tmpsk.cancel();
              tmpSel.selectNow(); // clear canceled key
            } catch (IOException ex2) {
              if (isRunning() && selector.isOpen() && tmpSel.isOpen()) {
                logger.warn("Unexpected Exception:", ex2);
              }
            }
          }
        }
      } catch (ClosedChannelException ignore) { // fix for bug 39650
        // just ignore this channel and try the next one
        finishCon(sc);
      } catch (IOException | NullPointerException ex) {
        if (isRunning() && selector.isOpen() && tmpSel.isOpen()) {
          logger.warn("Unexpected Exception:", ex);
        }
      } // fix bug 39644

    }
  }

  private int registeredKeys;

  private void runSelectorLoop() {
    // int zeroEventsCount = 0;
    try {
      long lastCheckedTime = System.nanoTime();
      logger.info("SELECTOR enabled");
      while (selector.isOpen() && !Thread.currentThread().isInterrupted()) {
        {
          SystemFailure.checkFailure();
          if (cache.isClosed()) { // bug 38834
            break; // TODO should just ask cache's CancelCriterion
          }
          if (cache.getCancelCriterion().isCancelInProgress()) {
            break;
          }
          long delta = System.nanoTime() - lastCheckedTime;
          if (checkRegisteredKeysInterval == 0 || delta >= checkRegisteredKeysInterval) {
            registeredKeys = checkRegisteredKeys(registeredKeys);
            lastCheckedTime = System.nanoTime();
          }
          ServerConnection sc;
          if (registeredKeys == 0) {
            // do blocking wait on queue until we get some keys registered
            // with the selector
            sc = selectorQueue.take();
          } else {
            // we already have some keys registered so just do a poll on queue
            sc = selectorQueue.poll();
          }
          while (sc != null) {
            try {
              sc.registerWithSelector2(selector);
              registeredKeys++;
              selectorRegistrations.add(sc);
            } catch (ClosedChannelException cce) {
              // for bug bug 38474
              finishCon(sc);
            } catch (RuntimeException ex) {
              finishCon(sc);
              logger.warn("ignoring", ex);
            }
            sc = selectorQueue.poll();
          }
        }
        if (registeredKeys == 0) {
          continue;
        }
        int events = selector.select();
        // select() could have returned due to wakeup() during close of cache
        if (cache.getCancelCriterion().isCancelInProgress()) {
          break;
        }
        if (events == 0) {
          checkForStuckKeys();
        }
        while (events > 0) {
          Set sk = selector.selectedKeys();
          if (sk == null) {
            // something really bad has happened I'm not even sure this is possible
            // but lhughes so an NPE during close one time so perhaps it can happen
            // during selector close.
            events = 0;
            break;
          }
          Iterator keysIterator = sk.iterator();
          int cancelCount = 0;
          while (keysIterator.hasNext()) {
            SelectionKey key = (SelectionKey) keysIterator.next();
            // Remove the key from the selector's selectedKeys
            keysIterator.remove();
            final ServerConnection sc = (ServerConnection) key.attachment();
            try {
              if (key.isValid() && key.isReadable()) {
                // this is the only event we currently register for
                try {
                  key.cancel();
                  selectorRegistrations.remove(sc);
                  registeredKeys--;
                  cancelCount++;
                  sc.makeBlocking();
                  // we need to say we are processing a message
                  // so that that client health monitor will not
                  // kill us while we wait for a thread in the thread pool.
                  // This is also be used to determine how long we are
                  // in the thread pool queue and to cancel operations that
                  // have waited too long in the queue.
                  sc.setProcessingMessage();
                } catch (ClosedChannelException ignore) {
                  finishCon(sc);
                  continue;
                } catch (IOException ex) {
                  finishCon(sc);
                  if (isRunning()) {
                    logger.warn("unexpected", ex);
                  }
                  continue;
                }
                try {
                  stats.incThreadQueueSize();
                  pool.execute(sc);
                } catch (RejectedExecutionException rejected) {
                  finishCon(sc);
                  stats.decThreadQueueSize();
                  if (!isRunning()) {
                    break;
                  }
                  logger.warn("unexpected", rejected);
                }
              } else {
                finishCon(sc);
                if (key.isValid()) {
                  logger.warn("ignoring event on selector key {}", key);
                }
              }
            } catch (CancelledKeyException ex) { // fix for bug 37739
              finishCon(sc);
            }
          }
          if (cancelCount > 0 && selector.isOpen()) {
            // we need to do a select to cause the cancel to be unregisters.
            events = selector.selectNow();
          } else {
            events = 0;
          }
        }
      }
    } catch (InterruptedException ex) {
      // allow this thread to die
      Thread.currentThread().interrupt();
    } catch (ClosedSelectorException ex) {
      // allow this thread to exit
    } catch (IOException ex) {
      logger.warn("unexpected", ex);
    } finally {
      try {
        drainSelectorQueue();
      } finally {
        // note that if this method was called by close then the
        // following call is a noop since the first thing it does
        // is call isRunning.
        close(); // make sure this is called to fix bug 37749
      }
    }
  }

  @Override
  public int getPort() {
    return localPort;
  }

  @Override
  public String getServerName() {
    String name = serverSock.getLocalSocketAddress().toString();
    try {
      name = LocalHostUtil.getCanonicalLocalHostName() + "-" + name;
    } catch (Exception e) {
    }
    return name;
  }

  @Override
  public InetAddress getServerInetAddress() {
    return serverSock.getInetAddress();
  }

  /**
   * The work loop of this acceptor
   *
   * @see #accept
   */
  @Override
  public void run() {
    try {
      accept();
    } catch (CancelException e) { // bug 39462
      // ignore
    } finally {
      try {
        if (serverSock != null) {
          serverSock.close();
        }
      } catch (IOException ignore) {
      }
      if (stats != null) {
        stats.close();
      }
    }
  }

  private Selector getSelector() {
    return selector;
  }

  private BlockingQueue<ServerConnection> getSelectorQueue() {
    return selectorQueue;
  }

  private boolean loggedAcceptError;

  private static void closeSocket(Socket s) {
    if (s != null) {
      try {
        s.close();
      } catch (IOException ignore) {
      }
    }
  }

  /**
   * {@linkplain ServerSocket#accept Listens}for a client to connect and then creates a new
   * {@link ServerConnection}to handle messages from that client.
   */
  @Override
  public void accept() {
    while (isRunning()) {
      if (SystemFailure.getFailure() != null) {
        // Allocate no objects here!
        ServerSocket s = serverSock;
        if (s != null) {
          try {
            s.close();
          } catch (IOException e) {
            // don't care
          }
        }
        SystemFailure.checkFailure(); // throws
      }
      // moved this check out of the try. If we are cancelled then we need
      // to break out of this while loop.

      crHelper.checkCancelInProgress(null); // throws

      Socket socket = null;
      try {
        socket = serverSock.accept();
        crHelper.checkCancelInProgress(null); // throws

        // Optionally enable SO_KEEPALIVE in the OS network protocol.
        socket.setKeepAlive(SocketCreator.ENABLE_TCP_KEEP_ALIVE);

        // The synchronization below was added to prevent close from being
        // called
        // while a ServerConnection is being instantiated. This should prevent
        // the
        // following exception:
        // [severe 2004/12/15 18:49:17.671 PST gemfire2 Server connection from
        // balrog.gemstone.com:58478-0x6ce1 nid=0x1334aa] Uncaught exception in
        // thread Server connection from balrog.gemstone.com:58478
        // java.lang.NullPointerException
        // at
        // org.apache.geode.internal.cache.tier.sockets.ServerConnection.run(ServerConnection.java:107)

        synchronized (syncLock) {
          if (!isRunning()) {
            closeSocket(socket);
            break;
          }
        }
        loggedAcceptError = false;

        handOffNewClientConnection(socket, serverConnectionFactory);
      } catch (InterruptedIOException e) { // Solaris only
        closeSocket(socket);
        if (isRunning()) {
          if (logger.isDebugEnabled()) {
            logger.debug("Aborted due to interrupt: {}", e);
          }
        }
      } catch (IOException e) {
        closeSocket(socket);
        if (isRunning()) {
          if (!loggedAcceptError) {
            loggedAcceptError = true;
            logger.error("Cache server: Unexpected IOException from accept", e);
          }
          // Why sleep?
          // try {Thread.sleep(3000);} catch (InterruptedException ie) {}
        }
      } catch (CancelException e) {
        closeSocket(socket);
        throw e;
      } catch (Exception e) {
        closeSocket(socket);
        if (isRunning()) {
          logger.fatal("Cache server: Unexpected Exception", e);
        }
      }
    }
  }

  /**
   * Hand off a new client connection to the thread pool that processes handshakes. If all the
   * threads in this pool are busy then the hand off will block until a thread is available. This
   * blocking is good because it will throttle the rate at which we create new connections.
   */
  private void handOffNewClientConnection(final Socket socket,
      final ServerConnectionFactory serverConnectionFactory) {
    try {
      stats.incAcceptsInProgress();
      hsPool.execute(() -> {
        boolean finished = false;
        try {
          handleNewClientConnection(socket, serverConnectionFactory);
          finished = true;
        } catch (RegionDestroyedException rde) {
          // aborted due to disconnect - bug 42273
          if (!rde.getMessage().contains("HARegion")) {
            throw rde;
          }
        } catch (CancelException e) {
          // aborted due to shutdown - bug 37318
        } catch (AsynchronousCloseException expected) {
          // this is expected when our TimerTask times out an accepted socket
        } catch (IOException | ToDataException ex) { // added ToDataException to fix bug 44659
          if (isRunning()) {
            if (!loggedAcceptError) {
              loggedAcceptError = true;
              if (ex instanceof SocketTimeoutException) {
                logger.warn(
                    "Cache server: failed accepting client connection due to socket timeout.");
              } else {
                logger.warn("Cache server: failed accepting client connection " +
                    ex,
                    ex);
              }
            }
          }
        } finally {
          if (!finished) {
            closeSocket(socket);
          }
          if (isRunning()) {
            stats.decAcceptsInProgress();
          }
        }
      });
    } catch (RejectedExecutionException rejected) {
      closeSocket(socket);
      if (isRunning()) {
        stats.decAcceptsInProgress();
        logger.warn("unexpected", rejected);
      }
    }
  }

  private ByteBuffer takeCommBuffer() {
    ByteBuffer result = commBufferQueue.poll();
    if (result == null) {
      result = ByteBuffer.allocateDirect(socketBufferSize);
    }
    return result;
  }

  private void releaseCommBuffer(ByteBuffer bb) {
    if (bb == null) { // fix for bug 37107
      return;
    }
    if (isRunning()) {
      commBufferQueue.offer(bb);
    }
  }

  private void incClientServerCnxCount() {
    clientServerCnxCount.incrementAndGet();
  }

  @Override
  public void decClientServerConnectionCount() {
    clientServerCnxCount.decrementAndGet();
  }

  @Override
  public int getClientServerConnectionCount() {
    return clientServerCnxCount.get();
  }

  private boolean isNotifyBySubscription() {
    return notifyBySubscription;
  }

  private void handleNewClientConnection(final Socket socket,
      final ServerConnectionFactory serverConnectionFactory) throws IOException {
    // Read the first byte. If this socket is being used for 'client to server'
    // communication, create a ServerConnection. If this socket is being used
    // for 'server to client' communication, send it to the CacheClientNotifier
    // for processing.
    final CommunicationMode communicationMode;
    try {
      if (isSelector()) {
        communicationMode = getCommunicationModeForSelector(socket);
      } else {
        communicationMode = getCommunicationModeForNonSelector(socket);
      }
      socket.setTcpNoDelay(tcpNoDelay);

    } catch (IllegalArgumentException e) {
      // possible if a client uses SSL & the server isn't configured to use SSL,
      // or if an invalid communication communication mode byte is sent.
      logger.warn("Error processing client connection", e);
      throw new EOFException();
    }

    // GEODE-3637 - If the communicationMode is client Subscriptions, hand-off the client queue
    // initialization to be done in another threadPool
    if (handOffQueueInitialization(socket, communicationMode)) {
      return;
    }

    logger.debug("cache server: Initializing {} communication socket: {}", communicationMode,
        socket);
    boolean notForQueue = communicationMode != ClientToServerForQueue;
    if (notForQueue) {
      int curCnt = getClientServerConnectionCount();
      if (curCnt >= maxConnections) {
        logger.warn(
            "Rejected connection from {} because current connection count of {} is greater than or equal to the configured max of {}",
            new Object[] {socket.getInetAddress(), curCnt,
                maxConnections});
        if (communicationMode.expectsConnectionRefusalMessage()) {
          try {
            refuseHandshake(socket.getOutputStream(),
                String.format("exceeded max-connections %s",
                    maxConnections),
                REPLY_REFUSED);
          } catch (Exception ex) {
            logger.debug("rejection message failed", ex);
          }
        }
        closeSocket(socket);
        return;
      }
    }

    ServerConnection serverConn =
        serverConnectionFactory.makeServerConnection(socket, cache, crHelper, stats,
            handshakeTimeout, socketBufferSize, communicationMode.toString(),
            communicationMode.getModeNumber(), this, securityService);

    synchronized (allSCsLock) {
      allSCs.add(serverConn);
      ServerConnection[] snap = allSCList; // avoid volatile read
      allSCList = (ServerConnection[]) ArrayUtils.insert(snap, snap.length, serverConn);
    }
    if (notForQueue) {
      incClientServerCnxCount();
    }
    if (isSelector()) {
      serverConn.registerWithSelector();
    } else {
      try {
        pool.execute(serverConn);
      } catch (RejectedExecutionException rejected) {
        if (!isRunning()) {
          return;
        }
        logger.warn(
            "Rejected connection from {} because incoming request was rejected by pool possibly due to thread exhaustion",
            serverConn);
        try {
          refuseHandshake(socket.getOutputStream(),
              String.format("exceeded max-connections %s",
                  maxConnections),
              REPLY_REFUSED);

        } catch (Exception ex) {
          logger.debug("rejection message failed", ex);
        }
        serverConn.cleanup();
      }
    }
  }

  @Override
  public void refuseHandshake(OutputStream out, String message, byte exception) throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
    DataOutputStream dos = new DataOutputStream(hdos);
    // Write refused reply
    dos.writeByte(exception);

    // write dummy endpointType
    dos.writeByte(0);
    // write dummy queueSize
    dos.writeInt(0);

    // Write the server's member
    DistributedMember member = InternalDistributedSystem.getAnyInstance().getDistributedMember();
    HeapDataOutputStream memberDos = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(member, memberDos);
    DataSerializer.writeByteArray(memberDos.toByteArray(), dos);
    memberDos.close();

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

  private boolean handOffQueueInitialization(Socket socket, CommunicationMode communicationMode) {
    if (communicationMode.isSubscriptionFeed()) {
      boolean isPrimaryServerToClient =
          communicationMode == CommunicationMode.PrimaryServerToClient;
      clientQueueInitPool
          .execute(new ClientQueueInitializerTask(socket, isPrimaryServerToClient, this));
      return true;
    }
    return false;
  }

  private CommunicationMode getCommunicationModeForNonSelector(Socket socket) throws IOException {
    socket.setSoTimeout(0);
    socketCreator.forCluster().handshakeIfSocketIsSSL(socket, acceptTimeout);
    byte communicationModeByte = (byte) socket.getInputStream().read();
    if (communicationModeByte == -1) {
      throw new EOFException();
    }
    return CommunicationMode.fromModeNumber(communicationModeByte);
  }

  private CommunicationMode getCommunicationModeForSelector(Socket socket) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1);
    final SocketChannel socketChannel = socket.getChannel();
    socketChannel.configureBlocking(false);
    // try to read the byte first in non-blocking mode
    int res = socketChannel.read(byteBuffer);
    socketChannel.configureBlocking(true);
    if (res < 0) {
      throw new EOFException();
    }
    if (res == 0) {
      // now do a blocking read so setup a timer to close the socket if the
      // the read takes too long
      SystemTimer.SystemTimerTask timerTask = new SystemTimer.SystemTimerTask() {
        @Override
        public void run2() {
          logger.warn("Cache server: timed out waiting for handshake from {}",
              socket.getRemoteSocketAddress());
          closeSocket(socket);
        }
      };
      hsTimer.schedule(timerTask, acceptTimeout);
      res = socketChannel.read(byteBuffer);
      if (!timerTask.cancel() || res <= 0) {
        throw new EOFException();
      }
    }
    return CommunicationMode.fromModeNumber(byteBuffer.get(0));
  }

  @Override
  public boolean isRunning() {
    return !shutdownStarted;
  }

  @Override
  public void close() {
    try {
      synchronized (syncLock) {
        if (!isRunning()) {
          return;
        }
        shutdownStarted = true;
        logger.info("Cache server on port {} is shutting down.", localPort);
        if (thread != null) {
          thread.interrupt();
        }
        try {
          serverSock.close();
        } catch (IOException ignore) {
        }

        crHelper.setShutdown(true); // set this before shutting down the pool
        shutdownSelectorIfIsSelector();
        ClientHealthMonitor.shutdownInstance();
        shutdownSCs();
        clientNotifier.shutdown(acceptorId);
        shutdownPools();
        stats.close();
        if (!cache.isClosed()) {
          // the cache isn't closing so we need to inform peers that this CacheServer no longer
          // exists
          notifyCacheMembersOfClose();
        }
      } // synchronized
    } catch (RuntimeException e) {/* ignore and log */
      logger.warn("unexpected", e);
    }
  }

  @Override
  public void notifyCacheMembersOfClose() {
    if (logger.isDebugEnabled()) {
      logger.debug("sending messages to all peers for removing this server..");
    }
    for (PartitionedRegion pr : cache.getPartitionedRegions()) {
      Map<Integer, BucketAdvisor.BucketProfile> profiles = new HashMap<>();
      // get all local real bucket advisors
      Map<Integer, BucketAdvisor> advisors = pr.getRegionAdvisor().getAllBucketAdvisors();
      for (Map.Entry<Integer, BucketAdvisor> entry : advisors.entrySet()) {
        BucketAdvisor advisor = entry.getValue();
        BucketProfile bp = (BucketProfile) advisor.createProfile();
        advisor.updateServerBucketProfile(bp);
        profiles.put(entry.getKey(), bp);
      }

      Set recipients = pr.getRegionAdvisor().adviseAllPRNodes();
      // send it to all in one message
      ReplyProcessor21 reply = AllBucketProfilesUpdateMessage.send(recipients,
          pr.getDistributionManager(), pr.getPRId(), profiles);
      if (reply != null) {
        reply.waitForRepliesUninterruptibly();
      }
    }
  }

  private void shutdownSelectorIfIsSelector() {
    if (isSelector()) {
      hsTimer.cancel();
      if (tmpSel != null) {
        try {
          tmpSel.close();
        } catch (IOException ignore) {
        }
      }
      try {
        wakeupSelector();
        selector.close();
      } catch (IOException ignore) {
      }
      if (selectorThread != null) {
        selectorThread.interrupt();
      }
      commBufferQueue.clear();
    }
  }

  private void shutdownPools() {
    pool.shutdown();
    try {
      if (!pool.awaitTermination(PoolImpl.SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
        logger.warn("Timeout waiting for background tasks to complete.");
        pool.shutdownNow();
      }
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
      pool.shutdownNow();
    }
    clientQueueInitPool.shutdown();
    hsPool.shutdown();
  }

  private void shutdownSCs() {
    // added to fix part 2 of bug 37351.
    synchronized (allSCsLock) {
      ServerConnection[] snap = allSCList;
      for (ServerConnection serverConnection : snap) {
        serverConnection.cleanup();
      }
    }
  }

  boolean isShutdownProperly() {
    return !isRunning() && !thread.isAlive()
        && (selectorThread == null || !selectorThread.isAlive())
        && (pool == null || pool.isShutdown()) && (hsPool == null || hsPool.isShutdown())
        && (clientQueueInitPool == null || clientQueueInitPool.isShutdown())
        && (selector == null || !selector.isOpen()) && (tmpSel == null || !tmpSel.isOpen());
  }

  /**
   * @param bindName the ip address or host name that this acceptor should bind to. If null or ""
   *        then calculate it.
   * @return the ip address or host name this acceptor will listen on. An "" if all local addresses
   *         will be listened to.
   * @since GemFire 5.7
   */
  private static String calcBindHostName(InternalCache cache, String bindName) {
    if (bindName != null && !bindName.isEmpty()) {
      return bindName;
    }

    InternalDistributedSystem system = cache.getInternalDistributedSystem();
    DistributionConfig config = system.getConfig();
    String hostName = null;

    // Get the server-bind-address. If it is not null, use it.
    // If it is null, get the bind-address. If it is not null, use it.
    // Otherwise set default.
    String serverBindAddress = config.getServerBindAddress();
    if (serverBindAddress != null && !serverBindAddress.isEmpty()) {
      hostName = serverBindAddress;
    } else {
      String bindAddress = config.getBindAddress();
      if (bindAddress != null && !bindAddress.isEmpty()) {
        hostName = bindAddress;
      }
    }
    return hostName;
  }

  private InetAddress getBindAddress() throws IOException {
    if (bindHostName == null || bindHostName.isEmpty()) {
      return null; // pick default local address
    }
    return InetAddress.getByName(bindHostName);
  }

  /**
   * Gets the address that this cache server can be contacted on from external processes.
   *
   * @since GemFire 5.7
   */
  @Override
  public String getExternalAddress() {
    String result = bindHostName;
    boolean needCanonicalHostName = false;
    if (result == null || result.isEmpty()) {
      needCanonicalHostName = true;
    } else {
      // check to see if we are listening on all local addresses
      ServerSocket ss = serverSock;
      if (ss != null && ss.getLocalSocketAddress() instanceof InetSocketAddress) {
        InetSocketAddress isa = (InetSocketAddress) ss.getLocalSocketAddress();
        InetAddress ssAddr = isa.getAddress();
        if (ssAddr != null) {
          if (ssAddr.isAnyLocalAddress()) {
            needCanonicalHostName = true;
          }
        }
      }
    }
    if (needCanonicalHostName) {
      try {
        result = LocalHostUtil.getCanonicalLocalHostName();
      } catch (UnknownHostException ex) {
        throw new IllegalStateException("getLocalHost failed with " + ex);
      }
    }
    return result;
  }

  /**
   * This method finds a client notifier and returns it. It is used to propagate interest
   * registrations to other servers
   *
   * @return the instance that provides client notification
   */
  @Override
  public CacheClientNotifier getCacheClientNotifier() {
    return clientNotifier;
  }

  @Override
  public CachedRegionHelper getCachedRegionHelper() {
    return crHelper;
  }

  @Override
  public ClientHealthMonitor getClientHealthMonitor() {
    return healthMonitor;
  }

  @Override
  public ConnectionListener getConnectionListener() {
    return connectionListener;
  }

  @Override
  public boolean isGatewayReceiver() {
    return isGatewayReceiver;
  }

  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return gatewayTransportFilters;
  }

  static boolean isAuthenticationRequired() {
    return isAuthenticationRequired;
  }

  static boolean isPostAuthzCallbackPresent() {
    return isPostAuthzCallbackPresent;
  }

  @Override
  public Set<ServerConnection> getAllServerConnections() {
    return Collections.unmodifiableSet(allSCs);
  }

  /**
   * This method returns a thread safe structure which can be iterated over without worrying about
   * ConcurrentModificationException. JMX MBeans/Commands need to iterate over this list to get
   * client info.
   */
  @Override
  public ServerConnection[] getAllServerConnectionList() {
    return allSCList;
  }

  @Override
  public void setTLCommBuffer() {
    // The thread local will only be set if maxThreads has been set.
    if (!isSelector()) {
      return;
    }

    Message.setTLCommBuffer(takeCommBuffer());
  }

  @Override
  public void releaseTLCommBuffer() {
    if (!isSelector()) {
      return;
    }

    releaseCommBuffer(Message.setTLCommBuffer(null));
  }

  private static class ClientQueueInitializerTask implements Runnable {
    private final Socket socket;
    private final boolean isPrimaryServerToClient;
    private final AcceptorImpl acceptor;

    ClientQueueInitializerTask(Socket socket, boolean isPrimaryServerToClient,
        AcceptorImpl acceptor) {
      this.socket = socket;
      this.acceptor = acceptor;
      this.isPrimaryServerToClient = isPrimaryServerToClient;
    }

    @Override
    public void run() {
      logger.info(":Cache server: Initializing {} server-to-client communication socket: {}",
          isPrimaryServerToClient ? "primary" : "secondary", socket);
      try {
        ClientRegistrationMetadata clientRegistrationMetadata =
            new ClientRegistrationMetadata(acceptor.cache, socket);

        if (clientRegistrationMetadata.initialize()) {
          acceptor.getCacheClientNotifier().registerClient(clientRegistrationMetadata, socket,
              isPrimaryServerToClient,
              acceptor.getAcceptorId(), acceptor.isNotifyBySubscription());
        }
      } catch (final IOException ex) {
        closeSocket(socket);
        if (acceptor.isRunning()) {
          if (!acceptor.loggedAcceptError) {
            acceptor.loggedAcceptError = true;
            if (ex instanceof SocketTimeoutException) {
              logger
                  .warn("Cache server: failed accepting client connection due to socket timeout.");
            } else {
              logger.warn("Cache server: failed accepting client connection " +
                  ex,
                  ex);
            }
          }
        }
      }
    }
  }
}
