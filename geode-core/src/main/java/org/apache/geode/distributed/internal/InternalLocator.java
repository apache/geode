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
package org.apache.geode.distributed.internal;

import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.admin.remote.DistributionLocatorId.asDistributionLocatorIds;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.client.internal.locator.ClientConnectionRequest;
import org.apache.geode.cache.client.internal.locator.ClientReplacementRequest;
import org.apache.geode.cache.client.internal.locator.GetAllServersRequest;
import org.apache.geode.cache.client.internal.locator.LocatorListRequest;
import org.apache.geode.cache.client.internal.locator.LocatorStatusRequest;
import org.apache.geode.cache.client.internal.locator.QueueConnectionRequest;
import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem.ConnectListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.adapter.ServiceConfig;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipLocatorBuilder;
import org.apache.geode.distributed.internal.membership.api.QuorumChecker;
import org.apache.geode.distributed.internal.tcpserver.InfoRequest;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheBuilder;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolServiceLoader;
import org.apache.geode.internal.cache.wan.WANServiceProvider;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.logging.CoreLoggingExecutors;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogWriterFactory;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.statistics.StatisticsConfig;
import org.apache.geode.logging.internal.InternalSessionContext;
import org.apache.geode.logging.internal.LoggingSession;
import org.apache.geode.logging.internal.NullLoggingSession;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigListener;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.internal.AgentUtil;
import org.apache.geode.management.internal.JmxManagerLocator;
import org.apache.geode.management.internal.JmxManagerLocatorRequest;
import org.apache.geode.management.internal.api.LocatorClusterManagementService;
import org.apache.geode.management.internal.configuration.domain.SharedConfigurationStatus;
import org.apache.geode.management.internal.configuration.handlers.ClusterManagementServiceInfoRequestHandler;
import org.apache.geode.management.internal.configuration.handlers.SharedConfigurationStatusRequestHandler;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfoRequest;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusRequest;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusResponse;
import org.apache.geode.metrics.internal.InternalDistributedSystemMetricsService;
import org.apache.geode.security.AuthTokenEnabledComponents;

/**
 * Provides the implementation of a distribution {@code Locator} as well as internal-only
 * functionality.
 *
 * <p>
 * This class has APIs that perform essentially three layers of services. At the bottom layer is the
 * JGroups location service. On top of that you can start a distributed system. And then on top of
 * that you can start server location services.
 *
 * <p>
 * Server Location Service DistributedSystem Peer Location Service
 *
 * <p>
 * The startLocator() methods provide a way to start all three services in one call. Otherwise, the
 * services can be started independently {@code  locator = createLocator();
 * locator.startPeerLocation(); locator.startDistributeSystem();}
 *
 * @since GemFire 4.0
 */
public class InternalLocator extends Locator implements ConnectListener, LogConfigSupplier {
  public static final int MAX_POOL_SIZE =
      Integer.getInteger(GEMFIRE_PREFIX + "TcpServer.MAX_POOL_SIZE", 100);
  public static final int POOL_IDLE_TIMEOUT = 60 * 1000;

  private static final Logger logger = LogService.getLogger();

  /**
   * system property name for forcing an locator distribution manager type
   */
  public static final String FORCE_LOCATOR_DM_TYPE = "Locator.forceLocatorDMType";

  /**
   * system property name for inhibiting DM banner
   */
  public static final String INHIBIT_DM_BANNER = "Locator.inhibitDMBanner";

  /**
   * system property name for forcing locators to be preferred as coordinators
   */
  public static final String LOCATORS_PREFERRED_AS_COORDINATORS =
      GEMFIRE_PREFIX + "disable-floating-coordinator";

  /**
   * the locator hosted by this JVM. As of 7.0 it is a singleton.
   *
   * GuardedBy must synchronize on locatorLock
   */
  @MakeNotStatic
  private static InternalLocator locator;

  private static final Object locatorLock = new Object();

  private final Set<RestartHandler> restartHandlers = new CopyOnWriteHashSet<>();
  private final LocatorMembershipListener locatorListener;
  private final AtomicBoolean shutdownHandled = new AtomicBoolean(false);
  private final LoggingSession loggingSession;
  private final LocatorStats locatorStats;
  private final Path workingDirectory;
  private final MembershipLocator<InternalDistributedMember> membershipLocator;

  /**
   * whether the locator was stopped during forced-disconnect processing but a reconnect will occur
   */
  private volatile boolean stoppedForReconnect;
  private volatile boolean reconnected;

  /**
   * whether the locator was stopped during forced-disconnect processing
   */
  private volatile boolean forcedDisconnect;
  private volatile boolean isSharedConfigurationStarted;
  private volatile Thread restartThread;

  /**
   * The distributed system owned by this locator, if any. Note that if a ds already exists because
   * the locator is being colocated in a normal member this field will be null.
   */
  private InternalDistributedSystem internalDistributedSystem;

  /**
   * The cache owned by this locator, if any. Note that if a cache already exists because the
   * locator is being colocated in a normal member this field will be null.
   */
  private InternalCache internalCache;

  /**
   * product use logging
   */
  private ProductUseLog productUseLog;
  private boolean peerLocator;
  private ServerLocator serverLocator;
  private Properties env;

  private DistributionConfigImpl distributionConfig;
  private WanLocatorDiscoverer locatorDiscoverer;
  private InternalConfigurationPersistenceService configurationPersistenceService;
  private ClusterManagementService clusterManagementService;
  // synchronization lock that ensures we only have one thread performing location services
  // restart at a time
  private final Object servicesRestartLock = new Object();

  public static InternalLocator getLocator() {
    synchronized (locatorLock) {
      return locator;
    }
  }

  public static boolean hasLocator() {
    synchronized (locatorLock) {
      return locator != null;
    }
  }

  private static void removeLocator(InternalLocator locator) {
    if (locator == null) {
      return;
    }
    synchronized (locatorLock) {
      if (locator.loggingSession.getState() != InternalSessionContext.State.STOPPED) {
        locator.loggingSession.stopSession();
        locator.loggingSession.shutdown();
      }
      if (locator.equals(InternalLocator.locator)) {
        InternalLocator.locator = null;
      }
    }
  }

  /**
   * Create a locator that listens on a given port. This locator will not have peer or server
   * location services available until they are started by calling startServerLocation or
   * startPeerLocation on the locator object.
   *
   * @param port the tcp/ip port to listen on
   * @param loggingSession the LoggingSession to use, may be a NullLoggingSession which does
   *        nothing
   * @param logFile the file that log messages should be written to
   * @param logWriter a log writer that should be used (logFile parameter is ignored)
   * @param securityLogWriter the logWriter to be used for security related log messages
   * @param distributedSystemProperties optional properties to configure the distributed system
   *        (e.g., mcast addr/port, other locators)
   * @param startDistributedSystem if true then this locator will also start its own ds
   *
   * @deprecated Please use
   *             {@link #createLocator(int, LoggingSession, File, InternalLogWriter, InternalLogWriter, InetAddress, String, Properties, Path)}
   *             instead.
   */
  @Deprecated
  public static InternalLocator createLocator(int port, LoggingSession loggingSession, File logFile,
      InternalLogWriter logWriter, InternalLogWriter securityLogWriter, InetAddress bindAddress,
      String hostnameForClients, Properties distributedSystemProperties,
      boolean startDistributedSystem) {
    return createLocator(port, loggingSession, logFile, logWriter, securityLogWriter, bindAddress,
        hostnameForClients, distributedSystemProperties,
        Paths.get(System.getProperty("user.dir")));
  }

  /**
   * Create a locator that listens on a given port. This locator will not have peer or server
   * location services available until they are started by calling startServerLocation or
   * startPeerLocation on the locator object.
   *
   * @param port the tcp/ip port to listen on
   * @param loggingSession the LoggingSession to use, may be a NullLoggingSession which does
   *        nothing
   * @param logFile the file that log messages should be written to
   * @param logWriter a log writer that should be used (logFile parameter is ignored)
   * @param securityLogWriter the logWriter to be used for security related log messages
   * @param distributedSystemProperties optional properties to configure the distributed system
   *        (e.g., mcast addr/port, other locators)
   * @param workingDirectory the working directory to use for any files
   */
  public static InternalLocator createLocator(int port, LoggingSession loggingSession, File logFile,
      InternalLogWriter logWriter, InternalLogWriter securityLogWriter, InetAddress bindAddress,
      String hostnameForClients, Properties distributedSystemProperties, Path workingDirectory) {
    synchronized (locatorLock) {
      if (hasLocator()) {
        throw new IllegalStateException(
            "A locator can not be created because one already exists in this JVM.");
      }
      InternalLocator locator =
          new InternalLocator(port, loggingSession, logFile, logWriter, securityLogWriter,
              bindAddress, hostnameForClients, distributedSystemProperties, null,
              workingDirectory);
      InternalLocator.locator = locator;
      return locator;
    }
  }

  private static void setLocator(InternalLocator locator) {
    synchronized (locatorLock) {
      if (InternalLocator.locator != null && InternalLocator.locator != locator) {
        throw new IllegalStateException(
            "A locator can not be created because one already exists in this JVM.");
      }

      InternalLocator.locator = locator;
    }
  }

  /**
   * Creates a distribution locator that runs in this VM on the given port and bind address in the
   * default working directory (Java "user.dir" System Property).
   *
   * <p>
   * This is for internal use only as it does not create a distributed system unless told to do so.
   *
   * @param port the tcp/ip port to listen on
   * @param logFile the file that log messages should be written to
   * @param logWriter a log writer that should be used (logFile parameter is ignored)
   * @param securityLogWriter the logWriter to be used for security related log messages
   * @param startDistributedSystem if true, a distributed system is started
   * @param distributedSystemProperties optional properties to configure the distributed system
   *        (e.g., mcast
   *        addr/port, other locators)
   * @param hostnameForClients the name to give to clients for connecting to this locator
   */
  public static InternalLocator startLocator(int port, File logFile, InternalLogWriter logWriter,
      InternalLogWriter securityLogWriter, InetAddress bindAddress, boolean startDistributedSystem,
      Properties distributedSystemProperties, String hostnameForClients)
      throws IOException {
    return startLocator(port, logFile, logWriter, securityLogWriter, bindAddress,
        startDistributedSystem, distributedSystemProperties, hostnameForClients,
        Paths.get(System.getProperty("user.dir")));
  }

  /**
   * Creates a distribution locator that runs in this VM on the given port and bind address in the
   * specified working directory.
   *
   * <p>
   * This is for internal use only as it does not create a distributed system unless told to do so.
   *
   * @param port the tcp/ip port to listen on
   * @param logFile the file that log messages should be written to
   * @param logWriter a log writer that should be used (logFile parameter is ignored)
   * @param securityLogWriter the logWriter to be used for security related log messages
   * @param startDistributedSystem if true, a distributed system is started
   * @param distributedSystemProperties optional properties to configure the distributed system
   *        (e.g., mcast
   *        addr/port, other locators)
   * @param hostnameForClients the name to give to clients for connecting to this locator
   * @param workingDirectory the working directory to use for any files
   */
  public static InternalLocator startLocator(int port, File logFile, InternalLogWriter logWriter,
      InternalLogWriter securityLogWriter, InetAddress bindAddress, boolean startDistributedSystem,
      Properties distributedSystemProperties, String hostnameForClients, Path workingDirectory)
      throws IOException {
    System.setProperty(FORCE_LOCATOR_DM_TYPE, "true");
    InternalLocator newLocator = null;

    boolean startedLocator = false;
    try {

      // if startDistributedSystem is true then Locator uses a NullLoggingSession (does nothing)
      LoggingSession loggingSession =
          startDistributedSystem ? NullLoggingSession.create() : LoggingSession.create();

      newLocator = createLocator(port, loggingSession, logFile, logWriter, securityLogWriter,
          bindAddress, hostnameForClients, distributedSystemProperties,
          workingDirectory);

      loggingSession.createSession(newLocator);
      loggingSession.startSession();

      try {
        newLocator.startPeerLocation();

        if (startDistributedSystem) {

          try {
            newLocator.startDistributedSystem();
          } catch (RuntimeException e) {
            newLocator.stop();
            throw e;
          }

          InternalDistributedSystem system = newLocator.internalDistributedSystem;
          if (system != null) {
            system.getDistributionManager().addHostedLocators(system.getDistributedMember(),
                getLocatorStrings(), newLocator.isSharedConfigurationEnabled());
          }
        }
      } catch (IllegalStateException e) {
        newLocator.stop();
        throw e;
      }

      InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
      if (system != null) {
        try {
          newLocator.startServerLocation(system);
        } catch (RuntimeException e) {
          newLocator.stop();
          throw e;
        }
      }

      newLocator.endStartLocator(null);
      startedLocator = true;
      return newLocator;

    } finally {
      System.clearProperty(FORCE_LOCATOR_DM_TYPE);

      if (!startedLocator) {
        removeLocator(newLocator);
      }
    }
  }

  /**
   * Determines if this VM is a locator which must ignore a shutdown.
   *
   * @return true if this VM is a locator which should ignore a shutdown, false if it is a normal
   *         member.
   */
  public static boolean isDedicatedLocator() {
    InternalLocator internalLocator = getLocator();
    if (internalLocator == null) {
      return false;
    }

    InternalDistributedSystem system = internalLocator.internalDistributedSystem;
    if (system == null) {
      return false;
    }
    DistributionManager distributionManager = system.getDistributionManager();
    if (distributionManager.isLoner()) {
      return false;
    }
    ClusterDistributionManager clusterDistributionManager =
        (ClusterDistributionManager) system.getDistributionManager();
    return clusterDistributionManager.getDMType() == ClusterDistributionManager.LOCATOR_DM_TYPE;
  }

  /**
   * Creates a new {@code Locator} with the given port, log file, logWriter, and bind address.
   *
   * @param port the tcp/ip port to listen on
   * @param logFile the file that log messages should be written to
   * @param logWriter a log writer that should be used (logFile parameter is ignored)
   * @param securityLogWriter the log writer to be used for security related log messages
   * @param hostnameForClients the name to give to clients for connecting to this locator
   * @param distributedSystemProperties optional properties to configure the distributed system
   *        (e.g., mcast addr/port, other locators)
   * @param distributionConfig the config if being called from a distributed system; otherwise null.
   * @param workingDirectory the working directory to use for files
   */
  @VisibleForTesting
  InternalLocator(int port, LoggingSession loggingSession, File logFile,
      InternalLogWriter logWriter, InternalLogWriter securityLogWriter, InetAddress bindAddress,
      String hostnameForClients, Properties distributedSystemProperties,
      DistributionConfigImpl distributionConfig, Path workingDirectory) {
    this.logFile = logFile;
    this.bindAddress = bindAddress;
    this.hostnameForClients = hostnameForClients;

    this.workingDirectory = workingDirectory;


    env = new Properties();

    // set bind-address explicitly only if not wildcard and let any explicit
    // value in distributedSystemProperties take precedence
    if (bindAddress != null && !bindAddress.isAnyLocalAddress()) {
      env.setProperty(BIND_ADDRESS, bindAddress.getHostAddress());
    }

    if (distributedSystemProperties != null) {
      env.putAll(distributedSystemProperties);
    }
    env.setProperty(CACHE_XML_FILE, "");

    // create a DC so that all of the lookup rules, gemfire.properties, etc,
    // are considered and we have a config object we can trust
    if (distributionConfig == null) {
      distributionConfig = new DistributionConfigImpl(env);
      env.clear();
      env.putAll(distributionConfig.getProps());
    }
    this.distributionConfig = distributionConfig;

    boolean hasLogFileButConfigDoesNot =
        this.logFile != null && this.distributionConfig.getLogFile()
            .toString().equals(DistributionConfig.DEFAULT_LOG_FILE.toString());
    if (logWriter == null && hasLogFileButConfigDoesNot) {
      // LOG: this is(was) a hack for when logFile and config don't match -- if config specifies a
      // different log-file things will break!
      this.distributionConfig.unsafeSetLogFile(this.logFile);
    }

    if (loggingSession == null) {
      throw new Error("LoggingSession must not be null");
    }
    this.loggingSession = loggingSession;

    // LOG: create LogWriters for GemFireTracer (or use whatever was passed in)
    if (logWriter == null) {
      LogWriterFactory.createLogWriterLogger(this.distributionConfig, false);
      if (logger.isDebugEnabled()) {
        logger.debug("LogWriter for locator is created.");
      }
    }

    if (securityLogWriter == null) {
      securityLogWriter = LogWriterFactory.createLogWriterLogger(this.distributionConfig, true);
      securityLogWriter.fine("SecurityLogWriter for locator is created.");
    }

    SocketCreatorFactory.setDistributionConfig(this.distributionConfig);

    locatorListener = WANServiceProvider.createLocatorMembershipListener();
    if (locatorListener != null) {
      // We defer setting the port until the handler is init'd - that way we'll have an actual port
      // in the case where we're starting with port = 0.
      locatorListener.setConfig(getConfig());
    }

    locatorStats = new LocatorStats();

    InternalLocatorTcpHandler handler = new InternalLocatorTcpHandler();
    try {
      MembershipConfig config = new ServiceConfig(
          new RemoteTransportConfig(distributionConfig, MemberIdentifier.LOCATOR_DM_TYPE),
          distributionConfig);
      Supplier<ExecutorService> executor = () -> CoreLoggingExecutors
          .newThreadPoolWithSynchronousFeed("locator request thread ",
              MAX_POOL_SIZE, new DelayedPoolStatHelper(),
              POOL_IDLE_TIMEOUT,
              new ThreadPoolExecutor.CallerRunsPolicy());
      final TcpSocketCreator socketCreator = SocketCreatorFactory
          .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR);
      membershipLocator =
          MembershipLocatorBuilder.<InternalDistributedMember>newLocatorBuilder(
              socketCreator,
              InternalDataSerializer.getDSFIDSerializer(),
              workingDirectory,
              executor)
              .setConfig(config)
              .setPort(port)
              .setBindAddress(bindAddress)
              .setProtocolChecker(new ProtocolCheckerImpl(this, new ClientProtocolServiceLoader()))
              .setFallbackHandler(handler)
              .setLocatorsAreCoordinators(shouldLocatorsBeCoordinators())
              .setLocatorStats(locatorStats)
              .create();
    } catch (MembershipConfigurationException | UnknownHostException e) {
      throw new GemFireConfigException(e.getMessage());
    }

    membershipLocator.addHandler(InfoRequest.class, new InfoRequestHandler());
    restartHandlers.add((ds, cache, sharedConfig) -> {
      final InternalDistributedSystem ids = (InternalDistributedSystem) ds;
      // let old locator know about new membership object
      membershipLocator.setMembership(ids.getDM().getDistribution().getMembership());
    });
  }

  public boolean isSharedConfigurationEnabled() {
    return distributionConfig.getEnableClusterConfiguration();
  }

  private boolean loadFromSharedConfigDir() {
    return distributionConfig.getLoadClusterConfigFromDir();
  }

  public boolean isSharedConfigurationRunning() {
    return configurationPersistenceService != null
        && configurationPersistenceService.getStatus() == SharedConfigurationStatus.RUNNING;
  }

  public LocatorMembershipListener getLocatorMembershipListener() {
    return locatorListener;
  }

  /**
   * @deprecated Please use {@link #getLocatorMembershipListener()} instead.
   */
  @Deprecated
  public LocatorMembershipListener getlocatorMembershipListener() {
    return getLocatorMembershipListener();
  }

  private void startTcpServer() throws IOException {
    logger.info("Starting {}", this);
    membershipLocator.start();
  }

  public InternalConfigurationPersistenceService getConfigurationPersistenceService() {
    return configurationPersistenceService;
  }

  public DistributionConfigImpl getConfig() {
    return distributionConfig;
  }

  public InternalCache getCache() {
    if (internalCache == null) {
      return GemFireCacheImpl.getInstance();
    }
    return internalCache;
  }

  /**
   * Start peer location in this locator. If you plan on starting a distributed system later, this
   * method should be called first so that the distributed system can use this locator.
   *
   * @return returns the port that the locator to which the locator is bound
   * @since GemFire 5.7
   */
  int startPeerLocation() throws IOException {
    if (isPeerLocator()) {
      throw new IllegalStateException(
          String.format("Peer location is already running for %s", this));
    }
    logger.info("Starting peer location for {}", this);

    peerLocator = true;
    int boundPort =
        membershipLocator
            .start();
    File productUseFile = workingDirectory.resolve("locator" + boundPort + "views.log").toFile();
    productUseLog = new ProductUseLog(productUseFile);

    return boundPort;
  }

  private boolean shouldLocatorsBeCoordinators() {
    // check for settings that would require only locators to hold the
    // coordinator - e.g., security and network-partition detection
    boolean locatorsAreCoordinators;
    boolean networkPartitionDetectionEnabled =
        distributionConfig.getEnableNetworkPartitionDetection();
    if (networkPartitionDetectionEnabled) {
      locatorsAreCoordinators = true;
    } else {
      // check if security is enabled
      String prop = distributionConfig.getSecurityPeerAuthInit();
      locatorsAreCoordinators = prop != null && !prop.isEmpty();
      if (!locatorsAreCoordinators) {
        locatorsAreCoordinators = Boolean.getBoolean(LOCATORS_PREFERRED_AS_COORDINATORS);
      }
    }
    return locatorsAreCoordinators;
  }

  /**
   * @return the TcpHandler for peer to peer discovery
   */
  public MembershipLocator<InternalDistributedMember> getMembershipLocator() {
    return membershipLocator;
  }

  /**
   * For backward-compatibility we retain this method
   *
   * @deprecated use a form of the method that does not have peerLocator/serverLocator parameters
   */
  @Deprecated
  public static InternalLocator startLocator(int locatorPort, File logFile,
      InternalLogWriter logWriter, InternalLogWriter securityLogWriter, InetAddress bindAddress,
      Properties distributedSystemProperties, boolean peerLocator, boolean serverLocator,
      String hostnameForClients, boolean b1) throws IOException {
    return startLocator(locatorPort, logFile, logWriter, securityLogWriter, bindAddress, true,
        distributedSystemProperties, hostnameForClients);
  }

  /**
   * Start a distributed system whose life cycle is managed by this locator. When the locator is
   * stopped, this distributed system will be disconnected. If a distributed system already exists,
   * this method will have no affect.
   *
   * @since GemFire 5.7
   */
  private void startDistributedSystem() throws IOException {
    InternalDistributedSystem existing = InternalDistributedSystem.getConnectedInstance();
    if (existing != null) {
      // LOG: changed from config to info
      logger.info("Using existing distributed system: {}", existing);
      startCache(existing);
    } else {

      StringBuilder sb = new StringBuilder(100);
      if (bindAddress != null) {
        sb.append(bindAddress.getHostAddress());
      } else {
        sb.append(LocalHostUtil.getLocalHost().getHostAddress());
      }
      sb.append('[').append(getPort()).append(']');
      String thisLocator = sb.toString();

      if (peerLocator) {
        // append this locator to the locators list from the config properties
        boolean setLocatorsProp = false;
        String locatorsConfigValue = distributionConfig.getLocators();
        if (StringUtils.isNotBlank(locatorsConfigValue)) {
          if (!locatorsConfigValue.contains(thisLocator)) {
            locatorsConfigValue = locatorsConfigValue + ',' + thisLocator;
            setLocatorsProp = true;
          }
        } else {
          locatorsConfigValue = thisLocator;
          setLocatorsProp = true;
        }
        if (setLocatorsProp) {
          Properties updateEnv = new Properties();
          updateEnv.setProperty(LOCATORS, locatorsConfigValue);
          distributionConfig.setApiProps(updateEnv);
          String locatorsPropertyName = GEMFIRE_PREFIX + LOCATORS;
          if (System.getProperty(locatorsPropertyName) != null) {
            System.setProperty(locatorsPropertyName, locatorsConfigValue);
          }
        }
        // No longer default mcast-port to zero.
      }

      Properties distributedSystemProperties = new Properties();
      // LogWriterAppender is now shared via that class
      // using a DistributionConfig earlier in this method
      distributedSystemProperties.put(DistributionConfig.DS_CONFIG_NAME, distributionConfig);

      logger.info("Starting distributed system");

      internalDistributedSystem =
          InternalDistributedSystem
              .connectInternal(distributedSystemProperties, null,
                  new InternalDistributedSystemMetricsService.Builder(),
                  membershipLocator);

      if (peerLocator) {
        // We've created a peer location message handler - it needs to be connected to
        // the membership service in order to get membership view notifications
        membershipLocator
            .setMembership(internalDistributedSystem.getDM()
                .getDistribution().getMembership());
      }

      internalDistributedSystem.addDisconnectListener(sys -> stop(false, false, false));

      startCache(internalDistributedSystem);

      logger.info("Locator started on {}", thisLocator);
    }
  }

  private void startCache(DistributedSystem system) throws IOException {
    InternalCache internalCache = GemFireCacheImpl.getInstance();
    if (internalCache == null) {
      logger.info("Creating cache for locator.");
      this.internalCache = new InternalCacheBuilder(system.getProperties())
          .create((InternalDistributedSystem) system);
      internalCache = this.internalCache;
    } else {
      logger.info("Using existing cache for locator.");
      ((InternalDistributedSystem) system).handleResourceEvent(ResourceEvent.LOCATOR_START, this);
    }
    startJmxManagerLocationService(internalCache);

    startClusterManagementService();
  }

  @VisibleForTesting
  void startClusterManagementService() throws IOException {
    startConfigurationPersistenceService();
    AgentUtil agentUtil = new AgentUtil(GemFireVersion.getGemFireVersion());
    startClusterManagementService(internalCache, agentUtil);
  }

  @VisibleForTesting
  void startClusterManagementService(InternalCache myCache, AgentUtil agentUtil) {

    if (myCache == null) {
      return;
    }

    clusterManagementService = new LocatorClusterManagementService(myCache,
        configurationPersistenceService);

    // Find the V2 Management rest WAR file
    URI gemfireManagementWar = agentUtil.findWarLocation("geode-web-management");
    if (gemfireManagementWar == null) {
      logger.info(
          "Unable to find GemFire V2 Management REST API WAR file; the Management REST Interface for Geode will not be accessible.");
      return;
    }

    Map<String, Object> serviceAttributes = new HashMap<>();
    serviceAttributes.put(HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM,
        myCache.getSecurityService());
    serviceAttributes.put(HttpService.CLUSTER_MANAGEMENT_SERVICE_CONTEXT_PARAM,
        clusterManagementService);

    String[] authEnabledComponents = distributionConfig.getSecurityAuthTokenEnabledComponents();

    boolean managementAuthTokenEnabled = Arrays.stream(authEnabledComponents)
        .anyMatch(AuthTokenEnabledComponents::hasManagement);
    serviceAttributes.put(HttpService.AUTH_TOKEN_ENABLED_PARAM, managementAuthTokenEnabled);

    if (distributionConfig.getEnableManagementRestService()) {
      myCache.getOptionalService(HttpService.class).ifPresent(x -> {
        try {
          ManagementService managementService = ManagementService.getManagementService(myCache);
          if (!managementService.isManager()) {
            // The management rest service requires the jmx-manager
            // since some of the things the rest service does need
            // the jmx-manager in the same JVM. For example the
            // rebalance operation needs mxbeans that are hosted
            // by the jmx-manager.
            managementService.startManager();
          }
          logger.info("Geode Property {}=true Geode Management Rest Service is enabled.",
              ConfigurationProperties.ENABLE_MANAGEMENT_REST_SERVICE);
          x.addWebApplication("/management", Paths.get(gemfireManagementWar), serviceAttributes);
        } catch (Throwable e) {
          logger.warn("Unable to start management service: {}", e.getMessage());
        }
      });
    } else {
      logger.info("Geode Property {}=false Geode Management Rest Service is disabled.",
          ConfigurationProperties.ENABLE_MANAGEMENT_REST_SERVICE);
    }
  }

  /**
   * End the initialization of the locator. This method should be called once the location services
   * and distributed system are started.
   *
   * @param distributedSystem The distributed system to use for the statistics.
   * @since GemFire 5.7
   */
  void endStartLocator(InternalDistributedSystem distributedSystem) {
    env = null;

    if (distributedSystem == null) {
      distributedSystem = InternalDistributedSystem.getConnectedInstance();
    }

    if (distributedSystem != null) {
      onConnect(distributedSystem);
    } else {
      InternalDistributedSystem.addConnectListener(this);
    }

    locatorDiscoverer = WANServiceProvider.createLocatorDiscoverer();
    if (locatorDiscoverer != null) {
      locatorDiscoverer.discover(getPort(), distributionConfig, locatorListener,
          hostnameForClients);
    }
  }

  /**
   * Start server location services in this locator. Server location can only be started once there
   * is a running distributed system.
   *
   * @param distributedSystem The distributed system which the server location services should use.
   *        If null, the method will try to find an already connected distributed system.
   * @since GemFire 5.7
   */
  void startServerLocation(InternalDistributedSystem distributedSystem) throws IOException {
    if (isServerLocator()) {
      throw new IllegalStateException(
          String.format("Server location is already running for %s", this));
    }
    logger.info("Starting server location for {}", this);

    if (distributedSystem == null) {
      distributedSystem = InternalDistributedSystem.getConnectedInstance();
      if (distributedSystem == null) {
        throw new IllegalStateException(
            "Since server location is enabled the distributed system must be connected.");
      }
    }

    ServerLocator serverLocator = new ServerLocator(getPort(), bindAddress, hostnameForClients,
        logFile, productUseLog, getConfig().getName(), distributedSystem, locatorStats);
    restartHandlers.add(serverLocator);
    membershipLocator.addHandler(LocatorListRequest.class, serverLocator);
    membershipLocator.addHandler(ClientConnectionRequest.class, serverLocator);
    membershipLocator.addHandler(QueueConnectionRequest.class, serverLocator);
    membershipLocator.addHandler(ClientReplacementRequest.class, serverLocator);
    membershipLocator.addHandler(GetAllServersRequest.class, serverLocator);
    membershipLocator.addHandler(LocatorStatusRequest.class, serverLocator);

    this.serverLocator = serverLocator;
    if (!membershipLocator.isAlive()) {
      startTcpServer();
    }
    // the product use is not guaranteed to be initialized until the server is started, so
    // the last thing we do is tell it to start logging
    productUseLog.monitorUse(distributedSystem);
  }

  /**
   * Stop this locator.
   */
  @Override
  public void stop() {
    stop(false, false, true);
  }

  /**
   * Stop this locator
   *
   * @param stopForReconnect - stopping for distributed system reconnect
   * @param waitForDisconnect - wait up to 60 seconds for the locator to completely stop
   */
  public void stop(boolean forcedDisconnect, boolean stopForReconnect, boolean waitForDisconnect) {
    boolean isDebugEnabled = logger.isDebugEnabled();

    stoppedForReconnect = stopForReconnect;
    this.forcedDisconnect = forcedDisconnect;

    if (membershipLocator.isShuttingDown()) {
      // fix for bug 46156
      // If we are already shutting down don't do all of this again.
      // But, give the server a bit of time to shut down so a new
      // locator can be created, if desired, when this method returns
      if (waitForDisconnect) {
        long endOfWait = System.currentTimeMillis() + 60000;
        if (isDebugEnabled && membershipLocator.isAlive()) {
          logger.debug("sleeping to wait for the locator server to shut down...");
        }

        while (membershipLocator.isAlive() && System.currentTimeMillis() < endOfWait) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            return;
          }
        }

        if (isDebugEnabled) {
          if (membershipLocator.isAlive()) {
            logger.debug(
                "60 seconds have elapsed waiting for the locator server to shut down - terminating wait and returning");
          } else {
            logger.debug("the locator server has shut down");
          }
        }
      }
    }

    if (locatorDiscoverer != null) {
      locatorDiscoverer.stop();
      locatorDiscoverer = null;
    }

    // stop the TCPServer
    if (!membershipLocator.isShuttingDown()) {
      membershipLocator.stop();
    }

    removeLocator(this);

    handleShutdown();
    logger.info("{} is stopped", this);

    if (stopForReconnect) {
      launchRestartThread();
    }
  }

  /**
   * answers whether this locator is currently stopped
   */
  public boolean isStopped() {
    return !membershipLocator.isAlive();
  }

  void handleShutdown() {
    if (!shutdownHandled.compareAndSet(false, true)) {
      // already shutdown
      return;
    }
    if (productUseLog != null) {
      productUseLog.close();
    }
    if (internalCache != null && !stoppedForReconnect && !forcedDisconnect) {
      logger.info("Closing locator's cache");
      try {
        internalCache.close();
      } catch (RuntimeException ex) {
        logger.info("Could not close locator's cache because: {}", ex.getMessage(), ex);
      }
    }

    if (locatorStats != null) {
      locatorStats.close();
    }

    if (locatorListener != null) {
      locatorListener.clearLocatorInfo();
    }

    isSharedConfigurationStarted = false;
    if (internalDistributedSystem != null && !forcedDisconnect) {
      if (internalDistributedSystem.isConnected()) {
        logger.info("Disconnecting distributed system for {}", this);
        internalDistributedSystem.disconnect();
      }
    }
  }

  /**
   * Waits for a locator to be told to stop.
   *
   * @throws InterruptedException thrown if the thread is interrupted
   */
  public void waitToStop() throws InterruptedException {
    boolean restarted;
    do {
      DistributedSystem system = internalDistributedSystem;
      restarted = false;
      membershipLocator.waitToShutdown();
      if (stoppedForReconnect) {
        logger.info("waiting for distributed system to disconnect...");
        while (system.isConnected()) {
          Thread.sleep(5000);
        }
        logger.info("waiting for distributed system to reconnect...");
        try {
          restarted = system.waitUntilReconnected(-1, TimeUnit.SECONDS);
        } catch (CancelException e) {
          // reconnect attempt failed
        }
        if (restarted) {
          logger.info("system restarted");
        } else {
          logger.info("system was not restarted");
        }
        Thread restartThread = this.restartThread;
        if (restartThread != null) {
          logger.info("waiting for services to restart...");
          restartThread.join();
          this.restartThread = null;
          logger.info("done waiting for services to restart");
        }
      }
    } while (restarted);
  }

  /**
   * launch a thread that will restart location services
   */
  private void launchRestartThread() {
    String threadName = "Location services restart thread";
    restartThread = new LoggingThread(threadName, () -> {
      synchronized (servicesRestartLock) {
        stoppedForReconnect = true;
        boolean restarted = false;
        try {
          restarted = attemptReconnect();
          logger.info("attemptReconnect returned {}", restarted);
        } catch (InterruptedException e) {
          logger.info("attempt to restart location services was interrupted", e);
        } catch (IOException e) {
          logger.info("attempt to restart location services terminated", e);
        } finally {
          shutdownHandled.set(false);
          if (!restarted) {
            stoppedForReconnect = false;
          }
          reconnected = restarted;
          restartThread = null;
        }
      }
    });
    restartThread.start();
  }

  public boolean isReconnected() {
    return reconnected;
  }

  /**
   * reconnects the locator to a restarting DistributedSystem. If quorum checks are enabled this
   * will start peer location services before a distributed system is available if the quorum check
   * succeeds. It will then wait for the system to finish reconnecting before returning. If quorum
   * checks are not being done this merely waits for the distributed system to reconnect and then
   * starts location services.
   *
   * @return true if able to reconnect the locator to the new distributed system
   */
  private boolean attemptReconnect() throws InterruptedException, IOException {
    boolean restarted = false;
    if (stoppedForReconnect) {
      logger.info("attempting to restart locator");
      boolean tcpServerStarted = false;
      InternalDistributedSystem system = internalDistributedSystem;
      long waitTime = system.getConfig().getMaxWaitTimeForReconnect() / 2;
      QuorumChecker quorumChecker = null;

      while (system.getReconnectedSystem() == null && !system.isReconnectCancelled()) {
        if (quorumChecker == null) {
          quorumChecker = system.getQuorumChecker();
          if (quorumChecker != null) {
            logger.info("The distributed system returned this quorum checker: {}", quorumChecker);
          }
        }

        if (quorumChecker != null && !tcpServerStarted) {
          boolean start = quorumChecker
              .checkForQuorum(3L * system.getConfig().getMemberTimeout());
          if (start) {
            // start up peer location. server location is started after the DS finishes reconnecting
            logger.info("starting peer location");
            if (locatorListener != null) {
              locatorListener.clearLocatorInfo();
            }
            stoppedForReconnect = false;
            internalDistributedSystem = null;
            internalCache = null;
            restartWithoutSystem();
            tcpServerStarted = true;
            setLocator(this);
          }
        }

        try {
          system.waitUntilReconnected(waitTime, TimeUnit.MILLISECONDS);
        } catch (CancelException e) {
          continue; // DistributedSystem failed to restart - loop until it gives up
        }
      }

      InternalDistributedSystem newSystem =
          (InternalDistributedSystem) system.getReconnectedSystem();
      if (newSystem != null) {
        boolean noprevlocator = false;
        if (!hasLocator()) {
          setLocator(this);
          noprevlocator = true;
        }
        if (!tcpServerStarted) {
          if (locatorListener != null) {
            locatorListener.clearLocatorInfo();
          }
          stoppedForReconnect = false;
        }

        try {
          restartWithSystem(newSystem, GemFireCacheImpl.getInstance());
        } catch (CancelException e) {
          stoppedForReconnect = true;
          if (noprevlocator) {
            removeLocator(this);
          }
          return false;
        }

        restarted = true;
      }
    }

    logger.info("restart thread exiting.  Service was {}restarted", restarted ? "" : "not ");
    return restarted;
  }

  private void restartWithoutSystem() throws IOException {
    synchronized (locatorLock) {
      if (locator != this && hasLocator()) {
        throw new IllegalStateException(
            "A locator can not be created because one already exists in this JVM.");
      }
      internalDistributedSystem = null;
      internalCache = null;

      logger.info("Locator restart: initializing TcpServer peer location services");
      membershipLocator.restarting();

      if (productUseLog.isClosed()) {
        productUseLog.reopen();
      }

      if (!membershipLocator.isAlive()) {
        logger.info("Locator restart: starting TcpServer");
        startTcpServer();
      }
    }
  }

  private void restartWithSystem(InternalDistributedSystem newSystem, InternalCache newCache)
      throws IOException {
    synchronized (locatorLock) {
      if (locator != this && hasLocator()) {
        throw new IllegalStateException(
            "A locator can not be created because one already exists in this JVM.");
      }
    }
    internalDistributedSystem = newSystem;
    internalCache = newCache;
    logger.info("Locator restart: initializing TcpServer");

    try {
      restartHandlers.forEach(
          handler -> handler.restarting(newSystem, newCache, configurationPersistenceService));
      membershipLocator.restarting();
    } catch (CancelException e) {
      internalDistributedSystem = null;
      internalCache = null;
      logger.info("Locator restart: attempt to restart location services failed", e);
      throw e;
    }

    if (productUseLog.isClosed()) {
      productUseLog.reopen();
    }
    productUseLog.monitorUse(newSystem);

    if (isSharedConfigurationEnabled()) {
      configurationPersistenceService =
          new InternalConfigurationPersistenceService(newCache, workingDirectory,
              JAXBService.create());
      startClusterManagementService();
    }

    if (!membershipLocator.isAlive()) {
      logger.info("Locator restart: starting TcpServer");
      startTcpServer();
    }

    logger.info("Locator restart: initializing JMX manager");
    startJmxManagerLocationService(newCache);
    endStartLocator(internalDistributedSystem);
    logger.info("Locator restart completed");

    restartHandlers.forEach(handler -> handler.restartCompleted(newSystem));
  }

  public ClusterManagementService getClusterManagementService() {
    return clusterManagementService;
  }

  @Override
  public DistributedSystem getDistributedSystem() {
    if (internalDistributedSystem == null) {
      return InternalDistributedSystem.getAnyInstance();
    }

    return internalDistributedSystem;
  }

  @Override
  public boolean isPeerLocator() {
    return peerLocator;
  }

  @Override
  public boolean isServerLocator() {
    return serverLocator != null;
  }

  /**
   * Returns null if no server locator; otherwise returns the advisee that represents the server
   * locator.
   */
  public ServerLocator getServerLocatorAdvisee() {
    return serverLocator;
  }

  /**
   * Return the port on which the locator is actually listening. If called before the locator has
   * actually started, this method will return null.
   *
   * @return the port the locator is listening on or null if it has not yet been started
   */
  @Override
  public Integer getPort() {
    if (membershipLocator != null && membershipLocator.isAlive()) {
      return membershipLocator.getPort();
    }
    return null;
  }

  @Override
  public LogConfig getLogConfig() {
    return distributionConfig;
  }

  @Override
  public StatisticsConfig getStatisticsConfig() {
    return distributionConfig;
  }

  @Override
  public void addLogConfigListener(LogConfigListener logConfigListener) {}

  @Override
  public void removeLogConfigListener(LogConfigListener logConfigListener) {}

  public SharedConfigurationStatusResponse getSharedConfigurationStatus() {
    ExecutorService waitingPoolExecutor =
        internalCache.getDistributionManager().getExecutors().getWaitingThreadPool();
    Future<SharedConfigurationStatusResponse> statusFuture =
        waitingPoolExecutor.submit(new FetchSharedConfigStatus());
    SharedConfigurationStatusResponse response;

    try {
      response = statusFuture.get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.info("Exception occurred while fetching the status {}", getStackTrace(e));
      response = new SharedConfigurationStatusResponse();
      response.setStatus(SharedConfigurationStatus.UNDETERMINED);
    }
    return response;
  }

  @Override
  public void onConnect(InternalDistributedSystem sys) {
    try {
      locatorStats.hookupStats(sys,
          LocalHostUtil.getCanonicalLocalHostName() + '-' + membershipLocator
              .getSocketAddress());
    } catch (UnknownHostException e) {
      logger.warn(e);
    }
  }

  /**
   * Returns collection of locator strings representing every locator instance hosted by this
   * member.
   *
   * @see #getLocators()
   */
  public static Collection<String> getLocatorStrings() {
    Collection<String> locatorStrings;
    try {
      Collection<DistributionLocatorId> locatorIds = asDistributionLocatorIds(getLocators());
      locatorStrings = DistributionLocatorId.asStrings(locatorIds);
    } catch (UnknownHostException ignored) {
      locatorStrings = null;
    }
    if (locatorStrings == null || locatorStrings.isEmpty()) {
      return null;
    }
    return locatorStrings;
  }

  private void startConfigurationPersistenceService() throws IOException {
    installRequestHandlers();

    if (!distributionConfig.getEnableClusterConfiguration()) {
      logger.info("Cluster configuration service is disabled");
      return;
    }

    if (!distributionConfig.getJmxManager()) {
      throw new IllegalStateException(
          "Cannot start cluster configuration without jmx-manager=true");
    }

    if (isSharedConfigurationStarted) {
      logger.info("Cluster configuration service is already started.");
      return;
    }

    if (!isDedicatedLocator()) {
      logger.info(
          "Cluster configuration service not enabled as it is only supported in dedicated locators");
      return;
    }


    if (configurationPersistenceService == null) {
      // configurationPersistenceService will already be created in case of auto-reconnect
      configurationPersistenceService =
          new InternalConfigurationPersistenceService(internalCache, workingDirectory,
              JAXBService.create());
    }
    configurationPersistenceService
        .initSharedConfiguration(loadFromSharedConfigDir());
    logger.info(
        "Cluster configuration service start up completed successfully and is now running ....");
    isSharedConfigurationStarted = true;
  }

  public void startJmxManagerLocationService(InternalCache internalCache) {
    if (internalCache.getJmxManagerAdvisor() != null) {
      if (!membershipLocator.isHandled(JmxManagerLocatorRequest.class)) {
        JmxManagerLocator jmxHandler = new JmxManagerLocator(internalCache);
        restartHandlers.add(jmxHandler);
        membershipLocator.addHandler(JmxManagerLocatorRequest.class,
            jmxHandler);
      }
    }
  }

  private void installRequestHandlers() {
    if (!membershipLocator.isHandled(SharedConfigurationStatusRequest.class)) {
      membershipLocator.addHandler(SharedConfigurationStatusRequest.class,
          new SharedConfigurationStatusRequestHandler());
      logger.info("SharedConfigStatusRequestHandler installed");
    }

    if (!membershipLocator.isHandled(ClusterManagementServiceInfoRequest.class)) {
      membershipLocator.addHandler(ClusterManagementServiceInfoRequest.class,
          new ClusterManagementServiceInfoRequestHandler());
      logger.info("ClusterManagementServiceInfoRequestHandler installed");
    }
  }

  public boolean hasHandlerForClass(Class messageClass) {
    return membershipLocator.isHandled(messageClass);
  }

  class FetchSharedConfigStatus implements Callable<SharedConfigurationStatusResponse> {

    @Override
    public SharedConfigurationStatusResponse call() throws InterruptedException {
      InternalLocator locator = InternalLocator.this;

      SharedConfigurationStatusResponse response;
      if (configurationPersistenceService != null) {
        response = configurationPersistenceService.createStatusResponse();
      } else {
        response = new SharedConfigurationStatusResponse();
        response.setStatus(SharedConfigurationStatus.UNDETERMINED);
      }
      return response;
    }
  }

  /**
   * A helper object so that the TcpServer can record its stats to the proper place. Stats are only
   * recorded if a distributed system is started.
   */
  protected class DelayedPoolStatHelper implements PoolStatHelper {

    @Override
    public void startJob() {
      locatorStats.incRequestInProgress(1);

    }

    @Override
    public void endJob() {
      locatorStats.incRequestInProgress(-1);
    }
  }

  private class InternalLocatorTcpHandler implements TcpHandler {
    @Override
    public Object processRequest(Object request) throws IOException {
      return locatorListener == null ? null : locatorListener.handleRequest(request);
    }

    @Override
    public void endRequest(Object request, long startTime) {

    }

    @Override
    public void endResponse(Object request, long startTime) {

    }

    @Override
    public void shutDown() {
      handleShutdown();
    }

    @Override
    public void init(TcpServer tcpServer) {
      if (locatorListener != null) {
        // This is deferred until now as the initial requested port could have been 0
        locatorListener.setPort(tcpServer.getPort());
      }
    }
  }
}
