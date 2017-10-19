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

import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.client.internal.locator.ClientConnectionRequest;
import org.apache.geode.cache.client.internal.locator.ClientReplacementRequest;
import org.apache.geode.cache.client.internal.locator.GetAllServersRequest;
import org.apache.geode.cache.client.internal.locator.LocatorListRequest;
import org.apache.geode.cache.client.internal.locator.LocatorStatusRequest;
import org.apache.geode.cache.client.internal.locator.QueueConnectionRequest;
import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.InternalDistributedSystem.ConnectListener;
import org.apache.geode.distributed.internal.InternalDistributedSystem.DisconnectListener;
import org.apache.geode.distributed.internal.membership.MemberFactory;
import org.apache.geode.distributed.internal.membership.QuorumChecker;
import org.apache.geode.distributed.internal.membership.gms.NetLocator;
import org.apache.geode.distributed.internal.membership.gms.locator.PeerLocatorRequest;
import org.apache.geode.distributed.internal.tcpserver.LocatorCancelException;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.TcpServerFactory;
import org.apache.geode.internal.cache.wan.WANServiceProvider;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LogWriterFactory;
import org.apache.geode.internal.logging.LoggingThreadGroup;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogWriterAppenders;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.management.internal.JmxManagerLocator;
import org.apache.geode.management.internal.JmxManagerLocatorRequest;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.configuration.domain.SharedConfigurationStatus;
import org.apache.geode.management.internal.configuration.handlers.ConfigurationRequestHandler;
import org.apache.geode.management.internal.configuration.handlers.SharedConfigurationStatusRequestHandler;
import org.apache.geode.management.internal.configuration.messages.ConfigurationRequest;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusRequest;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusResponse;

/**
 * Provides the implementation of a distribution {@code Locator} as well as internal-only
 * functionality.
 * <p>
 * This class has APIs that perform essentially three layers of services. At the bottom layer is the
 * JGroups location service. On top of that you can start a distributed system. And then on top of
 * that you can start server location services.
 * <p>
 * Server Location Service DistributedSystem Peer Location Service
 * <p>
 * The startLocator() methods provide a way to start all three services in one call. Otherwise, the
 * services can be started independently {@code  locator = createLocator();
 * locator.startPeerLocation(); locator.startDistributeSystem();}
 * 
 * @since GemFire 4.0
 */
public class InternalLocator extends Locator implements ConnectListener {
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
      DistributionConfig.GEMFIRE_PREFIX + "disable-floating-coordinator";

  /**
   * The tcp server responding to locator requests
   */
  private final TcpServer server;

  /**
   * @since GemFire 5.7
   */
  private final PrimaryHandler handler;

  /**
   * The distributed system owned by this locator, if any. Note that if a ds already exists because
   * the locator is being colocated in a normal member this field will be null.
   */
  private InternalDistributedSystem myDs;
  /**
   * The cache owned by this locator, if any. Note that if a cache already exists because the
   * locator is being colocated in a normal member this field will be null.
   */
  private InternalCache myCache;

  /**
   * locator state file
   */
  private File stateFile;

  /**
   * product use logging
   */
  private ProductUseLog productUseLog;

  private boolean peerLocator;

  private ServerLocator serverLocator;

  protected volatile LocatorStats stats;

  private Properties env;

  /**
   * the TcpHandler used for peer location
   */
  private NetLocator locatorImpl;

  private DistributionConfigImpl config;

  private final LocatorMembershipListener locatorListener;

  private WanLocatorDiscoverer locatorDiscoverer;

  /**
   * whether the locator was stopped during forced-disconnect processing but a reconnect will occur
   */
  private volatile boolean stoppedForReconnect;

  /**
   * whether the locator was stopped during forced-disconnect processing
   */
  private volatile boolean forcedDisconnect;

  private final AtomicBoolean shutdownHandled = new AtomicBoolean(false);

  private ClusterConfigurationService sharedConfig;

  private volatile boolean isSharedConfigurationStarted = false;

  private volatile Thread restartThread;

  boolean isSharedConfigurationEnabled() {
    return this.config.getEnableClusterConfiguration();
  }

  private boolean loadFromSharedConfigDir() {
    return this.config.getLoadClusterConfigFromDir();
  }

  public boolean isSharedConfigurationRunning() {
    return this.sharedConfig != null
        && this.sharedConfig.getStatus() == SharedConfigurationStatus.RUNNING;
  }

  /**
   * the locator hosted by this JVM. As of 7.0 it is a singleton.
   *
   * GuardedBy must synchronize on locatorLock
   */
  private static InternalLocator locator;

  private static final Object locatorLock = new Object();

  // TODO: getLocator() overrides static method of a superclass
  public static InternalLocator getLocator() {
    // synchronize in order to fix #46336 (race condition in createLocator)
    synchronized (locatorLock) {
      return locator;
    }
  }

  // TODO: hasLocator() overrides static method of a superclass
  public static boolean hasLocator() {
    synchronized (locatorLock) {
      return locator != null;
    }
  }

  // TODO: return value of removeLocator is never used
  private static boolean removeLocator(InternalLocator locator) {
    if (locator == null) {
      return false;
    }
    synchronized (locatorLock) {
      LogWriterAppenders.stop(LogWriterAppenders.Identifier.MAIN);
      LogWriterAppenders.stop(LogWriterAppenders.Identifier.SECURITY);
      LogWriterAppenders.destroy(LogWriterAppenders.Identifier.MAIN);
      LogWriterAppenders.destroy(LogWriterAppenders.Identifier.SECURITY);
      if (locator != null && locator.equals(InternalLocator.locator)) {
        InternalLocator.locator = null;
        return true;
      }
      return false;
    }
  }

  public LocatorMembershipListener getlocatorMembershipListener() {
    return this.locatorListener;
  }

  /**
   * Create a locator that listens on a given port. This locator will not have peer or server
   * location services available until they are started by calling startServerLocation or
   * startPeerLocation on the locator object.
   * 
   * @param port the tcp/ip port to listen on
   * @param logFile the file that log messages should be written to
   * @param stateFile the file that state should be read from / written to for recovery
   * @param logger a log writer that should be used (logFile parameter is ignored)
   * @param securityLogger the logger to be used for security related log messages
   * @param distributedSystemProperties optional properties to configure the distributed system
   *        (e.g., mcast addr/port, other locators)
   * @param startDistributedSystem if true then this locator will also start its own ds
   */
  public static InternalLocator createLocator(int port, File logFile, File stateFile,
      InternalLogWriter logger, InternalLogWriter securityLogger, InetAddress bindAddress,
      String hostnameForClients, Properties distributedSystemProperties,
      boolean startDistributedSystem) {
    synchronized (locatorLock) {
      if (hasLocator()) {
        throw new IllegalStateException(
            "A locator can not be created because one already exists in this JVM.");
      }
      InternalLocator locator =
          new InternalLocator(port, logFile, stateFile, logger, securityLogger, bindAddress,
              hostnameForClients, distributedSystemProperties, null, startDistributedSystem);
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
   * Creates a distribution locator that runs in this VM on the given port and bind address.
   * <p>
   * This is for internal use only as it does not create a distributed system unless told to do so.
   * 
   * @param port the tcp/ip port to listen on
   * @param logFile the file that log messages should be written to
   * @param logger a log writer that should be used (logFile parameter is ignored)
   * @param securityLogger the logger to be used for security related log messages
   * @param startDistributedSystem if true, a distributed system is started
   * @param dsProperties optional properties to configure the distributed system (e.g., mcast
   *        addr/port, other locators)
   * @param hostnameForClients the name to give to clients for connecting to this locator
   */
  public static InternalLocator startLocator(int port, File logFile, File stateFile,
      InternalLogWriter logger, InternalLogWriter securityLogger, InetAddress bindAddress,
      boolean startDistributedSystem, Properties dsProperties, String hostnameForClients)
      throws IOException {

    System.setProperty(FORCE_LOCATOR_DM_TYPE, "true");
    InternalLocator newLocator = null;

    boolean startedLocator = false;
    try {

      newLocator = createLocator(port, logFile, stateFile, logger, securityLogger, bindAddress,
          hostnameForClients, dsProperties, startDistributedSystem);

      // TODO:GEODE-1243: this.server is now a TcpServer and it should store or return its non-zero
      // port in a variable to use here
      try {
        newLocator.startPeerLocation(startDistributedSystem);
        if (startDistributedSystem) {
          try {
            // TODO:GEODE-1243: throws Exception if TcpServer still has zero for its locator port
            newLocator.startDistributedSystem();
          } catch (RuntimeException e) {
            newLocator.stop();
            throw e;
          }
          // fix bug #46324
          final InternalDistributedSystem ids = newLocator.myDs;
          if (ids != null) {
            ids.getDistributionManager().addHostedLocators(ids.getDistributedMember(),
                getLocatorStrings(), newLocator.isSharedConfigurationEnabled());
          }
        }
      } catch (final LocatorCancelException ignored) {
        newLocator.stop();
      }

      InternalDistributedSystem sys = InternalDistributedSystem.getConnectedInstance();
      if (sys != null) {
        try {
          newLocator.startServerLocation(sys);
        } catch (RuntimeException e) {
          newLocator.stop();
          throw e;
        }
      }

      newLocator.endStartLocator(null);
      startedLocator = true;
      return newLocator;

    } finally {
      System.getProperties().remove(FORCE_LOCATOR_DM_TYPE);
      if (!startedLocator) {
        // fix for bug 46314
        removeLocator(newLocator);
      }
    }
  }

  /***
   * Determines if this VM is a locator which must ignore a shutdown.
   * 
   * @return true if this VM is a locator which should ignore a shutdown , false if it is a normal
   *         member.
   */
  public static boolean isDedicatedLocator() {
    InternalLocator internalLocator = getLocator();
    if (internalLocator == null) {
      return false;
    }

    InternalDistributedSystem ids = internalLocator.myDs;
    if (ids == null) {
      return false;
    }
    DM dm = ids.getDistributionManager();
    if (dm.isLoner()) {
      return false;
    }
    DistributionManager distMgr = (DistributionManager) ids.getDistributionManager();
    return distMgr.getDMType() == DistributionManager.LOCATOR_DM_TYPE;
  }

  /**
   * Creates a new {@code Locator} with the given port, log file, logger, and bind address.
   * 
   * @param port the tcp/ip port to listen on
   * @param logF the file that log messages should be written to
   * @param stateF the file that state should be read from / written to for recovery
   * @param logWriter a log writer that should be used (logFile parameter is ignored)
   * @param securityLogWriter the log writer to be used for security related log messages
   * @param hostnameForClients the name to give to clients for connecting to this locator
   * @param distributedSystemProperties optional properties to configure the distributed system
   *        (e.g., mcast addr/port, other locators)
   * @param cfg the config if being called from a distributed system; otherwise null.
   * @param startDistributedSystem if true locator will start its own distributed system
   */
  private InternalLocator(int port, File logF, File stateF, InternalLogWriter logWriter,
      // LOG: 3 non-null sources: GemFireDistributionLocator, InternalDistributedSystem,
      // LocatorLauncher
      InternalLogWriter securityLogWriter,
      // LOG: 1 non-null source: GemFireDistributionLocator(same instance as logWriter),
      // InternalDistributedSystem
      InetAddress bindAddress, String hostnameForClients, Properties distributedSystemProperties,
      DistributionConfigImpl cfg, boolean startDistributedSystem) {

    // TODO: the following three assignments are already done in superclass
    this.logFile = logF;
    this.bindAddress = bindAddress;
    this.hostnameForClients = hostnameForClients;

    if (stateF == null) {
      this.stateFile = new File("locator" + port + "view.dat");
    } else {
      this.stateFile = stateF;
    }
    File productUseFile = new File("locator" + port + "views.log");
    this.productUseLog = new ProductUseLog(productUseFile);
    this.config = cfg;

    this.env = new Properties();

    // set bind-address explicitly only if not wildcard and let any explicit
    // value in distributedSystemProperties take precedence (#46870)
    if (bindAddress != null && !bindAddress.isAnyLocalAddress()) {
      this.env.setProperty(BIND_ADDRESS, bindAddress.getHostAddress());
    }

    if (distributedSystemProperties != null) {
      this.env.putAll(distributedSystemProperties);
    }
    this.env.setProperty(CACHE_XML_FILE, "");

    // create a DC so that all of the lookup rules, gemfire.properties, etc,
    // are considered and we have a config object we can trust
    if (this.config == null) {
      this.config = new DistributionConfigImpl(this.env);
      this.env.clear();
      this.env.putAll(this.config.getProps());
    }

    final boolean hasLogFileButConfigDoesNot = this.logFile != null && this.config.getLogFile()
        .toString().equals(DistributionConfig.DEFAULT_LOG_FILE.toString());
    if (logWriter == null && hasLogFileButConfigDoesNot) {
      // LOG: this is(was) a hack for when logFile and config don't match -- if config specifies a
      // different log-file things will break!
      this.config.unsafeSetLogFile(this.logFile);
    }

    // LOG: create LogWriterAppenders (these are closed at shutdown)
    final boolean hasLogFile =
        this.config.getLogFile() != null && !this.config.getLogFile().equals(new File(""));
    final boolean hasSecurityLogFile = this.config.getSecurityLogFile() != null
        && !this.config.getSecurityLogFile().equals(new File(""));
    LogService.configureLoggers(hasLogFile, hasSecurityLogFile);
    if (hasLogFile || hasSecurityLogFile) {

      if (hasLogFile) {
        // if log-file then create logWriterAppender
        LogWriterAppenders.getOrCreateAppender(LogWriterAppenders.Identifier.MAIN, true, false,
            this.config, !startDistributedSystem);
      }

      if (hasSecurityLogFile) {
        // if security-log-file then create securityLogWriterAppender
        LogWriterAppenders.getOrCreateAppender(LogWriterAppenders.Identifier.SECURITY, true, false,
            this.config, false);
      }
      // do not create a LogWriterAppender for security -- let it go through to logWriterAppender
    }

    // LOG: create LogWriters for GemFireTracer (or use whatever was passed in)
    if (logWriter == null) {
      logWriter = LogWriterFactory.createLogWriterLogger(false, false, this.config, false);
      if (logger.isDebugEnabled()) {
        logger.debug("LogWriter for locator is created.");
      }
    }

    if (securityLogWriter == null) {
      securityLogWriter = LogWriterFactory.createLogWriterLogger(false, true, this.config, false);
      logWriter.setLogWriterLevel(this.config.getSecurityLogLevel());
      securityLogWriter.fine("SecurityLogWriter for locator is created.");
    }

    SocketCreatorFactory.setDistributionConfig(this.config);

    this.locatorListener = WANServiceProvider.createLocatorMembershipListener();
    if (this.locatorListener != null) {
      // We defer setting the port until the handler is init'd - that way we'll have an actual port
      // in the case where we're starting with port = 0.
      this.locatorListener.setConfig(getConfig());
    }
    this.handler = new PrimaryHandler(this, locatorListener);

    ThreadGroup group = LoggingThreadGroup.createThreadGroup("Distribution locators", logger);
    this.stats = new LocatorStats();

    this.server = new TcpServerFactory().makeTcpServer(port, this.bindAddress, null, this.config,
        this.handler, new DelayedPoolStatHelper(), group, this.toString(), this);
  }

  // Reset the file names with the correct port number if startLocatorAndDS was called with port
  // number 0
  public void resetInternalLocatorFileNamesWithCorrectPortNumber(int port) {
    this.stateFile = new File("locator" + port + "view.dat");
    File productUseFile = new File("locator" + port + "views.log");
    this.productUseLog = new ProductUseLog(productUseFile);
  }

  private void startTcpServer() throws IOException {
    logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_STARTING_0, this));
    this.server.start();
  }

  public ClusterConfigurationService getSharedConfiguration() {
    return this.sharedConfig;
  }

  public DistributionConfigImpl getConfig() {
    return this.config;
  }

  public InternalCache getCache() {
    return myCache;
  }

  /**
   * Start peer location in this locator. If you plan on starting a distributed system later, this
   * method should be called first so that the distributed system can use this locator.
   * <p>
   * TODO: parameter withDS is never used
   *
   * @param withDS true if a distributed system has been or will be started
   *
   * @since GemFire 5.7
   */
  void startPeerLocation(boolean withDS) throws IOException {
    if (isPeerLocator()) {
      throw new IllegalStateException(
          LocalizedStrings.InternalLocator_PEER_LOCATION_IS_ALREADY_RUNNING_FOR_0
              .toLocalizedString(this));
    }
    logger.info(LocalizedMessage
        .create(LocalizedStrings.InternalLocator_STARTING_PEER_LOCATION_FOR_0, this));

    String locatorsProp = this.config.getLocators();

    // check for settings that would require only locators to hold the
    // coordinator - e.g., security and network-partition detection
    boolean locatorsAreCoordinators = false;
    boolean networkPartitionDetectionEnabled = this.config.getEnableNetworkPartitionDetection();
    String securityUDPDHAlgo = this.config.getSecurityUDPDHAlgo();
    if (networkPartitionDetectionEnabled) {
      locatorsAreCoordinators = true;
    } else {
      // check if security is enabled
      String prop = this.config.getSecurityPeerAuthInit();
      locatorsAreCoordinators = prop != null && !prop.isEmpty();
      if (!locatorsAreCoordinators) {
        locatorsAreCoordinators = Boolean.getBoolean(LOCATORS_PREFERRED_AS_COORDINATORS);
      }
    }

    this.locatorImpl = MemberFactory.newLocatorHandler(this.bindAddress, this.stateFile,
        locatorsProp, locatorsAreCoordinators, networkPartitionDetectionEnabled, this.stats,
        securityUDPDHAlgo);
    this.handler.addHandler(PeerLocatorRequest.class, this.locatorImpl);
    this.peerLocator = true;
    if (!this.server.isAlive()) {
      startTcpServer();
    }
  }

  /**
   * @return the TcpHandler for peer to peer discovery
   */
  public NetLocator getLocatorHandler() {
    return this.locatorImpl;
  }

  public PrimaryHandler getPrimaryHandler() {
    return this.handler;
  }

  /**
   * For backward-compatibility we retain this method
   * <p>
   * TODO: parameters peerLocator and serverLocator and b1 are never used
   * 
   * @deprecated use a form of the method that does not have peerLocator/serverLocator parameters
   */
  @Deprecated
  public static InternalLocator startLocator(int locatorPort, File logFile, File stateFile,
      InternalLogWriter logger, InternalLogWriter logger1, InetAddress addr,
      Properties dsProperties, boolean peerLocator, boolean serverLocator, String s, boolean b1)
      throws IOException {
    return startLocator(locatorPort, logFile, stateFile, logger, logger1, addr, true, dsProperties,
        s);
  }

  class SharedConfigurationRunnable implements Runnable {

    private final InternalLocator locator = InternalLocator.this;

    @Override
    public void run() {
      try {
        if (this.locator.sharedConfig == null) {
          // locator.sharedConfig will already be created in case of auto-reconnect
          this.locator.sharedConfig = new ClusterConfigurationService(locator.myCache);
        }
        this.locator.sharedConfig.initSharedConfiguration(this.locator.loadFromSharedConfigDir());
        this.locator.installSharedConfigDistribution();
        logger.info(
            "Cluster configuration service start up completed successfully and is now running ....");
      } catch (CancelException | LockServiceDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Cluster configuration start up was cancelled", e);
        }
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }
  }

  /**
   * Start a distributed system whose life cycle is managed by this locator. When the locator is
   * stopped, this distributed system will be disconnected. If a distributed system already exists,
   * this method will have no affect.
   * 
   * @since GemFire 5.7
   */
  private void startDistributedSystem() throws UnknownHostException {
    InternalDistributedSystem existing = InternalDistributedSystem.getConnectedInstance();
    if (existing != null) {
      // LOG: changed from config to info
      logger.info(LocalizedMessage
          .create(LocalizedStrings.InternalLocator_USING_EXISTING_DISTRIBUTED_SYSTEM__0, existing));
      startCache(existing);
    } else {

      StringBuilder sb = new StringBuilder(100);
      if (this.bindAddress != null) {
        sb.append(this.bindAddress.getHostAddress());
      } else {
        sb.append(SocketCreator.getLocalHost().getHostAddress());
      }
      sb.append('[').append(getPort()).append(']');
      String thisLocator = sb.toString();


      if (this.peerLocator) {
        // append this locator to the locators list from the config properties
        // this.logger.config("ensuring that this locator is in the locators list");
        boolean setLocatorsProp = false;
        String locatorsProp = this.config.getLocators();
        if (StringUtils.isNotBlank(locatorsProp)) {
          if (!locatorsProp.contains(thisLocator)) {
            locatorsProp = locatorsProp + ',' + thisLocator;
            setLocatorsProp = true;
          }
        } else {
          locatorsProp = thisLocator;
          setLocatorsProp = true;
        }
        if (setLocatorsProp) {
          Properties updateEnv = new Properties();
          updateEnv.setProperty(LOCATORS, locatorsProp);
          this.config.setApiProps(updateEnv);
          // fix for bug 41248
          String propName = DistributionConfig.GEMFIRE_PREFIX + LOCATORS;
          if (System.getProperty(propName) != null) {
            System.setProperty(propName, locatorsProp);
          }
        }
        // No longer default mcast-port to zero. See 46277.
      }

      Properties connectEnv = new Properties();
      // LogWriterAppender is now shared via that class
      // using a DistributionConfig earlier in this method
      connectEnv.put(DistributionConfig.DS_CONFIG_NAME, this.config);

      logger.info(
          LocalizedMessage.create(LocalizedStrings.InternalLocator_STARTING_DISTRIBUTED_SYSTEM));

      this.myDs = (InternalDistributedSystem) DistributedSystem.connect(connectEnv);

      if (this.peerLocator) {
        this.locatorImpl.setMembershipManager(this.myDs.getDM().getMembershipManager());
      }

      this.myDs.addDisconnectListener(new DisconnectListener() {
        @Override
        public void onDisconnect(InternalDistributedSystem sys) {
          stop(false, false, false);
        }
      });

      startCache(myDs);

      logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_LOCATOR_STARTED_ON__0,
          thisLocator));

      myDs.setDependentLocator(this);
    }
  }

  private void startCache(DistributedSystem ds) {
    InternalCache internalCache = GemFireCacheImpl.getInstance();
    if (internalCache == null) {
      logger.info("Creating cache for locator.");
      this.myCache = (InternalCache) new CacheFactory(ds.getProperties()).create();
      internalCache = this.myCache;
    } else {
      logger.info("Using existing cache for locator.");
      ((InternalDistributedSystem) ds).handleResourceEvent(ResourceEvent.LOCATOR_START, this);
    }
    startJmxManagerLocationService(internalCache);

    startSharedConfigurationService(internalCache);
  }

  /**
   * End the initialization of the locator. This method should be called once the location services
   * and distributed system are started.
   * 
   * @param distributedSystem The distributed system to use for the statistics.
   *
   * @since GemFire 5.7
   */
  void endStartLocator(InternalDistributedSystem distributedSystem) {
    this.env = null;
    if (distributedSystem == null) {
      distributedSystem = InternalDistributedSystem.getConnectedInstance();
    }
    if (distributedSystem != null) {
      onConnect(distributedSystem);
    } else {
      InternalDistributedSystem.addConnectListener(this);
    }

    this.locatorDiscoverer = WANServiceProvider.createLocatorDiscoverer();
    if (this.locatorDiscoverer != null) {
      this.locatorDiscoverer.discover(getPort(), this.config, this.locatorListener,
          this.hostnameForClients);
    }
  }

  /**
   * Start server location services in this locator. Server location can only be started once there
   * is a running distributed system.
   * 
   * @param distributedSystem The distributed system which the server location services should use.
   *        If null, the method will try to find an already connected distributed system.
   *
   * @since GemFire 5.7
   */
  void startServerLocation(InternalDistributedSystem distributedSystem) throws IOException {
    if (isServerLocator()) {
      throw new IllegalStateException(
          LocalizedStrings.InternalLocator_SERVER_LOCATION_IS_ALREADY_RUNNING_FOR_0
              .toLocalizedString(this));
    }
    logger.info(LocalizedMessage
        .create(LocalizedStrings.InternalLocator_STARTING_SERVER_LOCATION_FOR_0, this));

    if (distributedSystem == null) {
      distributedSystem = InternalDistributedSystem.getConnectedInstance();
      if (distributedSystem == null) {
        throw new IllegalStateException(
            LocalizedStrings.InternalLocator_SINCE_SERVER_LOCATION_IS_ENABLED_THE_DISTRIBUTED_SYSTEM_MUST_BE_CONNECTED
                .toLocalizedString());
      }
    }

    this.productUseLog.monitorUse(distributedSystem);

    ServerLocator serverLocator =
        new ServerLocator(getPort(), this.bindAddress, this.hostnameForClients, this.logFile,
            this.productUseLog, getConfig().getName(), distributedSystem, this.stats);
    this.handler.addHandler(LocatorListRequest.class, serverLocator);
    this.handler.addHandler(ClientConnectionRequest.class, serverLocator);
    this.handler.addHandler(QueueConnectionRequest.class, serverLocator);
    this.handler.addHandler(ClientReplacementRequest.class, serverLocator);
    this.handler.addHandler(GetAllServersRequest.class, serverLocator);
    this.handler.addHandler(LocatorStatusRequest.class, serverLocator);
    this.serverLocator = serverLocator;
    if (!this.server.isAlive()) {
      startTcpServer();
    }
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
    final boolean isDebugEnabled = logger.isDebugEnabled();

    this.stoppedForReconnect = stopForReconnect;
    this.forcedDisconnect = forcedDisconnect;

    if (this.server.isShuttingDown()) {
      // fix for bug 46156
      // If we are already shutting down don't do all of this again.
      // But, give the server a bit of time to shut down so a new
      // locator can be created, if desired, when this method returns
      if (!stopForReconnect && waitForDisconnect) {
        long endOfWait = System.currentTimeMillis() + 60000;
        if (isDebugEnabled && this.server.isAlive()) {
          logger.debug("sleeping to wait for the locator server to shut down...");
        }
        while (this.server.isAlive() && System.currentTimeMillis() < endOfWait) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        if (isDebugEnabled) {
          if (this.server.isAlive()) {
            logger.debug(
                "60 seconds have elapsed waiting for the locator server to shut down - terminating wait and returning");
          } else {
            logger.debug("the locator server has shut down");
          }
        }
      }
      return;
    }

    if (this.locatorDiscoverer != null) {
      this.locatorDiscoverer.stop();
      this.locatorDiscoverer = null;
    }

    if (this.server.isAlive()) {
      logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_STOPPING__0, this));
      try {
        new TcpClient().stop(this.bindAddress, getPort());
      } catch (ConnectException ignore) {
        // must not be running
      }
      boolean interrupted = Thread.interrupted();
      try {
        this.server.join(TcpServer.SHUTDOWN_WAIT_TIME * 1000 + 10000);

      } catch (InterruptedException ex) {
        interrupted = true;
        logger.warn(LocalizedMessage
            .create(LocalizedStrings.InternalLocator_INTERRUPTED_WHILE_STOPPING__0, this), ex);

        // Continue running -- doing our best to stop everything...
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }

      if (this.server.isAlive()) {
        logger.fatal(LocalizedMessage
            .create(LocalizedStrings.InternalLocator_COULD_NOT_STOP__0__IN_60_SECONDS, this));
      }
    }

    removeLocator(this);

    handleShutdown();

    logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_0__IS_STOPPED, this));

    if (this.stoppedForReconnect) {
      if (this.myDs != null) {
        launchRestartThread();
      }
    }
  }

  /**
   * answers whether this locator is currently stopped
   */
  public boolean isStopped() {
    return this.server == null || !this.server.isAlive();
  }

  private void handleShutdown() {
    if (!this.shutdownHandled.compareAndSet(false, true)) {
      return; // already shutdown
    }
    this.productUseLog.close();
    if (this.myDs != null) {
      this.myDs.setDependentLocator(null);
    }
    if (this.myCache != null && !this.stoppedForReconnect && !this.forcedDisconnect) {
      logger.info("Closing locator's cache");
      try {
        this.myCache.close();
      } catch (RuntimeException ex) {
        logger.info("Could not close locator's cache because: {}", ex.getMessage(), ex);
      }
    }

    if (this.stats != null) {
      this.stats.close();
    }

    if (this.locatorListener != null) {
      this.locatorListener.clearLocatorInfo();
    }

    this.isSharedConfigurationStarted = false;
    if (this.myDs != null && !this.forcedDisconnect) {
      if (this.myDs.isConnected()) {
        logger.info(LocalizedMessage
            .create(LocalizedStrings.InternalLocator_DISCONNECTING_DISTRIBUTED_SYSTEM_FOR_0, this));
        this.myDs.disconnect();
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
      DistributedSystem ds = this.myDs;
      restarted = false;
      this.server.join();
      if (this.stoppedForReconnect) {
        logger.info("waiting for distributed system to disconnect...");
        while (ds.isConnected()) {
          Thread.sleep(5000);
        }
        logger.info("waiting for distributed system to reconnect...");
        restarted = ds.waitUntilReconnected(-1, TimeUnit.SECONDS);
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
    // create a thread group having a last-chance exception-handler
    ThreadGroup group = LoggingThreadGroup.createThreadGroup("Locator restart thread group");
    // TODO: non-atomic operation on volatile field restartThread
    this.restartThread = new Thread(group, "Location services restart thread") {
      @Override
      public void run() {
        boolean restarted = false;
        try {
          restarted = attemptReconnect();
          logger.info("attemptReconnect returned {}", restarted);
        } catch (InterruptedException e) {
          logger.info("attempt to restart location services was interrupted", e);
        } catch (IOException e) {
          logger.info("attempt to restart location services terminated", e);
        } finally {
          if (!restarted) {
            stoppedForReconnect = false;
          }
        }
        InternalLocator.this.restartThread = null;
      }
    };
    this.restartThread.setDaemon(true);
    this.restartThread.start();
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
    if (this.stoppedForReconnect) {
      logger.info("attempting to restart locator");
      boolean tcpServerStarted = false;
      InternalDistributedSystem ds = this.myDs;
      long waitTime = ds.getConfig().getMaxWaitTimeForReconnect() / 2;
      QuorumChecker checker = null;
      while (ds.getReconnectedSystem() == null && !ds.isReconnectCancelled()) {
        if (checker == null) {
          checker = this.myDs.getQuorumChecker();
          if (checker != null) {
            logger.info("The distributed system returned this quorum checker: {}", checker);
          }
        }
        if (checker != null && !tcpServerStarted) {
          boolean start = checker.checkForQuorum(3 * this.myDs.getConfig().getMemberTimeout());
          if (start) {
            // start up peer location. server location is started after the DS finishes
            // reconnecting
            logger.info("starting peer location");
            if (this.locatorListener != null) {
              this.locatorListener.clearLocatorInfo();
            }
            this.stoppedForReconnect = false;
            this.myDs = null;
            this.myCache = null;
            restartWithoutDS();
            tcpServerStarted = true;
            setLocator(this);
          }
        }
        ds.waitUntilReconnected(waitTime, TimeUnit.MILLISECONDS);
      }
      InternalDistributedSystem newSystem = (InternalDistributedSystem) ds.getReconnectedSystem();
      if (newSystem != null) {
        if (!tcpServerStarted) {
          if (this.locatorListener != null) {
            this.locatorListener.clearLocatorInfo();
          }
          this.stoppedForReconnect = false;
        }
        restartWithDS(newSystem, GemFireCacheImpl.getInstance());
        setLocator(this);
        restarted = true;
      }
    }
    logger.info("restart thread exiting.  Service was {}restarted", restarted ? "" : "not ");
    return restarted;
  }

  private void restartWithoutDS() throws IOException {
    synchronized (locatorLock) {
      if (locator != this && hasLocator()) {
        throw new IllegalStateException(
            "A locator can not be created because one already exists in this JVM.");
      }
      this.myDs = null;
      this.myCache = null;
      logger.info("Locator restart: initializing TcpServer peer location services");
      this.server.restarting(null, null, null);
      if (this.productUseLog.isClosed()) {
        this.productUseLog.reopen();
      }
      if (!this.server.isAlive()) {
        logger.info("Locator restart: starting TcpServer");
        startTcpServer();
      }
    }
  }

  private void restartWithDS(InternalDistributedSystem newSystem, InternalCache newCache)
      throws IOException {
    synchronized (locatorLock) {
      if (locator != this && hasLocator()) {
        throw new IllegalStateException(
            "A locator can not be created because one already exists in this JVM.");
      }
      this.myDs = newSystem;
      this.myCache = newCache;
      this.myDs.setDependentLocator(this);
      logger.info("Locator restart: initializing TcpServer");
      if (isSharedConfigurationEnabled()) {
        this.sharedConfig = new ClusterConfigurationService(newCache);
      }
      this.server.restarting(newSystem, newCache, this.sharedConfig);
      if (this.productUseLog.isClosed()) {
        this.productUseLog.reopen();
      }
      this.productUseLog.monitorUse(newSystem);
      this.isSharedConfigurationStarted = true;
      if (isSharedConfigurationEnabled()) {
        ExecutorService es = newCache.getDistributionManager().getThreadPool();
        es.execute(new SharedConfigurationRunnable());
      }
      if (!this.server.isAlive()) {
        logger.info("Locator restart: starting TcpServer");
        startTcpServer();
      }
      logger.info("Locator restart: initializing JMX manager");
      startJmxManagerLocationService(newCache);
      endStartLocator(this.myDs);
      logger.info("Locator restart completed");
    }
  }

  @Override
  public DistributedSystem getDistributedSystem() {
    return this.myDs;
  }

  @Override
  public boolean isPeerLocator() {
    return this.peerLocator;
  }

  @Override
  public boolean isServerLocator() {
    return this.serverLocator != null;
  }

  /**
   * Returns null if no server locator; otherwise returns the advisee that represents the server
   * locator.
   */
  public ServerLocator getServerLocatorAdvisee() {
    return this.serverLocator;
  }

  /**
   * Return the port on which the locator is actually listening. If called before the locator has
   * actually started, this method will return null.
   * 
   * @return the port the locator is listening on or null if it has not yet been started
   */
  @Override
  public Integer getPort() {
    if (this.server != null) {
      return this.server.getPort();
    }
    return null;
  }

  class FetchSharedConfigStatus implements Callable<SharedConfigurationStatusResponse> {

    static final int SLEEPTIME = 1000;
    static final byte MAX_RETRIES = 5;

    @Override
    public SharedConfigurationStatusResponse call() throws InterruptedException {
      final InternalLocator locator = InternalLocator.this;
      // TODO: this for-loop is probably not necessary as the if to break is always true
      for (int i = 0; i < MAX_RETRIES; i++) {
        if (locator.sharedConfig != null) {
          SharedConfigurationStatus status = locator.sharedConfig.getStatus();
          if (status != SharedConfigurationStatus.STARTED
              || status != SharedConfigurationStatus.NOT_STARTED) {
            break;
          }
        }
        Thread.sleep(SLEEPTIME);
      }
      SharedConfigurationStatusResponse response;
      if (locator.sharedConfig != null) {
        response = locator.sharedConfig.createStatusResponse();
      } else {
        response = new SharedConfigurationStatusResponse();
        response.setStatus(SharedConfigurationStatus.UNDETERMINED);
      }
      return response;
    }
  }

  public SharedConfigurationStatusResponse getSharedConfigurationStatus() {
    ExecutorService es = this.myCache.getDistributionManager().getWaitingThreadPool();
    Future<SharedConfigurationStatusResponse> statusFuture =
        es.submit(new FetchSharedConfigStatus());
    SharedConfigurationStatusResponse response;

    try {
      response = statusFuture.get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.info("Exception occurred while fetching the status {}", CliUtil.stackTraceAsString(e));
      response = new SharedConfigurationStatusResponse();
      response.setStatus(SharedConfigurationStatus.UNDETERMINED);
    }
    return response;
  }

  public static class PrimaryHandler implements TcpHandler {

    private volatile HashMap<Class, TcpHandler> handlerMapping = new HashMap<>();
    private volatile HashSet<TcpHandler> allHandlers = new HashSet<>();
    private TcpServer tcpServer;
    private final LocatorMembershipListener locatorListener;
    private final InternalLocator internalLocator;
    // GEODE-2253 test condition
    private boolean hasWaitedForHandlerInitialization = false;

    PrimaryHandler(InternalLocator locator, LocatorMembershipListener listener) {
      this.locatorListener = listener;
      this.internalLocator = locator;
    }

    // this method is synchronized to make sure that no new handlers are added while
    // initialization is taking place.
    @Override
    public synchronized void init(TcpServer tcpServer) {
      if (this.locatorListener != null) {
        // This is deferred until now as the initial requested port could have been 0
        this.locatorListener.setPort(this.internalLocator.getPort());
      }
      this.tcpServer = tcpServer;
      for (TcpHandler handler : this.allHandlers) {
        handler.init(tcpServer);
      }
    }

    @Override
    public void restarting(DistributedSystem ds, GemFireCache cache,
        ClusterConfigurationService sharedConfig) {
      if (ds != null) {
        for (TcpHandler handler : this.allHandlers) {
          handler.restarting(ds, cache, sharedConfig);
        }
      }
    }

    @Override
    public Object processRequest(Object request) throws IOException {
      long giveup = 0;
      while (giveup == 0 || System.currentTimeMillis() < giveup) {
        TcpHandler handler;
        if (request instanceof PeerLocatorRequest) {
          handler = this.handlerMapping.get(PeerLocatorRequest.class);
        } else {
          handler = this.handlerMapping.get(request.getClass());
        }

        if (handler != null) {
          return handler.processRequest(request);
        } else {
          if (this.locatorListener != null) {
            return this.locatorListener.handleRequest(request);
          } else {
            // either there is a configuration problem or the locator is still starting up
            if (giveup == 0) {
              int locatorWaitTime = this.internalLocator.getConfig().getLocatorWaitTime();
              if (locatorWaitTime <= 0) {
                // always retry some number of times
                locatorWaitTime = 30;
              }
              this.hasWaitedForHandlerInitialization = true;
              giveup = System.currentTimeMillis() + locatorWaitTime * 1000;
              try {
                Thread.sleep(1000);
              } catch (InterruptedException ignored) {
                // running in an executor - no need to set the interrupted flag on the thread
                return null;
              }
            }
          }
        }
      } // while
      logger.info(
          "Received a location request of class {} but the handler for this is "
              + "either not enabled or is not ready to process requests",
          request.getClass().getSimpleName());
      return null;
    }

    /**
     * GEODE-2253 test condition - has this handler waited for a subordinate handler to be
     * installed?
     */
    public boolean hasWaitedForHandlerInitialization() {
      return this.hasWaitedForHandlerInitialization;
    }

    @Override
    public void shutDown() {
      try {
        for (TcpHandler handler : this.allHandlers) {
          handler.shutDown();
        }
      } finally {
        this.internalLocator.handleShutdown();
      }
    }

    synchronized boolean isHandled(Class clazz) {
      return this.handlerMapping.containsKey(clazz);
    }

    public synchronized void addHandler(Class clazz, TcpHandler handler) {
      HashMap<Class, TcpHandler> tmpHandlerMapping = new HashMap<>(this.handlerMapping);
      HashSet<TcpHandler> tmpAllHandlers = new HashSet<>(this.allHandlers);
      tmpHandlerMapping.put(clazz, handler);
      if (tmpAllHandlers.add(handler) && this.tcpServer != null) {
        handler.init(this.tcpServer);
      }
      this.handlerMapping = tmpHandlerMapping;
      this.allHandlers = tmpAllHandlers;
    }

    @Override
    public void endRequest(Object request, long startTime) {
      TcpHandler handler = this.handlerMapping.get(request.getClass());
      if (handler != null) {
        handler.endRequest(request, startTime);
      }
    }

    @Override
    public void endResponse(Object request, long startTime) {
      TcpHandler handler = this.handlerMapping.get(request.getClass());
      if (handler != null) {
        handler.endResponse(request, startTime);
      }
    }
  }

  @Override
  public void onConnect(InternalDistributedSystem sys) {
    try {
      this.stats.hookupStats(sys,
          SocketCreator.getLocalHost().getCanonicalHostName() + '-' + this.server.getBindAddress());
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
      Collection<DistributionLocatorId> locatorIds =
          DistributionLocatorId.asDistributionLocatorIds(getLocators());
      locatorStrings = DistributionLocatorId.asStrings(locatorIds);
    } catch (UnknownHostException ignored) {
      locatorStrings = null;
    }
    if (locatorStrings == null || locatorStrings.isEmpty()) {
      return null;
    } else {
      return locatorStrings;
    }
  }

  /**
   * A helper object so that the TcpServer can record its stats to the proper place. Stats are only
   * recorded if a distributed system is started.
   */
  protected class DelayedPoolStatHelper implements PoolStatHelper {
    @Override
    public void startJob() {
      stats.incRequestInProgress(1);

    }

    @Override
    public void endJob() {
      stats.incRequestInProgress(-1);
    }
  }

  private void startSharedConfigurationService(InternalCache internalCache) {
    installSharedConfigHandler();

    if (this.config.getEnableClusterConfiguration() && !this.isSharedConfigurationStarted) {
      if (!isDedicatedLocator()) {
        logger.info("Cluster configuration service not enabled as it is only supported "
            + "in dedicated locators");
        return;
      }

      ExecutorService es = internalCache.getDistributionManager().getThreadPool();
      es.execute(new SharedConfigurationRunnable());
    } else {
      logger.info("Cluster configuration service is disabled");
    }
  }

  public void startJmxManagerLocationService(InternalCache internalCache) {
    if (internalCache.getJmxManagerAdvisor() != null) {
      if (!this.handler.isHandled(JmxManagerLocatorRequest.class)) {
        this.handler.addHandler(JmxManagerLocatorRequest.class,
            new JmxManagerLocator(internalCache));
      }
    }
  }

  /**
   * Creates and installs the handler {@link ConfigurationRequestHandler}
   */
  private void installSharedConfigDistribution() {
    if (!this.handler.isHandled(ConfigurationRequest.class)) {
      this.handler.addHandler(ConfigurationRequest.class,
          new ConfigurationRequestHandler(this.sharedConfig));
      logger.info("ConfigRequestHandler installed");
    }
  }

  private void installSharedConfigHandler() {
    if (!this.handler.isHandled(SharedConfigurationStatusRequest.class)) {
      this.handler.addHandler(SharedConfigurationStatusRequest.class,
          new SharedConfigurationStatusRequestHandler());
      logger.info("SharedConfigStatusRequestHandler installed");
    }
  }

  public boolean hasHandlerForClass(Class messageClass) {
    return this.handler.isHandled(messageClass);
  }

}
