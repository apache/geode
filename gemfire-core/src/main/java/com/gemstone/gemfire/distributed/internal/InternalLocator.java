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
package com.gemstone.gemfire.distributed.internal;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.gemfire.InternalGemFireException;
import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.ClientReplacementRequest;
import com.gemstone.gemfire.cache.client.internal.locator.GetAllServersRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorListRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusResponse;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.ServerLocationRequest;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorMembershipListener;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.LockServiceDestroyedException;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.ConnectListener;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.DisconnectListener;
import com.gemstone.gemfire.distributed.internal.membership.MemberFactory;
import com.gemstone.gemfire.distributed.internal.membership.QuorumChecker;
import com.gemstone.gemfire.distributed.internal.membership.gms.NetLocator;
import com.gemstone.gemfire.distributed.internal.membership.gms.locator.PeerLocatorRequest;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpHandler;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.wan.WANServiceProvider;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LogWriterFactory;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterAppenders;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;
import com.gemstone.gemfire.management.internal.JmxManagerLocator;
import com.gemstone.gemfire.management.internal.JmxManagerLocatorRequest;
import com.gemstone.gemfire.management.internal.JmxManagerLocatorResponse;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.configuration.domain.SharedConfigurationStatus;
import com.gemstone.gemfire.management.internal.configuration.handlers.ConfigurationRequestHandler;
import com.gemstone.gemfire.management.internal.configuration.handlers.SharedConfigurationStatusRequestHandler;
import com.gemstone.gemfire.management.internal.configuration.messages.ConfigurationRequest;
import com.gemstone.gemfire.management.internal.configuration.messages.SharedConfigurationStatusRequest;
import com.gemstone.gemfire.management.internal.configuration.messages.SharedConfigurationStatusResponse;

/**
 * Provides the implementation of a distribution <code>Locator</code>
 * as well as internal-only functionality.  Currently, a distribution
 * locator is implemented using a JGroups {@link GossipServer}.
 * 
 * This class has APIs that perform essentially three layers of 
 * services. At the bottom layer is the JGroups location service. On
 * top of that you can start a distributed system. And then on top
 * of that you can start server location services.
 * 
 * Server Location Service
 * DistributedSystem
 * Peer Location Service
 * 
 * The startLocator() methods provide a way to start all three
 * services in one call. Otherwise, the services can be started 
 * independently
 * <code>
 * locator = createLocator()
 * locator.startPeerLocation();
 * locator.startDistributeSystem();
 *
 * @author David Whitlock
 * @since 4.0
 */
public class InternalLocator extends Locator implements ConnectListener {

  private static final Logger logger = LogService.getLogger();
  
  /** How long (in milliseconds) a member that we haven't heard from
   * in a while should live before we call it dead? */
  private static final long EXPIRY_MS = 60000; // one minute
  
  private static final int SHARED_CONFIG_STATUS_TIMEOUT = 10000; //10 seconds
  
  /** system property name for forcing an locator distribution manager type */
  public static final String FORCE_LOCATOR_DM_TYPE = "Locator.forceLocatorDMType";
  
  /** system property name for inhibiting DM banner */
  public static final String INHIBIT_DM_BANNER = "Locator.inhibitDMBanner";
  
  /** system property name for forcing locators to be preferred as coordinators */
  public static final String LOCATORS_PREFERRED_AS_COORDINATORS = "gemfire.disable-floating-coordinator";

  /////////////////////  Instance Fields  //////////////////////

  /** The tcp server responding to locator requests */
  private final TcpServer server;

  /**
   * @since 5.7
   */
  private final PrimaryHandler handler;
  
  /** The distributed system owned by this locator, if any.
   * Note that if a ds already exists because the locator is
   * being colocated in a normal member this field will be null.
   */
  private InternalDistributedSystem myDs;
  /** The cache owned by this locator, if any.
   * Note that if a cache already exists because the locator is
   * being colocated in a normal member this field will be null.
   */
  private Cache myCache;
  
  /** locator state file */
  private File stateFile;
  
  /** product use logging */
  private ProductUseLog productUseLog;
  
  private boolean peerLocator;
  
  private ServerLocator serverLocator;
  
  protected volatile LocatorStats stats;

  //TODO - these to properties are a waste of memory once
  //the system is started.
  private Properties env;
  
  /** the TcpHandler used for peer location */
  private NetLocator locatorImpl;
  
  private DistributionConfigImpl config;
  
  private LocatorMembershipListener locatorListener;
  
  /** whether the locator was stopped during forced-disconnect processing but a reconnect will occur */
  private volatile boolean stoppedForReconnect;
  
  /** whether the locator was stopped during forced-disconnect processing */
  private volatile boolean forcedDisconnect;
  
  private final AtomicBoolean shutdownHandled = new AtomicBoolean(false);
  
  private SharedConfiguration sharedConfig;
  
  private volatile boolean isSharedConfigurationStarted = false; 
  
  private volatile Thread restartThread;
  
  
  public boolean isSharedConfigurationEnabled() {
    return this.config.getEnableClusterConfiguration();
  }
  
  public boolean loadFromSharedConfigDir() {
    return this.config.getLoadClusterConfigFromDir();
  }
  
  public boolean isSharedConfigurationRunning() {
    if (this.sharedConfig != null) {
      return this.sharedConfig.getStatus() == SharedConfigurationStatus.RUNNING;
    } else {
      return false;
    }
  }
  
  //////////////////////  Static Methods  /////////////////////
  
  /** the locator hosted by this JVM. As of 7.0 it is a singleton. */
  private static InternalLocator locator; // must synchronize on locatorLock
  private static final Object locatorLock = new Object();

  public static InternalLocator getLocator() {
    // synchronize in order to fix #46336 (race condition in createLocator)
    synchronized (locatorLock) {
      return locator;
    }
  }
  public static boolean hasLocator() {
    synchronized (locatorLock) {
      return locator != null;
    }
  }
  private static boolean removeLocator(InternalLocator l) {
    if (l == null) return false;
    synchronized (locatorLock) {
      if (hasLocator()) {
        if (l.equals(locator)) {
          locator = null;
          return true;
        }
      }
      return false;
    }
  }
  
  public LocatorMembershipListener getlocatorMembershipListener() {
    return this.locatorListener;
  }

  /**
   * Create a locator that listens on a given port. This locator will not have
   * peer or server location services available until they are started by
   * calling startServerLocation or startPeerLocation on the locator object.
   * 
   * @param port
   *                the tcp/ip port to listen on
   * @param logFile
   *                the file that log messages should be written to
   * @param stateFile
   *                the file that state should be read from / written to for recovery               
   * @param logger
   *                a log writer that should be used (logFile parameter is
   *                ignored)
   * @param securityLogger
   *                the logger to be used for security related log messages
   * @param distributedSystemProperties
   *                optional properties to configure the distributed system
   *                (e.g., mcast addr/port, other locators)
   * @param startDistributedSystem if true then this locator will also start its own ds
   */
  public static InternalLocator createLocator(
      int port,
      File logFile,
      File stateFile,
      InternalLogWriter logger,
      InternalLogWriter securityLogger,
      InetAddress bindAddress,
      String hostnameForClients,
      java.util.Properties distributedSystemProperties, boolean startDistributedSystem) throws IOException {
    synchronized (locatorLock) {
      if (hasLocator()) {
        throw new IllegalStateException("A locator can not be created because one already exists in this JVM.");
      }
      InternalLocator l = new InternalLocator(port, logFile, stateFile, logger, securityLogger, bindAddress, hostnameForClients, distributedSystemProperties, null, startDistributedSystem);
      locator = l;
      return l;
    }
  }
  
  private static void setLocator(InternalLocator l) {
    synchronized(locatorLock) {
      if (locator != null  &&  locator != l) {
        throw new IllegalStateException("A locator can not be created because one already exists in this JVM.");
      }
      locator = l;
    }
  }

  
  /**
   * Creates a distribution locator that runs in this VM on the given
   * port and bind address and creates a distributed system.
   * 
   * @param port
   *    the tcp/ip port to listen on
   * @param logFile
   *    the file that log messages should be written to
   * @param logger
   *    a log writer that should be used (logFile parameter is ignored)
   * @param securityLogger
   *    the logger to be used for security related log messages
   * @param dsProperties
   *    optional properties to configure the distributed system (e.g., mcast addr/port, other locators)
   * @param peerLocator
   *    enable peer location services
   * @param enableServerLocator
   *    enable server location services
   * @param hostnameForClients
   *    the name to give to clients for connecting to this locator
   * @param loadSharedConfigFromDir 
   *    load the shared configuration from the shared configuration directory
   * @throws IOException 
   * @since 7.0
   */
  public static InternalLocator startLocator(
      int port,
      File logFile,
      File stateFile,
      InternalLogWriter logger,
      InternalLogWriter securityLogger,
      InetAddress bindAddress,
      java.util.Properties dsProperties,
      boolean peerLocator, 
      boolean enableServerLocator,
      String hostnameForClients, 
      boolean loadSharedConfigFromDir
      )
      throws IOException {
    return startLocator(port, logFile, stateFile, logger, securityLogger, bindAddress, true, dsProperties, peerLocator, enableServerLocator, hostnameForClients, loadSharedConfigFromDir);
  }
  
  
  /**
   * Creates a distribution locator that runs in this VM on the given
   * port and bind address.
   * 
   * This is for internal use only as it does not create a distributed
   * system unless told to do so.
   * 
   * @param port
   *    the tcp/ip port to listen on
   * @param logFile
   *    the file that log messages should be written to
   * @param logger
   *    a log writer that should be used (logFile parameter is ignored)
   * @param securityLogger
   *    the logger to be used for security related log messages
   * @param startDistributedSystem
   *    if true, a distributed system is started
   * @param dsProperties
   *    optional properties to configure the distributed system (e.g., mcast addr/port, other locators)
   * @param peerLocator
   *    enable peer location services
   * @param enableServerLocator
   *    enable server location services
   * @param hostnameForClients
   *    the name to give to clients for connecting to this locator
   * @param loadSharedConfigFromDir TODO:CONFIG
   * @throws IOException 
   */
  public static InternalLocator startLocator(
    int port,
    File logFile,
    File stateFile,
    InternalLogWriter logger,
    InternalLogWriter securityLogger,
    InetAddress bindAddress,
    boolean startDistributedSystem,
    java.util.Properties dsProperties,
    boolean peerLocator, 
    boolean enableServerLocator, 
    String hostnameForClients, 
    boolean loadSharedConfigFromDir
    )
    throws IOException
  {

    if(!peerLocator && !enableServerLocator) {
      throw new IllegalArgumentException(LocalizedStrings.InternalLocator_EITHER_PEER_LOCATOR_OR_SERVER_LOCATOR_MUST_BE_ENABLED.toLocalizedString());
    }
    
    System.setProperty(FORCE_LOCATOR_DM_TYPE, "true");
    InternalLocator slocator = null;
    
    boolean startedLocator = false;
    try {
      
    slocator = createLocator(port, logFile, stateFile, logger, securityLogger, bindAddress, hostnameForClients, dsProperties, startDistributedSystem);
    
    
    if (enableServerLocator) {
      slocator.handler.willHaveServerLocator = true;
    }
    
    if(peerLocator)  {
      slocator.startPeerLocation(startDistributedSystem);
    }
    
    if (startDistributedSystem) {
      try {
        slocator.startDistributedSystem();
      } catch (RuntimeException e) {
        slocator.stop();
        throw e;
      }
      // fix bug #46324
      final InternalDistributedSystem ids = (InternalDistributedSystem)slocator.myDs;
      if (ids != null) {
        ids.getDistributionManager().addHostedLocators(ids.getDistributedMember(), getLocatorStrings(), slocator.isSharedConfigurationEnabled());
      }
    }
    // during the period when the product is using only paper licenses we always
    // start server location services in order to be able to log information
    // about the use of cache servers
//    if(enableServerLocator) {
//      slocator.startServerLocation(InternalDistributedSystem.getConnectedInstance());
//  }
    InternalDistributedSystem sys = InternalDistributedSystem.getConnectedInstance();
    if (sys != null) {
      try {
        slocator.startServerLocation(sys);
      } catch (RuntimeException e) {
        slocator.stop();
        throw e;
      }
    }
    
    slocator.endStartLocator(null);
    startedLocator = true;
    return slocator;

    } finally {
      System.getProperties().remove(FORCE_LOCATOR_DM_TYPE);
      if (!startedLocator) {
        // fix for bug 46314
        removeLocator(slocator);
      }
    }
  }
  
  /***
   * Determines if this VM is a locator which must ignore a shutdown.
   * @return true if this VM is a locator which should ignore a shutdown , false if it is a normal member.
   */
  public static boolean isDedicatedLocator() {
    InternalLocator internalLocator = getLocator();
    if (internalLocator == null)
      return false;
    
    InternalDistributedSystem ids = (InternalDistributedSystem)internalLocator.myDs;
    if (ids == null) {
      return false;
    }
    DM dm = ids.getDistributionManager();
    if (dm.isLoner()) {
      return false;
    }
    DistributionManager distMgr = (DistributionManager)ids.getDistributionManager();
    return distMgr.getDMType() == DistributionManager.LOCATOR_DM_TYPE;
  }
  
  public static LocatorStatusResponse statusLocator(int port, InetAddress bindAddress) throws IOException {
    //final int timeout = (60 * 2 * 1000); // 2 minutes
    final int timeout = Integer.MAX_VALUE; // 2 minutes

    try {
      return (LocatorStatusResponse) TcpClient.requestToServer(bindAddress, port,
        new LocatorStatusRequest(), timeout, true);
    }
    catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Stops the distribution locator that runs on the given port and
   * bind address.
   */
  public static void stopLocator(int port, InetAddress bindAddress) 
    throws ConnectException {
    TcpClient.stop(bindAddress, port);
  }

  /**
   * Returns information about the locator running on the given host
   * and port or <code>null</code> if the information cannot be
   * obtained.  Two <code>String</code>s are returned: the first
   * string is the working directory of the locator and the second
   * string is the product directory of the locator.
   */
  public static String[] getLocatorInfo(InetAddress host, int port) {
    return TcpClient.getInfo(host, port);
  }

  ///////////////////////  Constructors  //////////////////////
  
  /**
   * Creates a new <code>Locator</code> with the given port, log file, logger,
   * and bind address.
   * 
   * @param port
   *                the tcp/ip port to listen on
   * @param logF
   *                the file that log messages should be written to
   * @param stateF
   *    the file that state should be read from / written to for recovery
   * @param logWriter
   *                a log writer that should be used (logFile parameter is
   *                ignored)
   * @param securityLogWriter
   *                the log writer to be used for security related log messages
   * @param hostnameForClients
   *    the name to give to clients for connecting to this locator
   * @param distributedSystemProperties
   *                optional properties to configure the distributed system
   *                (e.g., mcast addr/port, other locators)
   * @param cfg the config if being called from a distributed system; otherwise null.
   * @param startDistributedSystem if true locator will start its own distributed system
   * @throws IOException
   */
  private InternalLocator(
    int port,
    File logF,
    File stateF,
    InternalLogWriter logWriter, // LOG: 3 non-null sources: GemFireDistributionLocator, InternalDistributedSystem, LocatorLauncher
    InternalLogWriter securityLogWriter, // LOG: 1 non-null source: GemFireDistributionLocator(same instance as logWriter), InternalDistributedSystem
    InetAddress bindAddress,
    String hostnameForClients,
    java.util.Properties distributedSystemProperties, DistributionConfigImpl cfg, boolean startDistributedSystem) {
    this.port = port;
    this.logFile = logF;
    this.bindAddress = bindAddress;
    this.hostnameForClients = hostnameForClients;
    if (stateF == null) {
      this.stateFile = new File("locator" + port + "view.dat");
    }
    else {
      this.stateFile = stateF;
    }
    File productUseFile = new File("locator"+port+"views.log");
    this.productUseLog = new ProductUseLog(productUseFile);
    this.config = cfg;
    
    env = new Properties();

    // set bind-address explicitly only if not wildcard and let any explicit
    // value in distributedSystemProperties take precedence (#46870)
    if (bindAddress != null && !bindAddress.isAnyLocalAddress()) {
      env.setProperty(DistributionConfig.BIND_ADDRESS_NAME,
          bindAddress.getHostAddress());
    }
    
    

    if (distributedSystemProperties != null) {
      env.putAll(distributedSystemProperties);
    }
    env.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, "");

    // create a DC so that all of the lookup rules, gemfire.properties, etc,
    // are considered and we have a config object we can trust
    if (this.config == null) {
      this.config = new DistributionConfigImpl(env);
      this.env.clear();
      this.env.putAll(this.config.getProps());
    }
    
    final boolean hasLogFileButConfigDoesNot = this.logFile != null && this.config.getLogFile().toString().equals(DistributionConfig.DEFAULT_LOG_FILE.toString());
    if (logWriter == null && hasLogFileButConfigDoesNot) {
      this.config.unsafeSetLogFile(this.logFile); // LOG: this is(was) a hack for when logFile and config don't match -- if config specifies a different log-file things will break!
    }

    // LOG: create LogWriterAppenders (these are closed at shutdown)
    final boolean hasLogFile = this.config.getLogFile() != null && !this.config.getLogFile().equals(new File(""));
    final boolean hasSecurityLogFile = this.config.getSecurityLogFile() != null && !this.config.getSecurityLogFile().equals(new File(""));
    LogService.configureLoggers(hasLogFile, hasSecurityLogFile);
    if (hasLogFile || hasSecurityLogFile) {
      
      if (hasLogFile) {
        // if log-file then create logWriterAppender
        LogWriterAppenders.getOrCreateAppender(LogWriterAppenders.Identifier.MAIN, true, false, this.config, !startDistributedSystem);
      }
      
      if (hasSecurityLogFile) {
        // if security-log-file then create securityLogWriterAppender
        LogWriterAppenders.getOrCreateAppender(LogWriterAppenders.Identifier.SECURITY, true, false, this.config, false);
        
      } else {
        // do not create a LogWriterAppender for security -- let it go through to logWriterAppender
      }
    }    

    // LOG: create LogWriters for GemFireTracer (or use whatever was passed in)
    if (logWriter == null) {
      logWriter = LogWriterFactory.createLogWriterLogger(false, false, this.config, !startDistributedSystem);
      if (logger.isDebugEnabled()) {
        logger.debug("LogWriter for locator is created.");
      }
    }
    
    if (securityLogWriter == null) {
      securityLogWriter = LogWriterFactory.createLogWriterLogger(false, true, this.config, false);
      ((LogWriterLogger) logWriter).setLogWriterLevel(this.config.getSecurityLogLevel());
      securityLogWriter.fine("SecurityLogWriter for locator is created.");
    }
    
    this.locatorListener = WANServiceProvider.createLocatorMembershipListener();
    if(locatorListener != null) {
      this.locatorListener.setPort(this.port);
      this.locatorListener.setConfig(this.getConfig());
    }
    this.handler = new PrimaryHandler(this.port, this, locatorListener);
  
    ThreadGroup group = LoggingThreadGroup.createThreadGroup("Distribution locators", logger);
    stats = new LocatorStats();
    server = new TcpServer(this.port, this.bindAddress, null, this.config,
        this.handler, new DelayedPoolStatHelper(), group, this.toString());
  }

  private void startTcpServer() throws IOException {
    logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_STARTING_0, this));
    server.start();
  }
  
  public SharedConfiguration getSharedConfiguration() {
    return this.sharedConfig;
  }
  
  public DistributionConfigImpl getConfig() {
    return config;
  }
  
  /**
   * Start peer location in this locator. If you plan on starting a distributed
   * system later, this method should be called first so that the distributed
   * system can use this locator.
   * 
   * @param withDS true if a distributed system has been or will be started
   * @throws IOException
   * @since 5.7
   */
  public void startPeerLocation(boolean withDS) throws IOException {
    if(isPeerLocator()) {
      throw new IllegalStateException(LocalizedStrings.InternalLocator_PEER_LOCATION_IS_ALREADY_RUNNING_FOR_0.toLocalizedString(this));
    }
    logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_STARTING_PEER_LOCATION_FOR_0, this));
    
    String locatorsProp = this.config.getLocators();
    
    // check for settings that would require only locators to hold the
    // coordinator - e.g., security and network-partition detection
    boolean locatorsAreCoordinators = false;
    boolean networkPartitionDetectionEnabled = this.config.getEnableNetworkPartitionDetection();
    if (networkPartitionDetectionEnabled) {
      locatorsAreCoordinators = true;
    }
    else {
      // check if security is enabled
      String prop = this.config.getSecurityPeerAuthInit();
      locatorsAreCoordinators =  (prop != null && prop.length() > 0);
      if (!locatorsAreCoordinators) {
        locatorsAreCoordinators = Boolean.getBoolean(LOCATORS_PREFERRED_AS_COORDINATORS);
      }
    }
    if (locatorsAreCoordinators) {
      // LOG: changed from config to info
      logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_FORCING_GROUP_COORDINATION_INTO_LOCATORS));
    }
    
    this.locatorImpl = MemberFactory.newLocatorHandler(this.bindAddress, this.stateFile,
        locatorsProp, locatorsAreCoordinators, networkPartitionDetectionEnabled, stats);
    this.handler.addHandler(PeerLocatorRequest.class, this.locatorImpl);
    peerLocator = true;
    if(!server.isAlive()) {
      startTcpServer();
    }
  }

  /**
   * @return the TcpHandler for peer to peer discovery
   */
  public NetLocator getLocatorHandler() {
    return this.locatorImpl;
  }
  
  class SharedConfigurationRunnable implements Runnable{
    
    private final InternalLocator locator = InternalLocator.this;
    
    @Override
    public void run() {
      try {
        if (locator.sharedConfig == null) {
          // locator.sharedConfig will already be created in case of auto-reconnect
          locator.sharedConfig = new SharedConfiguration((GemFireCacheImpl) locator.myCache);
        }
        locator.sharedConfig.initSharedConfiguration(locator.loadFromSharedConfigDir());
        locator.installSharedConfigDistribution();
        logger.info("Cluster configuration service start up completed successfully and is now running ....");
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Cluster configuration start up was cancelled", e);
        }
      } catch (LockServiceDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Cluster configuration start up was cancelled", e);
        }
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }
  }
  /**
   * Start a distributed system whose life cycle is managed by this locator. When
   * the locator is stopped, this distributed system will be disconnected. If a
   * distributed system already exists, this method will have no affect.
   * 
   * @throws UnknownHostException
   * @since 5.7
   */
  public void startDistributedSystem() throws UnknownHostException {
    InternalDistributedSystem existing = InternalDistributedSystem.getConnectedInstance();
    
    //TODO : For now set this property to create a PDX registry that does nothing
    // Investigate keeping the typeRegistry in the locators
    if (existing != null) {
      // LOG: changed from config to info
      logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_USING_EXISTING_DISTRIBUTED_SYSTEM__0, existing));
      startCache(existing);
    }
    else {
      String thisLocator;
      {
        StringBuilder sb = new StringBuilder(100);
        if (bindAddress != null) {
          sb.append(bindAddress.getHostAddress());
        }
        else {
          sb.append(SocketCreator.getLocalHost().getHostAddress());
        }
        sb.append('[').append(port).append(']');
        thisLocator = sb.toString();
      }
      

      if(peerLocator) {
          // append this locator to the locators list from the config properties
          //this.logger.config("ensuring that this locator is in the locators list");
          boolean setLocatorsProp = false;
          String locatorsProp = this.config.getLocators();
          if (locatorsProp != null && locatorsProp.trim().length() > 0) {
            if (!locatorsProp.contains(thisLocator)) {
              locatorsProp = locatorsProp + "," + thisLocator;
              setLocatorsProp = true;
            }
          }
          else {
            locatorsProp = thisLocator;
            setLocatorsProp = true;
          }
          if (setLocatorsProp) {
            Properties updateEnv = new Properties();
            updateEnv.setProperty(DistributionConfig.LOCATORS_NAME, locatorsProp);
            this.config.setApiProps(updateEnv);
            // fix for bug 41248
            String propName = DistributionConfig.GEMFIRE_PREFIX +
                                 DistributionConfig.LOCATORS_NAME;
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

        logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_STARTING_DISTRIBUTED_SYSTEM));
        // LOG:CONFIG: changed from config to info
        logger.info(LogMarker.CONFIG, LocalizedMessage.create(LocalizedStrings.InternalDistributedSystem_STARTUP_CONFIGURATIONN_0, this.config.toLoggerString()));

        myDs = (InternalDistributedSystem)DistributedSystem.connect(connectEnv);
        
        if (peerLocator) {
          this.locatorImpl.setMembershipManager(myDs.getDM().getMembershipManager());
        }
        
        myDs.addDisconnectListener(new DisconnectListener() {
          @Override
          public void onDisconnect(InternalDistributedSystem sys) {
            stop(false, false, false);
          }
        });
        
        startCache(myDs);
        
        logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_LOCATOR_STARTED_ON__0, thisLocator));
          
        ((InternalDistributedSystem)myDs).setDependentLocator(this);
    }
  }
  
  
  private void startCache(DistributedSystem ds) {
  
    GemFireCacheImpl gfc = GemFireCacheImpl.getInstance();
    if (gfc == null) {
      logger.info("Creating cache for locator.");
      this.myCache = new CacheFactory(ds.getProperties()).create();
      gfc = (GemFireCacheImpl)this.myCache;
    } else {
      logger.info("Using existing cache for locator.");
      ((InternalDistributedSystem) ds).handleResourceEvent(
          ResourceEvent.LOCATOR_START, this);
    }
    startJmxManagerLocationService(gfc);
    
    startSharedConfigurationService(gfc);
  }
  
  /**
   * End the initialization of the locator. This method should
   * be called once the location services and distributed
   * system are started.
   * 
   * @param distributedSystem
   *                The distributed system to use for the statistics.
   *                
   * @since 5.7
   * 
   * @throws UnknownHostException
   */
  public void endStartLocator(InternalDistributedSystem distributedSystem) throws UnknownHostException {
    env = null;
    if (distributedSystem == null) {
      distributedSystem = InternalDistributedSystem.getConnectedInstance();
    }
    if(distributedSystem != null) {
      onConnect(distributedSystem);
    } else {
      InternalDistributedSystem.addConnectListener(this);
    }
    
    WanLocatorDiscoverer s = WANServiceProvider.createLocatorDiscoverer();
    if(s != null) {
      s.discover(this.port, config, locatorListener);
    }
  }
  
  /**
   * Start server location services in this locator. Server location
   * can only be started once there is a running distributed system.
   * 
   * @param distributedSystem
   *                The distributed system which the server location services
   *                should use. If null, the method will try to find an already
   *                connected distributed system.
   * @throws ExecutionException 
   * @since 5.7
   */
  public void startServerLocation(InternalDistributedSystem distributedSystem)
    throws IOException
  {
    if(isServerLocator()) {
      throw new IllegalStateException(LocalizedStrings.InternalLocator_SERVER_LOCATION_IS_ALREADY_RUNNING_FOR_0.toLocalizedString(this));
    }
    logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_STARTING_SERVER_LOCATION_FOR_0, this));
    
    if (distributedSystem == null) {
      distributedSystem = InternalDistributedSystem.getConnectedInstance();
      if (distributedSystem == null) {
        throw new IllegalStateException(LocalizedStrings.InternalLocator_SINCE_SERVER_LOCATION_IS_ENABLED_THE_DISTRIBUTED_SYSTEM_MUST_BE_CONNECTED.toLocalizedString());
      }
    }

    this.productUseLog.monitorUse(distributedSystem);
    
    ServerLocator sl = new ServerLocator(this.port, 
                                         this.bindAddress,
                                         this.hostnameForClients,
                                         this.logFile,
                                         this.productUseLog,
                                         getConfig().getName(),
                                         distributedSystem,
                                         stats);
    this.handler.addHandler(LocatorListRequest.class, sl);
    this.handler.addHandler(ClientConnectionRequest.class, sl);
    this.handler.addHandler(QueueConnectionRequest.class, sl);
    this.handler.addHandler(ClientReplacementRequest.class, sl);
    this.handler.addHandler(GetAllServersRequest.class, sl);
    this.handler.addHandler(LocatorStatusRequest.class, sl);
    this.serverLocator = sl;
    if(!server.isAlive()) {
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
   * Was this locator stopped during forced-disconnect processing but should
   * reconnect?
   */
  public boolean getStoppedForReconnect() {
    return this.stoppedForReconnect;
  }
  
  /**
   * Stop this locator
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
          try { Thread.sleep(500); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        if (isDebugEnabled) {
          if (this.server.isAlive()) {
            logger.debug("60 seconds have elapsed waiting for the locator server to shut down - terminating wait and returning");
          } else {
            logger.debug("the locator server has shut down");
          }
        }
      }
      return;
    }

    if (this.server.isAlive()) {
      logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_STOPPING__0, this));
      try {
        stopLocator(this.port, this.bindAddress);
      } catch ( ConnectException ignore ) {
        // must not be running
      }
      boolean interrupted = Thread.interrupted();
      try {
        this.server.join(TcpServer.SHUTDOWN_WAIT_TIME * 1000 + 10000);
  
      } catch (InterruptedException ex) {
        interrupted = true;
        logger.warn(LocalizedMessage.create(LocalizedStrings.InternalLocator_INTERRUPTED_WHILE_STOPPING__0, this), ex);
        
        // Continue running -- doing our best to stop everything...
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
  
      if (this.server.isAlive()) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.InternalLocator_COULD_NOT_STOP__0__IN_60_SECONDS, this));
      }
    }

    removeLocator(this);

    handleShutdown();

    logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_0__IS_STOPPED, this));
    
    if (stoppedForReconnect) {
      if (this.myDs != null) {
        launchRestartThread();
      }
    }
  }
  
  
  /** answers whether this locator is currently stopped */
  public boolean isStopped() {
    return this.server == null  ||  !this.server.isAlive();
  }
  
  private void handleShutdown() {
    if (!this.shutdownHandled.compareAndSet(false, true)) {
      return; // already shutdown
    }
    productUseLog.close();
    if (myDs != null) {
      ((InternalDistributedSystem)myDs).setDependentLocator(null);
    }
    if (this.myCache != null && !this.stoppedForReconnect && !this.forcedDisconnect) {
      logger.info("Closing locator's cache");
      try {
        this.myCache.close();
      } catch (RuntimeException ex) {
        logger.info("Could not close locator's cache because: {}", ex);
      }
    }
    
    if(stats != null) {
      stats.close();
    }
    
    if(this.locatorListener != null){
      this.locatorListener.clearLocatorInfo();
    }
    
    this.isSharedConfigurationStarted = false;
    if (myDs != null && !this.forcedDisconnect) {
      if (myDs.isConnected()) {
        logger.info(LocalizedMessage.create(LocalizedStrings.InternalLocator_DISCONNECTING_DISTRIBUTED_SYSTEM_FOR_0, this));
        myDs.disconnect();
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
        Thread rs = this.restartThread;
        if (rs != null) {
          logger.info("waiting for services to restart...");
          rs.join();
          this.restartThread = null;
          logger.info("done waiting for services to restart");
        }
      }
    } while (restarted);
  }
  
  /** launch a thread that will restart location services */
  private void launchRestartThread() {
    // create a thread group having a last-chance exception-handler
    ThreadGroup group = LoggingThreadGroup.createThreadGroup("Locator restart thread group");
    this.restartThread = new Thread(group, "Location services restart thread") {
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
          if (! restarted) {
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
   * reconnects the locator to a restarting DistributedSystem.  If quorum checks
   * are enabled this will start peer location services before a distributed
   * system is available if the quorum check succeeds.  It will then wait
   * for the system to finish reconnecting before returning.  If quorum checks
   * are not being done this merely waits for the distributed system to reconnect
   * and then starts location services.
   * @return true if able to reconnect the locator to the new distributed system
   */
  public boolean attemptReconnect() throws InterruptedException, IOException {
    boolean restarted = false;
    if (this.stoppedForReconnect) {
      logger.info("attempting to restart locator");
      boolean tcpServerStarted = false;
      InternalDistributedSystem ds = this.myDs;
      long waitTime = ds.getConfig().getMaxWaitTimeForReconnect()/2;
      QuorumChecker checker = null;
      while (ds.getReconnectedSystem() == null &&
          !ds.isReconnectCancelled()) {
        if (checker == null) {
          checker = this.myDs.getQuorumChecker();
          if (checker != null) {
            logger.info("The distributed system returned this quorum checker: {}", checker);
          }
        }
        if (checker != null && !tcpServerStarted) {
          boolean start = checker.checkForQuorum(3*this.myDs.getConfig().getMemberTimeout());
          if (start) {
            // start up peer location.  server location is started after the DS finishes
            // reconnecting
            logger.info("starting peer location");
            if(this.locatorListener != null){
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
      InternalDistributedSystem newSystem = (InternalDistributedSystem)ds.getReconnectedSystem();
//      LogWriter log = new ManagerLogWriter(LogWriterImpl.FINE_LEVEL, System.out);
      if (newSystem != null) {
//        log.fine("reconnecting locator: starting location services");
        if (!tcpServerStarted) {
          if(this.locatorListener != null){
            this.locatorListener.clearLocatorInfo();
          }
          this.stoppedForReconnect = false;
        }
        restartWithDS(newSystem, GemFireCacheImpl.getInstance());
        setLocator(this);
        restarted = true;
      }
    }
    logger.info("restart thread exiting.  Service was "+(restarted? "" : "not ") + "restarted");
    return restarted;
  }
  
  
  private void restartWithoutDS() throws IOException {
    synchronized (locatorLock) {
      if (locator != this && hasLocator()) {
        throw new IllegalStateException("A locator can not be created because one already exists in this JVM.");
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
  
  private void restartWithDS(InternalDistributedSystem newSystem, GemFireCacheImpl newCache) throws IOException {
    synchronized (locatorLock) {
      if (locator != this && hasLocator()) {
        throw new IllegalStateException("A locator can not be created because one already exists in this JVM.");
      }
      this.myDs = newSystem;
      this.myCache = newCache;
      ((InternalDistributedSystem)myDs).setDependentLocator(this);
      logger.info("Locator restart: initializing TcpServer");
      if (isSharedConfigurationEnabled()) {
        this.sharedConfig = new SharedConfiguration(newCache);
      }
      this.server.restarting(newSystem, newCache, this.sharedConfig);
      if (this.productUseLog.isClosed()) {
        this.productUseLog.reopen();
      }
      this.productUseLog.monitorUse(newSystem);
      this.isSharedConfigurationStarted = true;
      if (isSharedConfigurationEnabled()) {
        ExecutorService es = newCache.getDistributionManager().getThreadPool();
        es.submit(new SharedConfigurationRunnable());
      }
      if (!this.server.isAlive()) {
        logger.info("Locator restart: starting TcpServer");
        startTcpServer();
      }
      logger.info("Locator restart: initializing JMX manager");
      startJmxManagerLocationService(newCache);
      endStartLocator((InternalDistributedSystem)myDs);
      logger.info("Locator restart completed");
    }
  }
  
  
  // implementation of abstract method in Locator
  @Override
  public DistributedSystem getDistributedSystem() {
    return myDs;
  }
  
  @Override
  public boolean isPeerLocator() {
    return peerLocator;
  }
  
  @Override
  public boolean isServerLocator() {
    return this.serverLocator != null;
  }

  /**
   * Returns null if no server locator;
   * otherwise returns the advisee that represents the server locator.
   */
  public ServerLocator getServerLocatorAdvisee() {
    return this.serverLocator;
  }
  
  
  /******
   * 
   * @author bansods
   *
   */
  class FetchSharedConfigStatus implements Callable<SharedConfigurationStatusResponse> {
    static final int SLEEPTIME = 1000;
    static final byte MAX_RETRIES = 5;
    public SharedConfigurationStatusResponse call() throws Exception {
      SharedConfigurationStatusResponse response;
      
      final InternalLocator locator = InternalLocator.this;
      for (int i=0; i<MAX_RETRIES; i++) {
        if (locator.sharedConfig != null) {
          SharedConfigurationStatus status = locator.sharedConfig.getStatus();
          if (status != SharedConfigurationStatus.STARTED || status != SharedConfigurationStatus.NOT_STARTED) {
            break;
          }
        }
        Thread.sleep(SLEEPTIME);
      }
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
    ExecutorService es = ((GemFireCacheImpl)myCache).getDistributionManager().getWaitingThreadPool();
    Future<SharedConfigurationStatusResponse> statusFuture = es.submit(new FetchSharedConfigStatus());
    SharedConfigurationStatusResponse response = null;

    try {
      response = statusFuture.get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.info("Exception occured while fetching the status {}", CliUtil.stackTraceAsString(e));
      response =  new SharedConfigurationStatusResponse();
      response.setStatus(SharedConfigurationStatus.UNDETERMINED);
    } 
    return response;
  }
  
  
  private static class PrimaryHandler implements TcpHandler {
    private volatile HashMap<Class, TcpHandler> handlerMapping = new HashMap<Class, TcpHandler>();
    private volatile HashSet<TcpHandler> allHandlers = new HashSet<TcpHandler>();
    private TcpServer tcpServer;
    private final LocatorMembershipListener locatorListener;
    //private final List<LocatorJoinMessage> locatorJoinMessages;
    private Object locatorJoinObject = new Object();
    InternalLocator interalLocator;
    boolean willHaveServerLocator;  // flag to avoid warning about missing handlers during startup
    
    public PrimaryHandler(int port, InternalLocator locator,
        LocatorMembershipListener listener) {
      this.locatorListener = listener;
      interalLocator = locator;
      //this.locatorJoinMessages = new ArrayList<LocatorJoinMessage>();
    }

    // this method is synchronized to make sure that no new handlers are added while
    //initialization is taking place.
    public synchronized void init(TcpServer tcpServer) {
      this.tcpServer = tcpServer;
      for(Iterator itr = allHandlers.iterator(); itr.hasNext();) {
        TcpHandler handler = (TcpHandler) itr.next();
        handler.init(tcpServer);
      }
    }

    public void restarting(DistributedSystem ds, GemFireCache cache, SharedConfiguration sharedConfig) {
      if (ds != null) {
        for (TcpHandler handler: this.allHandlers) {
          handler.restarting(ds, cache, sharedConfig);
        }
      }
    }

    public Object processRequest(Object request) throws IOException {
      TcpHandler handler = null;
      if (request instanceof PeerLocatorRequest) {
        handler = (TcpHandler)handlerMapping.get(PeerLocatorRequest.class);
      }
      else {
        handler = (TcpHandler)handlerMapping.get(request.getClass());
      }
      
      if (handler != null) {
        Object result;
        result = handler.processRequest(request);
        return result;
      }
      else {  
        Object response;
        if(locatorListener != null){
          response = locatorListener.handleRequest(request);
        }
        else {
          if (!(willHaveServerLocator && (request instanceof ServerLocationRequest))) {
            logger
                .warn(LocalizedMessage
                    .create(
                        LocalizedStrings.InternalLocator_EXPECTED_ONE_OF_THESE_0_BUT_RECEIVED_1,
                        new Object[] { handlerMapping.keySet(), request }));
          }
          return null;
        }
        return response;
      }
    }
    private JmxManagerLocatorResponse findJmxManager(JmxManagerLocatorRequest request) {
      JmxManagerLocatorResponse result = null;
      // NYI
      return result;
    }
    
    public void shutDown() {
      try {
      for(Iterator itr = allHandlers.iterator(); itr.hasNext(); ) {
        TcpHandler handler = (TcpHandler) itr.next();
        handler.shutDown();
      }
      } finally {
        this.interalLocator.handleShutdown();
      }
    }
    
    public synchronized boolean isHandled(Class clazz) {
      return this.handlerMapping.containsKey(clazz);
    }
    
    public synchronized void addHandler(Class clazz, TcpHandler handler) {
      HashMap tmpHandlerMapping = new HashMap(handlerMapping);
      HashSet tmpAllHandlers = new HashSet(allHandlers);
      tmpHandlerMapping.put(clazz, handler);
      if(tmpAllHandlers.add(handler) && tcpServer != null ) {
        handler.init(tcpServer);
      }
      handlerMapping = tmpHandlerMapping;
      allHandlers = tmpAllHandlers;
    }
    
    public void endRequest(Object request,long startTime) {
      TcpHandler handler = (TcpHandler) handlerMapping.get(request.getClass());
      if(handler != null) {
        handler.endRequest(request, startTime);
      }
    }
    
    public void endResponse(Object request,long startTime) {
      TcpHandler handler = (TcpHandler) handlerMapping.get(request.getClass());
      if(handler != null) {
        handler.endResponse(request, startTime);
      }
    }
  }
  
  public void onConnect(InternalDistributedSystem sys) {
    try {
      stats.hookupStats(sys,  SocketCreator.getLocalHost().getCanonicalHostName() + "-" + server.getBindAddress().toString());
    } catch(UnknownHostException uhe) {
      uhe.printStackTrace();
    }
  }

  /**
   * Returns collection of locator strings representing every locator instance
   * hosted by this member.
   * 
   * @see #getLocators()
   */
  public static Collection<String> getLocatorStrings() {
    Collection<String> locatorStrings = null;
    try {
      Collection<DistributionLocatorId> locatorIds = 
          DistributionLocatorId.asDistributionLocatorIds(getLocators());
      locatorStrings = DistributionLocatorId.asStrings(locatorIds);
    } catch (UnknownHostException e) {
      locatorStrings = null;
    }
    if (locatorStrings == null || locatorStrings.isEmpty()) {
      return null;
    } else {
      return locatorStrings;
    }
  }
  
  /**
   * A helper object so that the TcpServer can record
   * its stats to the proper place. Stats are only recorded
   * if a distributed system is started.
   * 
   */
  protected class DelayedPoolStatHelper implements PoolStatHelper {
    
    public void startJob() {
      stats.incRequestInProgress(1);
      
    }
    public void endJob() {
      stats.incRequestInProgress(-1);
    }
  }
  
  public void startSharedConfigurationService(GemFireCacheImpl gfc){
    
    
    if (this.config.getEnableClusterConfiguration() && !this.isSharedConfigurationStarted) {
      if (!isDedicatedLocator()) {
        logger.info("Cluster configuration service is only supported in dedicated locators");
        return;
      } 
      
      this.isSharedConfigurationStarted = true;
      installSharedConfigStatus();
      ExecutorService es = gfc.getDistributionManager().getThreadPool();
      es.submit(new SharedConfigurationRunnable());
    } else {
      logger.info("Cluster configuration service is disabled");
    }
  }
  
  public void startJmxManagerLocationService(GemFireCacheImpl gfc) {
    if (gfc.getJmxManagerAdvisor() != null) {
      if (!this.handler.isHandled(JmxManagerLocatorRequest.class)) {
        this.handler.addHandler(JmxManagerLocatorRequest.class, new JmxManagerLocator(gfc));
      }
    }
  }
  
  /***
   * Creates and installs the handler {@link ConfigurationRequestHandler}
   */
  public void installSharedConfigDistribution() {
    if (!this.handler.isHandled(ConfigurationRequest.class)) {
      this.handler.addHandler(ConfigurationRequest.class, new ConfigurationRequestHandler(this.sharedConfig));
      logger.info("ConfigRequestHandler installed");
    }
  }
  
  public void installSharedConfigStatus() {
    if (!this.handler.isHandled(SharedConfigurationStatusRequest.class)) {
      this.handler.addHandler(SharedConfigurationStatusRequest.class, new SharedConfigurationStatusRequestHandler());
      logger.info("SharedConfigStatusRequestHandler installed");
    }
  }

}
