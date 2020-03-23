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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.LogWriter;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.SystemConnectException;
import org.apache.geode.SystemFailure;
import org.apache.geode.alerting.internal.AlertingSession;
import org.apache.geode.alerting.internal.ClusterAlertMessaging;
import org.apache.geode.alerting.internal.InternalAlertingService;
import org.apache.geode.alerting.internal.InternalAlertingServiceFactory;
import org.apache.geode.alerting.internal.NullAlertMessaging;
import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.internal.locks.GrantorRequestProcessor;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MembershipInformation;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.QuorumChecker;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheBuilder;
import org.apache.geode.internal.cache.execute.InternalFunctionService;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.internal.cache.tier.sockets.EncryptorImpl;
import org.apache.geode.internal.cache.xmlcache.CacheServerCreation;
import org.apache.geode.internal.config.ClusterConfigurationNotAvailableException;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogWriterFactory;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.offheap.OffHeapStorage;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.internal.statistics.DummyStatisticsRegistry;
import org.apache.geode.internal.statistics.GemFireStatSampler;
import org.apache.geode.internal.statistics.StatisticsConfig;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.internal.statistics.StatisticsManagerFactory;
import org.apache.geode.internal.statistics.StatisticsRegistry;
import org.apache.geode.internal.statistics.platform.LinuxProcFsStatistics;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.logging.internal.LoggingSession;
import org.apache.geode.logging.internal.NullLoggingSession;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.executors.LoggingUncaughtExceptionHandler;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigListener;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.logging.internal.spi.LogFile;
import org.apache.geode.management.ManagementException;
import org.apache.geode.metrics.internal.MeterRegistrySupplier;
import org.apache.geode.metrics.internal.MetricsService;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;

/**
 * The concrete implementation of {@link DistributedSystem} that provides internal-only
 * functionality.
 *
 * @since GemFire 3.0
 */
public class InternalDistributedSystem extends DistributedSystem
    implements LogConfigSupplier {

  private static final Logger logger = LogService.getLogger();

  private static final String DISABLE_MANAGEMENT_PROPERTY =
      GEMFIRE_PREFIX + "disableManagement";

  public static final String ALLOW_MULTIPLE_SYSTEMS_PROPERTY =
      GEMFIRE_PREFIX + "ALLOW_MULTIPLE_SYSTEMS";

  public static final String ALLOW_MEMORY_OVERCOMMIT =
      GEMFIRE_PREFIX + "Cache.ALLOW_MEMORY_OVERCOMMIT";
  public static final String AVOID_MEMORY_LOCK_WHEN_OVERCOMMIT =
      GEMFIRE_PREFIX + "Cache.AVOID_MEMORY_LOCK_WHEN_OVERCOMMIT";

  /**
   * If auto-reconnect is going on this will hold a reference to it
   */
  @MakeNotStatic
  public static volatile DistributedSystem systemAttemptingReconnect;

  @Immutable
  public static final CreationStackGenerator DEFAULT_CREATION_STACK_GENERATOR = config -> null;

  // the following is overridden from DistributedTestCase to fix #51058
  @MutableForTesting
  public static final AtomicReference<CreationStackGenerator> TEST_CREATION_STACK_GENERATOR =
      new AtomicReference<>(DEFAULT_CREATION_STACK_GENERATOR);

  /**
   * A value of Boolean.TRUE will identify a thread being used to execute
   * disconnectListeners. {@link #addDisconnectListener} will not throw ShutdownException if the
   * value is Boolean.TRUE.
   */
  private final ThreadLocal<Boolean> isDisconnectThread =
      ThreadLocal.withInitial(() -> Boolean.FALSE);

  private final StatisticsManager statisticsManager;
  private MetricsService metricsService;
  private final FunctionStatsManager functionStatsManager;
  /**
   * True if the user is allowed lock when memory resources appear to be overcommitted.
   */
  private final boolean allowMemoryLockWhenOvercommitted =
      Boolean.getBoolean(ALLOW_MEMORY_OVERCOMMIT);
  /**
   * True if memory lock is avoided when memory resources appear to be overcommitted.
   */
  private final boolean avoidMemoryLockWhenOvercommitted =
      Boolean.getBoolean(AVOID_MEMORY_LOCK_WHEN_OVERCOMMIT);

  /**
   * The distribution manager that is used to communicate with the distributed system.
   */
  protected DistributionManager dm;

  private final GrantorRequestProcessor.GrantorRequestContext grc;

  /**
   * services provided by other modules
   */
  private Map<Class, DistributedSystemService> services = new HashMap<>();

  private final AtomicReference<ClusterAlertMessaging> clusterAlertMessaging =
      new AtomicReference<>();

  // captured in initialize() when starting so that we can hand it to new instance when restarting
  private MembershipLocator<InternalDistributedMember> membershipLocator;

  /**
   * If the experimental multiple-system feature is enabled, always create a new system.
   *
   * <p>
   * Otherwise, create a new InternalDistributedSystem with the given properties, or connect to an
   * existing one with the same properties.
   */
  public static InternalDistributedSystem connectInternal(
      Properties config,
      SecurityConfig securityConfig,
      MetricsService.Builder metricsSessionBuilder) {
    return connectInternal(config, securityConfig, metricsSessionBuilder, null);
  }

  /**
   * If the experimental multiple-system feature is enabled, always create a new system.
   *
   * <p>
   * Otherwise, create a new InternalDistributedSystem with the given properties, or connect to an
   * existing one with the same properties.
   */
  public static InternalDistributedSystem connectInternal(
      Properties config,
      SecurityConfig securityConfig,
      MetricsService.Builder metricsSessionBuilder,
      final MembershipLocator<InternalDistributedMember> locator) {
    if (config == null) {
      config = new Properties();
    }

    if (Boolean.getBoolean(ALLOW_MULTIPLE_SYSTEMS_PROPERTY)) {
      return new Builder(config, metricsSessionBuilder)
          .setSecurityConfig(securityConfig)
          .setLocator(locator)
          .build();
    }

    synchronized (existingSystemsLock) {
      if (ClusterDistributionManager.isDedicatedAdminVM()) {
        // For a dedicated admin VM, check to see if there is already
        // a connect that will suit our purposes.
        InternalDistributedSystem existingSystem =
            (InternalDistributedSystem) getConnection(config);
        if (existingSystem != null) {
          return existingSystem;
        }

      } else {
        boolean existingSystemDisconnecting = true;
        boolean isReconnecting = false;
        while (!existingSystems.isEmpty() && existingSystemDisconnecting && !isReconnecting) {
          Assert.assertTrue(existingSystems.size() == 1);

          InternalDistributedSystem existingSystem = existingSystems.get(0);
          existingSystemDisconnecting = existingSystem.isDisconnecting();
          // a reconnecting DS will block on GemFireCache.class and a ReconnectThread
          // holds that lock and invokes this method, so we break out of the loop
          // if we detect this condition
          isReconnecting = existingSystem.isReconnectingDS();
          if (existingSystemDisconnecting) {
            boolean interrupted = Thread.interrupted();
            try {
              // no notify for existingSystemsLock, just to release the sync
              existingSystemsLock.wait(50);
            } catch (InterruptedException ex) {
              interrupted = true;
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          } else if (existingSystem.isConnected()) {
            existingSystem.validateSameProperties(config, existingSystem.isConnected());
            return existingSystem;
          } else {
            throw new AssertionError(
                "system should not have both disconnecting==false and isConnected==false");
          }
        }
      }

      // Make a new connection to the distributed system
      InternalDistributedSystem newSystem = new Builder(config, metricsSessionBuilder)
          .setSecurityConfig(securityConfig)
          .setLocator(locator)
          .build();
      addSystem(newSystem);
      return newSystem;
    }
  }

  public GrantorRequestProcessor.GrantorRequestContext getGrantorRequestContext() {
    return grc;
  }

  /**
   * Numeric id that identifies this node in a DistributedSystem
   */
  private long id;

  /**
   * The log writer used to log information messages
   */
  @Deprecated
  protected InternalLogWriter logWriter = null;

  /**
   * The log writer used to log security related messages
   */
  @Deprecated
  protected InternalLogWriter securityLogWriter = null;

  /**
   * Distributed System clock
   */
  private DSClock clock;

  /**
   * Time this system was created
   */
  private final long startTime;

  /**
   * Guards access to {@link #isConnected}
   */
  private final Object isConnectedMutex = new Object();

  /**
   * Is this <code>DistributedSystem</code> connected to a distributed system?
   * <p>
   * Concurrency: volatile for reads and protected by synchronization of {@link #isConnectedMutex}
   * for writes
   */
  protected volatile boolean isConnected;

  /**
   * Set to true if this distributed system is a singleton; it will always be the only member of the
   * system.
   */
  private boolean isLoner = false;

  /**
   * The sampler for this DistributedSystem.
   */
  private GemFireStatSampler sampler = null;

  /**
   * A set of listeners that are invoked when this connection to the distributed system is
   * disconnected
   */
  private final Set<DisconnectListener> disconnectListeners = new LinkedHashSet<>();

  /**
   * Set of listeners that are invoked whenever a connection is created to the distributed system
   */
  @MakeNotStatic
  private static final Set<ConnectListener> connectListeners = new LinkedHashSet<>();

  /**
   * auto-reconnect listeners
   */
  @MakeNotStatic
  private static final List<ReconnectListener> reconnectListeners = new ArrayList<>();

  /**
   * whether this DS is one created to reconnect to the distributed system after a
   * forced-disconnect. This state is cleared once reconnect is successful.
   */
  private boolean isReconnectingDS;

  /**
   * During a reconnect attempt this is used to perform quorum checks before allowing a location
   * service to be started up in this JVM. If quorum checks fail then we delay starting location
   * services until a live locator can be contacted.
   */
  private QuorumChecker quorumChecker;


  /**
   * Due to Bug 38407, be careful about moving this to another class.
   */
  public static final String SHUTDOWN_HOOK_NAME = "Distributed system shutdown hook";
  /**
   * A property to prevent shutdown hooks from being registered with the VM. This is regarding bug
   * 38407
   */
  public static final String DISABLE_SHUTDOWN_HOOK_PROPERTY =
      GEMFIRE_PREFIX + "disableShutdownHook";

  /**
   * A property to append to existing log-file instead of truncating it.
   */
  private static final String APPEND_TO_LOG_FILE = GEMFIRE_PREFIX + "append-log";

  //////////////////// Configuration Fields ////////////////////

  /**
   * The config object used to create this distributed system
   */
  private final DistributionConfig originalConfig;

  private final boolean statsDisabled =
      Boolean.getBoolean(GEMFIRE_PREFIX + "statsDisabled");

  /**
   * The config object to which most configuration work is delegated
   */
  private DistributionConfig config;

  private volatile boolean shareSockets;

  /**
   * if this distributed system starts a locator, it is stored here
   */
  private InternalLocator startedLocator;

  private final List<ResourceEventsListener> resourceListeners = new CopyOnWriteArrayList<>();

  private final boolean disableManagement = Boolean.getBoolean(DISABLE_MANAGEMENT_PROPERTY);

  /**
   * Stack trace showing the creation of this instance of InternalDistributedSystem.
   */
  private final Throwable creationStack;

  private volatile SecurityService securityService;

  /**
   * Used at client side, indicates whether the 'delta-propagation' property is enabled on the DS
   * this client is connected to. This variable is used to decide whether to send delta bytes or
   * full value to the server for a delta-update operation.
   */
  private boolean deltaEnabledOnServer = true;

  private final AlertingSession alertingSession;
  private final InternalAlertingService alertingService;

  private final LoggingSession loggingSession;
  private final Set<LogConfigListener> logConfigListeners = new HashSet<>();

  public boolean isDeltaEnabledOnServer() {
    return deltaEnabledOnServer;
  }

  public void setDeltaEnabledOnServer(boolean deltaEnabledOnServer) {
    this.deltaEnabledOnServer = deltaEnabledOnServer;
  }

  public static void removeSystem(InternalDistributedSystem oldSystem) {
    DistributedSystem.removeSystem(oldSystem);
  }

  /**
   * Returns a connection to the distributed system that is suitable for administration. For
   * administration, we are not as strict when it comes to existing connections.
   *
   * @since GemFire 4.0
   */
  public static DistributedSystem connectForAdmin(Properties props) {
    return DistributedSystem.connectForAdmin(props);
  }

  /**
   * Returns a connected distributed system for this VM, or null if there is no connected
   * distributed system in this VM. This method synchronizes on the existingSystems collection.
   * <p>
   * <p>
   * author bruce
   *
   * @since GemFire 5.0
   */
  public static InternalDistributedSystem getConnectedInstance() {
    InternalDistributedSystem result = null;
    synchronized (existingSystemsLock) {
      if (!existingSystems.isEmpty()) {
        InternalDistributedSystem existingSystem = existingSystems.get(0);
        if (existingSystem.isConnected()) {
          result = existingSystem;
        }
      }
    }
    return result;
  }

  /**
   * Returns the current distributed system, if there is one. Note: this method is no longer unsafe
   * size existingSystems uses copy-on-write.
   * <p>
   * author bruce
   *
   * @since GemFire 5.0
   */
  public static InternalDistributedSystem unsafeGetConnectedInstance() {
    InternalDistributedSystem result = getAnyInstance();
    if (result != null) {
      if (!result.isConnected()) {
        result = null;
      }
    }
    return result;
  }

  /**
   * @return distribution stats, or null if there is no distributed system available
   */
  public static DMStats getDMStats() {
    InternalDistributedSystem sys = getAnyInstance();
    if (sys != null && sys.dm != null) {
      return sys.dm.getStats();
    }
    return null;
  }

  /**
   * @return a log writer, or null if there is no distributed system available
   */
  @Deprecated
  public static LogWriter getLogger() {
    InternalDistributedSystem sys = getAnyInstance();
    if (sys != null && sys.logWriter != null) {
      return sys.logWriter;
    }
    return null;
  }

  @Deprecated
  public static InternalLogWriter getStaticInternalLogWriter() {
    InternalDistributedSystem sys = getAnyInstance();
    if (sys != null) {
      return sys.logWriter;
    }
    return null;
  }

  @Deprecated
  public InternalLogWriter getInternalLogWriter() {
    return logWriter;
  }

  @Deprecated
  public InternalLogWriter getSecurityInternalLogWriter() {
    InternalDistributedSystem sys = getAnyInstance();
    if (sys != null) {
      return sys.securityLogWriter;
    }
    return null;
  }

  /**
   * reset the reconnectAttempt counter for a new go at reconnecting
   */
  private static void resetReconnectAttemptCounter() {
    reconnectAttemptCounter.set(0);
  }

  /**
   * Creates a new {@code InternalDistributedSystem} with the given configuration.
   *
   * <p>
   * See {@link #connect} for a list of exceptions that may be thrown.
   *
   * @param config the configuration for the connection
   * @param statisticsManagerFactory creates the statistics manager for this member
   */
  private InternalDistributedSystem(ConnectionConfig config,
      StatisticsManagerFactory statisticsManagerFactory,
      FunctionStatsManager.Factory functionStatsManagerFactory) {
    alertingSession = AlertingSession.create();
    alertingService = new InternalAlertingServiceFactory().create();
    LoggingUncaughtExceptionHandler
        .setFailureSetter(error -> SystemFailure.setFailure((VirtualMachineError) error));
    loggingSession = LoggingSession.create();
    originalConfig = config.distributionConfig();
    isReconnectingDS = config.isReconnecting();
    quorumChecker = config.quorumChecker();

    // throws IllegalStateEx
    ((DistributionConfigImpl) originalConfig).checkForDisallowedDefaults();
    shareSockets = originalConfig.getConserveSockets();
    startTime = System.currentTimeMillis();
    grc = new GrantorRequestProcessor.GrantorRequestContext(stopper);

    creationStack =
        TEST_CREATION_STACK_GENERATOR.get().generateCreationStack(originalConfig);

    statisticsManager =
        statisticsManagerFactory.create(originalConfig.getName(), startTime, statsDisabled);

    this.functionStatsManager = functionStatsManagerFactory.create(statsDisabled, statisticsManager,
        new MeterRegistrySupplier(() -> this));
  }

  public SecurityService getSecurityService() {
    return securityService;
  }

  public void setSecurityService(SecurityService securityService) {
    this.securityService = securityService;
  }

  /**
   * Registers a listener to the system
   *
   * @param listener listener to be added
   */
  public void addResourceListener(ResourceEventsListener listener) {
    resourceListeners.add(listener);
  }

  /**
   * Un-Registers a listener to the system
   *
   * @param listener listener to be removed
   */
  public void removeResourceListener(ResourceEventsListener listener) {
    resourceListeners.remove(listener);
  }

  /**
   * @return the listeners registered with the system
   */
  public List<ResourceEventsListener> getResourceListeners() {
    return resourceListeners;
  }

  /**
   * Handles a particular event associated with a resource
   *
   * @param event Resource event
   * @param resource resource on which event is generated
   */
  public void handleResourceEvent(ResourceEvent event, Object resource) {
    if (disableManagement) {
      return;
    }
    if (resourceListeners.size() == 0) {
      return;
    }
    notifyResourceEventListeners(event, resource);
  }

  /**
   * Returns true if system is a loner (for testing)
   */
  public boolean isLoner() {
    return isLoner;
  }

  private MemoryAllocator offHeapStore = null;

  public MemoryAllocator getOffHeapStore() {
    return offHeapStore;
  }

  /**
   * Initialize any services that provided as extensions to the cache using the service loader
   * mechanism.
   */
  private void initializeServices() {
    ServiceLoader<DistributedSystemService> loader =
        ServiceLoader.load(DistributedSystemService.class);
    for (DistributedSystemService service : loader) {
      service.init(this);
      services.put(service.getInterface(), service);
    }
  }


  /**
   * Initializes this connection to a distributed system with the current configuration state.
   */
  private void initialize(SecurityManager securityManager, PostProcessor postProcessor,
      MetricsService.Builder metricsServiceBuilder,
      final MembershipLocator<InternalDistributedMember> membershipLocatorArg) {

    if (originalConfig.getLocators().equals("")) {
      if (originalConfig.getMcastPort() != 0) {
        throw new GemFireConfigException("The " + LOCATORS + " attribute can not be empty when the "
            + MCAST_PORT + " attribute is non-zero.");
      } else {
        // no distribution
        isLoner = true;
      }
    }

    config = new RuntimeDistributionConfigImpl(this);

    securityService = SecurityServiceFactory.create(
        config.getSecurityProps(),
        securityManager, postProcessor);

    if (!isLoner) {
      attemptingToReconnect = (reconnectAttemptCounter.get() > 0);
    }
    try {
      SocketCreatorFactory.setDistributionConfig(config);

      boolean logBanner = !attemptingToReconnect;
      boolean logConfiguration = !attemptingToReconnect;
      loggingSession.createSession(this, logBanner, logConfiguration);

      // LOG: create LogWriterLogger(s) for backwards compatibility of getLogWriter and
      // getSecurityLogWriter
      if (logWriter == null) {
        logWriter =
            LogWriterFactory.createLogWriterLogger(config, false);
        logWriter.fine("LogWriter is created.");
      }

      // logWriter.info("Created log writer for IDS@"+System.identityHashCode(this));

      if (securityLogWriter == null) {
        // LOG: whole new LogWriterLogger instance for security
        securityLogWriter =
            LogWriterFactory.createLogWriterLogger(config, true);
        securityLogWriter.fine("SecurityLogWriter is created.");
      }

      loggingSession.startSession();

      clock = new DSClock(isLoner);

      if (attemptingToReconnect && logger.isDebugEnabled()) {
        logger.debug(
            "This thread is initializing a new DistributedSystem in order to reconnect to other "
                + "members");
      }
      // Note we need loners to load the license in case they are a
      // cache server and will need to enforce the member limit
      if (Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE)) {
        locatorDMTypeForced = true;
      }

      initializeServices();
      InternalDataSerializer.initialize(config, services.values());

      // Initialize the Diffie-Hellman and public/private keys
      try {
        EncryptorImpl.initCertsMap(config.getSecurityProps());
        EncryptorImpl.initPrivateKey(config.getSecurityProps());
        EncryptorImpl.initDHKeys(config);
      } catch (Exception ex) {
        throw new GemFireSecurityException(
            "Problem in initializing keys for client authentication",
            ex);
      }

      final long offHeapMemorySize =
          OffHeapStorage.parseOffHeapMemorySize(getConfig().getOffHeapMemorySize());

      offHeapStore = OffHeapStorage.createOffHeapStorage(this, offHeapMemorySize, this);

      // Note: this can only happen on a linux system
      if (getConfig().getLockMemory()) {
        // This calculation is not exact, but seems fairly close. So far we have
        // not loaded much into the heap and the current RSS usage is already
        // included the available memory calculation.
        long avail = LinuxProcFsStatistics.getAvailableMemory(logger);
        long size = offHeapMemorySize + Runtime.getRuntime().totalMemory();
        lockMemory(avail, size);
      }

      try {
        startInitLocator(membershipLocatorArg);
      } catch (InterruptedException e) {
        throw new SystemConnectException("Startup has been interrupted", e);
      }

      synchronized (isConnectedMutex) {
        isConnected = true;
      }

      if (!isLoner) {
        try {
          dm = ClusterDistributionManager.create(this, membershipLocator);
          // fix bug #46324
          if (InternalLocator.hasLocator()) {
            InternalLocator internalLocator = InternalLocator.getLocator();
            getDistributionManager().addHostedLocators(getDistributedMember(),
                InternalLocator.getLocatorStrings(),
                internalLocator.isSharedConfigurationEnabled());
          }
        } finally {
          if (dm == null && quorumChecker != null) {
            quorumChecker.resume();
          }
          setDisconnected();
        }
      } else {
        dm = new LonerDistributionManager(this, logWriter);
      }

      Assert.assertTrue(dm != null);
      Assert.assertTrue(dm.getSystem() == this);

      if (dm != null && dm.getDistribution() != null) {
        id = dm.getDistribution().getLocalMember().getMembershipPort();
      }

      synchronized (isConnectedMutex) {
        isConnected = true;
      }
      if (attemptingToReconnect && (startedLocator == null)) {
        try {
          startInitLocator(membershipLocatorArg);
        } catch (InterruptedException e) {
          throw new SystemConnectException("Startup has been interrupted", e);
        }
      }
      try {
        endInitLocator();
      } catch (IOException e) {
        throw new GemFireIOException("Problem finishing a locator service start", e);
      }

      startSampler();

      clusterAlertMessaging.set(new ClusterAlertMessaging(this));
      alertingService.useAlertMessaging(clusterAlertMessaging.get());
      alertingSession.createSession(alertingService);
      alertingSession.startSession();

      metricsService = metricsServiceBuilder.build(this);
      metricsService.start();

      // Log any instantiators that were registered before the log writer
      // was created
      InternalInstantiator.logInstantiators();
    } catch (RuntimeException ex) {
      config.close();
      throw ex;
    }

    reconnected = attemptingToReconnect;
    attemptingToReconnect = false;
    reconnectAttemptCounter.set(0);
  }

  void lockMemory(long avail, long size) {
    if (avail < size) {
      if (avoidMemoryLockWhenOvercommitted) {
        logger.warn(
            "System memory appears to be over committed by {} bytes. Memory will not be locked because -D{} is set to true.",
            size - avail, AVOID_MEMORY_LOCK_WHEN_OVERCOMMIT);
      } else if (allowMemoryLockWhenOvercommitted) {
        logger.warn(
            "System memory appears to be over committed by {} bytes. Memory is locked anyway because -D{} is set to true. You may experience instability, performance issues, or terminated processes due to the Linux OOM killer.",
            size - avail, ALLOW_MEMORY_OVERCOMMIT);
        lockMemory();
      } else {
        throw new IllegalStateException(
            String.format(
                "Insufficient free memory (%s) when attempting to lock %s bytes.  Either reduce the amount of heap or off-heap memory requested or free up additional system memory.  You may also specify -D%s=true on the command-line to override the constraint check.",
                avail, size, ALLOW_MEMORY_OVERCOMMIT));
      }
    } else {
      lockMemory();
    }
  }

  void lockMemory() {
    logger.info("Locking memory. This may take a while...");
    GemFireCacheImpl.lockMemory();
    logger.info("Finished locking memory.");
  }

  private void startSampler() {
    if (statsDisabled) {
      return;
    }
    sampler = loggingSession.getLogFile()
        .map(logFile -> new GemFireStatSampler(this, logFile))
        .orElseGet(() -> new GemFireStatSampler(this));
    sampler.start();
  }

  /**
   * Starts a locator in this JVM iff the distribution config wants one started.
   *
   * @since GemFire 5.7
   * @param membershipLocatorArg on initial startup, a MembershipLocator provided explicitly by
   *        a caller, or null; on restart, the old MembershipLocator (from the previous instance of
   *        InternalDistributedSystem.)
   */
  private void startInitLocator(
      final MembershipLocator<InternalDistributedMember> membershipLocatorArg)
      throws InterruptedException {

    final String locatorString = originalConfig.getStartLocator();
    final boolean shouldStartLocator = locatorString.length() > 0;

    if (!shouldStartLocator) {
      membershipLocator = membershipLocatorArg;
      return;
    }

    // when reconnecting we don't want to join with a colocated locator unless
    // there is a quorum of the old members available
    if (attemptingToReconnect && !isConnected) {
      if (quorumChecker != null) {
        logger.info("performing a quorum check to see if location services can be started early");
        if (!quorumChecker.checkForQuorum(3L * config.getMemberTimeout())) {
          logger.info("quorum check failed - not allowing location services to start early");
          return;
        }
        logger.info("Quorum check passed - allowing location services to start early");
      }
    }
    DistributionLocatorId locId = new DistributionLocatorId(locatorString);
    try {
      startedLocator =
          InternalLocator.createLocator(locId.getPort(), NullLoggingSession.create(), null,
              logWriter, securityLogWriter, locId.getHost().getAddress(),
              locId.getHostnameForClients(), originalConfig.toProperties(), false);

      // if locator is started this way, cluster config is not enabled, set the flag correctly
      startedLocator.getConfig().setEnableClusterConfiguration(false);

      boolean startedPeerLocation = false;
      try {
        startedLocator.startPeerLocation();
        membershipLocator = startedLocator.getMembershipLocator();
        startedPeerLocation = true;
      } finally {
        if (!startedPeerLocation) {
          startedLocator.stop();
          startedLocator = null;
          membershipLocator = null;
        }
      }
    } catch (IOException e) {
      throw new GemFireIOException(
          "Problem starting a locator service",
          e);
    }
  }

  /**
   * @since GemFire 5.7
   */
  private void endInitLocator() throws IOException {
    InternalLocator loc = startedLocator;
    if (loc != null) {
      boolean finished = false;
      try {
        loc.startServerLocation(this);
        loc.endStartLocator(this);
        finished = true;
      } finally {
        if (!finished) {
          loc.stop();
        }
      }
    }
  }

  /**
   * Used by DistributionManager to fix bug 33362
   */
  void setDM(DistributionManager dm) {
    this.dm = dm;
  }

  /**
   * Checks whether or not this connection to a distributed system is closed.
   *
   * @throws DistributedSystemDisconnectedException This connection has been
   *         {@link #disconnect(boolean, String, boolean) disconnected}
   */
  private void checkConnected() {
    if (!isConnected()) {
      throw new DistributedSystemDisconnectedException(
          "This connection to a distributed system has been disconnected.",
          dm.getRootCause());
    }
  }

  @Override
  public boolean isConnected() {
    if (dm == null) {
      return false;
    }
    if (dm.getCancelCriterion().isCancelInProgress()) {
      return false;
    }
    if (isDisconnecting) {
      return false;
    }
    return isConnected;
  }

  /*
   * This method was introduced so we can deterministically query whether the distributed
   * system has fully disconnected or not. The isConnected() method will return false if a
   * disconnection/cancellation is in progress, so it does not provide a reliable way to
   * query if the distributed system is fully disconnected or not.
   */
  public boolean isDisconnected() {
    return !isConnected;
  }

  public StatisticsManager getStatisticsManager() {
    return statisticsManager;
  }

  @Override
  public StatisticDescriptor createIntCounter(String name,
      String description,
      String units) {
    return createLongCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name,
      String description,
      String units) {
    return statisticsManager.createLongCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name,
      String description,
      String units) {
    return statisticsManager.createDoubleCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name,
      String description,
      String units) {
    return createLongGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name,
      String description,
      String units) {
    return statisticsManager.createLongGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name,
      String description,
      String units) {
    return statisticsManager.createDoubleGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name,
      String description,
      String units, boolean largerBetter) {
    return createLongCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name,
      String description,
      String units, boolean largerBetter) {
    return statisticsManager.createLongCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name,
      String description,
      String units,
      boolean largerBetter) {
    return statisticsManager.createDoubleCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name,
      String description,
      String units, boolean largerBetter) {
    return createLongGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name,
      String description,
      String units, boolean largerBetter) {
    return statisticsManager.createLongGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name,
      String description,
      String units, boolean largerBetter) {
    return statisticsManager.createDoubleGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticsType createType(String name, String description,
      StatisticDescriptor[] stats) {
    return statisticsManager.createType(name, description, stats);
  }

  @Override
  public StatisticsType findType(String name) {
    return statisticsManager.findType(name);
  }

  @Override
  public StatisticsType[] createTypesFromXml(Reader reader)
      throws IOException {
    return statisticsManager.createTypesFromXml(reader);
  }

  @Override
  public Statistics createStatistics(StatisticsType type) {
    return statisticsManager.createStatistics(type);
  }

  @Override
  public Statistics createStatistics(StatisticsType type,
      String textId) {
    return statisticsManager.createStatistics(type, textId);
  }

  @Override
  public Statistics createStatistics(StatisticsType type,
      String textId, long numericId) {
    return statisticsManager.createStatistics(type, textId, numericId);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type) {
    return statisticsManager.createAtomicStatistics(type);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type,
      String textId) {
    return statisticsManager.createAtomicStatistics(type, textId);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type,
      String textId, long numericId) {
    return statisticsManager.createAtomicStatistics(type, textId, numericId);
  }

  @Override
  public Statistics[] findStatisticsByType(StatisticsType type) {
    return statisticsManager.findStatisticsByType(type);
  }

  @Override
  public Statistics[] findStatisticsByTextId(String textId) {
    return statisticsManager.findStatisticsByTextId(textId);
  }

  @Override
  public Statistics[] findStatisticsByNumericId(long numericId) {
    return statisticsManager.findStatisticsByNumericId(numericId);
  }

  @Override
  public String getName() {
    return getOriginalConfig().getName();
  }

  @Override
  public long getId() {
    return id;
  }

  public long getStartTime() {
    return startTime;
  }

  public MeterRegistry getMeterRegistry() {
    return metricsService.getMeterRegistry();
  }

  /**
   * This class defers to the DM. If we don't have a DM, we're dead.
   */
  protected class Stopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      checkFailure();
      if (dm == null) {
        return "No dm";
      }
      return dm.getCancelCriterion().cancelInProgress();
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      if (dm == null) {
        return new DistributedSystemDisconnectedException("no dm", e);
      }
      return dm.getCancelCriterion().generateCancelledException(e);
    }
  }

  /**
   * Handles all cancellation queries for this distributed system
   */
  private final Stopper stopper = new Stopper();

  @Override
  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

  public boolean isDisconnecting() {
    if (dm == null) {
      return true;
    }
    if (dm.getCancelCriterion().isCancelInProgress()) {
      return true;
    }
    if (!isConnected) {
      return true;
    }
    return isDisconnecting;
  }

  @Override
  public LogWriter getLogWriter() {
    return logWriter;
  }

  public DSClock getClock() {
    return clock;
  }

  @Override
  public LogWriter getSecurityLogWriter() {
    return securityLogWriter;
  }

  /**
   * Returns the stat sampler
   */
  public GemFireStatSampler getStatSampler() {
    return sampler;
  }

  /**
   * Has this system started the disconnect process?
   */
  protected volatile boolean isDisconnecting = false;

  /**
   * Disconnects this VM from the distributed system. Shuts down the distribution manager, and if
   * necessary,
   */
  @Override
  public void disconnect() {
    disconnect(false,
        "normal disconnect", false);
  }

  /**
   * Disconnects this member from the distributed system when an internal error has caused
   * distribution to fail (e.g., this member was shunned)
   *
   * @param reason a string describing why the disconnect is occurring
   * @param shunned whether this member was shunned by the membership coordinator
   */
  public void disconnect(String reason, boolean shunned) {
    boolean isForcedDisconnect = dm.getRootCause() instanceof ForcedDisconnectException;
    boolean rejoined = false;
    reconnected = false;
    if (isForcedDisconnect && !isReconnectingDS) {
      forcedDisconnect = true;
      resetReconnectAttemptCounter();
      rejoined = tryReconnect(true, reason, GemFireCacheImpl.getInstance());
    }
    if (!rejoined) {
      disconnect(false, reason, shunned);
    }
  }

  /**
   * This is how much time, in milliseconds to allow a disconnect listener to run before we
   * interrupt it.
   */
  private static final long MAX_DISCONNECT_WAIT =
      Long.getLong("DistributionManager.DISCONNECT_WAIT", 10 * 1000);

  /**
   * Run a disconnect listener, checking for errors and honoring the timeout
   * {@link #MAX_DISCONNECT_WAIT}.
   *
   * @param dc the listener to run
   */
  private void runDisconnect(final DisconnectListener dc) {
    // Create a general handler for running the disconnect
    // Launch it and wait a little bit
    Thread t = new LoggingThread(dc.toString(), false, () -> {
      try {
        isDisconnectThread.set(Boolean.TRUE);
        dc.onDisconnect(InternalDistributedSystem.this);
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Disconnect listener <{}> thwarted by cancellation: {}", dc, e,
              traceException(e));
        }
      }
    });
    try {
      t.start();
      t.join(MAX_DISCONNECT_WAIT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Interrupted while processing disconnect listener",
          e);
    }

    // Make sure the listener gets the cue to die
    if (t.isAlive()) {
      logger.warn("Disconnect listener still running: {}", dc);
      t.interrupt();

      try {
        t.join(MAX_DISCONNECT_WAIT);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      if (t.isAlive()) {
        logger.warn("Disconnect listener ignored its interrupt: {}",
            dc);
      }
    }

  }

  private Exception traceException(CancelException e) {
    return logger.isTraceEnabled() ? e : null;
  }

  public boolean isDisconnectThread() {
    return isDisconnectThread.get();
  }

  public void setIsDisconnectThread() {
    isDisconnectThread.set(Boolean.TRUE);
  }

  /**
   * Run a disconnect listener in the same thread sequence as the reconnect.
   *
   * @param dc the listener to run
   */

  private void runDisconnectForReconnect(final DisconnectListener dc) {
    try {
      dc.onDisconnect(this);
    } catch (DistributedSystemDisconnectedException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Disconnect listener <{}> thwarted by shutdown: {}", dc, e, traceException(e));
      }
    }
  }

  /**
   * Disconnect cache, run disconnect listeners.
   *
   * @param doReconnect whether a reconnect will be done
   * @return a collection of shutdownListeners
   */
  private HashSet<ShutdownListener> doDisconnects(boolean doReconnect) {
    // Make a pass over the disconnect listeners, asking them _politely_
    // to clean up.
    HashSet<ShutdownListener> shutdownListeners = new HashSet<>();
    for (;;) {
      DisconnectListener listener;
      synchronized (disconnectListeners) {
        Iterator<DisconnectListener> itr = disconnectListeners.iterator();
        if (!itr.hasNext()) {
          return shutdownListeners;
        }
        listener = itr.next();
        if (listener instanceof ShutdownListener) {
          shutdownListeners.add((ShutdownListener) listener);
        }
        itr.remove();
      } // synchronized

      if (doReconnect) {
        runDisconnectForReconnect(listener);
      } else {
        runDisconnect(listener);
      }
    } // for
  }

  /**
   * Process the shutdown listeners. It is essential that the DM has been shut down before calling
   * this step, to ensure that no new listeners are registering.
   *
   * @param shutdownListeners shutdown listeners initially registered with us
   */
  private void doShutdownListeners(HashSet<ShutdownListener> shutdownListeners) {
    if (shutdownListeners == null) {
      return;
    }

    // Process any shutdown listeners we reaped during first pass
    for (ShutdownListener shutdownListener : shutdownListeners) {
      try {
        shutdownListener.onShutdown(this);
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        // things could break since we continue, but we want to disconnect!
        logger.fatal(String.format("ShutdownListener < %s > threw...", shutdownListener), t);
      }
    }

    // During the window while we were running disconnect listeners, new
    // disconnect listeners may have appeared. After messagingDisabled is
    // set, no new ones will be created. However, we must process any
    // that appeared in the interim.
    for (;;) {
      // Pluck next listener from the list
      DisconnectListener dcListener;
      ShutdownListener sdListener = null;
      synchronized (disconnectListeners) {
        Iterator<DisconnectListener> itr = disconnectListeners.iterator();
        if (!itr.hasNext()) {
          break;
        }
        dcListener = itr.next();
        itr.remove();
        if (dcListener instanceof ShutdownListener) {
          sdListener = (ShutdownListener) dcListener;
        }
      }

      runDisconnect(dcListener);

      // Run the shutdown, if any
      if (sdListener != null) {
        try {
          sdListener.onShutdown(this);
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          // things could break since we continue, but we want to disconnect!
          logger.fatal("DisconnectListener/Shutdown threw...", t);
        }
      }
    } // for
  }

  /**
   * Closes the membership manager
   *
   * @see SystemFailure#emergencyClose()
   */
  public void emergencyClose() {
    if (dm != null) {
      Distribution mm = dm.getDistribution();
      if (mm != null) {
        mm.emergencyClose();
      }
    }

    // Garbage collection
    // Leave dm alone; its CancelCriterion will help people die
    isConnected = false;
    if (dm != null) {
      dm.setRootCause(SystemFailure.getFailure());
    }
    isDisconnecting = true;
    disconnectListeners.clear();
  }

  private void setDisconnected() {
    synchronized (isConnectedMutex) {
      isConnected = false;
      isConnectedMutex.notifyAll();
    }
  }

  private void waitDisconnected() {
    synchronized (isConnectedMutex) {
      while (isConnected) {
        boolean interrupted = Thread.interrupted();
        try {
          isConnectedMutex.wait();
        } catch (InterruptedException e) {
          interrupted = true;
          getLogWriter()
              .warning("Disconnect wait interrupted", e);
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // while
    }
  }

  /**
   * Disconnects this VM from the distributed system. Shuts down the distribution manager.
   *
   * @param preparingForReconnect true if called by a reconnect operation
   * @param reason the reason the disconnect is being performed
   * @param keepAlive true if user requested durable subscriptions are to be retained at server.
   */
  protected void disconnect(boolean preparingForReconnect, String reason, boolean keepAlive) {
    boolean isShutdownHook = (shutdownHook != null) && (Thread.currentThread() == shutdownHook);

    if (!preparingForReconnect) {
      // logger.info("disconnecting IDS@"+System.identityHashCode(this));
      synchronized (reconnectListeners) {
        reconnectListeners.clear();
      }
      cancelReconnect();
    }

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      HashSet<ShutdownListener> shutdownListeners = null;
      try {
        if (isDebugEnabled) {
          logger.debug("DistributedSystem.disconnect invoked on {}", this);
        }
        synchronized (GemFireCacheImpl.class) {
          // bug 36955, 37014: don't use a disconnect listener on the cache;
          // it takes too long.
          //
          // However, make sure cache is completely closed before starting
          // the distributed system close.
          InternalCache currentCache = getCache();
          if (currentCache != null && !currentCache.isClosed()) {
            isDisconnectThread.set(Boolean.TRUE); // bug #42663 - this must be set while
            // closing the cache
            try {
              currentCache.close(reason, dm.getRootCause(), keepAlive, true); // fix for 42150
            } catch (VirtualMachineError e) {
              SystemFailure.initiateFailure(e);
              throw e;
            } catch (Throwable e) {
              SystemFailure.checkFailure();
              // Whenever you catch Error or Throwable, you must also
              // check for fatal JVM error (see above). However, there is
              logger.warn(
                  "Exception trying to close cache",
                  e);
            } finally {
              isDisconnectThread.set(Boolean.FALSE);
            }
          }

          // While still holding the lock, make sure this instance is
          // marked as shutting down
          synchronized (this) {
            if (isDisconnecting) {
              // It's already started, but don't return
              // to the caller until it has completed.
              waitDisconnected();
              return;
            } // isDisconnecting
            isDisconnecting = true;

            if (!preparingForReconnect) {
              // move cancelReconnect above this synchronized block fix for bug 35202
              if (reconnectDS != null) {
                // break recursion
                if (isDebugEnabled) {
                  logger.debug("disconnecting reconnected DS: {}", reconnectDS);
                }
                InternalDistributedSystem r = reconnectDS;
                reconnectDS = null;
                r.disconnect(false, null, false);
              }
            } // !reconnect
          } // synchronized (this)
        } // synchronized (GemFireCache.class)

        securityService.close();

        if (!isShutdownHook) {
          shutdownListeners = doDisconnects(attemptingToReconnect);
        }

        if (!attemptingToReconnect) {
          alertingSession.stopSession();
        }

      } finally { // be ABSOLUTELY CERTAIN that dm closed
        try {
          // Do the bulk of the close...
          dm.close();
          // we close the locator after the DM so that when split-brain detection
          // is enabled, loss of the locator doesn't cause the DM to croak
          if (startedLocator != null) {
            startedLocator.stop(forcedDisconnect, preparingForReconnect, true);
            startedLocator = null;
          }
        } finally { // timer canceled
          // bug 38501: this has to happen *after*
          // the DM is closed :-(
          if (!preparingForReconnect) {
            SystemTimer.cancelTimers(this);
          }
        } // finally timer cancelled
      } // finally dm closed

      if (!isShutdownHook) {
        doShutdownListeners(shutdownListeners);
      }

      functionStatsManager.close();
      metricsService.stop();

      InternalFunctionService.unregisterAllFunctions();

      if (sampler != null) {
        sampler.stop();
        sampler = null;
      }

      if (!attemptingToReconnect) {
        loggingSession.stopSession();
      }

      EventID.unsetDS();

    } finally {
      try {
        if (getOffHeapStore() != null) {
          getOffHeapStore().close();
        }
      } finally {
        try {
          removeSystem(this);
          if (!attemptingToReconnect) {
            loggingSession.shutdown();
          }
          alertingService.useAlertMessaging(new NullAlertMessaging());
          clusterAlertMessaging.get().close();
          alertingSession.shutdown();
          // Close the config object
          config.close();
        } finally {
          // Finally, mark ourselves as disconnected
          setDisconnected();
          SystemFailure.stopThreads();
        }
      }
    }
  }

  /**
   * Returns the distribution manager for accessing this distributed system.
   */
  public DistributionManager getDistributionManager() {
    checkConnected();
    return dm;
  }

  /**
   * Returns the distribution manager without checking for connected or not so can also return null.
   */
  public DistributionManager getDM() {
    return dm;
  }

  /**
   * If this DistributedSystem is attempting to reconnect to the distributed system this will return
   * the quorum checker created by the old {@link Distribution} for checking to see if a quorum of
   * old
   * members can be reached.
   *
   * @return the quorum checking service
   */
  QuorumChecker getQuorumChecker() {
    return quorumChecker;
  }

  /**
   * Returns true if this DS has been attempting to reconnect but the attempt has been cancelled.
   */
  public boolean isReconnectCancelled() {
    return reconnectCancelled;
  }

  /**
   * Returns whether or not this distributed system has the same configuration as the given set of
   * properties.
   *
   * @see DistributedSystem#connect
   */
  public boolean sameAs(Properties props, boolean isConnected) {
    return originalConfig.sameAs(DistributionConfigImpl.produce(props, isConnected));
  }

  public boolean threadOwnsResources() {
    Boolean b = ConnectionTable.getThreadOwnsResourcesRegistration();
    if (b == null) {
      // thread does not have a preference so return default
      return !shareSockets;
    } else {
      return b;
    }
  }

  /**
   * Returns whether or not the given configuration properties refer to the same distributed system
   * as this <code>InternalDistributedSystem</code> connection.
   *
   * @since GemFire 4.0
   */
  public boolean sameSystemAs(Properties props) {
    DistributionConfig other = DistributionConfigImpl.produce(props);
    DistributionConfig me = getConfig();

    if (!me.getBindAddress().equals(other.getBindAddress())) {
      return false;
    }

    // locators
    String myLocators = me.getLocators();
    String otherLocators = other.getLocators();

    // quick check
    if (myLocators.equals(otherLocators)) {
      return true;

    } else {
      myLocators = canonicalizeLocators(myLocators);
      otherLocators = canonicalizeLocators(otherLocators);

      return myLocators.equals(otherLocators);
    }
  }

  /**
   * Canonicalizes a locators string so that they may be compared.
   *
   * @since GemFire 4.0
   */
  private static String canonicalizeLocators(String locators) {
    SortedSet<String> sorted = new TreeSet<>();
    StringTokenizer st = new StringTokenizer(locators, ",");
    while (st.hasMoreTokens()) {
      String l = st.nextToken();
      StringBuilder canonical = new StringBuilder();
      DistributionLocatorId locId = new DistributionLocatorId(l);
      String addr = locId.getBindAddress();
      if (addr != null && addr.trim().length() > 0) {
        canonical.append(addr);
      } else {
        canonical.append(locId.getHostName());
      }
      canonical.append("[");
      canonical.append(locId.getPort());
      canonical.append("]");
      sorted.add(canonical.toString());
    }

    StringBuilder sb = new StringBuilder();
    for (Iterator iter = sorted.iterator(); iter.hasNext();) {
      sb.append((String) iter.next());
      if (iter.hasNext()) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  /**
   * Returns the current configuration of this distributed system.
   */
  public DistributionConfig getConfig() {
    return config;
  }

  public InternalAlertingService getAlertingService() {
    return alertingService;
  }

  @Override
  public LogConfig getLogConfig() {
    return config;
  }

  @Override
  public StatisticsConfig getStatisticsConfig() {
    return config;
  }

  @Override
  public void addLogConfigListener(LogConfigListener logConfigListener) {
    logConfigListeners.add(logConfigListener);
  }

  @Override
  public void removeLogConfigListener(LogConfigListener logConfigListener) {
    logConfigListeners.remove(logConfigListener);
  }

  public Optional<LogFile> getLogFile() {
    return loggingSession.getLogFile();
  }

  void logConfigChanged() {
    for (LogConfigListener listener : logConfigListeners) {
      listener.configChanged();
    }
  }

  /**
   * Returns the string value of the distribution manager's id.
   */
  @Override
  public String getMemberId() {
    return String.valueOf(dm.getId());
  }

  @Override
  public InternalDistributedMember getDistributedMember() {
    return dm.getId();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Set<DistributedMember> getAllOtherMembers() {
    return (Set) dm.getAllOtherMembers();
  }

  @Override
  public Set<DistributedMember> getGroupMembers(String group) {
    return dm.getGroupMembers(group);
  }


  @Override
  public Set<DistributedMember> findDistributedMembers(InetAddress address) {
    Set<InternalDistributedMember> allMembers = dm.getDistributionManagerIdsIncludingAdmin();
    Set<DistributedMember> results = new HashSet<>(2);

    // Search through the set of all members
    for (InternalDistributedMember member : allMembers) {

      Set<InetAddress> equivalentAddresses = dm.getEquivalents(member.getInetAddress());
      // Check to see if the passed in address is matches one of the addresses on
      // the given member.
      if (address.equals(member.getInetAddress()) || equivalentAddresses.contains(address)) {
        results.add(member);
      }
    }

    return results;
  }

  @Override
  public DistributedMember findDistributedMember(String name) {
    for (DistributedMember member : dm.getDistributionManagerIdsIncludingAdmin()) {
      if (member.getName().equals(name)) {
        return member;
      }
    }
    return null;
  }

  /**
   * Returns the configuration this distributed system was created with.
   */
  public DistributionConfig getOriginalConfig() {
    return originalConfig;
  }

  /////////////////////// Utility Methods ///////////////////////

  /**
   * Since {@link DistributedSystem#connect} guarantees that there is a canonical instance of
   * <code>DistributedSystem</code> for each configuration, we can use the default implementation of
   * <code>equals</code>.
   *
   * @see #sameAs
   */
  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  /**
   * Since we use the default implementation of {@link #equals equals}, we can use the default
   * implementation of <code>hashCode</code>.
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Returns a string describing this connection to distributed system (including highlights of its
   * configuration).
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Connected ");
    String name = getName();
    if (name != null && !name.equals("")) {
      sb.append("\"");
      sb.append(name);
      sb.append("\" ");
    }
    sb.append("(id=");
    sb.append(Integer.toHexString(System.identityHashCode(this)));
    sb.append(") ");

    sb.append("to distributed system using ");
    int port = config.getMcastPort();
    if (port != 0) {
      sb.append("multicast port ");
      sb.append(port);
      sb.append(" ");

    } else {
      sb.append("locators \"");
      sb.append(config.getLocators());
      sb.append("\" ");
    }

    File logFile = config.getLogFile();
    sb.append("logging to ");
    if (logFile == null || logFile.equals(new File(""))) {
      sb.append("standard out ");

    } else {
      sb.append(logFile);
      sb.append(" ");
    }

    sb.append(" started at ");
    sb.append((new Date(startTime)).toString());

    if (!isConnected()) {
      sb.append(" (closed)");
    }

    return sb.toString().trim();
  }

  public FunctionStatsManager getFunctionStatsManager() {
    return functionStatsManager;
  }

  /**
   * Makes note of a <code>ConnectListener</code> whose <code>onConnect</code> method will be
   * invoked when a connection is created to a distributed system.
   *
   * @return set of currently existing system connections
   */
  public static List addConnectListener(ConnectListener listener) {
    synchronized (existingSystemsLock) {
      synchronized (connectListeners) {
        connectListeners.add(listener);
        return existingSystems;
      }
    }
  }

  public static void removeConnectListener(ConnectListener listener) {
    synchronized (existingSystemsLock) {
      synchronized (connectListeners) {
        connectListeners.remove(listener);
      }
    }
  }

  /**
   * Makes note of a <code>ReconnectListener</code> whose <code>onReconnect</code> method will be
   * invoked when a connection is recreated to a distributed system during auto-reconnect.
   * <p>
   * <p>
   * The ReconnectListener set is cleared after a disconnect.
   */
  public static void addReconnectListener(ReconnectListener listener) {
    synchronized (existingSystemsLock) {
      synchronized (reconnectListeners) {
        reconnectListeners.add(listener);
      }
    }
  }

  /**
   * Notifies all registered <code>ConnectListener</code>s that a connection to a distributed system
   * has been created.
   */
  private static void notifyConnectListeners(InternalDistributedSystem sys) {
    synchronized (connectListeners) {
      for (ConnectListener listener : connectListeners) {
        try {
          listener.onConnect(sys);
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          sys.getLogWriter()
              .severe("ConnectListener threw...", t);
        }
      }
    }
  }

  /**
   * Notifies all registered <code>ReconnectListener</code>s that a connection to a distributed
   * system has been recreated.
   */
  private static void notifyReconnectListeners(InternalDistributedSystem oldsys,
      InternalDistributedSystem newsys, boolean starting) {
    List<ReconnectListener> listeners;
    synchronized (reconnectListeners) {
      listeners = new ArrayList<>(reconnectListeners);
    }
    for (ReconnectListener listener : listeners) {
      try {
        if (starting) {
          listener.reconnecting(oldsys);
        } else {
          listener.onReconnect(oldsys, newsys);
        }
      } catch (Throwable t) {
        Error err;
        if (t instanceof OutOfMemoryError || t instanceof UnknownError) {
          err = (Error) t;
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        // Whenever you catch Error or Throwable, you must also
        // check for fatal JVM error (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.fatal("ConnectListener threw...", t);
      }
    }
  }

  /**
   * Notifies all resource event listeners. All exceptions are caught here and only a warning
   * message is printed in the log
   *
   * @param event Enumeration depicting particular resource event
   * @param resource the actual resource object.
   */
  private void notifyResourceEventListeners(ResourceEvent event, Object resource) {
    for (ResourceEventsListener listener : resourceListeners) {
      try {
        listener.handleEvent(event, resource);
      } catch (CancelException e) {
        // ignore
        logger.info("Skipping notifyResourceEventListeners for {} due to cancellation", event);
      } catch (GemFireSecurityException | ManagementException ex) {
        if (event == ResourceEvent.CACHE_CREATE) {
          throw ex;
        } else {
          logger.warn(ex.getMessage(), ex);
        }
      } catch (Exception err) {
        logger.warn(err.getMessage(), err);
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        SystemFailure.checkFailure();
        logger.warn(t.getMessage(), t);
      }
    }
  }

  /**
   * Makes note of a <code>DisconnectListener</code> whose <code>onDisconnect</code> method will be
   * invoked when this connection to the distributed system is disconnected.
   */
  public void addDisconnectListener(DisconnectListener listener) {
    synchronized (disconnectListeners) {
      disconnectListeners.add(listener);

      boolean disconnectThreadBoolean = isDisconnectThread.get();

      if (!disconnectThreadBoolean) {
        // Don't add disconnect listener after messaging has been disabled.
        // Do this test _after_ adding the listener to narrow the window.
        // It's possible to miss it still and never invoke the listener, but
        // other shutdown conditions will presumably get flagged.
        String reason = stopper.cancelInProgress();
        if (reason != null) {
          disconnectListeners.remove(listener); // don't leave in the list!
          throw new DistributedSystemDisconnectedException(
              String.format("No listeners permitted after shutdown: %s",
                  reason),
              dm.getRootCause());
        }
      }
    } // synchronized
  }

  /**
   * Removes a <code>DisconnectListener</code> from the list of listeners that will be notified when
   * this connection to the distributed system is disconnected.
   */
  public void removeDisconnectListener(DisconnectListener listener) {
    synchronized (disconnectListeners) {
      disconnectListeners.remove(listener);
    }
  }

  /**
   * Returns any existing <code>InternalDistributedSystem</code> instance. Returns <code>null</code>
   * if no instance exists.
   */
  public static InternalDistributedSystem getAnyInstance() {
    List l = existingSystems;
    if (l.isEmpty()) {
      return null;
    } else {
      return (InternalDistributedSystem) l.get(0);
    }
  }

  /**
   * Test hook
   */
  public static List getExistingSystems() {
    return existingSystems;
  }

  @Override
  public Properties getProperties() {
    return config.toProperties();
  }

  @Override
  public Properties getSecurityProperties() {
    return config.getSecurityProps();
  }

  /**
   * Installs a shutdown hook to ensure that we are disconnected if an application VM shuts down
   * without first calling disconnect itself.
   */
  @Immutable
  public static final Thread shutdownHook;

  static {
    // Create a shutdown hook to cleanly close connection if
    // VM shuts down with an open connection.
    Thread tmp_shutdownHook = null;
    try {
      // Added for bug 38407
      if (!Boolean.getBoolean(DISABLE_SHUTDOWN_HOOK_PROPERTY)) {
        tmp_shutdownHook = new LoggingThread(SHUTDOWN_HOOK_NAME, false, () -> {
          InternalDistributedSystem ds = InternalDistributedSystem.getAnyInstance();
          setThreadsSocketPolicy(true /* conserve sockets */);
          if (ds != null && ds.isConnected()) {
            logger.info("VM is exiting - shutting down distributed system");
            DurableClientAttributes dca = ds.getDistributedMember()
                .getDurableClientAttributes();
            boolean isDurableClient = false;

            if (dca != null) {
              isDurableClient = (!(dca.getId() == null || dca.getId().isEmpty()));
            }

            ds.disconnect(false,
                "normal disconnect",
                isDurableClient/* keep alive drive from this */);
            // this was how we wanted to do it for 5.7, but there were shutdown
            // issues in PR/dlock (see bug 39287)
            // InternalDistributedSystem ids = (InternalDistributedSystem)ds;
            // if (ids.getDistributionManager() != null &&
            // ids.getDistributionManager().getMembershipManager() != null) {
            // ids.getDistributionManager().getMembershipManager()
            // .uncleanShutdown("VM is exiting", null);
            // }
          }
        }, false);
        Runtime.getRuntime().addShutdownHook(tmp_shutdownHook);
      }
    } finally {
      shutdownHook = tmp_shutdownHook;
    }
  }
  /////////////////////// Inner Classes ///////////////////////

  /**
   * A listener that gets invoked before this connection to the distributed system is disconnected.
   */
  public interface DisconnectListener {

    /**
     * Invoked before a connection to the distributed system is disconnected.
     *
     * @param sys the the system we are disconnecting from process should take before returning.
     */
    void onDisconnect(InternalDistributedSystem sys);

  }

  /**
   * A listener that gets invoked before and after a successful auto-reconnect
   */
  public interface ReconnectListener {

    /**
     * Invoked when reconnect attempts are initiated
     *
     * @param oldSystem the old DS, which is in a partially disconnected state and cannot be used
     *        for messaging
     */
    default void reconnecting(InternalDistributedSystem oldSystem) {
      // nothing
    }

    /**
     * Invoked after a reconnect to the distributed system
     *
     * @param oldSystem the old DS
     * @param newSystem the new DS
     */
    default void onReconnect(InternalDistributedSystem oldSystem,
        InternalDistributedSystem newSystem) {
      // nothing
    }
  }

  /**
   * A listener that gets invoked after this connection to the distributed system is disconnected
   */
  public interface ShutdownListener extends DisconnectListener {

    /**
     * Invoked after the connection to the distributed system has been disconnected
     */
    void onShutdown(InternalDistributedSystem sys);
  }

  /**
   * Integer representing number of tries already made to reconnect and that failed.
   */
  @MakeNotStatic
  private static final AtomicInteger reconnectAttemptCounter = new AtomicInteger();

  /**
   * Boolean indicating if DS needs to reconnect and reconnect is in progress.
   */
  private volatile boolean attemptingToReconnect = false;

  /**
   * Boolean indicating this DS joined through a reconnect attempt
   */
  private volatile boolean reconnected = false;

  /**
   * If reconnect fails due to an exception it will be in this field
   */
  private Exception reconnectException;

  /**
   * Boolean indicating that this member has been shunned by other members or a network partition
   * has occurred
   */
  private volatile boolean forcedDisconnect = false;

  /**
   * Used to keep track of the DS created by doing an reconnect on this.
   */
  private volatile InternalDistributedSystem reconnectDS;
  /**
   * Was this distributed system started with FORCE_LOCATOR_DM_TYPE=true? We need to know when
   * reconnecting.
   */
  private boolean locatorDMTypeForced;


  /**
   * Returns true if we are reconnecting the distributed system or reconnect has completed. If this
   * returns true it means that this instance of the DS is now disconnected and unusable.
   */
  @Override
  public boolean isReconnecting() {
    InternalDistributedSystem rds = reconnectDS;
    if (!attemptingToReconnect) {
      return false;
    }
    if (reconnectCancelled) {
      return false;
    }
    return (rds == null || !rds.isConnected());
  }


  /**
   * Returns true if we are reconnecting the distributed system and this instance was created for
   * one of the connection attempts. If the connection succeeds this state is cleared and this
   * method will commence to return false.
   */
  boolean isReconnectingDS() {
    return isReconnectingDS;
  }

  /**
   * returns the membership socket of the old distributed system, if available, when
   * isReconnectingDS returns true. This is used to connect the new DM to the distributed system
   * through RemoteTransportConfig.
   */
  MembershipInformation oldDSMembershipInfo() {
    if (quorumChecker != null) {
      return quorumChecker.getMembershipInfo();
    }
    return null;
  }

  /**
   * Returns true if this DS reconnected to the distributed system after a forced disconnect or loss
   * of required-roles
   */
  public boolean reconnected() {
    return reconnected;
  }

  /**
   * Returns true if this DS has been kicked out of the distributed system
   */
  public boolean forcedDisconnect() {
    return forcedDisconnect;
  }

  /**
   * If true then this DS will never reconnect.
   */
  private volatile boolean reconnectCancelled = false;

  /**
   * Make sure this instance of DS never does a reconnect. Also if reconnect is in progress cancel
   * it.
   */
  private void cancelReconnect() {
    reconnectCancelled = true;
    if (isReconnecting()) {
      synchronized (reconnectLock) { // should the synchronized be first on this and
        // then on this.reconnectLock.
        reconnectLock.notifyAll();
      }
    }
  }

  /**
   * This lock must be acquired *after* locking any GemFireCache.
   */
  private final Object reconnectLock = new Object();

  /**
   * Tries to reconnect to the distributed system on role loss if configure to reconnect.
   *
   * @param oldCache cache that has apparently failed
   */
  public boolean tryReconnect(boolean forcedDisconnect, String reason, InternalCache oldCache) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isReconnectingDS && forcedDisconnect) {
      return false;
    }
    synchronized (InternalCacheBuilder.class) {
      synchronized (GemFireCacheImpl.class) {
        // bug 39329: must lock reconnectLock *after* the cache
        synchronized (reconnectLock) {
          if (!forcedDisconnect && !oldCache.isClosed()
              && oldCache.getCachePerfStats().getReliableRegionsMissing() == 0) {
            if (isDebugEnabled) {
              logger.debug("tryReconnect: No required roles are missing.");
            }
            return false;
          }

          if (isDebugEnabled) {
            logger.debug("tryReconnect: forcedDisconnect={}", forcedDisconnect);
          }
          if (forcedDisconnect) {
            if (config.getDisableAutoReconnect()) {
              if (isDebugEnabled) {
                logger.debug("tryReconnect: auto reconnect after forced disconnect is disabled");
              }
              return false;
            }
          }
          reconnect(forcedDisconnect, reason);
          return reconnectDS != null && reconnectDS.isConnected();
        } // synchronized reconnectLock
      } // synchronized cache
    } // synchronized CacheFactory.class
  }


  /**
   * Returns the value for the number of time reconnect has been tried. Test method used by DUnit.
   */
  public static int getReconnectAttemptCounter() {
    return reconnectAttemptCounter.get();
  }

  /**
   * A reconnect is tried when gemfire is configured to reconnect in case of a required role loss.
   * The reconnect will try reconnecting to the distributed system every max-time-out millseconds
   * for max-number-of-tries configured in gemfire.properties file. It uses the cache.xml file to
   * intialize the cache and create regions.
   */
  private void reconnect(boolean forcedDisconnect, String reason) {

    // Collect all the state for cache
    // Collect all the state for Regions
    // Close the cache,
    // loop trying to connect, waiting before each attempt
    //
    // If reconnecting for lost-roles the reconnected system's cache will decide
    // whether the reconnected system should stay up. After max-tries we will
    // give up.
    //
    // If reconnecting for forced-disconnect we ignore max-tries and keep attempting
    // to join the distributed system until successful

    attemptingToReconnect = true;
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids == null) {
      ids = this;
    }

    // first save the current cache description. This is created by
    // the membership manager when forced-disconnect starts. If we're
    // reconnecting for lost roles then this will be null
    String cacheXML = null;
    List<CacheServerCreation> cacheServerCreation = null;

    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      cacheXML = cache.getCacheConfig().getCacheXMLDescription();
      cacheServerCreation = cache.getCacheConfig().getCacheServerCreation();
    }

    DistributionConfig oldConfig = ids.getConfig();
    Properties configProps = config.toProperties();
    configProps.putAll(config.toSecurityProperties());

    int timeOut = oldConfig.getMaxWaitTimeForReconnect();
    int memberTimeout = oldConfig.getMemberTimeout();
    // we need to make sure that a surviving member is able
    // to take over coordination before trying to auto-reconnect.
    // failure detection can take 4 member-timeout intervals
    // so we set that as a minimum. (suspect, check suspect, final check, send new view)
    final int intervalsAllowedForFailureDetection = 4;
    timeOut = Math.max(timeOut, memberTimeout * intervalsAllowedForFailureDetection);

    int maxTries = oldConfig.getMaxNumReconnectTries();

    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (Thread.currentThread().getName().equals("DisconnectThread")) {
      if (isDebugEnabled) {
        logger.debug("changing thread name to ReconnectThread");
      }
      Thread.currentThread().setName("ReconnectThread");
    }

    // get the membership manager for quorum checks
    Distribution mbrMgr = dm.getDistribution();
    quorumChecker = mbrMgr.getQuorumChecker();
    if (logger.isDebugEnabled()) {
      if (quorumChecker == null) {
        logger.debug("No quorum checks will be performed during reconnect attempts");
      } else {
        logger.debug("Initialized quorum checking service: {}", quorumChecker);
      }
    }

    // LOG:CLEANUP: deal with reconnect and INHIBIT_DM_BANNER -- this should be ok now
    String appendToLogFile = System.getProperty(APPEND_TO_LOG_FILE);
    if (appendToLogFile == null) {
      System.setProperty(APPEND_TO_LOG_FILE, "true");
    }
    String inhibitBanner = System.getProperty(InternalLocator.INHIBIT_DM_BANNER);
    if (inhibitBanner == null) {
      System.setProperty(InternalLocator.INHIBIT_DM_BANNER, "true");
    }
    if (forcedDisconnect) {
      systemAttemptingReconnect = this;
    }
    try {
      while (reconnectDS == null || !reconnectDS.isConnected()) {
        if (isReconnectCancelled()) {
          break;
        }

        if (!forcedDisconnect) {
          if (isDebugEnabled) {
            logger.debug("Max number of tries : {} and max time out : {}", maxTries, timeOut);
          }
          if (reconnectAttemptCounter.get() >= maxTries) {
            if (isDebugEnabled) {
              logger.debug(
                  "Stopping the checkrequiredrole thread because reconnect : {} reached the max "
                      + "number of reconnect tries : {}",
                  reconnectAttemptCounter, maxTries);
            }
            InternalCache internalCache = dm.getCache();
            if (internalCache == null) {
              throw new CacheClosedException(
                  "Some required roles missing");
            } else {
              throw internalCache.getCacheClosedException(
                  "Some required roles missing");
            }
          }
        }

        reconnectAttemptCounter.getAndIncrement();

        if (isReconnectCancelled()) {
          return;
        }

        logger.info("Disconnecting old DistributedSystem to prepare for a reconnect attempt");

        try {
          disconnect(true, reason, false);
        } catch (Exception ee) {
          logger.warn("Exception disconnecting for reconnect", ee);
        }

        TypeRegistry.init();

        try {
          reconnectLock.wait(timeOut);
        } catch (InterruptedException e) {
          logger.warn("Waiting thread for reconnect got interrupted.");
          Thread.currentThread().interrupt();
          return;
        }

        if (isReconnectCancelled()) {
          return;
        }

        logger.info(
            "Attempting to reconnect to the distributed system.  This is attempt #{}.",
            reconnectAttemptCounter);

        int saveNumberOfTries = reconnectAttemptCounter.get();
        try {
          // notify listeners of each attempt and then again after successful
          notifyReconnectListeners(this, reconnectDS, true);

          if (locatorDMTypeForced) {
            System.setProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE, "true");
          }

          configProps.put(DistributionConfig.DS_RECONNECTING_NAME, Boolean.TRUE);
          if (quorumChecker != null) {
            configProps.put(DistributionConfig.DS_QUORUM_CHECKER_NAME, quorumChecker);
          }

          InternalDistributedSystem newDS = null;
          if (isReconnectCancelled()) {
            return;
          }

          try {

            newDS = connectInternal(configProps, null, metricsService.getRebuilder(),
                membershipLocator);

          } catch (CancelException e) {
            if (isReconnectCancelled()) {
              return;
            } else {
              throw e;
            }
          } finally {
            if (newDS == null && quorumChecker != null) {
              // make sure the quorum checker is listening for messages from former members
              quorumChecker.resume();
            }
          }

          if (reconnectCancelled) {
            newDS.disconnect();
            continue;
          }

          reconnectDS = newDS;
        } catch (SystemConnectException e) {
          logger.debug("Attempt to reconnect failed with SystemConnectException");

          if (e.getMessage().contains("Rejecting the attempt of a member using an older version")) {
            logger.warn("Exception occurred while trying to connect the system during reconnect",
                e);
            attemptingToReconnect = false;
            reconnectException = e;
            return;
          }
          logger.warn("Caught SystemConnectException in reconnect", e);
          continue;
        } catch (GemFireConfigException e) {
          logger.warn("Caught GemFireConfigException in reconnect", e);
          continue;
        } catch (Exception e) {
          logger.warn("Exception occurred while trying to connect the system during reconnect",
              e);
          attemptingToReconnect = false;
          reconnectException = e;
          return;
        } finally {
          if (locatorDMTypeForced) {
            System.getProperties().remove(InternalLocator.FORCE_LOCATOR_DM_TYPE);
          }
          reconnectAttemptCounter.set(saveNumberOfTries);
        }

        DistributionManager newDM = reconnectDS.getDistributionManager();
        if (newDM instanceof ClusterDistributionManager) {
          // Admin systems don't carry a cache, but for others we can now create
          // a cache
          if (newDM.getDMType() != ClusterDistributionManager.ADMIN_ONLY_DM_TYPE) {
            boolean retry;
            do {
              retry = false;
              try {
                cache = new InternalCacheBuilder()
                    .setCacheXMLDescription(cacheXML)
                    .create(reconnectDS);

                if (!cache.isClosed()) {
                  createAndStartCacheServers(cacheServerCreation, cache);
                  if (cache.getCachePerfStats().getReliableRegionsMissing() == 0) {
                    reconnectAttemptCounter.set(0);
                  }
                }

              } catch (GemFireConfigException e) {
                if (e.getCause() instanceof ClusterConfigurationNotAvailableException) {
                  retry = true;
                  logger.info("Reconnected to the cluster but the cluster configuration service "
                      + "isn't available - will retry creating the cache");
                  try {
                    Thread.sleep(5000);
                  } catch (InterruptedException e1) {
                    reconnectCancelled = true;
                    reconnectException = e;
                    break;
                  }
                }
              } catch (Exception e) {
                // We need to give up because we'll probably get the same exception in
                // the next attempt to build the cache.
                logger.warn(
                    "Exception occurred while trying to create the cache during reconnect.  "
                        + "Auto-reconnect is terminating.",
                    e);
                reconnectCancelled = true;
                reconnectException = e;
                break;
              }
            } while (retry);
          }
        }

        if (reconnectDS != null && reconnectDS.isConnected()) {
          // make sure the new DS and cache are stable before exiting this loop
          try {
            Thread.sleep(config.getMemberTimeout() * 3L);
          } catch (InterruptedException e) {
            logger.info("Reconnect thread has been interrupted - exiting");
            Thread.currentThread().interrupt();
            reconnectCancelled = true;
            reconnectException = e;
            return;
          }
        }
      } // while()

      if (isReconnectCancelled()) {
        if (reconnectDS != null) {
          reconnectDS.disconnect();
        }
      } else {
        reconnectDS.isReconnectingDS = false;
        if (reconnectDS.isConnected()) {
          notifyReconnectListeners(this, reconnectDS, false);
        }
      }

    } finally {
      systemAttemptingReconnect = null;
      attemptingToReconnect = false;
      if (appendToLogFile == null) {
        System.getProperties().remove(APPEND_TO_LOG_FILE);
      } else {
        System.setProperty(APPEND_TO_LOG_FILE, appendToLogFile);
      }
      if (inhibitBanner == null) {
        System.getProperties().remove(InternalLocator.INHIBIT_DM_BANNER);
      } else {
        System.setProperty(InternalLocator.INHIBIT_DM_BANNER, inhibitBanner);
      }
      dm.getDistribution().setReconnectCompleted(true);
      InternalDistributedSystem newds = reconnectDS;
      if (newds != null) {
        newds.getDM().getDistribution().setReconnectCompleted(true);
      }
      if (quorumChecker != null && (reconnectDS == null || !reconnectDS.isConnected())) {
        quorumChecker.close();
      }
    }

    if (isReconnectCancelled()) {
      logger.debug("reconnect can no longer be done because of an explicit disconnect");
      if (reconnectDS != null) {
        reconnectDS.disconnect();
      }
      attemptingToReconnect = false;
    } else if (reconnectDS != null && reconnectDS.isConnected()) {
      logger.info("Reconnect completed.\nNew DistributedSystem is {}\nNew Cache is {}", reconnectDS,
          cache);
    }
  }


  /**
   * after an auto-reconnect we may need to recreate a cache server and start it
   */
  public void createAndStartCacheServers(List<CacheServerCreation> cacheServerCreation,
      InternalCache cache) {

    List<CacheServer> servers = cache.getCacheServers();

    // if there used to be a cache server but now there isn't one we need
    // to recreate it.
    if (servers.isEmpty() && cacheServerCreation != null) {
      for (CacheServerCreation bridge : cacheServerCreation) {
        CacheServerImpl impl = (CacheServerImpl) cache.addCacheServer();
        impl.configureFrom(bridge);
      }
    }

    servers = cache.getCacheServers();
    for (CacheServer server : servers) {
      try {
        if (!server.isRunning()) {
          server.start();
        }
      } catch (IOException ex) {
        throw new GemFireIOException(
            String.format("While starting cache server %s", server),
            ex);
      }
    }

  }

  /**
   * Validates that the configuration provided is the same as the configuration for this
   * InternalDistributedSystem
   *
   * @param propsToCheck the Properties instance to compare with the existing Properties
   * @throws IllegalStateException when the configuration is not the same other returns
   */
  public void validateSameProperties(Properties propsToCheck, boolean isConnected) {
    if (!sameAs(propsToCheck, isConnected)) {
      StringBuilder sb = new StringBuilder();

      DistributionConfig wanted = DistributionConfigImpl.produce(propsToCheck);

      String[] validAttributeNames = originalConfig.getAttributeNames();
      for (String attName : validAttributeNames) {
        Object expectedAtt = wanted.getAttributeObject(attName);
        String expectedAttStr = expectedAtt.toString();
        Object actualAtt = originalConfig.getAttributeObject(attName);
        String actualAttStr = actualAtt.toString();
        sb.append("  ");
        sb.append(attName);
        sb.append("=\"");
        if (actualAtt.getClass().isArray()) {
          actualAttStr = arrayToString(actualAtt);
          expectedAttStr = arrayToString(expectedAtt);
        }

        sb.append(actualAttStr);
        sb.append("\"");
        if (!expectedAttStr.equals(actualAttStr)) {
          sb.append(" ***(wanted \"");
          sb.append(expectedAtt);
          sb.append("\")***");
        }

        sb.append("\n");
      }

      if (creationStack == null) {
        throw new IllegalStateException(
            String.format(
                "A connection to a distributed system already exists in this VM.  It has the "
                    + "following configuration:%s",
                sb.toString()));
      } else {
        throw new IllegalStateException(
            String.format(
                "A connection to a distributed system already exists in this VM.  It has the "
                    + "following configuration:%s",
                sb.toString()),
            creationStack);
      }
    }
  }

  public static String arrayToString(Object obj) {
    if (!obj.getClass().isArray()) {
      return "-not-array-object-";
    }
    StringBuilder buff = new StringBuilder("[");
    int arrayLength = Array.getLength(obj);
    for (int i = 0; i < arrayLength - 1; i++) {
      buff.append(Array.get(obj, i).toString());
      buff.append(",");
    }
    if (arrayLength > 0) {
      buff.append(Array.get(obj, arrayLength - 1).toString());
    }
    buff.append("]");

    return buff.toString();
  }

  public boolean isShareSockets() {
    return shareSockets;
  }

  public void setShareSockets(boolean shareSockets) {
    this.shareSockets = shareSockets;
  }

  /**
   * A listener that gets invoked whenever a connection is created to a distributed system
   */
  public interface ConnectListener {

    /**
     * Invoked after a connection to the distributed system is created
     */
    void onConnect(InternalDistributedSystem sys);
  }

  public boolean hasAlertListenerFor(DistributedMember member) {
    return hasAlertListenerFor(member, AlertLevel.WARNING.intLevel());
  }

  public boolean hasAlertListenerFor(DistributedMember member, int severity) {
    return alertingService.hasAlertListener(member, AlertLevel.find(severity));
  }

  /**
   * see {@link org.apache.geode.admin.AdminDistributedSystemFactory}
   *
   * @since GemFire 5.7
   */
  public static void setEnableAdministrationOnly(boolean adminOnly) {
    DistributedSystem.setEnableAdministrationOnly(adminOnly);
  }

  public static void setCommandLineAdmin(boolean adminOnly) {
    DistributedSystem.setEnableAdministrationOnly(adminOnly);
  }

  @Override
  public boolean waitUntilReconnected(long time, TimeUnit units) throws InterruptedException {
    int sleepTime = 1000;
    long endTime = System.currentTimeMillis();
    if (time < 0) {
      endTime = Long.MAX_VALUE;
    } else {
      endTime += TimeUnit.MILLISECONDS.convert(time, units);
    }
    synchronized (reconnectLock) {
      while (isReconnecting()) {
        if (reconnectCancelled) {
          break;
        }
        if (time != 0) {
          reconnectLock.wait(sleepTime);
        }
        if (time == 0 || System.currentTimeMillis() > endTime) {
          break;
        }
      }

      if (reconnectException != null) {
        throw new DistributedSystemDisconnectedException(
            "Reconnect attempts terminated due to exception", reconnectException);
      }
      InternalDistributedSystem recon = reconnectDS;
      return !attemptingToReconnect && recon != null && recon.isConnected();
    }
  }

  @Override
  public DistributedSystem getReconnectedSystem() {
    return reconnectDS;
  }

  @Override
  public void stopReconnecting() {
    reconnectCancelled = true;
    synchronized (reconnectLock) {
      reconnectLock.notify();
    }
    disconnect(false, "stopReconnecting was invoked", false);
    attemptingToReconnect = false;
  }

  public void stopReconnectingNoDisconnect() {
    reconnectCancelled = true;
    synchronized (reconnectLock) {
      reconnectLock.notify();
    }
    attemptingToReconnect = false;
  }

  /**
   * Provides hook for dunit to generate and store a detailed creation stack trace that includes the
   * keys/values of DistributionConfig including security related attributes without introducing
   * Privacy Violations that Fortify will complain about.
   * </p>
   */
  public interface CreationStackGenerator {

    Throwable generateCreationStack(final DistributionConfig config);
  }

  public void setCache(InternalCache instance) {
    dm.setCache(instance);
  }

  public InternalCache getCache() {
    return dm == null ? null : dm.getCache();
  }

  private static StatisticsManagerFactory defaultStatisticsManagerFactory() {
    return (name, startTime, statsDisabled) -> {
      if (statsDisabled) {
        return new DummyStatisticsRegistry(name, startTime);
      } else {
        return new StatisticsRegistry(name, startTime);
      }
    };
  }

  public static class Builder {

    private final Properties configProperties;

    private SecurityConfig securityConfig;
    private MetricsService.Builder metricsServiceBuilder;

    private MembershipLocator<InternalDistributedMember> locator;

    public Builder(Properties configProperties, MetricsService.Builder metricsServiceBuilder) {
      this.configProperties = configProperties;
      this.metricsServiceBuilder = metricsServiceBuilder;
    }

    public Builder setSecurityConfig(SecurityConfig securityConfig) {
      this.securityConfig = securityConfig;
      return this;
    }

    public Builder setLocator(
        final MembershipLocator<InternalDistributedMember> locator) {
      this.locator = locator;
      return this;
    }

    /**
     * Builds and initializes new instance of InternalDistributedSystem.
     */
    public InternalDistributedSystem build() {
      if (securityConfig == null) {
        securityConfig = new SecurityConfig(null, null);
      }

      boolean stopThreads = true;
      InternalDataSerializer.checkSerializationVersion();
      try {
        SystemFailure.startThreads();
        InternalDistributedSystem newSystem =
            new InternalDistributedSystem(new ConnectionConfigImpl(
                configProperties), defaultStatisticsManagerFactory(),
                FunctionStatsManager::new);
        newSystem
            .initialize(securityConfig.getSecurityManager(), securityConfig.getPostProcessor(),
                metricsServiceBuilder, locator);
        notifyConnectListeners(newSystem);
        stopThreads = false;
        return newSystem;
      } finally {
        if (stopThreads) {
          SystemFailure.stopThreads();
        }
      }
    }
  }

  @VisibleForTesting
  public static class BuilderForTesting {

    private final Properties configProperties;

    private DistributionManager distributionManager;
    private StatisticsManagerFactory statisticsManagerFactory = defaultStatisticsManagerFactory();

    public BuilderForTesting(Properties configProperties) {
      this.configProperties = configProperties;
    }

    public BuilderForTesting setDistributionManager(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
      return this;
    }

    public BuilderForTesting setStatisticsManagerFactory(
        StatisticsManagerFactory statisticsManagerFactory) {
      this.statisticsManagerFactory = statisticsManagerFactory;
      return this;
    }

    /**
     * Builds instance without initializing it for testing.
     */
    public InternalDistributedSystem build() {
      ConnectionConfigImpl connectionConfig = new ConnectionConfigImpl(configProperties);

      InternalDistributedSystem internalDistributedSystem =
          new InternalDistributedSystem(connectionConfig, statisticsManagerFactory,
              FunctionStatsManager::new);

      internalDistributedSystem.config =
          new RuntimeDistributionConfigImpl(internalDistributedSystem);
      internalDistributedSystem.dm = distributionManager;
      internalDistributedSystem.isConnected = true;

      return internalDistributedSystem;
    }
  }
}
