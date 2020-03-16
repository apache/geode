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
package org.apache.geode.admin.internal;

import static org.apache.geode.admin.internal.InetAddressUtilsWithLogging.toInetAddress;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.logging.log4j.Logger;
import org.jgroups.annotations.GuardedBy;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.Alert;
import org.apache.geode.admin.AlertLevel;
import org.apache.geode.admin.AlertListener;
import org.apache.geode.admin.BackupStatus;
import org.apache.geode.admin.CacheServer;
import org.apache.geode.admin.CacheServerConfig;
import org.apache.geode.admin.CacheVm;
import org.apache.geode.admin.ConfigurationParameter;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.DistributionLocator;
import org.apache.geode.admin.DistributionLocatorConfig;
import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.ManagedEntity;
import org.apache.geode.admin.ManagedEntityConfig;
import org.apache.geode.admin.OperationCancelledException;
import org.apache.geode.admin.RuntimeAdminException;
import org.apache.geode.admin.SystemMember;
import org.apache.geode.admin.SystemMemberCacheListener;
import org.apache.geode.admin.SystemMembershipEvent;
import org.apache.geode.admin.SystemMembershipListener;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.FutureCancelledException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.admin.ApplicationVM;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.GfManagerAgent;
import org.apache.geode.internal.admin.GfManagerAgentConfig;
import org.apache.geode.internal.admin.GfManagerAgentFactory;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.admin.remote.CompactRequest;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.admin.remote.MissingPersistentIDsRequest;
import org.apache.geode.internal.admin.remote.PrepareRevokePersistentIDRequest;
import org.apache.geode.internal.admin.remote.RemoteApplicationVM;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.admin.remote.RevokePersistentIDRequest;
import org.apache.geode.internal.admin.remote.ShutdownAllRequest;
import org.apache.geode.internal.cache.backup.BackupOperation;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.logging.Banner;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogWriterFactory;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.util.concurrent.FutureResult;
import org.apache.geode.logging.internal.LoggingSession;
import org.apache.geode.logging.internal.NullLoggingSession;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Represents a GemFire distributed system for remote administration/management.
 *
 * @since GemFire 3.5
 */
public class AdminDistributedSystemImpl implements org.apache.geode.admin.AdminDistributedSystem,
    org.apache.geode.internal.admin.JoinLeaveListener,
    org.apache.geode.internal.admin.AlertListener,
    org.apache.geode.distributed.internal.InternalDistributedSystem.DisconnectListener {

  private static final Logger logger = LogService.getLogger();

  /** String identity of this distributed system */
  private String id;

  /** Latest alert broadcast by any system members */
  private Alert latestAlert;

  // -------------------------------------------------------------------------

  /** Internal admin agent to delegate low-level work to */
  private volatile GfManagerAgent gfManagerAgent;

  /** Monitors the health of this distributed system */
  private GemFireHealth health;

  /** Set of non-Manager members in this system */
  private final Set applicationSet = new HashSet();

  /** Set of DistributionLocators for this system */
  private final Set locatorSet = new HashSet();

  /** Set of dedicated CacheServer members in this system */
  private final Set cacheServerSet = new HashSet();

  /** Configuration defining this distributed system */
  private final DistributedSystemConfigImpl config;

  /** Controller for starting and stopping managed entities */
  private ManagedEntityController controller;

  /** Log file collator for gathering and merging system member logs */
  private LogCollator logCollator = new LogCollator();

  /**
   * The level above which alerts will be delivered to the alert listeners
   */
  private AlertLevel alertLevel = AlertLevel.WARNING;

  /** The alert listeners registered on this distributed system. */
  private volatile Set<AlertListener> alertListeners = Collections.emptySet();
  private final Object alertLock = new Object();

  private InternalLogWriter logWriter;

  /** The membership listeners registered on this distributed system */
  private volatile Set membershipListeners = Collections.EMPTY_SET;
  private final Object membershipLock = new Object();

  /* The region listeners registered on this distributed system */
  // for feature requests #32887
  private volatile List cacheListeners = Collections.EMPTY_LIST;
  private final Object cacheListLock = new Object();

  private final LoggingSession loggingSession;

  /**
   * reference to AdminDistributedSystemImpl instance for feature requests #32887.
   * <p>
   * Guarded by {@link #CONNECTION_SYNC}.
   * <p>
   * TODO: reimplement this change and SystemMemberCacheEventProcessor to avoid using this static.
   * SystemMemberCacheEvents should only be sent to Admin VMs that express interest.
   * <p>
   * This is volatile to allow SystemFailure to deliver fatal poison-pill to thisAdminDS without
   * waiting on synchronization.
   *
   */
  @GuardedBy("CONNECTION_SYNC")
  @MakeNotStatic
  private static volatile AdminDistributedSystemImpl thisAdminDS;

  /**
   * Provides synchronization for {@link #connect()} and {@link #disconnect()}. {@link #thisAdminDS}
   * is also now protected by CONNECTION_SYNC and has its lifecycle properly tied to
   * connect/disconnect.
   */
  private static final Object CONNECTION_SYNC = new Object();


  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  private static LoggingSession createLoggingSession() {
    return NullLoggingSession.create();
  }

  /**
   * Constructs new DistributedSystemImpl with the given configuration.
   *
   * @param config configuration defining this distributed system
   */
  public AdminDistributedSystemImpl(DistributedSystemConfigImpl config) {
    loggingSession = createLoggingSession();

    // init from config...
    this.config = config;

    String systemId = this.config.getSystemId();
    if (systemId != null && systemId.length() > 0) {
      this.id = systemId;

    }
    if (this.getLocators() != null && this.getLocators().length() > 0) {
      this.id = this.getLocators();

    } else {
      this.id = new StringBuffer(this.getMcastAddress()).append("[").append(this.getMcastPort())
          .append("]").toString();
    }

    // LOG: create LogWriterAppender unless one already exists
    loggingSession.startSession();

    // LOG: look in DistributedSystemConfigImpl for existing LogWriter to use
    InternalLogWriter existingLogWriter = this.config.getInternalLogWriter();
    if (existingLogWriter != null) {
      this.logWriter = existingLogWriter;
    } else {
      // LOG: create LogWriterLogger
      this.logWriter = LogWriterFactory.createLogWriterLogger(this.config.createLogConfig(), false);
      if (!Boolean.getBoolean(InternalLocator.INHIBIT_DM_BANNER)) {
        // LOG: changed statement from config to info
        this.logWriter.info(new Banner().getString());
      } else {
        logger.debug("skipping banner - " + InternalLocator.INHIBIT_DM_BANNER + " is set to true");
      }
      // Set this log writer in DistributedSystemConfigImpl
      this.config.setInternalLogWriter(this.logWriter);
    }

    // set up other details that depend on config attrs...
    this.controller = ManagedEntityControllerFactory.createManagedEntityController(this);
    initializeDistributionLocators();
    initializeCacheServers();
  }

  // -------------------------------------------------------------------------
  // Initialization
  // -------------------------------------------------------------------------

  /**
   * Creates DistributionLocator instances for every locator entry in the
   * {@link org.apache.geode.admin.DistributedSystemConfig}
   */
  private void initializeDistributionLocators() {
    DistributionLocatorConfig[] configs = this.config.getDistributionLocatorConfigs();
    if (configs.length == 0) {
      // No work to do
      return;
    }

    for (int i = 0; i < configs.length; i++) {
      // the Locator impl may vary in this class from the config...
      DistributionLocatorConfig conf = configs[i];
      DistributionLocator locator = createDistributionLocatorImpl(conf);
      this.locatorSet.add(new FutureResult(locator));
    }
    // update locators string...
    setLocators(parseLocatorSet());
  }

  /**
   * Creates <code>CacheServer</code> instances for every cache server entry in the
   * {@link org.apache.geode.admin.DistributedSystemConfig}
   */
  private void initializeCacheServers() {
    CacheServerConfig[] cacheServerConfigs = this.config.getCacheServerConfigs();
    for (int i = 0; i < cacheServerConfigs.length; i++) {
      try {
        CacheServerConfig conf = cacheServerConfigs[i];
        CacheServerConfigImpl copy = new CacheServerConfigImpl(conf);
        this.cacheServerSet.add(new FutureResult(createCacheServer(copy)));
      } catch (java.lang.Exception e) {
        logger.warn(e.getMessage(), e);
        continue;
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (java.lang.Error e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.error(e.getMessage(), e);
        continue;
      }
    }
  }

  /**
   * Checks to make sure that {@link #connect()} has been called.
   *
   * @throws IllegalStateException If {@link #connect()} has not been called.
   */
  private void checkConnectCalled() {
    if (this.gfManagerAgent == null) {
      throw new IllegalStateException(
          "connect() has not been invoked on this AdminDistributedSystem.");
    }
  }

  // -------------------------------------------------------------------------
  // Attributes of this DistributedSystem
  // -------------------------------------------------------------------------

  public GfManagerAgent getGfManagerAgent() {
    return this.gfManagerAgent;
  }

  @Override
  public boolean isConnected() {
    return this.gfManagerAgent != null && this.gfManagerAgent.isConnected();
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public String getName() {
    String name = this.config.getSystemName();
    if (name != null && name.length() > 0) {
      return name;

    } else {
      return getId();
    }
  }

  public String getSystemName() {
    return this.config.getSystemName();
  }

  @Override
  public String getRemoteCommand() {
    return this.config.getRemoteCommand();
  }

  @Override
  public void setRemoteCommand(String remoteCommand) {
    this.config.setRemoteCommand(remoteCommand);
  }

  @Override
  public void setAlertLevel(AlertLevel level) {
    if (this.isConnected()) {
      this.gfManagerAgent.setAlertLevel(level.getSeverity());
    }

    this.alertLevel = level;
  }

  @Override
  public AlertLevel getAlertLevel() {
    return this.alertLevel;
  }

  @Override
  public void addAlertListener(AlertListener listener) {
    synchronized (this.alertLock) {
      Set<AlertListener> oldListeners = this.alertListeners;
      if (!oldListeners.contains(listener)) {
        Set<AlertListener> newListeners = new HashSet<AlertListener>(oldListeners);
        newListeners.add(listener);
        this.alertListeners = newListeners;
      }
    }
  }

  public int getAlertListenerCount() {
    synchronized (this.alertLock) {
      return this.alertListeners.size();
    }
  }

  @Override
  public void removeAlertListener(AlertListener listener) {
    synchronized (this.alertLock) {
      Set<AlertListener> oldListeners = this.alertListeners;
      if (oldListeners.contains(listener)) { // fixed bug 34687
        Set<AlertListener> newListeners = new HashSet<AlertListener>(oldListeners);
        if (newListeners.remove(listener)) {
          this.alertListeners = newListeners;
        }
      }
    }
  }

  @Override
  public void addMembershipListener(SystemMembershipListener listener) {
    synchronized (this.membershipLock) {
      Set oldListeners = this.membershipListeners;
      if (!oldListeners.contains(listener)) {
        Set newListeners = new HashSet(oldListeners);
        newListeners.add(listener);
        this.membershipListeners = newListeners;
      }
    }
  }

  @Override
  public void removeMembershipListener(SystemMembershipListener listener) {
    synchronized (this.membershipLock) {
      Set oldListeners = this.membershipListeners;
      if (oldListeners.contains(listener)) { // fixed bug 34687
        Set newListeners = new HashSet(oldListeners);
        if (newListeners.remove(listener)) {
          this.membershipListeners = newListeners;
        }
      }
    }
  }

  @Override
  public String getMcastAddress() {
    return this.config.getMcastAddress();
  }

  @Override
  public int getMcastPort() {
    return this.config.getMcastPort();
  }

  public boolean getDisableTcp() {
    return this.config.getDisableTcp();
  }

  public boolean getDisableAutoReconnect() {
    return this.config.getDisableAutoReconnect();
  }

  @Override
  public String getLocators() {
    return this.config.getLocators();
  }

  protected void setLocators(String locators) {
    this.config.setLocators(locators);
  }

  public String getMembershipPortRange() {
    return this.getConfig().getMembershipPortRange();
  }

  /** get the direct-channel port to use, or zero if not set */
  public int getTcpPort() {
    return this.getConfig().getTcpPort();
  }

  public void setTcpPort(int port) {
    this.getConfig().setTcpPort(port);
  }

  public void setMembershipPortRange(String membershipPortRange) {
    this.getConfig().setMembershipPortRange(membershipPortRange);
  }

  @Override
  public DistributedSystemConfig getConfig() {
    return this.config;
  }

  /**
   * Returns true if any members of this system are currently running.
   */
  @Override
  public boolean isRunning() {
    if (this.gfManagerAgent == null)
      return false;
    // is there a better way??
    // this.gfManagerAgent.isConnected() ... this.gfManagerAgent.isListening()

    if (isAnyMemberRunning())
      return true;
    return false;
  }

  /** Returns true if this system can use multicast for communications */
  @Override
  public boolean isMcastEnabled() {
    return this.getMcastPort() > 0;
  }

  ManagedEntityController getEntityController() {
    return this.controller;
  }

  private static final String TIMEOUT_MS_NAME = "AdminDistributedSystemImpl.TIMEOUT_MS";
  private static final int TIMEOUT_MS_DEFAULT = 60000; // 30000 -- see bug36470
  private static final int TIMEOUT_MS =
      Integer.getInteger(TIMEOUT_MS_NAME, TIMEOUT_MS_DEFAULT).intValue();


  // -------------------------------------------------------------------------
  // Operations of this DistributedSystem
  // -------------------------------------------------------------------------

  /**
   * Starts all managed entities in this system.
   */
  @Override
  public void start() throws AdminException {
    // Wait for each managed entity to start (see bug 32569)
    DistributionLocator[] locs = getDistributionLocators();
    for (int i = 0; i < locs.length; i++) {
      locs[i].start();
    }
    for (int i = 0; i < locs.length; i++) {
      try {
        if (!locs[i].waitToStart(TIMEOUT_MS)) {
          throw new AdminException(
              String.format("%s did not start after %s ms",
                  new Object[] {locs[i], Integer.valueOf(TIMEOUT_MS)}));
        }

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new AdminException(
            String.format("Interrupted while waiting for %s to start.",
                locs[i]),
            ex);
      }
    }

    CacheServer[] servers = getCacheServers();
    for (int i = 0; i < servers.length; i++) {
      servers[i].start();
    }
    for (int i = 0; i < servers.length; i++) {
      try {
        if (!servers[i].waitToStart(TIMEOUT_MS)) {
          throw new AdminException(
              String.format("%s did not start after %s ms",
                  new Object[] {servers[i], Integer.valueOf(TIMEOUT_MS)}));
        }

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new AdminException(
            String.format("Interrupted while waiting for %s to start.",
                servers[i]),
            ex);
      }
    }
  }

  /**
   * Stops all GemFire managers that are members of this system.
   */
  @Override
  public void stop() throws AdminException {
    // Stop cache server before GemFire managers because the cache
    // server might host a cache proxy that is dependent on the
    // manager. See bug 32569.

    // Wait for each managed entity to stop (see bug 32569)
    long timeout = 30;

    CacheServer[] servers = getCacheServers();
    for (int i = 0; i < servers.length; i++) {
      servers[i].stop();
    }
    for (int i = 0; i < servers.length; i++) {
      try {
        if (!servers[i].waitToStop(timeout * 1000)) {
          throw new AdminException(
              String.format("%s did not stop after %s seconds.",
                  new Object[] {servers[i], Long.valueOf(timeout)}));
        }

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new AdminException(
            String.format("Interrupted while waiting for %s to stop.",
                servers[i]),
            ex);
      }
    }

    DistributionLocator[] locs = getDistributionLocators();
    for (int i = 0; i < locs.length; i++) {
      locs[i].stop();
    }
    for (int i = 0; i < locs.length; i++) {
      try {
        if (!locs[i].waitToStop(timeout * 1000)) {
          throw new AdminException(
              String.format("%s did not stop after %s seconds.",
                  new Object[] {locs[i], Long.valueOf(timeout)}));
        }

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new AdminException(
            String.format("Interrupted while waiting for %s to stop.",
                locs[i]),
            ex);
      }
    }
  }

  /** Display merged system member logs */
  @Override
  public String displayMergedLogs() {
    return this.logCollator.collateLogs(this.gfManagerAgent);
  }

  /**
   * Returns the license for this GemFire product; else null if unable to retrieve license
   * information
   *
   * @return license for this GemFire product
   */
  @Override
  public java.util.Properties getLicense() {
    SystemMember member = findFirstRunningMember();
    if (member != null) {
      return new Properties();
    } else {
      return null;
    }
  }

  /**
   * Sets the distribution-related portion of the given managed entity's configuration so that the
   * entity is part of this distributed system.
   *
   * @throws AdminException TODO-javadocs
   */
  private void setDistributionParameters(SystemMember member) throws AdminException {

    Assert.assertTrue(member instanceof ManagedSystemMemberImpl);

    // set some config parms to match this system...
    ConfigurationParameter[] configParms = new ConfigurationParameter[] {
        new ConfigurationParameterImpl(MCAST_PORT, Integer.valueOf(this.config.getMcastPort())),
        new ConfigurationParameterImpl(LOCATORS, this.config.getLocators()),
        new ConfigurationParameterImpl(MCAST_ADDRESS, toInetAddress(this.config.getMcastAddress())),
        new ConfigurationParameterImpl(DISABLE_TCP, Boolean.valueOf(this.config.getDisableTcp())),};
    member.setConfiguration(configParms);
  }

  /**
   * Handles an <code>ExecutionException</code> by examining its cause and throwing an appropriate
   * runtime exception.
   */
  private static void handle(ExecutionException ex) {
    Throwable cause = ex.getCause();

    if (cause instanceof OperationCancelledException) {
      // Operation was cancelled, we don't necessary want to propagate
      // this up to the user.
      return;
    }
    if (cause instanceof CancelException) { // bug 37285
      throw new FutureCancelledException(
          "Future cancelled due to shutdown",
          ex);
    }

    // Don't just throw the cause because the stack trace can be
    // misleading. For instance, the cause might have occurred in a
    // different thread. In addition to the cause, we also want to
    // know which code was waiting for the Future.
    throw new RuntimeAdminException(
        "While waiting for Future",
        ex);
  }

  protected void checkCancellation() {
    DistributionManager dm = this.getDistributionManager();
    // TODO does dm == null mean we're dead?
    if (dm != null) {
      dm.getCancelCriterion().checkCancelInProgress(null);
    }
  }

  /**
   * Returns a list of manageable SystemMember instances for each member of this distributed system.
   *
   * @return array of system members for each non-manager member
   */
  @Override
  public SystemMember[] getSystemMemberApplications() throws org.apache.geode.admin.AdminException {
    synchronized (this.applicationSet) {
      Collection coll = new ArrayList(this.applicationSet.size());
      APPS: for (Iterator iter = this.applicationSet.iterator(); iter.hasNext();) {
        Future future = (Future) iter.next();
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            coll.add(future.get());
            break;
          } catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } catch (CancellationException ex) {
            continue APPS;
          } catch (ExecutionException ex) {
            handle(ex);
            continue APPS;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for
      } // APPS
      SystemMember[] array = new SystemMember[coll.size()];
      coll.toArray(array);
      return array;
    }
  }

  /**
   * Display in readable format the latest Alert in this distributed system.
   *
   * TODO: create an external admin api object for Alert
   */
  @Override
  public String getLatestAlert() {
    if (this.latestAlert == null) {
      return "";
    }
    return this.latestAlert.toString();
  }

  /**
   * Connects to the currently configured system.
   */
  @Override
  public void connect() {
    connect(this.logWriter);
  }

  /**
   * Connects to the currently configured system. This method is public for internal use only
   * (testing, for example).
   *
   * <p>
   *
   * See {@link org.apache.geode.distributed.DistributedSystem#connect} for a list of exceptions
   * that may be thrown.
   *
   * @param logWriter the InternalLogWriter to use for any logging
   */
  public void connect(InternalLogWriter logWriter) {
    synchronized (CONNECTION_SYNC) {
      // Check if the gfManagerAgent is NOT null.
      // If it is already listening, then just return since the connection is already established OR
      // in process.
      // Otherwise cleanup the state of AdminDistributedSystemImpl. This needs to happen
      // automatically.
      if (this.gfManagerAgent != null) {
        if (this.gfManagerAgent.isListening()) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "The RemoteGfManagerAgent is already listening for this AdminDistributedSystem.");
          }
          return;
        }
        this.disconnect();
      }

      if (thisAdminDS != null) { // TODO: beef up toString and add thisAdminDS
        throw new IllegalStateException(
            "Only one AdminDistributedSystem connection can be made at once.");
      }

      thisAdminDS = this; // added for feature requests #32887

      if (this.getLocators().length() == 0) {
        this.id = this.getMcastAddress() + "[" + this.getMcastPort() + "]";

      } else {
        this.id = this.getLocators();
      }

      if (this.config instanceof DistributedSystemConfigImpl) {
        ((DistributedSystemConfigImpl) this.config).validate();
        ((DistributedSystemConfigImpl) this.config).setDistributedSystem(this);
      }

      // LOG: passes the AdminDistributedSystemImpl LogWriterLogger into GfManagerAgentConfig for
      // RemoteGfManagerAgent
      GfManagerAgent agent = GfManagerAgentFactory.getManagerAgent(buildAgentConfig(logWriter));
      this.gfManagerAgent = agent;

      // sync to prevent bug 33341 Admin API can double-represent system members
      synchronized (this.membershipListenerLock) {
        // build the list of applications...
        ApplicationVM[] apps = this.gfManagerAgent.listApplications();
        for (int i = 0; i < apps.length; i++) {
          try {
            nodeJoined(null, apps[i]);
          } catch (RuntimeAdminException e) {
            this.logWriter.warning("encountered a problem processing member " + apps[i]);
          }
        }
      }

      // Build admin objects for all locators (see bug 31959)
      String locators = this.getLocators();
      StringTokenizer st = new StringTokenizer(locators, ",");
      NEXT: while (st.hasMoreTokens()) {
        String locator = st.nextToken();
        int first = locator.indexOf("[");
        int last = locator.indexOf("]");
        String host = locator.substring(0, first);
        int colidx = host.lastIndexOf('@');
        if (colidx < 0) {
          colidx = host.lastIndexOf(':');
        }
        String bindAddr = null;
        if (colidx > 0 && colidx < (host.length() - 1)) {
          String orig = host;
          bindAddr = host.substring(colidx + 1, host.length());
          host = host.substring(0, colidx);
          // if the host contains a colon and there's no '@', we probably
          // parsed an ipv6 address incorrectly - try again
          if (host.indexOf(':') >= 0) {
            int bindidx = orig.lastIndexOf('@');
            if (bindidx >= 0) {
              host = orig.substring(0, bindidx);
              bindAddr = orig.substring(bindidx + 1);
            } else {
              host = orig;
              bindAddr = null;
            }
          }
        }
        int port = Integer.parseInt(locator.substring(first + 1, last));

        synchronized (this.locatorSet) {
          LOCATORS: for (Iterator iter = this.locatorSet.iterator(); iter.hasNext();) {
            Future future = (Future) iter.next();
            DistributionLocatorImpl impl = null;
            for (;;) {
              checkCancellation();
              boolean interrupted = Thread.interrupted();
              try {
                impl = (DistributionLocatorImpl) future.get();
                break; // success
              } catch (InterruptedException ex) {
                interrupted = true;
                continue; // keep trying
              } catch (CancellationException ex) {
                continue LOCATORS;
              } catch (ExecutionException ex) {
                handle(ex);
                continue LOCATORS;
              } finally {
                if (interrupted) {
                  Thread.currentThread().interrupt();
                }
              }
            } // for

            DistributionLocatorConfig conf = impl.getConfig();

            InetAddress host1 = toInetAddress(host);
            InetAddress host2 = toInetAddress(conf.getHost());
            if (port == conf.getPort() && host1.equals(host2)) {
              // Already have an admin object for this locator
              continue NEXT;
            }
          }
        }

        // None of the existing locators matches the locator in the
        // string. Contact the locator to get information and create
        // an admin object for it.
        InetAddress bindAddress = null;
        if (bindAddr != null) {
          bindAddress = toInetAddress(bindAddr);
        }
        DistributionLocatorConfig conf =
            DistributionLocatorConfigImpl.createConfigFor(host, port, bindAddress);
        if (conf != null) {
          DistributionLocator impl = createDistributionLocatorImpl(conf);
          synchronized (this.locatorSet) {
            this.locatorSet.add(new FutureResult(impl));
          }
        }
      }
    }
  }

  /**
   * Polls to determine whether or not the connection to the distributed system has been made.
   */
  @Override
  public boolean waitToBeConnected(long timeout) throws InterruptedException {

    if (Thread.interrupted())
      throw new InterruptedException();

    checkConnectCalled();

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      if (this.gfManagerAgent.isInitialized()) {
        return true;

      } else {
        Thread.sleep(100);
      }
    }

    return this.isConnected();
  }

  /**
   * Closes all connections and resources to the connected distributed system.
   *
   * @see org.apache.geode.distributed.DistributedSystem#disconnect()
   */
  @Override
  public void disconnect() {
    synchronized (CONNECTION_SYNC) {
      loggingSession.stopSession();
      try {
        if (thisAdminDS == this) {
          thisAdminDS = null;
        }
        if (this.gfManagerAgent != null && this.gfManagerAgent.isListening()) {
          synchronized (this) {
            if (this.health != null) {
              this.health.close();
            }
          }
          this.gfManagerAgent.removeJoinLeaveListener(this);
          this.gfManagerAgent.disconnect();
        }
        this.gfManagerAgent = null;
        if (this.config instanceof DistributedSystemConfigImpl) {
          ((DistributedSystemConfigImpl) this.config).setDistributedSystem(null);
        }
      } finally {
        loggingSession.shutdown();
      }
    }
  }

  /**
   * Returns the DistributionManager this implementation is using to connect to the distributed
   * system.
   */
  public DistributionManager getDistributionManager() {
    GfManagerAgent agent = gfManagerAgent;
    if (agent == null) {
      return null;
    }
    return agent.getDM();
  }

  /**
   * Returns the internal admin API's agent used for administering this
   * <code>AdminDistributedSystem</code>.
   *
   * @since GemFire 4.0
   */
  public GfManagerAgent getAdminAgent() {
    return this.gfManagerAgent;
  }

  /**
   * Adds a new, unstarted <code>DistributionLocator</code> to this distributed system.
   */
  @Override
  public DistributionLocator addDistributionLocator() {
    DistributionLocatorConfig conf = new DistributionLocatorConfigImpl();
    DistributionLocator locator = createDistributionLocatorImpl(conf);
    synchronized (this.locatorSet) {
      this.locatorSet.add(new FutureResult(locator));
    }

    // update locators string...
    setLocators(parseLocatorSet());
    return locator;
  }

  @Override
  public DistributionLocator[] getDistributionLocators() {
    synchronized (this.locatorSet) {
      Collection coll = new ArrayList(this.locatorSet.size());
      LOCATORS: for (Iterator iter = this.locatorSet.iterator(); iter.hasNext();) {
        Future future = (Future) iter.next();
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            coll.add(future.get());
            break; // success
          } catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } catch (CancellationException ex) {
            continue LOCATORS;
          } catch (ExecutionException ex) {
            handle(ex);
            continue LOCATORS;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for
      }

      DistributionLocator[] array = new DistributionLocator[coll.size()];
      coll.toArray(array);
      return array;
    }
  }

  /**
   * Updates the locator string that is used to discover members of the distributed system.
   *
   * @see #getLocators
   */
  void updateLocatorsString() {
    this.setLocators(parseLocatorSet());
  }

  protected String parseLocatorSet() {
    StringBuffer sb = new StringBuffer();
    LOCATORS: for (Iterator iter = this.locatorSet.iterator(); iter.hasNext();) {
      Future future = (Future) iter.next();
      DistributionLocator locator = null;
      for (;;) {
        checkCancellation();
        boolean interrupted = Thread.interrupted();
        try {
          locator = (DistributionLocator) future.get();
          break; // success
        } catch (InterruptedException ex) {
          interrupted = true;
          continue; // keep trying
        } catch (CancellationException ex) {
          continue LOCATORS;
        } catch (ExecutionException ex) {
          handle(ex);
          continue LOCATORS;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
      sb.append(locator.getConfig().getHost());
      sb.append("[").append(locator.getConfig().getPort()).append("]");

      if (iter.hasNext()) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  // -------------------------------------------------------------------------
  // Listener callback methods
  // -------------------------------------------------------------------------

  /** sync to prevent bug 33341 Admin API can double-represent system members */
  private final Object membershipListenerLock = new Object();

  // --------- org.apache.geode.internal.admin.JoinLeaveListener ---------
  /**
   * Listener callback for when a member has joined this DistributedSystem.
   * <p>
   * React by adding the SystemMember to this system's internal lists, if they are not already
   * there. Notice that we add a {@link Future} into the list so that the admin object is not
   * initialized while locks are held.
   *
   * @param source the distributed system that fired nodeJoined
   * @param vm the VM that joined
   * @see org.apache.geode.internal.admin.JoinLeaveListener#nodeJoined
   */
  @Override
  public void nodeJoined(GfManagerAgent source, final GemFireVM vm) {
    // sync to prevent bug 33341 Admin API can double-represent system members
    synchronized (this.membershipListenerLock) {

      // does it already exist?
      SystemMember member = findSystemMember(vm);

      // if not then create it...
      if (member == null) {
        FutureTask future = null;
        if (vm instanceof ApplicationVM) {
          final ApplicationVM app = (ApplicationVM) vm;
          if (app.isDedicatedCacheServer()) {
            synchronized (this.cacheServerSet) {
              future = new AdminFutureTask(vm.getId(), new Callable() {
                @Override
                public Object call() throws Exception {
                  logger.info(LogMarker.DM_MARKER, "Adding new CacheServer for {}",
                      vm);
                  return createCacheServer(app);
                }
              });

              this.cacheServerSet.add(future);
            }

          } else {
            synchronized (this.applicationSet) {
              future = new AdminFutureTask(vm.getId(), new Callable() {
                @Override
                public Object call() throws Exception {
                  logger.info(LogMarker.DM_MARKER, "Adding new Application for {}",
                      vm);
                  return createSystemMember(app);
                }
              });
              this.applicationSet.add(future);
            }
          }

        } else {
          Assert.assertTrue(false, "Unknown GemFireVM type: " + vm.getClass().getName());
        }

        // Wait for the SystemMember to be created. We want to do this
        // outside of the "set" locks.
        future.run();
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            member = (SystemMember) future.get();
            break; // success
          } catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } catch (CancellationException ex) {
            return;
          } catch (ExecutionException ex) {
            handle(ex);
            return;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for

        Assert.assertTrue(member != null);

        // moved this up into the if that creates a new member to fix bug 34517
        SystemMembershipEvent event = new SystemMembershipEventImpl(member.getDistributedMember());
        for (Iterator iter = this.membershipListeners.iterator(); iter.hasNext();) {
          SystemMembershipListener listener = (SystemMembershipListener) iter.next();
          listener.memberJoined(event);
        }
      }

    }
  }

  /**
   * Listener callback for when a member of this DistributedSystem has left.
   * <p>
   * Reacts by removing the member.
   *
   * @param source the distributed system that fired nodeCrashed
   * @param vm the VM that left
   * @see org.apache.geode.internal.admin.JoinLeaveListener#nodeLeft
   */
  @Override
  public void nodeLeft(GfManagerAgent source, GemFireVM vm) {
    // sync to prevent bug 33341 Admin API can double-represent system members
    synchronized (this.membershipListenerLock) {
      // member has left...
      SystemMember member = AdminDistributedSystemImpl.this.removeSystemMember(vm.getId());
      if (member == null) {
        return; // reinstated this early-out because removal does not fix 39429
      }

      // Can't call member.getId() because it is nulled-out when the
      // SystemMember is removed.
      SystemMembershipEvent event = new SystemMembershipEventImpl(vm.getId());
      for (Iterator iter = this.membershipListeners.iterator(); iter.hasNext();) {
        SystemMembershipListener listener = (SystemMembershipListener) iter.next();
        listener.memberLeft(event);
      }
    }
  }

  /**
   * Listener callback for when a member of this DistributedSystem has crashed.
   * <p>
   * Reacts by removing the member.
   *
   * @param source the distributed system that fired nodeCrashed
   * @param vm the VM that crashed
   * @see org.apache.geode.internal.admin.JoinLeaveListener#nodeCrashed
   */
  @Override
  public void nodeCrashed(GfManagerAgent source, GemFireVM vm) {
    // sync to prevent bug 33341 Admin API can double-represent system members
    synchronized (this.membershipListenerLock) {
      // member has crashed...
      SystemMember member = AdminDistributedSystemImpl.this.removeSystemMember(vm.getId());
      if (member == null) {
        // Unknown member crashed. Hmm...
        return;
      }

      // Can't call member.getId() because it is nulled-out when the
      // SystemMember is removed.
      SystemMembershipEvent event = new SystemMembershipEventImpl(vm.getId());
      for (Iterator iter = this.membershipListeners.iterator(); iter.hasNext();) {
        SystemMembershipListener listener = (SystemMembershipListener) iter.next();
        listener.memberCrashed(event);
      }
    }
  }

  // ----------- org.apache.geode.internal.admin.AlertListener -----------
  /**
   * Listener callback for when a SystemMember of this DistributedSystem has crashed.
   *
   * @param alert the latest alert from the system
   * @see org.apache.geode.internal.admin.AlertListener#alert
   */
  @Override
  public void alert(org.apache.geode.internal.admin.Alert alert) {
    if (AlertLevel.forSeverity(alert.getLevel()).ordinal < alertLevel.ordinal) {
      return;
    }
    Alert alert2 = new AlertImpl(alert);
    this.latestAlert = alert2;
    for (Iterator<AlertListener> iter = this.alertListeners.iterator(); iter.hasNext();) {
      AlertListener listener = iter.next();
      listener.alert(alert2);
    }
  }

  @Override
  public void onDisconnect(InternalDistributedSystem sys) {
    logger.debug("Calling AdminDistributedSystemImpl#onDisconnect");
    disconnect();
    logger.debug("Completed AdminDistributedSystemImpl#onDisconnect");
  }

  // -------------------------------------------------------------------------
  // Template methods overriden from superclass...
  // -------------------------------------------------------------------------

  protected CacheServer createCacheServer(ApplicationVM member) throws AdminException {

    return new CacheServerImpl(this, member);
  }

  protected CacheServer createCacheServer(CacheServerConfigImpl conf) throws AdminException {

    return new CacheServerImpl(this, conf);
  }

  /**
   * Override createSystemMember by instantiating SystemMemberImpl
   *
   * @throws AdminException TODO-javadocs
   */
  protected SystemMember createSystemMember(ApplicationVM app)
      throws org.apache.geode.admin.AdminException {
    return new SystemMemberImpl(this, app);
  }

  /**
   * Constructs & returns a SystemMember instance using the corresponding InternalDistributedMember
   * object.
   *
   * @param member InternalDistributedMember instance for which a SystemMember instance is to be
   *        constructed.
   * @return constructed SystemMember instance
   * @throws org.apache.geode.admin.AdminException if construction of SystemMember instance fails
   * @since GemFire 6.5
   */
  protected SystemMember createSystemMember(InternalDistributedMember member)
      throws org.apache.geode.admin.AdminException {
    return new SystemMemberImpl(this, member);
  }

  /**
   * Template-method for creating a new <code>DistributionLocatorImpl</code> instance.
   */
  protected DistributionLocatorImpl createDistributionLocatorImpl(DistributionLocatorConfig conf) {
    return new DistributionLocatorImpl(conf, this);
  }

  // -------------------------------------------------------------------------
  // Non-public implementation methods... TODO: narrow access levels
  // -------------------------------------------------------------------------

  // TODO: public void connect(...) could stand to have some internals factored out

  /**
   * Returns List of Locators including Locators or Multicast.
   *
   * @return list of locators or multicast values
   */
  protected List parseLocators() {

    // assumes host[port] format, delimited by ","
    List locatorIds = new ArrayList();
    if (isMcastEnabled()) {
      String mcastId = new StringBuffer(this.getMcastAddress()).append("[")
          .append(this.getMcastPort()).append("]").toString();
      locatorIds.add(new DistributionLocatorId(mcastId));
    }
    StringTokenizer st = new StringTokenizer(this.getLocators(), ",");
    while (st.hasMoreTokens()) {
      locatorIds.add(new DistributionLocatorId(st.nextToken()));
    }

    if (logger.isDebugEnabled()) {
      StringBuffer sb = new StringBuffer("Locator set is: ");
      for (Iterator iter = locatorIds.iterator(); iter.hasNext();) {
        sb.append(iter.next());
        sb.append(" ");
      }
      logger.debug(sb);
    }

    return locatorIds;
  }

  /**
   * Returns whether or not a <code>SystemMember</code> corresponds to a <code>GemFireVM</code>.
   *
   * @param examineConfig Should we take the configuration of the member into consideration? In
   *        general, we want to consider the configuration when a member starts up. But when we are
   *        notified that it has shut down, we do not want to examine the configuration because that
   *        might involve contacting the member. Which, of course, cannot be done because it has
   *        shut down.
   */
  private boolean isSame(SystemMemberImpl member, GemFireVM vm, boolean examineConfig) {
    if (vm.equals(member.getGemFireVM())) {
      return true;
    }

    InternalDistributedMember memberId = member.getInternalId();
    InternalDistributedMember vmId = vm.getId();

    if (vmId.equals(memberId)) {
      return true;
    }

    if ((member instanceof ManagedSystemMemberImpl) && examineConfig) {

      // We can't compare information about managers because the
      // member might have already gone away. Attempts to send it
      // messages (to get its product directory, for instance) will
      // time out.

      ManagedSystemMemberImpl entity = (ManagedSystemMemberImpl) member;

      // Make sure that the type of the managed entity matches the
      // type of the internal admin object.
      if (entity instanceof CacheServer) {
        if (!(vm instanceof ApplicationVM)) {
          return false;
        }

        ApplicationVM app = (ApplicationVM) vm;
        if (!app.isDedicatedCacheServer()) {
          return false;
        }
      }

      ManagedEntityConfig conf = entity.getEntityConfig();
      InetAddress managedHost = toInetAddress(conf.getHost());
      File managedWorkingDir = new File(conf.getWorkingDirectory());
      File managedProdDir = new File(conf.getProductDirectory());

      InetAddress vmHost = vm.getHost();
      File vmWorkingDir = vm.getWorkingDirectory();
      File vmProdDir = vm.getGeodeHomeDir();

      if (vmHost.equals(managedHost) && isSameFile(vmWorkingDir, managedWorkingDir)
          && isSameFile(vmProdDir, managedProdDir)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns whether or not the names of the two files represent the same file.
   */
  private boolean isSameFile(File file1, File file2) {
    if (file1.equals(file2)) {
      return true;
    }

    if (file1.getAbsoluteFile().equals(file2.getAbsoluteFile())) {
      return true;
    }

    try {
      if (file1.getCanonicalFile().equals(file2.getCanonicalFile())) {
        return true;
      }
    } catch (IOException ex) {
      // oh well...
      logger.info("While getting canonical file", ex);
    }

    return false;
  }

  /**
   * Finds and returns the <code>SystemMember</code> that corresponds to the given
   * <code>GemFireVM</code> or <code>null</code> if no <code>SystemMember</code> corresponds.
   */
  protected SystemMember findSystemMember(GemFireVM vm) {
    return findSystemMember(vm, true);
  }

  /**
   * Finds and returns the <code>SystemMember</code> that corresponds to the given
   * <code>GemFireVM</code> or <code>null</code> if no Finds and returns the
   * <code>SystemMember</code> that corresponds to the given <code>GemFireVM</code> or
   * <code>null</code> if no <code>SystemMember</code> corresponds.
   *
   *
   * @param vm GemFireVM instance
   * @param compareConfig Should the members' configurations be compared? <code>true</code> when the
   *        member has joined, <code>false</code> when the member has left Should the members'
   *        configurations be compared? <code>true</code> when the member has joined,
   *        <code>false</code> when the member has left. Additionally also used to check if system
   *        member config is to be synchronized with the VM.
   */
  protected SystemMember findSystemMember(GemFireVM vm, boolean compareConfig) {

    SystemMemberImpl member = null;

    synchronized (this.cacheServerSet) {
      SERVERS: for (Iterator iter = this.cacheServerSet.iterator(); iter.hasNext();) {
        Future future = (Future) iter.next();
        CacheServerImpl cacheServer = null;
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            cacheServer = (CacheServerImpl) future.get();
            break; // success
          } catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } catch (CancellationException ex) {
            continue SERVERS;
          } catch (ExecutionException ex) {
            handle(ex);
            continue SERVERS;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for

        if (isSame(cacheServer, vm, compareConfig)) {
          member = cacheServer;
          break;
        }
      }
    }

    if (member == null) {
      synchronized (this.applicationSet) {
        APPS: for (Iterator iter = this.applicationSet.iterator(); iter.hasNext();) {
          Future future = (Future) iter.next();
          SystemMemberImpl application = null;
          for (;;) {
            checkCancellation();
            boolean interrupted = Thread.interrupted();
            try {
              application = (SystemMemberImpl) future.get();
              break; // success
            } catch (InterruptedException ex) {
              interrupted = true;
              continue; // keep trying
            } catch (CancellationException ex) {
              continue APPS;
            } catch (ExecutionException ex) {
              handle(ex);
              continue APPS;
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          } // for

          if (isSame(application, vm, compareConfig)) {
            member = application;
            break;
          }
        } // APPS
      }
    }

    if (member != null && compareConfig) {
      try {
        member.setGemFireVM(vm);

      } catch (AdminException ex) {
        logger.warn("Could not set the GemFire VM.", ex);
      }
    }

    return member;
  }

  /**
   * Removes a SystemMember from this system's list of known members.
   *
   * @param systemMember the member to remove
   * @return the system member that was removed; null if no match was found
   */
  protected SystemMember removeSystemMember(SystemMember systemMember) {
    return removeSystemMember(((SystemMemberImpl) systemMember).getInternalId());
  }

  /**
   * Removes a SystemMember from this system's list of known members. This method is called in
   * response to a member leaving the system. TODO: this method is a mess of defns
   *
   * @param internalId the unique id that specifies which member to remove
   * @return the system member that was removed; null if no match was found
   */
  protected SystemMember removeSystemMember(InternalDistributedMember internalId) {
    if (internalId == null)
      return null;

    boolean found = false;
    SystemMemberImpl member = null;

    synchronized (this.cacheServerSet) {
      SERVERS: for (Iterator iter = this.cacheServerSet.iterator(); iter.hasNext() && !found;) {
        Future future = (Future) iter.next();
        if (future instanceof AdminFutureTask) {
          AdminFutureTask task = (AdminFutureTask) future;
          if (task.getMemberId().equals(internalId)) {
            future.cancel(true);

          } else {
            // This is not the member we are looking for...
            continue SERVERS;
          }
        }
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            member = (SystemMemberImpl) future.get();
            break; // success
          } catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } catch (CancellationException ex) {
            continue SERVERS;
          } catch (ExecutionException ex) {
            handle(ex);
            return null; // Dead code
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        }

        InternalDistributedMember cacheServerId = member.getInternalId();
        if (internalId.equals(cacheServerId)) {
          // found a match...
          iter.remove();
          found = true;
        }
      } // SERVERS
    }

    synchronized (this.applicationSet) {
      for (Iterator iter = this.applicationSet.iterator(); iter.hasNext() && !found;) {
        Future future = (Future) iter.next();
        try {
          if (future instanceof AdminFutureTask) {
            AdminFutureTask task = (AdminFutureTask) future;
            if (task.getMemberId().equals(internalId)) {
              iter.remove(); // Only remove applications
              found = true;
              if (future.isDone()) {
                member = (SystemMemberImpl) future.get();
              }
              break;
            } else {
              // This is not the member we are looking for...
              continue;
            }
          }
          if (future.isDone()) {
            member = (SystemMemberImpl) future.get();
          } else {
            future.cancel(true);
          }

        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          checkCancellation();
          throw new RuntimeException(
              "Interrupted", ex);

        } catch (CancellationException ex) {
          continue;

        } catch (ExecutionException ex) {
          handle(ex);
          return null; // Dead code
        }

        InternalDistributedMember applicationId = member.getInternalId();
        if (internalId.equals(applicationId)) {
          // found a match...
          iter.remove(); // Only remove applications
          found = true;
        }
      }
    }

    if (found) {
      try {
        if (member != null) {
          member.setGemFireVM(null);
        }

      } catch (AdminException ex) {
        logger.fatal("Unexpected AdminException", ex);
      }
      return member;

    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Couldn't remove member {}", internalId);
      }
      return null;
    }
  }

  /**
   * Builds the configuration needed to connect to a GfManagerAgent which is the main gateway into
   * the internal.admin api. GfManagerAgent is used to actually connect to the distributed gemfire
   * system.
   *
   * @param logWriter the LogWriterI18n to use for any logging
   * @return the configuration needed to connect to a GfManagerAgent
   */
  // LOG: saves LogWriterLogger from AdminDistributedSystemImpl for RemoteGfManagerAgentConfig
  private GfManagerAgentConfig buildAgentConfig(InternalLogWriter logWriter) {
    RemoteTransportConfig conf = new RemoteTransportConfig(isMcastEnabled(), getDisableTcp(),
        getDisableAutoReconnect(), getBindAddress(), buildSSLConfig(), parseLocators(),
        getMembershipPortRange(), getTcpPort(), ClusterDistributionManager.ADMIN_ONLY_DM_TYPE);
    return new GfManagerAgentConfig(getSystemName(), conf, logWriter, this.alertLevel.getSeverity(),
        this, this);
  }

  protected SSLConfig buildSSLConfig() {
    SSLConfig.Builder sslConfigBuilder = new SSLConfig.Builder();

    if (getConfig() != null) {
      sslConfigBuilder.setEnabled(getConfig().isSSLEnabled());
      sslConfigBuilder.setProtocols(getConfig().getSSLProtocols());
      sslConfigBuilder.setCiphers(getConfig().getSSLCiphers());
      sslConfigBuilder.setRequireAuth(getConfig().isSSLAuthenticationRequired());
      sslConfigBuilder.setProperties(getConfig().getSSLProperties());
    }
    return sslConfigBuilder.build();
  }

  /**
   * Returns the currently configured address to bind to when administering this system.
   */
  private String getBindAddress() {
    return this.config.getBindAddress();
  }

  /** Returns whether or not the given member is running */
  private boolean isRunning(SystemMember member) {
    if (member instanceof ManagedEntity) {
      return ((ManagedEntity) member).isRunning();

    } else {
      // member must be an application VM. It is running
      return true;
    }
  }

  /** Returns any member manager that is known to be running */
  private SystemMember findFirstRunningMember() {
    synchronized (this.cacheServerSet) {
      SERVERS: for (Iterator iter = this.cacheServerSet.iterator(); iter.hasNext();) {
        Future future = (Future) iter.next();
        SystemMember member = null;
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            member = (SystemMember) future.get();
            break; // success
          } catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } catch (CancellationException ex) {
            continue SERVERS;
          } catch (ExecutionException ex) {
            handle(ex);
            return null; // Dead code
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for

        if (isRunning(member)) {
          return member;
        }
      }
    }

    synchronized (this.applicationSet) {
      APPS: for (Iterator iter = this.applicationSet.iterator(); iter.hasNext();) {
        Future future = (Future) iter.next();
        SystemMember member = null;
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            member = (SystemMember) future.get();
            break; // success
          } catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } catch (CancellationException ex) {
            continue APPS;
          } catch (ExecutionException ex) {
            handle(ex);
            return null; // Dead code
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for

        if (isRunning(member)) {
          return member;
        }
      } // APPS
    }

    return null;
  }

  /**
   * Returns the instance of system member that is running either as a CacheVm or only ApplicationVm
   * for the given string representation of the id.
   *
   * @param memberId string representation of the member identifier
   * @return instance of system member which could be either as a CacheVm or Application VM
   */
  protected SystemMember findCacheOrAppVmById(String memberId) {
    SystemMember found = null;

    if (memberId != null) {
      try {
        boolean foundSender = false;
        CacheVm[] cacheVms = getCacheVms();

        /*
         * cacheVms could be null. See AdminDistributedSystemImpl.getCacheVmsCollection() for
         * ExecutionException
         */
        if (cacheVms != null) {
          for (CacheVm cacheVm : cacheVms) {
            if (cacheVm.getId().equals(memberId) && cacheVm instanceof CacheVm) {
              found = (SystemMember) cacheVm;
              foundSender = true;
              break;
            }
          }
        }

        if (!foundSender) {
          SystemMember[] appVms = getSystemMemberApplications();

          for (SystemMember appVm : appVms) {
            if (appVm.getId().equals(memberId) && appVm instanceof SystemMember) {
              found = (SystemMember) appVm;
              foundSender = true;
              break;
            }
          }

        }
      } catch (AdminException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Could not find System Member for member id: {}", memberId, e);
        }
      }
    }

    return found;
  }

  /** Returns true if any member application is known to be running */
  protected boolean isAnyMemberRunning() {
    return findFirstRunningMember() != null;
  }

  // -------------------------------------------------------------------------
  // Health methods
  // -------------------------------------------------------------------------

  /**
   * Lazily initializes the GemFire health monitor
   *
   * @see #createGemFireHealth
   */
  @Override
  public GemFireHealth getGemFireHealth() {
    synchronized (this) {
      if (this.health == null || this.health.isClosed()) {
        try {
          this.health = createGemFireHealth(this.gfManagerAgent);

        } catch (AdminException ex) {
          throw new RuntimeAdminException(
              "An AdminException was thrown while getting the GemFire health.",
              ex);
        }
      }

      return this.health;
    }
  }

  /**
   * A "template factory" method for creating an instance of <code>GemFireHealth</code>. It can be
   * overridden by subclasses to produce instances of different <code>GemFireHealth</code>
   * implementations.
   *
   * @see #getGemFireHealth
   */
  protected GemFireHealth createGemFireHealth(GfManagerAgent agent) throws AdminException {

    if (agent == null) {
      throw new IllegalStateException(
          "GfManagerAgent must not be null");
    }
    return new GemFireHealthImpl(agent, this);
  }

  @Override
  public CacheVm addCacheVm() throws AdminException {
    return (CacheVm) addCacheServer();
  }

  @Override
  public CacheServer addCacheServer() throws AdminException {
    CacheServerConfigImpl conf = new CacheServerConfigImpl();
    CacheServer server = createCacheServer(conf);
    setDistributionParameters(server);

    synchronized (this.cacheServerSet) {
      this.cacheServerSet.add(new FutureResult(server));
    }

    return server;
  }

  private Collection getCacheVmsCollection() throws AdminException {
    synchronized (this.cacheServerSet) {
      Collection coll = new ArrayList(this.cacheServerSet.size());
      SERVERS: for (Iterator iter = this.cacheServerSet.iterator(); iter.hasNext();) {
        Future future = (Future) iter.next();
        Object get = null;
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            get = future.get();
            break; // success
          } catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } catch (CancellationException ex) {
            continue SERVERS;
          } catch (ExecutionException ex) {
            handle(ex);
            return null; // Dead code
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for
        coll.add(get);
      } // SERVERS
      return coll;
    }
  }

  /**
   * Returns all the cache server members of the distributed system which are hosting a client queue
   * for the particular durable-client having the given durableClientId
   *
   * @param durableClientId - durable-id of the client
   * @return array of CacheServer(s) having the queue for the durable client
   *
   * @since GemFire 5.6
   */
  @Override
  public CacheServer[] getCacheServers(String durableClientId) throws AdminException {
    Collection serversForDurableClient = new ArrayList();
    CacheServer[] servers = getCacheServers();

    for (int i = 0; i < servers.length; i++) {
      RemoteApplicationVM vm = (RemoteApplicationVM) ((CacheServerImpl) servers[i]).getGemFireVM();
      if (vm != null && vm.hasDurableClient(durableClientId)) {
        serversForDurableClient.add(servers[i]);
      }
    }
    CacheServer[] array = new CacheServer[serversForDurableClient.size()];
    serversForDurableClient.toArray(array);
    return array;
  }

  @Override
  public CacheVm[] getCacheVms() throws AdminException {
    Collection coll = getCacheVmsCollection();
    if (coll == null)
      return null;
    CacheVm[] array = new CacheVm[coll.size()];
    coll.toArray(array);
    return array;
  }

  @Override
  public CacheServer[] getCacheServers() throws AdminException {
    Collection coll = getCacheVmsCollection();
    if (coll == null)
      return null;
    CacheServer[] array = new CacheServer[coll.size()];
    coll.toArray(array);
    return array;
  }

  // -------------------------------------------------------------------------
  // Overriden java.lang.Object methods
  // -------------------------------------------------------------------------

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override // GemStoneAddition
  public String toString() {
    return getName();
  }

  /**
   * returns instance of AdminDistributedSystem that is current connected. See
   * <code>thisAdminDS</code>. (for feature requests #32887)
   * <p>
   * TODO: remove this static method during reimplementation of
   * {@link SystemMemberCacheEventProcessor}
   *
   */
  public static AdminDistributedSystemImpl getConnectedInstance() {
    synchronized (CONNECTION_SYNC) {
      return thisAdminDS;
    }
  }

  @Override
  public void addCacheListener(SystemMemberCacheListener listener) {
    synchronized (this.cacheListLock) {
      // never modify cacheListeners in place.
      // this allows iteration without concurrent mod worries
      List oldListeners = this.cacheListeners;
      if (!oldListeners.contains(listener)) {
        List newListeners = new ArrayList(oldListeners);
        newListeners.add(listener);
        this.cacheListeners = newListeners;
      }
    }
  }

  @Override
  public void removeCacheListener(SystemMemberCacheListener listener) {
    synchronized (this.cacheListLock) {
      List oldListeners = this.cacheListeners;
      if (oldListeners.contains(listener)) {
        List newListeners = new ArrayList(oldListeners);
        if (newListeners.remove(listener)) {
          if (newListeners.isEmpty()) {
            newListeners = Collections.EMPTY_LIST;
          }
          this.cacheListeners = newListeners;
        }
      }
    }
  }

  public List getCacheListeners() {
    return this.cacheListeners;
  }

  @Override
  public SystemMember lookupSystemMember(DistributedMember distributedMember)
      throws AdminException {
    if (distributedMember == null)
      return null;
    SystemMember[] members = getSystemMemberApplications();
    for (int i = 0; i < members.length; i++) {
      if (distributedMember.equals(members[i].getDistributedMember())) {
        return members[i];
      }
    }
    return null;
  }

  //////////////////////// Inner Classes ////////////////////////

  /**
   * Object that converts an <code>internal.admin.Alert</code> into an external
   * <code>admin.Alert</code>.
   */
  public class AlertImpl implements Alert {
    /** The Alert to which most behavior is delegated */
    private final org.apache.geode.internal.admin.Alert alert;
    private SystemMember systemMember;

    /////////////////////// Constructors ///////////////////////

    /**
     * Creates a new <code>Alert</code> that delegates to the given object.
     */
    AlertImpl(org.apache.geode.internal.admin.Alert alert) {
      this.alert = alert;
      GemFireVM vm = alert.getGemFireVM();

      /*
       * Related to #39657. Avoid setting GemFireVM again in the system member. Eager initialization
       * of member variable - systemMember.
       */
      this.systemMember = vm == null ? null : findSystemMember(vm, false);
      if (this.systemMember == null) {
        /*
         * try to use sender information to construct the SystemMember that can be used for disply
         * purpose at least
         */
        InternalDistributedMember sender = alert.getSender();
        if (sender != null) {
          try {
            this.systemMember = AdminDistributedSystemImpl.this.createSystemMember(sender);
          } catch (AdminException e) {
            /*
             * AdminException might be thrown if creation of System Member instance fails.
             */
            this.systemMember = null;
          }
        } // else this.systemMember will be null
      }
    }

    ////////////////////// Instance Methods //////////////////////

    @Override
    public AlertLevel getLevel() {
      return AlertLevel.forSeverity(alert.getLevel());
    }

    /*
     * Eager initialization of system member is done while creating this alert only.
     */
    @Override
    public SystemMember getSystemMember() {
      return systemMember;
    }

    @Override
    public String getConnectionName() {
      return alert.getConnectionName();
    }

    @Override
    public String getSourceId() {
      return alert.getSourceId();
    }

    @Override
    public String getMessage() {
      return alert.getMessage();
    }

    @Override
    public java.util.Date getDate() {
      return alert.getDate();
    }

    @Override
    public String toString() {
      return alert.toString();
    }
  }

  /**
   * A JSR-166 <code>FutureTask</code> whose {@link #get} method properly handles an
   * <code>ExecutionException</code> that wraps an <code>InterruptedException</code>. This is
   * necessary because there are places in the admin API that wrap
   * <code>InterruptedException</code>s. See bug 32634.
   *
   * <P>
   *
   * This is by no means an ideal solution to this problem. It would be better to modify the code
   * invoked by the <code>Callable</code> to explicitly throw <code>InterruptedException</code>.
   */
  static class AdminFutureTask extends FutureTask {

    /**
     * The id of the member whose admin object we are creating. Keeping track of this allows us to
     * cancel a FutureTask for a member that has gone away.
     */
    private final InternalDistributedMember memberId;

    public AdminFutureTask(InternalDistributedMember memberId, Callable callable) {
      super(callable);
      this.memberId = memberId;
    }

    /**
     * Returns the id of the member of the distributed system for which this <code>FutureTask</code>
     * is doing work.
     */
    public InternalDistributedMember getMemberId() {
      return this.memberId;
    }

    /**
     * If the <code>ExecutionException</code> is caused by an <code>InterruptedException</code>,
     * throw the <code>CancellationException</code> instead.
     */
    @Override
    public Object get() throws InterruptedException, ExecutionException {

      if (Thread.interrupted())
        throw new InterruptedException();
      try {
        return super.get();

      } catch (ExecutionException ex) {
        for (Throwable cause = ex.getCause(); cause != null; cause = cause.getCause()) {
          if (cause instanceof InterruptedException) {
            // We interrupted the runnable but we don't want the thread
            // that called get() to think that the runnable was interrupted.
            CancellationException ex2 = new CancellationException(
                "by interrupt");
            ex2.setStackTrace(cause.getStackTrace());
            throw ex2;
          }
        }

        throw ex;
      }

    }

  }

  public DistributedMember getDistributedMember() {
    return getDistributionManager().getId();
  }

  private void connectAdminDS() {
    connect((InternalLogWriter) this.logWriter);
    try {
      thisAdminDS.waitToBeConnected(3000);
    } catch (InterruptedException ie) {
      logger.warn("Interrupted while waiting to connect", ie);
    }
  }

  @Override
  public Set<PersistentID> getMissingPersistentMembers() throws AdminException {
    connectAdminDS();
    DistributionManager dm = getDistributionManager();
    if (dm == null) {
      throw new IllegalStateException(
          "connect() has not been invoked on this AdminDistributedSystem.");
    }
    return getMissingPersistentMembers(dm);
  }

  public static Set<PersistentID> getMissingPersistentMembers(DistributionManager dm) {
    return MissingPersistentIDsRequest.send(dm);
  }

  @Override
  public void revokePersistentMember(InetAddress host, String directory) throws AdminException {
    connectAdminDS();
    DistributionManager dm = getDistributionManager();
    if (dm == null) {
      throw new IllegalStateException(
          "connect() has not been invoked on this AdminDistributedSystem.");
    }
    revokePersistentMember(dm, host, directory);

  }

  @Override
  public void revokePersistentMember(UUID diskStoreID) throws AdminException {
    connectAdminDS();
    DistributionManager dm = getDistributionManager();
    if (dm == null) {
      throw new IllegalStateException(
          "connect() has not been invoked on this AdminDistributedSystem.");
    }
    revokePersistentMember(dm, diskStoreID);

  }

  public static void revokePersistentMember(DistributionManager dm, UUID diskStoreID) {
    PersistentMemberPattern pattern = new PersistentMemberPattern(diskStoreID);
    boolean success = false;
    try {
      // make sure that the disk store we're revoking is actually missing
      boolean found = false;
      Set<PersistentID> details = getMissingPersistentMembers(dm);
      if (details != null) {
        for (PersistentID id : details) {
          if (id.getUUID().equals(diskStoreID)) {
            found = true;
            break;
          }
        }
      }
      if (!found) {
        return;
      }

      // Fix for 42607 - verify that the persistent id is not already
      // running before revoking it.
      PrepareRevokePersistentIDRequest.send(dm, pattern);
      success = true;
    } finally {
      if (success) {
        // revoke the persistent member if were able to prepare the revoke
        RevokePersistentIDRequest.send(dm, pattern);
      } else {
        // otherwise, cancel the revoke.
        PrepareRevokePersistentIDRequest.cancel(dm, pattern);
      }
    }
  }

  /**
   *
   * @deprecated use {@link #revokePersistentMember(UUID)} instead
   */
  public static void revokePersistentMember(DistributionManager dm, InetAddress host,
      String directory) {

    PersistentMemberPattern pattern =
        new PersistentMemberPattern(host, directory, System.currentTimeMillis());
    boolean success = false;
    try {
      // Fix for 42607 - verify that the persistent id is not already
      // running before revoking it.
      PrepareRevokePersistentIDRequest.send(dm, pattern);
      success = true;
    } finally {
      if (success) {
        // revoke the persistent member if were able to prepare the revoke
        RevokePersistentIDRequest.send(dm, pattern);
      } else {
        // otherwise, cancel the revoke.
        PrepareRevokePersistentIDRequest.cancel(dm, pattern);
      }
    }
  }

  @Override
  public Set shutDownAllMembers() throws AdminException {
    return shutDownAllMembers(0);
  }

  @Override
  public Set shutDownAllMembers(long timeout) throws AdminException {
    connectAdminDS();
    DistributionManager dm = getDistributionManager();
    if (dm == null) {
      throw new IllegalStateException(
          "connect() has not been invoked on this AdminDistributedSystem.");
    }
    return shutDownAllMembers(dm, timeout);
  }

  /**
   * Shutdown all members.
   *
   * @param timeout the amount of time (in ms) to spending trying to shutdown the members
   *        gracefully. After this time period, the members will be forceable shut down. If the
   *        timeout is exceeded, persistent recovery after the shutdown may need to do a GII. -1
   *        indicates that the shutdown should wait forever.
   */
  public static Set shutDownAllMembers(DistributionManager dm, long timeout) {
    return ShutdownAllRequest.send(dm, timeout);
  }

  @Override
  public BackupStatus backupAllMembers(File targetDir) throws AdminException {
    return backupAllMembers(targetDir, null);
  }

  @Override
  public BackupStatus backupAllMembers(File targetDir, File baselineDir) throws AdminException {
    connectAdminDS();
    DistributionManager dm = getDistributionManager();
    if (dm == null) {
      throw new IllegalStateException(
          "connect() has not been invoked on this AdminDistributedSystem.");
    }
    return backupAllMembers(dm, targetDir, baselineDir);
  }

  public static BackupStatus backupAllMembers(DistributionManager dm, File targetDir,
      File baselineDir) throws AdminException {
    String baselineDirectory = baselineDir == null ? null : baselineDir.toString();
    return new BackupOperation(dm, dm.getCache()).backupAllMembers(targetDir.toString(),
        baselineDirectory);
  }

  @Override
  public Map<DistributedMember, Set<PersistentID>> compactAllDiskStores() throws AdminException {
    connectAdminDS();
    DistributionManager dm = getDistributionManager();
    if (dm == null) {
      throw new IllegalStateException(
          "connect() has not been invoked on this AdminDistributedSystem.");
    }
    return compactAllDiskStores(dm);
  }

  public static Map<DistributedMember, Set<PersistentID>> compactAllDiskStores(
      DistributionManager dm) throws AdminException {
    return CompactRequest.send(dm);
  }

  /**
   * This method can be used to process ClientMembership events sent for BridgeMembership by bridge
   * servers to all admin members.
   *
   * NOTE: Not implemented currently. JMX implementation which is a subclass of this class i.e.
   * AdminDistributedSystemJmxImpl implements it.
   *
   * @param senderId id of the member that sent the ClientMembership changes for processing (could
   *        be null)
   * @param clientId id of a client for which the notification was sent
   * @param clientHost host on which the client is/was running
   * @param eventType denotes whether the client Joined/Left/Crashed should be one of
   *        ClientMembershipMessage#JOINED, ClientMembershipMessage#LEFT,
   *        ClientMembershipMessage#CRASHED
   */
  public void processClientMembership(String senderId, String clientId, String clientHost,
      int eventType) {}

  @Override
  public void setAlertLevelAsString(String level) {
    AlertLevel newAlertLevel = AlertLevel.forName(level);

    if (newAlertLevel != null) {
      setAlertLevel(newAlertLevel);
    } else {
      System.out.println("ERROR:: " + level
          + " is invalid. Allowed alert levels are: WARNING, ERROR, SEVERE, OFF");
      throw new IllegalArgumentException(String.format("%s",
          level + " is invalid. Allowed alert levels are: WARNING, ERROR, SEVERE, OFF"));
    }
  }

  @Override
  public String getAlertLevelAsString() {
    return getAlertLevel().getName();
  }
}
