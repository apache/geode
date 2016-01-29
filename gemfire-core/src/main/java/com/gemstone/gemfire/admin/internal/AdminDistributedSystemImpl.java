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
package com.gemstone.gemfire.admin.internal;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.Alert;
import com.gemstone.gemfire.admin.AlertLevel;
import com.gemstone.gemfire.admin.AlertListener;
import com.gemstone.gemfire.admin.BackupStatus;
import com.gemstone.gemfire.admin.CacheServer;
import com.gemstone.gemfire.admin.CacheServerConfig;
import com.gemstone.gemfire.admin.CacheVm;
import com.gemstone.gemfire.admin.ConfigurationParameter;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.DistributionLocator;
import com.gemstone.gemfire.admin.DistributionLocatorConfig;
import com.gemstone.gemfire.admin.GemFireHealth;
import com.gemstone.gemfire.admin.ManagedEntity;
import com.gemstone.gemfire.admin.ManagedEntityConfig;
import com.gemstone.gemfire.admin.OperationCancelledException;
import com.gemstone.gemfire.admin.RuntimeAdminException;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.admin.SystemMemberCacheListener;
import com.gemstone.gemfire.admin.SystemMembershipEvent;
import com.gemstone.gemfire.admin.SystemMembershipListener;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.FutureCancelledException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.Banner;
import com.gemstone.gemfire.internal.admin.ApplicationVM;
import com.gemstone.gemfire.internal.admin.GemFireVM;
import com.gemstone.gemfire.internal.admin.GfManagerAgent;
import com.gemstone.gemfire.internal.admin.GfManagerAgentConfig;
import com.gemstone.gemfire.internal.admin.GfManagerAgentFactory;
import com.gemstone.gemfire.internal.admin.SSLConfig;
import com.gemstone.gemfire.internal.admin.remote.CompactRequest;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.admin.remote.MissingPersistentIDsRequest;
import com.gemstone.gemfire.internal.admin.remote.PrepareRevokePersistentIDRequest;
import com.gemstone.gemfire.internal.admin.remote.RemoteApplicationVM;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.admin.remote.RevokePersistentIDRequest;
import com.gemstone.gemfire.internal.admin.remote.ShutdownAllRequest;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberPattern;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LogWriterFactory;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterAppender;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterAppenders;
import com.gemstone.gemfire.internal.util.concurrent.FutureResult;

/**
 * Represents a GemFire distributed system for remote administration/management.
 *
 * @author    Kirk Lund
 * @since     3.5
 */
public class AdminDistributedSystemImpl
implements com.gemstone.gemfire.admin.AdminDistributedSystem,
           com.gemstone.gemfire.internal.admin.JoinLeaveListener,
           com.gemstone.gemfire.internal.admin.AlertListener,
           com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.DisconnectListener {

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
  
  /** The level above which alerts will be delivered to the alert
   * listeners */
  private AlertLevel alertLevel = AlertLevel.WARNING;

  /** The alert listeners registered on this distributed system. */
  private volatile Set<AlertListener> alertListeners = Collections.emptySet();
  private final Object alertLock = new Object();
  
  private LogWriterAppender logWriterAppender;
  
  private InternalLogWriter logWriter;
  
  /** The membership listeners registered on this distributed system */
  private volatile Set membershipListeners = Collections.EMPTY_SET;
  private final Object membershipLock = new Object();
  
  /* The region listeners registered on this distributed system */
  //for feature requests #32887
  private volatile List cacheListeners = Collections.EMPTY_LIST;
  private final Object cacheListLock = new Object();
  
  /** 
   * reference to AdminDistributedSystemImpl instance 
   * for feature requests #32887. 
   * <p>
   * Guarded by {@link #CONNECTION_SYNC}.
   * <p>
   * TODO: reimplement this change and SystemMemberCacheEventProcessor to avoid
   * using this static. SystemMemberCacheEvents should only be sent to Admin 
   * VMs that express interest.
   * <p>
   * This is volatile to allow SystemFailure to deliver fatal poison-pill
   * to thisAdminDS without waiting on synchronization.
   * 
   * @guarded.By CONNECTION_SYNC
   */
  private static volatile AdminDistributedSystemImpl thisAdminDS;

  /**
   * Provides synchronization for {@link #connect()} and {@link #disconnect()}.
   * {@link #thisAdminDS} is also now protected by CONNECTION_SYNC and has its
   * lifecycle properly tied to connect/disconnect.
   */
  private static final Object CONNECTION_SYNC = new Object();
   
  
  // -------------------------------------------------------------------------
  //   Constructor(s)
  // -------------------------------------------------------------------------
  
  /** 
   * Constructs new DistributedSystemImpl with the given configuration.
   *
   * @param config  configuration defining this distributed system
   */
  public AdminDistributedSystemImpl(DistributedSystemConfigImpl config) {
                          
    // init from config...
    this.config = config;

    String systemId = this.config.getSystemId();
    if (systemId != null && systemId.length() > 0) {
      this.id = systemId;

    } if (this.getLocators() != null && this.getLocators().length() > 0) {
      this.id = this.getLocators();

    } else {
      this.id = new StringBuffer(this.getMcastAddress()).append("[").append(
          this.getMcastPort()).append("]").toString();
    }

    // LOG: create LogWriterAppender unless one already exists
    this.logWriterAppender = LogWriterAppenders.getOrCreateAppender(LogWriterAppenders.Identifier.MAIN, false, this.config.createLogConfig(), false);
    
    // LOG: look in DistributedSystemConfigImpl for existing LogWriter to use
    InternalLogWriter existingLogWriter = this.config.getInternalLogWriter();
    if (existingLogWriter != null) {
      this.logWriter = existingLogWriter;
    } else {      
      // LOG: create LogWriterLogger
      this.logWriter = LogWriterFactory.createLogWriterLogger(false, false, this.config.createLogConfig(), false);
      // LOG: changed statement from config to info
      this.logWriter.info(Banner.getString(null));
      // Set this log writer in DistributedSystemConfigImpl
      this.config.setInternalLogWriter(this.logWriter);
    }
    
    // set up other details that depend on config attrs...
    this.controller = ManagedEntityControllerFactory.createManagedEntityController(this);
    initializeDistributionLocators();
    initializeCacheServers();
  }
  
  // -------------------------------------------------------------------------
  //   Initialization
  // -------------------------------------------------------------------------
  
  /**
   * Creates DistributionLocator instances for every locator entry in the
   * {@link com.gemstone.gemfire.admin.DistributedSystemConfig}
   */
  private void initializeDistributionLocators() {
    DistributionLocatorConfig[] configs =
      this.config.getDistributionLocatorConfigs();
    if (configs.length == 0) {
      // No work to do
      return;
    }

    for (int i = 0; i < configs.length; i++) {
      // the Locator impl may vary in this class from the config...
      DistributionLocatorConfig conf = configs[i];
      DistributionLocator locator =
        createDistributionLocatorImpl(conf);
      this.locatorSet.add(new FutureResult(locator));
    }
    // update locators string...
    setLocators(parseLocatorSet());
  }
  
  /**
   * Creates <code>CacheServer</code> instances for every cache server
   * entry in the {@link
   * com.gemstone.gemfire.admin.DistributedSystemConfig}
   */
  private void initializeCacheServers() {
    CacheServerConfig[] cacheServerConfigs =
      this.config.getCacheServerConfigs();
    for (int i = 0; i < cacheServerConfigs.length; i++) {
      try {
        CacheServerConfig conf = cacheServerConfigs[i];
        CacheServerConfigImpl copy =
          new CacheServerConfigImpl(conf);
        this.cacheServerSet.add(new FutureResult(createCacheServer(copy)));
      } catch (java.lang.Exception e) {
        logger.warn(e.getMessage(), e);
        continue;
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (java.lang.Error e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
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
   * @throws IllegalStateException
   *         If {@link #connect()} has not been called.
   */
  private void checkConnectCalled() {
    if (this.gfManagerAgent == null) {
      throw new IllegalStateException(LocalizedStrings.AdminDistributedSystemImpl_CONNECT_HAS_NOT_BEEN_INVOKED_ON_THIS_ADMINDISTRIBUTEDSYSTEM.toLocalizedString());
    }
  }

  // -------------------------------------------------------------------------
  //   Attributes of this DistributedSystem
  // -------------------------------------------------------------------------
  
  public GfManagerAgent getGfManagerAgent() {
    return this.gfManagerAgent;
  }
  
  public boolean isConnected() {
    return this.gfManagerAgent != null && this.gfManagerAgent.isConnected();
  }

  public String getId() {
    return this.id;
  }
  
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

  public String getRemoteCommand() {
    return this.config.getRemoteCommand();
  }

  public void setRemoteCommand(String remoteCommand) {
    this.config.setRemoteCommand(remoteCommand);
  }

  public void setAlertLevel(AlertLevel level) {
    if (this.isConnected()) {
      this.gfManagerAgent.setAlertLevel(level.getSeverity());
    }

    this.alertLevel = level;
  }

  public AlertLevel getAlertLevel() {
    return this.alertLevel;
  }

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

  public String getMcastAddress() {
    return this.config.getMcastAddress();
  }

  public int getMcastPort() {
    return this.config.getMcastPort();
  }
  
  public boolean getDisableTcp() {
    return this.config.getDisableTcp();
  }
  
  public boolean getDisableAutoReconnect() {
    return this.config.getDisableAutoReconnect();
  }

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

  public DistributedSystemConfig getConfig() {
    return this.config;
  }
  
  /**
   * Returns true if any members of this system are currently running.
   */
  public boolean isRunning() {
    if (this.gfManagerAgent == null) return false;
    // is there a better way??
    // this.gfManagerAgent.isConnected() ... this.gfManagerAgent.isListening()
    
    if (isAnyMemberRunning()) return true;
    return false;
  }
  
  /** Returns true if this system can use multicast for communications */
  public boolean isMcastEnabled() {
    return this.getMcastPort() > 0 ;
  }
  
  ManagedEntityController getEntityController() {
    return this.controller;
  }
  
  static private final String TIMEOUT_MS_NAME 
      = "AdminDistributedSystemImpl.TIMEOUT_MS";
  static private final int TIMEOUT_MS_DEFAULT = 60000; // 30000 -- see bug36470
  static private final int TIMEOUT_MS 
      = Integer.getInteger(TIMEOUT_MS_NAME, TIMEOUT_MS_DEFAULT).intValue();
  

  // -------------------------------------------------------------------------
  //   Operations of this DistributedSystem
  // -------------------------------------------------------------------------
  
  /**
   * Starts all managed entities in this system.
   */
  public void start() throws AdminException {
    // Wait for each managed entity to start (see bug 32569)
    DistributionLocator[] locs = getDistributionLocators();
    for (int i = 0; i < locs.length; i++) {
      locs[i].start();
    }
    for (int i = 0; i < locs.length; i++) {
      try {
        if (!locs[i].waitToStart(TIMEOUT_MS)) {
          throw new AdminException(LocalizedStrings.AdminDistributedSystemImpl_0_DID_NOT_START_AFTER_1_MS.toLocalizedString(new Object[] {locs[i], Integer.valueOf(TIMEOUT_MS)}));
        }

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new AdminException(LocalizedStrings.AdminDistributedSystemImpl_INTERRUPTED_WHILE_WAITING_FOR_0_TO_START.toLocalizedString(locs[i]), ex);
      }
    }

    CacheServer[] servers = getCacheServers();
    for (int i = 0; i < servers.length; i++) {
      servers[i].start();
    }
    for (int i = 0; i < servers.length; i++) {
      try {
        if (!servers[i].waitToStart(TIMEOUT_MS)) {
          throw new AdminException(LocalizedStrings.AdminDistributedSystemImpl_0_DID_NOT_START_AFTER_1_MS.toLocalizedString(new Object[] {servers[i], Integer.valueOf(TIMEOUT_MS)}));
        }

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new AdminException(LocalizedStrings.AdminDistributedSystemImpl_INTERRUPTED_WHILE_WAITING_FOR_0_TO_START.toLocalizedString(servers[i]), ex);
      }
    }
  }
  
  /**
   * Stops all GemFire managers that are members of this system.
   */
  public void stop() throws AdminException {
    // Stop cache server before GemFire managers because the cache
    // server might host a cache proxy that is dependent on the
    // manager.  See bug 32569.

    // Wait for each managed entity to stop (see bug 32569)
    long timeout = 30;

    CacheServer[] servers = getCacheServers();
    for (int i = 0; i < servers.length; i++) {
      servers[i].stop();
    }
    for (int i = 0; i < servers.length; i++) {
      try {
        if (!servers[i].waitToStop(timeout * 1000)) {
          throw new AdminException(LocalizedStrings.AdminDistributedSystemImpl_0_DID_NOT_STOP_AFTER_1_SECONDS.toLocalizedString(new Object[] {servers[i], Long.valueOf(timeout)}));
        }

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new AdminException(LocalizedStrings.AdminDistributedSystemImpl_INTERRUPTED_WHILE_WAITING_FOR_0_TO_STOP.toLocalizedString(servers[i]), ex);
      }
    }

    DistributionLocator[] locs = getDistributionLocators();
    for (int i = 0; i < locs.length; i++) {
      locs[i].stop();
    }
    for (int i = 0; i < locs.length; i++) {
      try {
        if (!locs[i].waitToStop(timeout * 1000)) {
          throw new AdminException(LocalizedStrings.AdminDistributedSystemImpl_0_DID_NOT_STOP_AFTER_1_SECONDS.toLocalizedString(new Object[] {locs[i], Long.valueOf(timeout)}));
        }

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new AdminException(LocalizedStrings.AdminDistributedSystemImpl_INTERRUPTED_WHILE_WAITING_FOR_0_TO_STOP.toLocalizedString(locs[i]), ex);
      }
    }
  }
  
  /** Display merged system member logs */
  public String displayMergedLogs() {
    return this.logCollator.collateLogs(this.gfManagerAgent);
  }

   /**
   * Returns the license for this GemFire product; else null if unable to
   * retrieve license information
   *
   * @return license for this GemFire product
   */
  public java.util.Properties getLicense() {
    SystemMember member = findFirstRunningMember();
    if (member != null) {
      return new Properties();
    } else {
      return null;
    }
  }

  /**
   * Sets the distribution-related portion of the given managed entity's
   * configuration so that the entity is part of this distributed system.
   * 
   * @throws AdminException
   *                 TODO-javadocs
   */
  private void setDistributionParameters(SystemMember member) 
    throws AdminException {

    Assert.assertTrue(member instanceof ManagedSystemMemberImpl);

    // set some config parms to match this system...
    ConfigurationParameter[] configParms = new ConfigurationParameter[] {
        new ConfigurationParameterImpl(
            DistributionConfig.MCAST_PORT_NAME, 
            Integer.valueOf(this.config.getMcastPort())),
        new ConfigurationParameterImpl(
            DistributionConfig.LOCATORS_NAME, 
            this.config.getLocators()),
        new ConfigurationParameterImpl(
            DistributionConfig.MCAST_ADDRESS_NAME, 
            InetAddressUtil.toInetAddress(this.config.getMcastAddress())),
        new ConfigurationParameterImpl(
            DistributionConfig.DISABLE_TCP_NAME,
            Boolean.valueOf(this.config.getDisableTcp()) ),
      };
    member.setConfiguration(configParms);
  }

  /**
   * Handles an <code>ExecutionException</code> by examining its cause
   * and throwing an appropriate runtime exception.
   */
  private static void handle(ExecutionException ex) {
    Throwable cause = ex.getCause();

    if (cause instanceof OperationCancelledException) {
      // Operation was cancelled, we don't necessary want to propagate
      // this up to the user.
      return;
    }
    if (cause instanceof CancelException) { // bug 37285
      throw new FutureCancelledException(LocalizedStrings.AdminDistributedSystemImpl_FUTURE_CANCELLED_DUE_TO_SHUTDOWN.toLocalizedString(), ex);
    }

    // Don't just throw the cause because the stack trace can be
    // misleading.  For instance, the cause might have occurred in a
    // different thread.  In addition to the cause, we also want to
    // know which code was waiting for the Future.
    throw new RuntimeAdminException(LocalizedStrings.AdminDistributedSystemImpl_WHILE_WAITING_FOR_FUTURE.toLocalizedString(), ex);
  }
  
  protected void checkCancellation() {
    DM dm = this.getDistributionManager();
    // TODO does dm == null mean we're dead?
    if (dm != null) {
      dm.getCancelCriterion().checkCancelInProgress(null);
    }
  }
  /**
   * Returns a list of manageable SystemMember instances for each
   * member of this distributed system.
   *
   * @return array of system members for each non-manager member
   */
  public SystemMember[] getSystemMemberApplications()
  throws com.gemstone.gemfire.admin.AdminException {
    synchronized(this.applicationSet) {
      Collection coll = new ArrayList(this.applicationSet.size());
      APPS: for (Iterator iter = this.applicationSet.iterator();
           iter.hasNext(); ) {
        Future future = (Future) iter.next();
//         this.logger.info("DEBUG: getSystemMemberApplications: " + future);
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            coll.add(future.get());
            break;
          } 
          catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } 
          catch (CancellationException ex) {
//             this.logger.info("DEBUG: cancelled: " + future, ex);
            continue APPS;
          } 
          catch (ExecutionException ex) {
//             this.logger.info("DEBUG: executed: " + future);
            handle(ex);
            continue APPS;
          }
          finally {
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
  public String getLatestAlert() {
    if (this.latestAlert == null) {
      return "";
    }
    return this.latestAlert.toString();
  }
  
  /**
   * Connects to the currently configured system.
   */
  public void connect() {
    connect(this.logWriter);
  }

  /**
   * Connects to the currently configured system.  This method is
   * public for internal use only (testing, for example).
   *
   * <p>
   *
   * See {@link
   * com.gemstone.gemfire.distributed.DistributedSystem#connect} for a
   * list of exceptions that may be thrown.
   *
   * @param logWriter the InternalLogWriter to use for any logging
   */
  public void connect(InternalLogWriter logWriter) {
    synchronized (CONNECTION_SYNC) {
      //Check if the gfManagerAgent is NOT null. 
      //If it is already listening, then just return since the connection is already established OR in process.
      //Otherwise cleanup the state of AdminDistributedSystemImpl. This needs to happen automatically.
      if(this.gfManagerAgent != null) {
       if(this.gfManagerAgent.isListening()) {
         if (logger.isDebugEnabled()) {
           logger.debug("The RemoteGfManagerAgent is already listening for this AdminDistributedSystem.");
         }
         return;
       }
       this.disconnect();
      }
      
      if (thisAdminDS != null) { // TODO: beef up toString and add thisAdminDS
        throw new IllegalStateException(LocalizedStrings.AdminDistributedSystemImpl_ONLY_ONE_ADMINDISTRIBUTEDSYSTEM_CONNECTION_CAN_BE_MADE_AT_ONCE.toLocalizedString());
      }
      
      thisAdminDS = this; //added for feature requests #32887
      
      if (this.getLocators().length() == 0) {
        this.id =
          this.getMcastAddress() + "[" + this.getMcastPort() + "]";
  
      } else {
        this.id = this.getLocators();
      }
  
      if (this.config instanceof DistributedSystemConfigImpl) {
        ((DistributedSystemConfigImpl) this.config).validate();
        ((DistributedSystemConfigImpl) this.config).setDistributedSystem(this);
      }
  
      // LOG: passes the AdminDistributedSystemImpl LogWriterLogger into GfManagerAgentConfig for RemoteGfManagerAgent
      GfManagerAgent agent = 
          GfManagerAgentFactory.getManagerAgent(buildAgentConfig(logWriter));         
      this.gfManagerAgent = agent;
  
      // sync to prevent bug 33341 Admin API can double-represent system members
      synchronized(this.membershipListenerLock) {
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
  NEXT:
      while(st.hasMoreTokens()) {
        String locator = st.nextToken();
        int first = locator.indexOf("[");
        int last = locator.indexOf("]");
        String host = locator.substring(0, first);
        int colidx = host.lastIndexOf('@');
        if (colidx < 0) {
          colidx = host.lastIndexOf(':');
        }
        String bindAddr = null;
        if (colidx > 0 && colidx < (host.length()-1)) {
          String orig = host;
          bindAddr = host.substring(colidx+1, host.length());
          host = host.substring(0, colidx);
          // if the host contains a colon and there's no '@', we probably
          // parsed an ipv6 address incorrectly - try again
          if (host.indexOf(':') >= 0) {
            int bindidx = orig.lastIndexOf('@');
            if (bindidx >= 0) {
              host = orig.substring(0, bindidx);
              bindAddr = orig.substring(bindidx+1);
            }
            else {
              host = orig;
              bindAddr = null;
            }
          }
        }
        int port = Integer.parseInt(locator.substring(first+1, last));

        synchronized (this.locatorSet) {
          LOCATORS:
            for (Iterator iter = this.locatorSet.iterator();
               iter.hasNext(); ) {
            Future future = (Future) iter.next();
            DistributionLocatorImpl impl = null;
            for (;;) {
              checkCancellation();
              boolean interrupted = Thread.interrupted();
              try {
                impl = (DistributionLocatorImpl) future.get();
                break; // success
              } 
              catch (InterruptedException ex) {
                interrupted = true;
                continue; // keep trying
              } 
              catch (CancellationException ex) {
                continue LOCATORS;
              } 
              catch (ExecutionException ex) {
                handle(ex);
                continue LOCATORS;
              }
              finally {
                if (interrupted) {
                  Thread.currentThread().interrupt();
                }
              }
            } // for

            DistributionLocatorConfig conf = impl.getConfig();

            InetAddress host1 = InetAddressUtil.toInetAddress(host);
            InetAddress host2 =
              InetAddressUtil.toInetAddress(conf.getHost());
            if (port == conf.getPort() && host1.equals(host2)) {
              // Already have an admin object for this locator
              continue NEXT;
            }
          }
        }

        // None of the existing locators matches the locator in the
        // string.  Contact the locator to get information and create
        // an admin object for it.
        InetAddress bindAddress = null;
        if (bindAddr != null) {
          bindAddress = InetAddressUtil.toInetAddress(bindAddr);
        }
        DistributionLocatorConfig conf =
          DistributionLocatorConfigImpl.createConfigFor(host, port,
                                                        bindAddress);
        if (conf != null) {
          DistributionLocator impl = 
            createDistributionLocatorImpl(conf);
          synchronized (this.locatorSet) {
            this.locatorSet.add(new FutureResult(impl));
          }
        }
      }
    }
  }
  
  /**
   * Polls to determine whether or not the connection to the
   * distributed system has been made.
   */
  public boolean waitToBeConnected(long timeout) 
    throws InterruptedException {

    if (Thread.interrupted()) throw new InterruptedException();
    
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
   * @see com.gemstone.gemfire.distributed.DistributedSystem#disconnect()
   */
  public void disconnect() {
    synchronized (CONNECTION_SYNC) {
//      if (!isConnected()) {
//        throw new IllegalStateException(this + " is not connected");
//      }
//      Assert.assertTrue(thisAdminDS == this);
      if (this.logWriterAppender != null) {
        LogWriterAppenders.stop(LogWriterAppenders.Identifier.MAIN);
      }
      try {
      if (thisAdminDS == this) {
        thisAdminDS = null;
      }
      if (this.gfManagerAgent != null && this.gfManagerAgent.isListening()){
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
        if (logWriterAppender != null) {
          LogWriterAppenders.destroy(LogWriterAppenders.Identifier.MAIN);
        }
      }
    }
  }
  
  /**
   * Returns the DistributionManager this implementation is using to
   * connect to the distributed system.
   */
  public DM getDistributionManager() {
    if (this.gfManagerAgent == null) {
      return null;
    }
    return this.gfManagerAgent.getDM();
    
  }
  
  /**
   * Returns the internal admin API's agent used for administering
   * this <code>AdminDistributedSystem</code>.
   *
   * @since 4.0
   */
  public GfManagerAgent getAdminAgent() {
    return this.gfManagerAgent;
  }
  
  /**
   * Adds a new, unstarted <code>DistributionLocator</code> to this
   * distributed system.
   */
  public DistributionLocator addDistributionLocator() {
    DistributionLocatorConfig conf =
      new DistributionLocatorConfigImpl();
    DistributionLocator locator = 
      createDistributionLocatorImpl(conf);
    synchronized (this.locatorSet) {
      this.locatorSet.add(new FutureResult(locator));
    }

    // update locators string...
    setLocators(parseLocatorSet());
    return locator;
  }
  
  public DistributionLocator[] getDistributionLocators() {
    synchronized(this.locatorSet) {
      Collection coll = new ArrayList(this.locatorSet.size());
      LOCATORS: for (Iterator iter = this.locatorSet.iterator();
           iter.hasNext();) {
        Future future = (Future) iter.next();
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            coll.add(future.get());
            break; // success
          } 
          catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } 
          catch (CancellationException ex) {
            continue LOCATORS;
          } 
          catch (ExecutionException ex) {
            handle(ex);
            continue LOCATORS;
          }
          finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for
      }

      DistributionLocator[] array =
        new DistributionLocator[coll.size()];
      coll.toArray(array);
      return array;
    }
  }
  
  /**
   * Updates the locator string that is used to discover members of
   * the distributed system.
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
        } 
        catch (InterruptedException ex) {
          interrupted = true;
          continue; // keep trying
        } 
        catch (CancellationException ex) {
          continue LOCATORS;
        } 
        catch (ExecutionException ex) {
          handle(ex);
          continue LOCATORS;
        }
        finally {
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
  //   Listener callback methods
  // -------------------------------------------------------------------------
  
  /** sync to prevent bug 33341 Admin API can double-represent system members */
  private final Object membershipListenerLock = new Object();
  
  // --------- com.gemstone.gemfire.internal.admin.JoinLeaveListener ---------
  /** 
   * Listener callback for when a member has joined this DistributedSystem.
   * <p>
   * React by adding the SystemMember to this system's
   * internal lists, if they are not already there.  Notice that we
   * add a {@link Future} into the list so that the admin object is
   * not initialized while locks are held.
   *
   * @param source  the distributed system that fired nodeJoined
   * @param vm  the VM that joined
   * @see com.gemstone.gemfire.internal.admin.JoinLeaveListener#nodeJoined
   */
  public void nodeJoined(GfManagerAgent source, final GemFireVM vm) {
    // sync to prevent bug 33341 Admin API can double-represent system members
    synchronized(this.membershipListenerLock) {
//     this.logger.info("DEBUG: nodeJoined: " + vm.getId(), new RuntimeException("STACK"));

    // does it already exist?
    SystemMember member = findSystemMember(vm);
    
    // if not then create it...
    if (member == null) {
//       this.logger.info("DEBUG: no existing member: " + vm.getId());
      FutureTask future = null;
      //try {
        if (vm instanceof ApplicationVM) {
          final ApplicationVM app = (ApplicationVM) vm;
          if (app.isDedicatedCacheServer()) {
            synchronized (this.cacheServerSet) {
              future = new AdminFutureTask(vm.getId(), new Callable() {
                  public Object call() throws Exception {
                    logger.info(LogMarker.DM, LocalizedMessage.create(LocalizedStrings.AdminDistributedSystemImpl_ADDING_NEW_CACHESERVER_FOR__0, vm));
                    return createCacheServer(app);
                  }
                });
                                      
              this.cacheServerSet.add(future);
            }

          } else {
            synchronized (this.applicationSet) {
              future = new AdminFutureTask(vm.getId(), new Callable() {
                  public Object call() throws Exception {
                    logger.info(LogMarker.DM, LocalizedMessage.create(LocalizedStrings.AdminDistributedSystemImpl_ADDING_NEW_APPLICATION_FOR__0, vm));
                    return createSystemMember(app); 
                  }
                });
              this.applicationSet.add(future);
            }
          }

        } else {
          Assert.assertTrue(false, "Unknown GemFireVM type: " +
                            vm.getClass().getName());
        } 

//      } catch (AdminException ex) {
//        String s = "Could not create a SystemMember for " + vm;
//        this.logger.warning(s, ex);
//      }

      // Wait for the SystemMember to be created.  We want to do this
      // outside of the "set" locks.
      future.run();
      for (;;) {
        checkCancellation();
        boolean interrupted = Thread.interrupted();
        try {
          member = (SystemMember) future.get();
          break; // success
        } 
        catch (InterruptedException ex) {
          interrupted = true;
          continue; // keep trying
        } 
        catch (CancellationException ex) {
//           this.logger.info("DEBUG: run cancelled: " + future, ex);
          return;
        } 
        catch (ExecutionException ex) {
//           this.logger.info("DEBUG: run executed: " + future, ex);
          handle(ex);
          return;
        }
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // for

      Assert.assertTrue(member != null);

      // moved this up into the if that creates a new member to fix bug 34517
      SystemMembershipEvent event = new SystemMembershipEventImpl(member.getDistributedMember());
      for (Iterator iter = this.membershipListeners.iterator();
           iter.hasNext(); ) {
        SystemMembershipListener listener =
          (SystemMembershipListener) iter.next();
        listener.memberJoined(event);
      }
//     } else {
//       this.logger.info("DEBUG: found existing member: " + member);
    }

    }
  }
  
  /** 
   * Listener callback for when a member of this DistributedSystem has left.
   * <p>
   * Reacts by removing the member.
   *
   * @param source  the distributed system that fired nodeCrashed
   * @param vm    the VM that left
   * @see com.gemstone.gemfire.internal.admin.JoinLeaveListener#nodeLeft
   */
  public void nodeLeft(GfManagerAgent source, GemFireVM vm) {
    // sync to prevent bug 33341 Admin API can double-represent system members
    synchronized(this.membershipListenerLock) {
      // member has left...
      SystemMember member = 
          AdminDistributedSystemImpl.this.removeSystemMember(vm.getId());
      if (member == null) {
        return; // reinstated this early-out because removal does not fix 39429
      }
  
      // Can't call member.getId() because it is nulled-out when the
      // SystemMember is removed.
      SystemMembershipEvent event = new SystemMembershipEventImpl(vm.getId());
      for (Iterator iter = this.membershipListeners.iterator();
           iter.hasNext(); ) {
        SystemMembershipListener listener =
          (SystemMembershipListener) iter.next();
        listener.memberLeft(event);
      }
    }
  }
  
  /** 
   * Listener callback for when a member of this DistributedSystem has crashed.
   * <p>
   * Reacts by removing the member.
   *
   * @param source  the distributed system that fired nodeCrashed
   * @param vm the VM that crashed
   * @see com.gemstone.gemfire.internal.admin.JoinLeaveListener#nodeCrashed
   */
  public void nodeCrashed(GfManagerAgent source, GemFireVM vm) {
    // sync to prevent bug 33341 Admin API can double-represent system members
    synchronized(this.membershipListenerLock) {
      // member has crashed...
      SystemMember member = 
        AdminDistributedSystemImpl.this.removeSystemMember(vm.getId());
      if (member == null) {
        // Unknown member crashed.  Hmm...
        return;
      }

      // Can't call member.getId() because it is nulled-out when the
      // SystemMember is removed.
      SystemMembershipEvent event = new SystemMembershipEventImpl(vm.getId());
      for (Iterator iter = this.membershipListeners.iterator();
      iter.hasNext(); ) {
        SystemMembershipListener listener =
          (SystemMembershipListener) iter.next();
        listener.memberCrashed(event);
      }
    }
  }

  // ----------- com.gemstone.gemfire.internal.admin.AlertListener -----------
  /** 
   * Listener callback for when a SystemMember of this DistributedSystem has 
   * crashed. 
   *
   * @param alert   the latest alert from the system
   * @see com.gemstone.gemfire.internal.admin.AlertListener#alert
   */
  public void alert(com.gemstone.gemfire.internal.admin.Alert alert) {
    if (AlertLevel.forSeverity(alert.getLevel()).ordinal < alertLevel.ordinal) {
      return;
    }
    Alert alert2 = new AlertImpl(alert);
    this.latestAlert = alert2;
    for (Iterator<AlertListener> iter = this.alertListeners.iterator();
         iter.hasNext(); ) {
      AlertListener listener = iter.next();
      listener.alert(alert2);
    }
  }
  
  public void onDisconnect(InternalDistributedSystem sys) {
   logger.debug("Calling AdminDistributedSystemImpl#onDisconnect");	 
   disconnect();
   logger.debug("Completed AdminDistributedSystemImpl#onDisconnect");
  }
  
  // -------------------------------------------------------------------------
  //   Template methods overriden from superclass...
  // -------------------------------------------------------------------------
  
  protected CacheServer createCacheServer(ApplicationVM member) 
    throws AdminException {

    return new CacheServerImpl(this, member);
  }

  protected CacheServer createCacheServer(CacheServerConfigImpl conf) 
    throws AdminException {

    return new CacheServerImpl(this, conf);
  }

  /** Override createSystemMember by instantiating SystemMemberImpl
   * 
   *  @throws AdminException TODO-javadocs
   */
  protected SystemMember createSystemMember(ApplicationVM app)
  throws com.gemstone.gemfire.admin.AdminException {
    return new SystemMemberImpl(this, app);
  }

  /**
   * Constructs & returns a SystemMember instance using the corresponding
   * InternalDistributedMember object.
   * 
   * @param member
   *          InternalDistributedMember instance for which a SystemMember
   *          instance is to be constructed.
   * @return constructed SystemMember instance
   * @throws com.gemstone.gemfire.admin.AdminException
   *           if construction of SystemMember instance fails
   * @since 6.5
   */
  protected SystemMember createSystemMember(InternalDistributedMember member)
    throws com.gemstone.gemfire.admin.AdminException {
    return new SystemMemberImpl(this, member);
  }

  /** 
   * Template-method for creating a new
   * <code>DistributionLocatorImpl</code> instance.  
   */
  protected DistributionLocatorImpl
    createDistributionLocatorImpl(DistributionLocatorConfig conf) {
    return new DistributionLocatorImpl(conf, this);
  }
  
  // -------------------------------------------------------------------------
  //   Non-public implementation methods... TODO: narrow access levels
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
      String mcastId = new StringBuffer(
          this.getMcastAddress()).append("[").append(
          this.getMcastPort()).append("]").toString();
      locatorIds.add(new DistributionLocatorId(mcastId));
    }
    StringTokenizer st = new StringTokenizer(this.getLocators(), ",");
    while (st.hasMoreTokens()) {
      locatorIds.add(new DistributionLocatorId(st.nextToken()));
    }

    if (logger.isDebugEnabled()) {
      StringBuffer sb = new StringBuffer("Locator set is: ");
      for (Iterator iter = locatorIds.iterator(); iter.hasNext(); ) {
        sb.append(iter.next());
        sb.append(" ");
      }
      logger.debug(sb);
    }

    return locatorIds;
  }
  
  /**
   * Returns whether or not a <code>SystemMember</code> corresponds
   * to a <code>GemFireVM</code>.
   *
   * @param examineConfig
   *        Should we take the configuration of the member into
   *        consideration?  In general, we want to consider the
   *        configuration when a member starts up.  But when we are
   *        notified that it has shut down, we do not want to examine
   *        the configuration because that might involve contacting
   *        the member.  Which, of course, cannot be done because it
   *        has shut down.
   */
  private boolean isSame(SystemMemberImpl member, GemFireVM vm,
                         boolean examineConfig) {
    if (vm.equals(member.getGemFireVM())) {
      return true;
    }

    InternalDistributedMember memberId = member.getInternalId();
    InternalDistributedMember vmId = vm.getId();

    if (vmId.equals(memberId)) {
      return true;
    }

    if ((member instanceof ManagedSystemMemberImpl) &&
        examineConfig) {

      // We can't compare information about managers because the
      // member might have already gone away.  Attempts to send it
      // messages (to get its product directory, for instance) will
      // time out.

      ManagedSystemMemberImpl entity =
        (ManagedSystemMemberImpl) member;

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
      InetAddress managedHost =
        InetAddressUtil.toInetAddress(conf.getHost());
      File managedWorkingDir = new File(conf.getWorkingDirectory());
      File managedProdDir = new File(conf.getProductDirectory());
      
      InetAddress vmHost = vm.getHost();
      File vmWorkingDir = vm.getWorkingDirectory();
      File vmProdDir = vm.getGemFireDir();

      if (vmHost.equals(managedHost) && 
          isSameFile(vmWorkingDir, managedWorkingDir) &&
          isSameFile(vmProdDir, managedProdDir)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns whether or not the names of the two files represent the
   * same file.
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

//       StringBuffer sb = new StringBuffer();
//       sb.append("File 1: ");
//       sb.append(file1);
//       sb.append("\nFile 2: ");
//       sb.append(file2);
//       sb.append("\n  Absolute 1: ");
//       sb.append(file1.getAbsoluteFile());
//       sb.append("\n  Absolute 2: ");
//       sb.append(file2.getAbsoluteFile());
//       sb.append("\n  Canonical 1: ");
//       sb.append(file1.getCanonicalFile());
//       sb.append("\n  Canonical 2: ");
//       sb.append(file2.getCanonicalFile());
//       logger.info(sb.toString());

    } catch (IOException ex) {
      // oh well...
      logger.info(LocalizedMessage.create(LocalizedStrings.AdminDistributedSystemImpl_WHILE_GETTING_CANONICAL_FILE), ex);
    }

    return false;
  }

  /**
   * Finds and returns the <code>SystemMember</code> that corresponds
   * to the given <code>GemFireVM</code> or <code>null</code> if no
   * <code>SystemMember</code> corresponds.
   */
  protected SystemMember findSystemMember(GemFireVM vm) {
    return findSystemMember(vm, true);
  }

  /**
   * Finds and returns the <code>SystemMember</code> that corresponds to the
   * given <code>GemFireVM</code> or <code>null</code> if no Finds and returns
   * the <code>SystemMember</code> that corresponds to the given
   * <code>GemFireVM</code> or <code>null</code> if no <code>SystemMember</code>
   * corresponds.
   * 
   * 
   * @param vm
   *          GemFireVM instance
   * @param compareConfig
   *          Should the members' configurations be compared? <code>true</code>
   *          when the member has joined, <code>false</code> when the member has
   *          left Should the members' configurations be compared?
   *          <code>true</code> when the member has joined, <code>false</code>
   *          when the member has left. Additionally also used to check if system 
   *          member config is to be synchronized with the VM.
   */
   protected SystemMember findSystemMember(GemFireVM vm,
                                           boolean compareConfig) {

    SystemMemberImpl member = null;

      synchronized (this.cacheServerSet) {
        SERVERS: for (Iterator iter = this.cacheServerSet.iterator();
             iter.hasNext(); ) {
          Future future = (Future) iter.next();
          CacheServerImpl cacheServer = null;
          for (;;) {
            checkCancellation();
            boolean interrupted = Thread.interrupted();
            try {
              cacheServer = (CacheServerImpl) future.get();
              break; // success
            } 
            catch (InterruptedException ex) {
              interrupted = true;
              continue; // keep trying
            } 
            catch (CancellationException ex) {
              continue SERVERS;
            } 
            catch (ExecutionException ex) {
              handle(ex);
              continue SERVERS;
            }
            finally {
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
        APPS: for (Iterator iter = this.applicationSet.iterator();
             iter.hasNext(); ) {
          Future future = (Future) iter.next();
          SystemMemberImpl application = null;
          for (;;) {
            checkCancellation();
            boolean interrupted = Thread.interrupted();
            try {
              application = (SystemMemberImpl) future.get();
              break; // success
            } 
            catch (InterruptedException ex) {
              interrupted = true;
              continue; // keep trying
            } 
            catch (CancellationException ex) {
              continue APPS;
            } 
            catch (ExecutionException ex) {
              handle(ex);
              continue APPS;
            }
            finally {
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
        logger.warn(LocalizedMessage.create(LocalizedStrings.AdminDistributedSystem_COULD_NOT_SET_THE_GEMFIRE_VM), ex);
      }
    }

    return member;
  }
  
  /** 
   * Removes a SystemMember from this system's list of known members.
   *
   * @param systemMember  the member to remove
   * @return the system member that was removed; null if no match was found
   */
  protected SystemMember removeSystemMember(SystemMember systemMember) {
    return removeSystemMember(
        ((SystemMemberImpl) systemMember).getInternalId());
  }
  
  /** 
   * Removes a SystemMember from this system's list of known members.  This 
   * method is called in response to a member leaving the system.
   * TODO: this method is a mess of defns
   *
   * @param internalId  the unique id that specifies which member to remove
   * @return the system member that was removed; null if no match was found
   */
  protected SystemMember removeSystemMember(InternalDistributedMember internalId) {
    if (internalId == null) return null;

//     this.logger.info("DEBUG: removeSystemMember: " + internalId, new RuntimeException("STACK"));

    boolean found = false;
    SystemMemberImpl member = null;

    synchronized(this.cacheServerSet) {
      SERVERS: for (Iterator iter = this.cacheServerSet.iterator();
           iter.hasNext() && !found; ) {
        Future future = (Future) iter.next();
        if (future instanceof AdminFutureTask) {
          AdminFutureTask task = (AdminFutureTask) future;
          if (task.getMemberId().equals(internalId)) {
//             this.logger.info("DEBUG: removeSystemMember cs cancelling: " + future);
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
            return null;          // Dead code
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

    synchronized(this.applicationSet) {
      for (Iterator iter = this.applicationSet.iterator();
           iter.hasNext() && !found; ) {
        Future future = (Future) iter.next();
        try {
          if (future instanceof AdminFutureTask) {
            AdminFutureTask task = (AdminFutureTask) future;
            if (task.getMemberId().equals(internalId)) {
              iter.remove();        // Only remove applications
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
//             this.logger.info("DEBUG: removeSystemMember as cancelling: " + future);
            future.cancel(true);
          }

        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          checkCancellation();
          throw new RuntimeException(LocalizedStrings.AdminDistributedSystemImpl_INTERRUPTED.toLocalizedString(), ex);
          
        } catch (CancellationException ex) {
          continue;

        } catch (ExecutionException ex) {
          handle(ex);
          return null;          // Dead code
        }

        InternalDistributedMember applicationId = member.getInternalId();
        if (internalId.equals(applicationId)) {
          // found a match...
          iter.remove();        // Only remove applications
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
        logger.fatal(LocalizedMessage.create(LocalizedStrings.AdminDistributedSystem_UNEXPECTED_ADMINEXCEPTION), ex);
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
   * Builds the configuration needed to connect to a GfManagerAgent which is the
   * main gateway into the internal.admin api.  GfManagerAgent is used to 
   * actually connect to the distributed gemfire system.
   *
   * @param logWriter the LogWriterI18n to use for any logging
   * @return the configuration needed to connect to a GfManagerAgent
   */
  // LOG: saves LogWriterLogger from AdminDistributedSystemImpl for RemoteGfManagerAgentConfig
  private GfManagerAgentConfig buildAgentConfig(InternalLogWriter logWriter) {
    RemoteTransportConfig conf = new RemoteTransportConfig(
        isMcastEnabled(), getDisableTcp(),
        getDisableAutoReconnect(),
        getBindAddress(), buildSSLConfig(), parseLocators(), 
        getMembershipPortRange(), getTcpPort(), DistributionManager.ADMIN_ONLY_DM_TYPE);
    return new GfManagerAgentConfig(
        getSystemName(), conf, logWriter, this.alertLevel.getSeverity(), this, this);
  }
  
  protected SSLConfig buildSSLConfig() {
    SSLConfig conf = new SSLConfig();
    if (getConfig() != null) {
      conf.setEnabled(getConfig().isSSLEnabled());
      conf.setProtocols(getConfig().getSSLProtocols());
      conf.setCiphers(getConfig().getSSLCiphers());
      conf.setRequireAuth(getConfig().isSSLAuthenticationRequired());
      conf.setProperties(getConfig().getSSLProperties());
    }
    return conf;
  }
  
  /**
   * Returns the currently configured address to bind to when administering
   * this system.
   */
  private String getBindAddress() {
    return this.config.getBindAddress();
  }

  /** Returns whether or not the given member is running */
  private boolean isRunning(SystemMember member) {
    if (member instanceof ManagedEntity) {
      return ((ManagedEntity) member).isRunning();

    } else {
      // member must be an application VM.  It is running
      return true;
    }
  }

  /** Returns any member manager that is known to be running */
  private SystemMember findFirstRunningMember() {
    synchronized(this.cacheServerSet) {
      SERVERS: for (Iterator iter = this.cacheServerSet.iterator();
           iter.hasNext();){
        Future future = (Future) iter.next();
        SystemMember member = null;
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            member = (SystemMember) future.get();
            break; // success
          } 
          catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } 
          catch (CancellationException ex) {
            continue SERVERS;
          } 
          catch (ExecutionException ex) {
            handle(ex);
            return null;          // Dead code
          }
          finally {
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

    synchronized(this.applicationSet) {
      APPS: for (Iterator iter = this.applicationSet.iterator();
           iter.hasNext();) {
        Future future = (Future) iter.next();
        SystemMember member = null;
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            member = (SystemMember) future.get();
            break; // success
          } 
          catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } 
          catch (CancellationException ex) {
            continue APPS;
          } 
          catch (ExecutionException ex) {
            handle(ex);
            return null;          // Dead code
          }
          finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } //  for

        if (isRunning(member)) {
          return member;
        }
      } // APPS
    }

    return null;
  }

  /**
   * Returns the instance of system member that is running either as a CacheVm
   * or only ApplicationVm for the given string representation of the id.
   * 
   * @param memberId
   *          string representation of the member identifier
   * @return instance of system member which could be either as a CacheVm or
   *         Application VM
   */
  protected SystemMember findCacheOrAppVmById(String memberId) {
    SystemMember found = null;
    
    if (memberId != null) {
      try {
        boolean foundSender = false;
        CacheVm[] cacheVms = getCacheVms();
        
        /* cacheVms could be null. See 
         * AdminDistributedSystemImpl.getCacheVmsCollection() for 
         * ExecutionException */
        if (cacheVms != null) {
          for (CacheVm cacheVm : cacheVms) {
            if (cacheVm.getId().equals(memberId) && 
                cacheVm instanceof CacheVm) {
              found = (SystemMember) cacheVm;    
              foundSender = true;
              break;
            }
          }
        }
        
        if (!foundSender) {
          SystemMember[] appVms = getSystemMemberApplications();
          
          for (SystemMember appVm : appVms) {
            if (appVm.getId().equals(memberId) && 
                appVm instanceof SystemMember) {
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
  //   Health methods
  // -------------------------------------------------------------------------
  
  /**
   * Lazily initializes the GemFire health monitor 
   *
   * @see #createGemFireHealth
   */
  public final GemFireHealth getGemFireHealth() {
    synchronized (this) {
      if (this.health == null || this.health.isClosed()) {
        try {
          this.health = createGemFireHealth(this.gfManagerAgent);

        } catch (AdminException ex) {
          throw new RuntimeAdminException(LocalizedStrings.AdminDistributedSystemImpl_AN_ADMINEXCEPTION_WAS_THROWN_WHILE_GETTING_THE_GEMFIRE_HEALTH.toLocalizedString(), ex);
        }
      }

      return this.health;
    }
  }

  /**
   * A "template factory" method for creating an instance of
   * <code>GemFireHealth</code>.  It can be overridden by subclasses
   * to produce instances of different <code>GemFireHealth</code>
   * implementations.
   *
   * @see #getGemFireHealth
   */
  protected GemFireHealth createGemFireHealth(GfManagerAgent agent) 
    throws AdminException {

    if (agent == null) {
      throw new IllegalStateException(LocalizedStrings.AdminDistributedSystemImpl_GFMANAGERAGENT_MUST_NOT_BE_NULL.toLocalizedString());
    }
    return new GemFireHealthImpl(agent, this);
  }
    
  public CacheVm addCacheVm() throws AdminException {
    return (CacheVm)addCacheServer();
  }
  
  public CacheServer addCacheServer() throws AdminException {
    CacheServerConfigImpl conf = new CacheServerConfigImpl();
    CacheServer server  = createCacheServer(conf);
    setDistributionParameters(server);

    synchronized (this.cacheServerSet) {
      this.cacheServerSet.add(new FutureResult(server));
    }

    return server;
  }

  private Collection getCacheVmsCollection() throws AdminException {
    synchronized(this.cacheServerSet) {
      Collection coll = new ArrayList(this.cacheServerSet.size());
      SERVERS: for (Iterator iter = this.cacheServerSet.iterator();
           iter.hasNext(); ) {
        Future future = (Future) iter.next();
        Object get = null;
        for (;;) {
          checkCancellation();
          boolean interrupted = Thread.interrupted();
          try {
            get = future.get();
            break; // success
          } 
          catch (InterruptedException ex) {
            interrupted = true;
            continue; // keep trying
          } 
          catch (CancellationException ex) {
            continue SERVERS;
          } 
          catch (ExecutionException ex) {
            handle(ex);
            return null;          // Dead code
          }
          finally {
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
   * Returns all the cache server members of the distributed system which are
   * hosting a client queue for the particular durable-client having the given
   * durableClientId
   * 
   * @param durableClientId -
   *                durable-id of the client
   * @return array of CacheServer(s) having the queue for the durable client
   * @throws AdminException
   * 
   * @since 5.6
   */
  public CacheServer[] getCacheServers(String durableClientId)
      throws AdminException
  {
    Collection serversForDurableClient = new ArrayList();
    CacheServer[] servers = getCacheServers();

    for (int i = 0; i < servers.length; i++) {
      RemoteApplicationVM vm = (RemoteApplicationVM)((CacheServerImpl)servers[i])
          .getGemFireVM();
      if (vm != null && vm.hasDurableClient(durableClientId)) {
        serversForDurableClient.add(servers[i]);
      }
    }
    CacheServer[] array = new CacheServer[serversForDurableClient.size()];
    serversForDurableClient.toArray(array);
    return array;
  }
  
  public CacheVm[] getCacheVms() throws AdminException {
    Collection coll = getCacheVmsCollection();
    if (coll == null) return null;
    CacheVm[] array = new CacheVm[coll.size()];
    coll.toArray(array);
    return array;
  }
  public CacheServer[] getCacheServers() throws AdminException {
    Collection coll = getCacheVmsCollection();
    if (coll == null) return null;
    CacheServer[] array = new CacheServer[coll.size()];
    coll.toArray(array);
    return array;
  }

  // -------------------------------------------------------------------------
  //   Overriden java.lang.Object methods
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
   * @return AdminDistributedSystem
   */
  public static AdminDistributedSystemImpl getConnectedInstance() {
    synchronized (CONNECTION_SYNC) {
      return thisAdminDS;
    }
  }
  
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

  public SystemMember lookupSystemMember(DistributedMember distributedMember) 
  throws AdminException {
    if (distributedMember == null) return null;
    SystemMember[] members = getSystemMemberApplications();
    for (int i = 0; i < members.length; i++) {
      if (distributedMember.equals(members[i].getDistributedMember())) {
        return members[i];
      }
    }
    return null;
  }
  
  ////////////////////////  Inner Classes  ////////////////////////

  /**
   * Object that converts an <code>internal.admin.Alert</code> into an
   * external <code>admin.Alert</code>.
   */
  public class AlertImpl implements Alert {
    /** The Alert to which most behavior is delegated */
    private final com.gemstone.gemfire.internal.admin.Alert alert;
    private SystemMember systemMember;

    ///////////////////////  Constructors  ///////////////////////

    /**
     * Creates a new <code>Alert</code> that delegates to the given
     * object. 
     */
    AlertImpl(com.gemstone.gemfire.internal.admin.Alert alert) {
      this.alert   = alert;
      GemFireVM vm = alert.getGemFireVM();

      /*
       * Related to #39657.
       * Avoid setting GemFireVM again in the system member.
       * Eager initialization of member variable - systemMember.
       */
      this.systemMember = vm == null ? null : findSystemMember(vm, false);
      if (this.systemMember == null) {
        /*
         * try to use sender information to construct the SystemMember that can
         * be used for disply purpose at least
         */
        InternalDistributedMember sender = alert.getSender();
        if (sender != null) {
          try {
            this.systemMember = 
              AdminDistributedSystemImpl.this.createSystemMember(sender);
          } catch (AdminException e) {
            /*
             * AdminException might be thrown if creation of System Member
             * instance fails.
             */
            this.systemMember = null;
          }
        } //else this.systemMember will be null
      }
    }
    
    //////////////////////  Instance Methods  //////////////////////

    public AlertLevel getLevel() {
      return AlertLevel.forSeverity(alert.getLevel());
    }

    /*
     * Eager initialization of system member is done while creating this alert 
     * only.
     */
    public SystemMember getSystemMember() {
      return systemMember;
    }

    public String getConnectionName() {
      return alert.getConnectionName();
    }

    public String getSourceId() {
      return alert.getSourceId();
    }

    public String getMessage() {
      return alert.getMessage();
    }

    public java.util.Date getDate() {
      return alert.getDate();
    }

    @Override
    public String toString() {
      return alert.toString();
    }
  }
  
  /**
   * A JSR-166 <code>FutureTask</code> whose {@link #get} method
   * properly handles an <code>ExecutionException</code> that wraps an
   * <code>InterruptedException</code>.  This is necessary because
   * there are places in the admin API that wrap
   * <code>InterruptedException</code>s.  See bug 32634.
   *
   * <P>
   *
   * This is by no means an ideal solution to this problem.  It would
   * be better to modify the code invoked by the <code>Callable</code>
   * to explicitly throw <code>InterruptedException</code>.
   */
  static class AdminFutureTask extends FutureTask  {

    /** The id of the member whose admin object we are creating.
     * Keeping track of this allows us to cancel a FutureTask for a
     * member that has gone away. */
    private final InternalDistributedMember memberId;

    public AdminFutureTask(InternalDistributedMember memberId,
                           Callable callable) {
      super(callable);
      this.memberId = memberId;
    }

    /**
     * Returns the id of the member of the distributed system for
     * which this <code>FutureTask</code> is doing work.
     */
    public InternalDistributedMember getMemberId() {
      return this.memberId;
    }

    /**
     * If the <code>ExecutionException</code> is caused by an
     * <code>InterruptedException</code>, throw the
     * <code>CancellationException</code> instead.
     */
    @Override
    public Object get()
      throws InterruptedException, ExecutionException {

      if (Thread.interrupted()) throw new InterruptedException();
      try {
        return super.get();

      } catch (ExecutionException ex) {
        for (Throwable cause = ex.getCause(); cause != null;
             cause = cause.getCause()) {
          if (cause instanceof InterruptedException) {
            // We interrupted the runnable but we don't want the thread
            // that called get to think he was interrupted.
            CancellationException ex2 = new CancellationException(LocalizedStrings.AdminDistributedSystemImpl_BY_INTERRUPT.toLocalizedString());
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
    connect((InternalLogWriter)this.logWriter);
    try {
      thisAdminDS.waitToBeConnected(3000);
    } catch (InterruptedException ie) {
      logger.warn("Interrupted while waiting to connect", ie);
    }
  }
  
  public Set<PersistentID> getMissingPersistentMembers()
      throws AdminException {
    connectAdminDS();
    DM dm = getDistributionManager();
    if(dm == null) {
      throw new IllegalStateException(LocalizedStrings.AdminDistributedSystemImpl_CONNECT_HAS_NOT_BEEN_INVOKED_ON_THIS_ADMINDISTRIBUTEDSYSTEM.toLocalizedString());
    }
    return getMissingPersistentMembers(dm);
  }

  public static Set<PersistentID> getMissingPersistentMembers(DM dm) {
    return MissingPersistentIDsRequest.send(dm);
  }

  public void revokePersistentMember(InetAddress host,
      String directory) throws AdminException {
    connectAdminDS();
    DM dm = getDistributionManager();
    if(dm == null) {
      throw new IllegalStateException(LocalizedStrings.AdminDistributedSystemImpl_CONNECT_HAS_NOT_BEEN_INVOKED_ON_THIS_ADMINDISTRIBUTEDSYSTEM.toLocalizedString());
    }
    revokePersistentMember(dm, host, directory);
    
  }
  
  public void revokePersistentMember(UUID diskStoreID) throws AdminException {
    connectAdminDS();
    DM dm = getDistributionManager();
    if(dm == null) {
      throw new IllegalStateException(LocalizedStrings.AdminDistributedSystemImpl_CONNECT_HAS_NOT_BEEN_INVOKED_ON_THIS_ADMINDISTRIBUTEDSYSTEM.toLocalizedString());
    }
    revokePersistentMember(dm, diskStoreID);
    
  }
  
  public static void revokePersistentMember(DM dm, UUID diskStoreID) {
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
      
      //Fix for 42607 - verify that the persistent id is not already
      //running before revoking it.
      PrepareRevokePersistentIDRequest.send(dm, pattern);
      success = true;
    } finally {
      if(success) {
        //revoke the persistent member if were able to prepare the revoke
        RevokePersistentIDRequest.send(dm, pattern);
      } else {
        //otherwise, cancel the revoke.
        PrepareRevokePersistentIDRequest.cancel(dm, pattern);
      }
    }
  }

  /**
   * 
   * @deprecated use {@link #revokePersistentMember(UUID)} instead
   */
  public static void revokePersistentMember(DM dm, InetAddress host, String directory) {
    
    PersistentMemberPattern pattern = new PersistentMemberPattern(host, directory, System.currentTimeMillis());
    boolean success = false;
    try {
      //Fix for 42607 - verify that the persistent id is not already
      //running before revoking it.
      PrepareRevokePersistentIDRequest.send(dm, pattern);
      success = true;
    } finally {
      if(success) {
        //revoke the persistent member if were able to prepare the revoke
        RevokePersistentIDRequest.send(dm, pattern);
      } else {
        //otherwise, cancel the revoke.
        PrepareRevokePersistentIDRequest.cancel(dm, pattern);
      }
    }
  }
  
  public Set shutDownAllMembers() throws AdminException {
    return shutDownAllMembers(0);
  }
  
  public Set shutDownAllMembers(long timeout) throws AdminException {
    connectAdminDS();
    DM dm = getDistributionManager();
    if(dm == null) {
      throw new IllegalStateException(LocalizedStrings.AdminDistributedSystemImpl_CONNECT_HAS_NOT_BEEN_INVOKED_ON_THIS_ADMINDISTRIBUTEDSYSTEM.toLocalizedString());
    }
    return shutDownAllMembers(dm, timeout);
  }

  /**
   * Shutdown all members.
   * @param dm
   * @param timeout the amount of time (in ms) to spending trying to shutdown the members
   * gracefully. After this time period, the members will be forceable shut down. If the
   * timeout is exceeded, persistent recovery after the shutdown may need to do a GII. -1
   *  indicates that the shutdown should wait forever. 
   */
  public static Set shutDownAllMembers(DM dm, long timeout) {
    return ShutdownAllRequest.send(dm, timeout);
  }
  
  public BackupStatus backupAllMembers(File targetDir) throws AdminException {
    return backupAllMembers(targetDir, null);
  }
  
  public BackupStatus backupAllMembers(File targetDir, File baselineDir) throws AdminException {
    connectAdminDS();
    DM dm = getDistributionManager();
    if(dm == null) {
      throw new IllegalStateException(LocalizedStrings.AdminDistributedSystemImpl_CONNECT_HAS_NOT_BEEN_INVOKED_ON_THIS_ADMINDISTRIBUTEDSYSTEM.toLocalizedString());
    }
    return backupAllMembers(dm, targetDir, baselineDir);
  }

  public static BackupStatus backupAllMembers(DM dm, File targetDir, File baselineDir)
      throws AdminException {
    Set<PersistentID> missingMembers = getMissingPersistentMembers(dm);
    Set recipients = dm.getOtherDistributionManagerIds();
    
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    targetDir = new File(targetDir, format.format(new Date()));
    FlushToDiskRequest.send(dm, recipients);
    Map<DistributedMember, Set<PersistentID>> existingDataStores 
        = PrepareBackupRequest.send(dm, recipients);
    Map<DistributedMember, Set<PersistentID>> successfulMembers 
        = FinishBackupRequest.send(dm, recipients, targetDir, baselineDir);
    
    // It's possible that when calling getMissingPersistentMembers, some members are 
    // still creating/recovering regions, and at FinishBackupRequest.send, the 
    // regions at the members are ready. Logically, since the members in successfulMembers
    // should override the previous missingMembers
    for(Set<PersistentID> onlineMembersIds : successfulMembers.values()) {
      missingMembers.removeAll(onlineMembersIds);
    }
    
    existingDataStores.keySet().removeAll(successfulMembers.keySet());
    for(Set<PersistentID> lostMembersIds : existingDataStores.values()) {
      missingMembers.addAll(lostMembersIds);
    }
    
    return new BackupStatusImpl(successfulMembers, missingMembers);
  }
  
  public Map<DistributedMember, Set<PersistentID>> compactAllDiskStores() throws AdminException {
    connectAdminDS();
    DM dm = getDistributionManager();
    if(dm == null) {
      throw new IllegalStateException(LocalizedStrings.AdminDistributedSystemImpl_CONNECT_HAS_NOT_BEEN_INVOKED_ON_THIS_ADMINDISTRIBUTEDSYSTEM.toLocalizedString());
    }
    return compactAllDiskStores(dm);
  }

  public static Map<DistributedMember, Set<PersistentID>> compactAllDiskStores(DM dm)
      throws AdminException {
    return CompactRequest.send(dm);
  }
  
  /**
   * This method can be used to process ClientMembership events sent for
   * BridgeMembership by bridge servers to all admin members.
   * 
   * NOTE: Not implemented currently. JMX implementation which is a subclass of 
   * this class i.e. AdminDistributedSystemJmxImpl implements it.
   * 
   * @param senderId
   *          id of the member that sent the ClientMembership changes for
   *          processing (could be null)
   * @param clientId
   *          id of a client for which the notification was sent
   * @param clientHost
   *          host on which the client is/was running
   * @param eventType
   *          denotes whether the client Joined/Left/Crashed should be one of
   *          ClientMembershipMessage#JOINED, ClientMembershipMessage#LEFT,
   *          ClientMembershipMessage#CRASHED
   */
  public void processClientMembership(String senderId, String clientId,
      String clientHost, int eventType) {
  }

  public void setAlertLevelAsString(String level)  {
    AlertLevel newAlertLevel = AlertLevel.forName(level);
    
    if (newAlertLevel != null) {    
      setAlertLevel(newAlertLevel);
    } else {
      System.out.println("ERROR:: "+level+" is invalid. Allowed alert levels are: WARNING, ERROR, SEVERE, OFF");
      throw new IllegalArgumentException(LocalizedStrings.DEBUG.toLocalizedString(level+" is invalid. Allowed alert levels are: WARNING, ERROR, SEVERE, OFF"));
    }
  }

  public String getAlertLevelAsString() {
    return getAlertLevel().getName();
  }
}

