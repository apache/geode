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
package com.gemstone.gemfire.distributed.internal.membership.gms;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.membership.DistributedMembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.auth.GMSAuthenticator;
import com.gemstone.gemfire.distributed.internal.membership.gms.fd.GMSHealthMonitor;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.*;
import com.gemstone.gemfire.distributed.internal.membership.gms.locator.GMSLocator;
import com.gemstone.gemfire.distributed.internal.membership.gms.membership.GMSJoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.messenger.JGroupsMessenger;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import org.apache.logging.log4j.Logger;

import java.util.Timer;

@SuppressWarnings("ConstantConditions")
public class Services {

  private static final Logger logger = LogService.getLogger();

  private static final ThreadGroup threadGroup = LoggingThreadGroup.createThreadGroup("GemFire Membership", logger); 
  
  private static InternalLogWriter staticLogWriter;
  private static InternalLogWriter staticSecurityLogWriter;

  final private Manager manager;
  final private JoinLeave joinLeave;
  private Locator locator;
  final private HealthMonitor healthMon;
  final private Messenger messenger;
  final private Authenticator auth;
  final private ServiceConfig config;
  final private DMStats stats;
  final private Stopper cancelCriterion;
  private volatile boolean stopping;
  private volatile boolean stopped;
  private volatile Exception shutdownCause;

  private InternalLogWriter logWriter;
  private InternalLogWriter securityLogWriter;
  
  private final Timer timer = new Timer("Geode Membership Timer", true);
  
  

  /**
   * A common logger for membership classes
   */
  public static Logger getLogger() {
    return logger;
  }

  /**
   * The thread group for all membership threads
   */
  public static ThreadGroup getThreadGroup() {
    return threadGroup;
  }
  
  /**
   * a timer used for membership tasks
   */
  public Timer getTimer() {
    return this.timer;
  }
  
  public boolean isStopped() {
    return this.stopped;
  }
  

  /**
   * for testing only - create a non-functional Services object with a Stopper
   */
  public Services() {
    this.cancelCriterion = new Stopper();
    this.stats = null;
    this.config = null;
    this.manager = null;
    this.joinLeave = null;
    this.healthMon = null;
    this.messenger = null;
    this.auth = null;
  }

  public Services(
      DistributedMembershipListener listener, DistributionConfig config,
      RemoteTransportConfig transport, DMStats stats) {
    this.cancelCriterion = new Stopper();
    this.stats = stats;
    this.config = new ServiceConfig(transport, config);
    this.manager = new GMSMembershipManager(listener);
    this.joinLeave = new GMSJoinLeave();
    this.healthMon = new GMSHealthMonitor();
    this.messenger = new JGroupsMessenger();
    this.auth = new GMSAuthenticator();
  }
  
  protected void init() {
    // InternalDistributedSystem establishes this log writer at boot time
    // TODO fix this so that IDS doesn't know about Services
    securityLogWriter = staticSecurityLogWriter;
    staticSecurityLogWriter = null;
    logWriter = staticLogWriter;
    staticLogWriter = null;
    this.auth.init(this);
    this.messenger.init(this);
    this.manager.init(this);
    this.joinLeave.init(this);
    this.healthMon.init(this);
    InternalLocator l = (InternalLocator)com.gemstone.gemfire.distributed.Locator.getLocator();
    if (l != null && l.getLocatorHandler() != null) {
      if (l.getLocatorHandler().setMembershipManager((MembershipManager)this.manager)) {
        this.locator = (Locator)l.getLocatorHandler();
      }
    }
  }
  
  protected void start() {
    boolean started = false;
    try {
      logger.info("Starting membership services");
      logger.debug("starting Authenticator");
      this.auth.start();
      logger.debug("starting Messenger");
      this.messenger.start();
      logger.debug("starting JoinLeave");
      this.joinLeave.start();
      logger.debug("starting HealthMonitor");
      this.healthMon.start();
      logger.debug("starting Manager");
      this.manager.start();
      started = true;
    } catch (RuntimeException e) {
      logger.fatal("Unexpected exception while booting membership services", e);
      throw e;
    } finally {
      if (!started) {
        this.manager.stop();
        this.healthMon.stop();
        this.joinLeave.stop();
        this.messenger.stop();
        this.auth.stop();
        this.timer.cancel();
      }
    }
    this.auth.started();
    this.messenger.started();
    this.joinLeave.started();
    this.healthMon.started();
    this.manager.started();
    logger.debug("All membership services have been started");
    try {
      this.manager.joinDistributedSystem();
    } catch (Throwable e) {
      stop();
      throw e;
    }
  }
  
  public void emergencyClose() {
    if (stopping) {
      return;
    }
    stopping = true;
    logger.info("Stopping membership services");
    timer.cancel();
    try {
      joinLeave.emergencyClose();
    } finally {
      try {
        healthMon.emergencyClose();
      } finally {
        try {
          auth.emergencyClose();
        } finally {
          try {
            messenger.emergencyClose();
          } finally {
            try {
              manager.emergencyClose();
            } finally {
              cancelCriterion.cancel("Membership services are shut down");
              stopped = true;
            }
          }
        }
      }
    }
  }
  
  public void stop() {
    if (stopping) {
      return;
    }
    logger.info("Stopping membership services");
    stopping = true;
    try {
      timer.cancel();
    } finally {
      try {
        joinLeave.stop();
      } finally {
        try {
          healthMon.stop();
        } finally {
          try {
            auth.stop();
          } finally {
            try {
              messenger.stop();
            } finally {
              try {
                manager.stop();
              } finally {
                cancelCriterion.cancel("Membership services are shut down");
                stopped = true;
              }
            }
          }
        }
      }
    }
  }

  public static void setLogWriter(InternalLogWriter writer) {
    staticLogWriter = writer;
  }

  public static void setSecurityLogWriter(InternalLogWriter securityWriter) {
    staticSecurityLogWriter = securityWriter;
  }

  public InternalLogWriter getLogWriter() {
    return this.logWriter;
  }

  public InternalLogWriter getSecurityLogWriter() {
    return this.securityLogWriter;
  }
  
  public Authenticator getAuthenticator() {
    return auth;
  }

  public void installView(NetView v) {
    try {
      auth.installView(v);
    } catch (AuthenticationFailedException e) {
      return;
    }
    if (locator != null) {
      locator.installView(v);
    }
    healthMon.installView(v);
    messenger.installView(v);
    manager.installView(v);
  }
  
  public void memberSuspected(InternalDistributedMember initiator, InternalDistributedMember suspect, String reason) {
    try {
      joinLeave.memberSuspected(initiator, suspect, reason);
    } finally {
      try {
        healthMon.memberSuspected(initiator, suspect, reason);
      } finally {
        try {
          auth.memberSuspected(initiator, suspect, reason);
        } finally {
          try {
            messenger.memberSuspected(initiator, suspect, reason);
          } finally {
            manager.memberSuspected(initiator, suspect, reason);
          }
        }
      }
    }
  }

  public Manager getManager() {
    return manager;
  }

  public Locator getLocator() {
    return locator;
  }

  public void setLocator(Locator locator) {
    this.locator = locator;
  }

  public JoinLeave getJoinLeave() {
    return joinLeave;
  }

  public HealthMonitor getHealthMonitor() {
    return healthMon;
  }

  public ServiceConfig getConfig() {
    return this.config;
  }
  
  public Messenger getMessenger() {
    return this.messenger;
  }
  
  public DMStats getStatistics() {
    return this.stats;
  }
  
  public Stopper getCancelCriterion() {
    return this.cancelCriterion;
  }
  
  public void setShutdownCause(Exception e) {
    this.shutdownCause = e;
  }
  
  public Exception getShutdownCause() {
    return shutdownCause;
  }
  
  public boolean isShutdownDueToForcedDisconnect() {
    return shutdownCause instanceof ForcedDisconnectException;
  }
  
  public boolean isAutoReconnectEnabled() {
    return !getConfig().getDistributionConfig().getDisableAutoReconnect();
  }

  public byte[] getPublicKey(InternalDistributedMember mbr) {
    if(locator != null) {
      return ((GMSLocator)locator).getPublicKey(mbr);
    } 
    return null;
  }
  
  public class Stopper extends CancelCriterion {
    volatile String reasonForStopping = null;

    public void cancel(String reason) {
      this.reasonForStopping = reason;
    }

    @Override
    public String cancelInProgress() {
      if(Services.this.shutdownCause != null)
        return Services.this.shutdownCause.toString();
      return reasonForStopping;
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      }
      else {
        if (e == null) {
          return new DistributedSystemDisconnectedException(reason);
        } else {
          return new DistributedSystemDisconnectedException(reason, e);
        }
      }
    }

  }

}
