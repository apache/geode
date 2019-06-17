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
package org.apache.geode.distributed.internal.membership.gms;

import java.util.Timer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.DistributedMembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.fd.GMSHealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Authenticator;
import org.apache.geode.distributed.internal.membership.gms.interfaces.HealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Locator;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.distributed.internal.membership.gms.messenger.JGroupsMessenger;
import org.apache.geode.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;

@SuppressWarnings("ConstantConditions")
public class Services {

  private static final Logger logger = LogService.getLogger();

  private final Manager manager;
  private final JoinLeave joinLeave;
  private final HealthMonitor healthMon;
  private final Messenger messenger;
  private final Authenticator auth;
  private final ServiceConfig config;
  private final DMStats stats;
  private final Stopper cancelCriterion;
  private final SecurityService securityService;
  private final InternalDistributedSystem distributedSystem;

  private volatile boolean stopping;
  private volatile boolean stopped;
  private volatile Exception shutdownCause;

  private Locator locator;

  private LogWriter logWriter;
  private LogWriter securityLogWriter;

  private final Timer timer = new Timer("Geode Membership Timer", true);

  /**
   * A common logger for membership classes
   */
  public static Logger getLogger() {
    return logger;
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
    this.securityService = SecurityServiceFactory.create();
    this.auth = null;
    this.distributedSystem = null;
  }

  public Services(DistributedMembershipListener listener,
      InternalDistributedSystem system,
      RemoteTransportConfig transport, DMStats stats, SecurityService securityService,
      final Authenticator authenticator) {
    this.distributedSystem = system;
    this.cancelCriterion = new Stopper();
    this.stats = stats;
    this.config = new ServiceConfig(transport, system.getConfig());
    this.manager = new GMSMembershipManager(listener);
    this.joinLeave = new GMSJoinLeave();
    this.healthMon = new GMSHealthMonitor();
    this.logWriter = distributedSystem.getLogWriter();
    this.messenger = new JGroupsMessenger();
    this.securityLogWriter = distributedSystem.getSecurityLogWriter();
    this.securityService = securityService;
    this.auth = authenticator;
  }

  protected void init() {
    this.messenger.init(this);
    this.manager.init(this);
    this.joinLeave.init(this);
    this.healthMon.init(this);
  }

  protected void start() {
    boolean started = false;
    try {
      logger.info("Starting membership services");
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
        this.timer.cancel();
      }
    }
    this.messenger.started();
    this.joinLeave.started();
    this.healthMon.started();
    this.manager.started();
    InternalLocator l = (InternalLocator) org.apache.geode.distributed.Locator.getLocator();
    if (l != null && l.getLocatorHandler() != null) {
      if (l.getLocatorHandler().setMembershipManager((MembershipManager) this.manager)) {
        this.locator = (Locator) l.getLocatorHandler();
      }
    }
    logger.debug("All membership services have been started");
    try {
      this.manager.joinDistributedSystem();
    } catch (Throwable e) {
      stop();
      throw e;
    }
  }

  public void setLocalAddress(InternalDistributedMember address) {
    this.messenger.setLocalAddress(address);
    this.joinLeave.setLocalAddress(address);
    this.healthMon.setLocalAddress(address);
    this.manager.setLocalAddress(address);
  }

  public void emergencyClose() {
    if (this.stopping) {
      return;
    }
    this.stopping = true;
    logger.info("Stopping membership services");
    this.timer.cancel();
    try {
      this.joinLeave.emergencyClose();
    } finally {
      try {
        this.healthMon.emergencyClose();
      } finally {
        try {
          this.messenger.emergencyClose();
        } finally {
          try {
            this.manager.emergencyClose();
          } finally {
            this.cancelCriterion.cancel("Membership services are shut down");
            this.stopped = true;
          }
        }
      }
    }
  }

  public void stop() {
    if (this.stopping) {
      return;
    }
    logger.info("Stopping membership services");
    this.stopping = true;
    config.setIsReconnecting(false);
    try {
      this.timer.cancel();
    } finally {
      try {
        this.joinLeave.stop();
      } finally {
        try {
          this.healthMon.stop();
        } finally {
          try {
            this.messenger.stop();
          } finally {
            try {
              this.manager.stop();
            } finally {
              this.cancelCriterion.cancel("Membership services are shut down");
              this.stopped = true;
            }
          }
        }
      }
    }
  }

  public InternalDistributedSystem getDistributedSystem() {
    return this.distributedSystem;
  }

  public SecurityService getSecurityService() {
    return this.securityService;
  }

  /**
   * returns the DistributedSystem's log writer
   *
   * @deprecated use a log4j-based LogService
   */
  public LogWriter getLogWriter() {
    return this.logWriter;
  }

  /**
   * returns the DistributedSystem's security log writer
   *
   * @deprecated use a log4j-based LogService
   */
  public LogWriter getSecurityLogWriter() {
    return this.securityLogWriter;
  }

  public Authenticator getAuthenticator() {
    return this.auth;
  }

  public void installView(NetView v) {
    if (this.locator != null) {
      this.locator.installView(v);
    }
    this.healthMon.installView(v);
    this.messenger.installView(v);
    this.manager.installView(v);
  }

  public void memberSuspected(InternalDistributedMember initiator,
      InternalDistributedMember suspect, String reason) {
    try {
      this.joinLeave.memberSuspected(initiator, suspect, reason);
    } finally {
      try {
        this.healthMon.memberSuspected(initiator, suspect, reason);
      } finally {
        try {
          this.messenger.memberSuspected(initiator, suspect, reason);
        } finally {
          this.manager.memberSuspected(initiator, suspect, reason);
        }
      }
    }
  }

  public Manager getManager() {
    return this.manager;
  }

  public Locator getLocator() {
    return this.locator;
  }

  public void setLocator(Locator locator) {
    this.locator = locator;
  }

  public JoinLeave getJoinLeave() {
    return this.joinLeave;
  }

  public HealthMonitor getHealthMonitor() {
    return this.healthMon;
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
    return this.shutdownCause;
  }

  public boolean isShutdownDueToForcedDisconnect() {
    return this.shutdownCause instanceof ForcedDisconnectException;
  }

  public boolean isAutoReconnectEnabled() {
    return !getConfig().getDistributionConfig().getDisableAutoReconnect();
  }

  public class Stopper extends CancelCriterion {
    volatile String reasonForStopping = null;

    public void cancel(String reason) {
      this.reasonForStopping = reason;
    }

    @Override
    public String cancelInProgress() {
      if (Services.this.shutdownCause != null)
        return Services.this.shutdownCause.toString();
      return this.reasonForStopping;
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      } else {
        if (e == null) {
          return new DistributedSystemDisconnectedException(reason);
        } else {
          return new DistributedSystemDisconnectedException(reason, e);
        }
      }
    }

  }

}
