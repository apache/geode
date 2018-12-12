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
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.DistributedMembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.auth.GMSAuthenticator;
import org.apache.geode.distributed.internal.membership.gms.fd.GMSHealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Authenticator;
import org.apache.geode.distributed.internal.membership.gms.interfaces.HealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Locator;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.membership.gms.locator.GMSLocator;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.distributed.internal.membership.gms.messenger.JGroupsMessenger;
import org.apache.geode.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.security.AuthenticationFailedException;

@SuppressWarnings("ConstantConditions")
public class Services {

  private static final Logger logger = LogService.getLogger();

  private static InternalLogWriter staticLogWriter;
  private static InternalLogWriter staticSecurityLogWriter;

  private final Manager manager;
  private final JoinLeave joinLeave;
  private final HealthMonitor healthMon;
  private final Messenger messenger;
  private final Authenticator auth;
  private final ServiceConfig config;
  private final DMStats stats;
  private final Stopper cancelCriterion;
  private final SecurityService securityService;

  private volatile boolean stopping;
  private volatile boolean stopped;
  private volatile Exception shutdownCause;

  private Locator locator;

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
  }

  public Services(DistributedMembershipListener listener, DistributionConfig config,
      RemoteTransportConfig transport, DMStats stats, SecurityService securityService) {
    this.cancelCriterion = new Stopper();
    this.stats = stats;
    this.config = new ServiceConfig(transport, config);
    this.manager = new GMSMembershipManager(listener);
    this.joinLeave = new GMSJoinLeave();
    this.healthMon = new GMSHealthMonitor();
    this.messenger = new JGroupsMessenger();
    this.securityService = securityService;
    this.auth = new GMSAuthenticator();
  }

  protected void init() {
    // InternalDistributedSystem establishes this log writer at boot time
    // TODO fix this so that IDS doesn't know about Services
    this.securityLogWriter = staticSecurityLogWriter;
    staticSecurityLogWriter = null;
    this.logWriter = staticLogWriter;
    staticLogWriter = null;
    this.auth.init(this);
    this.messenger.init(this);
    this.manager.init(this);
    this.joinLeave.init(this);
    this.healthMon.init(this);
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
          this.auth.emergencyClose();
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
  }

  public void stop() {
    if (this.stopping) {
      return;
    }
    logger.info("Stopping membership services");
    this.stopping = true;
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
            this.auth.stop();
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
  }

  public static void setLogWriter(InternalLogWriter writer) {
    staticLogWriter = writer;
  }

  public static void setSecurityLogWriter(InternalLogWriter securityWriter) {
    staticSecurityLogWriter = securityWriter;
  }

  public SecurityService getSecurityService() {
    return this.securityService;
  }

  public InternalLogWriter getLogWriter() {
    return this.logWriter;
  }

  public InternalLogWriter getSecurityLogWriter() {
    return this.securityLogWriter;
  }

  public Authenticator getAuthenticator() {
    return this.auth;
  }

  public void installView(NetView v) {
    try {
      this.auth.installView(v);
    } catch (AuthenticationFailedException e) {
      return;
    }
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
          this.auth.memberSuspected(initiator, suspect, reason);
        } finally {
          try {
            this.messenger.memberSuspected(initiator, suspect, reason);
          } finally {
            this.manager.memberSuspected(initiator, suspect, reason);
          }
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

  public byte[] getPublicKey(InternalDistributedMember mbr) {
    if (this.locator != null) {
      return ((GMSLocator) this.locator).getPublicKey(mbr);
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
