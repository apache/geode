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

import static org.apache.geode.internal.serialization.DataSerializableFixedID.FINAL_CHECK_PASSED_MESSAGE;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.FIND_COORDINATOR_REQ;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.FIND_COORDINATOR_RESP;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.GET_VIEW_REQ;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.GET_VIEW_RESP;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.HEARTBEAT_REQUEST;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.HEARTBEAT_RESPONSE;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.INSTALL_VIEW_MESSAGE;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.JOIN_REQUEST;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.JOIN_RESPONSE;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.LEAVE_REQUEST_MESSAGE;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.MEMBER_IDENTIFIER;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.NETVIEW;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.NETWORK_PARTITION_MESSAGE;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.REMOVE_MEMBER_REQUEST;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.SUSPECT_MEMBERS_MESSAGE;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.VIEW_ACK_MESSAGE;

import java.util.Timer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.membership.api.Authenticator;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactory;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.MembershipClosedException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.fd.GMSHealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.HealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Locator;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.locator.GetViewRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.GetViewResponse;
import org.apache.geode.distributed.internal.membership.gms.locator.MembershipLocatorImpl;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.distributed.internal.membership.gms.messages.FinalCheckPassedMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.NetworkPartitionMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.SuspectMembersMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.ViewAckMessage;
import org.apache.geode.distributed.internal.membership.gms.messenger.JGroupsMessenger;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.ModuleService;

/**
 * Services holds all of the membership services of a GMSMembership. It serves as a
 * directory for the services, enabling them to find one another. It also controls the
 * lifecycle of the services and holds a Stopper that services should consult in
 * exceptional situations to see if Membership is shutting down.
 */
@SuppressWarnings("ConstantConditions")
public class Services<ID extends MemberIdentifier> {

  private static final Logger logger = LogService.getLogger();

  private final Manager<ID> manager;
  private final JoinLeave<ID> joinLeave;
  private final HealthMonitor<ID> healthMon;
  private final Messenger<ID> messenger;
  private final Authenticator<ID> auth;
  private final MembershipConfig config;
  private final MembershipStatistics stats;
  private final Stopper cancelCriterion;
  private final DSFIDSerializer serializer;

  private final MemberIdentifierFactory<ID> memberFactory;

  private volatile boolean stopping;
  private volatile boolean stopped;
  private volatile Exception shutdownCause;

  private Locator<ID> locator;
  private MembershipLocator<ID> membershipLocator;

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
    this.auth = null;
    this.serializer = null;
    this.memberFactory = null;
  }

  public Services(Manager<ID> membershipManager, MembershipStatistics stats,
      final Authenticator<ID> authenticator, MembershipConfig membershipConfig,
      DSFIDSerializer serializer, MemberIdentifierFactory<ID> memberFactory,
      final TcpClient locatorClient, final TcpSocketCreator socketCreator) {
    this.cancelCriterion = new Stopper();
    this.stats = stats;
    this.config = membershipConfig;
    this.manager = membershipManager;
    this.joinLeave = new GMSJoinLeave<>(locatorClient);
    this.healthMon = new GMSHealthMonitor<>(socketCreator);
    this.messenger = new JGroupsMessenger<>();
    this.auth = authenticator;
    this.serializer = serializer;
    this.memberFactory = memberFactory;
    registerSerializables(serializer);
  }

  @VisibleForTesting
  public static void registerSerializables(DSFIDSerializer serializer) {
    serializer.registerDSFID(FINAL_CHECK_PASSED_MESSAGE, FinalCheckPassedMessage.class);
    serializer.registerDSFID(NETWORK_PARTITION_MESSAGE, NetworkPartitionMessage.class);
    serializer.registerDSFID(REMOVE_MEMBER_REQUEST, RemoveMemberMessage.class);
    serializer.registerDSFID(HEARTBEAT_REQUEST, HeartbeatRequestMessage.class);
    serializer.registerDSFID(HEARTBEAT_RESPONSE, HeartbeatMessage.class);
    serializer.registerDSFID(SUSPECT_MEMBERS_MESSAGE, SuspectMembersMessage.class);
    serializer.registerDSFID(LEAVE_REQUEST_MESSAGE, LeaveRequestMessage.class);
    serializer.registerDSFID(VIEW_ACK_MESSAGE, ViewAckMessage.class);
    serializer.registerDSFID(INSTALL_VIEW_MESSAGE, InstallViewMessage.class);
    serializer.registerDSFID(NETVIEW, GMSMembershipView.class);
    serializer.registerDSFID(GET_VIEW_REQ, GetViewRequest.class);
    serializer.registerDSFID(GET_VIEW_RESP, GetViewResponse.class);
    serializer.registerDSFID(FIND_COORDINATOR_REQ, FindCoordinatorRequest.class);
    serializer.registerDSFID(FIND_COORDINATOR_RESP, FindCoordinatorResponse.class);
    serializer.registerDSFID(JOIN_RESPONSE, JoinResponseMessage.class);
    serializer.registerDSFID(JOIN_REQUEST, JoinRequestMessage.class);
    serializer.registerDSFID(MEMBER_IDENTIFIER, MemberIdentifierImpl.class);

  }

  /**
   * Initialize services - do this before invoking start()
   */
  public void init(ModuleService moduleService) throws MembershipConfigurationException {
    this.messenger.init(this, moduleService);
    this.manager.init(this, moduleService);
    this.joinLeave.init(this, moduleService);
    this.healthMon.init(this, moduleService);
  }

  /**
   * Start services - this will start everything up and join the cluster.
   * Invoke init() before this method.
   */
  public void start() throws MemberStartupException {
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
      this.messenger.started();
      this.joinLeave.started();
      this.healthMon.started();
      this.manager.started();

      if (membershipLocator != null) {
        /*
         * Now that all the services have started we can let the membership locator know
         * about them. We must do this before telling the manager to joinDistributedSystem()
         * later in this method
         */
        final MembershipLocatorImpl locatorImpl =
            (MembershipLocatorImpl) this.membershipLocator;
        locatorImpl.setServices(this);
      }

      logger.debug("All membership services have been started");
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

    try {
      this.manager.joinDistributedSystem();
    } catch (Throwable e) {
      stop();
      throw e;
    }
  }

  public void setLocalAddress(ID address) {
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

  public Authenticator<ID> getAuthenticator() {
    return this.auth;
  }

  public void installView(GMSMembershipView<ID> v) {
    if (this.locator != null) {
      this.locator.installView(v);
    }
    this.healthMon.installView(v);
    this.messenger.installView(v);
    this.manager.installView(v);
  }

  public void memberSuspected(ID initiator,
      ID suspect, String reason) {
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

  public Manager<ID> getManager() {
    return this.manager;
  }

  public void setLocators(final Locator<ID> locator,
      final MembershipLocator<ID> membershipLocator) {
    this.locator = locator;
    this.membershipLocator = membershipLocator;
  }

  public Locator<ID> getLocator() {
    return locator;
  }

  public JoinLeave<ID> getJoinLeave() {
    return this.joinLeave;
  }

  public HealthMonitor<ID> getHealthMonitor() {
    return this.healthMon;
  }

  public MembershipConfig getConfig() {
    return this.config;
  }

  public Messenger<ID> getMessenger() {
    return this.messenger;
  }

  public MembershipStatistics getStatistics() {
    return this.stats;
  }

  public MemberIdentifierFactory<ID> getMemberFactory() {
    return memberFactory;
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
    return this.shutdownCause instanceof MemberDisconnectedException;
  }

  public boolean isAutoReconnectEnabled() {
    return !getConfig().getDisableAutoReconnect();
  }

  public DSFIDSerializer getSerializer() {
    return this.serializer;
  }

  public class Stopper {
    volatile String reasonForStopping = null;

    public void cancel(String reason) {
      this.reasonForStopping = reason;
    }

    public String cancelInProgress() {
      if (Services.this.shutdownCause != null)
        return Services.this.shutdownCause.toString();
      return this.reasonForStopping;
    }

    public RuntimeException generateCancelledException(Throwable e) {
      if (shutdownCause instanceof MemberDisconnectedException) {
        MembershipClosedException newException =
            new MembershipClosedException("membership shutdown",
                e);
        throw newException;
      }
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      } else {
        if (e == null) {
          return new MembershipClosedException(reason);
        } else {
          return new MembershipClosedException(reason, e);
        }
      }
    }

    public boolean isCancelInProgress() {
      return cancelInProgress() != null;
    }

    public void checkCancelInProgress(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return;
      }
      throw generateCancelledException(e);
    }

  }

}
