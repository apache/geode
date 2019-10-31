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

import static org.apache.geode.internal.serialization.DataSerializableFixedID.DISTRIBUTED_MEMBER;
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
import static org.apache.geode.internal.serialization.DataSerializableFixedID.NETVIEW;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.NETWORK_PARTITION_MESSAGE;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.REMOVE_MEMBER_REQUEST;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.SUSPECT_MEMBERS_MESSAGE;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.VIEW_ACK_MESSAGE;

import java.util.Timer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.api.Authenticator;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifierFactory;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipStatistics;
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
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.logging.internal.log4j.api.LogService;

@SuppressWarnings("ConstantConditions")
public class Services {

  private static final Logger logger = LogService.getLogger();

  private final Manager manager;
  private final JoinLeave joinLeave;
  private final HealthMonitor healthMon;
  private final Messenger messenger;
  private final Authenticator auth;
  private final MembershipConfig config;
  private final MembershipStatistics stats;
  private final Stopper cancelCriterion;
  private final DSFIDSerializer serializer;

  private final MemberIdentifierFactory memberFactory;

  private volatile boolean stopping;
  private volatile boolean stopped;
  private volatile Exception shutdownCause;

  private Locator locator;

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

  public Services(Manager membershipManager, MembershipStatistics stats,
      final Authenticator authenticator, MembershipConfig membershipConfig,
      DSFIDSerializer serializer, MemberIdentifierFactory memberFactory,
      final TcpClient locatorClient) {
    this.cancelCriterion = new Stopper();
    this.stats = stats;
    this.config = membershipConfig;
    this.manager = membershipManager;
    this.joinLeave = new GMSJoinLeave(locatorClient);
    this.healthMon = new GMSHealthMonitor();
    this.messenger = new JGroupsMessenger();
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
    serializer.registerDSFID(DISTRIBUTED_MEMBER, InternalDistributedMember.class);
    serializer.registerDSFID(NETVIEW, GMSMembershipView.class);
    serializer.registerDSFID(GET_VIEW_REQ, GetViewRequest.class);
    serializer.registerDSFID(GET_VIEW_RESP, GetViewResponse.class);
    serializer.registerDSFID(FIND_COORDINATOR_REQ, FindCoordinatorRequest.class);
    serializer.registerDSFID(FIND_COORDINATOR_RESP, FindCoordinatorResponse.class);
    serializer.registerDSFID(JOIN_RESPONSE, JoinResponseMessage.class);
    serializer.registerDSFID(JOIN_REQUEST, JoinRequestMessage.class);

  }

  /**
   * Initialize services - do this before invoking start()
   */
  public void init() {
    this.messenger.init(this);
    this.manager.init(this);
    this.joinLeave.init(this);
    this.healthMon.init(this);
  }

  /**
   * Start services - this will start everything up and join the cluster.
   * Invoke init() before this method.
   */
  public void start() {
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
    logger.debug("All membership services have been started");
    try {
      this.manager.joinDistributedSystem();
    } catch (Throwable e) {
      stop();
      throw e;
    }
  }

  public void setLocalAddress(MemberIdentifier address) {
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

  public Authenticator getAuthenticator() {
    return this.auth;
  }

  public void installView(GMSMembershipView v) {
    if (this.locator != null) {
      this.locator.installView(v);
    }
    this.healthMon.installView(v);
    this.messenger.installView(v);
    this.manager.installView(v);
  }

  public void memberSuspected(MemberIdentifier initiator,
      MemberIdentifier suspect, String reason) {
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

  public MembershipConfig getConfig() {
    return this.config;
  }

  public Messenger getMessenger() {
    return this.messenger;
  }

  public MembershipStatistics getStatistics() {
    return this.stats;
  }

  public MemberIdentifierFactory getMemberFactory() {
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
    return this.shutdownCause instanceof ForcedDisconnectException;
  }

  public boolean isAutoReconnectEnabled() {
    return !getConfig().getDisableAutoReconnect();
  }

  public DSFIDSerializer getSerializer() {
    return this.serializer;
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
