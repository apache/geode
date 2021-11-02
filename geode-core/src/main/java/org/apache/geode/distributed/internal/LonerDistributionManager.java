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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.CancelCriterion;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.alerting.internal.NullAlertingService;
import org.apache.geode.alerting.internal.api.AlertingService;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.locks.ElderState;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.monitoring.ThreadsMonitoringImpl;
import org.apache.geode.internal.monitoring.ThreadsMonitoringImplDummy;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.executors.LoggingExecutors;

/**
 * A <code>LonerDistributionManager</code> is a dm that never communicates with anyone else.
 *
 *
 *
 * @since GemFire 3.5
 */
public class LonerDistributionManager implements DistributionManager {
  private final InternalDistributedSystem system;
  private final InternalLogWriter logger;
  private ElderState elderState;

  /**
   * Thread Monitor mechanism to monitor system threads
   *
   * @see org.apache.geode.internal.monitoring.ThreadsMonitoring
   */
  private final ThreadsMonitoring threadMonitor;

  //////////////////////// Constructors ////////////////////////

  /**
   * Creates a new local distribution manager
   *
   * @param system The distributed system to which this distribution manager will send messages.
   *
   */
  public LonerDistributionManager(InternalDistributedSystem system, InternalLogWriter logger) {
    this.system = system;
    this.logger = logger;
    this.localAddress = generateMemberId();
    this.allIds = Collections.singleton(localAddress);
    this.viewMembers = new ArrayList<>(allIds);
    DistributionStats.enableClockStats = this.system.getConfig().getEnableTimeStatistics();

    DistributionConfig config = system.getConfig();

    if (config.getThreadMonitorEnabled()) {
      this.threadMonitor = new ThreadsMonitoringImpl(system, config.getThreadMonitorInterval(),
          config.getThreadMonitorTimeLimit());
      logger.info("[ThreadsMonitor] New Monitor object and process were created.\n");
    } else {
      this.threadMonitor = new ThreadsMonitoringImplDummy();
      logger.info("[ThreadsMonitor] Monitoring is disabled and will not be run.\n");
    }
  }

  ////////////////////// Instance Methods //////////////////////

  protected void shutdown() {
    threadMonitor.close();
    executor.shutdown();
    try {
      executor.awaitTermination(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new InternalGemFireError("Interrupted while waiting for DM shutdown");
    }
  }

  private final InternalDistributedMember localAddress;

  private final Set<InternalDistributedMember> allIds;
  private final List<InternalDistributedMember> viewMembers;
  private final ConcurrentMap<InternalDistributedMember, InternalDistributedMember> canonicalIds =
      new ConcurrentHashMap<>();
  @Immutable
  private static final DummyDMStats stats = new DummyDMStats();
  private final ExecutorService executor =
      LoggingExecutors.newCachedThreadPool("LonerDistributionManagerThread", false);

  private final OperationExecutors executors = new LonerOperationExecutors(executor);

  @Override
  public long cacheTimeMillis() {
    return this.system.getClock().cacheTimeMillis();
  }

  @Override
  public InternalDistributedMember getDistributionManagerId() {
    return localAddress;
  }

  @Override
  public Set<InternalDistributedMember> getDistributionManagerIds() {
    return allIds;
  }

  @Override
  public Set<InternalDistributedMember> getDistributionManagerIdsIncludingAdmin() {
    return allIds;
  }

  @Override
  public InternalDistributedMember getCanonicalId(DistributedMember dmid) {
    InternalDistributedMember iid = (InternalDistributedMember) dmid;
    InternalDistributedMember result = this.canonicalIds.putIfAbsent(iid, iid);
    if (result != null) {
      return result;
    }
    return iid;
  }

  @Override
  public DistributedMember getMemberWithName(String name) {
    for (DistributedMember id : canonicalIds.values()) {
      if (Objects.equals(id.getName(), name)) {
        return id;
      }
    }
    if (Objects.equals(localAddress.getName(), name)) {
      return localAddress;
    }
    return null;
  }

  @Override
  public Set<InternalDistributedMember> getOtherDistributionManagerIds() {
    return Collections.emptySet();
  }

  @Override
  public Set<InternalDistributedMember> getOtherNormalDistributionManagerIds() {
    return Collections.emptySet();
  }

  @Override
  public Set<InternalDistributedMember> getAllOtherMembers() {
    return Collections.emptySet();
  }

  @Override // DM method
  public void retainMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members,
      KnownVersion version) {
    members.removeIf(id -> id.getVersion().compareTo(version) < 0);
  }

  @Override // DM method
  public void removeMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members,
      KnownVersion version) {
    members.removeIf(id -> id.getVersion().compareTo(version) >= 0);
  }


  @Override
  public Set<InternalDistributedMember> addMembershipListenerAndGetDistributionManagerIds(
      MembershipListener l) {
    // return getOtherDistributionManagerIds();
    return allIds;
  }

  @Override
  public Set<InternalDistributedMember> addAllMembershipListenerAndGetAllIds(
      MembershipListener l) {
    return allIds;
  }

  @Override
  public InternalDistributedMember getId() {
    return getDistributionManagerId();
  }

  @Override
  public InternalDistributedMember getElderId() {
    return getId();
  }

  @Override
  public boolean isElder() {
    return true;
  }

  @Override
  public boolean isLoner() {
    return true;
  }

  @Override
  public synchronized ElderState getElderState(boolean force) {
    // loners are always the elder
    if (this.elderState == null) {
      this.elderState = new ElderState(this);
    }
    return this.elderState;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return this.system;
  }

  @Override
  public void addMembershipListener(MembershipListener l) {}

  @Override
  public void removeMembershipListener(MembershipListener l) {}

  @Override
  public void removeAllMembershipListener(MembershipListener l) {}

  @Override
  public Collection<MembershipListener> getMembershipListeners() {
    return Collections.emptySet();
  }

  @Override
  public void addAdminConsole(InternalDistributedMember p_id) {}

  @Override
  public DMStats getStats() {
    return stats;
  }

  @Override
  public DistributionConfig getConfig() {
    DistributionConfig result = null;
    if (getSystem() != null) {
      result = getSystem().getConfig();
    }
    return result;
  }

  @Override
  public void handleManagerDeparture(InternalDistributedMember p_id, boolean crashed,
      String reason) {}

  public LogWriterI18n getLoggerI18n() {
    return this.logger;
  }

  public InternalLogWriter getInternalLogWriter() {
    return this.logger;
  }

  @Override
  public OperationExecutors getExecutors() {
    return executors;
  }

  @Override
  public void close() {
    shutdown();
  }

  @Override
  public List<InternalDistributedMember> getViewMembers() {
    return viewMembers;
  }

  @Override
  public Set<InternalDistributedMember> getAdminMemberSet() {
    return Collections.emptySet();
  }

  public static class DummyDMStats implements DMStats {
    @Override
    public long getSentMessages() {
      return 0L;
    }

    @Override
    public void incSentMessages(long messages) {}

    @Override
    public void incTOSentMsg() {}

    @Override
    public long getSentCommitMessages() {
      return 0L;
    }

    @Override
    public void incSentCommitMessages(long messages) {}

    @Override
    public long getCommitWaits() {
      return 0L;
    }

    @Override
    public void incCommitWaits() {}

    @Override
    public long getSentMessagesTime() {
      return 0L;
    }

    @Override
    public void incSentMessagesTime(long nanos) {}

    @Override
    public long getBroadcastMessages() {
      return 0L;
    }

    @Override
    public void incBroadcastMessages(long messages) {}

    @Override
    public long getBroadcastMessagesTime() {
      return 0L;
    }

    @Override
    public void incBroadcastMessagesTime(long nanos) {}

    @Override
    public long getReceivedMessages() {
      return 0L;
    }

    @Override
    public void incReceivedMessages(long messages) {}

    @Override
    public long getReceivedBytes() {
      return 0L;
    }

    @Override
    public void incReceivedBytes(long bytes) {}

    @Override
    public void incSentBytes(long bytes) {}

    @Override
    public long startUDPDispatchRequest() {
      return 0L;
    }

    @Override
    public void endUDPDispatchRequest(long start) {

    }

    @Override
    public long getProcessedMessages() {
      return 0L;
    }

    @Override
    public void incProcessedMessages(long messages) {}

    @Override
    public long getProcessedMessagesTime() {
      return 0L;
    }

    @Override
    public void incProcessedMessagesTime(long nanos) {}

    @Override
    public long getMessageProcessingScheduleTime() {
      return 0L;
    }

    @Override
    public void incMessageProcessingScheduleTime(long nanos) {}

    @Override
    public long getOverflowQueueSize() {
      return 0L;
    }

    @Override
    public void incOverflowQueueSize(long messages) {}

    @Override
    public long getNumProcessingThreads() {
      return 0L;
    }

    @Override
    public void incNumProcessingThreads(long threads) {}

    @Override
    public long getNumSerialThreads() {
      return 0L;
    }

    @Override
    public void incNumSerialThreads(long threads) {}

    @Override
    public void incMessageChannelTime(long val) {}

    @Override
    public long getUDPDispatchRequestTime() {
      return 0L;
    }

    @Override
    public long getReplyMessageTime() {
      return 0L;
    }

    @Override
    public void incReplyMessageTime(long val) {}

    @Override
    public long getDistributeMessageTime() {
      return 0L;
    }

    @Override
    public void incDistributeMessageTime(long val) {}

    @Override
    public long getNodes() {
      return 0L;
    }

    @Override
    public void setNodes(long val) {}

    @Override
    public void incNodes(long val) {}

    @Override
    public long getReplyWaitsInProgress() {
      return 0L;
    }

    @Override
    public long getReplyWaitsCompleted() {
      return 0L;
    }

    @Override
    public long getReplyWaitTime() {
      return 0L;
    }

    @Override
    public long startReplyWait() {
      return 0L;
    }

    @Override
    public void endReplyWait(long startNanos, long startMillis) {}

    @Override
    public void incReplyTimeouts() {}

    @Override
    public long getReplyTimeouts() {
      return 0L;
    }

    @Override
    public void incReceivers() {}

    @Override
    public void decReceivers() {}

    @Override
    public void incFailedAccept() {}

    @Override
    public void incFailedConnect() {}

    @Override
    public void incReconnectAttempts() {}

    @Override
    public long getReconnectAttempts() {
      return 0L;
    }

    @Override
    public void incLostLease() {}

    @Override
    public long startSenderCreate() {
      return 0L;
    }

    @Override
    public void incSenders(boolean shared, boolean preserveOrder, long start) {}

    @Override
    public void decSenders(boolean shared, boolean preserveOrder) {}

    @Override
    public long getSendersSU() {
      return 0L;
    }

    @Override
    public long startSocketWrite(boolean sync) {
      return 0L;
    }

    @Override
    public void endSocketWrite(boolean sync, long start, long bytesWritten, long retries) {}

    @Override
    public long startSerialization() {
      return 0L;
    }

    @Override
    public void endSerialization(long start, int bytes) {}

    @Override
    public long startDeserialization() {
      return 0L;
    }

    @Override
    public void endDeserialization(long start, int bytes) {}

    @Override
    public long startMsgSerialization() {
      return 0L;
    }

    @Override
    public void endMsgSerialization(long start) {}

    @Override
    public long startMsgDeserialization() {
      return 0L;
    }

    @Override
    public void endMsgDeserialization(long start) {}

    @Override
    public void incBatchSendTime(long start) {}

    @Override
    public void incBatchCopyTime(long start) {}

    @Override
    public void incBatchWaitTime(long start) {}

    @Override
    public void incBatchFlushTime(long start) {}

    @Override
    public void incUcastWriteBytes(long bytesWritten) {}

    @Override
    public void incMcastWriteBytes(long bytesWritten) {}

    @Override
    public void incUcastRetransmits() {}

    @Override
    public void incMcastRetransmits() {}

    @Override
    public void incMcastRetransmitRequests() {}

    @Override
    public long getMcastRetransmits() {
      return 0L;
    }

    @Override
    public long getMcastWrites() {
      return 0L;
    }

    @Override
    public long getMcastReads() {
      return 0L;
    }

    @Override
    public void incUcastReadBytes(long amount) {}

    @Override
    public void incMcastReadBytes(long amount) {}

    @Override
    public long getAsyncSocketWritesInProgress() {
      return 0L;
    }

    @Override
    public long getAsyncSocketWrites() {
      return 0L;
    }

    @Override
    public long getAsyncSocketWriteRetries() {
      return 0L;
    }

    @Override
    public long getAsyncSocketWriteBytes() {
      return 0L;
    }

    @Override
    public long getAsyncSocketWriteTime() {
      return 0L;
    }

    @Override
    public long getAsyncQueues() {
      return 0L;
    }

    @Override
    public void incAsyncQueues(long inc) {}

    @Override
    public long getAsyncQueueFlushesInProgress() {
      return 0L;
    }

    @Override
    public long getAsyncQueueFlushesCompleted() {
      return 0L;
    }

    @Override
    public long getAsyncQueueFlushTime() {
      return 0L;
    }

    @Override
    public long startAsyncQueueFlush() {
      return 0L;
    }

    @Override
    public void endAsyncQueueFlush(long start) {}

    @Override
    public long getAsyncQueueTimeouts() {
      return 0L;
    }

    @Override
    public void incAsyncQueueTimeouts(long inc) {}

    @Override
    public long getAsyncQueueSizeExceeded() {
      return 0L;
    }

    @Override
    public void incAsyncQueueSizeExceeded(long inc) {}

    @Override
    public long getAsyncDistributionTimeoutExceeded() {
      return 0L;
    }

    @Override
    public void incAsyncDistributionTimeoutExceeded() {}

    @Override
    public long getAsyncQueueSize() {
      return 0L;
    }

    @Override
    public void incAsyncQueueSize(long inc) {}

    @Override
    public long getAsyncQueuedMsgs() {
      return 0L;
    }

    @Override
    public void incAsyncQueuedMsgs() {}

    @Override
    public long getAsyncDequeuedMsgs() {
      return 0L;
    }

    @Override
    public void incAsyncDequeuedMsgs() {}

    @Override
    public long getAsyncConflatedMsgs() {
      return 0L;
    }

    @Override
    public void incAsyncConflatedMsgs() {}

    @Override
    public long getAsyncThreads() {
      return 0L;
    }

    @Override
    public void incAsyncThreads(long inc) {}

    @Override
    public long getAsyncThreadInProgress() {
      return 0L;
    }

    @Override
    public long getAsyncThreadCompleted() {
      return 0L;
    }

    @Override
    public long getAsyncThreadTime() {
      return 0L;
    }

    @Override
    public long startAsyncThread() {
      return 0L;
    }

    @Override
    public void endAsyncThread(long start) {}

    @Override
    public long getAsyncQueueAddTime() {
      return 0L;
    }

    @Override
    public void incAsyncQueueAddTime(long inc) {}

    @Override
    public long getAsyncQueueRemoveTime() {
      return 0L;
    }

    @Override
    public void incAsyncQueueRemoveTime(long inc) {}

    @Override
    public void incReceiverBufferSize(long inc, boolean direct) {}

    @Override
    public void incSenderBufferSize(long inc, boolean direct) {}

    @Override
    public long startSocketLock() {
      return 0L;
    }

    @Override
    public void endSocketLock(long start) {}

    @Override
    public long startBufferAcquire() {
      return 0L;
    }

    @Override
    public void endBufferAcquire(long start) {}

    @Override
    public void incMessagesBeingReceived(boolean newMsg, long bytes) {}

    @Override
    public void decMessagesBeingReceived(long bytes) {}

    @Override
    public void incReplyHandOffTime(long start) {}

    @Override
    public long getElders() {
      return 0L;
    }

    @Override
    public void incElders(long val) {}

    @Override
    public long getInitialImageMessagesInFlight() {
      return 0L;
    }

    @Override
    public void incInitialImageMessagesInFlight(long val) {}

    @Override
    public long getInitialImageRequestsInProgress() {
      return 0L;
    }

    @Override
    public void incInitialImageRequestsInProgress(long val) {}

    @Override
    public void incPdxSerialization(long bytesWritten) {}

    @Override
    public void incPdxDeserialization(long i) {}

    @Override
    public long startPdxInstanceDeserialization() {
      return 0L;
    }

    @Override
    public void endPdxInstanceDeserialization(long start) {}

    @Override
    public void incPdxInstanceCreations() {}

    @Override
    public void incThreadOwnedReceivers(long value, long dominoCount) {}

    @Override
    public long getHeartbeatRequestsSent() {
      return 0L;
    }

    @Override
    public void incHeartbeatRequestsSent() {}

    @Override
    public long getHeartbeatRequestsReceived() {
      return 0L;
    }

    @Override
    public void incHeartbeatRequestsReceived() {}

    @Override
    public long getHeartbeatsSent() {
      return 0L;
    }

    @Override
    public void incHeartbeatsSent() {}

    @Override
    public long getHeartbeatsReceived() {
      return 0L;
    }

    @Override
    public void incHeartbeatsReceived() {}

    @Override
    public long getSuspectsSent() {
      return 0L;
    }

    @Override
    public void incSuspectsSent() {}

    @Override
    public long getSuspectsReceived() {
      return 0L;
    }

    @Override
    public void incSuspectsReceived() {}

    @Override
    public long getFinalCheckRequestsSent() {
      return 0L;
    }

    @Override
    public void incFinalCheckRequestsSent() {}

    @Override
    public long getFinalCheckRequestsReceived() {
      return 0L;
    }

    @Override
    public void incFinalCheckRequestsReceived() {}

    @Override
    public long getFinalCheckResponsesSent() {
      return 0L;
    }

    @Override
    public void incFinalCheckResponsesSent() {}

    @Override
    public long getFinalCheckResponsesReceived() {
      return 0L;
    }

    @Override
    public void incFinalCheckResponsesReceived() {}

    @Override
    public long getTcpFinalCheckRequestsSent() {
      return 0L;
    }

    @Override
    public void incTcpFinalCheckRequestsSent() {}

    @Override
    public long getTcpFinalCheckRequestsReceived() {
      return 0L;
    }

    @Override
    public void incTcpFinalCheckRequestsReceived() {}

    @Override
    public long getTcpFinalCheckResponsesSent() {
      return 0L;
    }

    @Override
    public void incTcpFinalCheckResponsesSent() {}

    @Override
    public long getTcpFinalCheckResponsesReceived() {
      return 0L;
    }

    @Override
    public void incTcpFinalCheckResponsesReceived() {}

    @Override
    public long getUdpFinalCheckRequestsSent() {
      return 0L;
    }

    @Override
    public void incUdpFinalCheckRequestsSent() {}

    @Override
    public long getUdpFinalCheckResponsesReceived() {
      return 0L;
    }

    @Override
    public void incUdpFinalCheckResponsesReceived() {}

    @Override
    public long startUDPMsgEncryption() {
      return 0L;
    }

    @Override
    public void endUDPMsgEncryption(long start) {}

    @Override
    public long startUDPMsgDecryption() {
      return 0L;
    }

    @Override
    public void endUDPMsgDecryption(long start) {}

    @Override
    public long getUDPMsgEncryptionTime() {
      return 0L;
    }

    @Override
    public long getUDPMsgDecryptionTime() {
      return 0L;
    }
  }

  @Override
  public void throwIfDistributionStopped() {
    stopper.checkCancelInProgress(null);
  }

  /** Returns count of members filling the specified role */
  @Override
  public int getRoleCount(Role role) {
    return localAddress.getRoles().contains(role) ? 1 : 0;
  }

  /** Returns true if at least one member is filling the specified role */
  @Override
  public boolean isRolePresent(Role role) {
    return localAddress.getRoles().contains(role);
  }

  /** Returns a set of all roles currently in the distributed system. */
  @Override
  public Set getAllRoles() {
    return localAddress.getRoles();
  }

  private int lonerPort = 0;

  private InternalDistributedMember generateMemberId() {
    InternalDistributedMember result;
    String host;
    try {
      // create string of the current millisecond clock time
      StringBuilder sb = new StringBuilder();
      // use low four bytes for backward compatibility
      long time = System.currentTimeMillis() & 0xffffffffL;
      for (long i = 0; i < 4; i++) {
        String hex = Integer.toHexString((int) (time & 0xff));
        if (hex.length() < 2) {
          sb.append('0');
        }
        sb.append(hex);
        time = time / 0x100;
      }
      String uniqueString = sb.toString();

      String name = this.system.getName();

      host = SocketCreator.getClientHostName();
      DistributionConfig config = system.getConfig();
      DurableClientAttributes dac = null;
      if (config.getDurableClientId() != null) {
        dac = new DurableClientAttributes(config.getDurableClientId(),
            config.getDurableClientTimeout());
      }
      result = new InternalDistributedMember(host, lonerPort, name, uniqueString,
          ClusterDistributionManager.LONER_DM_TYPE,
          MemberDataBuilder.parseGroups(config.getRoles(), config.getGroups()), dac);

    } catch (UnknownHostException ex) {
      throw new InternalGemFireError(
          "Cannot resolve local host name to an IP address");
    }
    return result;
  }

  /**
   * update the loner port with an integer that may be more unique than the default port (zero).
   * This updates the ID in place and establishes new default settings for the manufacture of new
   * IDs.
   *
   * @param newPort the new port to use
   */
  public void updateLonerPort(int newPort) {
    this.logger.config(
        String.format("Updating membership port.  Port changed from %s to %s.  ID is now %s",
            this.lonerPort, newPort, getId()));
    this.lonerPort = newPort;
    this.getId().setPort(this.lonerPort);
  }

  @Override
  public boolean isCurrentMember(DistributedMember p_id) {
    return getId().equals(p_id);
  }

  @Override
  @Nullable
  public Set<InternalDistributedMember> putOutgoing(@NotNull DistributionMessage msg) {
    return null;
  }

  @Override
  public boolean shutdownInProgress() {
    return false;
  }

  @Override
  public void removeUnfinishedStartup(InternalDistributedMember m, boolean departed) {}

  @Override
  public void setUnfinishedStartups(Collection<InternalDistributedMember> s) {}

  protected static class Stopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      checkFailure();
      return null;
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      return null;
    }
  }

  private final Stopper stopper = new Stopper();
  private volatile InternalCache cache;

  @Override
  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.distributed.internal.DM#getMembershipManager()
   */
  @Override
  public Distribution getDistribution() {
    // no membership manager
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.distributed.internal.DM#getRootCause()
   */
  @Override
  public Throwable getRootCause() {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.distributed.internal.DM#setRootCause(java.lang.Throwable)
   */
  @Override
  public void setRootCause(Throwable t) {}

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.distributed.internal.DM#getMembersOnThisHost()
   *
   * @since GemFire 5.9
   */
  @Override
  public Set<InternalDistributedMember> getMembersInThisZone() {
    return this.allIds;
  }

  @Override
  public void acquireGIIPermitUninterruptibly() {}

  @Override
  public void releaseGIIPermit() {}

  @Override
  public int getDistributedSystemId() {
    return getSystem().getConfig().getDistributedSystemId();
  }

  @Override
  public boolean enforceUniqueZone() {
    return system.getConfig().getEnforceUniqueHost()
        || (system.getConfig().getRedundancyZone() != null
            && !system.getConfig().getRedundancyZone().isEmpty());
  }

  @Override
  public String getRedundancyZone(InternalDistributedMember member) {
    return null;
  }

  @Override
  public boolean areInSameZone(InternalDistributedMember member1,
      InternalDistributedMember member2) {
    return false;
  }

  @Override
  public boolean areOnEquivalentHost(InternalDistributedMember member1,
      InternalDistributedMember member2) {
    return member1 == member2;
  }

  @Override
  public Set<InternalDistributedMember> getMembersInSameZone(
      InternalDistributedMember acceptedMember) {
    return Collections.singleton(acceptedMember);
  }

  @Override
  public Set<DistributedMember> getGroupMembers(String group) {
    if (getDistributionManagerId().getGroups().contains(group)) {
      return Collections.singleton(getDistributionManagerId());
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public void addHostedLocators(InternalDistributedMember member, Collection<String> locators,
      boolean isSharedConfigurationEnabled) {
    // no-op
  }

  @Override
  public Collection<String> getHostedLocators(InternalDistributedMember member) {
    return Collections.emptyList();
  }

  @Override
  public Map<InternalDistributedMember, Collection<String>> getAllHostedLocators() {
    return Collections.emptyMap();
  }

  @Override
  public Set<InternalDistributedMember> getNormalDistributionManagerIds() {
    return getDistributionManagerIds();
  }

  public Set<InternalDistributedMember> getLocatorDistributionManagerIds() {
    return Collections.emptySet();
  }

  @Override
  public Map<InternalDistributedMember, Collection<String>> getAllHostedLocatorsWithSharedConfiguration() {
    return Collections.emptyMap();
  }

  @Override
  public void forceUDPMessagingForCurrentThread() {
    // no-op for loners
  }

  @Override
  public void releaseUDPMessagingForCurrentThread() {
    // no-op for loners
  }

  @Override
  public int getDMType() {
    return 0;
  }

  @Override
  public void setCache(InternalCache instance) {
    this.cache = instance;
  }

  @Override
  public InternalCache getCache() {
    return this.cache;
  }

  @Override
  public InternalCache getExistingCache() {
    InternalCache result = this.cache;
    if (result == null) {
      throw new CacheClosedException(
          "A cache has not yet been created.");
    }
    result.getCancelCriterion().checkCancelInProgress(null);
    if (result.isClosed()) {
      throw result.getCacheClosedException(
          "The cache has been closed.", null);
    }
    return result;
  }

  @Override
  public HealthMonitor getHealthMonitor(InternalDistributedMember owner) {
    throw new UnsupportedOperationException(
        "getHealthMonitor is not supported by " + getClass().getSimpleName());
  }

  @Override
  public void removeHealthMonitor(InternalDistributedMember owner, int theId) {
    throw new UnsupportedOperationException(
        "removeHealthMonitor is not supported by " + getClass().getSimpleName());
  }

  @Override
  public void createHealthMonitor(InternalDistributedMember owner, GemFireHealthConfig cfg) {
    throw new UnsupportedOperationException(
        "createHealthMonitor is not supported by " + getClass().getSimpleName());
  }

  @Override
  public boolean exceptionInThreads() {
    return false;
  }

  @Override
  public void clearExceptionInThreads() {
    // no-op
  }

  @Override
  /* returns the Threads Monitoring instance */
  public ThreadsMonitoring getThreadMonitoring() {
    return this.threadMonitor;
  }

  @Override
  public AlertingService getAlertingService() {
    return NullAlertingService.get();
  }

  @Override
  public void registerTestHook(MembershipTestHook mth) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unregisterTestHook(MembershipTestHook mth) {
    throw new UnsupportedOperationException();
  }
}
