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

import java.io.NotSerializableException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.ToDataException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.direct.DirectChannel;
import org.apache.geode.distributed.internal.direct.ShunnedMemberException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipTestHook;
import org.apache.geode.distributed.internal.membership.MembershipView;
import org.apache.geode.distributed.internal.membership.QuorumChecker;
import org.apache.geode.distributed.internal.membership.adapter.ServiceConfig;
import org.apache.geode.distributed.internal.membership.adapter.auth.GMSAuthenticator;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipManager;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.api.Membership;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.api.MessageListener;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.tcp.ConnectExceptions;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.logging.internal.executors.LoggingThread;

public class MembershipManagerAdapter {
  private static final Logger logger = Services.getLogger();

  private final ClusterDistributionManager clusterDistributionManager;
  private final boolean tcpDisabled;
  private final boolean mcastEnabled;
  private final long ackSevereAlertThreshold;
  private final long ackWaitThreshold;
  private final RemoteTransportConfig transportConfig;
  private final Membership membershipManager;
  private DirectChannel directChannel;

  /**
   * thread-local used to force use of Messenger for communications, usually to avoid deadlock when
   * conserve-sockets=true. Use of this should be removed when connection pools are implemented in
   * the direct-channel
   */
  private final ThreadLocal<Boolean> forceUseUDPMessaging =
      ThreadLocal.withInitial(() -> Boolean.FALSE);

  private MyDCReceiver dcReceiver;
  private final long memberTimeout;


  public MembershipManagerAdapter(final ClusterDistributionManager clusterDistributionManager,
      final RemoteTransportConfig transport, final InternalDistributedSystem system,
      final MembershipListener listener,
      final MessageListener messageListener) {
    this.clusterDistributionManager = clusterDistributionManager;
    this.transportConfig = transport;
    this.tcpDisabled = transportConfig.isTcpDisabled();
    // cache these settings for use in send()
    mcastEnabled = transportConfig.isMcastEnabled();
    ackSevereAlertThreshold = system.getConfig().getAckSevereAlertThreshold();
    ackWaitThreshold = system.getConfig().getAckWaitThreshold();
    if (!tcpDisabled) {
      dcReceiver = new MyDCReceiver();
    }

    memberTimeout = system.getConfig().getMemberTimeout();
    membershipManager = MembershipBuilder.newMembershipBuilder(
        clusterDistributionManager)
        .setAuthenticator(
            new GMSAuthenticator(system.getSecurityProperties(), system.getSecurityService(),
                system.getSecurityLogWriter(), system.getInternalLogWriter()))
        .setStatistics(clusterDistributionManager.stats)
        .setMessageListener(messageListener)
        .setMembershipListener(listener)
        .setConfig(new ServiceConfig(transport, system.getConfig()))
        .setSerializer(InternalDataSerializer.getDSFIDSerializer())
        .setDirectChannelCallbacks(new LifecycleListenerImpl(this))
        .setMemberIDFactory(new ClusterDistributionManager.ClusterDistributionManagerIDFactory())
        .create();
  }

  @VisibleForTesting
  MembershipManagerAdapter(final ClusterDistributionManager clusterDistributionManager,
      final RemoteTransportConfig transport, final InternalDistributedSystem system,
      Membership membershipManager) {
    this.clusterDistributionManager = clusterDistributionManager;
    this.transportConfig = transport;
    this.tcpDisabled = transportConfig.isTcpDisabled();
    // cache these settings for use in send()
    mcastEnabled = transportConfig.isMcastEnabled();
    ackSevereAlertThreshold = system.getConfig().getAckSevereAlertThreshold();
    ackWaitThreshold = system.getConfig().getAckWaitThreshold();
    if (!tcpDisabled) {
      dcReceiver = new MyDCReceiver();
    }
    memberTimeout = system.getConfig().getMemberTimeout();
    this.membershipManager = membershipManager;
  }

  static MembershipManagerAdapter createMembershipManagerAdapter(
      ClusterDistributionManager clusterDistributionManager, RemoteTransportConfig transport,
      InternalDistributedSystem system,
      org.apache.geode.distributed.internal.membership.gms.api.MembershipListener listener,
      MessageListener messageListener) {

    return new MembershipManagerAdapter(clusterDistributionManager, transport, system, listener,
        messageListener);
  }


  public MembershipView getView() {
    return membershipManager.getView();
  }

  public InternalDistributedMember getLocalMember() {
    return membershipManager.getLocalMember();
  }

  public Set<InternalDistributedMember> send(InternalDistributedMember[] destinations,
      DistributionMessage msg) throws NotSerializableException {
    Set<InternalDistributedMember> result;
    boolean allDestinations = msg.forAll();

    membershipManager.checkCancelled();

    membershipManager.waitIfPlayingDead();

    if (membershipManager.isJoining()) {
      // If we get here, we are starting up, so just report a failure.
      if (allDestinations)
        return null;
      else {
        result = new HashSet<>();
        Collections.addAll(result, destinations);
        return result;
      }
    }

    if (msg instanceof AdminMessageType && shutdownInProgress()) {
      // no admin messages while shutting down - this can cause threads to hang
      return new HashSet<>(Arrays.asList(msg.getRecipients()));
    }

    // Handle trivial cases
    if (destinations == null) {
      if (logger.isTraceEnabled())
        logger.trace("Membership: Message send: returning early because null set passed in: '{}'",
            msg);
      return null; // trivially: all recipients received the message
    }
    if (destinations.length == 0) {
      if (logger.isTraceEnabled())
        logger.trace(
            "Membership: Message send: returning early because empty destination list passed in: '{}'",
            msg);
      return null; // trivially: all recipients received the message
    }

    msg.setSender(getLocalMember());

    msg.setBreadcrumbsInSender();
    Breadcrumbs.setProblem(null);

    boolean useMcast = false;
    if (mcastEnabled) {
      useMcast = (msg.getMulticast() || allDestinations);
    }

    boolean sendViaMessenger = isForceUDPCommunications() || (msg instanceof ShutdownMessage);

    if (useMcast || tcpDisabled || sendViaMessenger) {
      result = membershipManager.send(destinations, msg);
    } else {
      result = directChannelSend(destinations, msg);
    }

    // If the message was a broadcast, don't enumerate failures.
    if (allDestinations)
      return null;
    else {
      return result;
    }
  }

  /**
   * Perform the grossness associated with sending a message over a DirectChannel
   *
   * @param destinations the list of destinations
   * @param content the message
   * @return all recipients who did not receive the message (null if all received it)
   * @throws NotSerializableException if the message is not serializable
   */
  public Set<InternalDistributedMember> directChannelSend(
      InternalDistributedMember[] destinations,
      DistributionMessage content)
      throws NotSerializableException {
    MembershipStatistics theStats = clusterDistributionManager.getStats();
    boolean allDestinations;
    InternalDistributedMember[] keys;
    if (content.forAll()) {
      allDestinations = true;
      keys = membershipManager.getAllMembers();
    } else {
      allDestinations = false;
      keys = destinations;
    }

    int sentBytes;
    try {
      sentBytes =
          directChannel.send(membershipManager, keys, content, ackWaitThreshold,
              ackSevereAlertThreshold);

      if (theStats != null) {
        theStats.incSentBytes(sentBytes);
      }

      if (sentBytes == 0) {
        membershipManager.checkCancelled();
      }
    } catch (ConnectExceptions ex) {
      // Check if the connect exception is due to system shutting down.
      if (membershipManager.shutdownInProgress()) {
        membershipManager.checkCancelled();
        throw new DistributedSystemDisconnectedException();
      }

      if (allDestinations)
        return null;

      // We need to return this list of failures
      List<InternalDistributedMember> members = ex.getMembers();

      // SANITY CHECK: If we fail to send a message to an existing member
      // of the view, we have a serious error (bug36202).


      // Iterate through members and causes in tandem :-(
      Iterator it_mem = members.iterator();
      Iterator it_causes = ex.getCauses().iterator();
      while (it_mem.hasNext()) {
        InternalDistributedMember member = (InternalDistributedMember) it_mem.next();
        Throwable th = (Throwable) it_causes.next();

        if (!membershipManager.hasMember(member) || (th instanceof ShunnedMemberException)) {
          continue;
        }
        logger
            .fatal(String.format("Failed to send message <%s> to member <%s> view, %s",
                // TODO - This used to be services.getJoinLeave().getView(), which is a different
                // view object. Is it ok to log membershipManager.getView here?
                new Object[] {content, member, membershipManager.getView()}),
                th);
        // Assert.assertTrue(false, "messaging contract failure");
      }
      return new HashSet<>(members);
    } // catch ConnectionExceptions
    catch (ToDataException | CancelException e) {
      throw e;
    } catch (NotSerializableException | RuntimeException | Error e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Membership: directChannelSend caught exception: {}",
            e.getMessage(), e);
      }
      throw e;
    }
    return null;
  }


  public Map<String, Long> getMessageState(
      DistributedMember member, boolean includeMulticast) {
    final HashMap<String, Long> result = new HashMap<>();
    DirectChannel dc = directChannel;
    if (dc != null) {
      dc.getChannelStates(member, result);
    }
    return membershipManager.getMessageState(member, includeMulticast, result);
  }

  public void waitForMessageState(DistributedMember member,
      Map<String, Long> state) throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    DirectChannel dc = directChannel;
    if (dc != null) {
      dc.waitForChannelState(member, state);
    }
    membershipManager.waitForMessageState(member, state);


    if (mcastEnabled && !tcpDisabled) {
      // GEODE-2865: wait for scheduled multicast messages to be applied to the cache
      waitForSerialMessageProcessing((InternalDistributedMember) member);
    }
  }

  public boolean requestMemberRemoval(DistributedMember member,
      String reason) {
    return membershipManager.requestMemberRemoval(member, reason);
  }

  public boolean verifyMember(DistributedMember mbr,
      String reason) {
    return membershipManager.verifyMember(mbr, reason);
  }

  public boolean isShunned(DistributedMember m) {
    return membershipManager.isShunned(m);
  }

  public <V> V doWithViewLocked(
      Function<Membership, V> function) {
    return membershipManager.doWithViewLocked(function);
  }

  public boolean memberExists(DistributedMember m) {
    return membershipManager.memberExists(m);
  }

  public boolean isConnected() {
    return membershipManager.isConnected();
  }

  public void beSick() {
    membershipManager.beSick();
  }

  public void playDead() {
    membershipManager.playDead();
  }

  public void beHealthy() {
    membershipManager.beHealthy();
  }

  public boolean isBeingSick() {
    return membershipManager.isBeingSick();
  }

  public void disconnect(boolean beforeJoined) {
    membershipManager.disconnect(beforeJoined);
  }

  public void shutdown() {
    membershipManager.shutdown();
  }

  public void uncleanShutdown(String reason, Exception e) {
    membershipManager.uncleanShutdown(reason, e);
  }

  public void shutdownMessageReceived(DistributedMember id,
      String reason) {
    membershipManager.shutdownMessageReceived(id, reason);
  }

  public void waitForEventProcessing() throws InterruptedException {
    membershipManager.waitForEventProcessing();
  }

  public void startEventProcessing() {
    membershipManager.startEventProcessing();
  }

  public void setShutdown() {
    membershipManager.setShutdown();
  }

  public void setReconnectCompleted(boolean reconnectCompleted) {
    membershipManager.setReconnectCompleted(reconnectCompleted);
  }

  public boolean shutdownInProgress() {
    return membershipManager.shutdownInProgress();
  }

  public boolean waitForNewMember(DistributedMember remoteId) {
    return membershipManager.waitForNewMember(remoteId);
  }

  public void emergencyClose() {
    membershipManager.emergencyClose();
    if (directChannel != null) {
      directChannel.emergencyClose();
    }
  }

  public void addSurpriseMemberForTesting(DistributedMember mbr,
      long birthTime) {
    membershipManager.addSurpriseMemberForTesting(mbr, birthTime);
  }

  public void suspectMembers(Set<DistributedMember> members,
      String reason) {
    membershipManager.suspectMembers(members, reason);
  }

  public void suspectMember(DistributedMember member,
      String reason) {
    membershipManager.suspectMember(member, reason);
  }

  public Throwable getShutdownCause() {
    return membershipManager.getShutdownCause();
  }

  public void registerTestHook(
      MembershipTestHook mth) {
    membershipManager.registerTestHook(mth);
  }

  public void unregisterTestHook(
      MembershipTestHook mth) {
    membershipManager.unregisterTestHook(mth);
  }

  public void warnShun(DistributedMember mbr) {
    membershipManager.warnShun(mbr);
  }

  public boolean addSurpriseMember(DistributedMember mbr) {
    return membershipManager.addSurpriseMember(mbr);
  }

  public void startupMessageFailed(DistributedMember mbr,
      String failureMessage) {
    membershipManager.startupMessageFailed(mbr, failureMessage);
  }

  public boolean testMulticast() {
    return membershipManager.testMulticast();
  }

  public boolean isSurpriseMember(DistributedMember m) {
    return membershipManager.isSurpriseMember(m);
  }

  public QuorumChecker getQuorumChecker() {
    return membershipManager.getQuorumChecker();
  }

  public void releaseQuorumChecker(
      QuorumChecker checker,
      InternalDistributedSystem distributedSystem) {
    membershipManager.releaseQuorumChecker(checker, distributedSystem);
  }

  public DistributedMember getCoordinator() {
    return membershipManager.getCoordinator();
  }

  public Set<InternalDistributedMember> getMembersNotShuttingDown() {
    return membershipManager.getMembersNotShuttingDown();
  }

  public Services getServices() {
    return membershipManager.getServices();
  }

  // TODO - this method is only used by tests
  @VisibleForTesting
  public void forceDisconnect(String reason) {
    ((GMSMembershipManager) membershipManager).getGMSManager().forceDisconnect(reason);
  }

  // TODO - this method is only used by tests
  @VisibleForTesting
  public void replacePartialIdentifierInMessage(DistributionMessage message) {
    ((GMSMembershipManager) membershipManager).replacePartialIdentifierInMessage(message);

  }

  // TODO - this method is only used by tests
  @VisibleForTesting
  public boolean isCleanupTimerStarted() {
    return ((GMSMembershipManager) membershipManager).isCleanupTimerStarted();
  }

  // TODO - this method is only used by tests
  @VisibleForTesting
  public long getSurpriseMemberTimeout() {
    return ((GMSMembershipManager) membershipManager).getSurpriseMemberTimeout();
  }

  // TODO - this method is only used by tests
  @VisibleForTesting
  public void installView(GMSMembershipView newView) {
    ((GMSMembershipManager) membershipManager).getGMSManager().installView(newView);
  }

  // TODO - this method is only used by tests
  @VisibleForTesting
  public int getDirectChannelPort() {
    return directChannel == null ? 0 : directChannel.getPort();
  }

  /**
   * for mock testing this allows insertion of a DirectChannel mock
   */
  // TODO - this method is only used by tests
  @VisibleForTesting
  void setDirectChannel(DirectChannel dc) {
    this.directChannel = dc;
    // this.tcpDisabled = false;
  }

  public void disableDisconnectOnQuorumLossForTesting() {
    ((GMSMembershipManager) membershipManager).disableDisconnectOnQuorumLossForTesting();
  }

  private void startDirectChannel(final MemberIdentifier memberID) {
    int dcPort = 0;

    if (!tcpDisabled) {
      directChannel = new DirectChannel(membershipManager, dcReceiver, clusterDistributionManager);
      dcPort = directChannel.getPort();
    }
    memberID.setDirectChannelPort(dcPort);
  }

  private boolean disconnectDirectChannel(final Exception exception) {
    if (directChannel != null) {
      directChannel.disconnect(exception);
      return true;
    }
    return false;
  }

  private void setDirectChannelLocalAddress(final InternalDistributedMember address) {
    if (directChannel != null) {
      directChannel.setLocalAddr(address);
    }
  }

  private void destroyMember(final InternalDistributedMember member, final String reason) {
    final DirectChannel dc = directChannel;
    if (dc != null) {
      // Bug 37944: make sure this is always done in a separate thread,
      // so that shutdown conditions don't wedge the view lock
      // fix for bug 34010
      new LoggingThread("disconnect thread for " + member, () -> {
        try {
          Thread.sleep(Integer.getInteger("p2p.disconnectDelay", 3000).intValue());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          // Keep going, try to close the endpoint.
        }
        if (!dc.isOpen()) {
          return;
        }
        if (logger.isDebugEnabled())
          logger.debug("Membership: closing connections for departed member {}", member);
        // close connections, but don't do membership notification since it's already been done
        dc.closeEndpoint(member, reason, false);
      }).start();
    }
  }

  /**
   * Forces use of UDP for communications in the current thread. UDP is connectionless, so no tcp/ip
   * connections will be created or used for messaging until this setting is released with
   * releaseUDPMessagingForCurrentThread.
   */
  void forceUDPMessagingForCurrentThread() {
    forceUseUDPMessaging.set(Boolean.TRUE);
  }

  /**
   * Releases use of UDP for all communications in the current thread, as established by
   * forceUDPMessagingForCurrentThread.
   */
  void releaseUDPMessagingForCurrentThread() {
    forceUseUDPMessaging.set(Boolean.FALSE);
  }

  private boolean isForceUDPCommunications() {
    return forceUseUDPMessaging.get();
  }

  /**
   * Wait for the given member to not be in the membership view and for all direct-channel receivers
   * for this member to be closed.
   *
   * @param mbr the member
   * @return for testing purposes this returns true if the serial queue for the member was flushed
   * @throws InterruptedException if interrupted by another thread
   * @throws TimeoutException if we wait too long for the member to go away
   */
  public boolean waitForDeparture(DistributedMember mbr)
      throws TimeoutException, InterruptedException {
    return waitForDeparture(mbr, memberTimeout * 4);
  }

  /**
   * Wait for the given member to not be in the membership view and for all direct-channel receivers
   * for this member to be closed.
   *
   * @param mbr the member
   * @param timeoutMs amount of time to wait before giving up
   * @return for testing purposes this returns true if the serial queue for the member was flushed
   * @throws InterruptedException if interrupted by another thread
   * @throws TimeoutException if we wait too long for the member to go away
   */
  public boolean waitForDeparture(DistributedMember mbr, long timeoutMs)
      throws TimeoutException, InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    boolean result = false;
    // TODO - Move the bulk of this method to the adapter.
    DirectChannel dc = directChannel;
    InternalDistributedMember idm = (InternalDistributedMember) mbr;
    long pauseTime = (timeoutMs < 4000) ? 100 : timeoutMs / 40;
    boolean wait;
    int numWaits = 0;
    do {
      wait = false;
      if (dc != null) {
        if (dc.hasReceiversFor(idm)) {
          wait = true;
        }
        if (wait && logger.isDebugEnabled()) {
          logger.info("waiting for receivers for {} to shut down", mbr);
        }
      }
      if (!wait) {
        wait = memberExists(idm);
        if (wait && logger.isDebugEnabled()) {
          logger.debug("waiting for {} to leave the membership view", mbr);
        }
      }
      if (!wait) {
        if (waitForSerialMessageProcessing(idm)) {
          result = true;
        }
      }
      if (wait) {
        numWaits++;
        if (numWaits > 40) {
          throw new TimeoutException("waited too long for " + idm + " to be removed");
        }
        Thread.sleep(pauseTime);
      }
    } while (wait && (dc != null && dc.isOpen())
        && !shutdownInProgress());
    if (logger.isDebugEnabled()) {
      logger.debug("operations for {} should all be in the cache at this point", mbr);
    }
    return result;
  }

  /**
   * wait for serial executor messages from the given member to be processed
   */
  private boolean waitForSerialMessageProcessing(InternalDistributedMember idm)
      throws InterruptedException {
    // run a message through the member's serial execution queue to ensure that all of its
    // current messages have been processed
    boolean result = false;
    OverflowQueueWithDMStats<Runnable> serialQueue =
        clusterDistributionManager.getExecutors().getSerialQueue(idm);
    if (serialQueue != null) {
      final boolean done[] = new boolean[1];
      final FlushingMessage msg = new FlushingMessage(done);
      serialQueue.add(new SizeableRunnable(100) {
        @Override
        public void run() {
          msg.invoke();
        }

        public String toString() {
          return "Processing fake message";
        }
      });
      synchronized (done) {
        while (!done[0]) {
          done.wait(10);
        }
        result = true;
      }
    }
    return result;
  }


  /**
   * Insert our own MessageReceiver between us and the direct channel, in order to correctly filter
   * membership events.
   *
   *
   */
  class MyDCReceiver implements MessageListener {

    /**
     * Don't provide events until the caller has told us we are ready.
     *
     * Synchronization provided via GroupMembershipService.class.
     *
     * Note that in practice we only need to delay accepting the first client; we don't need to put
     * this check before every call...
     *
     */
    MyDCReceiver() {

    }

    @Override
    public void messageReceived(DistributionMessage msg) {
      membershipManager.processMessage(msg);

    }

  }

  /** this is a fake message class that is used to flush the serial execution queue */
  static class FlushingMessage extends DistributionMessage {
    final boolean[] done;

    FlushingMessage(boolean[] done) {
      this.done = done;
    }

    public void invoke() {
      synchronized (done) {
        done[0] = true;
        done.notify();
      }
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      // not used
    }

    @Override
    public int getDSFID() {
      return 0;
    }

    @Override
    public int getProcessorType() {
      return OperationExecutors.SERIAL_EXECUTOR;
    }
  }

  public static class LifecycleListenerImpl implements LifecycleListener {
    private MembershipManagerAdapter membershipManagerAdapter;

    public LifecycleListenerImpl(final MembershipManagerAdapter membershipManagerAdapter) {
      this.membershipManagerAdapter = membershipManagerAdapter;
    }

    @Override
    public void start(final MemberIdentifier memberID) {
      membershipManagerAdapter.startDirectChannel(memberID);
    }

    @Override
    public boolean disconnect(Exception exception) {
      return membershipManagerAdapter.disconnectDirectChannel(exception);
    }

    @Override
    public void setLocalAddress(final InternalDistributedMember address) {
      membershipManagerAdapter.setDirectChannelLocalAddress(address);
    }

    @Override
    public void destroyMember(InternalDistributedMember member, String reason) {
      membershipManagerAdapter.destroyMember(member, reason);
    }

  }
}
