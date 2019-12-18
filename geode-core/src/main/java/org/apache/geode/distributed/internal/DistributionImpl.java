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

import static org.apache.geode.distributed.internal.membership.adapter.SocketCreatorAdapter.asTcpSocketCreator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.SystemFailure;
import org.apache.geode.ToDataException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.direct.DirectChannel;
import org.apache.geode.distributed.internal.direct.ShunnedMemberException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.adapter.ServiceConfig;
import org.apache.geode.distributed.internal.membership.adapter.auth.GMSAuthenticator;
import org.apache.geode.distributed.internal.membership.gms.GMSMembership;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.gms.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.api.MemberShunnedException;
import org.apache.geode.distributed.internal.membership.gms.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.gms.api.Membership;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipClosedException;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipView;
import org.apache.geode.distributed.internal.membership.gms.api.Message;
import org.apache.geode.distributed.internal.membership.gms.api.MessageListener;
import org.apache.geode.distributed.internal.membership.gms.api.QuorumChecker;
import org.apache.geode.distributed.internal.membership.gms.fd.GMSHealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.tcp.ConnectExceptions;
import org.apache.geode.internal.tcp.ConnectionException;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;

public class DistributionImpl implements Distribution {
  private static final Logger logger = Services.getLogger();

  @Immutable
  public static final InternalDistributedMember[] EMPTY_MEMBER_ARRAY =
      new InternalDistributedMember[0];
  /**
   * @see SystemFailure#loadEmergencyClasses() /** break any potential circularity in
   *      {@link #loadEmergencyClasses()}
   */
  @MakeNotStatic
  private static volatile boolean emergencyClassesLoaded = false;

  private final ClusterDistributionManager clusterDistributionManager;
  private final boolean tcpDisabled;
  private final boolean mcastEnabled;
  private final long ackSevereAlertThreshold;
  private final long ackWaitThreshold;
  private final RemoteTransportConfig transportConfig;
  private final Membership<InternalDistributedMember> membership;
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
  private boolean disableAutoReconnect;


  public DistributionImpl(final ClusterDistributionManager clusterDistributionManager,
      final RemoteTransportConfig transport, final InternalDistributedSystem system,
      final MembershipListener<InternalDistributedMember> listener,
      final MessageListener<InternalDistributedMember> messageListener) {
    this.clusterDistributionManager = clusterDistributionManager;
    this.transportConfig = transport;
    this.tcpDisabled = transportConfig.isTcpDisabled();
    // cache these settings for use in send()
    mcastEnabled = transportConfig.isMcastEnabled();
    ackSevereAlertThreshold = system.getConfig().getAckSevereAlertThreshold();
    ackWaitThreshold = system.getConfig().getAckWaitThreshold();
    disableAutoReconnect = system.getConfig().getDisableAutoReconnect();
    if (!tcpDisabled) {
      dcReceiver = new MyDCReceiver();
    }

    memberTimeout = system.getConfig().getMemberTimeout();
    try {
      membership = MembershipBuilder.<InternalDistributedMember>newMembershipBuilder()
          .setAuthenticator(
              new GMSAuthenticator(system.getSecurityProperties(), system.getSecurityService(),
                  system.getSecurityLogWriter(), system.getInternalLogWriter()))
          .setStatistics(clusterDistributionManager.stats)
          .setMessageListener(messageListener)
          .setMembershipListener(listener)
          .setConfig(new ServiceConfig(transport, system.getConfig()))
          .setSerializer(InternalDataSerializer.getDSFIDSerializer())
          .setLifecycleListener(new LifecycleListenerImpl(this))
          .setMemberIDFactory(new ClusterDistributionManager.ClusterDistributionManagerIDFactory())
          .setLocatorClient(new TcpClient(
              asTcpSocketCreator(
                  SocketCreatorFactory
                      .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR)),
              InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
              InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer()))
          .create();
    } catch (MembershipConfigurationException e) {
      throw new GemFireConfigException(e.getMessage(), e.getCause());
    } catch (GemFireSecurityException e) {
      throw e;
    } catch (RuntimeException e) {
      Services.getLogger().error("Unexpected problem starting up membership services", e);
      throw new SystemConnectException("Problem starting up membership services", e);
    }
  }

  /**
   * Ensure that the critical classes from components get loaded.
   *
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    if (emergencyClassesLoaded)
      return;
    emergencyClassesLoaded = true;
    DirectChannel.loadEmergencyClasses();
    GMSJoinLeave.loadEmergencyClasses();
    GMSHealthMonitor.loadEmergencyClasses();
  }

  public static void connectLocatorToServices(Membership<InternalDistributedMember> membership) {
    // see if a locator was started and put it in GMS Services
    InternalLocator l = (InternalLocator) Locator.getLocator();
    if (l != null && l.getLocatorHandler() != null) {
      l.getLocatorHandler().setMembership(membership);
    }
  }

  @Override
  public Membership<InternalDistributedMember> getMembership() {
    return membership;
  }

  @Override
  public void start() {
    try {
      membership.start();
    } catch (ConnectionException e) {
      throw new DistributionException(
          "Unable to create membership manager",
          e);
    } catch (SecurityException e) {
      String failReason = e.getMessage();
      if (failReason.contains("Failed to find credentials")) {
        throw new AuthenticationRequiredException(failReason);
      }
      throw new GemFireSecurityException(e.getMessage(),
          e);
    } catch (MembershipConfigurationException e) {
      throw new GemFireConfigException(e.getMessage());
    } catch (MemberStartupException e) {
      throw new SystemConnectException(e.getMessage());
    } catch (RuntimeException e) {
      logger.error("Unexpected problem starting up membership services", e);
      throw new SystemConnectException("Problem starting up membership services: " + e.getMessage()
          + ".  Consult log file for more details");
    }
  }

  @VisibleForTesting
  DistributionImpl(final ClusterDistributionManager clusterDistributionManager,
      final RemoteTransportConfig transport, final InternalDistributedSystem system,
      Membership<InternalDistributedMember> membership) {
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
    this.membership = membership;
  }

  static DistributionImpl createDistribution(
      ClusterDistributionManager clusterDistributionManager, RemoteTransportConfig transport,
      InternalDistributedSystem system,
      MembershipListener<InternalDistributedMember> listener,
      MessageListener<InternalDistributedMember> messageListener) {

    DistributionImpl distribution =
        new DistributionImpl(clusterDistributionManager, transport, system, listener,
            messageListener);
    distribution.start();
    return distribution;
  }


  @Override
  public MembershipView<InternalDistributedMember> getView() {
    return membership.getView();
  }

  @Override
  public InternalDistributedMember getLocalMember() {
    return membership.getLocalMember();
  }

  @Override
  public Set<InternalDistributedMember> send(InternalDistributedMember[] destinations,
      DistributionMessage msg) throws NotSerializableException {
    Set<InternalDistributedMember> result;
    boolean allDestinations = msg.forAll();

    checkCancelled();

    membership.waitIfPlayingDead();

    if (membership.isJoining()) {
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
      return new HashSet<>(Arrays.asList(msg.getRecipientsArray()));
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
      result = membership.send(destinations, msg);
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
   * This method catches membership exceptions that need to be translated into
   * exceptions implementing CancelException in order to satisfy geode-core
   * error handling.
   */
  private void checkCancelled() {
    try {
      membership.checkCancelled();
    } catch (MembershipClosedException e) {
      throw new DistributedSystemDisconnectedException(e.getMessage());
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
  @Override
  public Set<InternalDistributedMember> directChannelSend(
      InternalDistributedMember[] destinations,
      DistributionMessage content)
      throws NotSerializableException {
    MembershipStatistics theStats = clusterDistributionManager.getStats();
    boolean allDestinations;
    InternalDistributedMember[] keys;
    if (content.forAll()) {
      allDestinations = true;
      keys = membership.getAllMembers(EMPTY_MEMBER_ARRAY);
    } else {
      allDestinations = false;
      keys = destinations;
    }

    int sentBytes;
    try {
      sentBytes =
          directChannel.send(membership, keys, content, ackWaitThreshold,
              ackSevereAlertThreshold);

      if (theStats != null) {
        theStats.incSentBytes(sentBytes);
      }

      if (sentBytes == 0) {
        membership.checkCancelled();
      }
    } catch (MembershipClosedException e) {
      throw new DistributedSystemDisconnectedException(e.getMessage(), e.getCause());
    } catch (DistributedSystemDisconnectedException ex) {
      throw ex;
    } catch (ConnectExceptions ex) {
      // Check if the connect exception is due to system shutting down.
      if (membership.shutdownInProgress()) {
        membership.checkCancelled();
        throw new DistributedSystemDisconnectedException();
      }

      if (allDestinations)
        return null;

      // We need to return this list of failures
      List<InternalDistributedMember> members = ex.getMembers();

      // SANITY CHECK: If we fail to send a message to an existing member
      // of the view, we have a serious error (bug36202).


      // Iterate through members and causes in tandem :-(
      Iterator<InternalDistributedMember> it_mem = members.iterator();
      Iterator<Throwable> it_causes = ex.getCauses().iterator();
      while (it_mem.hasNext()) {
        InternalDistributedMember member = it_mem.next();
        Throwable th = it_causes.next();

        if (!membership.hasMember(member) || (th instanceof ShunnedMemberException)) {
          continue;
        }
        logger
            .fatal(String.format("Failed to send message <%s> to member <%s> view, %s",
                // TODO - This used to be services.getJoinLeave().getView(), which is a different
                // view object. Is it ok to log membershipManager.getView here?
                new Object[] {content, member, membership.getView()}),
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


  @Override
  public Map<String, Long> getMessageState(
      DistributedMember member, boolean includeMulticast) {
    final HashMap<String, Long> result = new HashMap<>();
    DirectChannel dc = directChannel;
    if (dc != null) {
      dc.getChannelStates(member, result);
    }
    return membership.getMessageState((InternalDistributedMember) member, includeMulticast, result);
  }

  @Override
  public void waitForMessageState(InternalDistributedMember member,
      Map<String, Long> state) throws InterruptedException, TimeoutException {
    if (Thread.interrupted())
      throw new InterruptedException();
    DirectChannel dc = directChannel;
    if (dc != null) {
      dc.waitForChannelState(member, state);
    }
    membership.waitForMessageState(member, state);


    if (mcastEnabled && !tcpDisabled) {
      // GEODE-2865: wait for scheduled multicast messages to be applied to the cache
      waitForSerialMessageProcessing(member);
    }
  }

  @Override
  public boolean requestMemberRemoval(InternalDistributedMember member, String reason) {
    try {
      return membership.requestMemberRemoval(member, reason);
    } catch (MemberDisconnectedException | MembershipClosedException e) {
      throw new DistributedSystemDisconnectedException("Distribution is closed");
    } catch (RuntimeException e) {
      if (!membership.isConnected()) {
        throw new DistributedSystemDisconnectedException("Distribution is closed", e);
      }
      throw e;
    }
  }

  @Override
  public boolean verifyMember(InternalDistributedMember mbr, String reason) {
    return membership.verifyMember(mbr, reason);
  }

  @Override
  public <V> V doWithViewLocked(Supplier<V> function) {
    return membership.doWithViewLocked(function);
  }

  @Override
  public boolean memberExists(InternalDistributedMember m) {
    return membership.memberExists(m);
  }

  @Override
  public boolean isConnected() {
    return membership.isConnected();
  }

  @Override
  public void beSick() {
    membership.beSick();
  }

  @Override
  public void beHealthy() {
    membership.beHealthy();
  }

  @Override
  public void playDead() {
    membership.playDead();
  }

  @Override
  public boolean isBeingSick() {
    return membership.isBeingSick();
  }

  @Override
  public void disconnect(boolean beforeJoined) {
    membership.disconnect(beforeJoined);
  }

  @Override
  public void shutdown() {
    membership.shutdown();
  }

  @Override
  public void shutdownMessageReceived(InternalDistributedMember id,
      String reason) {
    membership.shutdownMessageReceived(id, reason);
  }

  @Override
  public void waitForEventProcessing() throws InterruptedException {
    membership.waitForEventProcessing();
  }

  @Override
  public void startEventProcessing() {
    membership.startEventProcessing();
  }

  @Override
  public void setShutdown() {
    membership.setShutdown();
  }

  @Override
  public void setReconnectCompleted(boolean reconnectCompleted) {
    membership.setReconnectCompleted(reconnectCompleted);
  }

  @Override
  public boolean shutdownInProgress() {
    return membership.shutdownInProgress();
  }

  @Override
  public void emergencyClose() {
    membership.emergencyClose();
    if (directChannel != null) {
      directChannel.emergencyClose();
    }
  }

  @Override
  public void addSurpriseMemberForTesting(InternalDistributedMember mbr,
      long birthTime) {
    membership.addSurpriseMemberForTesting(mbr, birthTime);
  }

  @Override
  public void suspectMembers(Set<InternalDistributedMember> members,
      String reason) {
    membership.suspectMembers(members, reason);
  }

  @Override
  public void suspectMember(InternalDistributedMember member,
      String reason) {
    membership.suspectMember(member, reason);
  }

  @Override
  public Throwable getShutdownCause() {
    return membership.getShutdownCause();
  }

  @Override
  public boolean addSurpriseMember(InternalDistributedMember mbr) {
    return membership.addSurpriseMember(mbr);
  }

  @Override
  public void startupMessageFailed(InternalDistributedMember mbr,
      String failureMessage) {
    membership.startupMessageFailed(mbr, failureMessage);
  }

  @Override
  public boolean testMulticast() {
    return membership.testMulticast();
  }

  @Override
  public boolean isSurpriseMember(InternalDistributedMember m) {
    return membership.isSurpriseMember(m);
  }

  @Override
  public QuorumChecker getQuorumChecker() {
    return membership.getQuorumChecker();
  }

  public DistributedMember getCoordinator() {
    return membership.getCoordinator();
  }

  @Override
  public Set<InternalDistributedMember> getMembersNotShuttingDown() {
    return membership.getMembersNotShuttingDown();
  }

  // TODO - this method is only used by tests
  @Override
  @VisibleForTesting
  public void forceDisconnect(String reason) {
    ((GMSMembership) membership).getGMSManager().forceDisconnect(reason);
  }

  // TODO - this method is only used by tests
  @Override
  @VisibleForTesting
  public void replacePartialIdentifierInMessage(DistributionMessage message) {
    ((GMSMembership) membership).replacePartialIdentifierInMessage(message);

  }

  // TODO - this method is only used by tests
  @Override
  @VisibleForTesting
  public boolean isCleanupTimerStarted() {
    return ((GMSMembership) membership).isCleanupTimerStarted();
  }

  // TODO - this method is only used by tests
  @Override
  @VisibleForTesting
  public long getSurpriseMemberTimeout() {
    return ((GMSMembership) membership).getSurpriseMemberTimeout();
  }

  // TODO - this method is only used by tests
  @Override
  @VisibleForTesting
  public void installView(GMSMembershipView newView) {
    ((GMSMembership) membership).getGMSManager().installView(newView);
  }

  // TODO - this method is only used by tests
  @Override
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
  }

  @Override
  public void disableDisconnectOnQuorumLossForTesting() {
    ((GMSMembership) membership).disableDisconnectOnQuorumLossForTesting();
  }

  private void startDirectChannel(final MemberIdentifier memberID) {
    int dcPort = 0;

    if (!tcpDisabled) {
      directChannel = new DirectChannel(membership, dcReceiver, clusterDistributionManager);
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
  public void forceUDPMessagingForCurrentThread() {
    forceUseUDPMessaging.set(Boolean.TRUE);
  }

  /**
   * Releases use of UDP for all communications in the current thread, as established by
   * forceUDPMessagingForCurrentThread.
   */
  public void releaseUDPMessagingForCurrentThread() {
    forceUseUDPMessaging.set(Boolean.FALSE);
  }

  @Override
  public void setCloseInProgress() {
    membership.setCloseInProgress();
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
  @Override
  public boolean waitForDeparture(InternalDistributedMember mbr)
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
  @Override
  public boolean waitForDeparture(InternalDistributedMember mbr, long timeoutMs)
      throws TimeoutException, InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    boolean result = false;
    // TODO - Move the bulk of this method to the adapter.
    DirectChannel dc = directChannel;
    InternalDistributedMember idm = mbr;
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
  class MyDCReceiver implements MessageListener<InternalDistributedMember> {

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
    public void messageReceived(Message<InternalDistributedMember> msg)
        throws MemberShunnedException {
      membership.processMessage(msg);

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
    public int getDSFID() {
      return 0;
    }

    @Override
    public int getProcessorType() {
      return 0;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {

    }

    @Override
    public void toData(DataOutput out, SerializationContext context) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void fromData(DataInput in, DeserializationContext context)
        throws IOException, ClassNotFoundException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }
  }

  private static class LifecycleListenerImpl
      implements LifecycleListener<InternalDistributedMember> {
    private DistributionImpl distribution;

    LifecycleListenerImpl(final DistributionImpl distribution) {
      this.distribution = distribution;
    }

    @Override
    public void start(final InternalDistributedMember memberID) {
      distribution.startDirectChannel(memberID);
    }

    @Override
    public boolean disconnect(Exception cause) {
      Exception exception = cause;
      // translate into a ForcedDisconnectException if necessary
      if (cause instanceof MemberDisconnectedException) {
        exception = new ForcedDisconnectException(cause.getMessage());
        if (cause.getCause() != null) {
          exception.initCause(cause.getCause());
        }
      }
      return distribution.disconnectDirectChannel(exception);
    }

    @Override
    public void setLocalAddress(final InternalDistributedMember address) {
      distribution.setDirectChannelLocalAddress(address);
    }

    @Override
    public void destroyMember(InternalDistributedMember member, String reason) {
      distribution.destroyMember(member, reason);
    }

    @Override
    public void started() {
      connectLocatorToServices(distribution.getMembership());
    }

    @Override
    public void forcedDisconnect() {
      // stop server locators immediately since they may not have correct
      // information. This has caused client failures in bridge/wan
      // network-down testing
      InternalLocator loc = (InternalLocator) Locator.getLocator();
      if (loc != null) {
        loc.stop(true, !distribution.disableAutoReconnect, false);
      }
    }
  }
}
