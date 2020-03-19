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
package org.apache.geode.distributed.internal.membership.gms.membership;

import static org.apache.geode.distributed.internal.membership.api.MembershipConfig.MEMBER_REQUEST_COLLECTION_INTERVAL;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.JOIN_REQUEST;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.LEAVE_REQUEST_MESSAGE;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.REMOVE_MEMBER_REQUEST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.MembershipClosedException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.GMSUtil;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.messages.AbstractGMSMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.HasMemberID;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.NetworkPartitionMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.ViewAckMessage;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * GMSJoinLeave is responsible for finding the membership coordinator and joining the
 * cluster. It is also responsible for informing other members of the cluster when
 * Membership is shutting down.
 * <p>
 * One member of the cluster must also take on the role of coordinator. This is typically
 * the oldest member of the cluster. The coordinator is responsible for sending out new
 * membership "views" that announce cluster membership.
 */
public class GMSJoinLeave<ID extends MemberIdentifier> implements JoinLeave<ID> {

  public static final String BYPASS_DISCOVERY_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "bypass-discovery";

  /**
   * amount of time to wait for responses to FindCoordinatorRequests
   */
  private static final int DISCOVERY_TIMEOUT =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "discovery-timeout", 3000);

  /**
   * amount of time to sleep before trying to join after a failed attempt
   */
  private static final int JOIN_RETRY_SLEEP =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "join-retry-sleep", 1000);

  /**
   * time to wait for a broadcast message to be transmitted by jgroups
   */
  private static final long BROADCAST_MESSAGE_SLEEP_TIME =
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "broadcast-message-sleep-time", 1000);

  /**
   * if the locators don't know who the coordinator is we send find-coord requests to this many
   * nodes
   */
  private static final int MAX_DISCOVERY_NODES =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "max-discovery-nodes", 30);

  /**
   * interval for broadcasting the current view to members in case they didn't get it the first time
   */
  private static final long VIEW_BROADCAST_INTERVAL =
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "view-broadcast-interval", 60000);

  /**
   * membership logger
   */
  private static final Logger logger = Services.getLogger();
  private static final boolean ALLOW_OLD_VERSION_FOR_TESTING = Boolean
      .getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "allow_old_members_to_join_for_testing");

  /**
   * the view ID where I entered into membership
   */
  private int birthViewId;

  /**
   * my address
   */
  private ID localAddress;

  private Services<ID> services;

  /**
   * have I connected to the distributed system?
   */
  private volatile boolean isJoined;

  /**
   * guarded by viewInstallationLock
   */
  @VisibleForTesting
  volatile boolean isCoordinator;

  /**
   * a synch object that guards view installation
   */
  private final Object viewInstallationLock = new Object();

  /**
   * the currently installed view. Guarded by viewInstallationLock
   */
  @VisibleForTesting
  volatile GMSMembershipView<ID> currentView;

  /**
   * the previous view
   **/
  private volatile GMSMembershipView<ID> previousView;

  /**
   * members who we have been declared dead in the current view
   */
  private final Set<ID> removedMembers = new HashSet<>();

  /**
   * members who we've received a leave message from
   **/
  private final Set<ID> leftMembers = new HashSet<>();

  /**
   * a new view being installed
   */
  private volatile GMSMembershipView<ID> preparedView;

  /**
   * the last view that conflicted with view preparation
   */
  private GMSMembershipView<ID> lastConflictingView;

  private List<HostAndPort> locators;

  /**
   * a list of join/leave/crashes
   */
  private final List<AbstractGMSMessage<ID>> viewRequests = new LinkedList<>();

  /**
   * the established request collection jitter. This can be overridden for testing with
   * delayViewCreationForTest
   */
  long requestCollectionInterval = MEMBER_REQUEST_COLLECTION_INTERVAL;

  /**
   * collects the response to a join request
   */
  private final JoinResponseMessage<ID>[] joinResponse = new JoinResponseMessage[1];

  /**
   * collects responses to new views
   */
  ViewReplyProcessor viewProcessor = new ViewReplyProcessor(false);

  /**
   * collects responses to view preparation messages
   */
  ViewReplyProcessor prepareProcessor = new ViewReplyProcessor(true);

  /**
   * whether quorum checks can cause a forced-disconnect
   */
  private boolean quorumRequired = false;

  /**
   * timeout in receiving view acknowledgement
   */
  private long viewAckTimeout;

  /**
   * background thread that creates new membership views
   */
  private ViewCreator viewCreator;

  /**
   * am I shutting down?
   */
  private volatile boolean isStopping;

  /**
   * state of collected artifacts during discovery
   */
  final SearchState<ID> searchState = new SearchState<>();

  /**
   * a collection used to detect unit testing
   */
  Set<String> unitTesting = new HashSet<>();

  /**
   * a test hook to make this member unresponsive
   */
  private volatile boolean playingDead;

  /**
   * the view where quorum was most recently lost
   */
  GMSMembershipView<ID> quorumLostView;

  /**
   * for messaging locator
   */
  private final TcpClient locatorClient;

  public GMSJoinLeave(final TcpClient locatorClient) {
    this.locatorClient = locatorClient;
  }

  static class SearchState<ID extends MemberIdentifier> {
    public int joinedMembersContacted;
    Set<ID> alreadyTried = new HashSet<>();
    Set<ID> registrants = new HashSet<>();
    ID possibleCoordinator;
    int viewId = -100;
    int locatorsContacted = 0;
    boolean hasContactedAJoinedLocator;
    GMSMembershipView<ID> view;
    int lastFindCoordinatorInViewId = -1000;
    final Set<FindCoordinatorResponse<ID>> responses = new HashSet<>();
    public int responsesExpected;

    void cleanup() {
      alreadyTried.clear();
      possibleCoordinator = null;
      view = null;
      synchronized (responses) {
        responses.clear();
      }
    }

    public String toString() {
      StringBuffer sb = new StringBuffer(200);
      sb.append("locatorsContacted=").append(locatorsContacted)
          .append("; findInViewResponses=").append(joinedMembersContacted)
          .append("; alreadyTried=").append(alreadyTried).append("; registrants=")
          .append(registrants).append("; possibleCoordinator=").append(possibleCoordinator)
          .append("; viewId=").append(viewId).append("; hasContactedAJoinedLocator=")
          .append(hasContactedAJoinedLocator).append("; view=").append(view).append("; responses=")
          .append(responses);
      return sb.toString();
    }
  }

  Object getViewInstallationLock() {
    return viewInstallationLock;
  }

  /**
   * attempt to join the distributed system loop send a join request to a locator & get a response
   * <p>
   * If the response indicates there's no coordinator it will contain a set of members that have
   * recently contacted it. The "oldest" member is selected as the coordinator based on ID sort
   * order.
   *
   * @return true if successful, false if not
   */
  @Override
  public boolean join() throws MemberStartupException {

    try {
      if (Boolean.getBoolean(BYPASS_DISCOVERY_PROPERTY)) {
        synchronized (viewInstallationLock) {
          becomeCoordinator();
        }
        return true;
      }

      SearchState<ID> state = searchState;

      long locatorWaitTime = ((long) services.getConfig().getLocatorWaitTime()) * 1000L;
      long timeout = services.getConfig().getJoinTimeout();
      logger.debug("join timeout is set to {}", timeout);
      long retrySleep = JOIN_RETRY_SLEEP;
      long startTime = System.currentTimeMillis();
      long locatorGiveUpTime = startTime + locatorWaitTime;
      long giveupTime = startTime + timeout;
      int minimumRetriesBeforeBecomingCoordinator = locators.size() * 2;

      for (int tries = 0; !this.isJoined && !this.isStopping; tries++) {
        logger.debug("searching for the membership coordinator");
        boolean found = findCoordinator();
        logger.info("Discovery state after looking for membership coordinator is {}",
            state);
        if (found) {
          logger.info("found possible coordinator {}", state.possibleCoordinator);
          if (localAddress.preferredForCoordinator()
              && state.possibleCoordinator.equals(this.localAddress)) {
            // if we haven't contacted a member of a cluster maybe this node should
            // become the coordinator.
            if (state.joinedMembersContacted <= 0 &&
                (tries >= minimumRetriesBeforeBecomingCoordinator ||
                    state.locatorsContacted >= locators.size())) {
              synchronized (viewInstallationLock) {
                becomeCoordinator();
              }
              return true;
            }
          } else {
            if (attemptToJoin()) {
              return true;
            }
            if (this.isStopping) {
              break;
            }
            if (!state.possibleCoordinator.equals(localAddress)) {
              state.alreadyTried.add(state.possibleCoordinator);
            }
            if (System.currentTimeMillis() > giveupTime) {
              break;
            }
          }
        } else {
          long now = System.currentTimeMillis();
          if (state.locatorsContacted <= 0) {
            if (now > locatorGiveUpTime) {
              // break out of the loop and return false
              break;
            }
            tries = 0;
            giveupTime = now + timeout;
          } else if (now > giveupTime) {
            break;
          }
        }
        try {
          if (found && !state.hasContactedAJoinedLocator) {
            // if locators are restarting they may be handing out IDs from a stale view that
            // we should go through quickly. Otherwise we should sleep a bit to let failure
            // detection select a new coordinator
            if (state.possibleCoordinator.getVmViewId() < 0) {
              logger.debug("sleeping for {} before making another attempt to find the coordinator",
                  retrySleep);
              Thread.sleep(retrySleep);
            } else {
              // since we were given a coordinator that couldn't be used we should keep trying
              tries = 0;
              giveupTime = System.currentTimeMillis() + timeout;
            }
          }
        } catch (InterruptedException e) {
          logger.debug("retry sleep interrupted - giving up on joining the distributed system");
          return false;
        }
      } // for

      if (!this.isJoined) {
        logger.debug("giving up attempting to join the distributed system after "
            + (System.currentTimeMillis() - startTime) + "ms");
      }

      // to preserve old behavior we need to throw a MemberStartupException if
      // unable to contact any of the locators
      if (!this.isJoined && state.hasContactedAJoinedLocator) {
        throw new MemberStartupException("Unable to join the distributed system in "
            + (System.currentTimeMillis() - startTime) + "ms");
      }

      return this.isJoined;
    } finally {
      // notify anyone waiting on the address to be completed
      if (this.isJoined) {
        synchronized (this.localAddress) {
          this.localAddress.notifyAll();
        }
      }
      searchState.cleanup();
    }
  }

  /**
   * send a join request and wait for a reply. Process the reply. This may throw a
   * MemberStartupException or an exception from the authenticator, if present.
   *
   * @return true if the attempt succeeded, false if it timed out
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WA_NOT_IN_LOOP")
  boolean attemptToJoin() throws MemberStartupException {
    SearchState<ID> state = searchState;

    // send a join request to the coordinator and wait for a response
    ID coord = state.possibleCoordinator;
    if (state.alreadyTried.contains(coord)) {
      logger.info("Probable coordinator is still {} - waiting for a join-response", coord);
    } else {
      logger.info("Attempting to join the distributed system through coordinator " + coord
          + " using address " + this.localAddress);
      int port = services.getHealthMonitor().getFailureDetectionPort();
      JoinRequestMessage<ID> req = new JoinRequestMessage<>(coord, this.localAddress,
          services.getAuthenticator().getCredentials(coord), port,
          services.getMessenger().getRequestId());
      services.getMessenger().send(req);
    }

    JoinResponseMessage<ID> response;
    try {
      response = waitForJoinResponse();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }

    if (response == null) {
      if (!isJoined) {
        logger.debug("received no join response");
      }
      return isJoined;
    }

    logger.info("received join response {}", response);
    joinResponse[0] = null;
    String failReason = response.getRejectionMessage();
    if (failReason != null) {
      if (failReason.contains("Rejecting the attempt of a member using an older version")
          || failReason.contains("15806")
          || failReason.contains("ForcedDisconnectException")) {
        throw new MemberStartupException(failReason);
      }
      throw new SecurityException(failReason);
    }

    throw new RuntimeException("Join Request Failed with response " + response);
  }

  private JoinResponseMessage<ID> waitForJoinResponse() throws InterruptedException {
    JoinResponseMessage<ID> response;
    synchronized (joinResponse) {
      if (joinResponse[0] == null && !isJoined) {
        // Note that if we give up waiting but a response is on
        // the way we will get the new view and join that way.
        // See installView()
        long timeout = Math.max(services.getConfig().getMemberTimeout(),
            services.getConfig().getJoinTimeout() / 5);
        joinResponse.wait(timeout);
      }
      response = joinResponse[0];

      if (services.getConfig().getSecurityUDPDHAlgo().length() > 0) {
        if (response != null && response.getCurrentView() != null && !isJoined) {
          // reset joinResponse[0]
          joinResponse[0] = null;
          // we got view here that means either we have to wait for
          GMSMembershipView<ID> v = response.getCurrentView();
          ID coord = v.getCoordinator();
          if (searchState.alreadyTried.contains(coord)) {
            searchState.view = response.getCurrentView();
            // we already sent join request to it..so lets wait some more time here
            // assuming we got this response immediately, so wait for same timeout here..
            long timeout = Math.max(services.getConfig().getMemberTimeout(),
                services.getConfig().getJoinTimeout() / 5);
            joinResponse.wait(timeout);
            response = joinResponse[0];
          } else {
            // try on this coordinator
            searchState.view = response.getCurrentView();
            response = null;
          }
          searchState.view = v;
        }
        if (isJoined) {
          return null;
        }
      }
    }
    return response;
  }

  @Override
  public boolean isMemberLeaving(ID mbr) {
    if (getPendingRequestIDs(LEAVE_REQUEST_MESSAGE).contains(mbr)
        || getPendingRequestIDs(REMOVE_MEMBER_REQUEST).contains(mbr)
        || !currentView.contains(mbr)) {
      return true;
    }
    synchronized (removedMembers) {
      if (removedMembers.contains(mbr)) {
        return true;
      }
    }
    synchronized (leftMembers) {
      if (leftMembers.contains(mbr)) {
        return true;
      }
    }
    return false;
  }

  /**
   * process a join request from another member. If this is the coordinator this method will enqueue
   * the request for processing in another thread. If this is not the coordinator but the
   * coordinator is known, the message is forwarded to the coordinator.
   *
   * @param incomingRequest the request to be processed
   */
  void processMessage(JoinRequestMessage<ID> incomingRequest) {
    if (isStopping) {
      return;
    }

    logger.info("Received a join request from {}", incomingRequest.getMemberID());

    if (!ALLOW_OLD_VERSION_FOR_TESTING
        && incomingRequest.getMemberID().getVersionOrdinal() < Version.getCurrentVersion()
            .ordinal()) {
      logger.warn("detected an attempt to start a peer using an older version of the product {}",
          incomingRequest.getMemberID());
      JoinResponseMessage<ID> m =
          new JoinResponseMessage<>(
              "Rejecting the attempt of a member using an older version of the "
                  + "product to join the distributed system",
              incomingRequest.getRequestId());
      m.setRecipient(incomingRequest.getMemberID());
      services.getMessenger().send(m);
      return;
    }

    Object creds = incomingRequest.getCredentials();
    String rejection;
    try {
      rejection = services.getAuthenticator().authenticate(incomingRequest.getMemberID(),
          (Properties) creds);
    } catch (Exception e) {
      rejection = e.getMessage();
    }
    if (rejection != null && rejection.length() > 0) {
      JoinResponseMessage<ID> m = new JoinResponseMessage<>(rejection, 0);
      m.setRecipient(incomingRequest.getMemberID());
      services.getMessenger().send(m);
      return;
    }

    recordViewRequest(incomingRequest);
  }

  /**
   * Process a Leave request from another member. This may cause this member to become the new
   * membership coordinator. If this is the coordinator a new view will be triggered.
   *
   * @param incomingRequest the request to be processed
   */
  void processMessage(LeaveRequestMessage<ID> incomingRequest) {
    if (isStopping) {
      return;
    }

    logger.info("received leave request from {} for {}", incomingRequest.getSender(),
        incomingRequest.getMemberID());

    GMSMembershipView<ID> v = currentView;
    if (v == null) {
      recordViewRequest(incomingRequest);
      return;
    }

    ID mbr = incomingRequest.getMemberID();

    logger.info(() -> "JoinLeave.processMessage(LeaveRequestMessage) invoked.  isCoordinator="
        + isCoordinator
        + "; isStopping=" + isStopping + "; cancelInProgress="
        + services.getCancelCriterion().isCancelInProgress());

    if (!v.contains(mbr) && mbr.getVmViewId() < v.getViewId()) {
      logger.info("ignoring leave request from old member");
      return;
    }

    if (incomingRequest.getMemberID().equals(this.localAddress)) {
      logger.info("I am being told to leave the distributed system by {}",
          incomingRequest.getSender());
      forceDisconnect(incomingRequest.getReason());
      return;
    }

    if (!isCoordinator && !isStopping && !services.getCancelCriterion().isCancelInProgress()) {
      logger.info("Checking to see if I should become coordinator.  My address is {}",
          localAddress);
      GMSMembershipView<ID> check = new GMSMembershipView<>(v, v.getViewId() + 1);
      check.remove(mbr);
      synchronized (removedMembers) {
        check.removeAll(removedMembers);
        check.addCrashedMembers(removedMembers);
      }
      synchronized (leftMembers) {
        leftMembers.add(mbr);
        check.removeAll(leftMembers);
      }
      ID coordinator = check.getCoordinator();
      logger.info("View with removed and left members removed is {} and coordinator would be {}",
          check, coordinator);
      if (coordinator.equals(localAddress)) {
        synchronized (viewInstallationLock) {
          becomeCoordinator(mbr);
        }
      }
    } else {
      if (!isStopping && !services.getCancelCriterion().isCancelInProgress()) {
        recordViewRequest(incomingRequest);
        this.viewProcessor.processLeaveRequest(incomingRequest.getMemberID());
        this.prepareProcessor.processLeaveRequest(incomingRequest.getMemberID());
      }
    }
  }

  /**
   * Process a Remove request from another member. This may cause this member to become the new
   * membership coordinator. If this is the coordinator a new view will be triggered.
   *
   * @param incomingRequest the request to process
   */
  void processMessage(RemoveMemberMessage<ID> incomingRequest) {
    if (isStopping) {
      return;
    }

    GMSMembershipView<ID> v = currentView;
    boolean fromMe =
        incomingRequest.getSender() == null || incomingRequest.getSender().equals(localAddress);

    ID mbr = incomingRequest.getMemberID();

    if (v != null && !v.contains(incomingRequest.getSender())) {
      logger.info("Membership ignoring removal request for " + mbr + " from non-member "
          + incomingRequest.getSender());
      return;
    }

    if (v == null) {
      // not yet a member
      return;
    }

    if (!fromMe) {
      logger.info("Membership received a request to remove " + mbr + " from "
          + incomingRequest.getSender() + " reason=" + incomingRequest.getReason());
    }

    if (mbr.equals(this.localAddress)) {
      // oops - I've been kicked out
      forceDisconnect(incomingRequest.getReason());
      return;
    }

    if (getPendingRequestIDs(REMOVE_MEMBER_REQUEST).contains(mbr)) {
      logger.debug("ignoring removal request as I already have a removal request for this member");
      return;
    }

    if (!isCoordinator && !isStopping && !services.getCancelCriterion().isCancelInProgress()) {
      logger.debug("Checking to see if I should become coordinator");
      GMSMembershipView<ID> check = new GMSMembershipView<>(v, v.getViewId() + 1);
      synchronized (removedMembers) {
        removedMembers.add(mbr);
        check.addCrashedMembers(removedMembers);
        check.removeAll(removedMembers);
      }
      synchronized (leftMembers) {
        check.removeAll(leftMembers);
      }
      if (check.getCoordinator().equals(localAddress)) {
        synchronized (viewInstallationLock) {
          becomeCoordinator(mbr);
        }
      }
    } else {
      if (!isStopping && !services.getCancelCriterion().isCancelInProgress()) {
        // suspect processing tends to get carried away sometimes during
        // shutdown (especially shutdownAll), so we check for a scheduled shutdown
        // message
        if (!getPendingRequestIDs(LEAVE_REQUEST_MESSAGE).contains(mbr)) {
          recordViewRequest(incomingRequest);
          this.viewProcessor.processRemoveRequest(mbr);
          this.prepareProcessor.processRemoveRequest(mbr);
        }
      }
      if (isCoordinator) {
        if (!v.contains(mbr)) {
          // removing a rogue process
          RemoveMemberMessage<ID> removeMemberMessage = new RemoveMemberMessage<>(mbr, mbr,
              incomingRequest.getReason());
          services.getMessenger().send(removeMemberMessage);
        }
      }
    }
  }

  @VisibleForTesting
  void recordViewRequest(AbstractGMSMessage<ID> request) {
    try {
      synchronized (viewRequests) {
        logger.debug("Recording the request to be processed in the next membership view");
        viewRequests.add(request);
        viewRequests.notifyAll();
      }
    } catch (RuntimeException | Error t) {
      logger.warn("unable to record a membership view request due to this exception", t);
      throw t;
    }
  }

  // for testing purposes, returns a copy of the view requests for verification
  List<AbstractGMSMessage<ID>> getViewRequests() {
    synchronized (viewRequests) {
      return new LinkedList<>(viewRequests);
    }
  }

  // for testing purposes, returns the view-creation thread
  ViewCreator getViewCreator() {
    return viewCreator;
  }

  /**
   * Yippeee - I get to be the coordinator
   */
  void becomeCoordinator() { // package access for unit testing
    becomeCoordinator(null);
  }

  /**
   * Test hook for delaying the creation of new views. This should be invoked before this member
   * becomes coordinator and creates its ViewCreator thread.
   *
   */
  public void delayViewCreationForTest(int millis) {
    requestCollectionInterval = millis;
  }

  /**
   * Transitions this member into the coordinator role. This must be invoked under a synch on
   * viewInstallationLock that was held at the time the decision was made to become coordinator so
   * that the decision is atomic with actually becoming coordinator.
   *
   * @param oldCoordinator may be null
   */
  private void becomeCoordinator(ID oldCoordinator) {

    assert Thread.holdsLock(viewInstallationLock);

    if (isCoordinator) {
      return;
    }

    logger.info("This member is becoming the membership coordinator with address {}", localAddress);
    isCoordinator = true;
    org.apache.geode.distributed.internal.membership.gms.interfaces.Locator<ID> locator =
        services.getLocator();
    if (locator != null) {
      locator.setIsCoordinator(true);
    }
    if (currentView == null) {
      // create the initial membership view
      GMSMembershipView<ID> newView = new GMSMembershipView<>(this.localAddress);
      newView.setFailureDetectionPort(localAddress,
          services.getHealthMonitor().getFailureDetectionPort());
      this.localAddress.setVmViewId(0);
      installView(newView);
      isJoined = true;
      createAndStartViewCreator(newView);
      startViewBroadcaster();
    } else {
      // create and send out a new view
      GMSMembershipView<ID> newView = copyCurrentViewAndAddMyAddress(oldCoordinator);
      createAndStartViewCreator(newView);
      startViewBroadcaster();
    }
  }

  private void createAndStartViewCreator(GMSMembershipView<ID> newView) {
    if (viewCreator == null || viewCreator.isShutdown()) {
      services.getMessenger().initClusterKey();
      viewCreator = new ViewCreator("Geode Membership View Creator");
      if (newView != null) {
        viewCreator.setInitialView(newView, newView.getNewMembers(),
            newView.getShutdownMembers(),
            newView.getCrashedMembers());
      }
      logger.info("ViewCreator starting on:" + localAddress);
      viewCreator.start();
    }
  }

  private GMSMembershipView<ID> copyCurrentViewAndAddMyAddress(ID oldCoordinator) {
    boolean testing = unitTesting.contains("noRandomViewChange");
    GMSMembershipView<ID> newView;
    Set<ID> leaving = new HashSet<>();
    Set<ID> removals;
    synchronized (viewInstallationLock) {
      int rand = testing ? 0 : GMSMembershipView.RANDOM.nextInt(10);
      int viewNumber = currentView.getViewId() + 5 + rand;
      if (this.localAddress.getVmViewId() < 0) {
        this.localAddress.setVmViewId(viewNumber);
      }
      List<ID> mbrs = new ArrayList<>(currentView.getMembers());
      if (!mbrs.contains(localAddress)) {
        mbrs.add(localAddress);
      }
      synchronized (this.removedMembers) {
        removals = new HashSet<>(this.removedMembers);
      }
      synchronized (this.leftMembers) {
        leaving.addAll(leftMembers);
      }
      if (oldCoordinator != null && !removals.contains(oldCoordinator)) {
        leaving.add(oldCoordinator);
      }
      mbrs.removeAll(removals);
      mbrs.removeAll(leaving);
      newView = new GMSMembershipView<>(this.localAddress, viewNumber, mbrs, leaving, removals);
      newView.setFailureDetectionPorts(currentView);
      newView.setPublicKeys(currentView);
      newView.setFailureDetectionPort(this.localAddress,
          services.getHealthMonitor().getFailureDetectionPort());
    }
    return newView;
  }

  private void sendRemoveMessages(List<ID> removals, List<String> reasons,
      Set<ID> oldIds) {
    Iterator<String> reason = reasons.iterator();
    for (ID mbr : removals) {
      // if olds not contains mbr then send remove request
      if (!oldIds.contains(mbr)) {
        RemoveMemberMessage<ID> response = new RemoveMemberMessage<>(mbr, mbr, reason.next());
        services.getMessenger().send(response);
      } else {
        reason.next();
      }
    }
  }

  boolean isShuttingDown() {
    return services.getCancelCriterion().isCancelInProgress()
        || services.getManager().shutdownInProgress()
        || services.getManager().isCloseInProgress();
  }

  boolean prepareView(GMSMembershipView<ID> view, List<ID> newMembers)
      throws InterruptedException {
    // GEODE-2193 - don't send a view with new members if we're shutting down
    if (isShuttingDown()) {
      throw new InterruptedException("shutting down");
    }
    return sendView(view, true, this.prepareProcessor);
  }

  void sendView(GMSMembershipView<ID> view, List<ID> newMembers)
      throws InterruptedException {
    if (isShuttingDown()) {
      throw new InterruptedException("shutting down");
    }
    sendView(view, false, this.viewProcessor);
  }

  private boolean sendView(GMSMembershipView<ID> view, boolean preparing,
      ViewReplyProcessor viewReplyProcessor)
      throws InterruptedException {

    int id = view.getViewId();
    InstallViewMessage<ID> msg = new InstallViewMessage<>(view,
        services.getAuthenticator().getCredentials(this.localAddress), preparing);
    List<ID> recips = new ArrayList<>(view.getMembers());

    // a recent member was seen not to receive a new view - I think this is why
    // recips.removeAll(newMembers); // new members get the view in a JoinResponseMessage
    recips.remove(this.localAddress); // no need to send it to ourselves

    List<ID> responders = recips;
    if (!view.getCrashedMembers().isEmpty()) {
      recips = new ArrayList<>(recips);
      recips.addAll(view.getCrashedMembers());
    }

    if (preparing) {
      this.preparedView = view;
    } else {
      // Added a check in the view processor to turn off the ViewCreator
      // if another server is the coordinator - GEODE-870
      ViewCreator thread = this.viewCreator;
      if (isCoordinator && !localAddress.equals(view.getCoordinator())
          && !localAddress.equals(view.getCreator())
          && thread != null) {
        thread.markViewCreatorForShutdown(view.getCoordinator());
        this.isCoordinator = false;
      }
      installView(new GMSMembershipView<>(view, view.getViewId()));
    }

    if (recips.isEmpty()) {
      if (!preparing) {
        logger.info("no recipients for new view aside from myself");
      }
      return true;
    }

    logger.info((preparing ? "preparing" : "sending") + " new view " + view);

    msg.setRecipients(recips);

    Set<ID> pendingLeaves = getPendingRequestIDs(LEAVE_REQUEST_MESSAGE);
    Set<ID> pendingRemovals = getPendingRequestIDs(REMOVE_MEMBER_REQUEST);
    pendingRemovals.removeAll(view.getCrashedMembers());
    viewReplyProcessor.initialize(id, new HashSet<>(responders));
    viewReplyProcessor.processPendingRequests(pendingLeaves, pendingRemovals);
    addPublicKeysToView(view);
    services.getMessenger().send(msg, view);

    // only wait for responses during preparation
    if (preparing) {
      logger.debug("waiting for view responses");

      Set<ID> failedToRespond = viewReplyProcessor.waitForResponses();

      logger.info("finished waiting for responses to view preparation");

      ID conflictingViewSender =
          viewReplyProcessor.getConflictingViewSender();
      GMSMembershipView<ID> conflictingView = viewReplyProcessor.getConflictingView();
      if (conflictingView != null) {
        logger.warn("received a conflicting membership view from " + conflictingViewSender
            + " during preparation: " + conflictingView);
        return false;
      }

      if (!failedToRespond.isEmpty() && (!services.getCancelCriterion().isCancelInProgress())) {
        logger.warn("these members failed to respond to the view change: " + failedToRespond);
        return false;
      }
    }

    return true;
  }

  private void addPublicKeysToView(GMSMembershipView<ID> view) {
    String sDHAlgo = services.getConfig().getSecurityUDPDHAlgo();
    if (sDHAlgo != null && !sDHAlgo.isEmpty()) {
      List<ID> members = view.getMembers();
      for (ID mbr : members) {
        if (Objects.isNull(view.getPublicKey(mbr))) {
          byte[] pk = services.getMessenger().getPublicKey(mbr);
          view.setPublicKey(mbr, pk);
        }
      }
    }
  }

  void processMessage(final InstallViewMessage<ID> m) {
    if (isStopping) {
      return;
    }

    logger.debug("processing membership view message {}", m);

    GMSMembershipView<ID> view = m.getView();

    // If our current view doesn't contain sender then we wanrt to ignore that view.
    if (currentView != null && !currentView.contains(m.getSender())) {
      // but if preparedView contains sender then we don't want to ignore that view.
      // this may happen when we locator re-join and it take over coordinator's responsibility.
      if (this.preparedView == null || !this.preparedView.contains(m.getSender())) {
        logger.info("Ignoring the view {} from member {}, which is not in my current view {} ",
            view, m.getSender(), currentView);
        return;
      }
    }

    if (currentView != null && view.getViewId() < currentView.getViewId()) {
      // ignore old views
      ackView(m);
      return;
    }

    boolean viewContainsMyNewAddress = false;
    if (!this.isJoined && !m.isPreparing()) {
      // if we're still waiting for a join response and we're in this view we
      // should install the view so join() can finish its work
      for (ID mbr : view.getMembers()) {
        if (localAddress.equals(mbr)) {
          viewContainsMyNewAddress = true;
          break;
        }
      }
    }

    if (m.isPreparing()) {
      if (this.preparedView != null && this.preparedView.getViewId() >= view.getViewId()) {
        if (this.preparedView.getViewId() == view.getViewId() &&
            this.preparedView.getCreator().equals(view.getCreator())) {
          // this can happen if we received two prepares during auto-reconnect
        } else {
          // send the conflicting view to the creator of this new view
          services.getMessenger()
              .send(new ViewAckMessage<>(view.getViewId(), m.getSender(), this.preparedView));
        }
      } else {
        this.preparedView = view;
        // complete filling in the member ID of this node, if possible
        for (ID mbr : view.getMembers()) {
          if (this.localAddress.equals(mbr)) {
            this.birthViewId = mbr.getVmViewId();
            this.localAddress.setVmViewId(this.birthViewId);
            ID me = this.localAddress;
            me.setVmViewId(birthViewId);
            break;
          }
        }
        ackView(m);
      }
    } else { // !preparing
      if (isJoined && currentView != null && !view.contains(this.localAddress)) {
        logger.fatal(
            "This member is no longer in the membership view.  My ID is {} and the new view is {}",
            localAddress, view);
        forceDisconnect("This node is no longer in the membership view");
      } else {
        if (isJoined || viewContainsMyNewAddress) {
          installView(view);
        }
        if (!m.isRebroadcast()) { // no need to ack a rebroadcast view
          ackView(m);
        }
      }
    }
  }

  private void forceDisconnect(String reason) {
    this.isStopping = true;
    if (!isJoined) {
      joinResponse[0] =
          new JoinResponseMessage<>(
              "Stopping due to ForcedDisconnectException caused by '" + reason + "'", -1);
      isJoined = false;
      synchronized (joinResponse) {
        joinResponse.notifyAll();
      }
    } else {
      services.getManager().forceDisconnect(reason);
    }
  }

  private void ackView(InstallViewMessage<ID> m) {
    if (!playingDead && m.getView().contains(m.getView().getCreator())) {
      services.getMessenger()
          .send(new ViewAckMessage<>(m.getSender(), m.getView().getViewId(), m.isPreparing()));
    }
  }

  void processMessage(ViewAckMessage<ID> m) {
    if (isStopping) {
      return;
    }

    if (m.isPrepareAck()) {
      this.prepareProcessor.processViewResponse(m.getViewId(), m.getSender(), m.getAlternateView());
    } else {
      this.viewProcessor.processViewResponse(m.getViewId(), m.getSender(), m.getAlternateView());
    }
  }

  /**
   * This contacts the locators to find out who the current coordinator is. All locators are
   * contacted. If they don't agree then we choose the oldest coordinator and return it.
   */
  boolean findCoordinator() throws MemberStartupException {
    SearchState<ID> state = searchState;

    assert this.localAddress != null;

    if (!state.hasContactedAJoinedLocator && state.registrants.size() >= locators.size()
        && state.view != null && state.viewId > state.lastFindCoordinatorInViewId) {
      state.lastFindCoordinatorInViewId = state.viewId;
      logger.info("using findCoordinatorFromView");
      return findCoordinatorFromView();
    }

    String dhalgo = services.getConfig().getSecurityUDPDHAlgo();
    FindCoordinatorRequest<ID> request = new FindCoordinatorRequest<>(this.localAddress,
        state.alreadyTried, state.viewId, services.getMessenger().getPublicKey(localAddress),
        services.getMessenger().getRequestId(), dhalgo);
    Set<ID> possibleCoordinators = new HashSet<ID>();
    Set<ID> coordinatorsWithView = new HashSet<ID>();

    long giveUpTime =
        System.currentTimeMillis() + ((long) services.getConfig().getLocatorWaitTime() * 1000L);

    int connectTimeout = (int) services.getConfig().getMemberTimeout() * 2;
    boolean anyResponses = false;

    logger.debug("sending {} to {}", request, locators);

    state.hasContactedAJoinedLocator = false;
    state.locatorsContacted = 0;

    do {
      for (HostAndPort laddr : locators) {
        try {
          Object o = locatorClient.requestToServer(laddr, request, connectTimeout, true);
          FindCoordinatorResponse<ID> response =
              (o instanceof FindCoordinatorResponse) ? (FindCoordinatorResponse<ID>) o : null;
          if (response != null) {
            if (response.getRejectionMessage() != null) {
              throw new MembershipConfigurationException(response.getRejectionMessage());
            }
            setCoordinatorPublicKey(response);
            state.locatorsContacted++;
            if (response.getRegistrants() != null) {
              state.registrants.addAll(response.getRegistrants());
            }
            logger.info("received {} from locator {}", response, laddr);
            if (!state.hasContactedAJoinedLocator && response.getSenderId() != null
                && response.getSenderId().getVmViewId() >= 0) {
              logger.info("Locator's address indicates it is part of a distributed system "
                  + "so I will not become membership coordinator on this attempt to join");
              state.hasContactedAJoinedLocator = true;
            }
            ID responseCoordinator = response.getCoordinator();
            if (responseCoordinator != null) {
              anyResponses = true;
              GMSMembershipView<ID> v = response.getView();
              int viewId = v == null ? -1 : v.getViewId();
              if (viewId > state.viewId) {
                state.viewId = viewId;
                state.view = v;
                state.registrants.clear();
              }
              if (viewId > -1) {
                coordinatorsWithView.add(responseCoordinator);
              }
              // if this node is restarting it should never create its own cluster because
              // the QuorumChecker would have contacted a quorum of live nodes and one of
              // them should already be the coordinator, or should become the coordinator soon
              boolean isMyOldAddress =
                  services.getConfig().isReconnecting() && localAddress.equals(responseCoordinator)
                      && responseCoordinator.getVmViewId() >= 0;
              if (!isMyOldAddress) {
                possibleCoordinators.add(response.getCoordinator());
              }
            }
          }
        } catch (IOException | ClassNotFoundException problem) {
          logger.info("Exception thrown when contacting a locator", problem);
          if (state.locatorsContacted == 0 && System.currentTimeMillis() < giveUpTime) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              services.getCancelCriterion().checkCancelInProgress(e);
              throw new MemberStartupException("Interrupted while trying to contact locators");
            }
          }
        }
      }
    } while (!anyResponses && System.currentTimeMillis() < giveUpTime);
    if (possibleCoordinators.isEmpty()) {
      return false;
    }

    if (coordinatorsWithView.size() > 0) {
      possibleCoordinators = coordinatorsWithView;// lets check current coordinators in view only
    }

    Iterator<ID> it = possibleCoordinators.iterator();
    if (possibleCoordinators.size() == 1) {
      state.possibleCoordinator = it.next();
    } else {
      ID oldest = it.next();
      while (it.hasNext()) {
        ID candidate = it.next();
        if (services.getMemberFactory().getComparator().compare(oldest, candidate) > 0) {
          oldest = candidate;
        }
      }
      state.possibleCoordinator = oldest;
    }
    logger.info("findCoordinator chose {} out of these possible coordinators: {}",
        state.possibleCoordinator, possibleCoordinators);
    return true;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WA_NOT_IN_LOOP")
  boolean findCoordinatorFromView() {
    ArrayList<FindCoordinatorResponse<ID>> result;
    SearchState<ID> state = searchState;
    GMSMembershipView<ID> v = state.view;
    List<ID> recipients = new ArrayList<>(v.getMembers());

    logger.info("searching for coordinator in findCoordinatorFromView");

    if (recipients.size() > MAX_DISCOVERY_NODES && MAX_DISCOVERY_NODES > 0) {
      recipients = recipients.subList(0, MAX_DISCOVERY_NODES);
    }
    if (state.registrants != null) {
      recipients.addAll(state.registrants);
    }
    recipients.remove(localAddress);

    logger.info("sending FindCoordinatorRequests to {}", recipients);

    boolean testing = unitTesting.contains("findCoordinatorFromView");
    synchronized (state.responses) {
      if (!testing) {
        state.responses.clear();
      }

      String dhalgo = services.getConfig().getSecurityUDPDHAlgo();
      if (!dhalgo.isEmpty()) {
        // Here we are sending message one-by-one to all recipients as we don't have cluster secret
        // key yet.
        // Usually this happens when locator re-joins the cluster and it has saved view.
        for (ID mbr : recipients) {
          List<ID> r = new ArrayList<>();
          r.add(mbr);
          FindCoordinatorRequest<ID> req =
              new FindCoordinatorRequest<>(localAddress, state.alreadyTried,
                  state.viewId, services.getMessenger().getPublicKey(localAddress),
                  services.getMessenger().getRequestId(), dhalgo);
          req.setRecipients(r);

          services.getMessenger().send(req, v);
        }
      } else {
        FindCoordinatorRequest<ID> req =
            new FindCoordinatorRequest<>(localAddress, state.alreadyTried,
                state.viewId, services.getMessenger().getPublicKey(localAddress),
                services.getMessenger().getRequestId(), dhalgo);
        req.setRecipients(recipients);

        services.getMessenger().send(req, v);
      }
      try {
        if (!testing) {
          state.responsesExpected = recipients.size();
          state.responses.wait(DISCOVERY_TIMEOUT);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      result = new ArrayList<>(state.responses);
      state.responses.clear();
    }

    ID bestGuessCoordinator = null;
    if (localAddress.preferredForCoordinator()) {
      // it's possible that all other potential coordinators are gone
      // and this new member must become the coordinator
      bestGuessCoordinator = localAddress;
    }
    state.joinedMembersContacted = 0;
    boolean bestGuessIsNotMember = true;
    for (FindCoordinatorResponse<ID> resp : result) {
      logger.info("findCoordinatorFromView processing {}", resp);
      ID suggestedCoordinator = resp.getCoordinator();
      if (resp.getSenderId().getVmViewId() >= 0) {
        state.joinedMembersContacted++;
      }
      if (!localAddress.equals(suggestedCoordinator)
          && !state.alreadyTried.contains(suggestedCoordinator)) {
        boolean suggestedIsNotMember = (suggestedCoordinator.getVmViewId() < 0);
        if (suggestedIsNotMember) {
          // member has not yet joined
          if (bestGuessIsNotMember && (bestGuessCoordinator == null
              || bestGuessCoordinator.getMemberData()
                  .compareTo(suggestedCoordinator.getMemberData(), false) > 0)) {
            bestGuessCoordinator = suggestedCoordinator;
          }
        } else {
          // member has already joined
          if (bestGuessIsNotMember
              || suggestedCoordinator.getVmViewId() > bestGuessCoordinator.getVmViewId()) {
            bestGuessCoordinator = suggestedCoordinator;
            bestGuessIsNotMember = false;
          }
        }
        logger.info("findCoordinatorFromView's best guess is now {}", bestGuessCoordinator);
      }
    }

    state.possibleCoordinator = bestGuessCoordinator;
    return bestGuessCoordinator != null;
  }

  /**
   * receives a JoinResponse holding a membership view or rejection message
   *
   * @param rsp the response message to process
   */
  void processMessage(JoinResponseMessage<ID> rsp) {
    if (isStopping) {
      return;
    }

    synchronized (joinResponse) {
      if (!this.isJoined) {
        // 1. our joinRequest rejected.
        // 2. Member which was coordinator but just now some other member became coordinator
        // 3. we got message with secret key, but still view is coming and that will inform the
        // joining thread
        if (rsp.getRejectionMessage() != null) {
          joinResponse[0] = rsp;
          joinResponse.notifyAll();
        } else if (rsp.getCurrentView() != null) {
          // ignore - we get to join when we receive a view. Joining earlier may
          // confuse other members if we've reused an old address
        } else {
          // we got secret key lets add it
          services.getMessenger().setClusterSecretKey(rsp.getSecretPk());
        }
      }
    }
  }

  /**
   * for testing, do not use in any other case as it is not thread safe
   */
  JoinResponseMessage[] getJoinResponseMessage() {
    return joinResponse;
  }

  /***
   * for testing purpose
   *
   * @param jrm the join response message to process
   */
  void setJoinResponseMessage(JoinResponseMessage<ID> jrm) {
    joinResponse[0] = jrm;
  }

  void processMessage(FindCoordinatorRequest<ID> req) {
    if (isStopping) {
      return;
    }

    FindCoordinatorResponse<ID> resp;
    if (this.isJoined) {
      GMSMembershipView<ID> v = currentView;
      resp = new FindCoordinatorResponse<>(v.getCoordinator(), localAddress,
          services.getMessenger().getPublicKey(v.getCoordinator()), req.getRequestId());
    } else {
      resp = new FindCoordinatorResponse<>(localAddress, localAddress,
          services.getMessenger().getPublicKey(localAddress), req.getRequestId());
    }
    resp.setRecipient(req.getMemberID());
    services.getMessenger().send(resp);
  }

  void processMessage(FindCoordinatorResponse<ID> resp) {
    if (isStopping) {
      return;
    }

    synchronized (searchState.responses) {
      searchState.responses.add(resp);
      if (searchState.responsesExpected <= searchState.responses.size()) {
        searchState.responses.notifyAll();
      }
    }
    setCoordinatorPublicKey(resp);
  }

  private void setCoordinatorPublicKey(FindCoordinatorResponse<ID> response) {
    if (response.getCoordinator() != null && response.getCoordinatorPublicKey() != null)
      services.getMessenger().setPublicKey(response.getCoordinatorPublicKey(),
          response.getCoordinator());
  }

  void processMessage(NetworkPartitionMessage<ID> msg) {
    if (isStopping) {
      return;
    }

    String str = "Membership coordinator " + msg.getSender()
        + " has declared that a network partition has occurred";
    forceDisconnect(str);
  }

  @Override
  public GMSMembershipView<ID> getView() {
    return currentView;
  }

  @Override
  public GMSMembershipView<ID> getPreviousView() {
    return previousView;
  }

  @Override
  public ID getMemberID() {
    return this.localAddress;
  }

  @Override
  public void installView(GMSMembershipView<ID> newView) {

    synchronized (viewInstallationLock) {
      if (currentView != null && currentView.getViewId() >= newView.getViewId()) {
        // old view - ignore it
        return;
      }

      logger.info("received new view: {}\nold view is: {}", newView, currentView);

      if (currentView == null && !this.isJoined) {
        boolean found = false;
        List<ID> members = newView.getMembers();
        for (ID mbr : members) {
          if (this.localAddress.equals(mbr)) {
            found = true;
            this.birthViewId = mbr.getVmViewId();
            this.localAddress.setVmViewId(this.birthViewId);
            ID me = this.localAddress;
            me.setVmViewId(birthViewId);
            break;
          }
        }
        if (!found) {
          logger.info("rejecting view (not yet joined)");
          return;
        }
      }

      if (isJoined && isNetworkPartition(newView, true)) {
        if (quorumRequired) {
          Set<ID> crashes = newView.getActualCrashedMembers(currentView);
          forceDisconnect(String.format(
              "Exiting due to possible network partition event due to loss of %s cache processes: %s",
              crashes.size(), crashes));
          return;
        }
      }

      previousView = currentView;
      currentView = newView;
      preparedView = null;
      lastConflictingView = null;
      services.installView(newView);

      if (!isJoined) {
        logger.debug("notifying join thread");
        isJoined = true;
        synchronized (joinResponse) {
          joinResponse.notifyAll();
        }
      }

      if (!newView.getCreator().equals(this.localAddress)) {
        GMSMembershipView<ID> check = new GMSMembershipView<>(newView, newView.getViewId() + 1);
        synchronized (leftMembers) {
          check.removeAll(leftMembers);
        }
        synchronized (removedMembers) {
          check.removeAll(removedMembers);
          check.addCrashedMembers(removedMembers);
        }
        if (check.shouldBeCoordinator(this.localAddress)) {
          if (!isCoordinator) {
            becomeCoordinator();
          }
        } else if (this.isCoordinator) {
          // stop being coordinator
          stopCoordinatorServices();
          this.isCoordinator = false;
        }
      }
      if (!this.isCoordinator) {
        // get rid of outdated requests. It's possible some requests are
        // newer than the view just processed - the senders will have to
        // resend these
        synchronized (viewRequests) {
          for (Iterator<AbstractGMSMessage<ID>> it = viewRequests.iterator(); it.hasNext();) {
            AbstractGMSMessage<ID> m = it.next();
            if (m instanceof JoinRequestMessage) {
              if (currentView.contains(((JoinRequestMessage<ID>) m).getMemberID())) {
                it.remove();
              }
            } else if (m instanceof LeaveRequestMessage) {
              if (!currentView.contains(((LeaveRequestMessage<ID>) m).getMemberID())) {
                it.remove();
              }
            } else if (m instanceof RemoveMemberMessage) {
              if (!currentView.contains(((RemoveMemberMessage<ID>) m).getMemberID())) {
                it.remove();
              }
            }
          }
        }
      }
    }
    synchronized (removedMembers) {
      removeMembersFromCollectionIfNotInView(removedMembers,
          currentView);
    }
    synchronized (leftMembers) {
      removeMembersFromCollectionIfNotInView(leftMembers,
          currentView);
    }
  }

  private void removeMembersFromCollectionIfNotInView(Collection<ID> members,
      GMSMembershipView<ID> currentView) {
    Iterator<ID> iterator = members.iterator();
    while (iterator.hasNext()) {
      if (!currentView.contains(iterator.next())) {
        iterator.remove();
      }
    }
  }

  /**
   * Sends a message declaring a network partition to the members of the given view via Messenger
   *
   */
  void sendNetworkPartitionMessage(GMSMembershipView<ID> view) {
    List<ID> recipients = new ArrayList<>(view.getMembers());
    recipients.remove(localAddress);
    NetworkPartitionMessage<ID> msg = new NetworkPartitionMessage<>(recipients);
    try {
      services.getMessenger().send(msg);
    } catch (RuntimeException e) {
      logger.debug("unable to send network partition message - continuing", e);
    }
  }

  /**
   * returns true if this member thinks it is the membership coordinator for the distributed system
   */
  public boolean isCoordinator() {
    return this.isCoordinator;
  }

  /**
   * return true if we're stopping or are stopped
   */
  public boolean isStopping() {
    return this.isStopping;
  }

  /**
   * returns the currently prepared view, if any
   */
  public GMSMembershipView<ID> getPreparedView() {
    return this.preparedView;
  }

  /**
   * check to see if the new view shows a drop of 51% or more
   */
  private boolean isNetworkPartition(GMSMembershipView<ID> newView, boolean logWeights) {
    if (currentView == null) {
      return false;
    }
    int oldWeight = currentView.memberWeight();
    int failedWeight = newView.getCrashedMemberWeight(currentView);
    if (failedWeight > 0 && logWeights) {
      if (logger.isInfoEnabled() && newView.getCreator().equals(localAddress)) { // view-creator
                                                                                 // logs this
        newView.logCrashedMemberWeights(currentView, logger);
      }
      int failurePoint = (int) (Math.round(51.0 * oldWeight) / 100.0);
      if (failedWeight > failurePoint && quorumLostView != newView) {
        quorumLostView = newView;
        logger.warn("total weight lost in this view change is {} of {}.  Quorum has been lost!",
            failedWeight, oldWeight);
        services.getManager().quorumLost(newView.getActualCrashedMembers(currentView), currentView);
        return true;
      }
    }
    return false;
  }

  private void stopCoordinatorServices() {
    if (viewCreator != null && !viewCreator.isShutdown()) {
      logger.debug("Shutting down ViewCreator");
      viewCreator.shutdown();
      try {
        viewCreator.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void startViewBroadcaster() {
    services.getTimer().schedule(new ViewBroadcaster(), VIEW_BROADCAST_INTERVAL,
        VIEW_BROADCAST_INTERVAL);
  }

  @Override
  public void emergencyClose() {
    isStopping = true;
    isJoined = false;
    stopCoordinatorServices();
    isCoordinator = false;
  }

  @Override
  public void beSick() {}

  @Override
  public void playDead() {
    playingDead = true;
  }

  @Override
  public void beHealthy() {
    playingDead = false;
  }

  @Override
  public void start() throws MemberStartupException {}

  @Override
  public void started() throws MemberStartupException {}

  public void setLocalAddress(ID address) {
    this.localAddress = address;
    ID mbr = this.localAddress;

    if (services.getConfig().areLocatorsPreferredAsCoordinators()) {
      boolean preferred = false;
      if (services.getLocator() != null || services.getConfig().getHasLocator()
          || !services.getConfig().getStartLocator().isEmpty()
          || localAddress.getVmKind() == MemberIdentifier.LOCATOR_DM_TYPE) {
        logger
            .info("This member is hosting a locator will be preferred as a membership coordinator");
        preferred = true;
      }
      mbr.setPreferredForCoordinator(preferred);
    } else {
      mbr.setPreferredForCoordinator(true);
    }
  }

  @Override
  public void stop() {
    logger.debug("JoinLeave stopping");
    leave();
  }

  @Override
  public void stopped() {}

  @Override
  public void memberSuspected(ID initiator,
      ID suspect, String reason) {
    prepareProcessor.memberSuspected(suspect);
    viewProcessor.memberSuspected(suspect);
  }

  @Override
  public void leave() {
    synchronized (viewInstallationLock) {
      GMSMembershipView<ID> view = currentView;
      isStopping = true;
      stopCoordinatorServices();
      if (view != null) {
        if (view.size() > 1) {
          List<ID> coords =
              view.getPreferredCoordinators(Collections.emptySet(), localAddress, 5);
          logger.debug("Sending my leave request to {}", coords);
          LeaveRequestMessage<ID> m =
              new LeaveRequestMessage<>(coords, this.localAddress, "this member is shutting down");
          services.getMessenger().send(m);
        } // view.size
      } // view != null
    }
  }

  @Override
  public void remove(ID m, String reason) {
    GMSMembershipView<ID> v = this.currentView;

    services.getCancelCriterion().checkCancelInProgress(null);

    if (v != null && v.contains(m)) {
      Set<ID> filter = new HashSet<>();
      filter.add(m);
      RemoveMemberMessage<ID> msg =
          new RemoveMemberMessage<>(v.getPreferredCoordinators(filter, getMemberID(), 5), m,
              reason);
      msg.setSender(this.localAddress);
      processMessage(msg);
      if (!this.isCoordinator) {
        msg.setRecipients(v.getPreferredCoordinators(Collections.emptySet(), localAddress,
            MembershipConfig.SMALL_CLUSTER_SIZE + 1));
        services.getMessenger().send(msg);
      }
    } else {
      RemoveMemberMessage<ID> msg = new RemoveMemberMessage<>(m, m, reason);
      services.getMessenger().send(msg);
    }
  }

  @Override
  public void memberShutdown(ID mbr, String reason) {
    LeaveRequestMessage<ID> msg =
        new LeaveRequestMessage<>(Collections.singletonList(this.localAddress),
            mbr, reason);
    msg.setSender(mbr);
    processMessage(msg);
  }

  boolean checkIfAvailable(ID fmbr) {
    // return the member id if it fails health checks
    logger.info("checking state of member " + fmbr);
    if (services.getHealthMonitor().checkIfAvailable(fmbr,
        "Member failed to acknowledge a membership view", false)) {
      logger.info("member " + fmbr + " passed availability check");
      return true;
    }
    logger.info("member " + fmbr + " failed availability check");
    return false;
  }

  private ID getMemId(ID jgId,
      List<ID> members) {
    for (ID m : members) {
      if (m.equals(jgId)) {
        return m;
      }
    }
    return null;
  }

  @Override
  public ID getMemberID(ID member) {
    GMSMembershipView<ID> v = currentView;
    ID ret = null;
    if (v != null) {
      ret = getMemId(member, v.getMembers());
    }

    if (ret == null) {
      v = preparedView;
      if (v != null) {
        ret = getMemId(member, v.getMembers());
      }
    }

    if (ret == null) {
      return member;
    }

    return ret;
  }

  @Override
  public void disableDisconnectOnQuorumLossForTesting() {
    this.quorumRequired = false;
  }

  @Override
  public void init(Services<ID> s) throws MembershipConfigurationException {
    this.services = s;

    MembershipConfig config = services.getConfig();
    if (config.getMcastPort() != 0 && StringUtils.isBlank(config.getLocators())
        && StringUtils.isBlank(config.getStartLocator())) {
      throw new MembershipConfigurationException(
          "Multicast cannot be configured for a non-distributed cache."
              + "  Please configure the locator services for this cache using "
              + MembershipConfig.LOCATORS + " or "
              + MembershipConfig.START_LOCATOR + ".");
    }

    services.getMessenger().addHandler(JoinRequestMessage.class, this::processMessage);
    services.getMessenger().addHandler(JoinResponseMessage.class, this::processMessage);
    services.getMessenger().addHandler(InstallViewMessage.class, this::processMessage);
    services.getMessenger().addHandler(ViewAckMessage.class, this::processMessage);
    services.getMessenger().addHandler(LeaveRequestMessage.class, this::processMessage);
    services.getMessenger().addHandler(RemoveMemberMessage.class, this::processMessage);
    services.getMessenger().addHandler(FindCoordinatorRequest.class, this::processMessage);
    services.getMessenger().addHandler(FindCoordinatorResponse.class, this::processMessage);
    services.getMessenger().addHandler(NetworkPartitionMessage.class, this::processMessage);

    long ackCollectionTimeout = config.getMemberTimeout() * 2 * 12437 / 10000;
    if (ackCollectionTimeout < 1500) {
      ackCollectionTimeout = 1500;
    } else if (ackCollectionTimeout > 12437) {
      ackCollectionTimeout = 12437;
    }
    ackCollectionTimeout = Long
        .getLong(GeodeGlossary.GEMFIRE_PREFIX + "VIEW_ACK_TIMEOUT", ackCollectionTimeout)
        .longValue();
    this.viewAckTimeout = ackCollectionTimeout;

    this.quorumRequired =
        config.isNetworkPartitionDetectionEnabled();

    String bindAddr = config.getBindAddress();
    locators = GMSUtil.parseLocators(config.getLocators(), bindAddr);
    if (logger.isDebugEnabled()) {
      logger.debug("Parsed locators are {}", locators);
    }
  }

  /**
   * returns the member IDs of the pending requests having the given DataSerializableFixedID
   */
  Set<ID> getPendingRequestIDs(int theDSFID) {
    Set<ID> result = new HashSet<>();
    synchronized (viewRequests) {
      for (AbstractGMSMessage<ID> msg : viewRequests) {
        if (msg.getDSFID() == theDSFID) {
          result.add(((HasMemberID<ID>) msg).getMemberID());
        }
      }
    }
    return result;
  }

  /***
   * test method
   *
   */
  protected ViewReplyProcessor getPrepareViewReplyProcessor() {
    return prepareProcessor;
  }

  protected boolean testPrepareProcessorWaiting() {
    return prepareProcessor.isWaiting();
  }

  class ViewReplyProcessor {
    volatile int viewId = -1;
    final Set<ID> notRepliedYet = new HashSet<>();
    GMSMembershipView<ID> conflictingView;
    ID conflictingViewSender;
    volatile boolean waiting;
    final boolean isPrepareViewProcessor;
    final Set<ID> pendingRemovals = new HashSet<>();

    ViewReplyProcessor(boolean forPreparation) {
      this.isPrepareViewProcessor = forPreparation;
    }

    synchronized void initialize(int viewId, Set<ID> recips) {
      waiting = true;
      this.viewId = viewId;
      notRepliedYet.clear();
      notRepliedYet.addAll(recips);
      conflictingView = null;
      pendingRemovals.clear();
    }

    boolean isWaiting() {
      return waiting;
    }

    synchronized void processPendingRequests(Set<ID> pendingLeaves,
        Set<ID> pendingRemovals) {
      // there's no point in waiting for members who have already
      // requested to leave or who have been declared crashed.
      // We don't want to mix the two because pending removals
      // aren't reflected as having crashed in the current view
      // and need to cause a new view to be generated
      for (ID mbr : pendingLeaves) {
        notRepliedYet.remove(mbr);
      }
      for (ID mbr : pendingRemovals) {
        if (this.notRepliedYet.contains(mbr)) {
          this.pendingRemovals.add(mbr);
        }
      }
    }

    synchronized void memberSuspected(ID suspect) {
      if (waiting) {
        // we will do a final check on this member if it hasn't already
        // been done, so stop waiting for it now
        logger.debug("view response processor recording suspect status for {}", suspect);
        if (notRepliedYet.contains(suspect) && !pendingRemovals.contains(suspect)) {
          pendingRemovals.add(suspect);
          checkIfDone();
        }
      }
    }

    synchronized void processLeaveRequest(ID mbr) {
      if (waiting) {
        logger.debug("view response processor recording leave request for {}", mbr);
        stopWaitingFor(mbr);
      }
    }

    synchronized void processRemoveRequest(ID mbr) {
      if (waiting) {
        logger.debug("view response processor recording remove request for {}", mbr);
        pendingRemovals.add(mbr);
        checkIfDone();
      }
    }

    synchronized void processViewResponse(int viewId, ID sender,
        GMSMembershipView<ID> conflictingView) {
      if (!waiting) {
        return;
      }

      if (viewId == this.viewId) {
        if (conflictingView != null) {
          this.conflictingViewSender = sender;
          this.conflictingView = conflictingView;
        }

        logger.debug("view response processor recording response for {}", sender);
        stopWaitingFor(sender);
      }
    }

    /**
     * call with synchronized(this)
     */
    private void stopWaitingFor(ID mbr) {
      notRepliedYet.remove(mbr);
      checkIfDone();
    }

    /**
     * call with synchronized(this)
     */
    private void checkIfDone() {
      if (notRepliedYet.isEmpty()
          || (pendingRemovals != null && pendingRemovals.containsAll(notRepliedYet))) {
        logger.debug("All anticipated view responses received - notifying waiting thread");
        waiting = false;
        notifyAll();
      } else {
        logger.debug("Still waiting for these view replies: {}", notRepliedYet);
      }
    }

    Set<ID> waitForResponses() throws InterruptedException {
      Set<ID> result;
      long endOfWait = System.currentTimeMillis() + viewAckTimeout;
      try {
        while (System.currentTimeMillis() < endOfWait
            && (!services.getCancelCriterion().isCancelInProgress())) {
          try {
            synchronized (this) {
              if (!waiting || this.notRepliedYet.isEmpty() || this.conflictingView != null) {
                break;
              }
              wait(1000);
            }
          } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting for view responses");
            throw e;
          }
        }
      } finally {
        synchronized (this) {
          if (!this.waiting) {
            // if we've set waiting to false due to incoming messages then
            // we've discounted receiving any other responses from the
            // remaining members due to leave/crash notification
            result = new HashSet<>(pendingRemovals);
          } else {
            result = new HashSet<>(this.notRepliedYet);
            result.addAll(pendingRemovals);
            this.waiting = false;
          }
        }
      }
      return result;
    }

    GMSMembershipView<ID> getConflictingView() {
      return this.conflictingView;
    }

    ID getConflictingViewSender() {
      return this.conflictingViewSender;
    }

    synchronized Set<ID> getUnresponsiveMembers() {
      return new HashSet<>(this.notRepliedYet);
    }
  }

  /**
   * ViewBroadcaster periodically sends the current view to all current and departed members. This
   * ensures that a member that missed the view will eventually see it and act on it.
   */
  class ViewBroadcaster extends TimerTask {

    @Override
    public void run() {
      if (!isCoordinator || isStopping) {
        cancel();
      } else {
        sendCurrentView();
      }
    }

    void sendCurrentView() {
      GMSMembershipView<ID> v = currentView;
      if (v != null) {
        InstallViewMessage<ID> msg = new InstallViewMessage<>(v,
            services.getAuthenticator().getCredentials(localAddress), false);
        List<ID> recips =
            new ArrayList<>(v.size() + v.getCrashedMembers().size());
        recips.addAll(v.getMembers());
        recips.remove(localAddress);
        recips.addAll(v.getCrashedMembers());
        msg.setRecipients(recips);
        // use sendUnreliably since we are sending to crashed members &
        // don't want any retransmission tasks set up for them
        services.getMessenger().sendUnreliably(msg);
      }
    }
  }

  class ViewCreator extends LoggingThread {
    volatile boolean shutdown = false;
    volatile boolean waiting = false;
    volatile boolean testFlagForRemovalRequest = false;
    // count of number of views abandoned due to conflicts
    volatile int abandonedViews = 0;
    private boolean markViewCreatorForShutdown = false; // see GEODE-870


    /**
     * initial view to install. guarded by synch on ViewCreator
     */
    GMSMembershipView<ID> initialView;
    /**
     * initial joining members. guarded by synch on ViewCreator
     */
    List<ID> initialJoins = Collections.emptyList();
    /**
     * initial leaving members guarded by synch on ViewCreator
     */
    Set<ID> initialLeaving;
    /**
     * initial crashed members. guarded by synch on ViewCreator
     */
    Set<ID> initialRemovals;

    ViewCreator(String name) {
      super(name);
    }

    void shutdown() {
      setShutdownFlag();
      synchronized (viewRequests) {
        viewRequests.notifyAll();
        interrupt();
      }
    }

    boolean isShutdown() {
      return shutdown;
    }

    boolean isWaiting() {
      return waiting;
    }

    int getAbandonedViewCount() {
      return abandonedViews;
    }

    /**
     * All views should be sent by the ViewCreator thread, so if this member becomes coordinator it
     * may have an initial view to transmit that announces the removal of the former coordinator to
     *
     * @param leaving - members leaving in this view
     * @param removals - members crashed in this view
     */
    synchronized void setInitialView(GMSMembershipView<ID> newView, List<ID> newMembers,
        Set<ID> leaving, Set<ID> removals) {
      this.initialView = newView;
      this.initialJoins = newMembers;
      this.initialLeaving = leaving;
      this.initialRemovals = removals;
    }

    private void sendInitialView() {
      boolean retry;
      do {
        retry = false;
        try {
          if (initialView == null) {
            return;
          }
          GMSMembershipView<ID> v = preparedView;
          if (v != null) {
            processPreparedView(v);
          }
          try {
            GMSMembershipView<ID> iView;
            List<ID> iJoins;
            Set<ID> iLeaves;
            Set<ID> iRemoves;
            synchronized (this) {
              iView = initialView;
              iJoins = initialJoins;
              iLeaves = initialLeaving;
              iRemoves = initialRemovals;
            }
            if (iView != null) {
              prepareAndSendView(iView, iJoins, iLeaves, iRemoves);
            }
          } finally {
            setInitialView(null, null, null, null);
          }
        } catch (ViewAbandonedException e) {
          // another view creator is active - sleep a bit to let it finish or go away
          retry = true;
          try {
            sleep(services.getConfig().getMemberTimeout());
          } catch (InterruptedException e2) {
            setShutdownFlag();
            retry = false;
          }
        } catch (InterruptedException e) {
          setShutdownFlag();
        } catch (MembershipClosedException e) {
          setShutdownFlag();
        }
      } while (retry);
    }

    /**
     * marks this ViewCreator as being shut down. It may be some short amount of time before the
     * ViewCreator thread exits.
     */
    private void setShutdownFlag() {
      shutdown = true;
    }

    /**
     * This allows GMSJoinLeave to tell the ViewCreator to shut down after finishing its current
     * task. See GEODE-870.
     */
    private void markViewCreatorForShutdown(ID viewCreator) {
      logger.info(
          "Marking view creator for shutdown because {} is now the coordinator.  My address is {}."
              + "  Net member IDs are {} and {} respectively",
          viewCreator, localAddress, viewCreator, localAddress);
      this.markViewCreatorForShutdown = true;
    }

    /**
     * During initial view processing a prepared view was discovered. This method will extract its
     * new members and create a new initial view containing them.
     *
     * @param v The prepared view
     */
    private void processPreparedView(GMSMembershipView<ID> v) {
      assert initialView != null;
      if (currentView == null || currentView.getViewId() < v.getViewId()) {
        // we have a prepared view that is newer than the current view
        // form a new View ID
        int viewId = Math.max(initialView.getViewId(), v.getViewId());
        viewId += 1;
        GMSMembershipView<ID> newView = new GMSMembershipView<>(initialView, viewId);

        // add the new members from the prepared view to the new view,
        // preserving their failure-detection ports
        List<ID> newMembers;
        if (currentView != null) {
          newMembers = v.getNewMembers(currentView);
        } else {
          newMembers = v.getMembers();
        }
        for (ID newMember : newMembers) {
          newView.add(newMember);
          newView.setFailureDetectionPort(newMember, v.getFailureDetectionPort(newMember));
          newView.setPublicKey(newMember, v.getPublicKey(newMember));
        }

        // use the new view as the initial view
        synchronized (this) {
          setInitialView(newView, newMembers, initialLeaving, initialRemovals);
        }
      }
    }

    @Override
    public void run() {
      List<AbstractGMSMessage<ID>> requests = null;
      logger.info("View Creator thread is starting");
      sendInitialView();
      long okayToCreateView = System.currentTimeMillis() + requestCollectionInterval;
      try {
        for (;;) {
          synchronized (viewRequests) {
            if (shutdown) {
              return;
            }
            if (viewRequests.isEmpty()) {
              try {
                logger.debug("View Creator is waiting for requests");
                waiting = true;
                viewRequests.wait();
              } catch (InterruptedException e) {
                return;
              } finally {
                waiting = false;
              }
              if (shutdown || Thread.currentThread().isInterrupted()) {
                return;
              }
              if (viewRequests.size() == 1) {
                // start the timer when we have only one request because
                // concurrent startup / shutdown of multiple members is
                // a common occurrence
                okayToCreateView = System.currentTimeMillis() + requestCollectionInterval;
                continue;
              }
            } else {
              long timeRemaining = okayToCreateView - System.currentTimeMillis();
              if (timeRemaining > 0) {
                // sleep to let more requests arrive
                try {
                  viewRequests.wait(Math.min(100, timeRemaining));
                  continue;
                } catch (InterruptedException e) {
                  return;
                }
              } else {
                // time to create a new membership view
                if (requests == null) {
                  requests = new ArrayList<AbstractGMSMessage<ID>>(viewRequests);
                } else {
                  requests.addAll(viewRequests);
                }
                viewRequests.clear();
                okayToCreateView = System.currentTimeMillis() + requestCollectionInterval;
              }
            }
          } // synchronized
          if (requests != null && !requests.isEmpty()) {
            logger.info("View Creator is processing {} requests for the next membership view ({})",
                requests.size(), requests);
            try {
              createAndSendView(requests);
              if (shutdown) {
                return;
              }
            } catch (ViewAbandonedException e) {
              synchronized (viewRequests) {
                viewRequests.addAll(requests);
              }
              // pause before reattempting so that another view creator can either finish
              // or fail
              try {
                sleep(services.getConfig().getMemberTimeout());
              } catch (InterruptedException e2) {
                setShutdownFlag();
              }
            } catch (MembershipClosedException e) {
              setShutdownFlag();
            } catch (InterruptedException e) {
              logger.info("View Creator thread interrupted");
              setShutdownFlag();
            }
            requests = null;
          }
        }
      } finally {
        logger.info("View Creator thread is exiting");
        setShutdownFlag();
        informToPendingJoinRequests();
        org.apache.geode.distributed.internal.membership.gms.interfaces.Locator<ID> locator =
            services.getLocator();
        if (locator != null) {
          locator.setIsCoordinator(false);
        }
      }
    }

    synchronized boolean informToPendingJoinRequests() {

      if (!shutdown) {
        return false;
      }
      GMSMembershipView<ID> v = currentView;
      if (v.getCoordinator().equals(localAddress)) {
        return false;
      }

      ArrayList<JoinRequestMessage<ID>> requests = new ArrayList<>();
      synchronized (viewRequests) {
        if (viewRequests.isEmpty()) {
          return false;
        }
        for (Iterator<AbstractGMSMessage<ID>> iterator = viewRequests.iterator(); iterator
            .hasNext();) {
          AbstractGMSMessage<ID> msg = iterator.next();
          switch (msg.getDSFID()) {
            case JOIN_REQUEST:
              requests.add((JoinRequestMessage<ID>) msg);
              break;
            default:
              break;
          }
        }
      }

      if (requests.isEmpty()) {
        return false;
      }

      for (JoinRequestMessage<ID> msg : requests) {
        logger.debug("Sending coordinator to pending join request from {} myid {} coord {}",
            msg.getSender(), localAddress, v.getCoordinator());
        JoinResponseMessage<ID> jrm =
            new JoinResponseMessage<>(msg.getMemberID(), v, msg.getRequestId());
        services.getMessenger().send(jrm);
      }

      return true;
    }

    /**
     * Create a new membership view and send it to members (including crashed members). Returns
     * false if the view cannot be prepared successfully, true otherwise
     *
     */
    void createAndSendView(List<AbstractGMSMessage<ID>> requests)
        throws InterruptedException, ViewAbandonedException {
      List<ID> joinReqs = new ArrayList<>(10);
      Map<ID, Integer> joinPorts = new HashMap<>(10);
      Set<ID> leaveReqs = new HashSet<>(10);
      List<ID> removalReqs = new ArrayList<>(10);
      List<String> removalReasons = new ArrayList<String>(10);

      GMSMembershipView<ID> oldView = currentView;
      List<ID> oldMembers;
      if (oldView != null) {
        oldMembers = new ArrayList<>(oldView.getMembers());
      } else {
        oldMembers = Collections.emptyList();
      }
      Set<ID> oldIDs = new HashSet<>();

      for (AbstractGMSMessage<ID> msg : requests) {
        logger.debug("processing request {}", msg);

        ID mbr;
        switch (msg.getDSFID()) {
          case JOIN_REQUEST:
            JoinRequestMessage<ID> jmsg = (JoinRequestMessage<ID>) msg;
            mbr = jmsg.getMemberID();
            int port = jmsg.getFailureDetectionPort();
            if (!joinReqs.contains(mbr)) {
              if (mbr.getVmViewId() >= 0 && oldMembers.contains(mbr)) {
                // already joined in a previous view
                logger.info("Ignoring join request from member {} who has already joined", mbr);
              } else {
                joinReqs.add(mbr);
                joinPorts.put(mbr, port);
                if (services.getConfig().isUDPSecurityEnabled() ||
                    services.getConfig().isMulticastEnabled()) {
                  // send a join response so the new member can get the multicast messaging digest
                  // and cluster key
                  JoinResponseMessage<ID> response = new JoinResponseMessage<>(jmsg.getSender(),
                      services.getMessenger().getClusterSecretKey(), jmsg.getRequestId());
                  services.getMessenger().send(response);
                }
              }
            }
            break;
          case LEAVE_REQUEST_MESSAGE:
            mbr = ((LeaveRequestMessage<ID>) msg).getMemberID();
            if (oldMembers.contains(mbr) && !leaveReqs.contains(mbr)) {
              leaveReqs.add(mbr);
            }
            break;
          case REMOVE_MEMBER_REQUEST:
            // process these after gathering all leave-requests so that
            // we don't kick out a member that's shutting down
            break;
          default:
            logger.warn("Unknown membership request encountered: {}", msg);
            break;
        }
      }

      for (AbstractGMSMessage<ID> msg : requests) {
        switch (msg.getDSFID()) {
          case REMOVE_MEMBER_REQUEST:
            ID mbr = ((RemoveMemberMessage<ID>) msg).getMemberID();
            if (!leaveReqs.contains(mbr)) {
              if (oldMembers.contains(mbr) && !removalReqs.contains(mbr)) {
                removalReqs.add(mbr);
                removalReasons.add(((RemoveMemberMessage<ID>) msg).getReason());
              } else {
                // unknown, probably rogue, process - send it a removal message
                sendRemoveMessages(Collections.singletonList(mbr),
                    Collections.singletonList(((RemoveMemberMessage<ID>) msg).getReason()),
                    new HashSet<>());
              }
            }
            break;
          default:
            break;
        }
      }

      for (ID mbr : oldIDs) {
        if (!leaveReqs.contains(mbr) && !removalReqs.contains(mbr)) {
          removalReqs.add(mbr);
          removalReasons.add("Removal of old ID that has been reused");
        }
      }

      if (removalReqs.isEmpty() && leaveReqs.isEmpty() && joinReqs.isEmpty()) {
        return;
      }

      GMSMembershipView<ID> newView;
      synchronized (viewInstallationLock) {
        int viewNumber = 0;
        List<ID> mbrs;
        if (currentView == null) {
          mbrs = new ArrayList<ID>();
        } else {
          viewNumber = currentView.getViewId() + 1;
          mbrs = new ArrayList<ID>(oldMembers);
        }
        mbrs.removeAll(leaveReqs);
        mbrs.removeAll(removalReqs);
        // add joinReqs after removing old members because an ID may
        // be reused in an auto-reconnect and get a new vmViewID
        mbrs.addAll(joinReqs);
        newView = new GMSMembershipView<>(localAddress, viewNumber, mbrs, leaveReqs,
            new HashSet<ID>(removalReqs));
        for (ID mbr : joinReqs) {
          if (mbrs.contains(mbr)) {
            newView.setFailureDetectionPort(mbr, joinPorts.get(mbr));
          }
        }
        if (currentView != null) {
          newView.setFailureDetectionPorts(currentView);
          newView.setPublicKeys(currentView);
        }
      }

      // if there are no membership changes then abort creation of
      // the new view
      if (joinReqs.isEmpty() && newView.getMembers().equals(currentView.getMembers())) {
        logger.info("membership hasn't changed - aborting new view {}", newView);
        return;
      }

      for (ID mbr : joinReqs) {
        if (mbr.getVmViewId() < 0) {
          mbr.setVmViewId(newView.getViewId());
        }
      }

      if (isShutdown()) {
        return;
      }

      // send removal messages before installing the view so we stop
      // getting messages from members that have been kicked out
      sendRemoveMessages(removalReqs, removalReasons, oldIDs);

      prepareAndSendView(newView, joinReqs, leaveReqs, newView.getCrashedMembers());

      return;
    }

    /**
     * This handles the 2-phase installation of the view
     *
     */
    void prepareAndSendView(GMSMembershipView<ID> newView, List<ID> joinReqs,
        Set<ID> leaveReqs, Set<ID> removalReqs)
        throws InterruptedException, ViewAbandonedException {
      boolean prepared;
      do {
        if (this.shutdown || Thread.currentThread().isInterrupted()) {
          return;
        }

        if (quorumRequired && isNetworkPartition(newView, true)) {
          sendNetworkPartitionMessage(newView);
          Thread.sleep(BROADCAST_MESSAGE_SLEEP_TIME);

          Set<ID> crashes = newView.getActualCrashedMembers(currentView);
          forceDisconnect(String.format(
              "Exiting due to possible network partition event due to loss of %s cache processes: %s",
              crashes.size(), crashes));
          setShutdownFlag();
          return;
        }

        prepared = prepareView(newView, joinReqs);
        logger.debug("view preparation phase completed.  prepared={}", prepared);

        GMSMembershipView<ID> conflictingView = prepareProcessor.getConflictingView();
        if (conflictingView == null) {
          conflictingView = GMSJoinLeave.this.preparedView;
        }

        if (prepared) {
          break;
        }

        Set<ID> unresponsive = prepareProcessor.getUnresponsiveMembers();
        unresponsive.removeAll(removalReqs);
        unresponsive.removeAll(leaveReqs);
        if (!unresponsive.isEmpty()) {
          removeHealthyMembers(unresponsive);
          synchronized (viewRequests) {
            // now lets get copy of it in viewRequests sync, as other thread might be accessing it
            unresponsive = new HashSet<>(unresponsive);
          }
        }

        logger.debug("unresponsive members that could not be reached: {}", unresponsive);
        List<ID> failures =
            new ArrayList<>(currentView.getCrashedMembers().size() + unresponsive.size());

        boolean conflictingViewNotFromMe =
            conflictingView != null && !conflictingView.getCreator().equals(localAddress)
                && conflictingView.getViewId() > newView.getViewId();
        if (conflictingViewNotFromMe) {
          boolean conflictingViewIsMostRecent = (lastConflictingView == null
              || conflictingView.getViewId() > lastConflictingView.getViewId());
          if (conflictingViewIsMostRecent) {
            lastConflictingView = conflictingView;
            // if I am not a locator and the conflicting view is from a locator I should
            // let it take control and stop sending membership views
            if (localAddress.getVmKind() != MemberIdentifier.LOCATOR_DM_TYPE
                && conflictingView.getCreator()
                    .getVmKind() == MemberIdentifier.LOCATOR_DM_TYPE) {
              logger.info("View preparation interrupted - a locator is taking over as "
                  + "membership coordinator in this view: {}", conflictingView);
              abandonedViews++;
              throw new ViewAbandonedException();
            }
            logger.info("adding these crashed members from a conflicting view to the crash-set "
                + "for the next view: {}\nconflicting view: {}", unresponsive, conflictingView);
            failures.addAll(conflictingView.getCrashedMembers());
            // this member may have been kicked out of the conflicting view
            if (failures.contains(localAddress)) {
              forceDisconnect("I am no longer a member of the distributed system");
              setShutdownFlag();
              return;
            }
            List<ID> newMembers = conflictingView.getNewMembers();
            if (!newMembers.isEmpty()) {
              logger.info("adding these new members from a conflicting view to the new view: {}",
                  newMembers);
              for (ID mbr : newMembers) {
                int port = conflictingView.getFailureDetectionPort(mbr);
                newView.add(mbr);
                newView.setFailureDetectionPort(mbr, port);
                joinReqs.add(mbr);
              }
            }
            // trump the view ID of the conflicting view so mine will be accepted
            if (conflictingView.getViewId() >= newView.getViewId()) {
              newView = new GMSMembershipView<>(newView, conflictingView.getViewId() + 1);
            }
          }
        }

        if (!unresponsive.isEmpty()) {
          logger.info("adding these unresponsive members to the crash-set for the next view: {}",
              unresponsive);
          failures.addAll(unresponsive);
        }

        failures.removeAll(removalReqs);
        failures.removeAll(leaveReqs);
        prepared = failures.isEmpty();
        if (!prepared) {
          // abort the current view and try again
          removalReqs.addAll(failures);
          List<ID> newMembers = new ArrayList<>(newView.getMembers());
          newMembers.removeAll(removalReqs);
          GMSMembershipView<ID> tempView =
              new GMSMembershipView<>(localAddress, newView.getViewId() + 1, newMembers,
                  leaveReqs, removalReqs);
          List<ID> members = newView.getMembers();
          for (ID mbr : members) {
            if (tempView.contains(mbr)) {
              tempView.setFailureDetectionPort(mbr, newView.getFailureDetectionPort(mbr));
            }
          }
          newView = tempView;
          int size = failures.size();
          List<String> reasons = new ArrayList<>(size);
          for (int i = 0; i < size; i++) {
            reasons.add(
                "Failed to acknowledge a new membership view and then failed tcp/ip connection attempt");
          }
          sendRemoveMessages(failures, reasons, new HashSet<>());
        }

        // if there is no conflicting view then we can count
        // the current state as being prepared. All members
        // who are going to ack have already done so or passed
        // a liveness test
        if (conflictingView == null) {
          prepared = true;
        }

      } while (!prepared);

      lastConflictingView = null;

      sendView(newView, joinReqs);

      // we also send a join response so that information like the multicast message digest
      // can be transmitted to the new members w/o including it in the view message

      if (markViewCreatorForShutdown && getViewCreator() != null) {
        markViewCreatorForShutdown = false;
        setShutdownFlag();
      }

      // after sending a final view we need to stop this thread if
      // the GMS is shutting down
      if (isStopping()) {
        setShutdownFlag();
      }
    }

    /**
     * performs health checks on the collection of members, removing any that are found to be
     * healthy
     *
     */
    private void removeHealthyMembers(final Set<ID> suspects)
        throws InterruptedException {
      List<Callable<ID>> checkers =
          new ArrayList<>(suspects.size());

      Set<ID> newRemovals = new HashSet<>();
      Set<ID> newLeaves = new HashSet<>();

      filterMembers(suspects, newRemovals, REMOVE_MEMBER_REQUEST);
      filterMembers(suspects, newLeaves, LEAVE_REQUEST_MESSAGE);
      newRemovals.removeAll(newLeaves); // if we received a Leave req the member is "healthy"

      suspects.removeAll(newLeaves);

      for (ID mbr : suspects) {
        if (newRemovals.contains(mbr) || newLeaves.contains(mbr)) {
          continue; // no need to check this member - it's already been checked or is leaving
        }
        checkers.add(new Callable<ID>() {
          @Override
          public ID call() throws Exception {
            boolean available = GMSJoinLeave.this.checkIfAvailable(mbr);

            synchronized (viewRequests) {
              if (available) {
                suspects.remove(mbr);
              }
              viewRequests.notifyAll();
            }
            return mbr;
          }

          @Override
          public String toString() {
            return mbr.toString();
          }
        });
      }

      if (checkers.isEmpty()) {
        logger.debug("all unresponsive members are already scheduled to be removed");
        return;
      }

      logger.debug("checking availability of these members: {}", checkers);
      ExecutorService svc =
          LoggingExecutors.newFixedThreadPool("Geode View Creator verification thread ",
              true, suspects.size());
      try {
        long giveUpTime = System.currentTimeMillis() + viewAckTimeout;
        // submit the tasks that will remove dead members from the suspects collection
        submitAll(svc, checkers);

        // now wait for the tasks to do their work
        long waitTime = giveUpTime - System.currentTimeMillis();
        synchronized (viewRequests) {
          while (waitTime > 0) {
            logger.debug("removeHealthyMembers: mbrs" + suspects.size());

            filterMembers(suspects, newRemovals, REMOVE_MEMBER_REQUEST);
            filterMembers(suspects, newLeaves, LEAVE_REQUEST_MESSAGE);
            newRemovals.removeAll(newLeaves);

            suspects.removeAll(newLeaves);

            if (suspects.isEmpty() || newRemovals.containsAll(suspects)) {
              break;
            }

            viewRequests.wait(waitTime);
            waitTime = giveUpTime - System.currentTimeMillis();
          }
        }
      } finally {
        svc.shutdownNow();
      }
    }

    /**
     * This gets pending requests and returns the IDs of any that are in the given collection
     *
     * @param mbrs collection of IDs to search for
     * @param matchingMembers collection to store matching IDs in
     * @param requestType leave/remove/join
     */
    protected void filterMembers(Collection<ID> mbrs,
        Set<ID> matchingMembers, short requestType) {
      Set<ID> requests = getPendingRequestIDs(requestType);

      if (!requests.isEmpty()) {
        logger.debug(
            "filterMembers: processing " + requests.size() + " requests for type " + requestType);
        Iterator<ID> itr = requests.iterator();
        while (itr.hasNext()) {
          ID memberID = itr.next();
          if (mbrs.contains(memberID)) {
            testFlagForRemovalRequest = true;
            matchingMembers.add(memberID);
          }
        }
      }
    }

    private <T> List<Future<T>> submitAll(ExecutorService executor,
        Collection<? extends Callable<T>> tasks) {
      List<Future<T>> result = new ArrayList<Future<T>>(tasks.size());

      for (Callable<T> task : tasks) {
        result.add(executor.submit(task));
      }

      return result;
    }

    boolean getTestFlagForRemovalRequest() {
      return testFlagForRemovalRequest;
    }

  }


  static class ViewAbandonedException extends Exception {
  }
}
