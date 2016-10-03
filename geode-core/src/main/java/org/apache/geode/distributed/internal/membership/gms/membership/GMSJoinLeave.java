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
package org.apache.geode.distributed.internal.membership.gms.membership;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.distributed.internal.membership.gms.ServiceConfig.*;
import static org.apache.geode.internal.DataSerializableFixedID.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.GMSUtil;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.interfaces.MessageHandler;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.messages.HasMemberID;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.NetworkPartitionMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.ViewAckMessage;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.logging.log4j.Logger;

/**
 * GMSJoinLeave handles membership communication with other processes in the
 * distributed system.  It replaces the JGroups channel membership services
 * that Geode formerly used for this purpose.
 */
public class GMSJoinLeave implements JoinLeave, MessageHandler {

  public static final String BYPASS_DISCOVERY_PROPERTY = DistributionConfig.GEMFIRE_PREFIX + "bypass-discovery";

  /**
   * amount of time to wait for responses to FindCoordinatorRequests
   */
  private static final int DISCOVERY_TIMEOUT = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "discovery-timeout", 3000);

  /**
   * amount of time to sleep before trying to join after a failed attempt
   */
  private static final int JOIN_RETRY_SLEEP = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "join-retry-sleep", 1000);

  /**
   * time to wait for a broadcast message to be transmitted by jgroups
   */
  private static final long BROADCAST_MESSAGE_SLEEP_TIME = Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "broadcast-message-sleep-time", 1000);

  /**
   * if the locators don't know who the coordinator is we send find-coord requests to this many nodes
   */
  private static final int MAX_DISCOVERY_NODES = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "max-discovery-nodes", 30);

  /**
   * interval for broadcasting the current view to members in case they didn't get it the first time
   */
  private static final long VIEW_BROADCAST_INTERVAL = Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "view-broadcast-interval", 60000);

  /**
   * membership logger
   */
  private static final Logger logger = Services.getLogger();

  /**
   * the view ID where I entered into membership
   */
  private int birthViewId;

  /**
   * my address
   */
  private InternalDistributedMember localAddress;

  private Services services;

  /**
   * have I connected to the distributed system?
   */
  private volatile boolean isJoined;

  /**
   * guarded by viewInstallationLock
   */
  private boolean isCoordinator;

  /**
   * a synch object that guards view installation
   */
  private final Object viewInstallationLock = new Object();

  /**
   * the currently installed view.  Guarded by viewInstallationLock
   */
  private volatile NetView currentView;

  /**
   * the previous view
   **/
  private volatile NetView previousView;

  /**
   * members who we have been declared dead in the current view
   */
  private final Set<InternalDistributedMember> removedMembers = new HashSet<>();

  /**
   * members who we've received a leave message from
   **/
  private final Set<InternalDistributedMember> leftMembers = new HashSet<>();

  /**
   * a new view being installed
   */
  private volatile NetView preparedView;

  /**
   * the last view that conflicted with view preparation
   */
  private NetView lastConflictingView;

  private List<InetSocketAddress> locators;

  /**
   * a list of join/leave/crashes
   */
  private final List<DistributionMessage> viewRequests = new LinkedList<DistributionMessage>();

  /**
   * the established request collection jitter.  This can be overridden for testing with delayViewCreationForTest
   */
  long requestCollectionInterval = MEMBER_REQUEST_COLLECTION_INTERVAL;

  /**
   * collects the response to a join request
   */
  private final JoinResponseMessage[] joinResponse = new JoinResponseMessage[1];

  /**
   * collects responses to new views
   */
  private ViewReplyProcessor viewProcessor = new ViewReplyProcessor(false);

  /**
   * collects responses to view preparation messages
   */
  private ViewReplyProcessor prepareProcessor = new ViewReplyProcessor(true);

  /**
   * whether quorum checks can cause a forced-disconnect
   */
  private boolean quorumRequired = false;

  /**
   * timeout in receiving view acknowledgement
   */
  private int viewAckTimeout;

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
  final SearchState searchState = new SearchState();

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
  NetView quorumLostView;

  /**
   * a flag to mark a coordinator's viewCreator for shutdown
   */
  private boolean markViewCreatorForShutdown = false;

  static class SearchState {
    Set<InternalDistributedMember> alreadyTried = new HashSet<>();
    Set<InternalDistributedMember> registrants = new HashSet<>();
    InternalDistributedMember possibleCoordinator;
    int viewId = -1;
    int locatorsContacted = 0;
    boolean hasContactedAJoinedLocator;
    NetView view;
    final Set<FindCoordinatorResponse> responses = new HashSet<>();

    void cleanup() {
      alreadyTried.clear();
      possibleCoordinator = null;
      view = null;
      synchronized (responses) {
        responses.clear();
      }
    }
  }

  Object getViewInstallationLock() {
    return viewInstallationLock;
  }

  /**
   * attempt to join the distributed system
   * loop
   * send a join request to a locator & get a response
   * <p>
   * If the response indicates there's no coordinator it
   * will contain a set of members that have recently contacted
   * it.  The "oldest" member is selected as the coordinator
   * based on ID sort order.
   *
   * @return true if successful, false if not
   */
  public boolean join() {

    try {
      if (Boolean.getBoolean(BYPASS_DISCOVERY_PROPERTY)) {
        synchronized (viewInstallationLock) {
          becomeCoordinator();
        }
        return true;
      }

      SearchState state = searchState;

      long locatorWaitTime = ((long) services.getConfig().getLocatorWaitTime()) * 1000L;
      long timeout = services.getConfig().getJoinTimeout();
      logger.debug("join timeout is set to {}", timeout);
      long retrySleep = JOIN_RETRY_SLEEP;
      long startTime = System.currentTimeMillis();
      long locatorGiveUpTime = startTime + locatorWaitTime;
      long giveupTime = startTime + timeout;

      for (int tries = 0; !this.isJoined && !this.isStopping; tries++) {
        logger.debug("searching for the membership coordinator");
        boolean found = findCoordinator();
        if (found) {
          logger.debug("found possible coordinator {}", state.possibleCoordinator);
          if (localAddress.getNetMember().preferredForCoordinator()
              && state.possibleCoordinator.equals(this.localAddress)) {
            if (tries > 2 || System.currentTimeMillis() < giveupTime) {
              synchronized (viewInstallationLock) {
                becomeCoordinator();
              }
              return true;
            }
          } else {
            if (attemptToJoin()) {
              return true;
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
            // reset the tries count and timer since we haven't actually tried to join yet
            tries = 0;
            giveupTime = now + timeout;
          } else if (now > giveupTime) {
            break;
          }
        }
        try {
          logger.debug("sleeping for {} before making another attempt to find the coordinator", retrySleep);
          Thread.sleep(retrySleep);
        } catch (InterruptedException e) {
          logger.debug("retry sleep interrupted - giving up on joining the distributed system");
          return false;
        }
      } // for

      if (!this.isJoined) {
        logger.debug("giving up attempting to join the distributed system after " + (System.currentTimeMillis() - startTime) + "ms");
      }

      // to preserve old behavior we need to throw a SystemConnectException if
      // unable to contact any of the locators
      if (!this.isJoined && state.hasContactedAJoinedLocator) {
        throw new SystemConnectException("Unable to join the distributed system in "
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
   * send a join request and wait for a reply.  Process the reply.
   * This may throw a SystemConnectException or an AuthenticationFailedException
   *
   * @return true if the attempt succeeded, false if it timed out
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WA_NOT_IN_LOOP")
  boolean attemptToJoin() {
    SearchState state = searchState;

    // send a join request to the coordinator and wait for a response
    InternalDistributedMember coord = state.possibleCoordinator;
    if (state.alreadyTried.contains(coord)) {
      logger.info("Probable coordinator is still {} - waiting for a join-response", coord);
    } else {
      logger.info("Attempting to join the distributed system through coordinator " + coord + " using address " + this.localAddress);
      int port = services.getHealthMonitor().getFailureDetectionPort();
      JoinRequestMessage req = new JoinRequestMessage(coord, this.localAddress, services.getAuthenticator().getCredentials(coord), port, 
          services.getMessenger().getRequestId());
      //services.getMessenger().send(req, state.view);
      services.getMessenger().send(req);
    }

    JoinResponseMessage response;
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

    logger.debug("received join response {}", response);
    joinResponse[0] = null;
    String failReason = response.getRejectionMessage();
    if (failReason != null) {
      if (failReason.contains("Rejecting the attempt of a member using an older version")
          || failReason.contains("15806")) {
        throw new SystemConnectException(failReason);
      }
      throw new GemFireSecurityException(failReason);
    }
    
    //there is no way we can rech here right now
    throw new RuntimeException("Join Request Failed with response " + joinResponse[0] );
  }

  private JoinResponseMessage waitForJoinResponse() throws InterruptedException {
    JoinResponseMessage response;
    synchronized (joinResponse) {
      if (joinResponse[0] == null && !isJoined) {
        // Note that if we give up waiting but a response is on
        // the way we will get the new view and join that way.
        // See installView()
        long timeout = Math.max(services.getConfig().getMemberTimeout(), services.getConfig().getJoinTimeout() / 5);
        joinResponse.wait(timeout);
      }
      response = joinResponse[0];
      
      if (response != null && response.getCurrentView() != null && !isJoined) {
        //reset joinResponse[0]
        joinResponse[0] = null;
        // we got view here that means either we have to wait for
        NetView v = response.getCurrentView();
        InternalDistributedMember coord = v.getCoordinator();
        if (searchState.alreadyTried.contains(coord)) {
          searchState.view = response.getCurrentView();
          // we already sent join request to it..so lets wait some more time here
          // assuming we got this response immediately, so wait for same timeout here..
          long timeout = Math.max(services.getConfig().getMemberTimeout(), services.getConfig().getJoinTimeout() / 5);
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
    return response;
  }

  @Override
  public boolean isMemberLeaving(DistributedMember mbr) {
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
   * process a join request from another member. If this is the coordinator
   * this method will enqueue the request for processing in another thread.
   * If this is not the coordinator but the coordinator is known, the message
   * is forwarded to the coordinator.
   *
   * @param incomingRequest the request to be processed
   */
  private void processJoinRequest(JoinRequestMessage incomingRequest) {

    logger.info("received join request from {}", incomingRequest.getMemberID());

    if (incomingRequest.getMemberID().getVersionObject().compareTo(Version.CURRENT) < 0) {
      logger.warn("detected an attempt to start a peer using an older version of the product {}", incomingRequest.getMemberID());
      JoinResponseMessage m = new JoinResponseMessage("Rejecting the attempt of a member using an older version", incomingRequest.getRequestId());
      m.setRecipient(incomingRequest.getMemberID());
      services.getMessenger().send(m);
      return;
    }
    Object creds = incomingRequest.getCredentials();
    String rejection;
    try {
      rejection = services.getAuthenticator().authenticate(incomingRequest.getMemberID(), (Properties)creds);
    } catch (Exception e) {
      rejection = e.getMessage();
    }
    if (rejection != null && rejection.length() > 0) {
      JoinResponseMessage m = new JoinResponseMessage(rejection, 0);
      m.setRecipient(incomingRequest.getMemberID());
      services.getMessenger().send(m);
      return;
    }

    //      Remove JoinResponseMessage to fix GEODE-870
//    if (!this.localAddress.getNetMember().preferredForCoordinator() &&
//        incomingRequest.getMemberID().getNetMember().preferredForCoordinator()) {
//      JoinResponseMessage joinResponseMessage = new JoinResponseMessage(incomingRequest.getMemberID(), currentView, true);
//      services.getMessenger().send(joinResponseMessage);
//      return;
//    }
    recordViewRequest(incomingRequest);
  }

  /**
   * Process a Leave request from another member. This may cause this member
   * to become the new membership coordinator. If this is the coordinator
   * a new view will be triggered.
   *
   * @param incomingRequest the request to be processed
   */
  private void processLeaveRequest(LeaveRequestMessage incomingRequest) {

    logger.info("received leave request from {} for {}", incomingRequest.getSender(), incomingRequest.getMemberID());

    NetView v = currentView;
    if (v == null) {
      recordViewRequest(incomingRequest);
      return;
    }

    InternalDistributedMember mbr = incomingRequest.getMemberID();

    if (logger.isDebugEnabled()) {
      logger.debug("JoinLeave.processLeaveRequest invoked.  isCoordinator=" + isCoordinator + "; isStopping=" + isStopping
          + "; cancelInProgress=" + services.getCancelCriterion().isCancelInProgress());
    }

    if (!v.contains(mbr) && mbr.getVmViewId() < v.getViewId()) {
      logger.debug("ignoring leave request from old member");
      return;
    }

    if (incomingRequest.getMemberID().equals(this.localAddress)) {
      logger.info("I am being told to leave the distributed system by {}", incomingRequest.getSender());
      forceDisconnect(incomingRequest.getReason());
      return;
    }

    if (!isCoordinator && !isStopping && !services.getCancelCriterion().isCancelInProgress()) {
      logger.debug("Checking to see if I should become coordinator");
      NetView check = new NetView(v, v.getViewId() + 1);
      check.remove(incomingRequest.getMemberID());
      synchronized (removedMembers) {
        check.removeAll(removedMembers);
        check.addCrashedMembers(removedMembers);
      }
      synchronized (leftMembers) {
        leftMembers.add(mbr);
        check.removeAll(leftMembers);
      }
      if (check.getCoordinator().equals(localAddress)) {
        synchronized (viewInstallationLock) {
          becomeCoordinator(incomingRequest.getMemberID());
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
   * Process a Remove request from another member. This may cause this member
   * to become the new membership coordinator. If this is the coordinator
   * a new view will be triggered.
   *
   * @param incomingRequest the request to process
   */
  private void processRemoveRequest(RemoveMemberMessage incomingRequest) {
    NetView v = currentView;
    boolean fromMe = incomingRequest.getSender() == null ||
        incomingRequest.getSender().equals(localAddress);

    InternalDistributedMember mbr = incomingRequest.getMemberID();

    if (v != null && !v.contains(incomingRequest.getSender())) {
      logger.info("Membership ignoring removal request for " + mbr + " from non-member " + incomingRequest.getSender());
      return;
    }

    if (v == null) {
      // not yet a member
      return;
    }

    if (!fromMe) {
      logger.info("Membership received a request to remove " + mbr
          + " from " + incomingRequest.getSender()
          + " reason=" + incomingRequest.getReason());
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
      NetView check = new NetView(v, v.getViewId() + 1);
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
    }
  }

  private void recordViewRequest(DistributionMessage request) {
    logger.debug("Recording the request to be processed in the next membership view");
    synchronized (viewRequests) {
      viewRequests.add(request);
      if (viewCreator != null) {
        boolean joinResponseSent = viewCreator.informToPendingJoinRequests();

        if (!joinResponseSent && request instanceof JoinRequestMessage) {
          JoinRequestMessage jreq = (JoinRequestMessage) request;
          // this will inform about cluster-secret key, as we have authenticated at this point
          JoinResponseMessage response = new JoinResponseMessage(jreq.getSender(), services.getMessenger().getClusterSecretKey(), jreq.getRequestId());
          services.getMessenger().send(response);
        }
      }
      viewRequests.notifyAll();
    }
    
  }
  
  // for testing purposes, returns a copy of the view requests for verification
  List<DistributionMessage> getViewRequests() {
    synchronized (viewRequests) {
      return new LinkedList<DistributionMessage>(viewRequests);
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
   * Test hook for delaying the creation of new views.
   * This should be invoked before this member becomes coordinator
   * and creates its ViewCreator thread.
   *
   * @param millis
   */
  public void delayViewCreationForTest(int millis) {
    requestCollectionInterval = millis;
  }

  /**
   * Transitions this member into the coordinator role.  This must
   * be invoked under a synch on viewInstallationLock that was held
   * at the time the decision was made to become coordinator so that
   * the decision is atomic with actually becoming coordinator.
   *
   * @param oldCoordinator may be null
   */
  private void becomeCoordinator(InternalDistributedMember oldCoordinator) {

    assert Thread.holdsLock(viewInstallationLock);

    if (isCoordinator) {
      return;
    }

    logger.info("This member is becoming the membership coordinator with address {}", localAddress);
    isCoordinator = true;
    if (currentView == null) {
      // create the initial membership view
      NetView newView = new NetView(this.localAddress);
      newView.setFailureDetectionPort(localAddress, services.getHealthMonitor().getFailureDetectionPort());
      this.localAddress.setVmViewId(0);
      installView(newView);
      isJoined = true;
      createAndStartViewCreator(newView);
      startViewBroadcaster();
    } else {
      // create and send out a new view
      NetView newView = addMemberToNetView(oldCoordinator);
      createAndStartViewCreator(newView);
      startViewBroadcaster();
    }
  }

  private void createAndStartViewCreator(NetView newView) {
    if (viewCreator == null || viewCreator.isShutdown()) {
      viewCreator = new ViewCreator("Geode Membership View Creator", Services.getThreadGroup());
      if (newView != null) {
        viewCreator.setInitialView(newView, newView.getNewMembers(), newView.getShutdownMembers(), newView.getCrashedMembers());
      }
      services.getMessenger().initClusterKey();
      viewCreator.setDaemon(true);
      logger.info("ViewCreator starting on:" + localAddress);
      viewCreator.start();
    }
  }

  private NetView addMemberToNetView(InternalDistributedMember oldCoordinator) {
    boolean testing = unitTesting.contains("noRandomViewChange");
    NetView newView;
    Set<InternalDistributedMember> leaving = new HashSet<>();
    Set<InternalDistributedMember> removals;
    synchronized (viewInstallationLock) {
      int rand = testing ? 0 : NetView.RANDOM.nextInt(10);
      int viewNumber = currentView.getViewId() + 5 + rand;
      if (this.localAddress.getVmViewId() < 0) {
        this.localAddress.setVmViewId(viewNumber);
      }
      List<InternalDistributedMember> mbrs = new ArrayList<>(currentView.getMembers());
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
      newView = new NetView(this.localAddress, viewNumber, mbrs, leaving,
          removals);
      newView.setFailureDetectionPorts(currentView);
      newView.setFailureDetectionPort(this.localAddress, services.getHealthMonitor().getFailureDetectionPort());
    }
    return newView;
  }

  private void sendJoinResponses(NetView newView, List<InternalDistributedMember> newMbrs) {
    for (InternalDistributedMember mbr : newMbrs) {
      JoinResponseMessage response = new JoinResponseMessage(mbr, newView, 0);
      services.getMessenger().send(response);
    }
  }

  private void sendRemoveMessages(List<InternalDistributedMember> removals, List<String> reasons, Set<InternalDistributedMember> oldIds) {
    Iterator<String> reason = reasons.iterator();
    for (InternalDistributedMember mbr : removals) {
      //if olds not contains mbr then send remove request 
      if (!oldIds.contains(mbr)) {
        RemoveMemberMessage response = new RemoveMemberMessage(mbr, mbr, reason.next());
        services.getMessenger().send(response);
      } else {
        reason.next();
      }
    }
  }

  boolean prepareView(NetView view, List<InternalDistributedMember> newMembers) throws InterruptedException {
    return sendView(view, true, this.prepareProcessor);
  }

  void sendView(NetView view, List<InternalDistributedMember> newMembers) throws InterruptedException {
    sendView(view, false, this.viewProcessor);
  }

  private boolean sendView(NetView view, boolean preparing, ViewReplyProcessor viewReplyProcessor) throws InterruptedException {

    int id = view.getViewId();
    InstallViewMessage msg = new InstallViewMessage(view, services.getAuthenticator().getCredentials(this.localAddress), preparing);
    Set<InternalDistributedMember> recips = new HashSet<>(view.getMembers());

    // a recent member was seen not to receive a new view - I think this is why
    // recips.removeAll(newMembers); // new members get the view in a JoinResponseMessage
    recips.remove(this.localAddress); // no need to send it to ourselves

    Set<InternalDistributedMember> responders = recips;
    if (!view.getCrashedMembers().isEmpty()) {
      recips = new HashSet<>(recips);
      recips.addAll(view.getCrashedMembers());
    }

    if (preparing) {
      this.preparedView = view;
    } else {
      //Added a check in the view processor to turn off the ViewCreator
      //if another server is the coordinator - GEODE-870
      if (isCoordinator && !localAddress.equals(view.getCoordinator()) && getViewCreator() != null) {
        markViewCreatorForShutdown = true;
        this.isCoordinator = false;
      }
      installView(view);
    }

    if (recips.isEmpty()) {
      if (!preparing) {
        logger.info("no recipients for new view aside from myself");
      }
      return true;
    }

    StringBuilder stringBuilder = new StringBuilder();
    int[] ports = view.getFailureDetectionPorts();
    int numMembers = view.size();
    for (int i = 0; i < numMembers; i++) {
      if (i > 0) {
        stringBuilder.append(' ');
      }
      stringBuilder.append(ports[i]);
    }
    logger.info((preparing ? "preparing" : "sending") + " new view " + view
        + "\nfailure detection ports: " + stringBuilder.toString());

    msg.setRecipients(recips);

    Set<InternalDistributedMember> pendingLeaves = getPendingRequestIDs(LEAVE_REQUEST_MESSAGE);
    Set<InternalDistributedMember> pendingRemovals = getPendingRequestIDs(REMOVE_MEMBER_REQUEST);
    pendingRemovals.removeAll(view.getCrashedMembers());
    viewReplyProcessor.initialize(id, responders);
    viewReplyProcessor.processPendingRequests(pendingLeaves, pendingRemovals);
    addPublicKeysToView(view);
    services.getMessenger().send(msg);

    // only wait for responses during preparation
    if (preparing) {
      logger.debug("waiting for view responses");

      Set<InternalDistributedMember> failedToRespond = viewReplyProcessor.waitForResponses();

      logger.info("finished waiting for responses to view preparation");

      InternalDistributedMember conflictingViewSender = viewReplyProcessor.getConflictingViewSender();
      NetView conflictingView = viewReplyProcessor.getConflictingView();
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

  private void addPublicKeysToView(NetView view) {
    String sDHAlgo = services.getConfig().getDistributionConfig().getSecurityUDPDHAlgo();
    if (sDHAlgo != null && !sDHAlgo.isEmpty()) {
      List<InternalDistributedMember> mbrs = view.getMembers();
      Iterator<InternalDistributedMember> itr = mbrs.iterator();

      while (itr.hasNext()) {
        InternalDistributedMember mbr = itr.next();
        byte[] pk = services.getMessenger().getPublicKey(mbr);
        view.setPublicKey(mbr, pk);
      }
    }
  }
  private void processViewMessage(final InstallViewMessage m) {

    NetView view = m.getView();
    
    //If our current view doesn't contaion sender then we wanrt to ignore that view.
    if(currentView != null && !currentView.contains(m.getSender())) {
      //but if preparedView contains sender then we don't want to ignore that view.
      //this may happen when we locator re-join and it take over coordinator's responsibility.
      if(this.preparedView == null || !this.preparedView.contains(m.getSender())) 
      { 
        logger.info("Ignoring the view {} from member {}, which is not in my current view {} ", view, m.getSender(), currentView);
        return;
      }
    }

    if (currentView != null && view.getViewId() < currentView.getViewId()) {
      // ignore old views
      ackView(m);
      return;
    }

    boolean viewContainsMyUnjoinedAddress = false;
    if (!this.isJoined) {
      // if we're still waiting for a join response and we're in this view we
      // should install the view so join() can finish its work
      for (InternalDistributedMember mbr : view.getMembers()) {
        if (localAddress.compareTo(mbr) == 0) {
          viewContainsMyUnjoinedAddress = true;
          break;
        }
      }
    }

    if (m.isPreparing()) {
      if (this.preparedView != null && this.preparedView.getViewId() >= view.getViewId()) {
        services.getMessenger().send(new ViewAckMessage(m.getSender(), this.preparedView));
      } else {
        this.preparedView = view;
        if (viewContainsMyUnjoinedAddress) {
          installView(view); // this will notifyAll the joinResponse
        }
        ackView(m);
      }
    } else { // !preparing
      if (isJoined && currentView != null && !view.contains(this.localAddress)) {
        logger.fatal("This member is no longer in the membership view.  My ID is {} and the new view is {}", localAddress, view);
        forceDisconnect("This node is no longer in the membership view");
      } else {
        if (isJoined || viewContainsMyUnjoinedAddress) {
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
    services.getManager().forceDisconnect(reason);
  }

  private void ackView(InstallViewMessage m) {
    if (!playingDead && m.getView().contains(m.getView().getCreator())) {
      services.getMessenger().send(new ViewAckMessage(m.getSender(), m.getView().getViewId(), m.isPreparing()));
    }
  }

  private void processViewAckMessage(ViewAckMessage m) {
    if (m.isPrepareAck()) {
      this.prepareProcessor.processViewResponse(m.getViewId(), m.getSender(), m.getAlternateView());
    } else {
      this.viewProcessor.processViewResponse(m.getViewId(), m.getSender(), m.getAlternateView());
    }
  }

  private TcpClientWrapper tcpClientWrapper = new TcpClientWrapper();

  /***
   * testing purpose.  Sets the TcpClient that is used by GMSJoinLeave
   * to communicate with Locators.
   *
   * @param tcpClientWrapper the wrapper
   */
  void setTcpClientWrapper(TcpClientWrapper tcpClientWrapper) {
    this.tcpClientWrapper = tcpClientWrapper;
  }

  /**
   * This contacts the locators to find out who the current coordinator is.
   * All locators are contacted. If they don't agree then we choose the oldest
   * coordinator and return it.
   */
  private boolean findCoordinator() {
    SearchState state = searchState;

    assert this.localAddress != null;

    // If we've already tried to bootstrap from locators that
    // haven't joined the system (e.g., a collocated locator)
    // then jump to using the membership view to try to find
    // the coordinator
    if (!state.hasContactedAJoinedLocator && state.view != null) {
      return findCoordinatorFromView();
    }

    String dhalgo = services.getConfig().getDistributionConfig().getSecurityUDPDHAlgo();
    FindCoordinatorRequest request = new FindCoordinatorRequest(this.localAddress, state.alreadyTried, state.viewId, 
        services.getMessenger().getPublicKey(localAddress), services.getMessenger().getRequestId(), dhalgo);
    Set<InternalDistributedMember> possibleCoordinators = new HashSet<InternalDistributedMember>();
    Set<InternalDistributedMember> coordinatorsWithView = new HashSet<InternalDistributedMember>();

    long giveUpTime = System.currentTimeMillis() + ((long) services.getConfig().getLocatorWaitTime() * 1000L);

    int connectTimeout = (int) services.getConfig().getMemberTimeout() * 2;
    boolean anyResponses = false;

    logger.debug("sending {} to {}", request, locators);

    state.hasContactedAJoinedLocator = false;
    state.locatorsContacted = 0;

    do {
      for (InetSocketAddress addr : locators) {
        try {
          Object o = tcpClientWrapper.sendCoordinatorFindRequest(addr, request, connectTimeout);
          FindCoordinatorResponse response = (o instanceof FindCoordinatorResponse) ? (FindCoordinatorResponse) o : null;
          if (response != null) {
            if(response.getRejectionMessage() != null ) {
              throw new GemFireConfigException(response.getRejectionMessage());
            }
            setCoordinatorPublicKey(response);
            state.locatorsContacted++;
            if (!state.hasContactedAJoinedLocator &&
                response.getSenderId() != null && response.getSenderId().getVmViewId() >= 0) {
              logger.debug("Locator's address indicates it is part of a distributed system "
                  + "so I will not become membership coordinator on this attempt to join");
              state.hasContactedAJoinedLocator = true;
            }
            if (response.getCoordinator() != null) {
              anyResponses = true;
              NetView v = response.getView();
              int viewId = v == null ? -1 : v.getViewId();
              if (viewId > state.viewId) {
                state.viewId = viewId;
                state.view = v;
                state.registrants.clear();
                if (response.getRegistrants() != null) {
                  state.registrants.addAll(response.getRegistrants());
                }
              }
              if (viewId > -1) {
                coordinatorsWithView.add(response.getCoordinator());
              }
              
              possibleCoordinators.add(response.getCoordinator());
            }
          }
        } catch (IOException | ClassNotFoundException problem) {
        }
      }
    } while (!anyResponses && System.currentTimeMillis() < giveUpTime);
    if (possibleCoordinators.isEmpty()) {
      return false;
    }

    if (coordinatorsWithView.size() > 0) {
      possibleCoordinators = coordinatorsWithView;// lets check current coordinators in view only
    }
    
    Iterator<InternalDistributedMember> it = possibleCoordinators.iterator();
    if (possibleCoordinators.size() == 1) {
      state.possibleCoordinator = it.next();
    } else {
      InternalDistributedMember oldest = it.next();
      while (it.hasNext()) {
        InternalDistributedMember candidate = it.next();
        if (oldest.compareTo(candidate) > 0) {
          oldest = candidate;
        }
      }
      state.possibleCoordinator = oldest;
    }
    InternalDistributedMember coord = null;
    boolean coordIsNoob = true;
    for (; it.hasNext(); ) {
      InternalDistributedMember mbr = it.next();
      if (!state.alreadyTried.contains(mbr)) {
        boolean mbrIsNoob = (mbr.getVmViewId() < 0);
        if (mbrIsNoob) {
          // member has not yet joined
          if (coordIsNoob && (coord == null || coord.compareTo(mbr) > 0)) {
            coord = mbr;
          }
        } else {
          // member has already joined
          if (coordIsNoob || mbr.getVmViewId() > coord.getVmViewId()) {
            coord = mbr;
            coordIsNoob = false;
          }
        }
      }
    }
    return true;
  }

  protected class TcpClientWrapper {
    protected Object sendCoordinatorFindRequest(InetSocketAddress addr, FindCoordinatorRequest request, int connectTimeout)
        throws ClassNotFoundException, IOException {
      TcpClient client = new TcpClient();
      return client.requestToServer(
          addr.getAddress(), addr.getPort(), request, connectTimeout,
          true);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WA_NOT_IN_LOOP")
  boolean findCoordinatorFromView() {
    ArrayList<FindCoordinatorResponse> result;
    SearchState state = searchState;
    NetView v = state.view;
    List<InternalDistributedMember> recipients = new ArrayList<>(v.getMembers());

    if (recipients.size() > MAX_DISCOVERY_NODES && MAX_DISCOVERY_NODES > 0) {
      recipients = recipients.subList(0, MAX_DISCOVERY_NODES);
    }
    if (state.registrants != null) {
      recipients.addAll(state.registrants);
    }
    recipients.remove(localAddress);    

   // FindCoordinatorRequest req = new FindCoordinatorRequest(localAddress, state.alreadyTried, state.viewId, services.getMessenger().getPublickey(
     //   localAddress), services.getMessenger().getRequestId());
    //req.setRecipients(v.getMembers());
    
    boolean testing = unitTesting.contains("findCoordinatorFromView");
    synchronized (state.responses) {
      if (!testing) {
        state.responses.clear();
      }
      
      String dhalgo = services.getConfig().getDistributionConfig().getSecurityUDPDHAlgo(); 
      if (!dhalgo.isEmpty()) {
        //Here we are sending message one-by-one to all recipients as we don't have cluster secret key yet.
        //Usually this happens when locator re-joins the cluster and it has saved view.
        for (InternalDistributedMember mbr : v.getMembers()) {
          Set<InternalDistributedMember> r = new HashSet<>();
          r.add(mbr);
          FindCoordinatorRequest req = new FindCoordinatorRequest(localAddress, state.alreadyTried, state.viewId, services.getMessenger().getPublicKey(
              localAddress), services.getMessenger().getRequestId(), dhalgo);
          req.setRecipients(r);

          services.getMessenger().send(req, v);
        }
      } else {
        FindCoordinatorRequest req = new FindCoordinatorRequest(localAddress, state.alreadyTried, state.viewId, services.getMessenger().getPublicKey(
            localAddress), services.getMessenger().getRequestId(), dhalgo);
        req.setRecipients(v.getMembers());

        services.getMessenger().send(req, v);
      }
      try {
        if (!testing) {
          state.responses.wait(DISCOVERY_TIMEOUT);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      result = new ArrayList<>(state.responses);
      state.responses.clear();
    }

    InternalDistributedMember coord = null;
    if (localAddress.getNetMember().preferredForCoordinator()) {
      // it's possible that all other potential coordinators are gone
      // and this new member must become the coordinator
      coord = localAddress;
    }
    boolean coordIsNoob = true;
    for (FindCoordinatorResponse resp : result) {
      InternalDistributedMember mbr = resp.getCoordinator();
      if (!state.alreadyTried.contains(mbr)) {
        boolean mbrIsNoob = (mbr.getVmViewId() < 0);
        if (mbrIsNoob) {
          // member has not yet joined
          if (coordIsNoob && (coord == null || coord.compareTo(mbr, false) > 0)) {
            coord = mbr;
          }
        } else {
          // member has already joined
          if (coordIsNoob || mbr.getVmViewId() > coord.getVmViewId()) {
            coord = mbr;
            coordIsNoob = false;
          }
        }
      }
    }

    state.possibleCoordinator = coord;
    return coord != null;
  }

  /**
   * receives a JoinResponse holding a membership view or rejection message
   *
   * @param rsp the response message to process
   */
  private void processJoinResponse(JoinResponseMessage rsp) {
    synchronized (joinResponse) {
      if (!this.isJoined) {
        //1. our joinRequest rejected.
        //2. Member which was coordinator but just now some other member became coordinator
        //3. we got message with secret key, but still view is coming and that will inform the joining thread
        if (rsp.getRejectionMessage() != null || rsp.getCurrentView() != null) {
          joinResponse[0] = rsp;
          joinResponse.notifyAll();
        } else {
          //we got secret key lets add it
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
  void setJoinResponseMessage(JoinResponseMessage jrm) {
    joinResponse[0] = jrm;
  }

  private void processFindCoordinatorRequest(FindCoordinatorRequest req) {
    FindCoordinatorResponse resp;
    if (this.isJoined) {
      NetView v = currentView;
      resp = new FindCoordinatorResponse(v.getCoordinator(), localAddress, 
          services.getMessenger().getPublicKey(v.getCoordinator()), req.getRequestId());
    } else {
      resp = new FindCoordinatorResponse(localAddress, localAddress, 
          services.getMessenger().getPublicKey(localAddress), req.getRequestId());
    }
    resp.setRecipient(req.getMemberID());
    services.getMessenger().send(resp);
  }

  private void processFindCoordinatorResponse(FindCoordinatorResponse resp) {
    synchronized (searchState.responses) {
      searchState.responses.add(resp);
      searchState.responses.notifyAll();
    }
    setCoordinatorPublicKey(resp);
  }
  
  private void setCoordinatorPublicKey(FindCoordinatorResponse response) {
    if (response.getCoordinator() != null && response.getCoordinatorPublicKey() != null)
      services.getMessenger().setPublicKey(response.getCoordinatorPublicKey(), response.getCoordinator());
  }

  private void processNetworkPartitionMessage(NetworkPartitionMessage msg) {
    String str = "Membership coordinator " + msg.getSender() + " has declared that a network partition has occurred";
    forceDisconnect(str);
  }

  @Override
  public NetView getView() {
    return currentView;
  }

  public NetView getPreviousView() {
    return previousView;
  }

  @Override
  public InternalDistributedMember getMemberID() {
    return this.localAddress;
  }

  public void installView(NetView newView) {

    synchronized (viewInstallationLock) {
      if (currentView != null && currentView.getViewId() >= newView.getViewId()) {
        // old view - ignore it
        return;
      }

      logger.info("received new view: {}\nold view is: {}", newView, currentView);

      if (currentView == null && !this.isJoined) {
        boolean found = false;
        for (InternalDistributedMember mbr : newView.getMembers()) {
          if (this.localAddress.equals(mbr)) {
            found = true;
            this.birthViewId = mbr.getVmViewId();
            this.localAddress.setVmViewId(this.birthViewId);
            GMSMember me = (GMSMember) this.localAddress.getNetMember();
            me.setBirthViewId(birthViewId);
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
          Set<InternalDistributedMember> crashes = newView.getActualCrashedMembers(currentView);
          forceDisconnect(LocalizedStrings.Network_partition_detected.toLocalizedString(crashes.size(), crashes));
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
        if (newView.shouldBeCoordinator(this.localAddress)) {
          becomeCoordinator();
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
          for (Iterator<DistributionMessage> it = viewRequests.iterator(); it.hasNext(); ) {
            DistributionMessage m = it.next();
            if (m instanceof JoinRequestMessage) {
              it.remove();
            } else if (m instanceof LeaveRequestMessage) {
              if (!currentView.contains(((LeaveRequestMessage) m).getMemberID())) {
                it.remove();
              }
            } else if (m instanceof RemoveMemberMessage) {
              if (!currentView.contains(((RemoveMemberMessage) m).getMemberID())) {
                it.remove();
              }
            }
          }
        }
      }
    }
    synchronized (removedMembers) {
      removeMembersFromCollectionIfNotInView(removedMembers, currentView);
    }
    synchronized (leftMembers) {
      removeMembersFromCollectionIfNotInView(leftMembers, currentView);
    }
  }

  private void removeMembersFromCollectionIfNotInView(Collection<InternalDistributedMember> members, NetView currentView) {
    Iterator<InternalDistributedMember> iterator = members.iterator();
    while (iterator.hasNext()) {
      if (!currentView.contains(iterator.next())) {
        iterator.remove();
      }
    }
  }

  /**
   * Sends a message declaring a network partition to the
   * members of the given view via Messenger
   *
   * @param view
   */
  void sendNetworkPartitionMessage(NetView view) {
    List<InternalDistributedMember> recipients = new ArrayList<>(view.getMembers());
    recipients.remove(localAddress);
    NetworkPartitionMessage msg = new NetworkPartitionMessage(recipients);
    try {
      services.getMessenger().send(msg);
    } catch (RuntimeException e) {
      logger.debug("unable to send network partition message - continuing", e);
    }
  }

  /**
   * returns true if this member thinks it is the membership coordinator
   * for the distributed system
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
  public NetView getPreparedView() {
    return this.preparedView;
  }

  /**
   * check to see if the new view shows a drop of 51% or more
   */
  private boolean isNetworkPartition(NetView newView, boolean logWeights) {
    if (currentView == null) {
      return false;
    }
    int oldWeight = currentView.memberWeight();
    int failedWeight = newView.getCrashedMemberWeight(currentView);
    if (failedWeight > 0 && logWeights) {
      if (logger.isInfoEnabled()
          && newView.getCreator().equals(localAddress)) { // view-creator logs this
        newView.logCrashedMemberWeights(currentView, logger);
      }
      int failurePoint = (int) (Math.round(51.0 * oldWeight) / 100.0);
      if (failedWeight > failurePoint && quorumLostView != newView) {
        quorumLostView = newView;
        logger.warn("total weight lost in this view change is {} of {}.  Quorum has been lost!", failedWeight, oldWeight);
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
    }
  }

  private void startViewBroadcaster() {
    services.getTimer().schedule(new ViewBroadcaster(), VIEW_BROADCAST_INTERVAL, VIEW_BROADCAST_INTERVAL);
  }

  public static void loadEmergencyClasses() {
  }

  @Override
  public void emergencyClose() {
    isStopping = true;
    isJoined = false;
    stopCoordinatorServices();
    isCoordinator = false;
  }

  public void beSick() {
  }

  public void playDead() {
    playingDead = true;
  }

  public void beHealthy() {
    playingDead = false;
  }

  @Override
  public void start() {
  }

  @Override
  public void started() {
    this.localAddress = services.getMessenger().getMemberID();
    GMSMember mbr = (GMSMember)this.localAddress.getNetMember();

    if (services.getConfig().areLocatorsPreferredAsCoordinators()) {
      boolean preferred = false;
      if (services.getLocator() != null
              || Locator.hasLocator()
              || !services.getConfig().getDistributionConfig().getStartLocator().isEmpty()
              || localAddress.getVmKind() == DistributionManager.LOCATOR_DM_TYPE) {
        logger.info("This member is hosting a locator will be preferred as a membership coordinator");
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
  public void stopped() {
  }

  @Override
  public void memberSuspected(InternalDistributedMember initiator, InternalDistributedMember suspect, String reason) {
    prepareProcessor.memberSuspected(suspect);
    viewProcessor.memberSuspected(suspect);
  }

  @Override
  public void leave() {
    synchronized (viewInstallationLock) {
      NetView view = currentView;
      isStopping = true;
      stopCoordinatorServices();
      if (view != null) {
        if (view.size() > 1) {
          List<InternalDistributedMember> coords = view.getPreferredCoordinators(Collections.emptySet(), localAddress, 5);
          logger.debug("Sending my leave request to {}", coords);
          LeaveRequestMessage m = new LeaveRequestMessage(coords, this.localAddress, "this member is shutting down");
          services.getMessenger().send(m);
        } // view.size
      } // view != null
    }
  }

  @Override
  public void remove(InternalDistributedMember m, String reason) {
    NetView v = this.currentView;

    services.getCancelCriterion().checkCancelInProgress(null);

    if (v != null && v.contains(m)) {
      Set<InternalDistributedMember> filter = new HashSet<>();
      filter.add(m);
      RemoveMemberMessage msg = new RemoveMemberMessage(v.getPreferredCoordinators(filter, getMemberID(), 5), m, reason);
      msg.setSender(this.localAddress);
      processRemoveRequest(msg);
      if (!this.isCoordinator) {
        msg.resetRecipients();
        msg.setRecipients(v.getPreferredCoordinators(Collections.emptySet(), localAddress, 10));
        services.getMessenger().send(msg);
      }
    } else {
      RemoveMemberMessage msg = new RemoveMemberMessage(m, m, reason);
      services.getMessenger().send(msg);
    }
  }

  @Override
  public void memberShutdown(DistributedMember mbr, String reason) {
    LeaveRequestMessage msg = new LeaveRequestMessage(Collections.singleton(this.localAddress), (InternalDistributedMember) mbr, reason);
    msg.setSender((InternalDistributedMember) mbr);
    processLeaveRequest(msg);
  }

  @Override
  public void disableDisconnectOnQuorumLossForTesting() {
    this.quorumRequired = false;
  }

  @Override
  public void init(Services s) {
    this.services = s;

    DistributionConfig dc = services.getConfig().getDistributionConfig();
    if (dc.getMcastPort() != 0 && dc.getLocators().trim().isEmpty() && dc.getStartLocator().trim().isEmpty()) {
      throw new GemFireConfigException(
          "Multicast cannot be configured for a non-distributed cache." + "  Please configure the locator services for this cache using "
              + LOCATORS + " or " + START_LOCATOR + ".");
    }

    services.getMessenger().addHandler(JoinRequestMessage.class, this);
    services.getMessenger().addHandler(JoinResponseMessage.class, this);
    services.getMessenger().addHandler(InstallViewMessage.class, this);
    services.getMessenger().addHandler(ViewAckMessage.class, this);
    services.getMessenger().addHandler(LeaveRequestMessage.class, this);
    services.getMessenger().addHandler(RemoveMemberMessage.class, this);
    services.getMessenger().addHandler(JoinRequestMessage.class, this);
    services.getMessenger().addHandler(JoinResponseMessage.class, this);
    services.getMessenger().addHandler(FindCoordinatorRequest.class, this);
    services.getMessenger().addHandler(FindCoordinatorResponse.class, this);
    services.getMessenger().addHandler(NetworkPartitionMessage.class, this);

    int ackCollectionTimeout = dc.getMemberTimeout() * 2 * 12437 / 10000;
    if (ackCollectionTimeout < 1500) {
      ackCollectionTimeout = 1500;
    } else if (ackCollectionTimeout > 12437) {
      ackCollectionTimeout = 12437;
    }
    ackCollectionTimeout = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "VIEW_ACK_TIMEOUT", ackCollectionTimeout).intValue();
    this.viewAckTimeout = ackCollectionTimeout;

    this.quorumRequired = services.getConfig().getDistributionConfig().getEnableNetworkPartitionDetection();

    DistributionConfig dconfig = services.getConfig().getDistributionConfig();
    String bindAddr = dconfig.getBindAddress();
    locators = GMSUtil.parseLocators(dconfig.getLocators(), bindAddr);
  }

  @Override
  public void processMessage(DistributionMessage m) {
    if (isStopping) {
      return;
    }
    logger.debug("processing {}", m);
    switch (m.getDSFID()) {
    case JOIN_REQUEST:
      assert m instanceof JoinRequestMessage;
      processJoinRequest((JoinRequestMessage) m);
      break;
    case JOIN_RESPONSE:
      assert m instanceof JoinResponseMessage;
      processJoinResponse((JoinResponseMessage) m);
      break;
    case INSTALL_VIEW_MESSAGE:
      assert m instanceof InstallViewMessage;
      processViewMessage((InstallViewMessage) m);
      break;
    case VIEW_ACK_MESSAGE:
      assert m instanceof ViewAckMessage;
      processViewAckMessage((ViewAckMessage) m);
      break;
    case LEAVE_REQUEST_MESSAGE:
      assert m instanceof LeaveRequestMessage;
      processLeaveRequest((LeaveRequestMessage) m);
      break;
    case REMOVE_MEMBER_REQUEST:
      assert m instanceof RemoveMemberMessage;
      processRemoveRequest((RemoveMemberMessage) m);
      break;
    case FIND_COORDINATOR_REQ:
      assert m instanceof FindCoordinatorRequest;
      processFindCoordinatorRequest((FindCoordinatorRequest) m);
      break;
    case FIND_COORDINATOR_RESP:
      assert m instanceof FindCoordinatorResponse;
      processFindCoordinatorResponse((FindCoordinatorResponse) m);
      break;
    case NETWORK_PARTITION_MESSAGE:
      assert m instanceof NetworkPartitionMessage;
      processNetworkPartitionMessage((NetworkPartitionMessage) m);
      break;
    default:
      throw new IllegalArgumentException("unknown message type: " + m);
    }
  }

  /**
   * returns the member IDs of the pending requests having the given
   * DataSerializableFixedID
   */
  Set<InternalDistributedMember> getPendingRequestIDs(int theDSFID) {
    Set<InternalDistributedMember> result = new HashSet<>();
    synchronized (viewRequests) {
      for (DistributionMessage msg : viewRequests) {
        if (msg.getDSFID() == theDSFID) {
          result.add(((HasMemberID) msg).getMemberID());
        }
      }
    }
    return result;
  }

  /***
   * test method
   *
   * @return ViewReplyProcessor
   */
  protected ViewReplyProcessor getPrepareViewReplyProcessor() {
    return prepareProcessor;
  }

  protected boolean testPrepareProcessorWaiting() {
    return prepareProcessor.isWaiting();
  }

  class ViewReplyProcessor {
    volatile int viewId = -1;
    final Set<InternalDistributedMember> notRepliedYet = new HashSet<>();
    NetView conflictingView;
    InternalDistributedMember conflictingViewSender;
    volatile boolean waiting;
    final boolean isPrepareViewProcessor;
    final Set<InternalDistributedMember> pendingRemovals = new HashSet<>();

    ViewReplyProcessor(boolean forPreparation) {
      this.isPrepareViewProcessor = forPreparation;
    }

    synchronized void initialize(int viewId, Set<InternalDistributedMember> recips) {
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

    synchronized void processPendingRequests(Set<InternalDistributedMember> pendingLeaves, Set<InternalDistributedMember> pendingRemovals) {
      // there's no point in waiting for members who have already
      // requested to leave or who have been declared crashed.
      // We don't want to mix the two because pending removals
      // aren't reflected as having crashed in the current view
      // and need to cause a new view to be generated
      for (InternalDistributedMember mbr : pendingLeaves) {
        notRepliedYet.remove(mbr);
      }
      for (InternalDistributedMember mbr : pendingRemovals) {
        if (this.notRepliedYet.contains(mbr)) {
          this.pendingRemovals.add(mbr);
        }
      }
    }

    synchronized void memberSuspected(InternalDistributedMember suspect) {
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

    synchronized void processLeaveRequest(InternalDistributedMember mbr) {
      if (waiting) {
        logger.debug("view response processor recording leave request for {}", mbr);
        stopWaitingFor(mbr);
      }
    }

    synchronized void processRemoveRequest(InternalDistributedMember mbr) {
      if (waiting) {
        logger.debug("view response processor recording remove request for {}", mbr);
        pendingRemovals.add(mbr);
        checkIfDone();
      }
    }

    synchronized void processViewResponse(int viewId, InternalDistributedMember sender, NetView conflictingView) {
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
    private void stopWaitingFor(InternalDistributedMember mbr) {
      notRepliedYet.remove(mbr);
      checkIfDone();
    }

    /**
     * call with synchronized(this)
     */
    private void checkIfDone() {
      if (notRepliedYet.isEmpty() || (pendingRemovals != null && pendingRemovals.containsAll(notRepliedYet))) {
        logger.debug("All anticipated view responses received - notifying waiting thread");
        waiting = false;
        notifyAll();
      } else {
        logger.debug("Still waiting for these view replies: {}", notRepliedYet);
      }
    }

    Set<InternalDistributedMember> waitForResponses() throws InterruptedException {
      Set<InternalDistributedMember> result;
      long endOfWait = System.currentTimeMillis() + viewAckTimeout;
      try {
        while (System.currentTimeMillis() < endOfWait && (!services.getCancelCriterion().isCancelInProgress())) {
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

    NetView getConflictingView() {
      return this.conflictingView;
    }

    InternalDistributedMember getConflictingViewSender() {
      return this.conflictingViewSender;
    }

    synchronized Set<InternalDistributedMember> getUnresponsiveMembers() {
      return new HashSet<>(this.notRepliedYet);
    }
  }

  /**
   * ViewBroadcaster periodically sends the current view to all
   * current and departed members.  This ensures that a member that
   * missed the view will eventually see it and act on it.
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
      NetView v = currentView;
      if (v != null) {
        InstallViewMessage msg = new InstallViewMessage(v, services.getAuthenticator().getCredentials(localAddress), false);
        Collection<InternalDistributedMember> recips = new ArrayList<>(v.size() + v.getCrashedMembers().size());
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

  class ViewCreator extends Thread {
    volatile boolean shutdown = false;
    volatile boolean waiting = false;
    volatile boolean testFlagForRemovalRequest = false;

    /**
     * initial view to install.  guarded by synch on ViewCreator
     */
    NetView initialView;
    /**
     * initial joining members.  guarded by synch on ViewCreator
     */
    List<InternalDistributedMember> initialJoins = Collections.emptyList();
    /**
     * initial leaving members  guarded by synch on ViewCreator
     */
    Set<InternalDistributedMember> initialLeaving;
    /**
     * initial crashed members.  guarded by synch on ViewCreator
     */
    Set<InternalDistributedMember> initialRemovals;

    ViewCreator(String name, ThreadGroup tg) {
      super(tg, name);
    }

    void shutdown() {
      shutdown = true;
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

    /**
     * All views should be sent by the ViewCreator thread, so
     * if this member becomes coordinator it may have an initial
     * view to transmit that announces the removal of the former coordinator to
     *
     * @param newView
     * @param leaving  - members leaving in this view
     * @param removals - members crashed in this view
     */
    synchronized void setInitialView(NetView newView, List<InternalDistributedMember> newMembers,
        Set<InternalDistributedMember> leaving, Set<InternalDistributedMember> removals) {
      this.initialView = newView;
      this.initialJoins = newMembers;
      this.initialLeaving = leaving;
      this.initialRemovals = removals;
    }

    private void sendInitialView() {
      try {
        if (initialView == null) {
          return;
        }
        NetView v = preparedView;
        if (v != null) {
          processPreparedView(v);
        }
        try {
          NetView iView;
          List<InternalDistributedMember> iJoins;
          Set<InternalDistributedMember> iLeaves;
          Set<InternalDistributedMember> iRemoves;
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
      } catch (InterruptedException e) {
        shutdown = true;
      }
    }

    /**
     * During initial view processing a prepared view was discovered.
     * This method will extract its new members and create a new
     * initial view containing them.
     *
     * @param v The prepared view
     */
    private void processPreparedView(NetView v) {
      assert initialView != null;
      if (currentView == null || currentView.getViewId() < v.getViewId()) {
        // we have a prepared view that is newer than the current view
        // form a new View ID
        int viewId = Math.max(initialView.getViewId(), v.getViewId());
        viewId += 1;
        NetView newView = new NetView(initialView, viewId);

        // add the new members from the prepared view to the new view,
        // preserving their failure-detection ports
        List<InternalDistributedMember> newMembers;
        if (currentView != null) {
          newMembers = v.getNewMembers(currentView);
        } else {
          newMembers = v.getMembers();
        }
        for (InternalDistributedMember newMember : newMembers) {
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
      List<DistributionMessage> requests = null;
      logger.info("View Creator thread is starting");
      sendInitialView();
      long okayToCreateView = System.currentTimeMillis() + requestCollectionInterval;
      try {
        for (; ; ) {
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
              if (System.currentTimeMillis() < okayToCreateView) {
                // sleep to let more requests arrive
                try {
                  viewRequests.wait(100);
                  continue;
                } catch (InterruptedException e) {
                  return;
                }
              } else {
                // time to create a new membership view
                if (requests == null) {
                  requests = new ArrayList<DistributionMessage>(viewRequests);
                } else {
                  requests.addAll(viewRequests);
                }
                viewRequests.clear();
                okayToCreateView = System.currentTimeMillis() + requestCollectionInterval;
              }
            }
          } // synchronized
          if (requests != null && !requests.isEmpty()) {
            logger.info("View Creator is processing {} requests for the next membership view", requests.size());
            try {
              createAndSendView(requests);
              if (shutdown) {
                return;
              }
            } catch (DistributedSystemDisconnectedException e) {
              shutdown = true;
            } catch (InterruptedException e) {
              shutdown = true;
            }
            requests = null;
          }
        }
      } finally {
        shutdown = true;
        informToPendingJoinRequests();
      }
    }
    
    synchronized boolean informToPendingJoinRequests() {
      boolean joinResponseSent = false;
      if (!shutdown) {
        return joinResponseSent;
      }
      ArrayList<DistributionMessage> requests = new ArrayList<>();
      synchronized (viewRequests) {
        if (viewRequests.size() > 0) {
          requests.addAll(viewRequests);
        } else {
          return joinResponseSent;
        }
        viewRequests.clear();
      }

      NetView v = currentView;
      for (DistributionMessage msg : requests) {
        switch (msg.getDSFID()) {
        case JOIN_REQUEST:               
          logger.debug("Informing to pending join requests {} myid {} coord {}", msg, localAddress, v.getCoordinator());
          if (!v.getCoordinator().equals(localAddress)) {
            joinResponseSent = true;
            //lets inform that coordinator has been changed
            JoinResponseMessage jrm = new JoinResponseMessage(((JoinRequestMessage) msg).getMemberID(), v, ((JoinRequestMessage) msg).getRequestId());
            services.getMessenger().send(jrm);
          }
        default:
          break;
        }
      }
      
      return joinResponseSent;
    }

    /**
     * Create a new membership view and send it to members (including crashed members).
     * Returns false if the view cannot be prepared successfully, true otherwise
     * @throws InterruptedException 
     */
    void createAndSendView(List<DistributionMessage> requests) throws InterruptedException {
      List<InternalDistributedMember> joinReqs = new ArrayList<>(10);
      Map<InternalDistributedMember, Integer> joinPorts = new HashMap<>(10);
      Set<InternalDistributedMember> leaveReqs = new HashSet<>(10);
      List<InternalDistributedMember> removalReqs = new ArrayList<>(10);
      List<String> removalReasons = new ArrayList<String>(10);

      NetView oldView = currentView;
      List<InternalDistributedMember> oldMembers;
      if (oldView != null) {
        oldMembers = new ArrayList<>(oldView.getMembers());
      } else {
        oldMembers = Collections.emptyList();
      }
      Set<InternalDistributedMember> oldIDs = new HashSet<>();

      for (DistributionMessage msg : requests) {
        logger.debug("processing request {}", msg);

        InternalDistributedMember mbr;
        switch (msg.getDSFID()) {
        case JOIN_REQUEST:
          JoinRequestMessage jmsg = (JoinRequestMessage) msg;
          mbr = jmsg.getMemberID();
          int port = jmsg.getFailureDetectionPort();
          // see if an old member ID is being reused. If
          // so we'll remove it from the new view
          for (InternalDistributedMember m : oldMembers) {
            if (mbr.compareTo(m, false) == 0) {
              oldIDs.add(m);
              break;
            }
          }
          if (!joinReqs.contains(mbr)) {
            joinReqs.add(mbr);
            joinPorts.put(mbr, port);
          }
          break;
        case LEAVE_REQUEST_MESSAGE:
          mbr = ((LeaveRequestMessage) msg).getMemberID();
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

      for (DistributionMessage msg : requests) {
        switch (msg.getDSFID()) {
        case REMOVE_MEMBER_REQUEST:
          InternalDistributedMember mbr = ((RemoveMemberMessage) msg).getMemberID();
          if (!leaveReqs.contains(mbr)) {
            if (oldMembers.contains(mbr) && !removalReqs.contains(mbr)) {
              removalReqs.add(mbr);
              removalReasons.add(((RemoveMemberMessage) msg).getReason());
            } else {
              sendRemoveMessages(Collections.singletonList(mbr),
                  Collections.singletonList(((RemoveMemberMessage) msg).getReason()), new HashSet<InternalDistributedMember>());
            }
          }
          break;
        default:
          break;
        }
      }

      for (InternalDistributedMember mbr : oldIDs) {
        if (!leaveReqs.contains(mbr) && !removalReqs.contains(mbr)) {
          removalReqs.add(mbr);
          removalReasons.add("Removal of old ID that has been reused");
        }
      }

      if (removalReqs.isEmpty() && leaveReqs.isEmpty() && joinReqs.isEmpty()) {
        return;
      }

      NetView newView;
      synchronized (viewInstallationLock) {
        int viewNumber = 0;
        List<InternalDistributedMember> mbrs;
        if (currentView == null) {
          mbrs = new ArrayList<InternalDistributedMember>();
        } else {
          viewNumber = currentView.getViewId() + 1;
          mbrs = new ArrayList<InternalDistributedMember>(oldMembers);
        }
        mbrs.removeAll(leaveReqs);
        mbrs.removeAll(removalReqs);
        // add joinReqs after removing old members because an ID may
        // be reused in an auto-reconnect and get a new vmViewID
        mbrs.addAll(joinReqs);
        newView = new NetView(localAddress, viewNumber, mbrs, leaveReqs, new HashSet<InternalDistributedMember>(removalReqs));
        for (InternalDistributedMember mbr : joinReqs) {
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

      for (InternalDistributedMember mbr : joinReqs) {
        mbr.setVmViewId(newView.getViewId());
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
     * @throws InterruptedException 
     */
    void prepareAndSendView(NetView newView, List<InternalDistributedMember> joinReqs, Set<InternalDistributedMember> leaveReqs,
        Set<InternalDistributedMember> removalReqs) throws InterruptedException {
      boolean prepared;
      do {
        if (this.shutdown || Thread.currentThread().isInterrupted()) {
          return;
        }

        if (quorumRequired && isNetworkPartition(newView, true)) {
          sendNetworkPartitionMessage(newView);
          Thread.sleep(BROADCAST_MESSAGE_SLEEP_TIME);
          
          Set<InternalDistributedMember> crashes = newView.getActualCrashedMembers(currentView);
          forceDisconnect(LocalizedStrings.Network_partition_detected.toLocalizedString(crashes.size(), crashes));
          shutdown = true;
          return;
        }

        prepared = prepareView(newView, joinReqs);
        logger.debug("view preparation phase completed.  prepared={}", prepared);

        NetView conflictingView = prepareProcessor.getConflictingView();

        if (prepared) {
          break;
        }

        Set<InternalDistributedMember> unresponsive = prepareProcessor.getUnresponsiveMembers();
        unresponsive.removeAll(removalReqs);
        unresponsive.removeAll(leaveReqs);
        if (!unresponsive.isEmpty()) {
          removeHealthyMembers(unresponsive);
          synchronized (viewRequests) {
            //now lets get copy of it in viewRequests sync, as other thread might be accessing it 
            unresponsive = new HashSet<>(unresponsive);
          }
        }

        logger.debug("unresponsive members that could not be reached: {}", unresponsive);
        List<InternalDistributedMember> failures = new ArrayList<>(currentView.getCrashedMembers().size() + unresponsive.size());

        if (conflictingView != null && !conflictingView.getCreator().equals(localAddress) && conflictingView.getViewId() > newView.getViewId()
            && (lastConflictingView == null || conflictingView.getViewId() > lastConflictingView.getViewId())) {
          lastConflictingView = conflictingView;
          logger.info("adding these crashed members from a conflicting view to the crash-set for the next view: {}\nconflicting view: {}", unresponsive,
              conflictingView);
          failures.addAll(conflictingView.getCrashedMembers());
          // this member may have been kicked out of the conflicting view
          if (failures.contains(localAddress)) {
            forceDisconnect("I am no longer a member of the distributed system");
            shutdown = true;
            return;
          }
          List<InternalDistributedMember> newMembers = conflictingView.getNewMembers();
          if (!newMembers.isEmpty()) {
            logger.info("adding these new members from a conflicting view to the new view: {}", newMembers);
            for (InternalDistributedMember mbr : newMembers) {
              int port = conflictingView.getFailureDetectionPort(mbr);
              newView.add(mbr);
              newView.setFailureDetectionPort(mbr, port);
              joinReqs.add(mbr);
            }
          }
          // trump the view ID of the conflicting view so mine will be accepted
          if (conflictingView.getViewId() >= newView.getViewId()) {
            newView = new NetView(newView, conflictingView.getViewId() + 1);
          }
        }

        if (!unresponsive.isEmpty()) {
          logger.info("adding these unresponsive members to the crash-set for the next view: {}", unresponsive);
          failures.addAll(unresponsive);
        }

        failures.removeAll(removalReqs);
        failures.removeAll(leaveReqs);
        prepared = failures.isEmpty();
        if (!prepared) {
          // abort the current view and try again
          removalReqs.addAll(failures);
          List<InternalDistributedMember> newMembers = new ArrayList<>(newView.getMembers());
          newMembers.removeAll(removalReqs);
          NetView tempView = new NetView(localAddress, newView.getViewId() + 1, newMembers, leaveReqs, removalReqs);
          for (InternalDistributedMember mbr : newView.getMembers()) {
            if (tempView.contains(mbr)) {
              tempView.setFailureDetectionPort(mbr, newView.getFailureDetectionPort(mbr));
            }
          }
          newView = tempView;
          int size = failures.size();
          List<String> reasons = new ArrayList<>(size);
          for (int i = 0; i < size; i++) {
            reasons.add("Failed to acknowledge a new membership view and then failed tcp/ip connection attempt");
          }
          sendRemoveMessages(failures, reasons, new HashSet<InternalDistributedMember>());
        }

        // if there is no conflicting view then we can count
        // the current state as being prepared.  All members
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
        shutdown = true;
      }

      // after sending a final view we need to stop this thread if
      // the GMS is shutting down
      if (isStopping()) {
        shutdown = true;
      }
    }

    /**
     * performs health checks on the collection of members, removing any that
     * are found to be healthy
     *
     * @param suspects
     */
    private void removeHealthyMembers(final Set<InternalDistributedMember> suspects) throws InterruptedException {
      List<Callable<InternalDistributedMember>> checkers = new ArrayList<Callable<InternalDistributedMember>>(suspects.size());

      Set<InternalDistributedMember> newRemovals = new HashSet<>();
      Set<InternalDistributedMember> newLeaves = new HashSet<>();

      filterMembers(suspects, newRemovals, REMOVE_MEMBER_REQUEST);
      filterMembers(suspects, newLeaves, LEAVE_REQUEST_MESSAGE);
      newRemovals.removeAll(newLeaves);  // if we received a Leave req the member is "healthy"

      suspects.removeAll(newLeaves);

      for (InternalDistributedMember mbr : suspects) {
        if (newRemovals.contains(mbr) || newLeaves.contains(mbr)) {
          continue; // no need to check this member - it's already been checked or is leaving
        }
        checkers.add(new Callable<InternalDistributedMember>() {
          @Override
          public InternalDistributedMember call() throws Exception {
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
      ExecutorService svc = Executors.newFixedThreadPool(suspects.size(), new ThreadFactory() {
        AtomicInteger i = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
          return new Thread(Services.getThreadGroup(), r,
              "Geode View Creator verification thread " + i.incrementAndGet());
        }
      });

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
     * @param mbrs            collection of IDs to search for
     * @param matchingMembers collection to store matching IDs in
     * @param requestType     leave/remove/join
     */
    protected void filterMembers(Collection<InternalDistributedMember> mbrs, Set<InternalDistributedMember> matchingMembers, short requestType) {
      Set<InternalDistributedMember> requests = getPendingRequestIDs(requestType);

      if (!requests.isEmpty()) {
        logger.debug("filterMembers: processing " + requests.size() + " requests for type " + requestType);
        Iterator<InternalDistributedMember> itr = requests.iterator();
        while (itr.hasNext()) {
          InternalDistributedMember memberID = itr.next();
          if (mbrs.contains(memberID)) {
            testFlagForRemovalRequest = true;
            matchingMembers.add(memberID);
          }
        }
      }
    }

    private <T> List<Future<T>> submitAll(ExecutorService executor, Collection<? extends Callable<T>> tasks) {
      List<Future<T>> result = new ArrayList<Future<T>>(tasks.size());

      for (Callable<T> task : tasks) {
        result.add(executor.submit(task));
      }

      return result;
    }

    boolean getTestFlageForRemovalRequest() {
      return testFlagForRemovalRequest;
    }
  }

  boolean checkIfAvailable(InternalDistributedMember fmbr) {
    // return the member id if it fails health checks
    logger.info("checking state of member " + fmbr);
    if (services.getHealthMonitor().checkIfAvailable(fmbr, "Member failed to acknowledge a membership view", false)) {
      logger.info("member " + fmbr + " passed availability check");
      return true;
    }
    logger.info("member " + fmbr + " failed availability check");
    return false;
  }
  
  private InternalDistributedMember getMemId(NetMember jgId, List<InternalDistributedMember> members) {
    for (InternalDistributedMember m : members) {
      if (((GMSMember) m.getNetMember()).equals(jgId)) {
        return m;
      }
    }
    return null;
  }

  @Override
  public InternalDistributedMember getMemberID(NetMember jgId) {
    NetView v = currentView;
    InternalDistributedMember ret = null;
    if (v != null) {
      ret = getMemId(jgId, v.getMembers());
    }

    if (ret == null) {
      v = preparedView;
      if (v != null) {
        ret = getMemId(jgId, v.getMembers());
      }
    }

    if (ret == null) {
      return new InternalDistributedMember(jgId);
    }
    
    return ret;
  }
}
