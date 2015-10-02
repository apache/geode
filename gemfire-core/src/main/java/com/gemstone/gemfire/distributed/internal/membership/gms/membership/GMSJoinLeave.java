package com.gemstone.gemfire.distributed.internal.membership.gms.membership;

import static com.gemstone.gemfire.internal.DataSerializableFixedID.INSTALL_VIEW_MESSAGE;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.JOIN_REQUEST;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.JOIN_RESPONSE;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.LEAVE_REQUEST_MESSAGE;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.REMOVE_MEMBER_REQUEST;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.VIEW_ACK_MESSAGE;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.FIND_COORDINATOR_REQ;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.FIND_COORDINATOR_RESP;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.NETWORK_PARTITION_MESSAGE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSUtil;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.JoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.MessageHandler;
import com.gemstone.gemfire.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import com.gemstone.gemfire.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.HasMemberID;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.InstallViewMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.JoinRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.JoinResponseMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.NetworkPartitionMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.ViewAckMessage;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.security.AuthenticationFailedException;

/**
 * GMSJoinLeave handles membership communication with other processes in the
 * distributed system.  It replaces the JGroups channel membership services
 * that Geode formerly used for this purpose.
 * 
 */
public class GMSJoinLeave implements JoinLeave, MessageHandler {
  
  public static String BYPASS_DISCOVERY = "gemfire.bypass-discovery";
  
  /** amount of time to wait for responses to FindCoordinatorRequests */
  private static final int DISCOVERY_TIMEOUT = Integer.getInteger("gemfire.discovery-timeout", 3000);

  /** amount of time to sleep before trying to join after a failed attempt */
  private static final int JOIN_RETRY_SLEEP = Integer.getInteger("gemfire.join-retry-sleep", 1000);
  
  /** stall time to wait for concurrent join/leave/remove requests to be received */
  public static final long MEMBER_REQUEST_COLLECTION_INTERVAL = Long.getLong("gemfire.member-request-collection-interval", 500);

  /** time to wait for a leave request to be transmitted by jgroups */
  private static final long LEAVE_MESSAGE_SLEEP_TIME = Long.getLong("gemfire.leave-message-sleep-time", 1000);
  
  /** if the locators don't know who the coordinator is we send find-coord requests to this many nodes */
  private static final int MAX_DISCOVERY_NODES = Integer.getInteger("gemfire.max-discovery-nodes", 30);
  
  /** membership logger */
  private static final Logger logger = Services.getLogger();


  /** the view ID where I entered into membership */
  private int birthViewId;

  /** my address */
  private InternalDistributedMember localAddress;

  private Services services;
  
  /** have I connected to the distributed system? */
  private volatile boolean isJoined;

  /** a lock governing GMS state */
  private ReadWriteLock stateLock = new ReentrantReadWriteLock();
  
  /** guarded by stateLock */
  private boolean isCoordinator;
  
  /** a synch object that guards view installation */
  private final Object viewInstallationLock = new Object();
  
  /** the currently installed view */
  private volatile NetView currentView;
  
  /** the previous view **/
  private volatile NetView previousView;
  
  private final Set<InternalDistributedMember> removedMembers = new HashSet<>();
  
  /** a new view being installed */
  private NetView preparedView;
  
  /** the last view that conflicted with view preparation */
  private NetView lastConflictingView;
  
  private List<InetSocketAddress> locators;
  
  /** a list of join/leave/crashes */
  private final List<DistributionMessage> viewRequests = new LinkedList<DistributionMessage>();

  /** collects the response to a join request */
  private JoinResponseMessage[] joinResponse = new JoinResponseMessage[1];
  
  /** collects responses to new views */
  private ViewReplyProcessor viewProcessor = new ViewReplyProcessor(false);
  
  /** collects responses to view preparation messages */
  private ViewReplyProcessor prepareProcessor = new ViewReplyProcessor(true);

  /** whether quorum checks can cause a forced-disconnect */
  private boolean quorumRequired = false;
  
  /** timeout in receiving view acknowledgement */
  private int viewAckTimeout;

  /** background thread that creates new membership views */
  private ViewCreator viewCreator;
  
  /** am I shutting down? */
  private volatile boolean isStopping;

  /** state of collected artifacts during discovery */
  final SearchState searchState = new SearchState();
  
  /** a collection used to detect unit testing */
  Set<String> unitTesting = new HashSet<>();
  
  /** the view where quorum was most recently lost */
  NetView quorumLostView;
  
  static class SearchState {
    Set<InternalDistributedMember> alreadyTried = new HashSet<>();
    Set<InternalDistributedMember> registrants = new HashSet<>();
    InternalDistributedMember possibleCoordinator;
    int viewId = -1;
    boolean hasContactedALocator;
    NetView view;
    Set<FindCoordinatorResponse> responses = new HashSet<>();
    
    void cleanup() {
      alreadyTried.clear();
      possibleCoordinator = null;
      view = null;
      synchronized(responses) {
        responses.clear();
      }
    }
  }

  /**
   * attempt to join the distributed system
   * loop
   *   send a join request to a locator & get a response
   *
   * If the response indicates there's no coordinator it
   * will contain a set of members that have recently contacted
   * it.  The "oldest" member is selected as the coordinator
   * based on ID sort order.
   * 
   * @return true if successful, false if not
   */
  public boolean join() {
    
    if (Boolean.getBoolean(BYPASS_DISCOVERY)) {
      becomeCoordinator();
      return true;
    }
    
    SearchState state = searchState;
    
    long timeout = services.getConfig().getJoinTimeout();
    logger.debug("join timeout is set to {}", timeout);
    long retrySleep =  JOIN_RETRY_SLEEP;
    long startTime = System.currentTimeMillis();
    long giveupTime = startTime + timeout;

    for (int tries=0; !this.isJoined; tries++) {

      boolean found = findCoordinator();
      if (found) {
        logger.debug("found possible coordinator {}", state.possibleCoordinator);
        if (localAddress.getNetMember().preferredForCoordinator()
            && state.possibleCoordinator.equals(this.localAddress)) {
          if (tries > 2 || System.currentTimeMillis() < giveupTime ) {
            becomeCoordinator();
            return true;
          }
        } else {
          if (attemptToJoin()) {
            return true;
          }
          if (System.currentTimeMillis() > giveupTime) {
            break;
          }
          if (!state.possibleCoordinator.equals(localAddress)) {
            state.alreadyTried.add(state.possibleCoordinator);
          }
        }
      } else {
        if (System.currentTimeMillis() > giveupTime) {
          break;
        }
      }
      try {
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
    if (!this.isJoined && state.hasContactedALocator) {
      throw new SystemConnectException("Unable to join the distributed system in "
         + (System.currentTimeMillis()-startTime) + "ms");
    }
    
    return this.isJoined;
  }

  /**
   * send a join request and wait for a reply.  Process the reply.
   * This may throw a SystemConnectException or an AuthenticationFailedException
   * @param coord
   * @return true if the attempt succeeded, false if it timed out
   */
  private boolean attemptToJoin() {
    SearchState state = searchState;
    
    // send a join request to the coordinator and wait for a response
    InternalDistributedMember coord = state.possibleCoordinator;
    logger.info("Attempting to join the distributed system through coordinator " + coord + " using address " + this.localAddress);
    JoinRequestMessage req = new JoinRequestMessage(coord, this.localAddress, 
        services.getAuthenticator().getCredentials(coord));

    services.getMessenger().send(req);
    
    JoinResponseMessage response = null;
    synchronized(joinResponse) {
      if (joinResponse[0] == null) {
        try {
          // Note that if we give up waiting but a response is on
          // the way we will get the new view and join that way.
          // See installView()
          long timeout = Math.max(services.getConfig().getMemberTimeout(),
                                     services.getConfig().getJoinTimeout()/5);
          joinResponse.wait(timeout);
        } catch (InterruptedException e) {
          logger.debug("join attempt was interrupted");
          Thread.currentThread().interrupt();
          return false;
        }
      }
      response = joinResponse[0];
    }
    if (response != null) {
      logger.debug("received join response {}", response);
      joinResponse[0] = null;
      String failReason = response.getRejectionMessage();
      if (failReason != null) {
        if (failReason.contains("Rejecting the attempt of a member using an older version")
            || failReason.contains("15806")) {
          throw new SystemConnectException(failReason);
        }
        throw new AuthenticationFailedException(failReason);
      }
      if (response.getCurrentView() != null) {
        this.birthViewId = response.getMemberID().getVmViewId();
        this.localAddress.setVmViewId(this.birthViewId);
        GMSMember me = (GMSMember)this.localAddress.getNetMember();
        me.setBirthViewId(birthViewId);
        installView(response.getCurrentView());

        if (response.getBecomeCoordinator()) {
          logger.info("I am being told to become the membership coordinator by {}", coord);
          this.currentView = response.getCurrentView();
          becomeCoordinator(null);
        }

        return true;

      } else {
        logger.info("received join response with no membership view: {}", response);
      }
    } else {
      logger.debug("received no join response");
    }
    return false;
  }
  
  
  /**
   * process a join request from another member.  If this is the coordinator
   * this method will enqueue the request for processing in another thread.
   * If this is not the coordinator but the coordinator is known, the message
   * is forwarded to the coordinator.
   * @param incomingRequest
   */
  private void processJoinRequest(JoinRequestMessage incomingRequest) {

    logger.info("received join request from {}", incomingRequest.getMemberID());

    if (incomingRequest.getMemberID().getVersionObject().compareTo(Version.CURRENT) < 0) {
      logger.warn("detected an attempt to start a peer using an older version of the product {}",
          incomingRequest.getMemberID());
      JoinResponseMessage m = new JoinResponseMessage("Rejecting the attempt of a member using an older version");
      m.setRecipient(incomingRequest.getMemberID());
      services.getMessenger().send(m);
      return;
    }
    Object creds = incomingRequest.getCredentials();
    String rejection = null;
    try {
      rejection = services.getAuthenticator().authenticate(incomingRequest.getMemberID(), creds);
    } catch (Exception e) {
      rejection = e.getMessage();
      e.printStackTrace();
    }
    if (rejection != null  &&  rejection.length() > 0) {
      JoinResponseMessage m = new JoinResponseMessage(rejection);
      m.setRecipient(incomingRequest.getMemberID());
      services.getMessenger().send(m);
      return;
    }
    
    if (!this.localAddress.getNetMember().preferredForCoordinator() &&
        incomingRequest.getMemberID().getNetMember().preferredForCoordinator()) {
      JoinResponseMessage m = new JoinResponseMessage(incomingRequest.getMemberID(), currentView, true);
      services.getMessenger().send(m);
      return;
    }
    recordViewRequest(incomingRequest);
  }
  
  
  /**
   * Process a Leave request from another member.  This may cause this member
   * to become the new membership coordinator.  If this is the coordinator
   * a new view will be triggered.
   * 
   * @param incomingRequest
   */
  private void processLeaveRequest(LeaveRequestMessage incomingRequest) {

    logger.info("received leave request from {} for {}", incomingRequest.getSender(), incomingRequest.getMemberID());
    
    
    NetView v = currentView;
    InternalDistributedMember mbr = incomingRequest.getMemberID();
    
    if (logger.isDebugEnabled()) {
      logger.debug("JoinLeave.processLeaveRequest invoked.  isCoordinator="+isCoordinator+ "; isStopping="+isStopping
          +"; cancelInProgress="+services.getCancelCriterion().isCancelInProgress());
    }

    if (!v.contains(mbr) && mbr.getVmViewId() < v.getViewId()) {
      logger.debug("ignoring leave request from old member");
      return;
    }
    
    if (incomingRequest.getMemberID().equals(this.localAddress)) {
      logger.info("I am being told to leave the distributed system");
      forceDisconnect(incomingRequest.getReason());
    }
    
    if (!isCoordinator && !isStopping && !services.getCancelCriterion().isCancelInProgress()) {
      logger.debug("JoinLeave is checking to see if I should become coordinator");
      NetView check = new NetView(v, v.getViewId()+1);
      check.remove(incomingRequest.getMemberID());
      synchronized(removedMembers) {
        check.removeAll(removedMembers);
        check.addCrashedMembers(removedMembers);
      }
      if (check.getCoordinator().equals(localAddress)) {
        becomeCoordinator(incomingRequest.getMemberID());
      }
    }
    else {
      if (!isStopping && !services.getCancelCriterion().isCancelInProgress()) {
        recordViewRequest(incomingRequest);
        this.viewProcessor.processLeaveRequest(incomingRequest.getMemberID());
        this.prepareProcessor.processLeaveRequest(incomingRequest.getMemberID());
      }
    }
  }
  
  
  /**
   * Process a Remove request from another member.  This may cause this member
   * to become the new membership coordinator.  If this is the coordinator
   * a new view will be triggered.
   * 
   * @param incomingRequest
   */
  private void processRemoveRequest(RemoveMemberMessage incomingRequest) {
    NetView v = currentView;

    InternalDistributedMember mbr = incomingRequest.getMemberID();

    if (v != null  &&  !v.contains(incomingRequest.getSender())) {
      logger.info("Membership ignoring removal request for " + mbr + " from non-member " + incomingRequest.getSender());
      return;
    }
    
    logger.info("Membership received a request to remove " + mbr
        + " from " + incomingRequest.getSender()
        + " reason="+incomingRequest.getReason());

    if (mbr.equals(this.localAddress)) {
      // oops - I've been kicked out
      forceDisconnect(incomingRequest.getReason());
      return;
    }
    
    if (getPendingRequestIDs(REMOVE_MEMBER_REQUEST).contains(mbr)) {
      logger.debug("ignoring request as I already have a removal request for this member");
      return;
    }
    
    if (!isCoordinator && !isStopping && !services.getCancelCriterion().isCancelInProgress()) {
      logger.debug("JoinLeave is checking to see if I should become coordinator");
      NetView check = new NetView(v, v.getViewId()+1);
      synchronized(removedMembers) {
        removedMembers.add(mbr);
        check = new NetView(v, v.getViewId());
        check.addCrashedMembers(removedMembers);
        check.removeAll(removedMembers);
      }
      if (check.getCoordinator().equals(localAddress)) {
        becomeCoordinator(mbr);
      }
    }
    else {
      if (!isStopping && !services.getCancelCriterion().isCancelInProgress()) {
        recordViewRequest(incomingRequest);
        this.viewProcessor.processRemoveRequest(mbr);
        this.prepareProcessor.processRemoveRequest(mbr);
      }
    }
  }
  
  
  private void recordViewRequest(DistributionMessage request) {
    logger.debug("JoinLeave is recording the request to be processed in the next membership view");
    synchronized(viewRequests) {
      viewRequests.add(request);
      viewRequests.notify();
    }
  }
  
  // for testing purposes, returns a copy of the view requests for verification
  List<DistributionMessage> getViewRequests() {
    synchronized(viewRequests) {
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
   * @param oldCoordinator may be null
   */
  private void becomeCoordinator(InternalDistributedMember oldCoordinator) {
    boolean testing = unitTesting.contains("noRandomViewChange");
    stateLock.writeLock().lock();
    try {
      if (isCoordinator) {
        return;
      }
      logger.info("This member is becoming the membership coordinator with address {}", localAddress);
      isCoordinator = true;
      if (currentView == null) {
        // create the initial membership view
        NetView newView = new NetView(this.localAddress);
        this.localAddress.setVmViewId(0);
        installView(newView);
        isJoined = true;
        if (viewCreator == null || viewCreator.isShutdown()) {
          viewCreator = new ViewCreator("GemFire Membership View Creator", Services.getThreadGroup());
          viewCreator.setDaemon(true);
          viewCreator.start();
        }
      } else {
        // create and send out a new view
        NetView newView;
        Set<InternalDistributedMember> leaving = new HashSet<>();
        Set<InternalDistributedMember> removals;
        synchronized(viewInstallationLock) {
          int rand = testing? 0 : NetView.RANDOM.nextInt(10);
          int viewNumber = currentView.getViewId() + 5 + rand;
          List<InternalDistributedMember> mbrs = new ArrayList<>(currentView.getMembers());
          if (!mbrs.contains(localAddress)) {
            mbrs.add(localAddress);
          }
          synchronized(this.removedMembers) {
            removals = new HashSet<>(this.removedMembers);
          }
          if (oldCoordinator != null && !removals.contains(oldCoordinator)) {
            leaving.add(oldCoordinator);
          }
          mbrs.removeAll(removals);
          mbrs.removeAll(leaving);
          newView = new NetView(this.localAddress, viewNumber, mbrs, leaving,
              removals);
        }
        if (viewCreator == null || viewCreator.isShutdown()) {
          viewCreator = new ViewCreator("GemFire Membership View Creator", Services.getThreadGroup());
          viewCreator.setInitialView(newView, leaving, removals);
          viewCreator.setDaemon(true);
          viewCreator.start();
        }
      }
    } finally {
      stateLock.writeLock().unlock();
    }
  }
  
  
  private void sendJoinResponses(List<InternalDistributedMember> newMbrs, NetView newView) {
    for (InternalDistributedMember mbr: newMbrs) {
      JoinResponseMessage response = new JoinResponseMessage(mbr, newView);
      services.getMessenger().send(response);
    }
  }
  
  private void sendRemoveMessages(Set<InternalDistributedMember> removals,
      List<String> reasons, NetView newView) {
    Iterator<String> reason = reasons.iterator();
    for (InternalDistributedMember mbr: removals) {
      RemoveMemberMessage response = new RemoveMemberMessage(mbr, mbr, reason.next());
      services.getMessenger().send(response);
    }
  }
  
  
  boolean prepareView(NetView view, Collection<InternalDistributedMember> newMembers) {
    return sendView(view, newMembers, true, this.prepareProcessor);
  }
  
  void sendView(NetView view, Collection<InternalDistributedMember> newMembers) {
    sendView(view, newMembers, false, this.viewProcessor);
  }
  
  
  boolean sendView(NetView view, Collection<InternalDistributedMember> newMembers, boolean preparing, ViewReplyProcessor rp) {
    int id = view.getViewId();
    InstallViewMessage msg = new InstallViewMessage(view, services.getAuthenticator().getCredentials(this.localAddress), preparing);
    Set<InternalDistributedMember> recips = new HashSet<>(view.getMembers());

    // a recent member was seen not to receive a new view - I think this is why
//    recips.removeAll(newMembers); // new members get the view in a JoinResponseMessage
    recips.remove(this.localAddress); // no need to send it to ourselves

    Set<InternalDistributedMember> responders = recips;
    if (!view.getCrashedMembers().isEmpty()) {
      recips = new HashSet<>(recips);
      recips.addAll(view.getCrashedMembers());
    }

    logger.info((preparing? "preparing" : "sending") + " new view " + view);

    if (preparing) {
      this.preparedView = view;
    } else {
      installView(view);
    }
    
    if (recips.isEmpty()) {
      return true;
    }
    
    msg.setRecipients(recips);
    
    Set<InternalDistributedMember> pendingLeaves = getPendingRequestIDs(LEAVE_REQUEST_MESSAGE);
    Set<InternalDistributedMember> pendingRemovals = getPendingRequestIDs(REMOVE_MEMBER_REQUEST);
    rp.initialize(id, responders);
    rp.processPendingRequests(pendingLeaves, pendingRemovals);
    services.getMessenger().send(msg);

    // only wait for responses during preparation
    if (preparing) {
logger.debug("waiting for view responses");
      Set<InternalDistributedMember> failedToRespond = rp.waitForResponses();

      logger.info("finished waiting for responses to view preparation");
      
      InternalDistributedMember conflictingViewSender = rp.getConflictingViewSender();
      NetView conflictingView = rp.getConflictingView();
      if (conflictingView != null) {
        logger.warn("received a conflicting membership view from " + conflictingViewSender
            + " during preparation: " + conflictingView);
        return false;
      }
      
      if (!failedToRespond.isEmpty()  &&  (services.getCancelCriterion().cancelInProgress() == null)) {
        logger.warn("these members failed to respond to the view change: " + failedToRespond);
        return false;
      }
    }
    
    return true;
  }
  
  

  private void processViewMessage(InstallViewMessage m) {
    
    logger.info("Membership: processing {}", m);
    
    NetView view = m.getView();
    
    if (currentView != null  &&  view.getViewId() < currentView.getViewId()) {
      // ignore old views
      ackView(m);
      return;
    }
    
    
    if (m.isPreparing()) {
      if (this.preparedView != null && this.preparedView.getViewId() >= view.getViewId()) {
        services.getMessenger().send(new ViewAckMessage(m.getSender(), this.preparedView));
      }
      else {
        this.preparedView = view;
        ackView(m);
      }
    }
    else { // !preparing
      if (currentView != null  &&  !view.contains(this.localAddress)) {
        if (quorumRequired) {
          forceDisconnect("This node is no longer in the membership view");
        }
      }
      else {
        ackView(m);
        installView(view);
      }
    }
  }
  
  private void forceDisconnect(String reason) {
    this.isStopping = true;
    services.getManager().forceDisconnect(reason);
  }
  

  private void ackView(InstallViewMessage m) {
    if (m.getView().contains(m.getView().getCreator())) {
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
  
  /**
   * This contacts the locators to find out who the current coordinator is.
   * All locators are contacted.  If they don't agree then we choose the oldest
   * coordinator and return it.
   */
  private boolean findCoordinator() {
    SearchState state = searchState;
    
    assert this.localAddress != null;
    
    // TODO - should we try more than one preferred coordinator
    // before jumping to asking view-members who the coordinator is?
    if ( !state.alreadyTried.isEmpty() && state.view != null) {
      return findCoordinatorFromView();
    }
    
    FindCoordinatorRequest request = new FindCoordinatorRequest(this.localAddress, state.alreadyTried, state.viewId);
    Set<InternalDistributedMember> coordinators = new HashSet<InternalDistributedMember>();
    long waitTime = services.getConfig().getLocatorWaitTime() * 1000;
    if (waitTime <= 0) {
      waitTime = services.getConfig().getMemberTimeout() * 2;
    }
    long giveUpTime = System.currentTimeMillis() + waitTime;
    int connectTimeout = (int)services.getConfig().getMemberTimeout();
    boolean anyResponses = false;
    boolean flagsSet = false;
    
    logger.debug("sending {} to {}", request, locators);
    
    do {
      for (InetSocketAddress addr: locators) { 
        try {
          Object o = TcpClient.requestToServer(
              addr.getAddress(), addr.getPort(), request, connectTimeout, 
              true);
          FindCoordinatorResponse response = (o instanceof FindCoordinatorResponse) ? (FindCoordinatorResponse)o : null;
          if (response != null && response.getCoordinator() != null) {
            anyResponses = false;
            NetView v = response.getView();
            int viewId = v == null? -1 : v.getViewId();
            if (viewId > state.viewId) {
              // if the view has changed it is possible that a member
              // that we already tried to join with will become coordinator
              state.alreadyTried.clear();
              state.viewId = viewId;
              state.view = v;
              state.registrants.clear();
              if (response.getRegistrants() != null) {
                state.registrants.addAll(response.getRegistrants());
              }
            }
            coordinators.add(response.getCoordinator());
            if (!flagsSet) {
              flagsSet = true;
              
              boolean enabled = response.isNetworkPartitionDetectionEnabled();
              if (!enabled && services.getConfig().isNetworkPartitionDetectionEnabled()) {
                throw new GemFireConfigException("locator at "+addr
                    +" does not have network-partition-detection enabled but my configuration has it enabled");
              }

              GMSMember mbr = (GMSMember)this.localAddress.getNetMember();
              mbr.setSplitBrainEnabled(enabled);
              services.getConfig().setNetworkPartitionDetectionEnabled(enabled);
              services.getConfig().getDistributionConfig().setEnableNetworkPartitionDetection(enabled);

              if (response.isUsePreferredCoordinators()) {
                this.quorumRequired = true;
                logger.debug("The locator indicates that all locators should be preferred as coordinators");
                if (services.getLocator() != null
                    || Locator.hasLocator()
                    || !services.getConfig().getDistributionConfig().getStartLocator().isEmpty()
                    || localAddress.getVmKind() == DistributionManager.LOCATOR_DM_TYPE) {
                  ((GMSMember)localAddress.getNetMember()).setPreferredForCoordinator(true);
                }
              } else {
                ((GMSMember)localAddress.getNetMember()).setPreferredForCoordinator(true);
              }
            }
          }
        } catch (IOException | ClassNotFoundException problem) {
        }
      }
      if (coordinators.isEmpty()) {
        return false;
      }
      if (!anyResponses) {
        try { Thread.sleep(2000); } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    } while (!anyResponses && System.currentTimeMillis() < giveUpTime);
    
    
    Iterator<InternalDistributedMember> it = coordinators.iterator();
    if (coordinators.size() == 1) {
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
  
  boolean findCoordinatorFromView() {
    ArrayList<FindCoordinatorResponse> result;
    SearchState state = searchState;
    NetView v = state.view;
    List<InternalDistributedMember> recipients = new ArrayList(v.getMembers());

    if (recipients.size() > MAX_DISCOVERY_NODES && MAX_DISCOVERY_NODES > 0) {
      recipients = recipients.subList(0, MAX_DISCOVERY_NODES);
    }
    if (state.registrants != null) {
      recipients.addAll(state.registrants);
    }
    recipients.remove(localAddress);
    FindCoordinatorRequest req = new FindCoordinatorRequest(localAddress, state.alreadyTried, state.viewId);
    req.setRecipients(v.getMembers());

    boolean testing = unitTesting.contains("findCoordinatorFromView"); 
    synchronized(state.responses) {
      if (!testing) {
        state.responses.clear();
      }
      services.getMessenger().send(req);
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
    for (FindCoordinatorResponse resp: result) {
      InternalDistributedMember mbr = resp.getCoordinator();
      if (!state.alreadyTried.contains(mbr)) {
        boolean mbrIsNoob = (mbr.getVmViewId() < 0);
        if (mbrIsNoob) {
          // member has not yet joined
          if (coordIsNoob && (coord == null || coord.compareTo(mbr,false) > 0)) {
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
   * @param rsp
   */
  private void processJoinResponse(JoinResponseMessage rsp) {
    synchronized(joinResponse) {
      joinResponse[0] = rsp;
      joinResponse.notify();
    }
  }
  
  private void processFindCoordinatorRequest(FindCoordinatorRequest req) {
    FindCoordinatorResponse resp;
    if (this.isJoined) {
      NetView v = currentView;
      resp = new FindCoordinatorResponse(v.getCoordinator(), localAddress);
    } else {
      resp = new FindCoordinatorResponse(localAddress, localAddress);
    }
    resp.setRecipient(req.getMemberID());
    services.getMessenger().send(resp);
  }
  
  private void processFindCoordinatorResponse(FindCoordinatorResponse resp) {
    synchronized(searchState.responses) {
      searchState.responses.add(resp);
    }
  }
  
  private void processNetworkPartitionMessage(NetworkPartitionMessage msg) {
    String str = "Membership coordinator "
        + msg.getSender() + " has declared that a network partition has occurred";
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
    
    logger.info("received new view: {}\nold view is: {}", newView, currentView);
    
    synchronized(viewInstallationLock) {
      if (currentView != null && currentView.getViewId() >= newView.getViewId()) {
        // old view - ignore it
        return;
      }
      
      if (currentView == null && !this.isJoined) {
        for (InternalDistributedMember mbr: newView.getMembers()) {
          if (this.localAddress.equals(mbr)) {
            this.birthViewId = mbr.getVmViewId();
            this.localAddress.setVmViewId(this.birthViewId);
            GMSMember me = (GMSMember)this.localAddress.getNetMember();
            me.setBirthViewId(birthViewId);
            isJoined = true;
            break;
          }
        }
      }
      
      if (isNetworkPartition(newView)) {
        if (quorumRequired) {
          Set<InternalDistributedMember> crashes = newView.getActualCrashedMembers(currentView);
          forceDisconnect(
              LocalizedStrings.Network_partition_detected.toLocalizedString(crashes.size(), crashes));
          return;
        }
      }
      previousView = currentView;
      currentView = newView;
      preparedView = null;
      lastConflictingView = null;
      services.installView(newView);
      
      if (!newView.getCreator().equals(this.localAddress)) {
        if (newView.shouldBeCoordinator(this.localAddress)) {
          becomeCoordinator();
        } else if (this.isCoordinator) {
          // stop being coordinator
          stateLock.writeLock().lock();
          try {
            stopCoordinatorServices();
            this.isCoordinator = false;
          } finally {
            stateLock.writeLock().unlock();
          }
        }
      }
      if (!this.isCoordinator) {
        // get rid of outdated requests.  It's possible some requests are
        // newer than the view just processed - the senders will have to
        // resend these
        synchronized(viewRequests) {
          for (Iterator<DistributionMessage> it = viewRequests.iterator(); it.hasNext(); ) {
            DistributionMessage m = it.next();
            if (m instanceof JoinRequestMessage) {
              it.remove();
            } else if (m instanceof LeaveRequestMessage) {
              if (!currentView.contains(((LeaveRequestMessage)m).getMemberID())) {
                it.remove();
              }
            } else if (m instanceof RemoveMemberMessage) {
              if (!currentView.contains(((RemoveMemberMessage)m).getMemberID())) {
                it.remove();
              }
            }
          }
        }
      }
    }
    synchronized(removedMembers) {
      removedMembers.clear();
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
  private boolean isNetworkPartition(NetView newView) {
    if (currentView == null) {
      return false;
    }
    int oldWeight = currentView.memberWeight();
    int failedWeight = newView.getCrashedMemberWeight(currentView);
    if (failedWeight > 0) {
      if (logger.isInfoEnabled()) {
        newView.logCrashedMemberWeights(currentView, logger);
      }
      int failurePoint = (int)(Math.round(51 * oldWeight) / 100.0);
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
      viewCreator.shutdown();
    }
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
  }

  public void beHealthy() {
  }



  @Override
  public void start() {
  }



  @Override
  public void started() {
    this.localAddress = services.getMessenger().getMemberID();
  }



  @Override
  public void stop() {
    logger.debug("JoinLeave stopping");
    leave();
  }



  @Override
  public void stopped() {
    // TODO Auto-generated method stub
    
  }



  @Override
  public void leave() {
    boolean waitForProcessing = false;
    synchronized(viewInstallationLock) {
      NetView view = currentView;
      isStopping = true;
      stopCoordinatorServices();
      if (view != null) {
        if (view.size() > 1) {
          if (this.isCoordinator) {
            logger.debug("JoinLeave stopping coordination services");
            NetView newView = new NetView(view, view.getViewId()+1);
            newView.remove(localAddress);
            InstallViewMessage m = new InstallViewMessage(newView, services.getAuthenticator().getCredentials(this.localAddress));
            m.setRecipients(newView.getMembers());
            services.getMessenger().send(m);
            waitForProcessing = true;
          }
          else {
            List<InternalDistributedMember> coords = view.getPreferredCoordinators(Collections.<InternalDistributedMember>emptySet(), localAddress, 5);

            logger.debug("JoinLeave sending a leave request to {}", coords);
            LeaveRequestMessage m = new LeaveRequestMessage(coords, this.localAddress, "this member is shutting down");
            services.getMessenger().send(m);
            waitForProcessing = true;
          }
        } // view.size
      }// view != null
    }
    if (waitForProcessing) {
      try {
        Thread.sleep(LEAVE_MESSAGE_SLEEP_TIME);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }



  @Override
  public void remove(InternalDistributedMember m, String reason) {
    NetView v = this.currentView;
    
    services.getCancelCriterion().checkCancelInProgress(null);
    
    if (v != null && v.contains(m)) {
      Set<InternalDistributedMember> filter = new HashSet<>();
      filter.add(m);
      RemoveMemberMessage msg = new RemoveMemberMessage(v.getPreferredCoordinators(filter, getMemberID(), 5), 
          m,
          reason);
      msg.setSender(this.localAddress);
      processRemoveRequest(msg);
      if (!this.isCoordinator) {
        msg.resetRecipients();
        msg.setRecipients(v.getPreferredCoordinators(Collections.<InternalDistributedMember>emptySet(),
            localAddress, 10));
        services.getMessenger().send(msg);
      }
    }
  }

  @Override
  public void memberShutdown(DistributedMember mbr, String reason) {
    
    if (this.isCoordinator) {
      LeaveRequestMessage msg = new LeaveRequestMessage(Collections.singleton(this.localAddress), (InternalDistributedMember)mbr, reason);
      recordViewRequest(msg);
    }
  }

  
  @Override
  public void disableDisconnectOnQuorumLossForTesting() {
    this.quorumRequired = false;
  }
  
  @Override
  public void init(Services s) {
    this.services = s;
    
    DistributionConfig dc = services.getConfig().getDistributionConfig();
    if (dc.getMcastPort() != 0
        && dc.getLocators().trim().isEmpty()
        && dc.getStartLocator().trim().isEmpty()) {
      throw new GemFireConfigException("Multicast cannot be configured for a non-distributed cache."
          + "  Please configure the locator services for this cache using "+DistributionConfig.LOCATORS_NAME
          + " or " + DistributionConfig.START_LOCATOR_NAME+".");
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
    ackCollectionTimeout = Integer.getInteger("gemfire.VIEW_ACK_TIMEOUT", ackCollectionTimeout).intValue();
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
    logger.debug("JoinLeave processing {}", m);
    switch (m.getDSFID()) {
    case JOIN_REQUEST:
      processJoinRequest((JoinRequestMessage)m);
      break;
    case JOIN_RESPONSE:
      processJoinResponse((JoinResponseMessage)m);
      break;
    case INSTALL_VIEW_MESSAGE:
      processViewMessage((InstallViewMessage)m);
      break;
    case VIEW_ACK_MESSAGE:
      processViewAckMessage((ViewAckMessage)m);
      break;
    case LEAVE_REQUEST_MESSAGE:
      processLeaveRequest((LeaveRequestMessage)m);
      break;
    case REMOVE_MEMBER_REQUEST:
      processRemoveRequest((RemoveMemberMessage)m);
      break;
    case FIND_COORDINATOR_REQ:
      processFindCoordinatorRequest((FindCoordinatorRequest)m);
      break;
    case FIND_COORDINATOR_RESP:
      processFindCoordinatorResponse((FindCoordinatorResponse)m);
      break;
    case NETWORK_PARTITION_MESSAGE:
      processNetworkPartitionMessage((NetworkPartitionMessage)m);
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
    synchronized(viewRequests) {
      for (DistributionMessage msg: viewRequests) {
        if (msg.getDSFID() == theDSFID) {
          result.add(((HasMemberID)msg).getMemberID());
        }
      }
    }
    return result;
  }
  
  
  class ViewReplyProcessor {
    volatile int viewId = -1;
    final Set<InternalDistributedMember> notRepliedYet = new HashSet<>();
    NetView conflictingView;
    InternalDistributedMember conflictingViewSender;
    boolean waiting;
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
    
    synchronized void processPendingRequests(Set<InternalDistributedMember> pendingLeaves,
        Set<InternalDistributedMember> pendingRemovals) {
      // there's no point in waiting for members who have already
      // requested to leave or who have been declared crashed.
      // We don't want to mix the two because pending removals
      // aren't reflected as having crashed in the current view
      // and need to cause a new view to be generated
      notRepliedYet.removeAll(pendingLeaves);
      synchronized(this.pendingRemovals) {
        this.pendingRemovals.addAll(pendingRemovals);
      }
    }
    
    synchronized void processLeaveRequest(InternalDistributedMember mbr) {
      if (waiting) {
        stopWaitingFor(mbr);
      }
    }
    
    synchronized void processRemoveRequest(InternalDistributedMember mbr) {
      if (waiting) {
        pendingRemovals.add(mbr);
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

        stopWaitingFor(sender);
      }
    }

    /** call with synchronized(this) */
    private void stopWaitingFor(InternalDistributedMember mbr) {
      notRepliedYet.remove(mbr);
      if (notRepliedYet.isEmpty() ||
          (pendingRemovals != null && pendingRemovals.containsAll(notRepliedYet))) {
        logger.debug("All anticipated view responses received - notifying waiting thread");
        waiting = false;
        notify();
      }
    }
    
    Set<InternalDistributedMember> waitForResponses() {
      Set<InternalDistributedMember> result = this.notRepliedYet;
      long endOfWait = System.currentTimeMillis() + viewAckTimeout;
      try {
        while (System.currentTimeMillis() < endOfWait
            &&  (services.getCancelCriterion().cancelInProgress() == null)) {
          try {
            synchronized(this) {
              if (!waiting || result.isEmpty() || this.conflictingView != null) {
                break;
              }
              wait(1000);
            }
          } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting for view responses");
            Thread.currentThread().interrupt();
            return result;
          }
        }
      } finally {
        if (!this.waiting) {
          // if we've set waiting to false due to incoming messages then
          // we've discounted receiving any other responses from the
          // remaining members due to leave/crash notification
          result = Collections.emptySet();
        } else {
          this.waiting = false;
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
    
    Set<InternalDistributedMember> getUnresponsiveMembers() {
      return this.notRepliedYet;
    }
  }
  

  
  

  class ViewCreator extends Thread {
    boolean shutdown = false;
    volatile boolean waiting = false;
    
    NetView initialView;
    Set<InternalDistributedMember> initialLeaving;
    Set<InternalDistributedMember> initialRemovals;
    
    ViewCreator(String name, ThreadGroup tg) {
      super(tg, name);
    }
    
    void shutdown() {
      shutdown = true;
      synchronized(viewRequests) {
        viewRequests.notify();
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
     * @param newView
     * @param leaving - members leaving in this view
     * @param removals - members crashed in this view
     */
    void setInitialView(NetView newView, Set<InternalDistributedMember> leaving, Set<InternalDistributedMember> removals) {
      this.initialView = newView;
      this.initialLeaving = leaving;
      this.initialRemovals = removals;
    }
    
    private void sendInitialView() {
      if (initialView != null) {
        try {
          prepareAndSendView(initialView, Collections.<InternalDistributedMember>emptyList(),
            initialLeaving, initialRemovals);
        } finally {
          this.initialView = null;
          this.initialLeaving = null;
          this.initialRemovals = null;
        }
      }
    }

    @Override
    public void run() {
      List<DistributionMessage> requests = null;
      logger.info("View Creator thread is starting");
      sendInitialView();
      long okayToCreateView = System.currentTimeMillis() + MEMBER_REQUEST_COLLECTION_INTERVAL;
      try {
        for (;;) {
          synchronized(viewRequests) {
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
            } else {
              if (System.currentTimeMillis() < okayToCreateView) {
                // sleep to let more requests arrive
                try {
                  sleep(100);
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
                okayToCreateView = System.currentTimeMillis() + MEMBER_REQUEST_COLLECTION_INTERVAL;
              }
            }
          } // synchronized
          if (requests != null  && !requests.isEmpty()) {
            logger.debug("View Creator is processing {} requests for the next membership view", requests.size());
            try {
              createAndSendView(requests);
            } catch (DistributedSystemDisconnectedException e) {
              shutdown = true;
            }
            requests = null;
          }
        }
      } finally {
        shutdown = true;
      }
    }
    
    /**
     * Create a new membership view and send it to members (including crashed members).
     * Returns false if the view cannot be prepared successfully, true otherwise
     */
    void createAndSendView(List<DistributionMessage> requests) {
      List<InternalDistributedMember> joinReqs = new ArrayList<>();
      Set<InternalDistributedMember> leaveReqs = new HashSet<>();
      Set<InternalDistributedMember> removalReqs = new HashSet<>();
      List<String> removalReasons = new ArrayList<String>();

      NetView oldView = currentView;
      List<InternalDistributedMember> oldMembers;
      if (oldView != null) {
        oldMembers = new ArrayList<>(oldView.getMembers());
      } else {
        oldMembers = Collections.emptyList();
      }
      Set<InternalDistributedMember> oldIDs = new HashSet<>();
      
      for (DistributionMessage msg: requests) {
        logger.debug("processing request {}", msg);

        InternalDistributedMember mbr = null;
        
        if (msg instanceof JoinRequestMessage) {
          mbr = ((JoinRequestMessage)msg).getMemberID();
          // see if an old member ID is being reused.  If
          // so we'll remove it from the new view
          for (InternalDistributedMember m: oldMembers) {
            if (mbr.compareTo(m, false) == 0) {
              oldIDs.add(m);
              break;
            }
          }
          if (!joinReqs.contains(mbr)) {
            joinReqs.add(mbr);
          }
        }
        else if (msg instanceof LeaveRequestMessage) {
          mbr = ((LeaveRequestMessage) msg).getMemberID();
          if (oldMembers.contains(mbr) && !leaveReqs.contains(mbr)) {
            leaveReqs.add(mbr);
          }
        }
        else if (msg instanceof RemoveMemberMessage) {
          mbr = ((RemoveMemberMessage) msg).getMemberID();
          if (oldMembers.contains(mbr) && !leaveReqs.contains(mbr) && !removalReqs.contains(mbr)) {
            removalReqs.add(mbr);
            removalReasons.add(((RemoveMemberMessage) msg).getReason());
          }
        }
        else {
          logger.warn("Unknown membership request encountered: {}", msg);
        }
      }
      
      for (InternalDistributedMember mbr: oldIDs) {
        if (!leaveReqs.contains(mbr) && !removalReqs.contains(mbr)) {
          removalReqs.add(mbr);
          removalReasons.add("Removal of old ID that has been reused");
        }
      }
      
      if (removalReqs.isEmpty() && leaveReqs.isEmpty() && joinReqs.isEmpty()) {
        return;
      }
      
      NetView newView;
      synchronized(viewInstallationLock) {
        int viewNumber = 0;
        List<InternalDistributedMember> mbrs;
        if (currentView == null) {
          mbrs = new ArrayList<InternalDistributedMember>(joinReqs.size());
        } else {
          viewNumber = currentView.getViewId()+1;
          mbrs = new ArrayList<InternalDistributedMember>(oldMembers);
        }
        mbrs.addAll(joinReqs);
        mbrs.removeAll(leaveReqs);
        mbrs.removeAll(removalReqs);
        newView = new NetView(localAddress, viewNumber, mbrs, leaveReqs,
            removalReqs);
      }
      
      // if there are no membership changes then abort creation of
      // the new view
      if (newView.getMembers().equals(currentView.getMembers())) {
        logger.info("membership hasn't changed - aborting new view {}", newView);
        return;
      }
      
      for (InternalDistributedMember mbr: joinReqs) {
        mbr.setVmViewId(newView.getViewId());
        mbr.getNetMember().setSplitBrainEnabled(services.getConfig().isNetworkPartitionDetectionEnabled());
      }
      // send removal messages before installing the view so we stop
      // getting messages from members that have been kicked out
      sendRemoveMessages(removalReqs, removalReasons, newView);
      
      // we want to always check for quorum loss but don't act on it
      // unless network-partition-detection is enabled
      if ( !(isNetworkPartition(newView) && quorumRequired) ) {
        sendJoinResponses(joinReqs, newView);
      }

      prepareAndSendView(newView, joinReqs, leaveReqs, removalReqs);
      return;
    }
    
    
    /**
     * This handles the 2-phase installation of the view
     */
    void prepareAndSendView(NetView newView,
        List<InternalDistributedMember> joinReqs,
        Set<InternalDistributedMember> leaveReqs,
        Set<InternalDistributedMember> removalReqs) {
      boolean prepared = false;
      do {
        if (this.shutdown || Thread.currentThread().isInterrupted()) {
          return;
        }
        
        if (quorumRequired && isNetworkPartition(newView)) {
          sendNetworkPartitionMessage(newView);
          try {
            Thread.sleep(LEAVE_MESSAGE_SLEEP_TIME);
          } catch (InterruptedException e) {
            // signal the run() method to exit
            shutdown = true;
            return;
          }
          Set<InternalDistributedMember> crashes = newView.getActualCrashedMembers(currentView);
          forceDisconnect(
              LocalizedStrings.Network_partition_detected.toLocalizedString(crashes.size(), crashes));
          shutdown = true;
          return;
        }

        prepared = prepareView(newView, joinReqs);
        if (prepared) {
          break;
        }

        Set<InternalDistributedMember> unresponsive = prepareProcessor.getUnresponsiveMembers();
        unresponsive.removeAll(removalReqs);
        unresponsive.removeAll(leaveReqs);
        try {
          removeHealthyMembers(unresponsive);
        } catch (InterruptedException e) {
          // abort the view if interrupted
          shutdown = true;
          return;
        }

        List<InternalDistributedMember> failures = new ArrayList<>(currentView.getCrashedMembers().size() + unresponsive.size());

        NetView conflictingView = prepareProcessor.getConflictingView();
        if (conflictingView != null
            && !conflictingView.getCreator().equals(localAddress)
            && conflictingView.getViewId() > newView.getViewId()
            && (lastConflictingView == null || conflictingView.getViewId() > lastConflictingView.getViewId())) {
          lastConflictingView = conflictingView;
          logger.info("adding these crashed members from a conflicting view to the crash-set for the next view: {}\nconflicting view: {}", unresponsive, conflictingView);
          failures.addAll(conflictingView.getCrashedMembers());
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
          newView = new NetView(localAddress, newView.getViewId()+1, newMembers, leaveReqs,
              removalReqs);
        }
      } while (!prepared);
      
      lastConflictingView = null;
      
      sendView(newView, joinReqs);
    }
    
    /**
     * performs health checks on the collection of members, removing any that
     * are found to be healthy
     * @param mbrs
     */
    private void removeHealthyMembers(Collection<InternalDistributedMember> mbrs) throws InterruptedException {
      List<Callable<InternalDistributedMember>> checkers = new ArrayList<Callable<InternalDistributedMember>>(mbrs.size()); 
      
      Set<InternalDistributedMember> newRemovals = new HashSet<>();
      Set<InternalDistributedMember> newLeaves = new HashSet<>();
      
      synchronized(viewRequests) {
        for (DistributionMessage msg: viewRequests) {
          switch (msg.getDSFID()) {
          case LEAVE_REQUEST_MESSAGE:
            newLeaves.add(((LeaveRequestMessage)msg).getMemberID());
            break;
          case REMOVE_MEMBER_REQUEST:
            newRemovals.add(((RemoveMemberMessage)msg).getMemberID());
            break;
          default:
            break;
          }
        }
      }
      
      for (InternalDistributedMember mbr: mbrs) {
        if (newRemovals.contains(mbr)) {
          // no need to do a health check on a member who is already leaving
          logger.info("member {} is already scheduled for removal", mbr);
          continue;
        }
        if (newLeaves.contains(mbr)) {
          // no need to do a health check on a member that is declared crashed
          logger.info("member {} has already sent a leave-request", mbr);
          continue;
        }
        final InternalDistributedMember fmbr = mbr;
        checkers.add(new Callable<InternalDistributedMember>() {
          @Override
          public InternalDistributedMember call() throws Exception {
            // return the member id if it fails health checks
            logger.info("checking state of member " + fmbr);
            if (services.getHealthMonitor().checkIfAvailable(fmbr, "Member failed to acknowledge a membership view", false)) {
              logger.info("member " + fmbr + " passed availability check");
              return fmbr;
            }
            logger.info("member " + fmbr + " failed availability check");
            return null;
          }
        });
      }
      
      mbrs.removeAll(newLeaves);
      
      if (mbrs.isEmpty()) {
        return;
      }
      
      ExecutorService svc = Executors.newFixedThreadPool(mbrs.size(), new ThreadFactory() {
        AtomicInteger i = new AtomicInteger();
        @Override
        public Thread newThread(Runnable r) {
          return new Thread(Services.getThreadGroup(), r,
              "Member verification thread " + i.incrementAndGet());
        }
      });
      
      try {
        List<Future<InternalDistributedMember>> futures;
        futures = svc.invokeAll(checkers);

        for (Future<InternalDistributedMember> future: futures) {
          try {
            InternalDistributedMember mbr = future.get(viewAckTimeout, TimeUnit.MILLISECONDS);
            if (mbr != null) {
              mbrs.remove(mbr);
            }
          } catch (java.util.concurrent.TimeoutException e) {
            // TODO should the member be removed if we can't verify it in time?
          } catch (ExecutionException e) {
            logger.info("unexpected exception caught during member verification", e);
          }
        }
      } finally {
        svc.shutdownNow();
      }
    }
  }
  
}
