package com.gemstone.gemfire.distributed.internal.membership.gms.membership;

import static com.gemstone.gemfire.distributed.internal.DistributionManager.LOCATOR_DM_TYPE;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.INSTALL_VIEW_MESSAGE;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.JOIN_REQUEST;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.JOIN_RESPONSE;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.LEAVE_REQUEST_MESSAGE;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.REMOVE_MEMBER_REQUEST;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.VIEW_ACK_MESSAGE;

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
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.InstallViewMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.JoinRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.JoinResponseMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.LeaveRequestMessage;
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

  /** number of times to try joining before giving up */
  private static final int JOIN_ATTEMPTS = Integer.getInteger("gemfire.join-attempts", 4);
  
  /** amount of time to sleep before trying to join after a failed attempt */
  private static final int JOIN_RETRY_SLEEP = Integer.getInteger("gemfire.join-retry-sleep", 1000);
  
  /** amount of time to wait for a view to be acked by all members before performing suspect processing on non-responders */
  private static final int VIEW_INSTALLATION_TIMEOUT = Integer.getInteger("gemfire.view-ack-timeout", 12500);

  /** stall time to wait for concurrent join/leave/remove requests to be received */
  private static final long MEMBER_REQUEST_COLLECTION_INTERVAL = Long.getLong("gemfire.member-request-collection-interval", 2000);

  /** time to wait for a leave request to be transmitted by jgroups */
  private static final long LEAVE_MESSAGE_SLEEP_TIME = Long.getLong("gemfire.leave-message-sleep-time", 2000);
  
  /** membership logger */
  private static final Logger logger = Services.getLogger();


  /** the view ID where I entered into membership */
  private int birthViewId;

  /** my address */
  private InternalDistributedMember localAddress;

  private Services services;
  
  /** have I connected to the distributed system? */
  private boolean isJoined;

  /** a lock governing GMS state */
  private ReadWriteLock stateLock = new ReentrantReadWriteLock();
  
  /** guarded by stateLock */
  private boolean isCoordinator;
  
  /** a synch object that guards view installation */
  private final Object viewInstallationLock = new Object();
  
  /** the currently installed view */
  private volatile NetView currentView;
  
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

    if (this.localAddress.getVmKind() == LOCATOR_DM_TYPE
        && Boolean.getBoolean("gemfire.first-member")) {
      becomeCoordinator();
      return true;
    }

    for (int tries=0; tries<JOIN_ATTEMPTS; tries++) {
      InternalDistributedMember coord = findCoordinator();
      logger.debug("found possible coordinator {}", coord);
      if (coord != null) {
        if (coord.equals(this.localAddress)) {
          if (tries > (JOIN_ATTEMPTS/2)) {
            becomeCoordinator();
            return true;
          }
        } else {
          if (attemptToJoin(coord)) {
            return true;
          } 
        }
      }
      try {
        Thread.sleep(JOIN_RETRY_SLEEP);
      } catch (InterruptedException e) {
        return false;
      }
    } // for
    return this.isJoined;
  }

  /**
   * send a join request and wait for a reply.  Process the reply.
   * This may throw a SystemConnectException or an AuthenticationFailedException
   * @param coord
   * @return true if the attempt succeeded, false if it timed out
   */
  private boolean attemptToJoin(InternalDistributedMember coord) {
    // send a join request to the coordinator and wait for a response
    logger.info("Attempting to join the distributed system through coordinator " + coord + " using address " + this.localAddress);
    JoinRequestMessage req = new JoinRequestMessage(coord, this.localAddress, 
        services.getAuthenticator().getCredentials(coord));

    services.getMessenger().send(req);
    
    JoinResponseMessage response = null;
    synchronized(joinResponse) {
      if (joinResponse[0] == null) {
        try {
          joinResponse.wait(services.getConfig().getJoinTimeout()/JOIN_ATTEMPTS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      response = joinResponse[0];
    }
    if (response != null) {
// DEBUGGING - REMOVE
logger.info("received join response {}", response);
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
        GMSMember o = (GMSMember)response.getMemberID().getNetMember();
        me.setBirthViewId(birthViewId);
        me.setSplitBrainEnabled(o.isSplitBrainEnabled());
        me.setPreferredForCoordinator(o.preferredForCoordinator());
        installView(response.getCurrentView());
        return true;
      } else {
        logger.info("received join response with no membership view: {}", response);
      }
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
    NetView v = currentView;
    if (logger.isDebugEnabled()) {
      logger.debug("JoinLeave.processLeaveRequest invoked.  isCoordinator="+isCoordinator+ "; isStopping="+isStopping
          +"; cancelInProgress="+services.getCancelCriterion().isCancelInProgress());
    }
    if (!isCoordinator && !isStopping && !services.getCancelCriterion().isCancelInProgress()) {
      logger.debug("JoinLeave is checking to see if I should become coordinator");
      NetView check = new NetView(v, v.getViewId()+1);
      check.remove(incomingRequest.getMemberID());
      if (check.getCoordinator().equals(localAddress)) {
        becomeCoordinator(incomingRequest.getMemberID());
      }
    }
    else {
      if (!isStopping && !services.getCancelCriterion().isCancelInProgress()) {
        recordViewRequest(incomingRequest);
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
    if (logger.isDebugEnabled()) {
      logger.debug("JoinLeave.processRemoveRequest invoked.  isCoordinator="+isCoordinator+ "; isStopping="+isStopping
          +"; cancelInProgress="+services.getCancelCriterion().isCancelInProgress());
    }
    InternalDistributedMember mbr = incomingRequest.getMemberID();

    if (v != null  &&  !v.contains(incomingRequest.getSender())) {
      logger.info("Membership ignoring removal request for " + mbr + " from non-member " + incomingRequest.getSender());
      return;
    }
    
    logger.info("Membership received a request to remove " + mbr
        + "; reason="+incomingRequest.getReason());

    if (mbr.equals(this.localAddress)) {
      // oops - I've been kicked out
      services.getManager().forceDisconnect(incomingRequest.getReason());
      return;
    }
    
    if (!isCoordinator && !isStopping && !services.getCancelCriterion().isCancelInProgress()) {
      logger.debug("JoinLeave is checking to see if I should become coordinator");
      NetView check = new NetView(v, v.getViewId()+1);
      check.remove(mbr);
      if (check.getCoordinator().equals(localAddress)) {
        becomeCoordinator(mbr);
      }
    }
    else {
      if (!isStopping && !services.getCancelCriterion().isCancelInProgress()) {
        recordViewRequest(incomingRequest);
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
  
  //for testing purposes, returns a copy of the view requests for verification
  List<DistributionMessage> getViewRequests() {
    synchronized(viewRequests) {
      return new LinkedList<DistributionMessage>(viewRequests);
    }
  }
  
  /**
   * Yippeee - I get to be the coordinator
   */
  private void becomeCoordinator() {
    becomeCoordinator(null);
  }
  
  /**
   * @param oldCoordinator may be null
   */
  private void becomeCoordinator(InternalDistributedMember oldCoordinator) {
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
        startCoordinatorServices();
      } else {
        // create and send out a new view
        NetView newView;
        synchronized(viewInstallationLock) {
          int viewNumber = currentView.getViewId() + 5;
          List<InternalDistributedMember> mbrs = new ArrayList<InternalDistributedMember>(currentView.getMembers());
          mbrs.add(localAddress);
          List<InternalDistributedMember> leaving = new ArrayList<InternalDistributedMember>();
          if (oldCoordinator != null) {
            leaving.add(oldCoordinator);
          }
          newView = new NetView(this.localAddress, viewNumber, mbrs, leaving,
              Collections.<InternalDistributedMember>emptyList());
        }
        sendView(newView, Collections.<InternalDistributedMember>emptyList());
        startCoordinatorServices();
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
  
  private void sendRemoveMessages(List<InternalDistributedMember> newMbrs,
      List<String> reasons, NetView newView) {
    Iterator<String> reason = reasons.iterator();
    for (InternalDistributedMember mbr: newMbrs) {
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
    Set<InternalDistributedMember> recips = new HashSet<InternalDistributedMember>(view.getMembers());
    recips.removeAll(newMembers); // new members get the view in a JoinResponseMessage
    recips.remove(this.localAddress); // no need to send it to ourselves
    installView(view);
    recips.addAll(view.getCrashedMembers());
    if (recips.isEmpty()) {
      return true;
    }
    msg.setRecipients(recips);
    rp.initialize(id, recips);

    logger.info((preparing? "preparing" : "sending") + " new view " + view);
    services.getMessenger().send(msg);

    // only wait for responses during preparation
    if (preparing) {
      Set<InternalDistributedMember> failedToRespond = rp.waitForResponses();

      logger.info("View Creator is finished waiting for responses to view preparation");
      
      InternalDistributedMember conflictingViewSender = rp.getConflictingViewSender();
      NetView conflictingView = rp.getConflictingView();
      if (conflictingView != null) {
        logger.warn("View Creator received a conflicting membership view from " + conflictingViewSender
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
          services.getManager().forceDisconnect("This node is no longer in the membership view");
        }
      }
      else {
        ackView(m);
        installView(view);
      }
    }
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
   * @return
   */
  private InternalDistributedMember findCoordinator() {
    assert this.localAddress != null;
    
    FindCoordinatorRequest request = new FindCoordinatorRequest(this.localAddress);
    Set<InternalDistributedMember> coordinators = new HashSet<InternalDistributedMember>();
    long giveUpTime = System.currentTimeMillis() + (services.getConfig().getLocatorWaitTime() * 1000L);
    boolean anyResponses = false;
    
    do {
      for (InetSocketAddress addr: locators) { 
        try {
          Object o = TcpClient.requestToServer(
              addr.getAddress(), addr.getPort(), request, services.getConfig().getJoinTimeout(), 
              true);
          FindCoordinatorResponse response = (o instanceof FindCoordinatorResponse) ? (FindCoordinatorResponse)o : null;
          if (response != null && response.getCoordinator() != null) {
            anyResponses = false;
            coordinators.add(response.getCoordinator());
            if (response.isFromView()) {
              GMSMember mbr = (GMSMember)this.localAddress.getNetMember();
              services.getConfig().setNetworkPartitionDetectionEnabled(response.isNetworkPartitionDetectionEnabled());
              if (response.isUsePreferredCoordinators()
                  && localAddress.getVmKind() != DistributionManager.LOCATOR_DM_TYPE) {
                mbr.setPreferredForCoordinator(false);
              }
            }
          }
        } catch (IOException | ClassNotFoundException problem) {
        }
      }
      if (coordinators.isEmpty()) {
        return null;
      }
      if (!anyResponses) {
        try { Thread.sleep(2000); } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      }
    } while (!anyResponses && System.currentTimeMillis() < giveUpTime);
    
    Iterator<InternalDistributedMember> it = coordinators.iterator();
    if (coordinators.size() == 1) {
      return it.next();
    }
    InternalDistributedMember oldest = it.next();
    while (it.hasNext()) {
      InternalDistributedMember candidate = it.next();
      if (oldest.compareTo(candidate) > 0) {
        oldest = candidate;
      }
    }
    return oldest;
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

  @Override
  public NetView getView() {
    return currentView;
  }

  @Override
  public InternalDistributedMember getMemberID() {
    return this.localAddress;
  }
  
  public void installView(NetView newView) {
    synchronized(viewInstallationLock) {
      if (currentView != null && currentView.getViewId() >= newView.getViewId()) {
        // old view - ignore it
        return;
      }
      
      if (checkForPartition(newView)) {
        if (quorumRequired) {
          List<InternalDistributedMember> crashes = newView.getActualCrashedMembers(currentView);
          services.getManager().forceDisconnect(
              LocalizedStrings.Network_partition_detected.toLocalizedString(crashes.size(), crashes));
        }
        return;
      }
      
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
  }
  

  /**
   * check to see if the new view shows a drop of 51% or more
   */
  private boolean checkForPartition(NetView newView) {
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
      if (failedWeight > failurePoint) {
        services.getManager().quorumLost(newView.getActualCrashedMembers(currentView), currentView);
        return true;
      }
    }
    return false;
  }
  
  
  /** invoke this under the viewInstallationLock */
  private void startCoordinatorServices() {
    if (viewCreator == null || viewCreator.isShutdown()) {
      viewCreator = new ViewCreator("GemFire Membership View Creator", Services.getThreadGroup());
      viewCreator.setDaemon(true);
      viewCreator.start();
    }
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
    currentView = null;
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
    synchronized(viewInstallationLock) {
      NetView view = currentView;
      isStopping = true;
      if (view != null) {
        if (view.size() > 1) {
          if (this.isCoordinator) {
            logger.debug("JoinLeave stopping coordination services");
            stopCoordinatorServices();
            NetView newView = new NetView(view, view.getViewId()+1);
            newView.remove(localAddress);
            InstallViewMessage m = new InstallViewMessage(newView, services.getAuthenticator().getCredentials(this.localAddress));
            m.setRecipients(newView.getMembers());
            services.getMessenger().send(m);
            try { Thread.sleep(LEAVE_MESSAGE_SLEEP_TIME); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
          }
          else {
            logger.debug("JoinLeave sending a leave request to {}", view.getCoordinator());
            LeaveRequestMessage m = new LeaveRequestMessage(view.getCoordinator(), this.localAddress);
            services.getMessenger().send(m);
          }
        } // view.size
      }// view != null
    }
      
  }



  @Override
  public void remove(InternalDistributedMember m, String reason) {
    NetView v = this.currentView;
    if (v != null) {
      RemoveMemberMessage msg = new RemoveMemberMessage(v.getCoordinator(), m,
          reason);
      services.getMessenger().send(msg);
    }
  }

  @Override
  public void memberShutdown(DistributedMember mbr, String reason) {
    if (this.isCoordinator) {
      LeaveRequestMessage msg = new LeaveRequestMessage(this.localAddress, (InternalDistributedMember)mbr);
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
    default:
      throw new IllegalArgumentException("unknown message type: " + m);
    }
  }
  

  
  
  class ViewReplyProcessor {
    volatile int viewId = -1;
    volatile Set<InternalDistributedMember> recipients;
    volatile NetView conflictingView;
    volatile InternalDistributedMember conflictingViewSender;
    volatile boolean waiting;
    final boolean isPrepareViewProcessor;
    
    ViewReplyProcessor(boolean forPreparation) {
      this.isPrepareViewProcessor = forPreparation;
    }
    
    void initialize(int viewId, Set<InternalDistributedMember> recips) {
      this.waiting = true;
      this.viewId = viewId;
      this.recipients = recips;
      this.conflictingView = null;
    }
    
    void processViewResponse(int viewId, InternalDistributedMember sender, NetView conflictingView) {
      if (!this.waiting) {
        return;
      }
      
      if (viewId == this.viewId) {
        if (conflictingView != null) {
          this.conflictingViewSender = sender;
          this.conflictingView = conflictingView;
        }

        Set<InternalDistributedMember> waitingFor = this.recipients;
        synchronized(waitingFor) {
          waitingFor.remove(sender);
          if (waitingFor.isEmpty()) {
            logger.debug("All view responses received - notifying waiting thread");
            waitingFor.notify();
          }
        }

      }
    }
    
    Set<InternalDistributedMember> waitForResponses() {
      Set<InternalDistributedMember> result = this.recipients;
      long endOfWait = System.currentTimeMillis() + viewAckTimeout;
      try {
        while (System.currentTimeMillis() < endOfWait
            &&  (services.getCancelCriterion().cancelInProgress() == null)) {
          try {
            synchronized(result) {
              result.wait(1000);
            }
          } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting for view resonses");
            Thread.currentThread().interrupt();
            return result;
          }
        }
      } finally {
        this.waiting = false;
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
      return this.recipients;
    }
  }
  

  
  

  class ViewCreator extends Thread {
    boolean shutdown = false;
    
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

    @Override
    public void run() {
      List<DistributionMessage> requests = null;
      logger.info("View Creator thread is starting");
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
                viewRequests.wait();
              } catch (InterruptedException e) {
                return;
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
            /*boolean success = */createAndSendView(requests);
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
    boolean createAndSendView(List<DistributionMessage> requests) {
      List<InternalDistributedMember> joinReqs = new ArrayList<InternalDistributedMember>();
      List<InternalDistributedMember> leaveReqs = new ArrayList<InternalDistributedMember>();
      List<InternalDistributedMember> removalReqs = new ArrayList<InternalDistributedMember>();
      List<String> removalReasons = new ArrayList<String>();
      
      NetView oldView = currentView;
      List<InternalDistributedMember> oldMembers;
      if (oldView != null) {
        oldMembers = oldView.getMembers();
      } else {
        oldMembers = Collections.emptyList();
      }
      
      for (DistributionMessage msg: requests) {
        logger.debug("processing request {}", msg);

        InternalDistributedMember mbr = null;
        
        if (msg instanceof JoinRequestMessage) {
          mbr = ((JoinRequestMessage)msg).getMemberID();
          if (!oldMembers.contains(mbr)) {
            joinReqs.add(mbr);
          }
        }
        else if (msg instanceof LeaveRequestMessage) {
          mbr = ((LeaveRequestMessage) msg).getMemberID();
          if (oldMembers.contains(mbr)) {
            leaveReqs.add(mbr);
          }
        }
        else if (msg instanceof RemoveMemberMessage) {
          mbr = ((RemoveMemberMessage) msg).getMemberID();
          if (oldMembers.contains(mbr)) {
            removalReqs.add(mbr);
            removalReasons.add(((RemoveMemberMessage) msg).getReason());
          }
        }
        else {
          // TODO: handle removals
          logger.warn("Unknown membership request encountered: {}", msg);
        }
      }
      
      NetView newView;
      synchronized(viewInstallationLock) {
        int viewNumber = 0;
        List<InternalDistributedMember> mbrs;
        if (currentView == null) {
          mbrs = new ArrayList<InternalDistributedMember>(joinReqs.size());
        } else {
          viewNumber = currentView.getViewId()+1;
          mbrs = new ArrayList<InternalDistributedMember>(currentView.getMembers());
        }
        mbrs.addAll(joinReqs);
        mbrs.removeAll(leaveReqs);
        mbrs.removeAll(removalReqs);
        newView = new NetView(localAddress, viewNumber, mbrs, leaveReqs,
            removalReqs);
      }
      
      for (InternalDistributedMember mbr: joinReqs) {
        mbr.setVmViewId(newView.getViewId());
      }
      // send removal messages before installing the view so we stop
      // getting messages from members that have been kicked out
      sendRemoveMessages(removalReqs, removalReasons, newView);
      
      // we want to always check for quorum loss but don't act on it
      // unless network-partition-detection is enabled
      if ( !(checkForPartition(newView) && quorumRequired) ) {
        sendJoinResponses(joinReqs, newView);
      }

      if (quorumRequired) {
        boolean prepared = false;
        do {
          if (this.shutdown || Thread.currentThread().isInterrupted()) {
            return false;
          }
          prepared = prepareView(newView, joinReqs);
          if (!prepared && quorumRequired) {
            Set<InternalDistributedMember> unresponsive = prepareProcessor.getUnresponsiveMembers();
            try {
              removeHealthyMembers(unresponsive);
            } catch (InterruptedException e) {
              // abort the view if interrupted
              shutdown = true;
              return false;
            }
  
            if (!unresponsive.isEmpty()) {
              List<InternalDistributedMember> failures = new ArrayList<InternalDistributedMember>(currentView.getCrashedMembers().size() + unresponsive.size());
              failures.addAll(unresponsive);

              NetView conflictingView = prepareProcessor.getConflictingView();
              if (conflictingView != null
                  && !conflictingView.getCreator().equals(localAddress)
                  && conflictingView.getViewId() > newView.getViewId()
                  && (lastConflictingView == null || conflictingView.getViewId() > lastConflictingView.getViewId())) {
                lastConflictingView = conflictingView;
                failures.addAll(conflictingView.getCrashedMembers());
              }

              failures.removeAll(removalReqs);
              if (failures.size() > 0) {
                // abort the current view and try again
                removalReqs.addAll(failures);
                newView = new NetView(localAddress, newView.getViewId()+1, newView.getMembers(), leaveReqs,
                    removalReqs);
              }
            }
          }
        } while (!prepared);
      } // quorumRequired
      
      lastConflictingView = null;
      
      sendView(newView, joinReqs);
      return true;
    }
    
    /**
     * performs health checks on the collection of members, removing any that
     * are found to be healthy
     * @param mbrs
     */
    private void removeHealthyMembers(Collection<InternalDistributedMember> mbrs) throws InterruptedException {
      List<Callable<InternalDistributedMember>> checkers = new ArrayList<Callable<InternalDistributedMember>>(mbrs.size()); 
      
      for (InternalDistributedMember mbr: mbrs) {
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
              logger.debug("disregarding lack of acknowledgement from {}", mbr);
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
