package com.gemstone.gemfire.distributed.internal.membership.gms.fd;

import static com.gemstone.gemfire.internal.DataSerializableFixedID.PING_REQUEST;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.PING_RESPONSE;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.SUSPECT_MEMBERS_MESSAGE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.HealthMonitor;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.MessageHandler;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.PingRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.PingResponseMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.SuspectMembersMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.SuspectRequest;

/**
 * Failure Detection
 * 
 * This class make sure that each member is alive and communicating to this member.
 * To make sure that we create the ring of members based on current view. On this
 * ring, each member make sure that next-member in ring is communicating with it.
 * For that we record last message timestamp from next-member. And if it sees this
 * member has not communicated in last period(member-timeout) then we check whether
 * this member is still alive or not. Based on that we informed probable coordinators
 * to remove that member from view.
 * 
 * It has {@link #suspect(InternalDistributedMember, String)} api, which can be used
 * to initiate suspect processing for any member. First is checks whether member is
 * responding or not. Then it informs to probable coordinators to remove that member from
 * view.
 * 
 * It has {@link #checkIfAvailable(DistributedMember, String, boolean)} api to see
 * if that member is alive. Then based on removal flag it initiate the suspect processing
 * for that member.
 * 
 * */
public class GMSHealthMonitor implements HealthMonitor, MessageHandler {

  private Services services;
  volatile private NetView currentView;
  volatile private InternalDistributedMember nextNeighbour;

  long memberTimeout;
  volatile private boolean isStopping = false;
  private final AtomicInteger requestId = new AtomicInteger();

  /** membership logger */
  private static Logger logger = Services.getLogger();

  /**
   * Member activity will be recorded per interval/period. Timer task will set interval's starting time.
   * Each interval will be member-timeout/LOGICAL_INTERVAL. LOGICAL_INTERVAL will be configured
   * via system property. Default will be 10. Atleast 1 interval is needed.
   */
  private static final int LOGICAL_INTERVAL = Integer.getInteger("geode.logical-message-received-interval", 10) > 1 ? Integer.getInteger(
      "geode.logical-message-received-interval", 10) : 10;

  /** stall time to wait for members leaving concurrently */
  public static final long MEMBER_SUSPECT_COLLECTION_INTERVAL = Long.getLong("geode.suspect-member-collection-interval", 200);

  volatile long currentTimeStamp;

  final private Map<InternalDistributedMember, CustomTimeStamp> memberVsLastMsgTS = new ConcurrentHashMap<>();
  final private Map<Integer, Response> requestIdVsResponse = new ConcurrentHashMap<>();
  final private ConcurrentHashMap<InternalDistributedMember, NetView> suspectedMemberVsView = new ConcurrentHashMap<>();
  final private Map<NetView, Set<SuspectRequest>> viewVsSuspectedMembers = new HashMap<>();

  private ScheduledExecutorService scheduler;

  private ExecutorService pingExecutor;

  List<SuspectRequest> suspectRequests = new ArrayList<SuspectRequest>();
  private RequestCollector<SuspectRequest> suspectRequestCollectorThread;

  /**
   * to stop check scheduler
   */
  private ScheduledFuture monitorFuture;

  public GMSHealthMonitor() {

  }

  public static void loadEmergencyClasses() {
  }

  /*
   * It records the member activity for current time interval.
   */
  @Override
  public void contactedBy(InternalDistributedMember sender) {
    CustomTimeStamp cTS = memberVsLastMsgTS.get(sender);
    if (cTS != null) {
      cTS.setTimeStamp(currentTimeStamp);
    }
  }

  /**
   * this class is to avoid garbage
   */
  private static class CustomTimeStamp {
    volatile long timeStamp;

    public long getTimeStamp() {
      return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
      this.timeStamp = timeStamp;
    }
  }

  /***
   * This class sets start interval timestamp to record the activity of all members.
   * That is used by {@link GMSHealthMonitor#contactedBy(InternalDistributedMember)} to
   * record the activity of member.
   * 
   * It initiates the suspect processing for next neighbour if it doesn't see any activity from that
   * member in last interval(member-timeout)
   */
  private class Monitor implements Runnable {
    final long memberTimeoutInMillis;

    public Monitor(long memberTimeout) {
      memberTimeoutInMillis = memberTimeout;
    }

    @Override
    public void run() {

      InternalDistributedMember neighbour = nextNeighbour;
      if (GMSHealthMonitor.this.isStopping) {
        return;
      }
      long currentTime = System.currentTimeMillis();
      //this is the start of interval to record member activity
      GMSHealthMonitor.this.currentTimeStamp = currentTime;

      if (neighbour != null) {
        CustomTimeStamp nextNeighbourTS;
        synchronized(GMSHealthMonitor.this) {
          nextNeighbourTS = GMSHealthMonitor.this.memberVsLastMsgTS.get(neighbour);
        }

        if (nextNeighbourTS == null) {
          CustomTimeStamp customTS = new CustomTimeStamp();
          customTS.setTimeStamp(System.currentTimeMillis());
          memberVsLastMsgTS.put(neighbour, customTS);
          return;
        }
        
        long interval = memberTimeoutInMillis / GMSHealthMonitor.LOGICAL_INTERVAL;
        long lastTS = currentTime - nextNeighbourTS.getTimeStamp();
        if (lastTS + interval >= memberTimeoutInMillis) {
          logger.trace("Checking member {} ", neighbour);
          // now do check request for this member;
          checkMember(neighbour);
        }
      }
    }
  }

  /***
   * Check thread waits on this object for response. It puts requestId in requestIdVsResponse map.
   * Response will have requestId, which is used to get ResponseObject. Then it is used to
   * notify waiting thread.
   */
  private class Response {
    private DistributionMessage responseMsg;

    public DistributionMessage getResponseMsg() {
      return responseMsg;
    }

    public void setResponseMsg(DistributionMessage responseMsg) {
      this.responseMsg = responseMsg;
    }

  }

  private PingRequestMessage constructPingRequestMessage(final InternalDistributedMember pingMember) {
    final int reqId = requestId.getAndIncrement();
    final PingRequestMessage prm = new PingRequestMessage(pingMember, reqId);
    prm.setRecipient(pingMember);

    return prm;
  }

  private void checkMember(final InternalDistributedMember pingMember) {
    final NetView cv = GMSHealthMonitor.this.currentView;

    // as ping may take time
    setNextNeighbour(cv, pingMember);

    // we need to ping this member
    pingExecutor.execute(new Runnable() {

      @Override
      public void run() {
        boolean pinged = GMSHealthMonitor.this.doCheckMember(pingMember);
        if (!pinged) {
          String reason = String.format("Member isn't responding to check message: %s", pingMember);
          GMSHealthMonitor.this.sendSuspectMessage(pingMember, reason);
        } else {
          logger.trace("Setting next neighbour as member {} has not responded.", pingMember);
          // back to previous one
          setNextNeighbour(GMSHealthMonitor.this.currentView, null);
        }
      }
    });

  }

  private void sendSuspectMessage(InternalDistributedMember mbr, String reason) {
    logger.debug("Suspecting {} reason=\"{}\"", mbr, reason);
    SuspectRequest sr = new SuspectRequest(mbr, reason);
    List<SuspectRequest> sl = new ArrayList<SuspectRequest>();
    sl.add(sr);
    sendSuspectRequest(sl);
  }

  /**
   * This method sends check request to other member and waits for {@link #MEMBER_CHECK_TIMEOUT}
   * time for response. If it doesn't see response then it returns false.
   * @param pingMember
   * @return
   */
  private boolean doCheckMember(InternalDistributedMember pingMember) {
    //TODO: need to some tcp check
    logger.trace("Checking member {}", pingMember);
    final PingRequestMessage prm = constructPingRequestMessage(pingMember);
    final Response pingResp = new Response();
    requestIdVsResponse.put(prm.getRequestId(), pingResp);
    try {
      Set<InternalDistributedMember> membersNotReceivedMsg = this.services.getMessenger().send(prm);
      // TODO: send is throwing exception
      if (membersNotReceivedMsg != null && membersNotReceivedMsg.contains(pingMember)) {
        // member is not part of current view.
        logger.trace("Member {} is not part of current view.", pingMember);
      } else {
        synchronized (pingResp) {
          if (pingResp.getResponseMsg() == null) {
            pingResp.wait(services.getConfig().getMemberTimeout());
          }
          if (pingResp.getResponseMsg() == null) {
            // double check the activity log
            CustomTimeStamp ts = memberVsLastMsgTS.get(pingMember);
            if (ts != null &&
                ts.getTimeStamp()
                  > (System.currentTimeMillis() - services.getConfig().getMemberTimeout())) {
              return true;
            }
            return false;
          } else {
            return true;
          }
        }
      }
    } catch (InterruptedException e) {
      logger.debug("GMSHealthMonitor checking thread interrupted, while waiting for response from member: {} .", pingMember);
    } finally {
      requestIdVsResponse.remove(prm.getRequestId());
    }
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.distributed.internal.membership.gms.fd.HealthMonitor#suspectMember(com.gemstone.gemfire.distributed.DistributedMember,
   * java.lang.String)
   */
  @Override
  public void suspect(InternalDistributedMember mbr, String reason) {
    synchronized (suspectRequests) {
      SuspectRequest sr = new SuspectRequest((InternalDistributedMember) mbr, reason);
      if (!suspectRequests.contains(sr)) {
        logger.info("Suspecting member {}. Reason= {}.", mbr, reason);
        suspectRequests.add(sr);
        suspectRequests.notify();
      }
    }
  }

  @Override
  public boolean checkIfAvailable(DistributedMember mbr, String reason, boolean initiateRemoval) {
    boolean pinged = doCheckMember((InternalDistributedMember) mbr);
    if (!pinged && initiateRemoval) {
      suspect((InternalDistributedMember) mbr, reason);
    }
    return pinged;
  }

  public void playDead(boolean b) {

  }

  public void start() {
    {
      scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread th = new Thread(Services.getThreadGroup(), r, "Member-Check Scheduler ");
          th.setDaemon(true);
          return th;
        }
      });
    }
    {
      pingExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        AtomicInteger threadIdx = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
          int id = threadIdx.getAndIncrement();
          Thread th = new Thread(Services.getThreadGroup(), r, "Member-Check Thread " + id);
          th.setDaemon(true);
          return th;
        }
      });
    }
    {
      Monitor m = this.new Monitor(memberTimeout);
      long delay = memberTimeout / LOGICAL_INTERVAL;
      monitorFuture = scheduler.scheduleAtFixedRate(m, delay, delay, TimeUnit.MILLISECONDS);
    }

    {
      suspectRequestCollectorThread = this.new RequestCollector<SuspectRequest>("Suspect message collector thread", Services.getThreadGroup(), suspectRequests,
          new Callback<SuspectRequest>() {
            @Override
            public void process(List<SuspectRequest> requests) {
              GMSHealthMonitor.this.sendSuspectRequest(requests);

            }
          }, MEMBER_SUSPECT_COLLECTION_INTERVAL);
      suspectRequestCollectorThread.setDaemon(true);
      suspectRequestCollectorThread.start();
    }
  }

  public synchronized void installView(NetView newView) {
    synchronized (viewVsSuspectedMembers) {
      viewVsSuspectedMembers.clear();
    }
    setNextNeighbour(newView, null);
    currentView = newView;
  }

  /***
   * This method sets next neighbour which it needs to watch in current view.
   * 
   * if nextTo == null
   * then it watches member next to it.
   * 
   * It becomes null when we suspect current neighbour, during that time it watches
   * member next to suspect member.
   */
  private synchronized void setNextNeighbour(NetView newView, InternalDistributedMember nextTo) {
    if (nextTo == null) {
      nextTo = services.getJoinLeave().getMemberID();
    }
    boolean sameView = false;

    if (currentView != null &&
        newView.getCreator().equals(currentView.getCreator()) &&
        newView.getViewId() == currentView.getViewId()) {
      sameView = true;
    }

    List<InternalDistributedMember> allMembers = newView.getMembers();
    int index = allMembers.indexOf(nextTo);
    if (index != -1) {
      int nextNeighbourIndex = (index + 1) % allMembers.size();
      nextNeighbour = allMembers.get(nextNeighbourIndex);
      logger.trace("Next neighbour to check is {}", nextNeighbour);
    }

    if (!sameView || memberVsLastMsgTS.size() == 0) {
      
      if (memberVsLastMsgTS.size() > 0) {
        memberVsLastMsgTS.clear();
      }

      long cts = System.currentTimeMillis();
      for (InternalDistributedMember mbr: allMembers) {
        CustomTimeStamp customTS = new CustomTimeStamp();
        customTS.setTimeStamp(cts);
        memberVsLastMsgTS.put(mbr, customTS);
      }
    }
  }

  /*** test method */
  public InternalDistributedMember getNextNeighbour() {
    return nextNeighbour;
  }

  @Override
  public void init(Services s) {
    services = s;
    memberTimeout = s.getConfig().getMemberTimeout();
    services.getMessenger().addHandler(PingRequestMessage.class, this);
    services.getMessenger().addHandler(PingResponseMessage.class, this);
    services.getMessenger().addHandler(SuspectMembersMessage.class, this);
  }

  @Override
  public void started() {

  }

  @Override
  public void stop() {
    stopServices();
  }

  private void stopServices() {
    logger.debug("Stopping HealthMonitor");
    isStopping = true;
    {
      monitorFuture.cancel(true);
      scheduler.shutdown();
    }
    {
      Collection<Response> val = requestIdVsResponse.values();
      for (Iterator<Response> it = val.iterator(); it.hasNext();) {
        Response r = it.next();
        synchronized (r) {
          r.notify();
        }
      }

      pingExecutor.shutdown();
    }
    {
      suspectRequestCollectorThread.shutdown();
    }
  }

  /***
   * test method
   */
  public boolean isShutdown() {
    return scheduler.isShutdown() && pingExecutor.isShutdown() && !suspectRequestCollectorThread.isAlive();
  }

  @Override
  public void stopped() {

  }

  @Override
  public void beSick() {

  }

  @Override
  public void playDead() {

  }

  @Override
  public void beHealthy() {

  }

  @Override
  public void emergencyClose() {
    stopServices();
  }

  @Override
  public void processMessage(DistributionMessage m) {
    if (isStopping) {
      return;
    }

    logger.debug("HealthMonitor processing {}", m);

    switch (m.getDSFID()) {
    case PING_REQUEST:
      processPingRequest((PingRequestMessage) m);
      break;
    case PING_RESPONSE:
      processPingResponse((PingResponseMessage) m);
      break;
    case SUSPECT_MEMBERS_MESSAGE:
      processSuspectMembersRequest((SuspectMembersMessage) m);
      break;
    default:
      throw new IllegalArgumentException("unknown message type: " + m);
    }
  }

  private void processPingRequest(PingRequestMessage m) {
    // only respond if the intended recipient is this member
    InternalDistributedMember me = services.getMessenger().getMemberID();
    if (me.getVmViewId() < 0 || m.getTarget().equals(me)) {
      PingResponseMessage prm = new PingResponseMessage(m.getRequestId());
      prm.setRecipient(m.getSender());
      Set<InternalDistributedMember> membersNotReceivedMsg = services.getMessenger().send(prm);
      // TODO: send is throwing exception right now
      if (membersNotReceivedMsg != null && membersNotReceivedMsg.contains(m.getSender())) {
        logger.debug("Unable to send check response to member: {}", m.getSender());
      }
    } else {
      logger.debug("Ignoring ping request intended for {}.  My ID is {}", m.getTarget(), me);
    }
  }

  private void processPingResponse(PingResponseMessage m) {
    Response pingResp = requestIdVsResponse.get(m.getRequestId());
    logger.debug("Got check response from member {}. {}", m.getSender(), (pingResp != null ? "Check Thread still waiting" : "Check thread is not waiting"));
    if (pingResp != null) {
      synchronized (pingResp) {
        pingResp.setResponseMsg(m);
        pingResp.notify();
      }
    }
  }

  /**
   * Process a Suspect request from another member. This may cause this member
   * to become the new membership coordinator.
   * it will to final check on that member and then it will send remove request
   * for that member
   * 
   * @param incomingRequest
   */
  private void processSuspectMembersRequest(SuspectMembersMessage incomingRequest) {
    NetView cv = currentView;

    if (cv == null) {
      return;
    }

    List<SuspectRequest> sMembers = incomingRequest.getMembers();

    if (!cv.contains(incomingRequest.getSender())) {
      logger.info("Membership ignoring suspect request for " + incomingRequest + " from non-member " + incomingRequest.getSender());
      return;
    }

    InternalDistributedMember localAddress = services.getJoinLeave().getMemberID();

    if (cv.getCoordinator().equals(localAddress)) {
      doFinalCheck(sMembers, cv, localAddress);
    }// coordinator ends
    else {

      NetView check = new NetView(cv, cv.getViewId() + 1);
      ArrayList<SuspectRequest> smbr = new ArrayList<SuspectRequest>();
      synchronized (viewVsSuspectedMembers) {
        recordSuspectRequests(sMembers, cv);
        Set<SuspectRequest> viewVsMembers = viewVsSuspectedMembers.get(cv);
        Iterator<SuspectRequest> itr = viewVsMembers.iterator();
        while (itr.hasNext()) {
          SuspectRequest sr = itr.next();
          check.remove(sr.getSuspectMember());
          smbr.add(sr);
        }
      }

      InternalDistributedMember coordinator = check.getCoordinator();
      if (coordinator != null && coordinator.equals(localAddress)) {
        // new coordinator
        doFinalCheck(smbr, cv, localAddress);
      } else {
        recordSuspectRequests(sMembers, cv);
      }
    }

  }

  /***
   * This method make sure that records suspectRequest. We need to make sure this
   * on preferred coordinators, as elder coordinator might be in suspected list next. 
   * @param sMembers
   * @param cv
   */
  private void recordSuspectRequests(List<SuspectRequest> sMembers, NetView cv) {
    // record suspect requests
    Set<SuspectRequest> viewVsMembers = null;
    synchronized (viewVsSuspectedMembers) {
      viewVsMembers = viewVsSuspectedMembers.get(cv);
      if (viewVsMembers == null) {
        viewVsMembers = new HashSet<SuspectRequest>();
        viewVsSuspectedMembers.put(cv, viewVsMembers);
      }
      for (int i = 0; i < sMembers.size(); i++) {
        SuspectRequest sr = sMembers.get(i);
        if (!viewVsMembers.contains(sr)) {
          viewVsMembers.add(sr);
        }
      }
    }
  }

  private void doFinalCheck(List<SuspectRequest> sMembers, NetView cv, InternalDistributedMember localAddress) {
    for (int i = 0; i < sMembers.size(); i++) {
      final SuspectRequest sr = sMembers.get(i);
      final InternalDistributedMember mbr = sr.getSuspectMember();

      if (!cv.contains(mbr)) {
        continue;
      }

      if (mbr.equals(localAddress)) {
        continue;// self
      }

      NetView view;
      view = suspectedMemberVsView.putIfAbsent(mbr, cv);

      if (view == null || !view.equals(cv)) {
        final String reason = sr.getReason();
        logger.debug("Doing final check for member {}", mbr);
        // its a coordinator
        pingExecutor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              logger.debug("Doing final check for member {}", mbr);
              boolean pinged = GMSHealthMonitor.this.doCheckMember(mbr);
              if (!pinged) {
                GMSHealthMonitor.this.services.getJoinLeave().remove(mbr, reason);
              }
            } catch (DistributedSystemDisconnectedException e) {
              return;
            } catch (Exception e) {
              logger.info("Unexpected exception while verifying member", e);
            } finally {
              GMSHealthMonitor.this.suspectedMemberVsView.remove(mbr);
            }
          }
        });
      }// scheduling for final check and removing it..
    }
  }

  interface Callback<T> {
    public void process(List<T> requests);
  }

  /***
   * this thread will collect suspect message for some time interval
   * then it send message to current coordinator first if its not in
   * suspected list. if its in then it will send message to next probable
   * coordinator. NOTE: this thread will not check-server for verification
   * assuming many servers are going down and lets coordinator deals with it.
   * 
   * Should we wait for ack from coordinator/probable coordinator that I got
   * request to suspect these members.
   * 
   */
  class RequestCollector<T> extends Thread {
    volatile boolean shutdown = false;
    final List<T> listToTrack;
    final Callback<T> callback;
    final long timeout;

    public RequestCollector(String name, ThreadGroup tg, List<T> l, Callback<T> c, long t) {
      super(tg, name);
      listToTrack = l;
      callback = c;
      timeout = t;
    }

    void shutdown() {
      shutdown = true;
      synchronized (listToTrack) {
        listToTrack.notify();
        interrupt();
      }
    }

    boolean isShutdown() {
      return shutdown;
    }

    @Override
    public void run() {
      List<T> requests = null;
      logger.info("Suspect thread is starting");
      long okayToSendSuspectRequest = System.currentTimeMillis() + timeout;
      try {
        for (;;) {
          synchronized (listToTrack) {
            if (shutdown || services.getCancelCriterion().isCancelInProgress()) {
              return;
            }
            if (listToTrack.isEmpty()) {
              try {
                logger.debug("Result collector is waiting");
                listToTrack.wait();
              } catch (InterruptedException e) {
                return;
              }
            } else {
              long now = System.currentTimeMillis();
              if (now < okayToSendSuspectRequest) {
                // sleep to let more suspect requests arrive
                try {
                  sleep(okayToSendSuspectRequest - now);
                  continue;
                } catch (InterruptedException e) {
                  return;
                }
              } else {
                if (requests == null) {
                  requests = new ArrayList<T>(listToTrack);
                } else {
                  requests.addAll(listToTrack);
                }
                listToTrack.clear();
                okayToSendSuspectRequest = System.currentTimeMillis() + timeout;
              }
            }
          } // synchronized
          if (requests != null && !requests.isEmpty()) {
            if (logger != null && logger.isDebugEnabled())
              logger.debug("Health Monitor is sending {} member suspect requests to coordinator", requests.size());
            callback.process(requests);
            requests = null;
          }
        }
      } finally {
        shutdown = true;
        logger.info("Suspect thread is stopped");
      }
    }
  }

  private void sendSuspectRequest(final List<SuspectRequest> requests) {
    logger.debug("Sending suspect request for members {}", requests);
    synchronized (suspectRequests) {
      if (suspectRequests.size() > 0) {
        for (int i = 0; i < suspectRequests.size(); i++) {
          SuspectRequest sr = suspectRequests.get(0);
          if (!requests.contains(sr)) {
            requests.add(sr);
          }
        }
        suspectRequests.clear();
      }
    }
    HashSet<InternalDistributedMember> filter = new HashSet<InternalDistributedMember>();
    for (int i = 0; i < requests.size(); i++) {
      filter.add(requests.get(i).getSuspectMember());
    }
    List<InternalDistributedMember> recipients = currentView.getPreferredCoordinators(filter, services.getJoinLeave().getMemberID(), 5);

    SuspectMembersMessage rmm = new SuspectMembersMessage(recipients, requests);
    Set<InternalDistributedMember> failedRecipients;
    try {
      failedRecipients = services.getMessenger().send(rmm);
    } catch (DistributedSystemDisconnectedException e) {
      return;
    }

    if (failedRecipients != null && failedRecipients.size() > 0) {
      logger.info("Unable to send suspect message to {}", recipients);
    }
  }

  @Override
  public void memberShutdown(DistributedMember mbr, String reason) {
    // TODO Auto-generated method stub
    
  }
}
