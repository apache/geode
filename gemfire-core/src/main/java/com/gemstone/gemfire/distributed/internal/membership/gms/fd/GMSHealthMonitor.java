package com.gemstone.gemfire.distributed.internal.membership.gms.fd;

import static com.gemstone.gemfire.internal.DataSerializableFixedID.CHECK_REQUEST;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.CHECK_RESPONSE;
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
import java.util.concurrent.ConcurrentMap;
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
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.CheckRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.CheckResponseMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.SuspectMembersMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.SuspectRequest;
import com.gemstone.gemfire.internal.concurrent.ConcurrentHashSet;

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
  volatile private InternalDistributedMember nextNeighbor;

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
  
  /** this member's ID */
  private InternalDistributedMember localAddress;

  final ConcurrentMap<InternalDistributedMember, CustomTimeStamp> memberVsLastMsgTS = new ConcurrentHashMap<>();
  final private Map<Integer, Response> requestIdVsResponse = new ConcurrentHashMap<>();
  final private ConcurrentHashMap<InternalDistributedMember, NetView> suspectedMemberVsView = new ConcurrentHashMap<>();
  final private Map<NetView, Set<SuspectRequest>> viewVsSuspectedMembers = new HashMap<>();

  /**
   * currentSuspects tracks members that we've already checked and
   * did not receive a response from.  This collection keeps us from
   * checking the same member over and over if it's already under
   * suspicion
   */
  final private Set<InternalDistributedMember> currentSuspects = new ConcurrentHashSet<>();

  private ScheduledExecutorService scheduler;

  private ExecutorService checkExecutor;

  List<SuspectRequest> suspectRequests = new ArrayList<SuspectRequest>();
  private RequestCollector<SuspectRequest> suspectRequestCollectorThread;

  /**
   * to stop check scheduler
   */
  private ScheduledFuture<?> monitorFuture;
  
  /** test hook */
  boolean playingDead = false;

  /** test hook */
  boolean beingSick = false;

  public GMSHealthMonitor() {

  }

  public static void loadEmergencyClasses() {
  }

  /*
   * It records the member activity for current time interval.
   */
  @Override
  public void contactedBy(InternalDistributedMember sender) {
    CustomTimeStamp cTS = new CustomTimeStamp(currentTimeStamp);
    cTS = memberVsLastMsgTS.putIfAbsent(sender, cTS);
    if (cTS != null) {
      cTS.setTimeStamp(currentTimeStamp);
    }
    if (currentSuspects.remove(sender)) {
      logger.info("No longer suspecting {}", sender);
      setNextNeighbor(currentView, null);
    }
  }

  /**
   * this class is to avoid garbage
   */
  private static class CustomTimeStamp {
    private volatile long timeStamp;
    
    CustomTimeStamp(long timeStamp) {
      this.timeStamp = timeStamp;
    }

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

      if (GMSHealthMonitor.this.isStopping) {
        return;
      }
      
      InternalDistributedMember neighbour = nextNeighbor;
      
      long currentTime = System.currentTimeMillis();
      //this is the start of interval to record member activity
      GMSHealthMonitor.this.currentTimeStamp = currentTime;

      if (neighbour != null) {
        CustomTimeStamp nextNeighborTS;
        synchronized(GMSHealthMonitor.this) {
          nextNeighborTS = GMSHealthMonitor.this.memberVsLastMsgTS.get(neighbour);
        }

        if (nextNeighborTS == null) {
          CustomTimeStamp customTS = new CustomTimeStamp(currentTime);
          memberVsLastMsgTS.put(neighbour, customTS);
          return;
        }
        
        long interval = memberTimeoutInMillis / GMSHealthMonitor.LOGICAL_INTERVAL;
        long lastTS = currentTime - nextNeighborTS.getTimeStamp();
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

  private CheckRequestMessage constructCheckRequestMessage(final InternalDistributedMember mbr) {
    final int reqId = requestId.getAndIncrement();
    final CheckRequestMessage prm = new CheckRequestMessage(mbr, reqId);
    prm.setRecipient(mbr);

    return prm;
  }

  private void checkMember(final InternalDistributedMember mbr) {
    final NetView cv = GMSHealthMonitor.this.currentView;

    // as check may take time
    setNextNeighbor(cv, mbr);

    // we need to check this member
    checkExecutor.execute(new Runnable() {

      @Override
      public void run() {
        boolean pinged = GMSHealthMonitor.this.doCheckMember(mbr);
        if (!pinged) {
          String reason = "Member isn't responding to health checks";
          GMSHealthMonitor.this.sendSuspectMessage(mbr, reason);
          currentSuspects.add(mbr);
        } else {
          logger.trace("Setting next neighbor as member {} has not responded.", mbr);
          currentSuspects.remove(mbr);
          // back to previous one
          setNextNeighbor(GMSHealthMonitor.this.currentView, null);
        }
      }
    });

  }

  private void sendSuspectMessage(InternalDistributedMember mbr, String reason) {
    if (beingSick || playingDead) {
      logger.debug("sick member is not sending suspect message concerning {}", mbr);
      return;
    }
    logger.info("Sending suspect request {} reason=\"{}\"", mbr, reason);
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
    if (playingDead) {
      return true;
    }
    //TODO: need to some tcp check
    logger.trace("Checking member {}", pingMember);
    final CheckRequestMessage prm = constructCheckRequestMessage(pingMember);
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
          CustomTimeStamp ts = memberVsLastMsgTS.get(pingMember);
          if (pingResp.getResponseMsg() == null) {
            // double check the activity map
            if (isStopping ||
                (ts != null &&
                 ts.getTimeStamp()
                  > (System.currentTimeMillis() - services.getConfig().getMemberTimeout())
                  )) {
              return true;
            }
            return false;
          } else {
            if (ts != null) {
              ts.setTimeStamp(System.currentTimeMillis());
            }
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

  public void start() {
    {
      scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread th = new Thread(Services.getThreadGroup(), r, "GemFire Member-Check Scheduler ");
          th.setDaemon(true);
          return th;
        }
      });
    }
    {
      checkExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        AtomicInteger threadIdx = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
          int id = threadIdx.getAndIncrement();
          Thread th = new Thread(Services.getThreadGroup(), r, "GemFire Member-Check Thread " + id);
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
      suspectRequestCollectorThread = this.new RequestCollector<SuspectRequest>("GemFire Suspect Message Collector", Services.getThreadGroup(), suspectRequests,
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
    currentSuspects.removeAll(newView.getCrashedMembers());
    currentSuspects.removeAll(newView.getShutdownMembers());
    currentView = newView;
    setNextNeighbor(newView, null);
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
  private synchronized void setNextNeighbor(NetView newView, InternalDistributedMember nextTo) {
    if (nextTo == null) {
      nextTo = localAddress;
    }
    boolean sameView = false;

    if (currentView != null &&
        newView.getCreator().equals(currentView.getCreator()) &&
        newView.getViewId() == currentView.getViewId()) {
      sameView = true;
    }

    List<InternalDistributedMember> allMembers = newView.getMembers();
    
    Set<?> checkAllSuspected = new HashSet<>(allMembers);
    checkAllSuspected.removeAll(currentSuspects);
    checkAllSuspected.remove(localAddress);
    if (checkAllSuspected.isEmpty() && allMembers.size() > 1) {
      logger.info("All other members are suspect at this point");
      nextNeighbor = null;
      return;
    }
    
    int index = allMembers.indexOf(nextTo);
    if (index != -1) {
      int nextNeighborIndex = (index + 1) % allMembers.size();
      InternalDistributedMember newNeighbor = allMembers.get(nextNeighborIndex);
      if (currentSuspects.contains(newNeighbor)) {
        setNextNeighbor(newView, newNeighbor);
        return;
      }
      InternalDistributedMember oldNeighbor = nextNeighbor;
      if (oldNeighbor != newNeighbor) {
        logger.debug("Failure detection is now watching {}", newNeighbor);
        nextNeighbor = newNeighbor;
      }
    }
    
    if (nextNeighbor != null && nextNeighbor.equals(localAddress)) {
      nextNeighbor = null;
    }

    if (!sameView || memberVsLastMsgTS.size() == 0) {
      
      if (memberVsLastMsgTS.size() > 0) {
        memberVsLastMsgTS.clear();
      }

      long cts = System.currentTimeMillis();
      for (InternalDistributedMember mbr: allMembers) {
        CustomTimeStamp customTS = new CustomTimeStamp(cts);
        memberVsLastMsgTS.put(mbr, customTS);
      }
    }
  }

  /*** test method */
  public InternalDistributedMember getNextNeighbor() {
    return nextNeighbor;
  }

  @Override
  public void init(Services s) {
    services = s;
    memberTimeout = s.getConfig().getMemberTimeout();
    services.getMessenger().addHandler(CheckRequestMessage.class, this);
    services.getMessenger().addHandler(CheckResponseMessage.class, this);
    services.getMessenger().addHandler(SuspectMembersMessage.class, this);
  }

  @Override
  public void started() {
    this.localAddress = services.getMessenger().getMemberID();
  }

  @Override
  public void stop() {
    stopServices();
  }

  private void stopServices() {
    logger.debug("Stopping HealthMonitor");
    isStopping = true;
    if (monitorFuture != null) {
      monitorFuture.cancel(true);
    }
    if (scheduler != null) {
      scheduler.shutdown();
    }

    Collection<Response> val = requestIdVsResponse.values();
    for (Iterator<Response> it = val.iterator(); it.hasNext();) {
      Response r = it.next();
      synchronized (r) {
        r.notify();
      }
    }

    if (checkExecutor != null) {
      checkExecutor.shutdown();
    }

    if (suspectRequestCollectorThread != null) {
      suspectRequestCollectorThread.shutdown();
    }
  }

  /***
   * test method
   */
  public boolean isShutdown() {
    return scheduler.isShutdown() && checkExecutor.isShutdown() && !suspectRequestCollectorThread.isAlive();
  }

  @Override
  public void stopped() {

  }

  @Override
  public void memberSuspected(InternalDistributedMember initiator, InternalDistributedMember suspect) {
  }

  @Override
  public void beSick() {
    this.beingSick = true;
    sendSuspectMessage(localAddress, "beSick invoked on GMSHealthMonitor");
  }

  @Override
  public void playDead() {
    sendSuspectMessage(localAddress, "playDead invoked on GMSHealthMonitor");
    this.playingDead = true;
  }

  @Override
  public void beHealthy() {
    this.beingSick = false;
    this.playingDead = false;
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

    logger.trace("HealthMonitor processing {}", m);

    switch (m.getDSFID()) {
    case CHECK_REQUEST:
      if (beingSick || playingDead) {
        logger.debug("sick member is ignoring check request");
      } else {
        processCheckRequest((CheckRequestMessage) m);
      }
      break;
    case CHECK_RESPONSE:
      if (beingSick || playingDead) {
        logger.debug("sick member is ignoring check response");
      } else {
        processCheckResponse((CheckResponseMessage) m);
      }
      break;
    case SUSPECT_MEMBERS_MESSAGE:
      if (beingSick || playingDead) {
        logger.debug("sick member is ignoring suspect message");
      } else {
        processSuspectMembersRequest((SuspectMembersMessage) m);
      }
      break;
    default:
      throw new IllegalArgumentException("unknown message type: " + m);
    }
  }

  private void processCheckRequest(CheckRequestMessage m) {
    
    if (this.isStopping || this.playingDead) {
      return;
    }
    
    // only respond if the intended recipient is this member
    InternalDistributedMember me = localAddress;
    // TODO the first part of this check should be removed.
    // because a restarted server will probably have the same
    // membership port as it had in its last incarnation, causing
    // delays in removing the old member ID from the view.
    if (me.getVmViewId() < 0 || m.getTarget().equals(me)) {
      CheckResponseMessage prm = new CheckResponseMessage(m.getRequestId());
      prm.setRecipient(m.getSender());
      Set<InternalDistributedMember> membersNotReceivedMsg = services.getMessenger().send(prm);
      if (membersNotReceivedMsg != null && membersNotReceivedMsg.contains(m.getSender())) {
        logger.debug("Unable to send check response to member: {}", m.getSender());
      }
    } else {
      logger.debug("Ignoring check request intended for {}.  My ID is {}", m.getTarget(), me);
    }
  }

  private void processCheckResponse(CheckResponseMessage m) {
    Response resp = requestIdVsResponse.get(m.getRequestId());
    logger.trace("Got check response from member {}. {}", m.getSender(), (resp != null ? "Check Thread still waiting" : "Check thread is not waiting"));
    if (resp != null) {
      synchronized (resp) {
        resp.setResponseMsg(m);
        resp.notify();
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

    InternalDistributedMember sender = incomingRequest.getSender();
    int viewId = sender.getVmViewId();
    if (cv.getViewId() >= viewId && !cv.contains(incomingRequest.getSender())) {
      logger.info("Membership ignoring suspect request for " + incomingRequest + " from non-member " + incomingRequest.getSender());
      services.getJoinLeave().remove(sender, "this process is initiating suspect processing but is no longer a member");
      return;
    }

    if (cv.getCoordinator().equals(localAddress)) {
      for (SuspectRequest req: incomingRequest.getMembers()) {
        logger.info("received suspect message from {} for {}: {}",
           sender, req.getSuspectMember(), req.getReason());
      }
      doFinalCheck(sender, sMembers, cv, localAddress);
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
        for (SuspectRequest req: incomingRequest.getMembers()) {
          logger.info("received suspect message from {} for {}: {}",
             sender, req.getSuspectMember(), req.getReason());
        }
        doFinalCheck(sender, smbr, cv, localAddress);
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
      for (SuspectRequest sr: sMembers) {       
        if (!viewVsMembers.contains(sr)) {
          viewVsMembers.add(sr);
        }
      }
    }
  }

  private void doFinalCheck(final InternalDistributedMember initiator,
      List<SuspectRequest> sMembers, NetView cv, InternalDistributedMember localAddress) {
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
        logger.debug("Scheduling final check for member {}; reason={}", mbr, reason);
        // its a coordinator
        checkExecutor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              services.memberSuspected(initiator, mbr);
              long startTime = System.currentTimeMillis();
              CustomTimeStamp ts = new CustomTimeStamp(startTime);
              memberVsLastMsgTS.put(mbr, ts);

              logger.info("Performing final check for suspect member {} reason={}", mbr, reason);
              boolean pinged = GMSHealthMonitor.this.doCheckMember(mbr);
              logger.info("Final check {}", pinged? "succeeded" : "failed");
              
              if (!pinged && !isStopping) {
                ts = memberVsLastMsgTS.get(mbr);
                if (ts == null || ts.getTimeStamp() <= startTime) {
                  services.getJoinLeave().remove(mbr, reason);
                }
              }
              // whether it's alive or not, at this point we allow it to
              // be watched again
              contactedBy(mbr);
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
      logger.debug("Suspect thread is starting");
      long okayToSendSuspectRequest = System.currentTimeMillis() + timeout;
      try {
        for (;;) {
          synchronized (listToTrack) {
            if (shutdown || services.getCancelCriterion().isCancelInProgress()) {
              return;
            }
            if (listToTrack.isEmpty()) {
              try {
                logger.trace("Result collector is waiting");
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
            if (logger != null && logger.isDebugEnabled()) {
              logger.debug("Health Monitor is sending {} member suspect requests to coordinator", requests.size());
            }
            callback.process(requests);
            requests = null;
          }
        }
      } finally {
        shutdown = true;
        logger.debug("Suspect thread is stopped");
      }
    }
  }

  private void sendSuspectRequest(final List<SuspectRequest> requests) {
    if (beingSick || playingDead) {
      logger.debug("sick member is not sending suspect request");
      return;
    }
    logger.debug("Sending suspect request for members {}", requests);
    synchronized (suspectRequests) {
      if (suspectRequests.size() > 0) {
        for (SuspectRequest sr: suspectRequests) {
          if (!requests.contains(sr)) {
            requests.add(sr);
          }
        }
        suspectRequests.clear();
      }
    }
    List<InternalDistributedMember> recipients;
//    if (v.size() > 20) {
//      // TODO this needs some rethinking - we need the guys near the
//      // front of the membership view who aren't preferred for coordinator
//      // to see the suspect message.
//      HashSet<InternalDistributedMember> filter = new HashSet<InternalDistributedMember>();
//      for (int i = 0; i < requests.size(); i++) {
//        filter.add(requests.get(i).getSuspectMember());
//      }
//      recipients = currentView.getPreferredCoordinators(filter, services.getJoinLeave().getMemberID(), 5);
//    } else {
      recipients = currentView.getMembers();
//    }

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
