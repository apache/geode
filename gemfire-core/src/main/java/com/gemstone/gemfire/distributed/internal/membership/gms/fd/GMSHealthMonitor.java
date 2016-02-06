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
package com.gemstone.gemfire.distributed.internal.membership.gms.fd;

import static com.gemstone.gemfire.internal.DataSerializableFixedID.HEARTBEAT_REQUEST;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.HEARTBEAT_RESPONSE;
import static com.gemstone.gemfire.internal.DataSerializableFixedID.SUSPECT_MEMBERS_MESSAGE;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.jgroups.util.UUID;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.HealthMonitor;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.MessageHandler;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.HeartbeatMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.HeartbeatRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.SuspectMembersMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.SuspectRequest;
import com.gemstone.gemfire.internal.ConnectionWatcher;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;

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
 * to initiate suspect processing for any member. First is checks whether the member is
 * responding or not. Then it informs probable coordinators to remove that member from
 * view.
 * 
 * It has {@link #checkIfAvailable(DistributedMember, String, boolean)} api to see
 * if that member is alive. Then based on removal flag it initiates the suspect processing
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
   * The number of recipients of periodic heartbeats.  The recipients will
   * be selected from the members that are likely to be monitoring this member.
   */
  private static final int NUM_HEARTBEATS = Integer.getInteger("geode.heartbeat-recipients", 2);

  /**
   * Member activity will be recorded per interval/period. Timer task will set interval's starting time.
   * Each interval will be member-timeout/LOGICAL_INTERVAL. LOGICAL_INTERVAL may be configured
   * via a system property with a default of 2. At least 1 interval is needed.
   */
  public static final int LOGICAL_INTERVAL = Integer.getInteger("geode.logical-message-received-interval", 2);

  /** stall time to wait for members leaving concurrently */
  public static final long MEMBER_SUSPECT_COLLECTION_INTERVAL = Long.getLong("geode.suspect-member-collection-interval", 200);

  volatile long currentTimeStamp;
  
  /** this member's ID */
  private InternalDistributedMember localAddress;

  /**
   * Timestamp at which we last had contact from a member
   */
  final ConcurrentMap<InternalDistributedMember, TimeStamp> memberTimeStamps = new ConcurrentHashMap<>();
  
  /**
   * Members currently being suspected and the view they were suspected in
   */
  final private ConcurrentHashMap<InternalDistributedMember, NetView> suspectedMemberInView = new ConcurrentHashMap<>();
  
//  /**
//   * Members undergoing final checks
//   */
//  final private List<InternalDistributedMember> membersInFinalCheck = Collections.synchronizedList(new ArrayList<>(30));

  /**
   * Replies to messages
   */
  final private Map<Integer, Response> requestIdVsResponse = new ConcurrentHashMap<>();
  
  /**
   * Members suspected in a particular view
   */
  final private Map<NetView, Set<SuspectRequest>> viewVsSuspectedMembers = new HashMap<>();

  private ScheduledExecutorService scheduler;

  private ExecutorService checkExecutor;

  /**
   * to stop check scheduler
   */
  private ScheduledFuture<?> monitorFuture;
  
  /** test hook */
  volatile boolean playingDead = false;

  /** test hook */
  volatile boolean beingSick = false;
  
  // For TCP check
  private ExecutorService serverSocketExecutor;
  static final int OK = 0x7B;
  static final int ERROR = 0x00;  
  private volatile int socketPort;
  private volatile ServerSocket serverSocket;
  
  /** Statistics about health monitor */
  protected DMStats stats;

  /**
   * this class is to avoid garbage
   */
  private static class TimeStamp {
    private volatile long timeStamp;
    
    TimeStamp(long timeStamp) {
      this.timeStamp = timeStamp;
    }

    public long getTime() {
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
        TimeStamp nextNeighborTS;
        synchronized(GMSHealthMonitor.this) {
          nextNeighborTS = GMSHealthMonitor.this.memberTimeStamps.get(neighbour);
        }
        
        if (nextNeighborTS == null) {
          TimeStamp customTS = new TimeStamp(currentTime);
          memberTimeStamps.put(neighbour, customTS);
          return;
        }
        
        long interval = memberTimeoutInMillis / GMSHealthMonitor.LOGICAL_INTERVAL;
        long lastTS = currentTime - nextNeighborTS.getTime();
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

  class ClientSocketHandler implements Runnable {

    private Socket socket;

    public ClientSocketHandler(Socket socket) {
      this.socket = socket;
    }

    public void run() {
      try {
        socket.setTcpNoDelay(true);
        DataInputStream in = new DataInputStream(socket.getInputStream());
        OutputStream out = socket.getOutputStream();
        @SuppressWarnings("unused")
        short version = in.readShort();
        int  vmViewId = in.readInt();
        long uuidLSBs = in.readLong();
        long uuidMSBs = in.readLong();
        GMSHealthMonitor.this.stats.incFinalCheckRequestsReceived();
        GMSHealthMonitor.this.stats.incTcpFinalCheckRequestsReceived();
        boolean debug = logger.isDebugEnabled();
        GMSMember gmbr = (GMSMember) GMSHealthMonitor.this.localAddress.getNetMember();
        UUID myUUID = gmbr.getUUID();
        // during reconnect or rapid restart we will have a zero viewId but there may still
        // be an old ID in the membership view that we do not want to respond to
        int myVmViewId = gmbr.getVmViewId();
        if (debug) {
          if (playingDead) {
            logger.debug("simulating sick member in health check");
          } else if (vmViewId == myVmViewId
            && uuidLSBs == myUUID.getLeastSignificantBits()
            && uuidMSBs == myUUID.getMostSignificantBits()) {
            logger.debug("UUID matches my own - sending OK reply");
          } else {
            logger.debug("GMSHealthMonitor my UUID is {},{} received is {},{}.  My viewID is {} received is {}",
              Long.toHexString(myUUID.getMostSignificantBits()),
              Long.toHexString(myUUID.getLeastSignificantBits()),
              Long.toHexString(uuidMSBs), Long.toHexString(uuidLSBs),
              myVmViewId, vmViewId);
          }
        }
        if (!playingDead
            && uuidLSBs == myUUID.getLeastSignificantBits()
            && uuidMSBs == myUUID.getMostSignificantBits()
            && vmViewId == myVmViewId) {
          out.write(OK);
          out.flush();
          socket.shutdownOutput();
          GMSHealthMonitor.this.stats.incFinalCheckResponsesSent();
          GMSHealthMonitor.this.stats.incTcpFinalCheckResponsesSent();
          if (debug) {
            logger.debug("GMSHealthMonitor server socket replied OK.");
          }
        }
        else {
          out.write(ERROR);
          out.flush();
          socket.shutdownOutput();
          GMSHealthMonitor.this.stats.incFinalCheckResponsesSent();
          GMSHealthMonitor.this.stats.incTcpFinalCheckResponsesSent();
          if (debug) {
            logger.debug("GMSHealthMonitor server socket replied ERROR.");
          }
        }
      } catch (IOException e) {
        logger.debug("Unexpected exception", e);
      } catch (RuntimeException e) {
        logger.debug("Unexpected runtime exception", e);
        throw e;
      } catch (Error e) {
        logger.debug("Unexpected error", e);
        throw e;
      } finally {
        if (socket != null) {
          try {
            socket.close();
          } catch (IOException e) {
            logger.info("Unexpected exception", e);
          }
        }
      }
    }
  }

  public GMSHealthMonitor() {

  }

  public static void loadEmergencyClasses() {
  }

  /*
   * It records the member activity for current time interval.
   */
  @Override
  public void contactedBy(InternalDistributedMember sender) {
    TimeStamp cTS = new TimeStamp(currentTimeStamp);
    cTS = memberTimeStamps.putIfAbsent(sender, cTS);
    if (cTS != null) {
      cTS.setTimeStamp(currentTimeStamp);
    }
    if (suspectedMemberInView.remove(sender) != null) {
      logger.info("No longer suspecting {}", sender);
    }
    setNextNeighbor(currentView, null);
  }

  private HeartbeatRequestMessage constructHeartbeatRequestMessage(final InternalDistributedMember mbr) {
    final int reqId = requestId.getAndIncrement();
    final HeartbeatRequestMessage hrm = new HeartbeatRequestMessage(mbr, reqId);
    hrm.setRecipient(mbr);

    return hrm;
  }

  private void checkMember(final InternalDistributedMember mbr) {
    final NetView cv = GMSHealthMonitor.this.currentView;

    // as check may take time
    setNextNeighbor(cv, mbr);

    // we need to check this member
    checkExecutor.execute(new Runnable() {

      @Override
      public void run() {
        // TODO GemFire used the tcp/ip connection but this is using heartbeats
        boolean pinged = GMSHealthMonitor.this.doCheckMember(mbr);
        if (!pinged) {
          suspectedMemberInView.put(mbr, currentView);
          String reason = "Member isn't responding to heartbeat requests";
          GMSHealthMonitor.this.initiateSuspicion(mbr, reason);
        } else {
          logger.trace("Setting next neighbor as member {} has responded.", mbr);
          suspectedMemberInView.remove(mbr);
          // back to previous one
          setNextNeighbor(GMSHealthMonitor.this.currentView, null);
        }
      }
    });

  }

  private void initiateSuspicion(InternalDistributedMember mbr, String reason) {
    SuspectRequest sr = new SuspectRequest(mbr, reason);
    List<SuspectRequest> sl = new ArrayList<SuspectRequest>();
    sl.add(sr);
    sendSuspectRequest(sl);
  }

  /**
   * This method sends heartbeat request to other member and waits for member-timeout
   * time for response. If it doesn't see response then it returns false.
   * @param member
   * @return
   */
  private boolean doCheckMember(InternalDistributedMember member) {
    if (playingDead || beingSick) {
      // a member playingDead should not be sending messages to other
      // members, so we avoid sending heartbeat requests or suspect
      // messages by returning true.
      return true;
    }
    logger.trace("Checking member {}", member);
    final HeartbeatRequestMessage hrm = constructHeartbeatRequestMessage(member);
    final Response pingResp = new Response();
    requestIdVsResponse.put(hrm.getRequestId(), pingResp);
    try {
      Set<InternalDistributedMember> membersNotReceivedMsg = this.services.getMessenger().send(hrm);
      this.stats.incHeartbeatRequestsSent();
      if (membersNotReceivedMsg != null && membersNotReceivedMsg.contains(member)) {
        // member is not part of current view.
        logger.trace("Member {} is not part of current view.", member);
      } else {
        synchronized (pingResp) {
          if (pingResp.getResponseMsg() == null) {
            pingResp.wait(memberTimeout);
          }
          TimeStamp ts = memberTimeStamps.get(member);
          if (pingResp.getResponseMsg() == null) {
            if (isStopping) {
              return true;
            }
            logger.trace("no heartbeat response received from {} and no recent activity", member);
            return false;
          } else {
            logger.trace("received heartbeat from {}", member);
            this.stats.incHeartbeatsReceived();
            if (ts != null) {
              ts.setTimeStamp(System.currentTimeMillis());
            }
            return true;
          }
        }
      }
    } catch (InterruptedException e) {
      logger.debug("GMSHealthMonitor checking thread interrupted, while waiting for response from member: {} .", member);
    } finally {
      requestIdVsResponse.remove(hrm.getRequestId());
    }
    return false;
  }

  /**
   * During final check, establish TCP connection between current member and suspect member.
   * And exchange PING/PONG message to see if the suspect member is still alive.
   * 
   * @param suspectMember member that does not respond to HeartbeatRequestMessage
   * @return true if successfully exchanged PING/PONG with TCP connection, otherwise false.
   */
  private boolean doTCPCheckMember(InternalDistributedMember suspectMember, int port) {
    Socket clientSocket = null;
    try {
      logger.debug("Checking member {} with TCP socket connection {}:{}.", suspectMember, suspectMember.getInetAddress(), port);
      clientSocket = SocketCreator.getDefaultInstance().connect(suspectMember.getInetAddress(), port,
          (int)memberTimeout, new ConnectTimeoutTask(services.getTimer(), memberTimeout), false, -1, false);
      clientSocket.setTcpNoDelay(true);
      return doTCPCheckMember(suspectMember, clientSocket);
    }
    catch (IOException e) {
      logger.debug("Unexpected exception", e);
    } 
    finally {
      try {
        if (clientSocket != null) {
          clientSocket.setSoLinger(true, 0); // abort the connection
          clientSocket.close();
        }
      } catch (IOException e) {
        logger.trace("Unexpected exception", e);
      }
    }
    return false;
  }

  //Package protected for testing purposes
  boolean doTCPCheckMember(InternalDistributedMember suspectMember, Socket clientSocket) {
    try {
      if (clientSocket.isConnected()) {
        clientSocket.setSoTimeout((int) services.getConfig().getMemberTimeout());
        InputStream in = clientSocket.getInputStream();
        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
        GMSMember gmbr = (GMSMember) suspectMember.getNetMember();
        writeMemberToStream(gmbr, out);
        clientSocket.shutdownOutput();
        this.stats.incFinalCheckRequestsSent();
        this.stats.incTcpFinalCheckRequestsSent();
        logger.debug("Connected - reading response from suspect member {}", suspectMember);
        int b = in.read();
        this.stats.incFinalCheckResponsesReceived();
        this.stats.incTcpFinalCheckResponsesReceived();
        logger.debug("Received {}", (b == OK ? "OK" : (b == ERROR ? "ERROR" : b)), suspectMember);
        if (b == OK) {
          TimeStamp ts = memberTimeStamps.get(suspectMember);
          if (ts != null) {
            ts.setTimeStamp(System.currentTimeMillis());
          }
          return true;
        } else {
          //received ERROR
          return false;
        }
      } else {// cannot establish TCP connection with suspect member
        return false;
      }
    } catch (SocketTimeoutException e) {
      logger.debug("Final check TCP/IP connection timed out for suspect member {}", suspectMember);
      return false;
    } catch (IOException e) {
      logger.trace("Unexpected exception", e);
    } 
    return false;
  }
  
  void writeMemberToStream(GMSMember gmbr, DataOutputStream out) throws IOException {
    out.writeShort(Version.CURRENT_ORDINAL);
    out.writeInt(gmbr.getVmViewId());
    out.writeLong(gmbr.getUuidLSBs());
    out.writeLong(gmbr.getUuidMSBs());
    out.flush();
  }
  
  @Override
  public void suspect(InternalDistributedMember mbr, String reason) {
    initiateSuspicion(mbr, reason);
    // Background suspect-collecting thread is currently disabled - it takes too long
//    synchronized (suspectRequests) {
//      SuspectRequest sr = new SuspectRequest((InternalDistributedMember) mbr, reason);
//      if (!suspectRequests.contains(sr)) {
//        logger.info("Suspecting member {}. Reason= {}.", mbr, reason);
//        suspectRequests.add(sr);
//        suspectRequests.notify();
//      }
//    }
  }

  @Override
  public boolean checkIfAvailable(DistributedMember mbr, String reason, boolean initiateRemoval) {
    return inlineCheckIfAvailable(localAddress, currentView, initiateRemoval, (InternalDistributedMember)mbr, reason);
  }

  public void start() {
    scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread th = new Thread(Services.getThreadGroup(), r, "Geode Failure Detection Scheduler");
        th.setDaemon(true);
        return th;
      }
    });

    checkExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
      AtomicInteger threadIdx = new AtomicInteger();

      @Override
      public Thread newThread(Runnable r) {
        int id = threadIdx.getAndIncrement();
        Thread th = new Thread(Services.getThreadGroup(), r, "Geode Failure Detection thread " + id);
        th.setDaemon(true);
        return th;
      }
    });
    Monitor m = this.new Monitor(memberTimeout);
    long delay = memberTimeout / LOGICAL_INTERVAL;
    monitorFuture = scheduler.scheduleAtFixedRate(m, delay, delay, TimeUnit.MILLISECONDS);

//    suspectRequestCollectorThread = this.new RequestCollector<SuspectRequest>("Geode Suspect Message Collector", Services.getThreadGroup(), suspectRequests,
//        new Callback<SuspectRequest>() {
//      @Override
//      public void process(List<SuspectRequest> requests) {
//        GMSHealthMonitor.this.sendSuspectRequest(requests);
//
//      }
//    }, MEMBER_SUSPECT_COLLECTION_INTERVAL);
//    suspectRequestCollectorThread.setDaemon(true);
//    suspectRequestCollectorThread.start();
    
    serverSocketExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
      AtomicInteger threadIdx = new AtomicInteger();

      @Override
      public Thread newThread(Runnable r) {
        int id = threadIdx.getAndIncrement();
        Thread th = new Thread(Services.getThreadGroup(), r, "Geode Failure Detection Server thread " + id);
        th.setDaemon(true);
        return th;
      }
    });

  }

  ServerSocket createServerSocket(InetAddress socketAddress, int[] portRange) {
    ServerSocket serverSocket = null;
    try {
      serverSocket = SocketCreator.getDefaultInstance().createServerSocketUsingPortRange(socketAddress, 50/*backlog*/, true/*isBindAddress*/, false/*useNIO*/, 65536/*tcpBufferSize*/, portRange);
      socketPort = serverSocket.getLocalPort();
    } catch (IOException e) {
      throw new GemFireConfigException("Unable to allocate a failure detection port in the membership-port range", e);
    } catch (SystemConnectException e) {
      throw new GemFireConfigException("Unable to allocate a failure detection port in the membership-port range", e);
    }
    return serverSocket;
  }
  
  /**
   * start the thread that listens for tcp/ip connections and responds
   * to connection attempts
   */
  private void startTcpServer(ServerSocket ssocket) {
    // allocate a socket here so there are no race conditions between knowing the FD
    // socket port and joining the system

    serverSocketExecutor.execute(new Runnable() {
      @Override
      public void run() {
        logger.info("Started failure detection server thread on {}:{}.", ssocket.getInetAddress(), socketPort);
        Socket socket = null;
        try {
          while (!services.getCancelCriterion().isCancelInProgress() 
              && !GMSHealthMonitor.this.isStopping) {
            try {
              socket = ssocket.accept();
              if (GMSHealthMonitor.this.playingDead) {
                continue;
              }
              serverSocketExecutor.execute(new ClientSocketHandler(socket)); //start();  [bruce] I'm seeing a lot of failures due to this thread not being created fast enough, sometimes as long as 30 seconds
            
            } catch (RejectedExecutionException e) {
              // this can happen during shutdown

            } catch (IOException e) {
              if (!isStopping) {
                logger.trace("Unexpected exception", e);
              }
              try {
                if (socket != null) {
                  socket.close();
                }
              } catch (IOException ioe) {
                logger.trace("Unexpected exception", ioe);
              }
            }
          }
          logger.info("GMSHealthMonitor server thread exiting");
        } finally {
          // close the server socket
          if (ssocket != null && !ssocket.isClosed()) {
            try {
              ssocket.close();
              serverSocket = null;
              logger.info("GMSHealthMonitor server socket closed.");
            } catch (IOException e) {
              logger.debug("Unexpected exception", e);
            }
          }
        }
      }
    });
  }
  
  /**
   * start the thread that periodically sends a message to processes
   * that might be watching this process
   */
  private void startHeartbeatThread() {
    checkExecutor.execute(new Runnable() {
      public void run() {
        Thread.currentThread().setName("Geode Heartbeat Sender");
        sendPeriodicHeartbeats();
      }
      private void sendPeriodicHeartbeats() {
        while (!isStopping && !services.getCancelCriterion().isCancelInProgress()) {
          try {
            Thread.sleep(memberTimeout/LOGICAL_INTERVAL);
          } catch (InterruptedException e) {
            return;
          }
          NetView v = currentView;
          if (v != null) {
            List<InternalDistributedMember> mbrs = v.getMembers();
            int index = mbrs.indexOf(localAddress);
            if (index < 0 || mbrs.size() < 2) {
              continue;
            }
            if (!playingDead) {
              sendHeartbeats(mbrs, index);
            }
          }
        }
      }
      
      private void sendHeartbeats(List<InternalDistributedMember> mbrs, int startIndex) {
        InternalDistributedMember coordinator = currentView.getCoordinator();
        if (coordinator != null && !coordinator.equals(localAddress)) {
          HeartbeatMessage message = new HeartbeatMessage(-1);
          message.setRecipient(coordinator);
          try {
            if (isStopping) {
              return;
            }
            services.getMessenger().sendUnreliably(message);
            GMSHealthMonitor.this.stats.incHeartbeatsSent();
          } catch (CancelException e) {
            return;
          }
        }

        int index = startIndex;
        int numSent = 0;
        for (;;) {
          index--;
          if (index < 0) {
            index = mbrs.size()-1;
          }
          InternalDistributedMember mbr = mbrs.get(index);
          if (mbr.equals(localAddress)) {
            break;
          }
          if (mbr.equals(coordinator)) {
            continue;
          }
          if (isStopping) {
            return;
          }
          HeartbeatMessage message = new HeartbeatMessage(-1);
          message.setRecipient(mbr);
          try {
            services.getMessenger().sendUnreliably(message);
            GMSHealthMonitor.this.stats.incHeartbeatsSent();
            numSent++;
            if (numSent >= NUM_HEARTBEATS) {
              break;
            }
          } catch (CancelException e) {
            return;
          }
        }
      } // for (;;)
    });
  }

  public synchronized void installView(NetView newView) {
    synchronized (viewVsSuspectedMembers) {
      viewVsSuspectedMembers.clear();
    }
    for (Iterator<InternalDistributedMember> it=memberTimeStamps.keySet().iterator(); it.hasNext(); ) {
      if (!newView.contains(it.next())) {
        it.remove();
      }
    }
    for (Iterator<InternalDistributedMember> it=suspectedMemberInView.keySet().iterator(); it.hasNext(); ) {
      if (!newView.contains(it.next())) {
        it.remove();
      }
    }
//    for (InternalDistributedMember mbr: newView.getMembers()) {
//      if (!memberVsLastMsgTS.containsKey(mbr)) {
//        CustomTimeStamp customTS = new CustomTimeStamp(System.currentTimeMillis());
//        memberVsLastMsgTS.put(mbr, customTS);
//      }
//    }
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
    if (newView == null) {
      return;
    }
    if (nextTo == null) {
      nextTo = localAddress;
    }

    List<InternalDistributedMember> allMembers = newView.getMembers();
    
    Set<?> checkAllSuspected = new HashSet<>(allMembers);
    checkAllSuspected.removeAll(suspectedMemberInView.keySet());
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
      if (suspectedMemberInView.containsKey(newNeighbor)) {
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

  }

  /*** test method */
  public InternalDistributedMember getNextNeighbor() {
    return nextNeighbor;
  }

  @Override
  public void init(Services s) {
    isStopping = false;
    services = s;
    memberTimeout = s.getConfig().getMemberTimeout();
    this.stats = services.getStatistics();
    services.getMessenger().addHandler(HeartbeatRequestMessage.class, this);
    services.getMessenger().addHandler(HeartbeatMessage.class, this);
    services.getMessenger().addHandler(SuspectMembersMessage.class, this);
  }
  
  @Override
  public void started() {
    setLocalAddress( services.getMessenger().getMemberID());
    serverSocket = createServerSocket(localAddress.getInetAddress(), services.getConfig().getMembershipPortRange());
    startTcpServer(serverSocket);
    startHeartbeatThread();
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

    if (serverSocketExecutor != null) {
      if (serverSocket != null && !serverSocket.isClosed()) {
        try {
          serverSocket.close();
          serverSocket = null;
          logger.info("GMSHealthMonitor server socket is closed in stopServices().");
        }
        catch (IOException e) {
          logger.trace("Unexpected exception", e);
        }
      }      
      serverSocketExecutor.shutdownNow();
      try {
        serverSocketExecutor.awaitTermination(2000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      logger.info("GMSHealthMonitor serverSocketExecutor is " + (serverSocketExecutor.isTerminated() ? "terminated" : "not terminated"));
    }
    
//    if (suspectRequestCollectorThread != null) {
//      suspectRequestCollectorThread.shutdown();
//    }
  }

  /***
   * test method
   */
  public boolean isShutdown() {
    return scheduler.isShutdown() && checkExecutor.isShutdown() && serverSocketExecutor.isShutdown() /*&& !suspectRequestCollectorThread.isAlive()*/;
  }

  /**
   * Test method - check to see if a member is under suspicion
   */
  public boolean isSuspectMember(InternalDistributedMember m) {
    return this.suspectedMemberInView.containsKey(m);
  }

  @Override
  public void stopped() {

  }

  @Override
  public void memberSuspected(InternalDistributedMember initiator, InternalDistributedMember suspect, String reason) {
  }

  @Override
  public void beSick() {
    this.beingSick = true;
  }

  @Override
  public void playDead() {
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
  
  void setLocalAddress(InternalDistributedMember idm) {
    this.localAddress = idm;
  }

  @Override
  public void processMessage(DistributionMessage m) {
    if (isStopping) {
      return;
    }

    logger.trace("processing {}", m);

    switch (m.getDSFID()) {
    case HEARTBEAT_REQUEST:
      if (beingSick || playingDead) {
        logger.debug("sick member is ignoring check request");
      } else {
        processHeartbeatRequest((HeartbeatRequestMessage) m);
      }
      break;
    case HEARTBEAT_RESPONSE:
      if (beingSick || playingDead) {
        logger.debug("sick member is ignoring check response");
      } else {
        processHeartbeat((HeartbeatMessage) m);
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

  private void processHeartbeatRequest(HeartbeatRequestMessage m) {
    
    this.stats.incHeartbeatRequestsReceived();
    
    if (this.isStopping || this.playingDead) {
      return;
    }
    
    // only respond if the intended recipient is this member
    InternalDistributedMember me = localAddress;

    if (me.getVmViewId() >= 0  &&  m.getTarget().equals(me)) {
      HeartbeatMessage hm = new HeartbeatMessage(m.getRequestId());
      hm.setRecipient(m.getSender());
      Set<InternalDistributedMember> membersNotReceivedMsg = services.getMessenger().send(hm);
      this.stats.incHeartbeatsSent();
      if (membersNotReceivedMsg != null && membersNotReceivedMsg.contains(m.getSender())) {
        logger.debug("Unable to send heartbeat to member: {}", m.getSender());
      }
    } else {
      logger.debug("Ignoring heartbeat request intended for {}.  My ID is {}", m.getTarget(), me);
    }
  }

  private void processHeartbeat(HeartbeatMessage m) {
    this.stats.incHeartbeatsReceived();
    if (m.getRequestId() < 0) {
      // a periodic heartbeat
      contactedBy(m.getSender());
    } else {
      Response resp = requestIdVsResponse.get(m.getRequestId());
      logger.trace("Got heartbeat from member {}. {}", m.getSender(), (resp != null ? "Check thread still waiting" : "Check thread is not waiting"));
      if (resp != null) {
        synchronized (resp) {
          resp.setResponseMsg(m);
          resp.notify();
        }
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
    
    this.stats.incSuspectsReceived();
    
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

    // take care of any suspicion of this member by sending a heartbeat back
    if (!playingDead) {
      for (Iterator<SuspectRequest> it = incomingRequest.getMembers().iterator(); it.hasNext(); ) {
        SuspectRequest req = it.next();
        if (req.getSuspectMember().equals(localAddress)) {
          HeartbeatMessage message = new HeartbeatMessage(-1);
          message.setRecipient(sender);
          try {
            services.getMessenger().send(message);
            this.stats.incHeartbeatsSent();
            it.remove();
          } catch (CancelException e) {
            return;
          }
        }
      }
    }

    
    if (cv.getCoordinator().equals(localAddress)) {
      for (SuspectRequest req: incomingRequest.getMembers()) {
        logger.info("received suspect message from {} for {}: {}",
           sender, req.getSuspectMember(), req.getReason());
      }
      checkIfAvailable(sender, sMembers, cv);
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
        checkIfAvailable(sender, smbr, cv);
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
        viewVsMembers.add(sr);
      }
    }
  }

  /**
   * performs a "final" health check on the member.  If failure-detection
   * socket information is available for the member (in the view) then
   * we attempt to connect to its socket and ask if it's the expected member.
   * Otherwise we send a heartbeat request and wait for a reply.
   *  
   * @param initiator
   * @param sMembers
   * @param cv
   * @param initiateRemoval
   */
  private void checkIfAvailable(final InternalDistributedMember initiator,
      List<SuspectRequest> sMembers, final NetView cv) {

//    List<InternalDistributedMember> membersChecked = new ArrayList<>(10);
    try {
      for (int i = 0; i < sMembers.size(); i++) {
        final SuspectRequest sr = sMembers.get(i);
        final InternalDistributedMember mbr = sr.getSuspectMember();

        if (!cv.contains(mbr) /*|| membersInFinalCheck.contains(mbr)*/) {
          continue;
        }

        if (mbr.equals(localAddress)) {
          continue;// self
        }
        
//        membersChecked.add(mbr);

        // suspectMemberInView is now set by the heartbeat monitoring code
        // to allow us to move on from watching members we've already
        // suspected.  Since that code is updating this collection we
        // cannot use it here as an indication that a member is currently
        // undergoing a final check.
        //      NetView view;
        //      view = suspectedMemberInView.putIfAbsent(mbr, cv);

        //      if (view == null || !view.equals(cv)) {
        final String reason = sr.getReason();
        logger.debug("Scheduling final check for member {}; reason={}", mbr, reason);
        // its a coordinator
        checkExecutor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              inlineCheckIfAvailable(initiator, cv, true, mbr,
                  reason);
            } catch (DistributedSystemDisconnectedException e) {
              return;
            } catch (Exception e) {
              logger.info("Unexpected exception while verifying member", e);
            } finally {
              GMSHealthMonitor.this.suspectedMemberInView.remove(mbr);
            }
          }

          
        });
        //      }// scheduling for final check and removing it..
      }
    } finally {
//      membersInFinalCheck.removeAll(membersChecked);
    }
  }

  private boolean inlineCheckIfAvailable(
      final InternalDistributedMember initiator, final NetView cv,
      boolean initiateRemoval,
      final InternalDistributedMember mbr, final String reason) {

    services.memberSuspected(initiator, mbr, reason);
    long startTime = System.currentTimeMillis();
    // for some reason we used to update the timestamp for the member
    // with the startTime, but we don't want to do that because it looks
    // like a heartbeat has been received

    logger.info("Performing final check for suspect member {} reason={}", mbr, reason);
    boolean pinged;
    int port = cv.getFailureDetectionPort(mbr);
    if (port <= 0) {
      logger.info("Unable to locate failure detection port - requesting a heartbeat");
      if (logger.isDebugEnabled()) {
        logger.debug("\ncurrent view: {}\nports: {}", cv, Arrays.toString(cv.getFailureDetectionPorts()));
      }
      pinged = GMSHealthMonitor.this.doCheckMember(mbr);
      GMSHealthMonitor.this.stats.incFinalCheckRequestsSent();
      GMSHealthMonitor.this.stats.incUdpFinalCheckRequestsSent();
      if (pinged) {
        GMSHealthMonitor.this.stats.incFinalCheckResponsesReceived();
        GMSHealthMonitor.this.stats.incUdpFinalCheckResponsesReceived();
      }
    } else {
      pinged = GMSHealthMonitor.this.doTCPCheckMember(mbr, port);
    }

    boolean failed = false;
    if (!pinged && !isStopping) {
      TimeStamp ts = memberTimeStamps.get(mbr);
      if (ts == null || ts.getTime() <= startTime) {
        logger.info("Final check failed - requesting removal of suspect member " + mbr);
        if (initiateRemoval) {
          services.getJoinLeave().remove(mbr, reason);
        }
        failed = true;
      } else {
        logger.info("Final check failed but detected recent message traffic for suspect member " + mbr);
      }
    }
    if (!failed) {
      logger.info("Final check passed for suspect member " + mbr);
    }
    // whether it's alive or not, at this point we allow it to
    // be watched again
    suspectedMemberInView.remove(mbr);
    return !failed;
  }
    
  @Override
  public void memberShutdown(DistributedMember mbr, String reason) {
  }
  
  @Override
  public int getFailureDetectionPort() {
    return this.socketPort;
  }

  private void sendSuspectRequest(final List<SuspectRequest> requests) {
    // the background suspect-collector thread is currently disabled
//    synchronized (suspectRequests) {
//      if (suspectRequests.size() > 0) {
//        for (SuspectRequest sr: suspectRequests) {
//          if (!requests.contains(sr)) {
//            requests.add(sr);
//          }
//        }
//        suspectRequests.clear();
//      }
//    }
    logger.debug("Sending suspect request for members {}", requests);
    List<InternalDistributedMember> recipients;
//  TODO this needs some rethinking - we need the guys near the
//  front of the membership view who aren't preferred for coordinator
//  to see the suspect message.
//    if (v.size() > 20) {
//      HashSet<InternalDistributedMember> filter = new HashSet<InternalDistributedMember>();
//      for (int i = 0; i < requests.size(); i++) {
//        filter.add(requests.get(i).getSuspectMember());
//      }
//      recipients = currentView.getPreferredCoordinators(filter, services.getJoinLeave().getMemberID(), 5);
//    } else {
      recipients = currentView.getMembers();
//    }

    SuspectMembersMessage smm = new SuspectMembersMessage(recipients, requests);
    Set<InternalDistributedMember> failedRecipients;
    try {
      failedRecipients = services.getMessenger().send(smm);
      this.stats.incSuspectsSent();
    } catch (CancelException e) {
      return;
    }

    if (failedRecipients != null && failedRecipients.size() > 0) {
      logger.info("Unable to send suspect message to {}", recipients);
    }
  }

  private static class ConnectTimeoutTask extends TimerTask implements ConnectionWatcher {
    Timer scheduler;
    Socket socket;
    long timeout;
    
    ConnectTimeoutTask(Timer scheduler, long timeout) {
      this.scheduler = scheduler;
      this.timeout = timeout;
    }
    
    @Override
    public void beforeConnect(Socket socket) {
      this.socket = socket;
      scheduler.schedule(this, timeout);
    }

    @Override
    public void afterConnect(Socket socket) {
      cancel();
    }
    
    @Override
    public void run() {
      try {
        if (socket != null) {
          socket.close();
        }
      } catch (IOException e) {
        // ignored - nothing useful to do here
      }
    }
    
  }
  
  public DMStats getStats() {
    return this.stats;
  }
}
