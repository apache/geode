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
package org.apache.geode.distributed.internal.membership.gms.fd;


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
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.jgroups.util.UUID;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.MembershipClosedException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.HealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.messages.AbstractGMSMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.FinalCheckPassedMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.SuspectMembersMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.SuspectRequest;
import org.apache.geode.distributed.internal.tcpserver.ConnectionWatcher;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.lang.JavaWorkarounds;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Failure Detection
 * <p>
 * This class make sure that each member is alive and communicating to this member. To make sure
 * that we create the ring of members based on current view. On this ring, each member make sure
 * that next-member in ring is communicating with it. For that we record last message timestamp from
 * next-member. And if it sees this member has not communicated in last period(member-timeout) then
 * we check whether this member is still alive or not. Based on that we informed probable
 * coordinators to remove that member from view.
 * <p>
 * It has {@link #suspect(MemberIdentifier, String)} api, which can be used to initiate
 * suspect processing for any member. First is checks whether the member is responding or not. Then
 * it informs probable coordinators to remove that member from view.
 * <p>
 * It has {@link HealthMonitor#checkIfAvailable(MemberIdentifier, String, boolean)} api to
 * see if that member is
 * alive. Then based on removal flag it initiates the suspect processing for that member.
 */
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "NullableProblems"})
public class GMSHealthMonitor<ID extends MemberIdentifier> implements HealthMonitor<ID> {

  private final TcpSocketCreator socketCreator;
  private Services<ID> services;
  private volatile GMSMembershipView<ID> currentView;
  private volatile ID nextNeighbor;

  long memberTimeout;
  private volatile boolean isStopping = false;
  private final AtomicInteger requestId = new AtomicInteger();

  /**
   * membership logger
   */
  private static final Logger logger = Services.getLogger();

  /**
   * The number of recipients of periodic heartbeats. The recipients will be selected from the
   * members that are likely to be monitoring this member.
   */
  private static final int NUM_HEARTBEATS = Integer.getInteger("geode.heartbeat-recipients", 2);

  /**
   * Member activity will be recorded per interval/period. Timer task will set interval's starting
   * time. Each interval will be member-timeout/LOGICAL_INTERVAL. LOGICAL_INTERVAL may be configured
   * via a system property with a default of 2. At least 1 interval is needed.
   */
  public static final int LOGICAL_INTERVAL =
      Integer.getInteger("geode.logical-message-received-interval", 2);

  /**
   * stall time to wait for members leaving concurrently
   */
  public static final long MEMBER_SUSPECT_COLLECTION_INTERVAL =
      Long.getLong("geode.suspect-member-collection-interval", 200);

  /**
   * A millisecond clock reading used to mark the last time a peer made contact.
   */
  @VisibleForTesting
  volatile long currentTimeStamp;

  /**
   * this member's ID
   */
  private ID localAddress;

  /**
   * Timestamp at which we last had contact from a member
   */
  final ConcurrentMap<ID, TimeStamp> memberTimeStamps =
      new ConcurrentHashMap<>();

  /**
   * Members currently being suspected and the view they were suspected in
   */
  private final ConcurrentHashMap<ID, GMSMembershipView<ID>> suspectedMemberIds =
      new ConcurrentHashMap<>();

  /**
   * Members undergoing final checks
   */
  @VisibleForTesting
  final List<ID> membersInFinalCheck =
      Collections.synchronizedList(new ArrayList<>(30));

  /**
   * Replies to messages
   */
  private final Map<Integer, Response> requestIdVsResponse = new ConcurrentHashMap<>();

  /**
   * Members suspected in a particular view
   */
  private final Map<GMSMembershipView<ID>, Set<SuspectRequest<ID>>> suspectRequestsInView =
      new HashMap<>();

  private ScheduledExecutorService scheduler;

  private ExecutorService checkExecutor;

  /**
   * to stop check scheduler
   */
  private ScheduledFuture<?> monitorFuture;

  /**
   * test hook
   */
  private volatile boolean playingDead = false;

  /**
   * test hook
   */
  private volatile boolean beingSick = false;

  // For TCP check
  private ExecutorService serverSocketExecutor;
  static final int OK = 0x7B;
  static final int ERROR = 0x00;
  private volatile int socketPort;
  private volatile ServerSocket serverSocket;

  /**
   * Statistics about health monitor
   */
  private MembershipStatistics stats;

  /**
   * Interval to run the Monitor task
   */
  private long monitorInterval;

  /**
   * /**
   * this class is to avoid garbage
   */
  static class TimeStamp {

    private volatile long timeStamp;

    TimeStamp(long timeStamp) {
      this.timeStamp = timeStamp;
    }

    public long getTime() {
      return timeStamp;
    }

    public void setTime(long timeStamp) {
      this.timeStamp = timeStamp;
    }
  }

  /***
   * This class sets start interval timestamp to record the activity of all members. That is used by
   * {@link GMSHealthMonitor#contactedBy(MemberIdentifier)} to record the activity of
   * member.
   *
   * It initiates the suspect processing for next neighbour if it doesn't see any activity from that
   * member in last interval(member-timeout)
   */
  private class Monitor implements Runnable {
    /**
     * Here we use the same threshold for detecting JVM pauses as the StatSampler
     */
    private final long MONITOR_DELAY_THRESHOLD =
        Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "statSamplerDelayThreshold", 3000);


    final long memberTimeoutInMillis;

    public Monitor(long memberTimeout) {
      memberTimeoutInMillis = memberTimeout;
    }

    @Override
    public void run() {

      ID neighbor = nextNeighbor;
      if (logger.isDebugEnabled()) {
        logger.debug("cluster health monitor invoked with {}", neighbor);
      }
      try {
        if (GMSHealthMonitor.this.isStopping) {
          return;
        }

        // TODO - why are we taking two clock readings and setting currentTimeStamp twice?
        long currentTime = System.currentTimeMillis();
        // this is the start of interval to record member activity
        GMSHealthMonitor.this.currentTimeStamp = currentTime;

        long oldTimeStamp = currentTimeStamp;
        currentTimeStamp = System.currentTimeMillis();

        GMSMembershipView<ID> myView = GMSHealthMonitor.this.currentView;
        if (myView == null) {
          return;
        }

        if (currentTimeStamp - oldTimeStamp > monitorInterval + MONITOR_DELAY_THRESHOLD) {
          // delay in running this task - don't suspect anyone for a while
          logger.info(
              "Failure detector has noticed a JVM pause and is giving all members a heartbeat in view {}",
              currentView);
          for (ID member : myView.getMembers()) {
            contactedBy(member);
          }
          return;
        }

        if (neighbor != null) {
          TimeStamp nextNeighborTS;
          synchronized (GMSHealthMonitor.this) {
            nextNeighborTS = GMSHealthMonitor.this.memberTimeStamps.get(neighbor);
          }

          if (nextNeighborTS == null) {
            logger.debug("timestamp for {} was found null - setting current time as timestamp",
                neighbor);
            TimeStamp customTS = new TimeStamp(currentTime);
            memberTimeStamps.put(neighbor, customTS);
            return;
          }

          long interval = memberTimeoutInMillis / GMSHealthMonitor.LOGICAL_INTERVAL;
          long lastTS = currentTime - nextNeighborTS.getTime();
          if (lastTS + interval >= memberTimeoutInMillis) {
            logger.debug("Checking member {} ", neighbor);
            // now do check request for this member;
            checkMember(neighbor);
          }
        }
      } finally {
        if (logger.isDebugEnabled()) {
          logger.debug("cluster health monitor pausing");
        }
      }
    }
  }

  /***
   * Check thread waits on this object for response. It puts requestId in requestIdVsResponse map.
   * Response will have requestId, which is used to get ResponseObject. Then it is used to notify
   * waiting thread.
   */
  private class Response {

    private AbstractGMSMessage<ID> responseMsg;

    public AbstractGMSMessage<ID> getResponseMsg() {
      return responseMsg;
    }

    public void setResponseMsg(AbstractGMSMessage<ID> responseMsg) {
      this.responseMsg = responseMsg;
    }

  }

  class ClientSocketHandler implements Runnable {

    private final Socket socket;

    public ClientSocketHandler(Socket socket) {
      this.socket = socket;
    }

    @Override
    public void run() {
      try {
        socket.setTcpNoDelay(true);
        DataInputStream in = new DataInputStream(socket.getInputStream());
        OutputStream out = socket.getOutputStream();
        @SuppressWarnings("UnusedAssignment")
        short version = in.readShort();
        int vmViewId = in.readInt();
        long uuidLSBs = in.readLong();
        long uuidMSBs = in.readLong();
        GMSHealthMonitor.this.stats.incFinalCheckRequestsReceived();
        GMSHealthMonitor.this.stats.incTcpFinalCheckRequestsReceived();
        ID gmbr = localAddress;
        UUID myUUID = gmbr.getUUID();
        // during reconnect or rapid restart we will have a zero viewId but there may still
        // be an old ID in the membership view that we do not want to respond to
        int myVmViewId = gmbr.getVmViewId();
        if (playingDead) {
          logger.debug("HealthMonitor: simulating sick member in health check");
        } else if (uuidLSBs == myUUID.getLeastSignificantBits()
            && uuidMSBs == myUUID.getMostSignificantBits()
            && (vmViewId == myVmViewId || myVmViewId < 0)) {
          logger.debug("HealthMonitor: sending OK reply");
          out.write(OK);
          out.flush();
          socket.shutdownOutput();
          GMSHealthMonitor.this.stats.incFinalCheckResponsesSent();
          GMSHealthMonitor.this.stats.incTcpFinalCheckResponsesSent();
          logger.debug("HealthMonitor: server replied OK.");
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "HealthMonitor: sending ERROR reply - my UUID is {},{} received is {},{}.  My viewID is {} received is {}",
                Long.toHexString(myUUID.getMostSignificantBits()),
                Long.toHexString(myUUID.getLeastSignificantBits()), Long.toHexString(uuidMSBs),
                Long.toHexString(uuidLSBs), myVmViewId, vmViewId);
          }
          out.write(ERROR);
          out.flush();
          socket.shutdownOutput();
          GMSHealthMonitor.this.stats.incFinalCheckResponsesSent();
          GMSHealthMonitor.this.stats.incTcpFinalCheckResponsesSent();
          logger.debug("HealthMonitor: server replied ERROR.");
        }
      } catch (IOException e) {
        // this is expected if it is a connection-timeout or other failure
        // to connect
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
            // expected if the socket is already closed
          }
        }
      }
    }
  }

  public GMSHealthMonitor(final TcpSocketCreator socketCreator) {
    Objects.requireNonNull(socketCreator);
    this.socketCreator = socketCreator;
  }

  /*
   * Record the member activity for current time interval.
   */
  @Override
  public void contactedBy(ID sender) {
    contactedBy(sender, currentTimeStamp);
  }


  /**
   * Record member activity at a specified time
   */
  private void contactedBy(ID sender, long timeStamp) {
    final TimeStamp cTS = JavaWorkarounds.computeIfAbsent(memberTimeStamps, sender, (s) -> {
      return new TimeStamp(timeStamp);
    });
    if (cTS != null && cTS.getTime() < timeStamp) {
      cTS.setTime(timeStamp);
    }
    if (suspectedMemberIds.containsKey(sender)) {
      memberUnsuspected(sender);
      setNextNeighbor(currentView, null);
    }
  }


  private HeartbeatRequestMessage<ID> constructHeartbeatRequestMessage(
      final ID mbr) {
    final int reqId = requestId.getAndIncrement();
    final HeartbeatRequestMessage<ID> hrm = new HeartbeatRequestMessage<ID>(mbr, reqId);
    hrm.setRecipient(mbr);

    return hrm;
  }

  private void checkMember(final ID mbr) {
    final GMSMembershipView<ID> cv = GMSHealthMonitor.this.currentView;

    // as check may take time
    setNextNeighbor(cv, mbr);

    // we need to check this member
    checkExecutor.execute(() -> {
      boolean pinged;
      try {
        pinged = GMSHealthMonitor.this.doCheckMember(mbr, true);
      } catch (MembershipClosedException e) {
        return;
      }

      if (!pinged) {
        String reason = "Member isn't responding to heartbeat requests";
        memberSuspected(localAddress, mbr, reason);
        initiateSuspicion(mbr, reason);
        setNextNeighbor(currentView, null);
      } else {
        logger.trace("Setting next neighbor as member {} has responded.", mbr);
        memberUnsuspected(mbr);
        // back to previous one
        setNextNeighbor(currentView, null);
      }
    });

  }

  private void initiateSuspicion(ID mbr, String reason) {
    if (services.getJoinLeave().isMemberLeaving(mbr)) {
      return;
    }
    sendSuspectRequest(Collections.singletonList(new SuspectRequest<ID>(mbr, reason)));
  }

  /**
   * This method sends heartbeat request to other member and waits for member-timeout time for
   * response. If it doesn't see response then it returns false.
   */
  private boolean doCheckMember(ID member, boolean waitForResponse) {
    if (playingDead || beingSick) {
      // a member playingDead should not be sending messages to other
      // members, so we avoid sending heartbeat requests or suspect
      // messages by returning true.
      return true;
    }
    long startTime = System.currentTimeMillis();
    logger.debug("Requesting heartbeat from {}", member);
    final HeartbeatRequestMessage<ID> hrm = constructHeartbeatRequestMessage(member);
    Response pingResp = null;
    if (waitForResponse) {
      pingResp = new Response();
      requestIdVsResponse.put(hrm.getRequestId(), pingResp);
    } else {
      hrm.clearRequestId();
    }
    try {
      Set<ID> membersNotReceivedMsg = this.services.getMessenger().send(hrm);
      this.stats.incHeartbeatRequestsSent();
      if (membersNotReceivedMsg != null && membersNotReceivedMsg.contains(member)) {
        // member is not part of current view.
        logger.trace("Member {} is not part of current view.", member);
      } else if (waitForResponse) {
        synchronized (pingResp) {
          if (pingResp.getResponseMsg() == null) {
            pingResp.wait(memberTimeout);
          }
          TimeStamp ts = memberTimeStamps.get(member);
          if (ts != null && ts.getTime() > startTime) {
            return true;
          }
          if (pingResp.getResponseMsg() == null) {
            if (isStopping) {
              return true;
            }
            logger.debug("no heartbeat response received from {} and no recent activity", member);
            return false;
          } else {
            logger.trace("received heartbeat from {}", member);
            this.stats.incHeartbeatsReceived();
            if (ts != null) {
              ts.setTime(System.currentTimeMillis());
            }
            return true;
          }
        }
      }
    } catch (InterruptedException e) {
      logger.debug(
          "GMSHealthMonitor checking thread interrupted, while waiting for response from member: {} .",
          member);
    } finally {
      if (waitForResponse) {
        requestIdVsResponse.remove(hrm.getRequestId());
      }
    }
    return false;
  }

  /**
   * During final check, establish TCP connection between current member and suspect member. And
   * exchange PING/PONG message to see if the suspect member is still alive.
   *
   * @param suspectMember member that does not respond to HeartbeatRequestMessage
   * @return true if successfully exchanged PING/PONG with TCP connection, otherwise false.
   */
  boolean doTCPCheckMember(ID suspectMember, int port,
      boolean retryIfConnectFails) {
    Socket clientSocket = null;
    // make sure we try to check on the member for the contracted memberTimeout period
    // in case a timed socket.connect() returns immediately. Use milliseconds to be in
    // sync with the socket timeout parameter unit of measure
    long giveupTime = System.currentTimeMillis() + services.getConfig().getMemberTimeout();
    boolean passed = false;
    int iteration = 0;
    do {
      iteration++;
      if (iteration > 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      try {
        logger.debug("Checking member {} with TCP socket connection {}:{}.", suspectMember,
            suspectMember.getInetAddress(), port);
        clientSocket =
            socketCreator
                .connect(suspectMember.getInetAddress(), port, (int) memberTimeout,
                    new ConnectTimeoutTask(services.getTimer(), memberTimeout), false, -1, false);
        clientSocket.setTcpNoDelay(true);
        passed = doTCPCheckMember(suspectMember, clientSocket);
      } catch (IOException e) {
        // this is expected if it is a connection-timeout or other failure
        // to connect
      } catch (IllegalStateException e) {
        if (!isStopping) {
          logger.trace("Unexpected exception", e);
        }
      } finally {
        try {
          if (clientSocket != null) {
            clientSocket.setSoLinger(true, 0); // abort the connection
            clientSocket.close();
          }
        } catch (IOException e) {
          // expected
        }
      }
    } while (retryIfConnectFails && !passed && !this.isShutdown()
        && System.currentTimeMillis() < giveupTime);
    return passed;
  }

  // Package protected for testing purposes
  boolean doTCPCheckMember(ID suspectMember, Socket clientSocket) {
    try {
      if (clientSocket.isConnected()) {
        clientSocket.setSoTimeout((int) services.getConfig().getMemberTimeout());
        InputStream in = clientSocket.getInputStream();
        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
        ID gmbr = suspectMember;
        writeMemberToStream(gmbr, out);
        this.stats.incFinalCheckRequestsSent();
        this.stats.incTcpFinalCheckRequestsSent();
        logger.debug("Connected to suspect member - reading response");
        int b = in.read();
        if (logger.isDebugEnabled()) {
          logger.debug("Received {}",
              (b == OK ? "OK" : (b == ERROR ? "ERROR" : "unknown response: " + b)));
        }
        if (b >= 0) {
          this.stats.incFinalCheckResponsesReceived();
          this.stats.incTcpFinalCheckResponsesReceived();
        }
        if (b == OK) {
          TimeStamp ts = memberTimeStamps.get(suspectMember);
          if (ts != null) {
            ts.setTime(System.currentTimeMillis());
          }
          return true;
        } else {
          // received ERROR
          return false;
        }
      } else {// cannot establish TCP connection with suspect member
        return false;
      }
    } catch (SocketTimeoutException e) {
      logger.debug("Availability check TCP/IP connection timed out for suspect member {}",
          suspectMember);
      return false;
    } catch (IOException e) {
      logger.trace("Unexpected exception", e);
    }
    return false;
  }

  void writeMemberToStream(ID gmbr, DataOutputStream out) throws IOException {
    out.writeShort(Version.getCurrentVersion().ordinal());
    out.writeInt(gmbr.getVmViewId());
    out.writeLong(gmbr.getUuidLeastSignificantBits());
    out.writeLong(gmbr.getUuidMostSignificantBits());
    out.flush();
  }

  @Override
  public void suspect(ID mbr, String reason) {
    initiateSuspicion(mbr, reason);
  }

  @Override
  public boolean checkIfAvailable(ID mbr, String reason,
      boolean initiateRemoval) {
    if (membersInFinalCheck.contains(mbr)) {
      return true; // status unknown for now but someone is checking
    }
    return inlineCheckIfAvailable(localAddress, currentView, initiateRemoval,
        mbr, reason);
  }

  @Override
  public void start() throws MemberStartupException {
    scheduler = LoggingExecutors.newScheduledThreadPool("Geode Failure Detection Scheduler", 1);
    checkExecutor = LoggingExecutors.newCachedThreadPool("Geode Failure Detection thread ", true);
    Monitor m = this.new Monitor(memberTimeout);
    monitorInterval = memberTimeout / LOGICAL_INTERVAL;
    monitorFuture =
        scheduler.scheduleAtFixedRate(m, monitorInterval, monitorInterval, TimeUnit.MILLISECONDS);
    serverSocketExecutor =
        LoggingExecutors.newCachedThreadPool("Geode Failure Detection Server thread ", true);
  }

  ServerSocket createServerSocket(InetAddress socketAddress, int[] portRange) throws IOException {
    ServerSocket newSocket = socketCreator
        .createServerSocketUsingPortRange(socketAddress, 50/* backlog */, true/* isBindAddress */,
            false/* useNIO */, 65536/* tcpBufferSize */, portRange, false);
    socketPort = newSocket.getLocalPort();
    return newSocket;
  }

  /**
   * start the thread that listens for tcp/ip connections and responds to connection attempts
   */
  private void startTcpServer(ServerSocket ssocket) {
    // allocate a socket here so there are no race conditions between knowing the FD
    // socket port and joining the system

    serverSocketExecutor.execute(() -> {
      logger.info("Started failure detection server thread on {}:{}.", ssocket.getInetAddress(),
          socketPort);
      Socket socket = null;
      try {
        while (!services.getCancelCriterion().isCancelInProgress()
            && !GMSHealthMonitor.this.isStopping) {
          try {
            socket = ssocket.accept();
            if (GMSHealthMonitor.this.playingDead) {
              continue;
            }
            serverSocketExecutor.execute(new ClientSocketHandler(socket));
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
        if (!ssocket.isClosed()) {
          try {
            ssocket.close();
          } catch (IOException e) {
            logger.debug("Unexpected exception", e);
          }
        }
      }
    });
  }

  /**
   * start the thread that periodically sends a message to processes that might be watching this
   * process
   */
  private void startHeartbeatThread() {
    checkExecutor.execute(new Runnable() {
      @Override
      public void run() {
        Thread.currentThread().setName("Geode Heartbeat Sender");
        sendPeriodicHeartbeats();
      }

      private void sendPeriodicHeartbeats() {
        while (!isStopping && !services.getCancelCriterion().isCancelInProgress()) {
          try {
            Thread.sleep(memberTimeout / LOGICAL_INTERVAL);
          } catch (InterruptedException e) {
            return;
          }
          GMSMembershipView<ID> v = currentView;
          if (v != null) {
            List<ID> mbrs = v.getMembers();
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

      private void sendHeartbeats(List<ID> mbrs, int startIndex) {
        ID coordinator = currentView.getCoordinator();
        if (coordinator != null && !coordinator.equals(localAddress)) {
          HeartbeatMessage<ID> message = new HeartbeatMessage<>(-1);
          message.setRecipient(coordinator);
          try {
            if (isStopping) {
              return;
            }
            services.getMessenger().sendUnreliably(message);
            GMSHealthMonitor.this.stats.incHeartbeatsSent();
          } catch (MembershipClosedException e) {
            return;
          }
        }

        int index = startIndex;
        int numSent = 0;
        for (;;) {
          index--;
          if (index < 0) {
            index = mbrs.size() - 1;
          }
          ID mbr = mbrs.get(index);
          if (mbr.equals(localAddress)) {
            break;
          }
          if (mbr.equals(coordinator)) {
            continue;
          }
          if (isStopping) {
            return;
          }
          HeartbeatMessage<ID> message = new HeartbeatMessage<>(-1);
          message.setRecipient(mbr);
          try {
            services.getMessenger().sendUnreliably(message);
            GMSHealthMonitor.this.stats.incHeartbeatsSent();
            numSent++;
            if (numSent >= NUM_HEARTBEATS) {
              break;
            }
          } catch (MembershipClosedException e) {
            return;
          }
        }
      } // for (;;)
    });
  }

  @Override
  public synchronized void installView(GMSMembershipView<ID> newView) {
    synchronized (suspectRequestsInView) {
      suspectRequestsInView.clear();
    }
    for (Iterator<ID> it = memberTimeStamps.keySet().iterator(); it
        .hasNext();) {
      if (!newView.contains(it.next())) {
        it.remove();
      }
    }
    for (Iterator<ID> it = suspectedMemberIds.keySet().iterator(); it
        .hasNext();) {
      if (!newView.contains(it.next())) {
        it.remove();
      }
    }
    currentView = newView;
    setNextNeighbor(newView, null);
  }

  /**
   * this method is primarily for tests. The current view should be pulled from JoinLeave or the
   * Membership (which includes surprise members)
   */
  public synchronized GMSMembershipView<ID> getView() {
    return currentView;
  }

  /***
   * This method sets next neighbour which it needs to watch in current view.
   *
   * if nextTo == null then it watches member next to it.
   *
   * It becomes null when we suspect current neighbour, during that time it watches member next to
   * suspect member.
   */
  protected synchronized void setNextNeighbor(GMSMembershipView<ID> newView,
      ID nextTo) {
    if (newView == null) {
      return;
    }
    if (nextTo == null) {
      nextTo = localAddress;
    }

    List<ID> allMembers = newView.getMembers();

    if (allMembers.size() > 1 && suspectedMemberIds.size() >= allMembers.size() - 1) {
      boolean nonSuspectFound = false;
      for (ID member : allMembers) {
        if (member.equals(localAddress)) {
          continue;
        }
        if (!suspectedMemberIds.containsKey(member)) {
          nonSuspectFound = true;
          break;
        }
      }
      if (!nonSuspectFound) {
        logger.info("All other members are suspect at this point");
        nextNeighbor = null;
        return;
      }
    }

    int index = allMembers.indexOf(nextTo);
    if (index != -1) {
      int nextNeighborIndex = (index + 1) % allMembers.size();
      ID newNeighbor = allMembers.get(nextNeighborIndex);
      if (suspectedMemberIds.containsKey(newNeighbor)) {
        setNextNeighbor(newView, newNeighbor);
        return;
      }
      ID oldNeighbor = nextNeighbor;
      if (oldNeighbor != newNeighbor) {
        logger.debug("Failure detection is now watching " + newNeighbor);
        nextNeighbor = newNeighbor;
      }
    }

    if (nextNeighbor != null && nextNeighbor.equals(localAddress)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Health monitor is unable to find a neighbor to watch.  "
            + "Current suspects are {}", suspectedMemberIds.keySet());
      }
      nextNeighbor = null;
    }

  }

  /** test method */
  public ID getNextNeighbor() {
    return nextNeighbor;
  }

  @Override
  public void init(Services<ID> s) throws MembershipConfigurationException {
    isStopping = false;
    services = s;
    memberTimeout = s.getConfig().getMemberTimeout();
    this.stats = services.getStatistics();

    services.getMessenger().addHandler(HeartbeatRequestMessage.class,
        this::processMessage);
    services.getMessenger().addHandler(HeartbeatMessage.class,
        this::processMessage);
    services.getMessenger().addHandler(SuspectMembersMessage.class,
        this::processMessage);
    services.getMessenger().addHandler(FinalCheckPassedMessage.class,
        this::processMessage);
  }

  @Override
  public void started() throws MemberStartupException {
    setLocalAddress(services.getMessenger().getMemberID());
    try {
      serverSocket = createServerSocket(localAddress.getInetAddress(),
          services.getConfig().getMembershipPortRange());
    } catch (IOException e) {
      throw new MemberStartupException("Problem creating HealthMonitor socket", e);
    }
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
    for (Response r : val) {
      synchronized (r) {
        r.notify();
      }
    }

    if (checkExecutor != null) {
      checkExecutor.shutdown();
    }

    stopServer();
  }

  void stopServer() {
    if (serverSocketExecutor != null) {
      if (serverSocket != null && !serverSocket.isClosed()) {
        try {
          serverSocket.close();
        } catch (IOException e) {
          logger.trace("Unexpected exception", e);
        }
      }
      serverSocketExecutor.shutdownNow();
      try {
        serverSocketExecutor.awaitTermination(2000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /***
   * test method
   */
  public boolean isShutdown() {
    return scheduler.isShutdown() && checkExecutor.isShutdown()
        && serverSocketExecutor.isShutdown();
  }

  /**
   * Test method - check to see if a member is under suspicion
   */
  public boolean isSuspectMember(ID m) {
    return this.suspectedMemberIds.containsKey(m);
  }

  @Override
  public void stopped() {

  }

  @Override
  public void memberSuspected(ID initiator,
      ID suspect, String reason) {
    synchronized (suspectRequestsInView) {
      suspectedMemberIds.put(suspect, currentView);
      Collection<SuspectRequest<ID>> requests = suspectRequestsInView.get(currentView);
      boolean found = false;
      if (requests == null) {
        requests = new HashSet<>();
        requests.add(new SuspectRequest<>(suspect, reason));
      }
      for (SuspectRequest<ID> request : requests) {
        if (suspect.equals(request.getSuspectMember())) {
          found = true;
          break;
        }
      }
      if (!found) {
        requests.add(new SuspectRequest<>(suspect, reason));
      }
    }
  }

  private void memberUnsuspected(ID mbr) {
    synchronized (suspectRequestsInView) {
      if (suspectedMemberIds.remove(mbr) != null) {
        logger.info("No longer suspecting {}", mbr);
      }
      Collection<SuspectRequest<ID>> suspectRequests = suspectRequestsInView.get(currentView);
      if (suspectRequests != null) {
        Collection<SuspectRequest<ID>> removals = new ArrayList<>(suspectRequests.size());
        for (SuspectRequest<ID> suspectRequest : suspectRequests) {
          if (mbr.equals(suspectRequest.getSuspectMember())) {
            removals.add(suspectRequest);
          }
        }
        suspectRequests.removeAll(removals);
      }
    }
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

  @Override
  public void setLocalAddress(ID idm) {
    this.localAddress = idm;
  }

  void processMessage(HeartbeatRequestMessage<ID> m) {
    if (isStopping) {
      return;
    }
    if (beingSick || playingDead) {
      logger.debug("sick member is ignoring check request");
      return;
    }

    this.stats.incHeartbeatRequestsReceived();

    if (this.isStopping) {
      return;
    }

    // only respond if the intended recipient is this member
    ID me = localAddress;

    if (me == null || me.getVmViewId() >= 0 && m.getTarget().equals(me)) {
      HeartbeatMessage<ID> hm = new HeartbeatMessage<>(m.getRequestId());
      hm.setRecipient(m.getSender());
      Set<ID> membersNotReceivedMsg = services.getMessenger().send(hm);
      this.stats.incHeartbeatsSent();
      if (membersNotReceivedMsg != null && membersNotReceivedMsg.contains(m.getSender())) {
        logger.debug("Unable to send heartbeat to member: {}", m.getSender());
      }
    } else {
      logger.debug("Ignoring heartbeat request intended for {}.  My ID is {}", m.getTarget(), me);
    }
  }



  void processMessage(HeartbeatMessage<ID> m) {
    if (isStopping) {
      return;
    }
    if (beingSick || playingDead) {
      logger.debug("sick member is ignoring check response");
      return;
    }

    this.stats.incHeartbeatsReceived();
    if (m.getRequestId() >= 0) {
      Response resp = requestIdVsResponse.get(m.getRequestId());
      logger.trace("Got heartbeat from member {}. {}", m.getSender(),
          (resp != null ? "Check thread still waiting" : "Check thread is not waiting"));
      if (resp != null) {
        synchronized (resp) {
          resp.setResponseMsg(m);
          resp.notify();
        }
      }

    }
    // we got heartbeat lets update timestamp
    contactedBy(m.getSender(), System.currentTimeMillis());
  }

  /**
   * Process a Suspect request from another member. This may cause this member to become the new
   * membership coordinator. it will to final check on that member and then it will send remove
   * request for that member
   */
  void processMessage(SuspectMembersMessage<ID> incomingRequest) {
    if (isStopping) {
      return;
    }
    if (beingSick || playingDead) {
      logger.debug("sick member is ignoring suspect message");
      return;
    }

    logSuspectRequests(incomingRequest, incomingRequest.getSender());

    this.stats.incSuspectsReceived();

    GMSMembershipView<ID> cv = currentView;

    if (cv == null) {
      return;
    }

    List<SuspectRequest<ID>> suspectRequests = incomingRequest.getMembers();

    ID sender = incomingRequest.getSender();
    int viewId = sender.getVmViewId();
    if (cv.getViewId() >= viewId && !cv.contains(incomingRequest.getSender())) {
      logger.info("Membership ignoring suspect request for " + incomingRequest + " from non-member "
          + incomingRequest.getSender());
      services.getJoinLeave().remove(sender,
          "this process is initiating suspect processing but is no longer a member");
      return;
    }

    // take care of any suspicion of this member by sending a heartbeat back
    for (Iterator<SuspectRequest<ID>> it = incomingRequest.getMembers().iterator(); it.hasNext();) {
      SuspectRequest<ID> req = it.next();
      if (req.getSuspectMember().equals(localAddress)) {
        HeartbeatMessage<ID> message = new HeartbeatMessage<>(-1);
        message.setRecipient(sender);
        try {
          services.getMessenger().send(message);
          this.stats.incHeartbeatsSent();
          it.remove();
        } catch (MembershipClosedException e) {
          return;
        }
      }
    }

    logger.debug("Processing {}", incomingRequest);
    if (cv.getCoordinator().equals(localAddress)) {
      // This process is the membership coordinator and should perform a final check
      checkIfAvailable(sender, suspectRequests, cv);

    } else {
      // Another process has raised suspicion - check to see if
      // this process should become the membership coordinator if
      // all current suspects are gone
      GMSMembershipView<ID> check = new GMSMembershipView<>(cv, cv.getViewId() + 1);
      ArrayList<SuspectRequest<ID>> membersToCheck = new ArrayList<>();
      synchronized (suspectRequestsInView) {
        recordSuspectRequests(suspectRequests, cv);
        Set<SuspectRequest<ID>> suspectsInView = suspectRequestsInView.get(cv);
        logger.debug("Current suspects are {}", suspectsInView);
        for (final SuspectRequest<ID> sr : suspectsInView) {
          check.remove(sr.getSuspectMember());
          membersToCheck.add(sr);
        }
      }
      List<ID> membersLeaving = new ArrayList<>();
      for (ID member : cv.getMembers()) {
        if (services.getJoinLeave().isMemberLeaving(member)) {
          membersLeaving.add(member);
        }
      }
      if (!membersLeaving.isEmpty()) {
        logger.debug("Current leave requests are {}", membersLeaving);
        check.removeAll(membersLeaving);
      }
      logger.debug(
          "Proposed view with suspects & leaving members removed is {}\nwith coordinator {}\nmy address is {}",
          check,
          check.getCoordinator(), localAddress);

      ID coordinator = check.getCoordinator();
      if (coordinator != null && coordinator.equals(localAddress)) {
        // new coordinator
        checkIfAvailable(sender, membersToCheck, cv);
      }
    }

  }

  void processMessage(FinalCheckPassedMessage<ID> m) {
    if (isStopping) {
      return;
    }
    // if we're currently processing a final-check for this member don't artificially update the
    // timestamp of the member or the final-check will be invalid
    if (!membersInFinalCheck.contains(m.getSuspect())) {
      contactedBy(m.getSuspect());
    }
  }



  private void logSuspectRequests(SuspectMembersMessage<ID> incomingRequest,
      ID sender) {
    for (SuspectRequest<ID> req : incomingRequest.getMembers()) {
      String who = sender.equals(localAddress) ? "myself" : sender.toString();
      logger.info("received suspect message from {} for {}: {}", who, req.getSuspectMember(),
          req.getReason());
    }
  }

  /***
   * This method make sure that records suspectRequest. We need to make sure this on preferred
   * coordinators, as elder coordinator might be in suspected list next.
   */
  private void recordSuspectRequests(List<SuspectRequest<ID>> suspectRequests,
      GMSMembershipView<ID> cv) {
    // record suspect requests
    Set<SuspectRequest<ID>> suspectedMembers;
    synchronized (suspectRequestsInView) {
      suspectedMembers = suspectRequestsInView.get(cv);
      if (suspectedMembers == null) {
        suspectedMembers = new HashSet<>();
        suspectRequestsInView.put(cv, suspectedMembers);
      }
      suspectedMembers.addAll(suspectRequests);
    }
  }

  /**
   * performs a "final" health check on the member. If failure-detection socket information is
   * available for the member (in the view) then we attempt to connect to its socket and ask if it's
   * the expected member. Otherwise we send a heartbeat request and wait for a reply.
   */
  private void checkIfAvailable(final ID initiator,
      List<SuspectRequest<ID>> sMembers, final GMSMembershipView<ID> cv) {

    for (final SuspectRequest<ID> sr : sMembers) {
      final ID mbr = sr.getSuspectMember();

      if (!cv.contains(mbr) || membersInFinalCheck.contains(mbr)) {
        continue;
      }

      if (mbr.equals(localAddress)) {
        continue;// self
      }

      final String reason = sr.getReason();
      logger.debug("Scheduling availability check for member {}; reason={}", mbr, reason);
      // its a coordinator
      checkExecutor.execute(() -> {
        try {
          inlineCheckIfAvailable(initiator, cv, true, mbr, reason);
        } catch (MembershipClosedException e) {
          // shutting down
        } catch (Exception e) {
          logger.info("Unexpected exception while verifying member", e);
        }
      });
    }
  }

  /**
   * Check to see if a member is available
   *
   * @param initiator member who initiated this check
   * @param cv the view we're basing the check upon
   * @param isFinalCheck whether the member should be kicked out if it fails the check
   * @param mbr the member to check
   * @param reason why we're doing this check
   * @return true if the check passes
   */
  protected boolean inlineCheckIfAvailable(final ID initiator,
      final GMSMembershipView<ID> cv, boolean isFinalCheck, final ID mbr,
      final String reason) {

    if (services.getJoinLeave().isMemberLeaving(mbr)) {
      return false;
    }

    boolean failed = false;

    logger.info("Performing availability check for suspect member {} reason={}", mbr, reason);
    membersInFinalCheck.add(mbr);
    setNextNeighbor(currentView, mbr);

    try {
      services.memberSuspected(initiator, mbr, reason);
      long startTime = System.currentTimeMillis();
      // for some reason we used to update the timestamp for the member
      // with the startTime, but we don't want to do that because it looks
      // like a heartbeat has been received

      boolean pinged;
      int port = cv.getFailureDetectionPort(mbr);
      if (port <= 0) {
        logger.info("Unable to locate failure detection port - requesting a heartbeat");
        if (logger.isDebugEnabled()) {
          logger.debug("\ncurrent view: {}\nports: {}", cv,
              Arrays.toString(cv.getFailureDetectionPorts()));
        }
        pinged = GMSHealthMonitor.this.doCheckMember(mbr, true);
        GMSHealthMonitor.this.stats.incFinalCheckRequestsSent();
        GMSHealthMonitor.this.stats.incUdpFinalCheckRequestsSent();
        if (pinged) {
          GMSHealthMonitor.this.stats.incFinalCheckResponsesReceived();
          GMSHealthMonitor.this.stats.incUdpFinalCheckResponsesReceived();
        }
      } else {
        // this will just send heartbeat request, it will not wait for response
        // if we will get heartbeat then it will change the timestamp, which we are
        // checking below in case of tcp check failure..
        doCheckMember(mbr, false);
        // now, while waiting for a heartbeat, try connecting to the suspect's failure detection
        // port
        final boolean retryIfConnectFails = isFinalCheck;
        pinged = doTCPCheckMember(mbr, port, retryIfConnectFails);
      }

      if (!pinged && !isStopping) {
        failed = true;
        TimeStamp ts = memberTimeStamps.get(mbr);
        if (ts == null || ts.getTime() < startTime) {
          logger.info("Availability check failed for member {}", mbr);
          // if the final check fails & this VM is the coordinator we don't need to do another final
          // check
          if (isFinalCheck) {
            logger.info("Requesting removal of suspect member {}", mbr);
            services.getJoinLeave().remove(mbr, reason);
            // make sure it is still suspected
            memberSuspected(localAddress, mbr, reason);
          } else {
            // if this node can survive an availability check then initiate suspicion about
            // the node that failed the availability check
            if (doTCPCheckMember(localAddress, this.socketPort, false)) {
              membersInFinalCheck.remove(mbr);
              // tell peers about this member and then perform another availability check
              memberSuspected(localAddress, mbr, reason);
              initiateSuspicion(mbr, reason);
              SuspectMembersMessage<ID> suspectMembersMessage =
                  new SuspectMembersMessage<>(Collections.singletonList(localAddress),
                      Collections
                          .singletonList(new SuspectRequest<>(mbr, "failed availability check")));
              suspectMembersMessage.setSender(localAddress);
              logger.debug("Performing local processing on suspect request");
              processMessage(suspectMembersMessage);
            } else {
              logger.info(
                  "Self-check for availability failed - will not continue to suspect {} for now",
                  mbr);
              failed = false;
            }
          }
        } else {
          logger.info(
              "Availability check detected recent message traffic for suspect member "
                  + mbr + " at time " + new Date(ts.getTime()));
          failed = false;
        }
      }

      if (!failed) {
        if (!isStopping
            && initiator.getVersionOrdinal() >= Version.GEODE_1_4_0.ordinal()) {
          // let others know that this member is no longer suspect
          FinalCheckPassedMessage<ID> message = new FinalCheckPassedMessage<>(initiator, mbr);
          List<ID> members = cv.getMembers();
          List<ID> recipients = new ArrayList<>(members.size());
          for (ID member : members) {
            if (!isSuspectMember(member) && !membersInFinalCheck.contains(member) &&
                !member.equals(localAddress)) {
              recipients.add(member);
            }
          }
          if (recipients.size() > 0) {
            message.setRecipients(recipients);
            services.getMessenger().send(message);
          }
        }

        logger.info("Availability check passed for suspect member " + mbr);
      }
    } finally {
      if (!failed) {
        memberUnsuspected(mbr);
        setNextNeighbor(currentView, null);
      }
      membersInFinalCheck.remove(mbr);
    }
    return !failed;
  }

  @Override
  public void memberShutdown(ID mbr, String reason) {}

  @Override
  public int getFailureDetectionPort() {
    return this.socketPort;
  }

  private void sendSuspectRequest(final List<SuspectRequest<ID>> requests) {
    logger.debug("Sending suspect request for members {}", requests);
    List<ID> recipients;
    if (currentView.size() > MembershipConfig.SMALL_CLUSTER_SIZE) {
      HashSet<ID> filter = new HashSet<>();
      for (Enumeration<ID> e = suspectedMemberIds.keys(); e
          .hasMoreElements();) {
        filter.add(e.nextElement());
      }
      filter.addAll(
          requests.stream().map(SuspectRequest::getSuspectMember)
              .collect(Collectors.toList()));
      recipients =
          currentView.getPreferredCoordinators(filter, services.getJoinLeave().getMemberID(),
              MembershipConfig.SMALL_CLUSTER_SIZE + 1);
    } else {
      recipients = currentView.getMembers();
    }

    logger.trace("Sending suspect messages to {}", recipients);
    SuspectMembersMessage<ID> smm = new SuspectMembersMessage<>(recipients, requests);
    smm.setSender(localAddress);
    Set<ID> failedRecipients;
    try {
      failedRecipients = services.getMessenger().send(smm);
      this.stats.incSuspectsSent();
    } catch (MembershipClosedException e) {
      return;
    }

    if (failedRecipients != null && failedRecipients.size() > 0) {
      logger.trace("Unable to send suspect message to {}", failedRecipients);
    }
    logger.trace("Processing suspect message locally");
    processMessage(smm);
  }

  private static class ConnectTimeoutTask extends TimerTask implements ConnectionWatcher {

    final Timer scheduler;
    Socket socket;
    final long timeout;

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

  public MembershipStatistics getStats() {
    return this.stats;
  }
}
