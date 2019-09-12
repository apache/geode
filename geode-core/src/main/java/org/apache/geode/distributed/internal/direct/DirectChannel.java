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
package org.apache.geode.distributed.internal.direct;

import java.io.IOException;
import java.io.NotSerializableException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.gms.api.MessageListener;
import org.apache.geode.internal.alerting.AlertingAction;
import org.apache.geode.internal.cache.DirectReplyMessage;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.tcp.BaseMsgStreamer;
import org.apache.geode.internal.tcp.ConnectExceptions;
import org.apache.geode.internal.tcp.Connection;
import org.apache.geode.internal.tcp.ConnectionException;
import org.apache.geode.internal.tcp.MemberShunnedException;
import org.apache.geode.internal.tcp.MsgStreamer;
import org.apache.geode.internal.tcp.TCPConduit;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.internal.util.concurrent.ReentrantSemaphore;

/**
 * DirectChannel is used to interact directly with other Direct servers to distribute GemFire
 * messages to other nodes. It is held by a
 * org.apache.geode.internal.cache.distribution.DistributionChannel, which is used by the
 * DistributionManager to send and receive asynchronous messages.
 */
public class DirectChannel {

  private static final Logger logger = LogService.getLogger();

  /** this is the conduit used for communications */
  private final transient TCPConduit conduit;
  private final ClusterDistributionManager dm;

  private volatile boolean disconnected = true;

  /** This is set to true when completely disconnected (all connections are closed) */
  private volatile boolean disconnectCompleted = true;

  /** this is the DistributionManager, most of the time */
  private final MessageListener receiver;

  private final InetAddress address;

  InternalDistributedMember localAddr;

  /**
   * Callback to set the local address, must be done before this channel is used.
   *
   * @throws ConnectionException if the conduit has stopped
   */
  public void setLocalAddr(InternalDistributedMember localAddr) {
    this.localAddr = localAddr;
    conduit.setMemberId(localAddr);
    if (disconnected) {
      disconnected = false;
      disconnectCompleted = false;
      this.groupOrderedSenderSem = new ReentrantSemaphore(MAX_GROUP_SENDERS);
      this.groupUnorderedSenderSem = new ReentrantSemaphore(MAX_GROUP_SENDERS);
    }
  }

  /**
   * Returns the cancel criterion for the channel, which will note if the channel is abnormally
   * closing
   */
  public CancelCriterion getCancelCriterion() {
    return conduit.getCancelCriterion();
  }

  public DirectChannel(MembershipManager mgr, MessageListener listener,
      ClusterDistributionManager dm)
      throws ConnectionException {
    this.receiver = listener;
    this.dm = dm;

    DistributionConfig dc = dm.getConfig();
    this.address = initAddress(dc);
    boolean isBindAddress = dc.getBindAddress() != null;
    try {
      int port = Integer.getInteger("tcpServerPort", 0).intValue();
      if (port == 0) {
        port = dc.getTcpPort();
      }
      Properties props = System.getProperties();
      if (props.getProperty("p2p.shareSockets") == null) {
        props.setProperty("p2p.shareSockets", String.valueOf(dc.getConserveSockets()));
      }
      if (dc.getSocketBufferSize() != DistributionConfig.DEFAULT_SOCKET_BUFFER_SIZE) {
        // Note that the system property "p2p.tcpBufferSize" will be
        // overridden by the new "socket-buffer-size".
        props.setProperty("p2p.tcpBufferSize", String.valueOf(dc.getSocketBufferSize()));
      }
      if (props.getProperty("p2p.idleConnectionTimeout") == null) {
        props.setProperty("p2p.idleConnectionTimeout", String.valueOf(dc.getSocketLeaseTime()));
      }
      int[] range = dc.getMembershipPortRange();
      props.setProperty("membership_port_range_start", "" + range[0]);
      props.setProperty("membership_port_range_end", "" + range[1]);

      this.conduit = new TCPConduit(mgr, port, address, isBindAddress, this, props);
      disconnected = false;
      disconnectCompleted = false;
      this.groupOrderedSenderSem = new ReentrantSemaphore(MAX_GROUP_SENDERS);
      this.groupUnorderedSenderSem = new ReentrantSemaphore(MAX_GROUP_SENDERS);
      logger.info("GemFire P2P Listener started on {}",
          conduit.getSocketId());

    } catch (ConnectionException ce) {
      logger.fatal(String.format("Unable to initialize direct channel because: %s",
          new Object[] {ce.getMessage()}),
          ce);
      throw ce; // fix for bug 31973
    }
  }


  /**
   * Return how many concurrent operations should be allowed by default. since 6.6, this has been
   * raised to Integer.MAX value from the number of available processors. Setting this to a lower
   * value raises the possibility of a deadlock when serializing a message with PDX objects, because
   * the PDX serialization can trigger further distribution.
   */
  public static final int DEFAULT_CONCURRENCY_LEVEL =
      Integer.getInteger("p2p.defaultConcurrencyLevel", Integer.MAX_VALUE / 2).intValue();

  /**
   * The maximum number of concurrent senders sending a message to a group of recipients.
   */
  private static final int MAX_GROUP_SENDERS =
      Integer.getInteger("p2p.maxGroupSenders", DEFAULT_CONCURRENCY_LEVEL).intValue();
  private Semaphore groupUnorderedSenderSem;
  private Semaphore groupOrderedSenderSem;

  private Semaphore getGroupSem(boolean ordered) {
    if (ordered) {
      return this.groupOrderedSenderSem;
    } else {
      return this.groupUnorderedSenderSem;
    }
  }

  private void acquireGroupSendPermission(boolean ordered) {
    if (this.disconnected) {
      throw new org.apache.geode.distributed.DistributedSystemDisconnectedException(
          "Direct channel has been stopped");
    }
    // @todo darrel: add some stats
    final Semaphore s = getGroupSem(ordered);
    for (;;) {
      this.conduit.getCancelCriterion().checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        s.acquire();
        break;
      } catch (InterruptedException ex) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // for
    if (this.disconnected) {
      s.release();
      throw new DistributedSystemDisconnectedException(
          "communications disconnected");
    }
  }

  private void releaseGroupSendPermission(boolean ordered) {
    final Semaphore s = getGroupSem(ordered);
    s.release();
  }

  /**
   * Returns true if calling thread owns its own communication resources.
   */
  boolean threadOwnsResources() {
    if (dm != null) {
      return dm.getSystem().threadOwnsResources() && !AlertingAction.isThreadAlerting();
    }
    return false;

  }

  /**
   * This is basically just sendToMany, giving us a way to see on the stack whether we are sending
   * to a single member or multiple members, in which case the group-send lock will be held during
   * distribution.
   *
   * @param mgr - the membership manager
   * @param p_destinations - the list of addresses to send the message to.
   * @param msg - the message to send
   * @param ackSAThreshold the severe alert threshold
   * @return number of bytes sent
   * @throws ConnectExceptions if message could not be send to its <code>destination</code>
   * @throws NotSerializableException If the msg cannot be serialized
   */
  private int sendToOne(final MembershipManager mgr,
      InternalDistributedMember[] p_destinations,
      final DistributionMessage msg, long ackWaitThreshold, long ackSAThreshold)
      throws ConnectExceptions, NotSerializableException {
    return sendToMany(mgr, p_destinations, msg, ackWaitThreshold, ackSAThreshold);
  }


  /**
   * Sends a msg to a list of destinations. This code does some special optimizations to stream
   * large messages
   *
   * @param mgr - the membership manager
   * @param p_destinations - the list of addresses to send the message to.
   * @param msg - the message to send
   * @param ackSAThreshold the severe alert threshold
   * @return number of bytes sent
   * @throws ConnectExceptions if message could not be send to its <code>destination</code>
   * @throws NotSerializableException If the msg cannot be serialized
   */
  private int sendToMany(final MembershipManager mgr,
      InternalDistributedMember[] p_destinations,
      final DistributionMessage msg, long ackWaitThreshold, long ackSAThreshold)
      throws ConnectExceptions, NotSerializableException {
    InternalDistributedMember destinations[] = p_destinations;

    // Collects connect exceptions that happened during previous attempts to send.
    // These represent members we are not able to distribute to.
    ConnectExceptions failedCe = null;
    // Describes the destinations that we need to retry the send to.
    ConnectExceptions retryInfo = null;
    int bytesWritten = 0;
    boolean retry = false;
    final boolean orderedMsg = msg.orderedDelivery() || Connection.isDominoThread();
    // Connections we actually sent messages to.
    final List totalSentCons = new ArrayList(destinations.length);
    boolean interrupted = false;

    long ackTimeout = 0;
    long ackSDTimeout = 0;
    long startTime = 0;
    final DirectReplyMessage directMsg;
    if (msg instanceof DirectReplyMessage) {
      directMsg = (DirectReplyMessage) msg;
    } else {
      directMsg = null;
    }
    if (directMsg != null || msg.getProcessorId() > 0) {
      ackTimeout = (int) (ackWaitThreshold * 1000);
      if (msg.isSevereAlertCompatible() || ReplyProcessor21.isSevereAlertProcessingForced()) {
        ackSDTimeout = (int) (ackSAThreshold * 1000);
        if (ReplyProcessor21.getShortSevereAlertProcessing()) {
          ackSDTimeout = (int) (ReplyProcessor21.PR_SEVERE_ALERT_RATIO * ackSDTimeout);
        }
      }
    }

    boolean directReply = false;
    if (directMsg != null && directMsg.supportsDirectAck() && threadOwnsResources()) {
      directReply = true;
    }

    // If this is a direct reply message, but we are sending it
    // over the shared socket, tell the message it needs to
    // use a regular reply processor.
    if (!directReply && directMsg != null) {
      directMsg.registerProcessor();
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Sending ({}) to {} peers ({}) via tcp/ip",
          msg, p_destinations.length, Arrays.toString(p_destinations));
    }

    try {
      do {
        interrupted = Thread.interrupted() || interrupted;
        /**
         * Exceptions that happened during one attempt to send
         */
        if (retryInfo != null) {
          // need to retry to each of the members in the exception
          List retryMembers = retryInfo.getMembers();
          InternalDistributedMember[] retryDest =
              new InternalDistributedMember[retryMembers.size()];
          retryDest = (InternalDistributedMember[]) retryMembers.toArray(retryDest);
          destinations = retryDest;
          retryInfo = null;
          retry = true;
        }
        final List cons = new ArrayList(destinations.length);
        ConnectExceptions ce = getConnections(mgr, msg, destinations, orderedMsg, retry, ackTimeout,
            ackSDTimeout, cons);
        if (directReply && msg.getProcessorId() > 0) { // no longer a direct-reply message?
          directReply = false;
        }
        if (ce != null) {
          if (failedCe != null) {
            failedCe.getMembers().addAll(ce.getMembers());
            failedCe.getCauses().addAll(ce.getCauses());
          } else {
            failedCe = ce;
          }
          ce = null;
        }
        if (cons.isEmpty()) {
          if (failedCe != null) {
            throw failedCe;
          }
          return bytesWritten;
        }

        boolean sendingToGroup = cons.size() > 1;
        Connection permissionCon = null;
        if (sendingToGroup) {
          acquireGroupSendPermission(orderedMsg);
        } else {
          // sending over just one connection
          permissionCon = (Connection) cons.get(0);
          if (permissionCon != null) {
            try {
              permissionCon.acquireSendPermission();
            } catch (ConnectionException conEx) {
              // Set retryInfo and then retry.
              // We want to keep calling TCPConduit.getConnection until it doesn't
              // return a connection.
              retryInfo = new ConnectExceptions();
              retryInfo.addFailure(permissionCon.getRemoteAddress(), conEx);
              continue;
            }
          }
        }

        try {
          if (retry && logger.isDebugEnabled()) {
            logger.debug("Retrying send ({}{}) to {} peers ({}) via tcp/ip",
                msg, cons.size(), cons);
          }
          DMStats stats = getDMStats();
          List<?> sentCons; // used for cons we sent to this time

          final BaseMsgStreamer ms =
              MsgStreamer.create(cons, msg, directReply, stats, getConduit().getBufferPool());
          try {
            startTime = 0;
            if (ackTimeout > 0) {
              startTime = System.currentTimeMillis();
            }
            ms.reserveConnections(startTime, ackTimeout, ackSDTimeout);

            int result = ms.writeMessage();
            if (bytesWritten == 0) {
              // bytesWritten only needs to be set once.
              // if we have to do a retry we don't want to count
              // each one's bytes.
              bytesWritten = result;
            }
            ce = ms.getConnectExceptions();
            sentCons = ms.getSentConnections();

            totalSentCons.addAll(sentCons);
          } catch (NotSerializableException e) {
            throw e;
          } catch (IOException ex) {
            throw new InternalGemFireException(
                "Unknown error serializing message",
                ex);
          } finally {
            try {
              ms.close();
            } catch (IOException e) {
              throw new InternalGemFireException("Unknown error serializing message", e);
            }
          }

          if (ce != null) {
            retryInfo = ce;
            ce = null;
          }

          if (directReply && !sentCons.isEmpty()) {
            long readAckStart = 0;
            if (stats != null) {
              readAckStart = stats.startReplyWait();
            }
            try {
              ce = readAcks(sentCons, startTime, ackTimeout, ackSDTimeout, ce,
                  directMsg.getDirectReplyProcessor());
            } finally {
              if (stats != null) {
                stats.endReplyWait(readAckStart, startTime);
              }
            }
          }
        } finally {
          if (sendingToGroup) {
            releaseGroupSendPermission(orderedMsg);
          } else if (permissionCon != null) {
            permissionCon.releaseSendPermission();
          }
        }
        if (ce != null) {
          if (retryInfo != null) {
            retryInfo.getMembers().addAll(ce.getMembers());
            retryInfo.getCauses().addAll(ce.getCauses());
          } else {
            retryInfo = ce;
          }
          ce = null;
        }
        if (retryInfo != null) {
          this.conduit.getCancelCriterion().checkCancelInProgress(null);
        }
      } while (retryInfo != null);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      for (Iterator it = totalSentCons.iterator(); it.hasNext();) {
        Connection con = (Connection) it.next();
        con.setInUse(false, 0, 0, 0, null);
      }
    }
    if (failedCe != null) {
      throw failedCe;
    }
    return bytesWritten;
  }

  private ConnectExceptions readAcks(List sentCons, long startTime, long ackTimeout,
      long ackSDTimeout, ConnectExceptions cumulativeExceptions, DirectReplyProcessor processor) {

    ConnectExceptions ce = cumulativeExceptions;

    for (Iterator it = sentCons.iterator(); it.hasNext();) {
      Connection con = (Connection) it.next();
      // We don't expect replies on shared connections.
      if (con.isSharedResource()) {
        continue;
      }
      try {
        try {
          con.readAck(processor);
        } catch (SocketTimeoutException ex) {
          handleAckTimeout(ackTimeout, ackSDTimeout, con, processor);
        }
      } catch (ConnectionException conEx) {
        if (ce == null) {
          ce = new ConnectExceptions();
        }
        ce.addFailure(con.getRemoteAddress(), conEx);
      }
    }
    return ce;
  }

  /**
   * Obtain the connections needed to transmit a message. The connections are put into the cons
   * object (the last parameter)
   *
   * @param mgr the membership manager
   * @param msg the message to send
   * @param destinations who to send the message to
   * @param preserveOrder true if the msg should ordered
   * @param retry whether this is a retransmission
   * @param ackTimeout the ack warning timeout
   * @param ackSDTimeout the ack severe alert timeout
   * @param cons a list to hold the connections
   * @return null if everything went okay, or a ConnectExceptions object if some connections
   *         couldn't be obtained
   */
  private ConnectExceptions getConnections(MembershipManager mgr, DistributionMessage msg,
      InternalDistributedMember[] destinations, boolean preserveOrder, boolean retry,
      long ackTimeout, long ackSDTimeout, List cons) {
    ConnectExceptions ce = null;
    for (int i = 0; i < destinations.length; i++) {
      InternalDistributedMember destination = destinations[i];
      if (destination == null) {
        continue;
      }
      if (localAddr.equals(destination)) {
        // jgroups does not deliver messages to a sender, so we don't support
        // it here either.
        continue;
      }

      if (!mgr.memberExists(destination) || mgr.shutdownInProgress()
          || mgr.isShunned(destination)) {
        // This should only happen if the member is no longer in the view.
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "Not a member: {}", destination);
        }
        if (ce == null)
          ce = new ConnectExceptions();
        ce.addFailure(destination, new ShunnedMemberException(
            String.format("Member is being shunned: %s", destination)));
      } else {
        try {
          long startTime = 0;
          if (ackTimeout > 0) {
            startTime = System.currentTimeMillis();
          }
          Connection con = conduit.getConnection(destination, preserveOrder, retry, startTime,
              ackTimeout, ackSDTimeout);

          con.setInUse(true, startTime, 0, 0, null); // fix for bug#37657
          cons.add(con);
          if (con.isSharedResource() && msg instanceof DirectReplyMessage) {
            DirectReplyMessage directMessage = (DirectReplyMessage) msg;
            directMessage.registerProcessor();
          }
        } catch (IOException ex) {
          if (ce == null)
            ce = new ConnectExceptions();
          ce.addFailure(destination, ex);
        }
      }
    } // for
    return ce;
  }


  /**
   * Method send.
   *
   * @param mgr - the membership manager
   * @param destinations - the address(es) to send the message to.
   * @param msg - the message to send
   * @param ackSAThreshold severe alert threshold
   * @return number of bytes sent
   * @throws ConnectExceptions if message could not be send to one or more of the
   *         <code>destinations</code>
   * @throws NotSerializableException If the content cannot be serialized
   * @throws ConnectionException if the conduit has stopped
   */
  public int send(MembershipManager mgr, InternalDistributedMember[] destinations,
      DistributionMessage msg, long ackWaitThreshold, long ackSAThreshold)
      throws ConnectExceptions, NotSerializableException {

    if (disconnected) {
      if (logger.isDebugEnabled()) {
        logger.debug("Returning from DirectChannel send because channel is disconnected: {}", msg);
      }
      return 0;
    }
    if (destinations == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Returning from DirectChannel send because null set passed in: {}", msg);
      }
      return 0;
    }
    if (destinations.length == 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("Returning from DirectChannel send because empty destinations passed in {}",
            msg);
      }
      return 0;
    }

    msg.setSender(localAddr);
    if (destinations.length == 1) {
      return sendToOne(mgr, destinations, msg, ackWaitThreshold, ackSAThreshold);
    } else {
      return sendToMany(mgr, destinations, msg, ackWaitThreshold, ackSAThreshold);
    }
  }



  /**
   * Returns null if no stats available.
   */
  public DMStats getDMStats() {
    if (dm != null) {
      return dm.getStats(); // fix for bug#34004
    } else {
      return null;
    }
  }

  /**
   * Returns null if no config is available.
   *
   * @since GemFire 4.2.2
   */
  public DistributionConfig getDMConfig() {
    if (dm != null) {
      return dm.getConfig();
    } else {
      return null;
    }
  }

  /**
   * Returns null if no dm available.
   */
  public DistributionManager getDM() {
    return dm;
  }

  /**
   *
   * @param ackTimeout ack wait threshold
   * @param ackSATimeout severe alert threshold
   */
  private void handleAckTimeout(long ackTimeout, long ackSATimeout, Connection c,
      DirectReplyProcessor processor) throws ConnectionException {
    List activeMembers = dm.getDistributionManagerIds();

    // Increment the stat
    dm.getStats().incReplyTimeouts();

    // an alert that will show up in the console
    {
      String msg =
          "%s seconds have elapsed while waiting for reply from %s on %s whose current membership list is: [%s]";
      final Object[] msgArgs = new Object[] {Long.valueOf(ackTimeout / 1000), c.getRemoteAddress(),
          dm.getId(), activeMembers};
      logger.warn(String.format(msg, msgArgs));
      msgArgs[3] = "(omitted)";
      Breadcrumbs.setProblem(msg, msgArgs);

      if (ReplyProcessor21.THROW_EXCEPTION_ON_TIMEOUT) {
        // init the cause to be a TimeoutException so catchers can determine cause
        TimeoutException cause =
            new TimeoutException("Timed out waiting for ACKS.");
        throw new InternalGemFireException(String.format(msg, msgArgs), cause);
      }
    }

    if (activeMembers.contains(c.getRemoteAddress())) {
      // wait for ack-severe-alert-threshold period first, then wait forever
      if (ackSATimeout > 0) {
        try {
          c.readAck(processor);
          return;
        } catch (SocketTimeoutException e) {
          Object[] args = new Object[] {Long.valueOf((ackSATimeout + ackTimeout) / 1000),
              c.getRemoteAddress(), dm.getId(), activeMembers};
          logger.fatal(
              "{} seconds have elapsed while waiting for reply from {} on {} whose currentFull membership list is: [{}]",
              args);
        }
      }
      try {
        c.readAck(processor);
      } catch (SocketTimeoutException ex) {
        // this can never happen when called with timeout of 0
        logger.error(String.format("Unexpected timeout while waiting for ack from %s",
            c.getRemoteAddress()),
            ex);
      }
    } else {
      logger.warn("View no longer has {} as an active member, so we will no longer wait for it.",
          c.getRemoteAddress());
      processor.memberDeparted(getDM(), c.getRemoteAddress(), true);
    }
  }

  public void receive(DistributionMessage msg, int bytesRead) {
    if (disconnected) {
      return;
    }
    try {
      receiver.messageReceived(msg);
    } catch (MemberShunnedException e) {
      throw e;
    } catch (CancelException e) {
      // ignore
    } catch (Exception ex) {
      // Don't freak out if the DM is shutting down
      if (!conduit.getCancelCriterion().isCancelInProgress()) {
        logger.fatal("While pulling a message", ex);
      }
    }
  }

  /**
   * Ensure that the TCPConduit class gets loaded.
   *
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    TCPConduit.loadEmergencyClasses();
  }

  /**
   * Close the Conduit
   *
   * @see SystemFailure#emergencyClose()
   */
  public void emergencyClose() {
    this.conduit.emergencyClose();
  }

  /**
   * This closes down the Direct connection. Theoretically you can disconnect and, if you need to
   * use the channel again you can and it will automatically reconnect. Reconnection will cause a
   * new local address to be generated.
   */
  public synchronized void disconnect(Exception cause) {
    this.disconnected = true;
    this.disconnectCompleted = false;
    try {
      groupOrderedSenderSem.release();
    } catch (Error e) {
      // GEODE-1076 - already released
    }
    try {
      groupUnorderedSenderSem.release();
    } catch (Error e) {
      // GEODE-1076 - already released
    }
    this.conduit.stop(cause);
    this.disconnectCompleted = true;
  }

  public boolean isOpen() {
    return !disconnectCompleted;
  }

  /** returns the receiver to which this DirectChannel is delivering messages */
  protected MessageListener getReceiver() {
    return receiver;
  }

  /**
   * Returns the port on which this direct channel sends messages
   */
  public int getPort() {
    return this.conduit.getPort();
  }

  /**
   * Returns the conduit over which this channel sends messages
   *
   * @since GemFire 2.1
   */
  public TCPConduit getConduit() {
    return this.conduit;
  }

  private InetAddress initAddress(DistributionConfig dc) {

    String bindAddress = dc.getBindAddress();

    try {
      /*
       * note: had to change the following to make sure the prop wasn't empty in addition to not
       * null for admin.DistributedSystemFactory
       */
      if (bindAddress != null && bindAddress.length() > 0) {
        return InetAddress.getByName(bindAddress);

      } else {
        return SocketCreator.getLocalHost();
      }
    } catch (java.net.UnknownHostException unhe) {
      throw new RuntimeException(unhe);

    }
  }

  public void closeEndpoint(InternalDistributedMember member, String reason) {
    closeEndpoint(member, reason, true);
  }


  /**
   * Closes any connections used to communicate with the given jgroupsAddress.
   */
  public void closeEndpoint(InternalDistributedMember member, String reason,
      boolean notifyDisconnect) {
    TCPConduit tc = this.conduit;
    if (tc != null) {
      tc.removeEndpoint(member, reason, notifyDisconnect);
    }
  }

  /**
   * adds state for thread-owned serial connections to the given member to the parameter
   * <i>result</i>. This can be used to wait for the state to reach the given level in the member's
   * vm.
   *
   * @param member the member whose state is to be captured
   * @param result the map to add the state to
   * @since GemFire 5.1
   */
  public void getChannelStates(DistributedMember member, Map result) {
    TCPConduit tc = this.conduit;
    if (tc != null) {
      tc.getThreadOwnedOrderedConnectionState(member, result);
    }
  }

  /**
   * wait for the given connections to process the number of messages associated with the connection
   * in the given map
   */
  public void waitForChannelState(DistributedMember member, Map channelState)
      throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    TCPConduit tc = this.conduit;
    if (tc != null) {
      tc.waitForThreadOwnedOrderedConnectionState(member, channelState);
    }
  }

  /**
   * returns true if there are still receiver threads for the given member
   */
  public boolean hasReceiversFor(DistributedMember mbr) {
    return this.conduit.hasReceiversFor(mbr);
  }
}
