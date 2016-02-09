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

package com.gemstone.gemfire.distributed.internal.direct;

import java.io.IOException;
import java.io.NotSerializableException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.ToDataException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.DistributedMembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.i18n.StringId;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.DirectReplyMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.tcp.BaseMsgStreamer;
import com.gemstone.gemfire.internal.tcp.ConnectExceptions;
import com.gemstone.gemfire.internal.tcp.Connection;
import com.gemstone.gemfire.internal.tcp.ConnectionException;
import com.gemstone.gemfire.internal.tcp.MemberShunnedException;
import com.gemstone.gemfire.internal.tcp.MsgStreamer;
import com.gemstone.gemfire.internal.tcp.TCPConduit;
import com.gemstone.gemfire.internal.util.Breadcrumbs;
import com.gemstone.gemfire.internal.util.concurrent.ReentrantSemaphore;

/**
 * @author Bruce Schuchardt
 * @author Darrel Schneider
 * DirectChannel is used to interact directly with other Direct servers to
 * distribute GemFire messages to other nodes.  It is held by a
 * com.gemstone.gemfire.internal.cache.distribution.DistributionChannel,
 * which is used by the DistributionManager to send and receive asynchronous
 * messages.
 */
public class DirectChannel {
  
  private static final Logger logger = LogService.getLogger();

    /** this is the conduit used for communications */
    private final transient TCPConduit conduit;

    private volatile boolean disconnected = true;
    
    /** This is set to true when completely disconnected (all connections are closed) */
    private volatile boolean disconnectCompleted = true;

    /** this is the DistributionManager, most of the time */
    private final DirectChannelListener receiver;

    private final InetAddress address;
    
    InternalDistributedMember localAddr;

    /**
     * Callback to set the local address, must be done before this channel is used.
     * 
     * @param localAddr
     * @throws ConnectionException if the conduit has stopped
     */
    public void setLocalAddr(InternalDistributedMember localAddr) {
      this.localAddr = localAddr;
      conduit.setLocalAddr(localAddr);
      if (disconnected) {
        disconnected = false;
        disconnectCompleted = false;
        this.groupOrderedSenderSem = new ReentrantSemaphore(MAX_GROUP_SENDERS);
        this.groupUnorderedSenderSem = new ReentrantSemaphore(MAX_GROUP_SENDERS);
      }
    }
    
    /**
     * when the initial number of members is known, this method is invoked
     * to ensure that connections to those members can be established in a
     * reasonable amount of time.  See bug 39848 
     * @param numberOfMembers
     */
    public void setMembershipSize(int numberOfMembers) {
      conduit.setMaximumHandshakePoolSize(numberOfMembers);
    }
    
    /**
     * Returns the cancel criterion for the channel,
     * which will note if the channel is abnormally
     * closing
     */
    public CancelCriterion getCancelCriterion() {
      return conduit.getCancelCriterion();
    }

    public DirectChannel(MembershipManager mgr, DirectChannelListener listener,
        DistributionConfig dc) 
        throws ConnectionException {
      this.receiver = listener;

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
        props.setProperty("membership_port_range_start", ""+range[0]);
        props.setProperty("membership_port_range_end", ""+range[1]);

        this.conduit = new TCPConduit(mgr, port, address, isBindAddress, this, props);
        disconnected = false;
        disconnectCompleted = false;
        this.groupOrderedSenderSem = new ReentrantSemaphore(MAX_GROUP_SENDERS);
        this.groupUnorderedSenderSem = new ReentrantSemaphore(MAX_GROUP_SENDERS);
        logger.info(LocalizedMessage.create(
            LocalizedStrings.DirectChannel_GEMFIRE_P2P_LISTENER_STARTED_ON__0, conduit.getLocalAddr()));

      }
      catch (ConnectionException ce) {
        logger.fatal(LocalizedMessage.create(
            LocalizedStrings.DirectChannel_UNABLE_TO_INITIALIZE_DIRECT_CHANNEL_BECAUSE__0, new Object[]{ce.getMessage()}), ce);
        throw ce; // fix for bug 31973
      }
    }

 
  /**
   * Return how many concurrent operations should be allowed by default.
   * since 6.6, this has been raised to Integer.MAX value from the number
   * of available processors. Setting this to a lower value raises the possibility
   * of a deadlock when serializing a message with PDX objects, because the 
   * PDX serialization can trigger further distribution.
   */
  static public final int DEFAULT_CONCURRENCY_LEVEL = Integer.getInteger("p2p.defaultConcurrencyLevel", Integer.MAX_VALUE / 2).intValue();
  
  /**
   * The maximum number of concurrent senders sending a message to a group of recipients.
   */
  static private final int MAX_GROUP_SENDERS = Integer.getInteger("p2p.maxGroupSenders", DEFAULT_CONCURRENCY_LEVEL).intValue();
  private Semaphore groupUnorderedSenderSem; // TODO this should be final?
  private Semaphore groupOrderedSenderSem; // TODO this should be final?

//  /**
//   * cause of abnormal shutdown, if any
//   */
//  private volatile Exception shutdownCause;

  private Semaphore getGroupSem(boolean ordered) {
    if (ordered) {
      return this.groupOrderedSenderSem;
    } else {
      return this.groupUnorderedSenderSem;
    }
  }
  private void acquireGroupSendPermission(boolean ordered) {
    if (this.disconnected) {
      throw new com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException(LocalizedStrings.DirectChannel_DIRECT_CHANNEL_HAS_BEEN_STOPPED.toLocalizedString());
    }
    // @todo darrel: add some stats
    final Semaphore s = getGroupSem(ordered);
    for (;;) {
      this.conduit.getCancelCriterion().checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        s.acquire();
        break;
      } 
      catch (InterruptedException ex) {
        interrupted = true;
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // for
    if (this.disconnected) {
      s.release();
      throw new DistributedSystemDisconnectedException(LocalizedStrings.DirectChannel_COMMUNICATIONS_DISCONNECTED.toLocalizedString());
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
    DM d = getDM();
    if (d != null) {
      return d.getSystem().threadOwnsResources() && !AlertAppender.isThreadAlerting();
    }
    return false;
    
//    Boolean b = getThreadOwnsResourcesRegistration();
//    if (b == null) {
//      // thread does not have a preference so return default
//      return !this.owner.shareSockets;
//      return false;
//    } else {
//      return b.booleanValue();
//    }
  }

  /**
   * This is basically just sendToMany, giving us a way to see on the stack
   * whether we are sending to a single member or multiple members, in which
   * case the group-send lock will be held during distribution.
   * 
   * @param mgr - the membership manager
   * @param p_destinations - the list of addresses to send the message to.
   * @param msg - the message to send
   * @param ackWaitThreshold
   * @param ackSAThreshold the severe alert threshold
   * @return number of bytes sent
   * @throws ConnectExceptions if message could not be send to its
   *         <code>destination</code>
   * @throws NotSerializableException
   *         If the msg cannot be serialized
   */
  private final int sendToOne(final MembershipManager mgr, 
      InternalDistributedMember[] p_destinations,
      final DistributionMessage msg, long ackWaitThreshold, long ackSAThreshold)
      throws ConnectExceptions, NotSerializableException {
    return sendToMany(mgr, p_destinations, msg, ackWaitThreshold, ackSAThreshold);
  }
  
  
  /**
   * Sends a msg to a list of destinations. This code does some special optimizations
   * to stream large messages
   * @param mgr - the membership manager
   * @param p_destinations - the list of addresses to send the message to.
   * @param msg - the message to send
   * @param ackWaitThreshold
   * @param ackSAThreshold the severe alert threshold
   * @return number of bytes sent
   * @throws ConnectExceptions if message could not be send to its
   *         <code>destination</code>
   * @throws NotSerializableException
   *         If the msg cannot be serialized
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
    //Connections we actually sent messages to.
    final List totalSentCons = new ArrayList(destinations.length);
    boolean interrupted = false;

    long ackTimeout = 0;
    long ackSDTimeout = 0;
    long startTime = 0;
    final DirectReplyMessage directMsg;
    if (msg instanceof DirectReplyMessage) {
      directMsg = (DirectReplyMessage)msg;
    }
    else {
      directMsg = null;
    }
    if (directMsg != null || msg.getProcessorId() > 0) {
      ackTimeout = (int)(ackWaitThreshold * 1000);
      if (msg.isSevereAlertCompatible() || ReplyProcessor21.isSevereAlertProcessingForced()) {
        ackSDTimeout = (int)(ackSAThreshold * 1000);
        if (ReplyProcessor21.getShortSevereAlertProcessing()) {
          ackSDTimeout = (int)(ReplyProcessor21.PR_SEVERE_ALERT_RATIO * ackSDTimeout);
        }
      }
    }

    boolean directReply = false;
    if (directMsg != null
        && directMsg.supportsDirectAck()
        && threadOwnsResources()) {
      directReply = true;
    }

    //If this is a direct reply message, but we are sending it
    //over the shared socket, tell the message it needs to
    //use a regular reply processor.
    if (!directReply && directMsg != null) {
      directMsg.registerProcessor();
    }

    try {
    do {
      interrupted = interrupted || Thread.interrupted();
      /**
       * Exceptions that happened during one attempt to send
       */
      if (retryInfo != null) {
        // need to retry to each of the guys in the exception
        List retryMembers = retryInfo.getMembers();
        InternalDistributedMember[] retryDest = new InternalDistributedMember[retryMembers.size()];
        retryDest = (InternalDistributedMember[])retryMembers.toArray(retryDest);
        destinations = retryDest;
        retryInfo = null;
        retry = true;
      }
      final List cons = new ArrayList(destinations.length);
      ConnectExceptions ce = getConnections(mgr, msg, destinations, orderedMsg,
            retry, ackTimeout, ackSDTimeout, cons);
      if (directReply && msg.getProcessorId() > 0) { // no longer a direct-reply message?
        directReply = false;
      }
      if (ce != null) {
        if (failedCe != null) {
          failedCe.getMembers().addAll(ce.getMembers());
          failedCe.getCauses().addAll(ce.getCauses());
        } 
        else {
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
      }
      else {
        // sending to just one guy
        permissionCon = (Connection)cons.get(0);
        if (permissionCon != null) {
          try {
            permissionCon.acquireSendPermission();
          }
          catch (ConnectionException conEx) {
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
        if (logger.isDebugEnabled()) {
          logger.debug("{}{}) to {} peers ({}) via tcp/ip",
              (retry ? "Retrying send (" : "Sending ("),  msg, cons.size(), cons);
        }
        DMStats stats = getDMStats();
        List<?> sentCons; // used for cons we sent to this time

        final BaseMsgStreamer ms = MsgStreamer.create(cons, msg, directReply,
            stats);
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
        } 
        catch (NotSerializableException e) {
          throw e;
        } 
        catch (ToDataException e) {
          throw e;
        } 
        catch (IOException ex) {
          throw new InternalGemFireException(LocalizedStrings.DirectChannel_UNKNOWN_ERROR_SERIALIZING_MESSAGE.toLocalizedString(), ex);
        }
        finally {
          try {
            ms.close();
          } catch (IOException e) {
            throw new InternalGemFireException(
                "Unknown error serializing message", e);
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
            ce = readAcks(sentCons, startTime, ackTimeout, ackSDTimeout, ce, directMsg.getDirectReplyProcessor());
          } finally {
            if (stats != null) {
              stats.endReplyWait(readAckStart, startTime);
            }
          }
        }
      } finally {
        if (sendingToGroup) {
          releaseGroupSendPermission(orderedMsg);
        }
        else if (permissionCon != null) {
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
    }
    finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      for (Iterator it=totalSentCons.iterator(); it.hasNext();) {
        Connection con = (Connection)it.next();
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

    for (Iterator it=sentCons.iterator(); it.hasNext();) {
      Connection con = (Connection)it.next();
      //We don't expect replies on shared connections.
      if(con.isSharedResource()) {
        continue;
      }
      int msToWait = (int)(ackTimeout - (System.currentTimeMillis() - startTime));
      // if the wait threshold has already been reached during transmission
      // of the message, set a small wait period just to make sure the
      // acks haven't already come back
      if (msToWait <= 0) {
        msToWait = 10;
      }
      long msInterval = ackSDTimeout;
      if (msInterval <= 0) {
        msInterval = Math.max(ackTimeout, 1000);
      }
      try {
        try {
          con.readAck(msToWait, msInterval, processor);
        } 
        catch (SocketTimeoutException ex) {
          handleAckTimeout(ackTimeout, ackSDTimeout, con, processor);
        }
      } 
      catch (ConnectionException conEx) {
        if (ce == null) {
          ce = new ConnectExceptions();
        }
        ce.addFailure(con.getRemoteAddress(), conEx);
      }
    }
    return ce;
  }

  /**
   * Obtain the connections needed to transmit a message.  The connections are
   * put into the cons object (the last parameter)
   * 
   * @param mgr the membership manager
   * @param msg the message to send
   * @param destinations who to send the message to
   * @param preserveOrder true if the msg should ordered
   * @param retry whether this is a retransmission
   * @param ackTimeout the ack warning timeout
   * @param ackSDTimeout the ack severe alert timeout
   * @param cons a list to hold the connections
   * @return null if everything went okay, or a ConnectExceptions object if some connections couldn't be obtained
   */
  private ConnectExceptions getConnections(
      MembershipManager mgr, DistributionMessage msg,
      InternalDistributedMember[] destinations,
      boolean preserveOrder, boolean retry,
      long ackTimeout, long ackSDTimeout,
      List cons) {
    ConnectExceptions ce = null;
    for (int i=0; i < destinations.length; i++) {
      InternalDistributedMember destination = destinations[i];
      if (destination == null) {
        continue;
      }
      if (localAddr.equals(destination)) {
        // jgroups does not deliver messages to a sender, so we don't support
        // it here either.
        continue;
      }

      if (!mgr.memberExists(destination) || mgr.shutdownInProgress() || mgr.isShunned(destination)) {
        // This should only happen if the member is no longer in the view.
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "Not a member: {}", destination);
        }
        if (ce == null) ce = new ConnectExceptions();
        ce.addFailure(destination, new ShunnedMemberException(LocalizedStrings.DirectChannel_SHUNNING_0.toLocalizedString(destination)));
      }
      else {
        try {
          long startTime = 0;
          if (ackTimeout > 0) {
            startTime = System.currentTimeMillis();
          }
          Connection con = conduit.getConnection(destination, preserveOrder,
              retry, startTime, ackTimeout, ackSDTimeout);
          
          con.setInUse(true, startTime, 0, 0, null); // fix for bug#37657
          cons.add(con);
          if(con.isSharedResource() && msg instanceof DirectReplyMessage) {
            DirectReplyMessage directMessage = (DirectReplyMessage) msg;
            directMessage.registerProcessor();
          }
        } catch (IOException ex) {
          if (ce == null) ce = new ConnectExceptions();
          ce.addFailure(destination, ex);
        }
      }
    } // for
    return ce;
  }

  
  /**
   * Method send.
   * @param mgr - the membership manager
   * @param destinations - the address(es) to send the message to.
   * @param msg - the message to send
   * @param ackWaitThreshold
   * @param ackSAThreshold severe alert threshold
   * @return number of bytes sent
   * @throws ConnectExceptions if message could not be send to one or more
   *         of the <code>destinations</code>
   * @throws NotSerializableException
   *         If the content cannot be serialized
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
        logger.debug("Returning from DirectChannel send because empty destinations passed in {}", msg);
      }
      return 0;
    }

    msg.setSender(localAddr);
    if (destinations.length==1) {
      return sendToOne(mgr, destinations, msg, ackWaitThreshold, ackSAThreshold);
    } else {
      return sendToMany(mgr, destinations, msg, ackWaitThreshold, ackSAThreshold);
    }
  }
  
  

  /**
   * Returns null if no stats available.
   */
  public DMStats getDMStats() {
    DM dm = getDM();
    if (dm != null) {
      return dm.getStats(); // fix for bug#34004
    }
    else {
      return null;
    }
  }
  
  /**
   * Returns null if no config is available.
   * @since 4.2.2
   */
  public DistributionConfig getDMConfig() {
    DM dm = getDM();
    if (dm != null) {
      return dm.getConfig();
    }
    else {
      return null;
    }
  }
  
  /**
   * Returns null if no dm available.
   */
  public DM getDM() {
    return this.receiver.getDM();
  }
  
  /**
   * 
   * @param ackTimeout ack wait threshold
   * @param ackSATimeout severe alert threshold
   * @param c
   * @param processor 
   * @throws ConnectionException
   */
  private void handleAckTimeout(long ackTimeout, long ackSATimeout, Connection c, DirectReplyProcessor processor) 
      throws ConnectionException {
    DM dm = getDM();
    Set activeMembers = dm.getDistributionManagerIds();

    // Increment the stat
    dm.getStats().incReplyTimeouts();

    // an alert that will show up in the console
    {
      final StringId msg = LocalizedStrings.DirectChannel_0_SECONDS_HAVE_ELAPSED_WHILE_WAITING_FOR_REPLY_FROM_1_ON_2_WHOSE_CURRENT_MEMBERSHIP_LIST_IS_3;  
      final Object[] msgArgs = new Object[] {Long.valueOf(ackTimeout/1000), c.getRemoteAddress(), dm.getId(), activeMembers};
      logger.warn(LocalizedMessage.create(msg, msgArgs));
      msgArgs[3] = "(omitted)";
      Breadcrumbs.setProblem(msg, msgArgs);
      
      if (ReplyProcessor21.THROW_EXCEPTION_ON_TIMEOUT) {
        // init the cause to be a TimeoutException so catchers can determine cause
        TimeoutException cause = new TimeoutException(LocalizedStrings.TIMED_OUT_WAITING_FOR_ACKS.toLocalizedString());
        throw new InternalGemFireException(msg.toLocalizedString(msgArgs), cause);
      }
    }

    if (activeMembers.contains(c.getRemoteAddress())) {
      // wait for ack-severe-alert-threshold period first, then wait forever
      if (ackSATimeout > 0) {
        try {
          c.readAck((int)ackSATimeout, ackSATimeout, processor);
          return;
        }
        catch (SocketTimeoutException e) {
          Object[] args = new Object[] {Long.valueOf((ackSATimeout+ackTimeout)/1000), c.getRemoteAddress(), dm.getId(), activeMembers};
          logger.fatal(LocalizedMessage.create(
              LocalizedStrings.DirectChannel_0_SECONDS_HAVE_ELAPSED_WHILE_WAITING_FOR_REPLY_FROM_1_ON_2_WHOSE_CURRENT_MEMBERSHIP_LIST_IS_3, args));
        }
      }
      try {
        c.readAck(0, 0, processor);
      }
      catch (SocketTimeoutException ex) {
        // this can never happen when called with timeout of 0
        logger.error(LocalizedMessage.create(
            LocalizedStrings.DirectChannel_UNEXPECTED_TIMEOUT_WHILE_WAITING_FOR_ACK_FROM__0, c.getRemoteAddress()), ex);
      }
    } else {
      logger.warn(LocalizedMessage.create(
        LocalizedStrings.DirectChannel_VIEW_NO_LONGER_HAS_0_AS_AN_ACTIVE_MEMBER_SO_WE_WILL_NO_LONGER_WAIT_FOR_IT,
        c.getRemoteAddress()));
      processor.memberDeparted(c.getRemoteAddress(), true);
    }
  }

  
  public void receive(DistributionMessage msg, int bytesRead) {
    if (disconnected) {
      return;
    }
    try {
      receiver.messageReceived(msg);
    }
    catch (MemberShunnedException e) {
      throw e;
    }
    catch (CancelException e) {
      // ignore
    }
    catch (Exception ex) {
      // Don't freak out if the DM is shutting down
      if (this.conduit.getCancelCriterion().cancelInProgress() == null) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.DirectChannel_WHILE_PULLING_A_MESSAGE), ex);
      }
    }
  }

  public InternalDistributedMember getLocalAddress() {
    return this.localAddr;
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
   * This closes down the Direct connection.  Theoretically you can disconnect
   * and, if you need to use the channel again you can and it will automatically
   * reconnect.  Reconnection will cause a new local address to be generated.
   */
  public synchronized void disconnect(Exception cause) {
//    this.shutdownCause = cause;
    this.disconnected = true;
    this.disconnectCompleted = false;
    releaseGroupSendPermission(true);
    releaseGroupSendPermission(false);
    this.conduit.stop(cause);
    this.disconnectCompleted = true;
  }
  
  public boolean isOpen() {
    return !disconnectCompleted;
  }

  /** returns the receiver to which this DirectChannel is delivering messages */
  protected DirectChannelListener getReceiver() {
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
   * @since 2.1
   */
  public TCPConduit getConduit() {
    return this.conduit;
  }

  private InetAddress initAddress(DistributionConfig dc) {

    String bindAddress = dc.getBindAddress();

    try {
      /* note: had to change the following to make sure the prop wasn't empty 
         in addition to not null for admin.DistributedSystemFactory */
      if (bindAddress != null && bindAddress.length() > 0) {
        return InetAddress.getByName(bindAddress);

      }
      else {
       return SocketCreator.getLocalHost();
      }
    }
    catch (java.net.UnknownHostException unhe) {
      throw new RuntimeException(unhe);

    }
  }
  
  public void closeEndpoint(InternalDistributedMember member, String reason) {
    closeEndpoint(member, reason, true);
  }


  /**
   * Closes any connections used to communicate with the given jgroupsAddress.
   */
  public void closeEndpoint(InternalDistributedMember member, String reason, boolean notifyDisconnect) {
    TCPConduit tc = this.conduit;
    if (tc != null) {
      tc.removeEndpoint(member, reason, notifyDisconnect);
    }
  }

  /**
   * adds state for thread-owned serial connections to the given member to
   * the parameter <i>result</i>.  This can be used to wait for the state to
   * reach the given level in the member's vm.
   * @param member
   *    the member whose state is to be captured
   * @param result
   *    the map to add the state to
   * @since 5.1
   */
  public void getChannelStates(DistributedMember member, Map result)
  {
    TCPConduit tc = this.conduit;
    if (tc != null) {
      tc.getThreadOwnedOrderedConnectionState(member, result);
    }
  }
  
  /**
   * wait for the given connections to process the number of messages
   * associated with the connection in the given map
   */
  public void waitForChannelState(DistributedMember member, Map channelState)
    throws InterruptedException
  {
    if (Thread.interrupted()) throw new InterruptedException();
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
  
  /**
   * cause the channel to be sick
   */
  public void beSick() {
    TCPConduit tc = this.conduit;
    if (tc != null) {
      tc.beSick();
    }
  }
  
  /**
   * cause the channel to be healthy
   */
  public void beHealthy() {
    TCPConduit tc = this.conduit;
    if (tc != null) {
      tc.beHealthy();
    }
  }
  

}
