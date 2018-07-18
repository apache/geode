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
package org.apache.geode.distributed.internal.membership.gms.mgr;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemConnectException;
import org.apache.geode.SystemFailure;
import org.apache.geode.ToDataException;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.AdminMessageType;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionException;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.OverflowQueueWithDMStats;
import org.apache.geode.distributed.internal.ShutdownMessage;
import org.apache.geode.distributed.internal.SizeableRunnable;
import org.apache.geode.distributed.internal.StartupMessage;
import org.apache.geode.distributed.internal.direct.DirectChannel;
import org.apache.geode.distributed.internal.direct.DirectChannelListener;
import org.apache.geode.distributed.internal.direct.ShunnedMemberException;
import org.apache.geode.distributed.internal.membership.DistributedMembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.MembershipTestHook;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.QuorumChecker;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.SuspectMember;
import org.apache.geode.distributed.internal.membership.gms.fd.GMSHealthMonitor;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.partitioned.PartitionMessageWithDirectReply;
import org.apache.geode.internal.cache.xmlcache.CacheServerCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.log4j.AlertAppender;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.shared.StringPrintWriter;
import org.apache.geode.internal.tcp.ConnectExceptions;
import org.apache.geode.internal.tcp.MemberShunnedException;
import org.apache.geode.internal.util.Breadcrumbs;

public class GMSMembershipManager implements MembershipManager, Manager {
  private static final Logger logger = Services.getLogger();

  /** product version to use for multicast serialization */
  private volatile boolean disableMulticastForRollingUpgrade;

  /**
   * set to true if the distributed system that created this manager was auto-reconnecting when it
   * was created.
   */
  private boolean wasReconnectingSystem;

  /**
   * A quorum checker is created during reconnect and is held here so it is available to the UDP
   * protocol for passing off the ping-pong responses used in the quorum-checking algorithm.
   */
  private volatile QuorumChecker quorumChecker;

  /**
   * thread-local used to force use of Messenger for communications, usually to avoid deadlock when
   * conserve-sockets=true. Use of this should be removed when connection pools are implemented in
   * the direct-channel
   */
  private final ThreadLocal<Boolean> forceUseUDPMessaging =
      ThreadLocal.withInitial(() -> Boolean.FALSE);

  /**
   * Trick class to make the startup synch more visible in stack traces
   *
   * @see GMSMembershipManager#startupLock
   */
  static class EventProcessingLock {
    public EventProcessingLock() {}
  }

  static class StartupEvent {
    static final int SURPRISE_CONNECT = 1;
    static final int VIEW = 2;
    static final int MESSAGE = 3;

    /**
     * indicates whether the event is a departure, a surprise connect (i.e., before the view message
     * arrived), a view, or a regular message
     *
     * @see #SURPRISE_CONNECT
     * @see #VIEW
     * @see #MESSAGE
     */
    private final int kind;

    // Miscellaneous state depending on the kind of event
    InternalDistributedMember member;
    DistributionMessage dmsg;
    NetView gmsView;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("kind=");
      switch (kind) {
        case SURPRISE_CONNECT:
          sb.append("connect; member = <").append(member).append(">");
          break;
        case VIEW:
          String text = gmsView.toString();
          sb.append("view <").append(text).append(">");
          break;
        case MESSAGE:
          sb.append("message <").append(dmsg).append(">");
          break;
        default:
          sb.append("unknown=<").append(kind).append(">");
          break;
      }
      return sb.toString();
    }

    /**
     * Create a surprise connect event
     *
     * @param member the member connecting
     */
    StartupEvent(final InternalDistributedMember member) {
      this.kind = SURPRISE_CONNECT;
      this.member = member;
    }

    /**
     * Indicate if this is a surprise connect event
     *
     * @return true if this is a connect event
     */
    boolean isSurpriseConnect() {
      return this.kind == SURPRISE_CONNECT;
    }

    /**
     * Create a view event
     *
     * @param v the new view
     */
    StartupEvent(NetView v) {
      this.kind = VIEW;
      this.gmsView = v;
    }

    /**
     * Indicate if this is a view event
     *
     * @return true if this is a view event
     */
    boolean isGmsView() {
      return this.kind == VIEW;
    }

    /**
     * Create a message event
     *
     * @param d the message
     */
    StartupEvent(DistributionMessage d) {
      this.kind = MESSAGE;
      this.dmsg = d;
    }

    /**
     * Indicate if this is a message event
     *
     * @return true if this is a message event
     */
    boolean isDistributionMessage() {
      return this.kind == MESSAGE;
    }
  }

  private int membershipCheckTimeout =
      DistributionConfig.DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT;

  /**
   * This object synchronizes threads waiting for startup to finish. Updates to
   * {@link #startupMessages} are synchronized through this object.
   */
  private final EventProcessingLock startupLock = new EventProcessingLock();

  /**
   * This is the latest view (ordered list of DistributedMembers) that has been installed
   *
   * All accesses to this object are protected via {@link #latestViewLock}
   */
  private NetView latestView = new NetView();

  /**
   * This is the lock for protecting access to latestView
   *
   * @see #latestView
   */
  private final ReadWriteLock latestViewLock = new ReentrantReadWriteLock();
  private final Lock latestViewReadLock = latestViewLock.readLock();
  private final Lock latestViewWriteLock = latestViewLock.writeLock();

  /**
   * This is the listener that accepts our membership events
   */
  private final org.apache.geode.distributed.internal.membership.DistributedMembershipListener listener;

  /**
   * Membership failure listeners - for testing
   */
  private List membershipTestHooks;

  /**
   * This is a representation of the local member (ourself)
   */
  private InternalDistributedMember address = null; // new DistributedMember(-1);

  private DirectChannel directChannel;

  private MyDCReceiver dcReceiver;

  volatile boolean isJoining;

  /** have we joined successfully? */
  private volatile boolean hasJoined;

  /**
   * Members of the distributed system that we believe have shut down. Keys are instances of
   * {@link InternalDistributedMember}, values are Longs indicating the time this member was
   * shunned.
   *
   * Members are removed after {@link #SHUNNED_SUNSET} seconds have passed.
   *
   * Accesses to this list needs to be under the read or write lock of {@link #latestViewLock}
   *
   * @see System#currentTimeMillis()
   */
  // protected final Set shunnedMembers = Collections.synchronizedSet(new HashSet());
  private final Map<DistributedMember, Long> shunnedMembers = new ConcurrentHashMap<>();

  /**
   * Members that have sent a shutdown message. This is used to suppress suspect processing that
   * otherwise becomes pretty aggressive when a member is shutting down.
   */
  private final Map<DistributedMember, Object> shutdownMembers = new BoundedLinkedHashMap<>();

  /**
   * per bug 39552, keep a list of members that have been shunned and for which a message is
   * printed. Contents of this list are cleared at the same time they are removed from
   * {@link #shunnedMembers}.
   *
   * Accesses to this list needs to be under the read or write lock of {@link #latestViewLock}
   */
  private final HashSet<DistributedMember> shunnedAndWarnedMembers = new HashSet<>();
  /**
   * The identities and birth-times of others that we have allowed into membership at the
   * distributed system level, but have not yet appeared in a view.
   * <p>
   * Keys are instances of {@link InternalDistributedMember}, values are Longs indicating the time
   * this member was shunned.
   * <p>
   * Members are removed when a view containing them is processed. If, after
   * {@link #surpriseMemberTimeout} milliseconds have passed, a view containing the member has not
   * arrived, the member is removed from membership and member-left notification is performed.
   * <p>
   * > Accesses to this list needs to be under the read or write lock of {@link #latestViewLock}
   *
   * @see System#currentTimeMillis()
   */
  private final Map<InternalDistributedMember, Long> surpriseMembers = new ConcurrentHashMap<>();

  /**
   * the timeout interval for surprise members. This is calculated from the member-timeout setting
   */
  private int surpriseMemberTimeout;

  /**
   * javagroups can skip views and omit telling us about a crashed member. This map holds a history
   * of suspected members that we use to detect crashes.
   */
  private final Map<InternalDistributedMember, Long> suspectedMembers = new ConcurrentHashMap<>();

  /**
   * Length of time, in seconds, that a member is retained in the zombie set
   *
   * @see #shunnedMembers
   */
  private static final int SHUNNED_SUNSET = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "shunned-member-timeout", 300).intValue();

  /**
   * Set to true when the service should stop.
   */
  private volatile boolean shutdownInProgress = false;

  /**
   * Set to true when upcalls should be generated for events.
   */
  private volatile boolean processingEvents = false;

  /**
   * This is the latest viewId installed
   */
  private long latestViewId = -1;

  /**
   * A list of messages received during channel startup that couldn't be processed yet. Additions or
   * removals of this list must be synchronized via {@link #startupLock}.
   *
   * @since GemFire 5.0
   */
  private final LinkedList<StartupEvent> startupMessages = new LinkedList<>();

  /**
   * ARB: the map of latches is used to block peer handshakes till authentication is confirmed.
   */
  private final HashMap<DistributedMember, CountDownLatch> memberLatch = new HashMap<>();

  /**
   * Insert our own MessageReceiver between us and the direct channel, in order to correctly filter
   * membership events.
   *
   *
   */
  class MyDCReceiver implements DirectChannelListener {

    final DirectChannelListener upCall;

    /**
     * Don't provide events until the caller has told us we are ready.
     *
     * Synchronization provided via GroupMembershipService.class.
     *
     * Note that in practice we only need to delay accepting the first client; we don't need to put
     * this check before every call...
     *
     */
    MyDCReceiver(DirectChannelListener up) {
      upCall = up;
    }

    public void messageReceived(DistributionMessage msg) {
      // bug 36851 - notify failure detection that we've had contact from a member
      services.getHealthMonitor().contactedBy(msg.getSender());
      handleOrDeferMessage(msg);
    }

    public ClusterDistributionManager getDM() {
      return upCall.getDM();
    }

  }


  /**
   * Analyze a given view object, generate events as appropriate
   */
  protected void processView(long newViewId, NetView newView) {
    // Sanity check...
    if (logger.isDebugEnabled()) {
      StringBuilder msg = new StringBuilder(200);
      msg.append("Membership: Processing view ");
      msg.append(newView);
      msg.append("} on ").append(address.toString());
      logger.debug(msg);
      if (!newView.contains(address)) {
        logger.info(LocalizedMessage.create(
            LocalizedStrings.GroupMembershipService_THE_MEMBER_WITH_ID_0_IS_NO_LONGER_IN_MY_OWN_VIEW_1,
            new Object[] {address, newView}));
      }
    }

    // We perform the update under a global lock so that other
    // incoming events will not be lost in terms of our global view.
    latestViewWriteLock.lock();
    try {
      // first determine the version for multicast message serialization
      Version version = Version.CURRENT;
      for (final Entry<InternalDistributedMember, Long> internalDistributedMemberLongEntry : surpriseMembers
          .entrySet()) {
        InternalDistributedMember mbr = internalDistributedMemberLongEntry.getKey();
        Version itsVersion = mbr.getVersionObject();
        if (itsVersion != null && version.compareTo(itsVersion) < 0) {
          version = itsVersion;
        }
      }
      for (InternalDistributedMember mbr : newView.getMembers()) {
        Version itsVersion = mbr.getVersionObject();
        if (itsVersion != null && itsVersion.compareTo(version) < 0) {
          version = mbr.getVersionObject();
        }
      }
      disableMulticastForRollingUpgrade = !version.equals(Version.CURRENT);

      if (newViewId < latestViewId) {
        // ignore this view since it is old news
        return;
      }

      // Save previous view, for delta analysis
      NetView priorView = latestView;

      // update the view to reflect our changes, so that
      // callbacks will see the new (updated) view.
      latestViewId = newViewId;
      latestView = new NetView(newView, newView.getViewId());

      // look for additions
      for (int i = 0; i < newView.getMembers().size(); i++) { // additions
        InternalDistributedMember m = newView.getMembers().get(i);

        // Once a member has been seen via a view, remove them from the
        // newborn set. Replace the netmember of the surpriseMember ID
        // in case it was a partial ID and is being retained by DistributionManager
        // or some other object
        boolean wasSurprise = surpriseMembers.containsKey(m);
        if (wasSurprise) {
          for (Iterator<Map.Entry<InternalDistributedMember, Long>> iterator =
              surpriseMembers.entrySet().iterator(); iterator.hasNext();) {
            Entry<InternalDistributedMember, Long> entry = iterator.next();
            if (entry.getKey().equals(m)) {
              entry.getKey().setNetMember(m.getNetMember());
              iterator.remove();
              break;
            }
          }
        }

        // if it's in a view, it's no longer suspect
        suspectedMembers.remove(m);

        if (priorView.contains(m) || wasSurprise) {
          continue; // already seen
        }

        // unblock any waiters for this particular member.
        // i.e. signal any waiting threads in tcpconduit.
        String authInit =
            this.services.getConfig().getDistributionConfig().getSecurityPeerAuthInit();
        boolean isSecure = authInit != null && authInit.length() != 0;

        if (isSecure) {
          CountDownLatch currentLatch;
          if ((currentLatch = memberLatch.get(m)) != null) {
            currentLatch.countDown();
          }
        }

        if (shutdownInProgress()) {
          addShunnedMember(m);
          continue; // no additions processed after shutdown begins
        } else {
          boolean wasShunned = endShun(m); // bug #45158 - no longer shun a process that is now in
          // view
          if (wasShunned && logger.isDebugEnabled()) {
            logger.debug("No longer shunning {} as it is in the current membership view", m);
          }
        }

        logger.info(LocalizedMessage
            .create(LocalizedStrings.GroupMembershipService_MEMBERSHIP_PROCESSING_ADDITION__0_, m));

        try {
          listener.newMemberConnected(m);
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (DistributedSystemDisconnectedException e) {
          // don't log shutdown exceptions
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.info(LocalizedMessage.create(
              LocalizedStrings.GroupMembershipService_MEMBERSHIP_FAULT_WHILE_PROCESSING_VIEW_ADDITION_OF__0,
              m), t);
        }
      } // additions

      // look for departures
      for (int i = 0; i < priorView.getMembers().size(); i++) { // departures
        InternalDistributedMember m = priorView.getMembers().get(i);
        if (newView.contains(m)) {
          continue; // still alive
        }

        if (surpriseMembers.containsKey(m)) {
          continue; // member has not yet appeared in a view
        }

        try {
          removeWithViewLock(m,
              newView.getCrashedMembers().contains(m) || suspectedMembers.containsKey(m),
              "departed membership view");
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.info(LocalizedMessage.create(
              LocalizedStrings.GroupMembershipService_MEMBERSHIP_FAULT_WHILE_PROCESSING_VIEW_REMOVAL_OF__0,
              m), t);
        }
      } // departures

      // expire surprise members, add others to view
      long oldestAllowed = System.currentTimeMillis() - this.surpriseMemberTimeout;
      for (Iterator<Map.Entry<InternalDistributedMember, Long>> it =
          surpriseMembers.entrySet().iterator(); it.hasNext();) {
        Map.Entry<InternalDistributedMember, Long> entry = it.next();
        Long birthtime = entry.getValue();
        if (birthtime.longValue() < oldestAllowed) {
          it.remove();
          InternalDistributedMember m = entry.getKey();
          logger.info(LocalizedMessage.create(
              LocalizedStrings.GroupMembershipService_MEMBERSHIP_EXPIRING_MEMBERSHIP_OF_SURPRISE_MEMBER_0,
              m));
          removeWithViewLock(m, true,
              "not seen in membership view in " + this.surpriseMemberTimeout + "ms");
        } else {
          if (!latestView.contains(entry.getKey())) {
            latestView.add(entry.getKey());
          }
        }
      }
      // expire suspected members
      /*
       * the timeout interval for suspected members
       */
      final long suspectMemberTimeout = 180000;
      oldestAllowed = System.currentTimeMillis() - suspectMemberTimeout;
      for (Iterator it = suspectedMembers.entrySet().iterator(); it.hasNext();) {
        Map.Entry entry = (Map.Entry) it.next();
        Long birthtime = (Long) entry.getValue();
        if (birthtime.longValue() < oldestAllowed) {
          it.remove();
        }
      }
      try {
        listener.viewInstalled(latestView);
      } catch (DistributedSystemDisconnectedException se) {
      }
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  public boolean isCleanupTimerStarted() {
    return this.cleanupTimer != null;
  }

  /**
   * the timer used to perform periodic tasks
   *
   * Concurrency: protected by {@link #latestViewLock} ReentrantReadWriteLock
   */
  private SystemTimer cleanupTimer;

  private Services services;

  private boolean mcastEnabled;

  private boolean tcpDisabled;


  @Override
  public boolean isMulticastAllowed() {
    return !disableMulticastForRollingUpgrade;
  }

  /**
   * Joins the distributed system
   *
   * @throws GemFireConfigException - configuration error
   * @throws SystemConnectException - problem joining
   */
  private void join() {
    services.setShutdownCause(null);
    services.getCancelCriterion().cancel(null);

    latestViewWriteLock.lock();
    try {
      try {
        this.isJoining = true; // added for bug #44373

        // connect
        long start = System.currentTimeMillis();

        boolean ok = services.getJoinLeave().join();

        if (!ok) {
          throw new GemFireConfigException("Unable to join the distributed system.  "
              + "Operation either timed out, was stopped or Locator does not exist.");
        }

        long delta = System.currentTimeMillis() - start;

        logger.info(LogMarker.DISTRIBUTION_MARKER, LocalizedMessage
            .create(LocalizedStrings.GroupMembershipService_JOINED_TOOK__0__MS, delta));

        NetView initialView = services.getJoinLeave().getView();
        latestView = new NetView(initialView, initialView.getViewId());
        listener.viewInstalled(latestView);

      } catch (RuntimeException ex) {
        throw ex;
      } catch (Exception ex) {
        if (ex.getCause() != null && ex.getCause().getCause() instanceof SystemConnectException) {
          throw (SystemConnectException) (ex.getCause().getCause());
        }
        throw new DistributionException(
            LocalizedStrings.GroupMembershipService_AN_EXCEPTION_WAS_THROWN_WHILE_JOINING
                .toLocalizedString(),
            ex);
      } finally {
        this.isJoining = false;
      }
    } finally {
      latestViewWriteLock.unlock();
    }
  }


  public GMSMembershipManager(DistributedMembershipListener listener) {
    Assert.assertTrue(listener != null);
    this.listener = listener;
  }

  @Override
  public void init(Services services) {
    this.services = services;

    Assert.assertTrue(services != null);

    DistributionConfig config = services.getConfig().getDistributionConfig();
    RemoteTransportConfig transport = services.getConfig().getTransport();

    this.membershipCheckTimeout = config.getSecurityPeerMembershipTimeout();
    this.wasReconnectingSystem = transport.getIsReconnectingDS();

    // cache these settings for use in send()
    this.mcastEnabled = transport.isMcastEnabled();
    this.tcpDisabled = transport.isTcpDisabled();

    if (!this.tcpDisabled) {
      dcReceiver = new MyDCReceiver(listener);
    }

    surpriseMemberTimeout =
        Math.max(20 * DistributionConfig.DEFAULT_MEMBER_TIMEOUT, 20 * config.getMemberTimeout());
    surpriseMemberTimeout =
        Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "surprise-member-timeout",
            surpriseMemberTimeout).intValue();

  }

  @Override
  public void start() {
    DistributionConfig config = services.getConfig().getDistributionConfig();

    int dcPort = 0;
    if (!tcpDisabled) {
      directChannel = new DirectChannel(this, dcReceiver, config);
      dcPort = directChannel.getPort();
    }


    services.getMessenger().getMemberID().setDirectChannelPort(dcPort);

  }


  @Override
  public void joinDistributedSystem() {
    long startTime = System.currentTimeMillis();

    try {
      join();
    } catch (RuntimeException e) {
      if (directChannel != null) {
        directChannel.disconnect(e);
      }
      throw e;
    }

    this.address = services.getMessenger().getMemberID();

    if (directChannel != null) {
      directChannel.setLocalAddr(address);
    }

    this.hasJoined = true;

    // in order to debug startup issues we need to announce the membership
    // ID as soon as we know it
    logger.info(LocalizedMessage.create(
        LocalizedStrings.GroupMembershipService_entered_into_membership_in_group_0_with_id_1,
        new Object[] {"" + (System.currentTimeMillis() - startTime)}));

  }

  @Override
  public void started() {
    startCleanupTimer();
  }


  /** this is invoked by JoinLeave when there is a loss of quorum in the membership system */
  public void quorumLost(Collection<InternalDistributedMember> failures, NetView view) {
    // notify of quorum loss if split-brain detection is enabled (meaning we'll shut down) or
    // if the loss is more than one member

    boolean notify = failures.size() > 1;
    if (!notify) {
      notify = services.getConfig().isNetworkPartitionDetectionEnabled();
    }

    if (notify) {
      List<InternalDistributedMember> remaining = new ArrayList<>(view.getMembers());
      remaining.removeAll(failures);

      if (inhibitForceDisconnectLogging) {
        if (logger.isDebugEnabled()) {
          logger.debug("<ExpectedException action=add>Possible loss of quorum</ExpectedException>");
        }
      }
      logger.fatal(LocalizedMessage.create(
          LocalizedStrings.GroupMembershipService_POSSIBLE_LOSS_OF_QUORUM_DETECTED,
          new Object[] {failures.size(), failures}));
      if (inhibitForceDisconnectLogging) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "<ExpectedException action=remove>Possible loss of quorum</ExpectedException>");
        }
      }


      try {
        this.listener.quorumLost(new HashSet<>(failures), remaining);
      } catch (CancelException e) {
        // safe to ignore - a forced disconnect probably occurred
      }
    }
  }


  @Override
  public boolean testMulticast() {
    try {
      return services.getMessenger().testMulticast(services.getConfig().getMemberTimeout());
    } catch (InterruptedException e) {
      services.getCancelCriterion().checkCancelInProgress(e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * Remove a member. {@link #latestViewLock} must be held before this method is called. If member
   * is not already shunned, the uplevel event handler is invoked.
   */
  private void removeWithViewLock(InternalDistributedMember dm, boolean crashed, String reason) {
    boolean wasShunned = isShunned(dm);

    // Delete resources
    destroyMember(dm, reason);

    if (wasShunned) {
      return; // Explicit deletion, no upcall.
    }

    try {
      listener.memberDeparted(dm, crashed, reason);
    } catch (DistributedSystemDisconnectedException se) {
      // let's not get huffy about it
    }
  }

  /**
   * Process a surprise connect event, or place it on the startup queue.
   *
   * @param member the member
   */
  protected void handleOrDeferSurpriseConnect(InternalDistributedMember member) {
    synchronized (startupLock) {
      if (!processingEvents) {
        startupMessages.add(new StartupEvent(member));
        return;
      }
    }
    processSurpriseConnect(member);
  }

  public void startupMessageFailed(DistributedMember mbr, String failureMessage) {
    // fix for bug #40666
    addShunnedMember((InternalDistributedMember) mbr);
    // fix for bug #41329, hang waiting for replies
    try {
      listener.memberDeparted((InternalDistributedMember) mbr, true,
          "failed to pass startup checks");
    } catch (DistributedSystemDisconnectedException se) {
      // let's not get huffy about it
    }
  }


  /**
   * Logic for handling a direct connection event (message received from a member not in the view).
   * Does not employ the startup queue.
   * <p>
   * Must be called with {@link #latestViewLock} held. Waits until there is a stable view. If the
   * member has already been added, simply returns; else adds the member.
   *
   * @param dm the member joining
   */
  public boolean addSurpriseMember(DistributedMember dm) {
    final InternalDistributedMember member = (InternalDistributedMember) dm;
    boolean warn = false;

    latestViewWriteLock.lock();
    try {
      // At this point, the join may have been discovered by
      // other means.
      if (latestView.contains(member)) {
        return true;
      }
      if (surpriseMembers.containsKey(member)) {
        return true;
      }
      if (member.getVmViewId() < 0) {
        logger.warn(
            "adding a surprise member that has not yet joined the distributed system: " + member,
            new Exception("stack trace"));
      }
      if (latestView.getViewId() > member.getVmViewId()) {
        // tell the process that it should shut down distribution.
        // Run in a separate thread so we don't hold the view lock during the request. Bug #44995
        new Thread(Thread.currentThread().getThreadGroup(),
            "Removing shunned GemFire node " + member) {
          @Override
          public void run() {
            // fix for bug #42548
            // this is an old member that shouldn't be added
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.GroupMembershipService_Invalid_Surprise_Member,
                new Object[] {member, latestView}));
            try {
              requestMemberRemoval(member,
                  "this member is no longer in the view but is initiating connections");
            } catch (CancelException e) {
              // okay to ignore
            }
          }
        }.start();
        addShunnedMember(member);
        return false;
      }

      // Adding him to this set ensures we won't remove him if a new
      // view comes in and he's still not visible.
      surpriseMembers.put(member, Long.valueOf(System.currentTimeMillis()));

      if (shutdownInProgress()) {
        // Force disconnect, esp. the TCPConduit
        String msg =
            LocalizedStrings.GroupMembershipService_THIS_DISTRIBUTED_SYSTEM_IS_SHUTTING_DOWN
                .toLocalizedString();
        if (directChannel != null) {
          try {
            directChannel.closeEndpoint(member, msg);
          } catch (DistributedSystemDisconnectedException e) {
            // ignore - happens during shutdown
          }
        }
        destroyMember(member, msg); // for good luck
        return true; // allow during shutdown
      }

      if (isShunned(member)) {
        warn = true;
        surpriseMembers.remove(member);
      } else {

        // Ensure that the member is accounted for in the view
        // Conjure up a new view including the new member. This is necessary
        // because we are about to tell the listener about a new member, so
        // the listener should rightfully expect that the member is in our
        // membership view.

        // However, we put the new member at the end of the list. This
        // should ensure he's not chosen as an elder.
        // This will get corrected when he finally shows up in the
        // view.
        NetView newMembers = new NetView(latestView, latestView.getViewId());
        newMembers.add(member);
        latestView = newMembers;
      }
    } finally {
      latestViewWriteLock.unlock();
    }
    if (warn) { // fix for bug #41538 - deadlock while alerting
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.GroupMembershipService_MEMBERSHIP_IGNORING_SURPRISE_CONNECT_FROM_SHUNNED_MEMBER_0,
          member));
    } else {
      listener.newMemberConnected(member);
    }
    return !warn;
  }


  /** starts periodic task to perform cleanup chores such as expire surprise members */
  private void startCleanupTimer() {
    if (this.listener == null || listener.getDM() == null) {
      return;
    }
    DistributedSystem ds = this.listener.getDM().getSystem();
    this.cleanupTimer = new SystemTimer(ds, true);
    SystemTimer.SystemTimerTask st = new SystemTimer.SystemTimerTask() {
      @Override
      public void run2() {
        cleanUpSurpriseMembers();
      }
    };
    this.cleanupTimer.scheduleAtFixedRate(st, surpriseMemberTimeout, surpriseMemberTimeout / 3);
  }

  // invoked from the cleanupTimer task
  private void cleanUpSurpriseMembers() {
    latestViewWriteLock.lock();
    try {
      long oldestAllowed = System.currentTimeMillis() - surpriseMemberTimeout;
      for (Iterator it = surpriseMembers.entrySet().iterator(); it.hasNext();) {
        Map.Entry entry = (Map.Entry) it.next();
        Long birthtime = (Long) entry.getValue();
        if (birthtime.longValue() < oldestAllowed) {
          it.remove();
          InternalDistributedMember m = (InternalDistributedMember) entry.getKey();
          logger.info(LocalizedMessage.create(
              LocalizedStrings.GroupMembershipService_MEMBERSHIP_EXPIRING_MEMBERSHIP_OF_SURPRISE_MEMBER_0,
              m));
          removeWithViewLock(m, true,
              "not seen in membership view in " + surpriseMemberTimeout + "ms");
        }
      }
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  /**
   * Dispatch the distribution message, or place it on the startup queue.
   *
   * @param msg the message to process
   */
  protected void handleOrDeferMessage(DistributionMessage msg) {
    synchronized (startupLock) {
      if (beingSick || playingDead) {
        // cache operations are blocked in a "sick" member
        if (msg.containsRegionContentChange() || msg instanceof PartitionMessageWithDirectReply) {
          startupMessages.add(new StartupEvent(msg));
          return;
        }
      }
      if (!processingEvents) {
        startupMessages.add(new StartupEvent(msg));
        return;
      }
    }
    dispatchMessage(msg);
  }

  public void warnShun(DistributedMember m) {
    latestViewWriteLock.lock();
    try {
      if (!shunnedMembers.containsKey(m)) {
        return; // not shunned
      }
      if (shunnedAndWarnedMembers.contains(m)) {
        return; // already warned
      }
      shunnedAndWarnedMembers.add(m);
    } finally {
      latestViewWriteLock.unlock();
    }
    // issue warning outside of sync since it may cause messaging and we don't
    // want to hold the view lock while doing that
    logger.warn(LocalizedMessage.create(
        LocalizedStrings.GroupMembershipService_MEMBERSHIP_DISREGARDING_SHUNNED_MEMBER_0, m));
  }

  @Override
  public void processMessage(DistributionMessage msg) {
    handleOrDeferMessage(msg);
  }

  /**
   * Logic for processing a distribution message.
   * <p>
   * It is possible to receive messages not consistent with our view. We handle this here, and
   * generate an uplevel event if necessary
   *
   * @param msg the message
   */
  private void dispatchMessage(DistributionMessage msg) {
    InternalDistributedMember m = msg.getSender();
    boolean shunned = false;

    // UDP messages received from surprise members will have partial IDs.
    // Attempt to replace these with full IDs from the MembershipManager's view.
    if (msg.getSender().isPartial()) {
      replacePartialIdentifierInMessage(msg);
    }

    // If this member is shunned or new, grab the latestViewWriteLock: update the appropriate data
    // structure.
    if (isShunnedOrNew(m)) {
      latestViewWriteLock.lock();
      try {
        if (isShunned(m)) {
          if (msg instanceof StartupMessage) {
            endShun(m);
          } else {
            // fix for bug 41538 - sick alert listener causes deadlock
            // due to view latestViewReadWriteLock being held during messaging
            shunned = true;
          }
        }

        if (!shunned) {
          // If it's a new sender, wait our turn, generate the event
          if (isNew(m)) {
            shunned = !addSurpriseMember(m);
          }
        }
      } finally {
        latestViewWriteLock.unlock();
      }
    }

    if (shunned) { // bug #41538 - shun notification must be outside synchronization to avoid
      // hanging
      warnShun(m);
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_VIEWS_VERBOSE)) {
        logger.trace(LogMarker.DISTRIBUTION_VIEWS_VERBOSE,
            "Membership: Ignoring message from shunned member <{}>:{}", m, msg);
      }
      throw new MemberShunnedException(m);
    }

    listener.messageReceived(msg);
  }

  /**
   * If the message's sender ID is a partial ID, which can happen if it's received in a JGroups
   * message, replace it with an ID from the current membership view.
   *
   * @param msg the message holding the sender ID
   */
  public void replacePartialIdentifierInMessage(DistributionMessage msg) {
    InternalDistributedMember sender = msg.getSender();
    sender = this.services.getJoinLeave().getMemberID(sender.getNetMember());
    if (sender.isPartial()) {
      // the DM's view also has surprise members, so let's check it as well
      sender = this.dcReceiver.getDM().getCanonicalId(sender);
    }
    if (!sender.isPartial()) {
      msg.setSender(sender);
    }
  }


  /**
   * Process a new view object, or place on the startup queue
   *
   * @param viewArg the new view
   */
  protected void handleOrDeferViewEvent(NetView viewArg) {
    if (this.isJoining) {
      // bug #44373 - queue all view messages while joining.
      // This is done under the latestViewLock, but we can't block here because
      // we're sitting in the UDP reader thread.
      synchronized (startupLock) {
        startupMessages.add(new StartupEvent(viewArg));
        return;
      }
    }
    latestViewWriteLock.lock();
    try {
      synchronized (startupLock) {
        if (!processingEvents) {
          startupMessages.add(new StartupEvent(viewArg));
          return;
        }
      }
      // view processing can take a while, so we use a separate thread
      // to avoid blocking a reader thread
      long newId = viewArg.getViewId();
      LocalViewMessage v = new LocalViewMessage(address, newId, viewArg, GMSMembershipManager.this);

      listener.messageReceived(v);
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  @Override
  public void memberSuspected(InternalDistributedMember initiator,
      InternalDistributedMember suspect, String reason) {
    SuspectMember s = new SuspectMember(initiator, suspect, reason);
    handleOrDeferSuspect(s);
  }

  /**
   * Process a new view object, or place on the startup queue
   *
   * @param suspectInfo the suspectee and suspector
   */
  protected void handleOrDeferSuspect(SuspectMember suspectInfo) {
    latestViewWriteLock.lock();
    try {
      synchronized (startupLock) {
        if (!processingEvents) {
          return;
        }
      }
      InternalDistributedMember suspect = suspectInfo.suspectedMember;
      InternalDistributedMember who = suspectInfo.whoSuspected;
      this.suspectedMembers.put(suspect, Long.valueOf(System.currentTimeMillis()));
      try {
        listener.memberSuspect(suspect, who, suspectInfo.reason);
      } catch (DistributedSystemDisconnectedException se) {
        // let's not get huffy about it
      }
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  /**
   * Process a potential direct connect. Does not use the startup queue. It grabs the
   * {@link #latestViewLock} and then processes the event.
   * <p>
   * It is a <em>potential</em> event, because we don't know until we've grabbed a stable view if
   * this is really a new member.
   */
  private void processSurpriseConnect(InternalDistributedMember member) {
    addSurpriseMember(member);
  }

  /**
   * Dispatch routine for processing a single startup event
   *
   * @param o the startup event to handle
   */
  private void processStartupEvent(StartupEvent o) {
    // Most common events first

    if (o.isDistributionMessage()) { // normal message
      try {
        dispatchMessage(o.dmsg);
      } catch (MemberShunnedException e) {
        // message from non-member - ignore
      }
    } else if (o.isGmsView()) { // view event
      processView(o.gmsView.getViewId(), o.gmsView);
    } else if (o.isSurpriseConnect()) { // connect
      processSurpriseConnect(o.member);
    }

    else // sanity
      throw new InternalGemFireError(
          LocalizedStrings.GroupMembershipService_UNKNOWN_STARTUP_EVENT_0.toLocalizedString(o));
  }

  /**
   * Special mutex to create a critical section for {@link #startEventProcessing()}
   */
  private final Object startupMutex = new Object();


  public void startEventProcessing() {
    // Only allow one thread to perform the work
    synchronized (startupMutex) {
      if (logger.isDebugEnabled())
        logger.debug("Membership: draining startup events.");
      // Remove the backqueue of messages, but allow
      // additional messages to be added.
      for (;;) {
        StartupEvent ev;
        // Only grab the mutex while reading the queue.
        // Other events may arrive while we're attempting to
        // drain the queue. This is OK, we'll just keep processing
        // events here until we've caught up.
        synchronized (startupLock) {
          int remaining = startupMessages.size();
          if (remaining == 0) {
            // While holding the lock, flip the bit so that
            // no more events get put into startupMessages, and
            // notify all waiters to proceed.
            processingEvents = true;
            startupLock.notifyAll();
            break; // ...and we're done.
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Membership: {} remaining startup message(s)", remaining);
          }
          ev = startupMessages.removeFirst();
        } // startupLock
        try {
          processStartupEvent(ev);
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.warn(
              LocalizedMessage.create(
                  LocalizedStrings.GroupMembershipService_MEMBERSHIP_ERROR_HANDLING_STARTUP_EVENT),
              t);
        }

      } // for
      if (logger.isDebugEnabled())
        logger.debug("Membership: finished processing startup events.");
    } // startupMutex
  }


  public void waitForEventProcessing() throws InterruptedException {
    // First check outside of a synchronized block. Cheaper and sufficient.
    if (Thread.interrupted())
      throw new InterruptedException();
    if (processingEvents)
      return;
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: waiting until the system is ready for events");
    }
    for (;;) {
      directChannel.getCancelCriterion().checkCancelInProgress(null);
      synchronized (startupLock) {
        // Now check using a memory fence and synchronization.
        if (processingEvents)
          break;
        boolean interrupted = Thread.interrupted();
        try {
          startupLock.wait();
        } catch (InterruptedException e) {
          interrupted = true;
          directChannel.getCancelCriterion().checkCancelInProgress(e);
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // synchronized
    } // for
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: continuing");
    }
  }

  /**
   * for testing we need to validate the startup event list
   */
  public List<StartupEvent> getStartupEvents() {
    return this.startupMessages;
  }

  public ReadWriteLock getViewLock() {
    return this.latestViewLock;
  }

  /**
   * Returns a copy (possibly not current) of the current view (a list of
   * {@link DistributedMember}s)
   */
  public NetView getView() {
    // Grab the latest view under a mutex...
    NetView v;

    latestViewReadLock.lock();
    v = latestView;
    latestViewReadLock.unlock();

    NetView result = new NetView(v, v.getViewId());

    v.getMembers().stream().filter(this::isShunned).forEachOrdered(result::remove);

    return result;
  }

  /**
   * test hook
   * <p>
   * The lead member is the eldest member with partition detection enabled.
   * <p>
   * If no members have partition detection enabled, there will be no lead member and this method
   * will return null.
   *
   * @return the lead member associated with the latest view
   */
  public DistributedMember getLeadMember() {
    latestViewReadLock.lock();
    try {
      return latestView == null ? null : latestView.getLeadMember();
    } finally {
      latestViewReadLock.unlock();
    }
  }

  private boolean isJoining() {
    return this.isJoining;
  }

  /**
   * test hook
   *
   * @return the current membership view coordinator
   */
  @Override
  public DistributedMember getCoordinator() {
    latestViewReadLock.lock();
    try {
      return latestView == null ? null : latestView.getCoordinator();
    } finally {
      latestViewReadLock.unlock();
    }
  }

  public boolean memberExists(DistributedMember m) {
    latestViewReadLock.lock();
    NetView v = latestView;
    latestViewReadLock.unlock();
    return v.contains(m);
  }

  /**
   * Returns the identity associated with this member. WARNING: this value will be returned after
   * the channel is closed, but in that case it is good for logging purposes only. :-)
   */
  public InternalDistributedMember getLocalMember() {
    return address;
  }

  public Services getServices() {
    return services;
  }

  public void postConnect() {}

  /**
   * @see SystemFailure#loadEmergencyClasses() /** break any potential circularity in
   *      {@link #loadEmergencyClasses()}
   */
  private static volatile boolean emergencyClassesLoaded = false;

  /**
   * inhibits logging of ForcedDisconnectException to keep dunit logs clean while testing this
   * feature
   */
  private static volatile boolean inhibitForceDisconnectLogging;

  /**
   * Ensure that the critical classes from components get loaded.
   *
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    if (emergencyClassesLoaded)
      return;
    emergencyClassesLoaded = true;
    DirectChannel.loadEmergencyClasses();
    GMSJoinLeave.loadEmergencyClasses();
    GMSHealthMonitor.loadEmergencyClasses();
  }

  /**
   * Close the receiver, avoiding all potential deadlocks and eschewing any attempts at being
   * graceful.
   *
   * @see SystemFailure#emergencyClose()
   */
  public void emergencyClose() {
    setShutdown();

    // We can't call close() because they will allocate objects. Attempt
    // a surgical strike and touch the important protocols.

    // MOST important, kill the FD protocols...
    services.emergencyClose();

    // Close the TCPConduit sockets...
    if (directChannel != null) {
      directChannel.emergencyClose();
    }

  }


  /**
   * in order to avoid split-brain occurring when a member is shutting down due to race conditions
   * in view management we add it as a shutdown member when we receive a shutdown message. This is
   * not the same as a SHUNNED member.
   */
  public void shutdownMessageReceived(InternalDistributedMember id, String reason) {
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: recording shutdown status of {}", id);
    }
    synchronized (this.shutdownMembers) {
      this.shutdownMembers.put(id, id);
      services.getHealthMonitor().memberShutdown(id, reason);
      services.getJoinLeave().memberShutdown(id, reason);
    }
  }

  public void shutdown() {
    setShutdown();
    services.stop();
  }

  @Override
  public void stop() {

    // [bruce] Do not null out the channel w/o adding appropriate synchronization

    logger.debug("MembershipManager closing");

    if (directChannel != null) {
      directChannel.disconnect(null);

      if (address != null) {
        // Make sure that channel information is consistent
        // Probably not important in this particular case, but just
        // to be consistent...
        latestViewWriteLock.lock();
        try {
          destroyMember(address, "orderly shutdown");
        } finally {
          latestViewWriteLock.unlock();
        }
      }
    }

    if (cleanupTimer != null) {
      cleanupTimer.cancel();
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Membership: channel closed");
    }
  }

  public void uncleanShutdown(String reason, final Exception e) {
    inhibitForcedDisconnectLogging(false);

    if (services.getShutdownCause() == null) {
      services.setShutdownCause(e);
    }

    if (this.directChannel != null) {
      this.directChannel.disconnect(e);
    }

    // first shut down communication so we don't do any more harm to other
    // members
    services.emergencyClose();

    if (e != null) {
      try {
        if (membershipTestHooks != null) {
          List l = membershipTestHooks;
          for (final Object aL : l) {
            MembershipTestHook dml = (MembershipTestHook) aL;
            dml.beforeMembershipFailure(reason, e);
          }
        }
        listener.membershipFailure(reason, e);
        if (membershipTestHooks != null) {
          List l = membershipTestHooks;
          for (final Object aL : l) {
            MembershipTestHook dml = (MembershipTestHook) aL;
            dml.afterMembershipFailure(reason, e);
          }
        }
      } catch (RuntimeException re) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.GroupMembershipService_EXCEPTION_CAUGHT_WHILE_SHUTTING_DOWN), re);
      }
    }
  }

  /** generate XML for the cache before shutting down due to forced disconnect */
  private void saveCacheXmlForReconnect() {
    // there are two versions of this method so it can be unit-tested
    boolean sharedConfigEnabled =
        services.getConfig().getDistributionConfig().getUseSharedConfiguration();
    saveCacheXmlForReconnect(sharedConfigEnabled);
  }

  /** generate XML from the cache before shutting down due to forced disconnect */
  public void saveCacheXmlForReconnect(boolean sharedConfigEnabled) {
    // first save the current cache description so reconnect can rebuild the cache
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      if (!Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "autoReconnect-useCacheXMLFile")
          && !sharedConfigEnabled) {
        try {
          logger.info("generating XML to rebuild the cache after reconnect completes");
          StringPrintWriter pw = new StringPrintWriter();
          CacheXmlGenerator.generate((Cache) cache, pw, true, false);
          String cacheXML = pw.toString();
          cache.getCacheConfig().setCacheXMLDescription(cacheXML);
          logger.info("XML generation completed: {}", cacheXML);
        } catch (CancelException e) {
          logger.info(LocalizedMessage
              .create(LocalizedStrings.GroupMembershipService_PROBLEM_GENERATING_CACHE_XML), e);
        }
      } else if (sharedConfigEnabled && !cache.getCacheServers().isEmpty()) {
        // we need to retain a cache-server description if this JVM was started by gfsh
        List<CacheServerCreation> list = new ArrayList<>(cache.getCacheServers().size());
        for (final Object o : cache.getCacheServers()) {
          CacheServerImpl cs = (CacheServerImpl) o;
          if (cs.isDefaultServer()) {
            CacheServerCreation bsc = new CacheServerCreation(cache, cs);
            list.add(bsc);
          }
        }
        cache.getCacheConfig().setCacheServerCreation(list);
        logger.info("CacheServer configuration saved");
      }
    }
  }

  public boolean requestMemberRemoval(DistributedMember mbr, String reason) {
    if (mbr.equals(this.address)) {
      return false;
    }
    logger.warn(LocalizedMessage.create(
        LocalizedStrings.GroupMembershipService_MEMBERSHIP_REQUESTING_REMOVAL_OF_0_REASON_1,
        new Object[] {mbr, reason}));
    try {
      services.getJoinLeave().remove((InternalDistributedMember) mbr, reason);
    } catch (RuntimeException e) {
      Throwable problem = e;
      if (services.getShutdownCause() != null) {
        Throwable cause = services.getShutdownCause();
        // If ForcedDisconnectException occurred then report it as actual
        // problem.
        if (cause instanceof ForcedDisconnectException) {
          problem = cause;
        } else {
          Throwable ne = problem;
          while (ne.getCause() != null) {
            ne = ne.getCause();
          }
          try {
            ne.initCause(services.getShutdownCause());
          } catch (IllegalArgumentException selfCausation) {
            // fix for bug 38895 - the cause is already in place
          }
        }
      }
      if (!services.getConfig().getDistributionConfig().getDisableAutoReconnect()) {
        saveCacheXmlForReconnect();
      }
      listener.membershipFailure("Channel closed", problem);
      throw new DistributedSystemDisconnectedException("Channel closed", problem);
    }
    return true;
  }

  public void suspectMembers(Set members, String reason) {
    for (final Object member : members) {
      verifyMember((DistributedMember) member, reason);
    }
  }

  public void suspectMember(DistributedMember mbr, String reason) {
    if (!this.shutdownInProgress && !this.shutdownMembers.containsKey(mbr)) {
      verifyMember(mbr, reason);
    }
  }

  /*
   * like memberExists() this checks to see if the given ID is in the current membership view. If it
   * is in the view though we try to contact it to see if it's still around. If we can't contact it
   * then suspect messages are sent to initiate final checks
   *
   * @param mbr the member to verify
   *
   * @param reason why the check is being done (must not be blank/null)
   *
   * @return true if the member checks out
   */
  public boolean verifyMember(DistributedMember mbr, String reason) {
    return mbr != null && memberExists(mbr)
        && this.services.getHealthMonitor().checkIfAvailable(mbr, reason, true);
  }

  /**
   * Perform the grossness associated with sending a message over a DirectChannel
   *
   * @param destinations the list of destinations
   * @param content the message
   * @param theStats the statistics object to update
   * @return all recipients who did not receive the message (null if all received it)
   * @throws NotSerializableException if the message is not serializable
   */
  protected Set<InternalDistributedMember> directChannelSend(
      InternalDistributedMember[] destinations, DistributionMessage content, DMStats theStats)
      throws NotSerializableException {
    boolean allDestinations;
    InternalDistributedMember[] keys;
    if (content.forAll()) {
      allDestinations = true;
      latestViewReadLock.lock();
      try {
        List<InternalDistributedMember> keySet = latestView.getMembers();
        keys = new InternalDistributedMember[keySet.size()];
        keys = keySet.toArray(keys);
      } finally {
        latestViewReadLock.unlock();
      }
    } else {
      allDestinations = false;
      keys = destinations;
    }

    int sentBytes;
    try {
      sentBytes = directChannel.send(this, keys, content,
          this.services.getConfig().getDistributionConfig().getAckWaitThreshold(),
          this.services.getConfig().getDistributionConfig().getAckSevereAlertThreshold());

      if (theStats != null) {
        theStats.incSentBytes(sentBytes);
      }

      if (sentBytes == 0) {
        if (services.getCancelCriterion().isCancelInProgress()) {
          throw new DistributedSystemDisconnectedException();
        }
      }
    } catch (DistributedSystemDisconnectedException ex) {
      if (services.getShutdownCause() != null) {
        throw new DistributedSystemDisconnectedException("DistributedSystem is shutting down",
            services.getShutdownCause());
      } else {
        throw ex; // see bug 41416
      }
    } catch (ConnectExceptions ex) {
      // Check if the connect exception is due to system shutting down.
      if (shutdownInProgress()) {
        if (services.getShutdownCause() != null) {
          throw new DistributedSystemDisconnectedException("DistributedSystem is shutting down",
              services.getShutdownCause());
        } else {
          throw new DistributedSystemDisconnectedException();
        }
      }

      if (allDestinations)
        return null;

      // We need to return this list of failures
      List<InternalDistributedMember> members = (List<InternalDistributedMember>) ex.getMembers();

      // SANITY CHECK: If we fail to send a message to an existing member
      // of the view, we have a serious error (bug36202).

      // grab a recent view, excluding shunned members
      NetView view = services.getJoinLeave().getView();

      // Iterate through members and causes in tandem :-(
      Iterator it_mem = members.iterator();
      Iterator it_causes = ex.getCauses().iterator();
      while (it_mem.hasNext()) {
        InternalDistributedMember member = (InternalDistributedMember) it_mem.next();
        Throwable th = (Throwable) it_causes.next();

        if (!view.contains(member) || (th instanceof ShunnedMemberException)) {
          continue;
        }
        logger.fatal(LocalizedMessage.create(
            LocalizedStrings.GroupMembershipService_FAILED_TO_SEND_MESSAGE_0_TO_MEMBER_1_VIEW_2,
            new Object[] {content, member, view}), th);
        // Assert.assertTrue(false, "messaging contract failure");
      }
      return new HashSet<>(members);
    } // catch ConnectionExceptions
    catch (ToDataException | CancelException e) {
      throw e;
    } catch (IOException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Membership: directChannelSend caught exception: {}", e.getMessage(), e);
      }
      if (e instanceof NotSerializableException) {
        throw (NotSerializableException) e;
      }
    } catch (RuntimeException | Error e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Membership: directChannelSend caught exception: {}", e.getMessage(), e);
      }
      throw e;
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.distributed.internal.membership.MembershipManager#isConnected()
   */
  public boolean isConnected() {
    return (this.hasJoined && !this.shutdownInProgress);
  }

  /**
   * Returns true if the distributed system is in the process of auto-reconnecting. Otherwise
   * returns false.
   */
  public boolean isReconnectingDS() {
    return !this.hasJoined && this.wasReconnectingSystem;
  }

  @Override
  public QuorumChecker getQuorumChecker() {
    if (!(services.isShutdownDueToForcedDisconnect())) {
      return null;
    }
    if (this.quorumChecker != null) {
      return this.quorumChecker;
    }

    QuorumChecker impl = services.getMessenger().getQuorumChecker();
    this.quorumChecker = impl;
    return impl;
  }

  @Override
  public void releaseQuorumChecker(QuorumChecker checker) {
    checker.suspend();
    InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    if (system == null || !system.isConnected()) {
      checker.close();
    }
  }

  public Set<InternalDistributedMember> send(InternalDistributedMember[] destinations,
      DistributionMessage msg, DMStats theStats) throws NotSerializableException {
    Set<InternalDistributedMember> result;
    boolean allDestinations = msg.forAll();

    if (services.getCancelCriterion().isCancelInProgress()) {
      throw new DistributedSystemDisconnectedException("Distributed System is shutting down",
          services.getCancelCriterion().generateCancelledException(null));
    }

    if (playingDead) { // wellness test hook
      while (playingDead && !shutdownInProgress) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    if (isJoining()) {
      // If we get here, we are starting up, so just report a failure.
      if (allDestinations)
        return null;
      else {
        result = new HashSet<>();
        Collections.addAll(result, destinations);
        return result;
      }
    }

    if (msg instanceof AdminMessageType && this.shutdownInProgress) {
      // no admin messages while shutting down - this can cause threads to hang
      return new HashSet<>(Arrays.asList(msg.getRecipients()));
    }

    // Handle trivial cases
    if (destinations == null) {
      if (logger.isTraceEnabled())
        logger.trace("Membership: Message send: returning early because null set passed in: '{}'",
            msg);
      return null; // trivially: all recipients received the message
    }
    if (destinations.length == 0) {
      if (logger.isTraceEnabled())
        logger.trace(
            "Membership: Message send: returning early because empty destination list passed in: '{}'",
            msg);
      return null; // trivially: all recipients received the message
    }

    msg.setSender(address);

    msg.setBreadcrumbsInSender();
    Breadcrumbs.setProblem(null);

    boolean useMcast = false;
    if (mcastEnabled) {
      useMcast = (msg.getMulticast() || allDestinations);
    }

    boolean sendViaMessenger = isForceUDPCommunications() || (msg instanceof ShutdownMessage);

    if (useMcast || tcpDisabled || sendViaMessenger) {
      checkAddressesForUUIDs(destinations);
      result = services.getMessenger().send(msg);
    } else {
      result = directChannelSend(destinations, msg, theStats);
    }

    // If the message was a broadcast, don't enumerate failures.
    if (allDestinations)
      return null;
    else {
      return result;
    }
  }

  void checkAddressesForUUIDs(InternalDistributedMember[] addresses) {
    for (int i = 0; i < addresses.length; i++) {
      InternalDistributedMember m = addresses[i];
      if (m != null) {
        GMSMember id = (GMSMember) m.getNetMember();
        if (!id.hasUUID()) {
          latestViewReadLock.lock();
          try {
            addresses[i] = latestView.getCanonicalID(addresses[i]);
          } finally {
            latestViewReadLock.unlock();
          }
        }
      }
    }
  }

  // MembershipManager method
  @Override
  public void forceUDPMessagingForCurrentThread() {
    forceUseUDPMessaging.set(Boolean.TRUE);
  }

  // MembershipManager method
  @Override
  public void releaseUDPMessagingForCurrentThread() {
    forceUseUDPMessaging.set(Boolean.FALSE);
  }

  private boolean isForceUDPCommunications() {
    return forceUseUDPMessaging.get();
  }

  public void setShutdown() {
    latestViewWriteLock.lock();
    shutdownInProgress = true;
    latestViewWriteLock.unlock();
  }

  @Override
  public boolean shutdownInProgress() {
    // Impossible condition (bug36329): make sure that we check DM's
    // view of shutdown here
    ClusterDistributionManager dm = listener.getDM();
    return shutdownInProgress || (dm != null && dm.shutdownInProgress());
  }


  /**
   * Clean up and create consistent new view with member removed. No uplevel events are generated.
   *
   * Must be called with the {@link #latestViewLock} held.
   */
  private void destroyMember(final InternalDistributedMember member, final String reason) {

    // Make sure it is removed from the view
    latestViewWriteLock.lock();
    try {
      if (latestView.contains(member)) {
        NetView newView = new NetView(latestView, latestView.getViewId());
        newView.remove(member);
        latestView = newView;
      }
    } finally {
      latestViewWriteLock.unlock();
    }

    surpriseMembers.remove(member);

    // Trickiness: there is a minor recursion
    // with addShunnedMembers, since it will
    // attempt to destroy really really old members. Performing the check
    // here breaks the recursion.
    if (!isShunned(member)) {
      addShunnedMember(member);
    }

    final DirectChannel dc = directChannel;
    if (dc != null) {
      // if (crashed) {
      // dc.closeEndpoint(member, reason);
      // }
      // else
      // Bug 37944: make sure this is always done in a separate thread,
      // so that shutdown conditions don't wedge the view lock
      { // fix for bug 34010
        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              Thread.sleep(Integer.getInteger("p2p.disconnectDelay", 3000).intValue());
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              // Keep going, try to close the endpoint.
            }
            if (!dc.isOpen()) {
              return;
            }
            if (logger.isDebugEnabled())
              logger.debug("Membership: closing connections for departed member {}", member);
            // close connections, but don't do membership notification since it's already been done
            dc.closeEndpoint(member, reason, false);
          }
        };
        t.setDaemon(true);
        t.setName("disconnect thread for " + member);
        t.start();
      } // fix for bug 34010
    }
  }

  /**
   * Indicate whether the given member is in the zombie list (dead or dying)
   *
   * @param m the member in question
   *
   *        This also checks the time the given member was shunned, and has the side effect of
   *        removing the member from the list if it was shunned too far in the past.
   *
   *        Concurrency: protected by {@link #latestViewLock} ReentrantReadWriteLock
   *
   * @return true if the given member is a zombie
   */
  public boolean isShunned(DistributedMember m) {
    if (!shunnedMembers.containsKey(m)) {
      return false;
    }

    latestViewWriteLock.lock();
    try {
      // Make sure that the entry isn't stale...
      long shunTime = shunnedMembers.get(m).longValue();
      long now = System.currentTimeMillis();
      if (shunTime + SHUNNED_SUNSET * 1000 > now) {
        return true;
      }

      // Oh, it _is_ stale. Remove it while we're here.
      endShun(m);
      return false;
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  private boolean isShunnedOrNew(final InternalDistributedMember m) {
    latestViewReadLock.lock();
    try {
      return shunnedMembers.containsKey(m) || isNew(m);
    } finally { // synchronized
      latestViewReadLock.unlock();
    }
  }

  // must be invoked under view read or write lock
  private boolean isNew(final InternalDistributedMember m) {
    return !latestView.contains(m) && !surpriseMembers.containsKey(m);
  }

  /**
   * Indicate whether the given member is in the surprise member list
   * <P>
   * Unlike isShunned, this method will not cause expiry of a surprise member. That must be done
   * during view processing.
   * <p>
   * Like isShunned, this method holds the view lock while executing
   *
   * Concurrency: protected by {@link #latestViewLock} ReentrantReadWriteLock
   *
   * @param m the member in question
   * @return true if the given member is a surprise member
   */
  public boolean isSurpriseMember(DistributedMember m) {
    latestViewReadLock.lock();
    try {
      if (surpriseMembers.containsKey(m)) {
        long birthTime = surpriseMembers.get(m).longValue();
        long now = System.currentTimeMillis();
        return (birthTime >= (now - this.surpriseMemberTimeout));
      }
      return false;
    } finally {
      latestViewReadLock.unlock();
    }
  }

  /**
   * for testing we need to be able to inject surprise members into the view to ensure that
   * sunsetting works properly
   *
   * @param m the member ID to add
   * @param birthTime the millisecond clock time that the member was first seen
   */
  public void addSurpriseMemberForTesting(DistributedMember m, long birthTime) {
    if (logger.isDebugEnabled()) {
      logger.debug("test hook is adding surprise member {} birthTime={}", m, birthTime);
    }
    latestViewWriteLock.lock();
    try {
      surpriseMembers.put((InternalDistributedMember) m, Long.valueOf(birthTime));
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  /**
   * returns the surpriseMemberTimeout interval, in milliseconds
   */
  public int getSurpriseMemberTimeout() {
    return this.surpriseMemberTimeout;
  }

  private boolean endShun(DistributedMember m) {
    boolean wasShunned = (shunnedMembers.remove(m) != null);
    shunnedAndWarnedMembers.remove(m);
    return wasShunned;
  }

  /**
   * Add the given member to the shunned list. Also, purge any shunned members that are really
   * really old.
   * <p>
   * Must be called with {@link #latestViewLock} held and the view stable.
   *
   * @param m the member to add
   */
  private void addShunnedMember(InternalDistributedMember m) {
    long deathTime = System.currentTimeMillis() - SHUNNED_SUNSET * 1000;

    surpriseMembers.remove(m); // for safety

    // Update the shunned set.
    if (!isShunned(m)) {
      shunnedMembers.put(m, Long.valueOf(System.currentTimeMillis()));
    }

    // Remove really really old shunned members.
    // First, make a copy of the old set. New arrivals _a priori_ don't matter,
    // and we're going to be updating the list so we don't want to disturb
    // the iterator.
    Set<Map.Entry<DistributedMember, Long>> oldMembers = new HashSet<>(shunnedMembers.entrySet());

    Set<DistributedMember> removedMembers = new HashSet<>();

    for (final Object oldMember : oldMembers) {
      Entry e = (Entry) oldMember;

      // Key is the member. Value is the time to remove it.
      long ll = ((Long) e.getValue()).longValue();
      if (ll >= deathTime) {
        continue; // too new.
      }

      InternalDistributedMember mm = (InternalDistributedMember) e.getKey();

      if (latestView.contains(mm)) {
        // Fault tolerance: a shunned member can conceivably linger but never
        // disconnect.
        //
        // We may not delete it at the time that we shun it because the view
        // isn't necessarily stable. (Note that a well-behaved cache member
        // will depart on its own accord, but we force the issue here.)
        destroyMember(mm, "shunned but never disconnected");
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Membership: finally removed shunned member entry <{}>", mm);
      }

      removedMembers.add(mm);
    }

    // removed timed-out entries from the shunned-members collections
    if (removedMembers.size() > 0) {
      for (final Object removedMember : removedMembers) {
        InternalDistributedMember idm = (InternalDistributedMember) removedMember;
        endShun(idm);
      }
    }
  }


  /**
   * for testing verification purposes, this return the port for the direct channel, or zero if
   * there is no direct channel
   */
  public int getDirectChannelPort() {
    return directChannel == null ? 0 : directChannel.getPort();
  }

  /**
   * for mock testing this allows insertion of a DirectChannel mock
   */
  protected void setDirectChannel(DirectChannel dc) {
    this.directChannel = dc;
    this.tcpDisabled = false;
  }

  /*
   * non-thread-owned serial channels and high priority channels are not included
   */
  public Map getMessageState(DistributedMember member, boolean includeMulticast) {
    Map result = new HashMap();
    DirectChannel dc = directChannel;
    if (dc != null) {
      dc.getChannelStates(member, result);
    }
    services.getMessenger().getMessageState((InternalDistributedMember) member, result,
        includeMulticast);
    return result;
  }

  public void waitForMessageState(DistributedMember otherMember, Map state)
      throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    DirectChannel dc = directChannel;
    if (dc != null) {
      dc.waitForChannelState(otherMember, state);
    }
    services.getMessenger().waitForMessageState((InternalDistributedMember) otherMember, state);

    if (services.getConfig().getTransport().isMcastEnabled()
        && !services.getConfig().getDistributionConfig().getDisableTcp()) {
      // GEODE-2865: wait for scheduled multicast messages to be applied to the cache
      waitForSerialMessageProcessing((InternalDistributedMember) otherMember);
    }
  }

  /*
   * (non-Javadoc) MembershipManager method: wait for the given member to be gone. Throws
   * TimeoutException if the wait goes too long
   *
   * @see
   * org.apache.geode.distributed.internal.membership.MembershipManager#waitForDeparture(org.apache.
   * geode.distributed.DistributedMember)
   */
  public boolean waitForDeparture(DistributedMember mbr)
      throws TimeoutException, InterruptedException {
    int memberTimeout = this.services.getConfig().getDistributionConfig().getMemberTimeout();
    return waitForDeparture(mbr, memberTimeout * 4);
  }

  /*
   * (non-Javadoc) MembershipManager method: wait for the given member to be gone. Throws
   * TimeoutException if the wait goes too long
   *
   * @see
   * org.apache.geode.distributed.internal.membership.MembershipManager#waitForDeparture(org.apache.
   * geode.distributed.DistributedMember)
   */
  public boolean waitForDeparture(DistributedMember mbr, int timeoutMs)
      throws TimeoutException, InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    boolean result = false;
    DirectChannel dc = directChannel;
    InternalDistributedMember idm = (InternalDistributedMember) mbr;
    long pauseTime = (timeoutMs < 4000) ? 100 : timeoutMs / 40;
    boolean wait;
    int numWaits = 0;
    do {
      wait = false;
      if (dc != null) {
        if (dc.hasReceiversFor(idm)) {
          wait = true;
        }
        if (wait && logger.isDebugEnabled()) {
          logger.info("waiting for receivers for {} to shut down", mbr);
        }
      }
      if (!wait) {
        latestViewReadLock.lock();
        try {
          wait = this.latestView.contains(idm);
        } finally {
          latestViewReadLock.unlock();
        }
        if (wait && logger.isDebugEnabled()) {
          logger.debug("waiting for {} to leave the membership view", mbr);
        }
      }
      if (!wait) {
        if (waitForSerialMessageProcessing(idm)) {
          result = true;
        }
      }
      if (wait) {
        numWaits++;
        if (numWaits > 40) {
          throw new TimeoutException("waited too long for " + idm + " to be removed");
        }
        Thread.sleep(pauseTime);
      }
    } while (wait && (dc != null && dc.isOpen())
        && !services.getCancelCriterion().isCancelInProgress());
    if (logger.isDebugEnabled()) {
      logger.debug("operations for {} should all be in the cache at this point", mbr);
    }
    return result;
  }

  /**
   * wait for serial executor messages from the given member to be processed
   */
  public boolean waitForSerialMessageProcessing(InternalDistributedMember idm)
      throws InterruptedException {
    // run a message through the member's serial execution queue to ensure that all of its
    // current messages have been processed
    boolean result = false;
    OverflowQueueWithDMStats serialQueue = listener.getDM().getSerialQueue(idm);
    if (serialQueue != null) {
      final boolean done[] = new boolean[1];
      final FlushingMessage msg = new FlushingMessage(done);
      serialQueue.add(new SizeableRunnable(100) {
        public void run() {
          msg.invoke();
        }

        public String toString() {
          return "Processing fake message";
        }
      });
      synchronized (done) {
        while (!done[0]) {
          done.wait(10);
        }
        result = true;
      }
    }
    return result;
  }


  // TODO GEODE-1752 rewrite this to get rid of the latches, which are currently a memory leak
  public boolean waitForNewMember(InternalDistributedMember remoteId) {
    boolean foundRemoteId = false;
    CountDownLatch currentLatch = null;
    // ARB: preconditions
    // remoteId != null
    latestViewWriteLock.lock();
    try {
      if (latestView == null) {
        // Not sure how this would happen, but see bug 38460.
        // No view?? Not found!
      } else if (latestView.contains(remoteId)) {
        // ARB: check if remoteId is already in membership view.
        // If not, then create a latch if needed and wait for the latch to open.
        foundRemoteId = true;
      } else if ((currentLatch = this.memberLatch.get(remoteId)) == null) {
        currentLatch = new CountDownLatch(1);
        this.memberLatch.put(remoteId, currentLatch);
      }
    } finally {
      latestViewWriteLock.unlock();
    }

    if (!foundRemoteId) {
      try {
        if (currentLatch.await(membershipCheckTimeout, TimeUnit.MILLISECONDS)) {
          foundRemoteId = true;
          // @todo
          // ARB: remove latch from memberLatch map if this is the last thread waiting on latch.
        }
      } catch (InterruptedException ex) {
        // ARB: latch attempt was interrupted.
        Thread.currentThread().interrupt();
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.GroupMembershipService_THE_MEMBERSHIP_CHECK_WAS_TERMINATED_WITH_AN_EXCEPTION));
      }
    }

    // ARB: postconditions
    // (foundRemoteId == true) ==> (currentLatch is non-null ==> currentLatch is open)
    return foundRemoteId;
  }

  /* returns the cause of shutdown, if known */
  public Throwable getShutdownCause() {
    return services.getShutdownCause();
  }

  public void registerTestHook(MembershipTestHook mth) {
    // lock for additions to avoid races during startup
    latestViewWriteLock.lock();
    try {
      if (this.membershipTestHooks == null) {
        this.membershipTestHooks = Collections.singletonList(mth);
      } else {
        List l = new ArrayList(this.membershipTestHooks);
        l.add(mth);
        this.membershipTestHooks = l;
      }
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  public void unregisterTestHook(MembershipTestHook mth) {
    latestViewWriteLock.lock();
    try {
      if (this.membershipTestHooks != null) {
        if (this.membershipTestHooks.size() == 1) {
          this.membershipTestHooks = null;
        } else {
          List l = new ArrayList(this.membershipTestHooks);
          l.remove(mth);
        }
      }
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  private boolean beingSick;
  private boolean playingDead;

  /**
   * Test hook - be a sick member
   */
  @Override
  public synchronized void beSick() {
    if (!beingSick) {
      beingSick = true;
      logger.info("GroupMembershipService.beSick invoked for {} - simulating sickness",
          this.address);
      services.getJoinLeave().beSick();
      services.getHealthMonitor().beSick();
    }
  }

  /**
   * Test hook - don't answer "are you alive" requests
   */
  @Override
  public synchronized void playDead() {
    if (!playingDead) {
      playingDead = true;
      logger.info("GroupMembershipService.playDead invoked for {}", this.address);
      services.getJoinLeave().playDead();
      services.getHealthMonitor().playDead();
      services.getMessenger().playDead();
    }
  }

  /**
   * Test hook - recover health
   */
  @Override
  public synchronized void beHealthy() {
    if (beingSick || playingDead) {
      synchronized (startupMutex) {
        beingSick = false;
        playingDead = false;
        startEventProcessing();
      }
      logger.info("GroupMembershipService.beHealthy invoked for {} - recovering health now",
          this.address);
      services.getJoinLeave().beHealthy();
      services.getHealthMonitor().beHealthy();
      services.getMessenger().beHealthy();
    }
  }

  /**
   * Test hook
   */
  public boolean isBeingSick() {
    return this.beingSick;
  }

  /**
   * Test hook - inhibit ForcedDisconnectException logging to keep dunit logs clean
   */
  public static void inhibitForcedDisconnectLogging(boolean b) {
    inhibitForceDisconnectLogging = b;
  }

  /** this is a fake message class that is used to flush the serial execution queue */
  static class FlushingMessage extends DistributionMessage {
    final boolean[] done;

    FlushingMessage(boolean[] done) {
      this.done = done;
    }

    public void invoke() {
      synchronized (done) {
        done[0] = true;
        done.notify();
      }
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      // not used
    }

    @Override
    public int getDSFID() {
      return 0;
    }

    @Override
    public int getProcessorType() {
      return ClusterDistributionManager.SERIAL_EXECUTOR;
    }
  }

  @Override
  public void stopped() {}

  @Override
  public void installView(NetView v) {
    if (latestViewId < 0 && !isConnected()) {
      if (this.directChannel != null) {
        this.directChannel.setMembershipSize(v.getMembers().size());
      }
      latestViewId = v.getViewId();
      latestView = v;
      logger.debug("MembershipManager: initial view is {}", latestView);
    } else {
      handleOrDeferViewEvent(v);
    }
  }

  @Override
  public Set<InternalDistributedMember> send(DistributionMessage m)
      throws NotSerializableException {
    return send(m.getRecipients(), m, this.services.getStatistics());
  }

  @Override
  public void forceDisconnect(final String reason) {
    if (GMSMembershipManager.this.shutdownInProgress || isJoining()) {
      return; // probably a race condition
    }

    setShutdown();

    final Exception shutdownCause = new ForcedDisconnectException(reason);

    // cache the exception so it can be appended to ShutdownExceptions
    services.setShutdownCause(shutdownCause);
    services.getCancelCriterion().cancel(reason);

    AlertAppender.getInstance().shuttingDown();

    if (!inhibitForceDisconnectLogging) {
      logger.fatal(
          LocalizedMessage
              .create(LocalizedStrings.GroupMembershipService_MEMBERSHIP_SERVICE_FAILURE_0, reason),
          shutdownCause);
    }

    if (!services.getConfig().getDistributionConfig().getDisableAutoReconnect()) {
      saveCacheXmlForReconnect();
    }


    Thread reconnectThread = new Thread(() -> {
      // stop server locators immediately since they may not have correct
      // information. This has caused client failures in bridge/wan
      // network-down testing
      InternalLocator loc = (InternalLocator) Locator.getLocator();
      if (loc != null) {
        loc.stop(true, !services.getConfig().getDistributionConfig().getDisableAutoReconnect(),
            false);
      }

      uncleanShutdown(reason, shutdownCause);
    });

    reconnectThread.setName("DisconnectThread");
    reconnectThread.setDaemon(false);
    reconnectThread.start();
  }


  public void disableDisconnectOnQuorumLossForTesting() {
    services.getJoinLeave().disableDisconnectOnQuorumLossForTesting();
  }


  /**
   * Class <code>BoundedLinkedHashMap</code> is a bounded <code>LinkedHashMap</code>. The bound is
   * the maximum number of entries the <code>BoundedLinkedHashMap</code> can contain.
   */
  static class BoundedLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = -3419897166186852692L;

    /**
     * Constructor.
     *
     */
    public BoundedLinkedHashMap() {
      super();
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
      return size() > 1000;
    }
  }


  @Override
  public boolean isShutdownStarted() {
    ClusterDistributionManager dm = listener.getDM();
    return shutdownInProgress || (dm != null && dm.isShutdownStarted());
  }

  public void disconnect(boolean beforeJoined) {
    if (beforeJoined) {
      uncleanShutdown("Failed to start distribution", null);
    } else {
      shutdown();
    }
  }
}
