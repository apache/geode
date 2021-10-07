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

package org.apache.geode.distributed.internal.membership.gms;

import static org.apache.geode.distributed.internal.membership.api.LifecycleListener.RECONNECTING.NOT_RECONNECTING;
import static org.apache.geode.distributed.internal.membership.api.LifecycleListener.RECONNECTING.RECONNECTING;

import java.io.NotSerializableException;
import java.util.ArrayList;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.membership.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberShunnedException;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipClosedException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.api.MembershipView;
import org.apache.geode.distributed.internal.membership.api.Message;
import org.apache.geode.distributed.internal.membership.api.MessageListener;
import org.apache.geode.distributed.internal.membership.api.QuorumChecker;
import org.apache.geode.distributed.internal.membership.api.StopShunningMarker;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * GMSMembership is the implementation of the Membership interface for Geode
 */
public class GMSMembership<ID extends MemberIdentifier> implements Membership<ID> {
  private static final Logger logger = Services.getLogger();

  /** product version to use for multicast serialization */
  private volatile boolean disableMulticastForRollingUpgrade;

  /**
   * set to true if the distributed system that created this manager was auto-reconnecting when it
   * was created.
   */
  private boolean wasReconnectingSystem;

  /**
   * This indicates that the DistributedSystem using this membership manager performed
   * a successful auto-reconnect. This may include successful recreation of a Cache
   */
  private boolean reconnectCompleted;

  /**
   * A quorum checker is created during reconnect and is held here so it is available to the UDP
   * protocol for passing off the ping-pong responses used in the quorum-checking algorithm.
   */
  private volatile QuorumChecker quorumChecker;

  private final ManagerImpl gmsManager;


  private final LifecycleListener<ID> lifecycleListener;

  private volatile boolean isCloseInProgress;

  private ExecutorService viewExecutor;

  /**
   * Trick class to make the startup synch more visible in stack traces
   *
   * @see GMSMembership#startupLock
   */
  static class EventProcessingLock {
    public EventProcessingLock() {}
  }

  static class StartupEvent<ID extends MemberIdentifier> {
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
    ID member;
    Message<ID> dmsg;
    MembershipView<ID> gmsView;

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
    StartupEvent(final ID member) {
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
    StartupEvent(MembershipView<ID> v) {
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
    StartupEvent(Message<ID> d) {
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
      MembershipConfig.DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT;

  /**
   * This object synchronizes threads waiting for startup to finish. Updates to
   * {@link #startupMessages} are synchronized through this object.
   */
  private final EventProcessingLock startupLock = new EventProcessingLock();

  /**
   * This is the latest view (ordered list of IDs) that has been installed
   *
   * Writing to this object is protected via {@link #latestViewWriteLock}
   */
  private volatile MembershipView<ID> latestView = new MembershipView<>();

  /**
   * This is the lock for protecting access to latestView and modification of data used in
   * {@link #processView} method
   *
   * @see #latestView
   */
  private final ReadWriteLock latestViewLock = new ReentrantReadWriteLock();
  private final Lock latestViewReadLock = latestViewLock.readLock();
  private final Lock latestViewWriteLock = latestViewLock.writeLock();

  /**
   * This is the listener that accepts our membership events
   */
  private final MembershipListener<ID> listener;

  /**
   * This is the listener that accepts our membership messages
   */
  private final MessageListener<ID> messageListener;

  /**
   * This is a representation of the local member (ourself)
   */
  private ID address = null; // new ID(-1);

  volatile boolean isJoining;

  /** have we joined successfully? */
  private volatile boolean hasJoined;

  /**
   * Members that have sent a shutdown message. This is used to suppress suspect processing that
   * otherwise becomes pretty aggressive when a member is shutting down.
   */
  private final Set<ID> shutdownMembers = Collections.newSetFromMap(new BoundedLinkedHashMap<>());

  /**
   * This is the lock for protecting access to shutdownMembers
   *
   * @see #shutdownMembers
   */
  private final ReadWriteLock shutdownMembersLock = new ReentrantReadWriteLock();
  private final Lock shutdownMembersReadLock = shutdownMembersLock.readLock();
  private final Lock shutdownMembersWriteLock = shutdownMembersLock.writeLock();

  /**
   * The identities and birth-times of others that we have allowed into membership at the
   * distributed system level, but have not yet appeared in a view.
   * <p>
   * Keys are instances of {@link ID}, values are Longs indicating the time
   * this member was shunned.
   * <p>
   * Members are removed when a view containing them is processed. If, after
   * {@link #surpriseMemberTimeout} milliseconds have passed, a view containing the member has not
   * arrived, the member is removed from membership and member-left notification is performed.
   * <p>
   * > Writing to this list needs to be under {@link #latestViewWriteLock}
   *
   * @see System#currentTimeMillis()
   */
  private final Map<ID, Long> surpriseMembers = new ConcurrentHashMap<>();

  /**
   * the timeout interval for surprise members. This is calculated from the member-timeout setting
   */
  private long surpriseMemberTimeout;

  /**
   * javagroups can skip views and omit telling us about a crashed member. This map holds a history
   * of suspected members that we use to detect crashes.
   */
  private final Map<ID, Long> suspectedMembers = new ConcurrentHashMap<>();

  /**
   * Set to true when the service should stop.
   */
  private volatile boolean shutdownInProgress = false;

  /**
   * Set to true when upcalls should be generated for events.
   */
  private volatile boolean processingEvents = false;

  /**
   * Set to true under startupLock when processingEvents has been set to true
   * and startup messages have been removed from the queue and dispatched
   */
  private boolean startupMessagesDrained = false;

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
  private final LinkedList<StartupEvent<ID>> startupMessages = new LinkedList<>();

  /**
   * ARB: the map of latches is used to block peer handshakes till authentication is confirmed.
   */
  private final HashMap<ID, CountDownLatch> memberLatch = new HashMap<>();



  /**
   * Analyze a given view object, generate events as appropriate
   */
  public void processView(final MembershipView<ID> newView) {
    // Sanity check...
    if (logger.isDebugEnabled()) {
      StringBuilder msg = new StringBuilder(200);
      msg.append("Membership: Processing view ");
      msg.append(newView);
      msg.append("} on ").append(address.toString());
      logger.debug(msg);
      if (!newView.contains(address)) {
        logger.info("The Member with id {}, is no longer in my own view, {}",
            address, newView);
      }
    }

    // We perform the update under a global lock so that other
    // incoming events will not be lost in terms of our global view.
    latestViewWriteLock.lock();
    try {
      setIsMulticastAllowedFrom(newView, surpriseMembers);
      // Save previous view, for delta analysis
      MembershipView<ID> priorView = latestView;

      if (newView.getViewId() < priorView.getViewId()) {
        // ignore this view since it is old news
        return;
      }

      // update the view to reflect our changes, so that
      // callbacks will see the new (updated) view.
      MembershipView<ID> newlatestView = newView;
      final List<ID> newMembers = new ArrayList<>();

      // look for additions
      for (ID m : newView.getMembers()) { // additions
        // Once a member has been seen via a view, remove them from the
        // newborn set. Replace the member data of the surpriseMember ID
        // in case it was a partial ID and is being retained by DistributionManager
        // or some other object
        boolean wasSurprise = surpriseMembers.containsKey(m);
        if (wasSurprise) {
          for (Iterator<Map.Entry<ID, Long>> iterator =
              surpriseMembers.entrySet().iterator(); iterator.hasNext();) {
            Entry<ID, Long> entry = iterator.next();
            if (entry.getKey().equals(m)) {
              entry.getKey().setMemberData(m.getMemberData());
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
            this.services.getConfig().getSecurityPeerAuthInit();
        boolean isSecure = authInit != null && authInit.length() != 0;

        if (isSecure) {
          CountDownLatch currentLatch;
          if ((currentLatch = memberLatch.get(m)) != null) {
            currentLatch.countDown();
          }
        }

        if (shutdownInProgress()) {
          continue; // no additions processed after shutdown begins
        }

        logger.info("Membership: Processing addition <{}>", m);
        newMembers.add(m);
      } // additions

      // look for departures
      for (ID m : priorView.getMembers()) { // departures
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
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          logger.info(String.format("Membership: Fault while processing view removal of %s",
              m),
              t);
        }
      } // departures

      // expire surprise members, add others to view
      long oldestAllowed = System.currentTimeMillis() - this.surpriseMemberTimeout;
      for (Iterator<Map.Entry<ID, Long>> it =
          surpriseMembers.entrySet().iterator(); it.hasNext();) {
        Map.Entry<ID, Long> entry = it.next();
        Long birthtime = entry.getValue();
        if (birthtime.longValue() < oldestAllowed) {
          it.remove();
          ID m = entry.getKey();
          logger.info("Membership: expiring membership of surprise member <{}>",
              m);
          removeWithViewLock(m, true,
              "not seen in membership view in " + this.surpriseMemberTimeout + "ms");
        } else {
          if (!newlatestView.contains(entry.getKey())) {
            newlatestView = newlatestView.createNewViewWithMember(entry.getKey());
          }
        }
      }
      // expire suspected members
      final long suspectMemberTimeout = 180000;
      oldestAllowed = System.currentTimeMillis() - suspectMemberTimeout;
      for (Iterator<Map.Entry<ID, Long>> it = suspectedMembers.entrySet().iterator(); it
          .hasNext();) {
        Map.Entry<ID, Long> entry = it.next();
        Long birthtime = entry.getValue();
        if (birthtime.longValue() < oldestAllowed) {
          it.remove();
        }
      }

      // the view is complete - let's install it
      latestView = newlatestView;
      for (ID newMember : newMembers) {
        listener.newMemberConnected(newMember);
      }
      listener.viewInstalled(latestView);
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  private void setIsMulticastAllowedFrom(final MembershipView<ID> newView,
      final Map<ID, Long> surpriseMembers) {
    disableMulticastForRollingUpgrade =
        anyMemberHasOlderVersion(
            Stream.concat(surpriseMembers.keySet().stream(), newView.getMembers().stream()));
  }

  private boolean anyMemberHasOlderVersion(final Stream<ID> members) {
    return members
        .anyMatch(member -> KnownVersion.CURRENT.isNewerThan(member.getVersion()));
  }

  @Override
  public <V> V doWithViewLocked(Supplier<V> function) {
    latestViewReadLock.lock();
    try {
      return function.get();
    } finally {
      latestViewReadLock.unlock();
    }
  }

  /**
   * the timer used to perform periodic tasks
   */
  private ScheduledExecutorService cleanupTimer;

  private Services<ID> services;


  /**
   * Joins the distributed system
   *
   * @throws MemberStartupException - configuration error
   */
  private void join() throws MemberStartupException {
    services.setShutdownCause(null);
    services.getCancelCriterion().cancel(null);

    latestViewWriteLock.lock();
    try {
      try {
        this.isJoining = true; // added for bug #44373

        // connect
        services.getJoinLeave().join();

        latestView = createGeodeView(services.getJoinLeave().getView());
        listener.viewInstalled(latestView);
      } finally {
        this.isJoining = false;
      }
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  private MembershipView<ID> createGeodeView(GMSMembershipView<ID> view) {
    return new MembershipView<>(view.getCreator(), view.getViewId(), view.getMembers(),
        view.getShutdownMembers(),
        view.getCrashedMembers());
  }

  private Set<ID> gmsMemberCollectionToIDSet(
      Collection<ID> gmsMembers) {
    if (gmsMembers.size() == 0) {
      return Collections.emptySet();
    } else if (gmsMembers.size() == 1) {
      return Collections.singleton(
          gmsMembers.iterator().next());
    } else {
      Set<ID> idmMembers = new HashSet<>(gmsMembers.size());
      for (ID member : gmsMembers) {
        idmMembers.add(member);
      }
      return idmMembers;
    }
  }


  private List<ID> gmsMemberListToIDList(
      List<ID> gmsMembers) {
    if (gmsMembers.size() == 0) {
      return Collections.emptyList();
    } else if (gmsMembers.size() == 1) {
      return Collections
          .singletonList(gmsMembers.get(0));
    } else {
      List<ID> idmMembers = new ArrayList<>(gmsMembers.size());
      for (ID member : gmsMembers) {
        idmMembers.add(member);
      }
      return idmMembers;
    }
  }



  public GMSMembership(MembershipListener<ID> listener, MessageListener<ID> messageListener,
      LifecycleListener<ID> lifecycleListener) {
    this.lifecycleListener = lifecycleListener;
    this.listener = listener;
    this.messageListener = messageListener;
    this.gmsManager = new ManagerImpl();
    this.viewExecutor = LoggingExecutors.newSingleThreadExecutor("Geode View Processor", true);
  }

  public Manager<ID> getGMSManager() {
    return this.gmsManager;
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
   * Remove a member. {@link #latestViewWriteLock} must be held before this method is called. If
   * member is not already shunned, the uplevel event handler is invoked.
   */
  private void removeWithViewLock(ID dm, boolean crashed, String reason) {
    final boolean wasShunned = isShunned(dm);

    // Delete resources
    destroyMember(dm, reason);

    if (wasShunned) {
      return; // Explicit deletion, no upcall.
    }

    if (!isMemberShuttingDown(dm)) {
      // if we've received a shutdown message then DistributionManager will already have
      // notified listeners
      listener.memberDeparted(dm, crashed, reason);
    }
  }

  private boolean isMemberShuttingDown(ID dm) {
    shutdownMembersReadLock.lock();
    try {
      return shutdownMembers.contains(dm);
    } finally {
      shutdownMembersReadLock.unlock();
    }
  }

  /**
   * Process a surprise connect event, or place it on the startup queue.
   *
   * @param member the member
   */
  protected void handleOrDeferSurpriseConnect(ID member) {
    if (!processingEvents) {
      synchronized (startupLock) {
        if (!startupMessagesDrained) {
          startupMessages.add(new StartupEvent<>(member));
          return;
        }
      }
    }
    processSurpriseConnect(member);
  }

  @Override
  public void startupMessageFailed(ID mbr, String failureMessage) {
    // fix for bug #40666
    listener.memberDeparted(mbr, true,
        "failed to pass startup checks");
  }


  /**
   * Logic for handling a direct connection event (message received from a member not in the view).
   * Does not employ the startup queue.
   * <p>
   * Waits until there is a stable view. If the member has already been added, simply returns;
   * else adds the member.
   *
   * @param dm the member joining
   */
  @Override
  public boolean addSurpriseMember(ID dm) {
    final ID member = dm;
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
        new LoggingThread("Removing shunned GemFire node " + member, false, () -> {
          // fix for bug #42548
          // this is an old member that shouldn't be added
          logger.warn("attempt to add old member: {} as surprise member to {}",
              member, latestView);
          try {
            requestMemberRemoval(member,
                "this member is no longer in the view but is initiating connections");
          } catch (MembershipClosedException | MemberDisconnectedException e) {
            // okay to ignore
          }
        }).start();
        return false;
      }

      // Adding the member to this set ensures we won't remove it if a new
      // view comes in and it is still not visible.
      surpriseMembers.put(member, Long.valueOf(System.currentTimeMillis()));

      if (shutdownInProgress()) {
        // Force disconnect, esp. the TCPConduit
        String msg =
            "This distributed system is shutting down.";

        destroyMember(member, msg);
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
        // should ensure it is not chosen as an elder.
        // This will get corrected when the member finally shows up in the
        // view.
        latestView = latestView.createNewViewWithMember(member);
      }
    } finally {
      latestViewWriteLock.unlock();
    }
    if (warn) { // fix for bug #41538 - deadlock while alerting
      logger.warn("Membership: Ignoring surprise connect from shunned member <{}>",
          member);
    } else {
      listener.newMemberConnected(member);
    }
    return !warn;
  }


  /** starts periodic task to perform cleanup chores such as expire surprise members */
  private void startCleanupTimer() {
    this.cleanupTimer =
        LoggingExecutors.newScheduledThreadPool(1, "GMSMembership.cleanupTimer", false);

    this.cleanupTimer.scheduleAtFixedRate(this::cleanUpSurpriseMembers, surpriseMemberTimeout,
        surpriseMemberTimeout / 3, TimeUnit.MILLISECONDS);
  }

  // invoked from the cleanupTimer task
  private void cleanUpSurpriseMembers() {
    latestViewWriteLock.lock();
    try {
      long oldestAllowed = System.currentTimeMillis() - surpriseMemberTimeout;
      for (Iterator<Map.Entry<ID, Long>> it = surpriseMembers.entrySet().iterator(); it
          .hasNext();) {
        Map.Entry<ID, Long> entry = it.next();
        Long birthtime = entry.getValue();
        if (birthtime.longValue() < oldestAllowed) {
          it.remove();
          ID m = entry.getKey();
          logger.info("Membership: expiring membership of surprise member <{}>",
              m);
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
  protected void handleOrDeferMessage(Message<ID> msg) throws MemberShunnedException {
    if (msg.dropMessageWhenMembershipIsPlayingDead() && (beingSick || playingDead)) {
      return;
    }
    if (!processingEvents) {
      synchronized (startupLock) {
        if (!startupMessagesDrained) {
          startupMessages.add(new StartupEvent<>(msg));
          return;
        }
      }
    }
    dispatchMessage(msg);
  }

  /**
   * Logic for processing a distribution message.
   * <p>
   * It is possible to receive messages not consistent with our view. We handle this here, and
   * generate an uplevel event if necessary
   *
   * @param msg the message
   */
  protected void dispatchMessage(Message<ID> msg) throws MemberShunnedException {
    ID m = msg.getSender();
    boolean shunned = false;

    // If this member is shunned or new, grab the latestViewWriteLock: update the appropriate data
    // structure.
    if (isShunnedOrNew(m)) {
      if (isShunned(m)) {
        if (!(msg instanceof StopShunningMarker)) {
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
    }

    if (shunned) { // bug #41538 - shun notification must be outside synchronization to avoid
      // hanging
      logger.warn("Membership: disregarding shunned member <{}>", m);
      if (logger.isTraceEnabled()) {
        logger.trace("Membership: Ignoring message from shunned member <{}>:{}", m, msg);
      }
      throw new MemberShunnedException();
    }

    messageListener.messageReceived(msg);
  }

  /**
   * If the message's sender ID is a partial ID, which can happen if it's received in a JGroups
   * message, replace it with an ID from the current membership view.
   *
   * @param msg the message holding the sender ID
   */
  public void replacePartialIdentifierInMessage(Message<ID> msg) {
    ID sender = msg.getSender();
    ID oldID = sender;
    ID newID = this.services.getJoinLeave().getMemberID(oldID);
    if (newID != null && newID != oldID) {
      sender.setMemberData(newID.getMemberData());
      sender.setIsPartial(false);
    } else {
      MembershipView currentView = latestView;
      if (currentView != null) {
        sender = (ID) currentView.getCanonicalID(sender);
      }
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
  protected void handleOrDeferViewEvent(MembershipView<ID> viewArg) {
    if (this.isJoining) {
      // bug #44373 - queue all view messages while joining.
      // This is done under the latestViewLock, but we can't block here because
      // we're sitting in the UDP reader thread.
      synchronized (startupLock) {
        startupMessages.add(new StartupEvent<>(viewArg));
        return;
      }
    }
    if (!processingEvents) {
      synchronized (startupLock) {
        if (!startupMessagesDrained) {
          startupMessages.add(new StartupEvent<>(viewArg));
          return;
        }
      }
    }

    viewExecutor.submit(() -> processView(viewArg));
  }

  private ID gmsMemberToDMember(ID gmsMember) {
    return gmsMember;
  }

  /**
   * Process a new view object, or place on the startup queue
   *
   * @param suspectInfo the suspectee and suspector
   */
  protected void handleOrDeferSuspect(SuspectMember<ID> suspectInfo) {
    if (!processingEvents) {
      return;
    }
    ID suspect = gmsMemberToDMember(suspectInfo.suspectedMember);
    ID who = gmsMemberToDMember(suspectInfo.whoSuspected);
    this.suspectedMembers.put(suspect, System.currentTimeMillis());
    listener.memberSuspect(suspect, who, suspectInfo.reason);
  }

  /**
   * Process a potential direct connect. Does not use the startup queue.
   * <p>
   * It is a <em>potential</em> event, because we don't know until we've grabbed a stable view if
   * this is really a new member.
   */
  private void processSurpriseConnect(ID member) {
    addSurpriseMember(member);
  }

  /**
   * Dispatch routine for processing a single startup event
   *
   * @param o the startup event to handle
   */
  private void processStartupEvent(StartupEvent<ID> o) {
    // Most common events first

    if (o.isDistributionMessage()) { // normal message
      try {
        o.dmsg.setSender(
            latestView.getCanonicalID(o.dmsg.getSender()));
        dispatchMessage(o.dmsg);
      } catch (MemberShunnedException e) {
        // message from non-member - ignore
      }
    } else if (o.isGmsView()) { // view event
      processView(o.gmsView);
    } else if (o.isSurpriseConnect()) { // connect
      processSurpriseConnect(o.member);
    } else {
      throw new IllegalArgumentException("unknown startup event: " + o);
    }
  }

  /**
   * Special mutex to create a critical section for {@link #startEventProcessing()}
   */
  private final Object startupMutex = new Object();


  @Override
  public void startEventProcessing() {
    // Only allow one thread to perform the work
    synchronized (startupMutex) {
      if (logger.isDebugEnabled()) {
        logger.debug("Membership: draining startup events.");
      }
      // Remove the backqueue of messages, but allow
      // additional messages to be added.
      for (;;) {
        StartupEvent<ID> ev;
        // Only grab the mutex while reading the queue.
        // Other events may arrive while we're attempting to
        // drain the queue. This is OK, we'll just keep processing
        // events here until we've caught up.
        synchronized (startupLock) {
          int remaining = startupMessages.size();
          if (remaining == 0) {
            // While holding the lock, flip the bit so that
            // no more events get put into startupMessages
            startupMessagesDrained = true;
            // set the volatile boolean that states that queueing is completely done now
            processingEvents = true;
            // notify any threads waiting for event processing that we're open for business
            startupLock.notifyAll();
            break;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Membership: {} remaining startup message(s)", remaining);
          }
          ev = startupMessages.removeFirst();
        } // startupLock
        try {
          processStartupEvent(ev);
        } catch (VirtualMachineError err) {
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          logger.warn("Membership: Error handling startup event",
              t);
        }
      } // for
      if (logger.isDebugEnabled()) {
        logger.debug("Membership: finished processing startup events.");
      }
    } // startupMutex
  }


  @Override
  public void waitForEventProcessing() throws InterruptedException {
    // First check outside of a synchronized block. Cheaper and sufficient.
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    if (processingEvents) {
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: waiting until the system is ready for events");
    }
    for (;;) {
      services.getCancelCriterion().checkCancelInProgress(null);
      synchronized (startupLock) {
        // Now check using a memory fence and synchronization.
        if (processingEvents && startupMessagesDrained) {
          break;
        }
        boolean interrupted = Thread.interrupted();
        try {
          startupLock.wait();
        } catch (InterruptedException e) {
          interrupted = true;
          services.getCancelCriterion().checkCancelInProgress(e);
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
  public List<StartupEvent<ID>> getStartupEvents() {
    return this.startupMessages;
  }

  /**
   * Returns a copy (possibly not current) of the current view (a list of
   * {@link ID}s)
   */
  @Override
  public MembershipView<ID> getView() {
    return latestView;
  }

  public boolean isJoining() {
    return this.isJoining;
  }

  /**
   * test hook
   *
   * @return the current membership view coordinator
   */
  @Override
  public ID getCoordinator() {
    final MembershipView<ID> view = latestView;
    return view == null ? null : view.getCoordinator();
  }

  @Override
  public boolean memberExists(ID m) {
    return latestView.contains(m);
  }

  /**
   * Returns the identity associated with this member. WARNING: this value will be returned after
   * the channel is closed, but in that case it is good for logging purposes only. :-)
   */
  @Override
  public ID getLocalMember() {
    return address;
  }

  public Services<ID> getServices() {
    return services;
  }

  @Override
  public void processMessage(final Message<ID> msg) throws MemberShunnedException {
    // notify failure detection that we've had contact from a member
    services.getHealthMonitor().contactedBy(msg.getSender());
    handleOrDeferMessage(msg);
  }

  /**
   * inhibits logging of ForcedDisconnectException to keep dunit logs clean while testing this
   * feature
   */
  @MakeNotStatic
  private static volatile boolean inhibitForceDisconnectLogging;

  /**
   * Close the receiver, avoiding all potential deadlocks and eschewing any attempts at being
   * graceful.
   */
  @Override
  public void emergencyClose() {
    setShutdown();

    // We can't call close() because they will allocate objects. Attempt
    // a surgical strike and touch the important protocols.

    // MOST important, kill the FD protocols...
    services.emergencyClose();
  }


  /**
   * in order to avoid split-brain occurring when a member is shutting down due to race conditions
   * in view management we add it as a shutdown member when we receive a shutdown message. This is
   * not the same as a SHUNNED member.
   */
  @Override
  public void shutdownMessageReceived(ID id, String reason) {
    if (logger.isDebugEnabled()) {
      logger.debug("Membership: recording shutdown status of {}", id);
    }
    shutdownMembersWriteLock.lock();
    try {
      this.shutdownMembers.add(id);
      services.getJoinLeave().memberShutdown(id, reason);
    } finally {
      shutdownMembersWriteLock.unlock();
    }
  }

  @Override
  public Set<ID> getMembersNotShuttingDown() {
    shutdownMembersReadLock.lock();
    try {
      return latestView.getMembers().stream().filter(id -> !shutdownMembers.contains(id))
          .collect(
              Collectors.toSet());
    } finally {
      shutdownMembersReadLock.unlock();
    }
  }

  @Override
  public void shutdown() {
    setShutdown();
    services.stop();
    viewExecutor.shutdownNow();
  }

  @Override
  public void uncleanShutdown(String reason, final Exception e) {
    inhibitForcedDisconnectLogging(false);

    try {
      if (services.getShutdownCause() == null) {
        services.setShutdownCause(e);
      }

      if (cleanupTimer != null && !cleanupTimer.isShutdown()) {
        cleanupTimer.shutdownNow();
      }

      lifecycleListener.disconnect(e);

      // first shut down communication so we don't do any more harm to other
      // members
      services.emergencyClose();
    } finally {
      if (e != null) {
        try {
          listener.membershipFailure(reason, e);
        } catch (RuntimeException re) {
          logger.warn("Exception caught while shutting down", re);
        }
      }
    }
  }



  @Override
  public boolean requestMemberRemoval(ID mbr, String reason) throws MemberDisconnectedException {
    if (mbr.equals(this.address)) {
      return false;
    }
    logger.warn("Membership: requesting removal of {}. Reason={}",
        new Object[] {mbr, reason});
    try {
      services.getJoinLeave().remove(mbr, reason);
    } catch (RuntimeException e) {
      RuntimeException problem = e;
      if (services.getShutdownCause() != null) {
        Throwable cause = services.getShutdownCause();
        // If ForcedDisconnectException occurred then report it as actual
        // problem.
        if ((cause instanceof MemberDisconnectedException)) {
          throw (MemberDisconnectedException) cause;
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
      listener.saveConfig();

      listener.membershipFailure("Channel closed", problem);
      throw new MembershipClosedException("Channel closed", problem);
    }
    return true;
  }

  @Override
  public void suspectMembers(Set<ID> members, String reason) {
    for (final ID member : members) {
      verifyMember(member, reason);
    }
  }

  @Override
  public void suspectMember(ID mbr, String reason) {
    if (!this.shutdownInProgress && !isMemberShuttingDown(mbr)) {
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
  @Override
  public boolean verifyMember(ID mbr, String reason) {
    return mbr != null && memberExists(mbr)
        && this.services.getHealthMonitor()
            .checkIfAvailable(mbr, reason, false);
  }

  @Override
  public ID[] getAllMembers(final ID[] arrayType) {
    final List<ID> keySet = latestView.getMembers();
    return keySet.toArray(arrayType);
  }

  @Override
  public boolean hasMember(final ID member) {
    return services.getJoinLeave().getView().contains(member);
  }

  /**
   * @see Membership#isConnected()
   */
  @Override
  public boolean isConnected() {
    return (this.hasJoined && !this.shutdownInProgress);
  }

  @Override
  public QuorumChecker getQuorumChecker() {
    if (!(services.isShutdownDueToForcedDisconnect())) {
      return null;
    }
    if (quorumChecker != null) {
      return quorumChecker;
    }

    quorumChecker = services.getMessenger().getQuorumChecker();
    return quorumChecker;
  }

  /**
   * Check to see if the membership system is being shutdown
   *
   * @throws MembershipClosedException if the system is shutting down
   */
  public void checkCancelled() throws MembershipClosedException {
    if (services.getCancelCriterion().isCancelInProgress()) {
      throw new MembershipClosedException("Distributed System is shutting down",
          services.getCancelCriterion().generateCancelledException(services.getShutdownCause()));
    }
  }

  /**
   * Test hook to wait until we are no longer "Playing dead"
   *
   * TODO - remove this!
   */
  public void waitIfPlayingDead() {
    if (playingDead) { // wellness test hook
      while (playingDead && !shutdownInProgress) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Override
  public Set<ID> send(final ID[] destinations,
      final Message<ID> content)
      throws NotSerializableException {
    checkAddressesForUUIDs(destinations);
    Set<ID> failures = services.getMessenger().send(content);
    if (failures == null || failures.size() == 0) {
      return Collections.emptySet();
    }
    return failures;
  }

  void checkAddressesForUUIDs(ID[] addresses) {
    GMSMembershipView<ID> view = services.getJoinLeave().getView();
    for (int i = 0; i < addresses.length; i++) {
      ID id = addresses[i];
      if (id != null) {
        if (!id.hasUUID()) {
          id.setMemberData(view.getCanonicalID(id).getMemberData());
        }
      }
    }
  }

  /**
   * This method holds the {@link #latestViewWriteLock} to protect
   * {@link #shutdownInProgress} from modification while {@link #processView} is in progress.
   */
  @Override
  public void setShutdown() {
    latestViewWriteLock.lock();
    shutdownInProgress = true;
    latestViewWriteLock.unlock();
  }

  /**
   * Clean up and create consistent new view with member removed. No uplevel events are generated.
   */
  private void destroyMember(final ID member, final String reason) {

    // Make sure it is removed from the view
    latestViewWriteLock.lock();
    try {
      if (latestView.contains(member)) {
        latestView = latestView.createNewViewWithoutMember(member);
      }
      surpriseMembers.remove(member);
    } finally {
      latestViewWriteLock.unlock();
    }

    lifecycleListener.destroyMember(member, reason);
  }

  /**
   * Indicate whether the given member is in the zombie list (dead or dying)
   *
   * @param m the member in question
   *
   * @return true if the given member is a zombie
   */
  @Override
  public boolean isShunned(ID m) {
    final MembershipView<ID> view = latestView;
    return m.getVmViewId() <= view.getViewId() && !view.contains(m);
  }

  private boolean isShunnedOrNew(final ID m) {
    final MembershipView<ID> view = latestView;
    if (m.getVmViewId() <= view.getViewId() && view.contains(m)) {
      return false;
    }
    latestViewReadLock.lock();
    try {
      return isShunned(m) || isNew(m);
    } finally { // synchronized
      latestViewReadLock.unlock();
    }
  }

  // must be invoked under view read or write lock
  private boolean isNew(final ID m) {
    return !latestView.contains(m) && !surpriseMembers.containsKey(m);
  }

  /**
   * Indicate whether the given member is in the surprise member list
   * <P>
   * This method will not cause expiry of a surprise member. That must be done
   * during view processing.
   * <p>
   * Concurrency: protected by {@link #latestViewReadLock}
   *
   * @param m the member in question
   * @return true if the given member is a surprise member
   */
  @Override
  public boolean isSurpriseMember(ID m) {
    latestViewReadLock.lock();
    try {
      if (surpriseMembers.containsKey(m)) {
        final long birthTime = surpriseMembers.get(m);
        final long now = System.currentTimeMillis();
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
  @Override
  public void addSurpriseMemberForTesting(ID m, long birthTime) {
    if (logger.isDebugEnabled()) {
      logger.debug("test hook is adding surprise member {} birthTime={}", m, birthTime);
    }
    latestViewWriteLock.lock();
    try {
      surpriseMembers.put(m, Long.valueOf(birthTime));
    } finally {
      latestViewWriteLock.unlock();
    }
  }

  /**
   * returns the surpriseMemberTimeout interval, in milliseconds
   */
  public long getSurpriseMemberTimeout() {
    return this.surpriseMemberTimeout;
  }

  @Override
  public void setReconnectCompleted(boolean reconnectCompleted) {
    this.reconnectCompleted = reconnectCompleted;
  }


  /*
   * non-thread-owned serial channels and high priority channels are not included
   */
  @Override
  public Map<String, Long> getMessageState(ID member, boolean includeMulticast,
      Map<String, Long> result) {
    services.getMessenger().getMessageState(member, result, includeMulticast);
    return result;
  }

  @Override
  public void waitForMessageState(ID otherMember, Map<String, Long> state)
      throws InterruptedException, TimeoutException {
    services.getMessenger().waitForMessageState(otherMember, state);
  }

  @Override
  public boolean shutdownInProgress() {
    return shutdownInProgress;
  }

  @Override
  public boolean waitForNewMember(ID remoteId) {
    boolean foundRemoteId = false;
    CountDownLatch currentLatch = null;
    // latestViewWriteLock protects memberLatch from modification while processView is in progress
    latestViewWriteLock.lock();
    try {
      if (latestView != null) {
        if (latestView.contains(remoteId)) {
          // check if remoteId is already in membership view.
          // If not, then create a latch if needed and wait for the latch to open.
          foundRemoteId = true;
        } else if ((currentLatch = this.memberLatch.get(remoteId)) == null) {
          currentLatch = new CountDownLatch(1);
          this.memberLatch.put(remoteId, currentLatch);
        }
      }
    } finally {
      latestViewWriteLock.unlock();
    }

    if (!foundRemoteId) {
      try {
        if (currentLatch != null
            && currentLatch.await(membershipCheckTimeout, TimeUnit.MILLISECONDS)) {
          foundRemoteId = true;
        }
      } catch (InterruptedException ex) {
        // latch attempt was interrupted.
        Thread.currentThread().interrupt();
        logger.warn("The membership check was terminated with an exception.");
      }
    }
    return foundRemoteId;
  }

  /* returns the cause of shutdown, if known */
  @Override
  public Throwable getShutdownCause() {
    return services.getShutdownCause();
  }

  private volatile boolean beingSick;
  private volatile boolean playingDead;

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

  @VisibleForTesting
  public void forceDisconnect(String reason) {
    getGMSManager().forceDisconnect(reason);
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
  @Override
  public boolean isBeingSick() {
    return this.beingSick;
  }

  /**
   * Test hook - inhibit ForcedDisconnectException logging to keep dunit logs clean
   */
  public static void inhibitForcedDisconnectLogging(boolean b) {
    inhibitForceDisconnectLogging = b;
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
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > 1000;
    }
  }


  @Override
  public void disconnect(boolean beforeJoined) {
    if (beforeJoined) {
      uncleanShutdown("Failed to start distribution", null);
    } else {
      shutdown();
    }
  }

  @Override
  public void start() throws MemberStartupException {
    services.start();
  }

  @Override
  public void setCloseInProgress() {
    isCloseInProgress = true;
  }


  class ManagerImpl implements Manager<ID> {

    @Override
    public Services<ID> getServices() {
      return services;
    }

    @Override
    /* Service interface */
    public void init(Services<ID> services) throws MembershipConfigurationException {
      GMSMembership.this.services = services;

      MembershipConfig config = services.getConfig();

      membershipCheckTimeout = config.getSecurityPeerMembershipTimeout();
      wasReconnectingSystem = config.getIsReconnectingDS();


      surpriseMemberTimeout =
          Math.max(20 * MembershipConfig.DEFAULT_MEMBER_TIMEOUT, 20 * config.getMemberTimeout());
      surpriseMemberTimeout =
          Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "surprise-member-timeout",
              surpriseMemberTimeout).longValue();

    }

    /* Service interface */
    @Override
    public void start() throws MemberStartupException {
      lifecycleListener.start(services.getMessenger().getMemberID());

    }

    /* Service interface */
    @Override
    public void started() {
      startCleanupTimer();
    }

    /* Service interface */
    @Override
    public void stop() {
      logger.debug("Membership closing");

      if (lifecycleListener.disconnect(null)) {
        if (address != null) {
          destroyMember(address, "orderly shutdown");
        }
      }

      if (cleanupTimer != null && !cleanupTimer.isShutdown()) {
        cleanupTimer.shutdown();
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Membership: channel closed");
      }
    }

    /* Service interface */
    @Override
    public void stopped() {}

    /* Service interface */
    @Override
    public void installView(GMSMembershipView<ID> v) {
      MembershipView<ID> currentView = latestView;
      if (currentView.getViewId() < 0 && !isConnected()) {
        latestView = createGeodeView(v);
        logger.debug("Membership: initial view is {}", latestView);
      } else {
        handleOrDeferViewEvent(createGeodeView(v));
      }
    }

    @Override
    public void beSick() {
      // no-op
    }

    @Override
    public void playDead() {
      // no-op
    }

    @Override
    public void beHealthy() {
      // no-op
    }

    @Override
    public void emergencyClose() {
      // no-op
    }


    @Override
    public void joinDistributedSystem() throws MemberStartupException {
      long startTime = System.currentTimeMillis();

      try {
        join();
      } catch (MemberStartupException | RuntimeException e) {
        lifecycleListener.disconnect(e);
        throw e;
      }

      GMSMembership.this.address =
          services.getMessenger().getMemberID();

      lifecycleListener.joinCompleted(address);

      GMSMembership.this.hasJoined = true;

      // in order to debug startup issues we need to announce the membership
      // ID as soon as we know it
      logger.info("Finished joining (took {}ms).",
          "" + (System.currentTimeMillis() - startTime));

    }

    @Override
    public void memberSuspected(ID initiator, ID suspect, String reason) {
      SuspectMember<ID> s = new SuspectMember<>(initiator, suspect, reason);
      handleOrDeferSuspect(s);
    }

    void uncleanShutdownReconnectingDS(String reason, Exception shutdownCause) {
      logger.info("Reconnecting system failed to connect");
      lifecycleListener.forcedDisconnect(reason, RECONNECTING);
      uncleanShutdown(reason,
          new MemberDisconnectedException("reconnecting system failed to connect"));
    }

    void uncleanShutdownDS(String reason, Exception shutdownCause) {
      try {
        listener.saveConfig();
      } finally {
        new LoggingThread("DisconnectThread", false, () -> {
          lifecycleListener.forcedDisconnect(reason, NOT_RECONNECTING);
          uncleanShutdown(reason, shutdownCause);
        }).start();
      }
    }

    @Override
    public void forceDisconnect(final String reason) {
      if (GMSMembership.this.shutdownInProgress || isJoining()) {
        return; // probably a race condition
      }

      setShutdown();

      final Exception shutdownCause = new MemberDisconnectedException(reason);

      // cache the exception so it can be appended to ShutdownExceptions
      services.setShutdownCause(shutdownCause);
      services.getCancelCriterion().cancel(reason);

      if (!inhibitForceDisconnectLogging) {
        logger.fatal(
            String.format("Membership service failure: %s", reason),
            shutdownCause);
      }

      if (this.isReconnectingDS()) {
        uncleanShutdownReconnectingDS(reason, shutdownCause);
      } else {
        uncleanShutdownDS(reason, shutdownCause);
      }
    }


    /** this is invoked by JoinLeave when there is a loss of quorum in the membership system */
    @Override
    public void quorumLost(Collection<ID> failures, GMSMembershipView<ID> view) {
      // notify of quorum loss if split-brain detection is enabled (meaning we'll shut down) or
      // if the loss is more than one member

      boolean notify = failures.size() > 1;
      if (!notify) {
        notify = services.getConfig().isNetworkPartitionDetectionEnabled();
      }

      if (notify) {
        List<ID> remaining =
            gmsMemberListToIDList(view.getMembers());
        remaining.removeAll(failures);

        if (inhibitForceDisconnectLogging) {
          if (logger.isDebugEnabled()) {
            logger
                .debug("<ExpectedException action=add>Possible loss of quorum</ExpectedException>");
          }
        }
        logger.fatal("Possible loss of quorum due to the loss of {} cache processes: {}",
            failures.size(), failures);
        if (inhibitForceDisconnectLogging) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "<ExpectedException action=remove>Possible loss of quorum</ExpectedException>");
          }
        }


        try {
          listener.quorumLost(
              gmsMemberCollectionToIDSet(failures),
              remaining);
        } catch (Exception e) {
          logger.info("Quorum-loss listener threw an exception", e);
        }
      }
    }

    @Override
    public void processMessage(Message<ID> msg) throws MemberShunnedException {
      // UDP messages received from surprise members will have partial IDs.
      // Attempt to replace these with full IDs from the Membership's view.
      if (msg.getSender().isPartial()) {
        replacePartialIdentifierInMessage(msg);
      }

      handleOrDeferMessage(msg);
    }

    @Override
    public boolean isMulticastAllowed() {
      return !disableMulticastForRollingUpgrade;
    }

    @Override
    public boolean shutdownInProgress() {
      return shutdownInProgress;
    }

    @Override
    public boolean isCloseInProgress() {
      return shutdownInProgress || isCloseInProgress;
    }


    @Override
    public boolean isReconnectingDS() {
      return wasReconnectingSystem && !reconnectCompleted;
    }

  }
}
