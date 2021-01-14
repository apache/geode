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
package org.apache.geode.internal.cache.tier.sockets;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.CancelException;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.ClientSession;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.internal.RegisterInterestTracker;
import org.apache.geode.cache.operations.DestroyOperationContext;
import org.apache.geode.cache.operations.InvalidateOperationContext;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.cache.operations.PutOperationContext;
import org.apache.geode.cache.operations.RegionClearOperationContext;
import org.apache.geode.cache.operations.RegionCreateOperationContext;
import org.apache.geode.cache.operations.RegionDestroyOperationContext;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.SystemTimer.SystemTimerTask;
import org.apache.geode.internal.cache.CacheDistributionAdvisee;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.StateFlushOperation;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.ha.HARegionQueueStats;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl.CqNameToOp;
import org.apache.geode.internal.cache.tier.sockets.command.Get70;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.AccessControl;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Class <code>CacheClientProxy</code> represents the server side of the {@link CacheClientUpdater}.
 * It queues messages to be sent from the server to the client. It then reads those messages from
 * the queue and sends them to the client.
 *
 * @since GemFire 4.2
 */
@SuppressWarnings("synthetic-access")
public class CacheClientProxy implements ClientSession {
  private static final Logger logger = LogService.getLogger();

  @Immutable
  @VisibleForTesting
  protected static final CacheClientProxyStatsFactory DEFAULT_CACHECLIENTPROXYSTATSFACTORY =
      (statisticsFactory, proxyId, remoteHostAddress) -> new CacheClientProxyStats(
          statisticsFactory,
          "id_" + proxyId.getDistributedMember().getId() + "_at_" + remoteHostAddress);
  @Immutable
  private static final MessageDispatcherFactory DEFAULT_MESSAGEDISPATCHERFACTORY =
      MessageDispatcher::new;

  /**
   * The socket between the server and the client
   */
  protected Socket _socket;

  private final AtomicBoolean _socketClosed = new AtomicBoolean();

  /**
   * A communication buffer used by each message we send to the client
   */
  protected ByteBuffer _commBuffer;

  /**
   * The remote host's IP address string (cached for convenience)
   */
  protected String _remoteHostAddress;

  /**
   * Concurrency: protected by synchronization of {@link #isMarkedForRemovalLock}
   */
  protected volatile boolean isMarkedForRemoval = false;

  /**
   * @see #isMarkedForRemoval
   */
  protected final Object isMarkedForRemovalLock = new Object();

  /**
   * The proxy id of the client represented by this proxy
   */
  protected ClientProxyMembershipID proxyID;

  /**
   * The GemFire cache
   */
  protected final InternalCache _cache;

  public List<ClientInterestList> getClientInterestList() {
    return Arrays.asList(cils);
  }

  /**
   * The list of keys that the client represented by this proxy is interested in (stored by region)
   */
  protected final ClientInterestList[] cils = new ClientInterestList[2];

  /**
   * A thread that dispatches messages to the client
   */
  protected volatile MessageDispatcher _messageDispatcher;

  /**
   * The statistics for this proxy
   */
  protected final CacheClientProxyStats _statistics;

  protected final AtomicReference<SystemTimerTask> _durableExpirationTask = new AtomicReference<>();

  protected SystemTimer durableTimer;

  /**
   * Whether this dispatcher is paused
   */
  protected volatile boolean _isPaused = true;

  /**
   * True if we are connected to a client.
   */
  private volatile boolean connected = false;

  /**
   * True if a marker message is still in the ha queue.
   */
  private boolean markerEnqueued = false;

  /**
   * The number of times to peek on shutdown before giving up and shutting down
   */
  protected static final int MAXIMUM_SHUTDOWN_PEEKS = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "MAXIMUM_SHUTDOWN_PEEKS", 50);

  /*
   * The default maximum message queue size
   */
  // protected static final int MESSAGE_QUEUE_SIZE_DEFAULT = 230000;

  /** The message queue size */
  protected final int _maximumMessageCount;

  /**
   * The time (in seconds ) after which a message in the client queue will expire.
   */
  protected final int _messageTimeToLive;

  /**
   * The <code>CacheClientNotifier</code> registering this proxy.
   */
  protected final CacheClientNotifier _cacheClientNotifier;

  /**
   * for testing purposes, delays the start of the dispatcher thread
   */
  @MutableForTesting
  public static boolean isSlowStartForTesting = false;

  private boolean isPrimary;

  /** @since GemFire 5.7 */
  protected byte clientConflation = Handshake.CONFLATION_DEFAULT;

  /**
   * Flag to indicate whether to keep a durable client's queue alive
   */
  boolean keepalive = false;

  /**
   * for single user environment
   */
  private AccessControl postAuthzCallback;
  private Subject subject;

  /**
   * used for cq name to subject/auth mapping, always initialized in single/multi user casees
   */
  private ClientUserAuths clientUserAuths;

  private final Object clientUserAuthsLock = new Object();

  /**
   * The version of the client
   */
  private KnownVersion clientVersion;

  /**
   * A map of region name as key and integer as its value. Basically, it stores the names of the
   * regions with <code>DataPolicy</code> as EMPTY. If an event's region name is present in this
   * map, it's full value (and not delta) is sent to the client represented by this proxy.
   *
   * @since GemFire 6.1
   */
  private final Map<String, Integer> regionsWithEmptyDataPolicy = new HashMap<>();

  /**
   * A debug flag used for testing Backward compatibility
   */
  @MutableForTesting
  public static boolean AFTER_MESSAGE_CREATION_FLAG = false;

  /**
   * Notify the region when a client interest registration occurs. This tells the region to update
   * access time when an update is to be pushed to a client. It is enabled only for
   * <code>PartitionedRegion</code>s currently.
   */
  protected static final boolean NOTIFY_REGION_ON_INTEREST =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "updateAccessTimeOnClientInterest");

  /**
   * The AcceptorImpl identifier to which the proxy is connected.
   */
  private final long _acceptorId;

  /** acceptor's setting for notifyBySubscription */
  private final boolean notifyBySubscription;

  /**
   * A counter that keeps track of how many task iterations that have occurred since the last ping
   * or message. The {@linkplain CacheClientNotifier#scheduleClientPingTask ping task} increments
   * it. Normal messages sent to the client reset it. If the counter reaches 3, a ping is sent.
   */
  private final AtomicInteger pingCounter = new AtomicInteger();


  /** Date on which this instances was created */
  private Date creationDate;

  /**
   * true when the durable client associated with this proxy is being restarted and prevents cqs
   * from being closed and drained
   **/
  private boolean drainLocked = false;
  private final Object drainLock = new Object();

  /** number of cq drains that are currently in progress **/
  private int numDrainsInProgress = 0;
  private final Object drainsInProgressLock = new Object();

  private final SecurityService securityService;
  private final StatisticsClock statisticsClock;

  private final MessageDispatcherFactory messageDispatcherFactory;

  /**
   * Constructor.
   *
   * @param ccn The <code>CacheClientNotifier</code> registering this proxy
   * @param socket The socket between the server and the client
   * @param proxyID representing the Connection Proxy of the clien
   * @param isPrimary The boolean stating whether this prozxy is primary
   * @throws CacheException {
   */
  protected CacheClientProxy(CacheClientNotifier ccn, Socket socket,
      ClientProxyMembershipID proxyID, boolean isPrimary, byte clientConflation,
      KnownVersion clientVersion, long acceptorId, boolean notifyBySubscription,
      SecurityService securityService, Subject subject, StatisticsClock statisticsClock)
      throws CacheException {
    this(ccn.getCache(), ccn, socket, proxyID, isPrimary, clientConflation, clientVersion,
        acceptorId, notifyBySubscription, securityService, subject, statisticsClock,
        ccn.getCache().getInternalDistributedSystem().getStatisticsManager(),
        DEFAULT_CACHECLIENTPROXYSTATSFACTORY,
        DEFAULT_MESSAGEDISPATCHERFACTORY);
  }

  @VisibleForTesting
  protected CacheClientProxy(InternalCache cache, CacheClientNotifier ccn, Socket socket,
      ClientProxyMembershipID proxyID, boolean isPrimary, byte clientConflation,
      KnownVersion clientVersion, long acceptorId, boolean notifyBySubscription,
      SecurityService securityService, Subject subject, StatisticsClock statisticsClock,
      StatisticsFactory statisticsFactory,
      CacheClientProxyStatsFactory cacheClientProxyStatsFactory,
      MessageDispatcherFactory messageDispatcherFactory)
      throws CacheException {
    initializeTransientFields(socket, proxyID, isPrimary, clientConflation, clientVersion);
    _cacheClientNotifier = ccn;
    _cache = cache;
    this.securityService = securityService;
    _maximumMessageCount = ccn.getMaximumMessageCount();
    _messageTimeToLive = ccn.getMessageTimeToLive();
    _acceptorId = acceptorId;
    this.notifyBySubscription = notifyBySubscription;
    this.statisticsClock = statisticsClock;
    _statistics =
        cacheClientProxyStatsFactory.create(statisticsFactory, proxyID, _remoteHostAddress);
    this.subject = subject;

    // Create the interest list
    cils[RegisterInterestTracker.interestListIndex] =
        new ClientInterestList(this, this.proxyID);
    // Create the durable interest list
    cils[RegisterInterestTracker.durableInterestListIndex] =
        new ClientInterestList(this, getDurableId());
    postAuthzCallback = null;
    _cacheClientNotifier.getAcceptorStats().incCurrentQueueConnections();
    creationDate = new Date();
    this.messageDispatcherFactory = messageDispatcherFactory;
    initializeClientAuths();
  }

  void initializeClientAuths() {
    clientUserAuths = ServerConnection.getClientUserAuths(proxyID);
  }

  private void reinitializeClientAuths() {
    synchronized (clientUserAuthsLock) {
      ClientUserAuths newClientAuth = ServerConnection.getClientUserAuths(proxyID);
      newClientAuth.fillPreviousCQAuth(clientUserAuths);
      clientUserAuths = newClientAuth;
    }
  }

  public void setPostAuthzCallback(AccessControl authzCallback) {
    // TODO:hitesh synchronization
    synchronized (clientUserAuthsLock) {
      if (postAuthzCallback != null) {
        postAuthzCallback.close();
      }
      postAuthzCallback = authzCallback;
    }
  }

  public void setSubject(Subject subject) {
    // if we are replacing a subject here, the old subject's logout should be handled
    // by the ClientUserAuths already
    synchronized (clientUserAuthsLock) {
      this.subject = subject;
    }
  }

  protected Subject getSubject(String cqName) {
    synchronized (clientUserAuthsLock) {
      return clientUserAuths.getSubject(cqName);
    }
  }

  public void setCQVsUserAuth(String cqName, long uniqueId, boolean isDurable) {
    clientUserAuths.setUserAuthAttributesForCq(cqName, uniqueId, isDurable);
  }

  private void initializeTransientFields(Socket socket, ClientProxyMembershipID pid, boolean ip,
      byte cc, KnownVersion vers) {
    _socket = socket;
    proxyID = pid;
    connected = true;
    {
      int bufSize = 1024;
      try {
        bufSize = _socket.getSendBufferSize();
        if (bufSize < 1024) {
          bufSize = 1024;
        }
      } catch (SocketException ignore) {
      }
      _commBuffer = ServerConnection.allocateCommBuffer(bufSize, socket);
    }
    _remoteHostAddress = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
    isPrimary = ip;
    clientConflation = cc;
    clientVersion = vers;
  }

  public boolean isMarkerEnqueued() {
    return markerEnqueued;
  }

  public void setMarkerEnqueued(boolean bool) {
    markerEnqueued = bool;
  }

  public long getAcceptorId() {
    return _acceptorId;
  }

  /**
   * @return the notifyBySubscription
   */
  public boolean isNotifyBySubscription() {
    return notifyBySubscription;
  }


  /**
   * Returns the DistributedMember represented by this proxy
   */
  public ClientProxyMembershipID getProxyID() {
    return proxyID;
  }

  protected boolean isMember(ClientProxyMembershipID memberId) {
    return proxyID.equals(memberId);
  }

  /**
   * Set the queue keepalive option
   *
   * @param option whether to keep the durable client's queue alive
   */
  protected void setKeepAlive(boolean option) {
    keepalive = option;
  }

  /**
   * Returns the socket between the server and the client
   *
   * @return the socket between the server and the client
   */
  protected Socket getSocket() {
    return _socket;
  }

  public String getSocketHost() {
    return _socket.getInetAddress().getHostAddress();
  }

  protected ByteBuffer getCommBuffer() {
    return _commBuffer;
  }

  /**
   * Returns the remote host's IP address string
   *
   * @return the remote host's IP address string
   */
  protected String getRemoteHostAddress() {
    return _remoteHostAddress;
  }

  /**
   * Returns the remote host's port
   *
   * @return the remote host's port
   */
  public int getRemotePort() {
    return _socket.getPort();
  }

  /**
   * Returns whether the proxy is connected to a remote client
   *
   * @return whether the proxy is connected to a remote client
   */
  public boolean isConnected() {
    return connected;
  }

  /**
   * Mark the receiver as needing removal
   *
   * @return true if it was already marked for removal
   */
  protected boolean startRemoval() {
    boolean result;
    synchronized (isMarkedForRemovalLock) {
      result = isMarkedForRemoval;
      isMarkedForRemoval = true;
    }
    return result;
  }

  /**
   * Wait until the receiver's removal has completed before returning.
   *
   * @return true if the proxy was initially marked for removal
   */
  protected boolean waitRemoval() {
    boolean result;
    synchronized (isMarkedForRemovalLock) {
      result = isMarkedForRemoval;
      boolean interrupted = false;
      try {
        while (isMarkedForRemoval) {
          if (logger.isDebugEnabled()) {
            logger.debug("Waiting for CacheClientProxy removal: {}", this);
          }
          try {
            isMarkedForRemovalLock.wait();
          } catch (InterruptedException e) {
            interrupted = true;
            _cache.getCancelCriterion().checkCancelInProgress(e);
          }
        } // while
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // synchronized
    return result;
  }

  /**
   * Indicate that removal has completed on this instance
   */
  protected void notifyRemoval() {
    synchronized (isMarkedForRemovalLock) {
      isMarkedForRemoval = false;
      isMarkedForRemovalLock.notifyAll();
    }
  }

  /**
   * Returns the GemFire cache
   *
   * @return the GemFire cache
   */
  public InternalCache getCache() {
    return _cache;
  }

  public Set<String> getInterestRegisteredRegions() {
    HashSet<String> regions = new HashSet<>();
    for (final ClientInterestList cil : cils) {
      if (!cil.regions.isEmpty()) {
        regions.addAll(cil.regions);
      }
    }
    return regions;
  }

  /**
   * Returns the proxy's statistics
   *
   * @return the proxy's statistics
   */
  public CacheClientProxyStats getStatistics() {
    return _statistics;
  }

  /**
   * Returns this proxy's <code>CacheClientNotifier</code>.
   *
   * @return this proxy's <code>CacheClientNotifier</code>
   */
  protected CacheClientNotifier getCacheClientNotifier() {
    return _cacheClientNotifier;
  }

  /**
   * Returns the size of the queue for heuristic purposes. This size may be changing concurrently if
   * puts/gets are occurring at the same time.
   */
  public int getQueueSize() {
    return _messageDispatcher == null ? 0 : _messageDispatcher.getQueueSize();
  }

  /**
   * returns the queue size calculated through stats
   */
  public int getQueueSizeStat() {
    return _messageDispatcher == null ? 0 : _messageDispatcher.getQueueSizeStat();
  }

  public HARegionQueueStats getRegionQueueStats() {
    return this._messageDispatcher._messageQueue.stats;
  }


  public boolean drainInProgress() {
    synchronized (drainsInProgressLock) {
      return numDrainsInProgress > 0;
    }
  }

  // Called from CacheClientNotifier when attempting to restart paused proxy
  // locking the drain lock requires that no drains are in progress
  // when the lock was acquired.
  public boolean lockDrain() {
    synchronized (drainsInProgressLock) {
      if (!drainInProgress()) {
        synchronized (drainLock) {
          if (testHook != null) {
            testHook.doTestHook("PRE_ACQUIRE_DRAIN_LOCK_UNDER_SYNC");
          }
          // prevent multiple lockings of drain lock
          if (!drainLocked) {
            drainLocked = true;
            return true;
          }
        }
      }
    }
    return false;
  }

  // Called from CacheClientNotifier when completed restart of proxy
  public void unlockDrain() {
    if (testHook != null) {
      testHook.doTestHook("PRE_RELEASE_DRAIN_LOCK");
    }
    synchronized (drainLock) {
      drainLocked = false;
    }
  }

  // Only close the client cq if it is paused and no one is attempting to restart the proxy
  public boolean closeClientCq(String clientCQName) throws CqException {
    if (testHook != null) {
      testHook.doTestHook("PRE_DRAIN_IN_PROGRESS");
    }
    synchronized (drainsInProgressLock) {
      numDrainsInProgress++;
    }
    if (testHook != null) {
      testHook.doTestHook("DRAIN_IN_PROGRESS_BEFORE_DRAIN_LOCK_CHECK");
    }
    try {
      // If the drain lock was acquired, the other thread did so before we could bump up
      // the numDrainsInProgress. That means we need to stop.
      if (drainLocked) {
        // someone is trying to restart a paused proxy
        String msg =
            String.format(
                "CacheClientProxy: Could not drain cq %s due to client proxy id %s reconnecting.",
                clientCQName, proxyID.getDurableId());
        logger.info(msg);
        throw new CqException(msg);
      }
      // isConnected is to protect against the case where a durable client has reconnected
      // but has not yet sent a ready for events message
      // we can probably remove the isPaused check
      if (isPaused() && !isConnected()) {
        CqService cqService = getCache().getCqService();
        if (cqService != null) {
          InternalCqQuery cqToClose =
              cqService.getCq(cqService.constructServerCqName(clientCQName, proxyID));
          // close and drain
          if (cqToClose != null) {
            cqService.closeCq(clientCQName, proxyID);
            _messageDispatcher.drainClientCqEvents(proxyID, cqToClose);
          } else {
            String msg = String.format("CQ Not found, Failed to close the specified CQ %s",
                clientCQName);
            logger.info(msg);
            throw new CqException(msg);
          }
        }
      } else {
        String msg =
            String.format(
                "CacheClientProxy: Could not drain cq %s because client proxy id %s is connected.",
                clientCQName, proxyID.getDurableId());
        logger.info(msg);
        throw new CqException(msg);
      }
    } finally {
      synchronized (drainsInProgressLock) {
        numDrainsInProgress--;
      }
      if (testHook != null) {
        testHook.doTestHook("DRAIN_COMPLETE");
      }

    }
    return true;
  }


  /**
   * Returns whether the proxy is alive. It is alive if its message dispatcher is processing
   * messages.
   *
   * @return whether the proxy is alive
   */
  protected boolean isAlive() {
    if (_messageDispatcher == null) {
      return false;
    }
    return !_messageDispatcher.isStopped();
  }

  /**
   * Returns whether the proxy is paused. It is paused if its message dispatcher is paused. This
   * only applies to durable clients.
   *
   * @return whether the proxy is paused
   *
   * @since GemFire 5.5
   */
  public boolean isPaused() {
    return _isPaused;
  }

  protected void setPaused(boolean isPaused) {
    _isPaused = isPaused;
  }

  /**
   * Closes the proxy. This method checks the message queue for any unprocessed messages and
   * processes them for MAXIMUM_SHUTDOWN_PEEKS.
   *
   * @see CacheClientProxy#MAXIMUM_SHUTDOWN_PEEKS
   */
  protected void close() {
    close(true, false);
  }

  /**
   * Set to true once this proxy starts being closed. Remains true for the rest of its existence.
   */
  private final AtomicBoolean closing = new AtomicBoolean(false);

  /**
   * Close the <code>CacheClientProxy</code>.
   *
   * @param checkQueue Whether to message check the queue and process any contained messages (up to
   *        MAXIMUM_SHUTDOWN_PEEKS).
   * @param stoppedNormally Whether client stopped normally
   *
   * @return whether to keep this <code>CacheClientProxy</code>
   * @see CacheClientProxy#MAXIMUM_SHUTDOWN_PEEKS
   */
  protected boolean close(boolean checkQueue, boolean stoppedNormally) {
    // If the client is durable and either (a) it hasn't stopped normally or (b) it
    // has stopped normally but it is configured to be kept alive, set pauseDurable
    // to true
    final boolean pauseDurable =
        isDurable() && (!stoppedNormally || (getDurableKeepAlive() && stoppedNormally));

    boolean keepProxy = false;
    if (pauseDurable) {
      pauseDispatching();
      keepProxy = true;
    } else {
      terminateDispatching(checkQueue);
      closeTransientFields();
    }

    connected = false;

    // Close the Authorization callback (if any)
    try {
      if (!pauseDurable) {
        if (postAuthzCallback != null) {// for single user
          postAuthzCallback.close();
          postAuthzCallback = null;
        }
        if (clientUserAuths != null) {// for multiple users
          clientUserAuths.cleanup(true);
          clientUserAuths = null;
        }
      }
    } catch (Exception ex) {
      if (_cache.getSecurityLogger().warningEnabled()) {
        _cache.getSecurityLogger().warning(String.format("%s : %s", this, ex));
      }
    }
    // Notify the caller whether to keep this proxy. If the proxy is durable
    // and should be paused, then return true; otherwise return false.
    return keepProxy;
  }

  protected void pauseDispatching() {
    if (_messageDispatcher == null) {
      return;
    }

    // If this is the primary, pause the dispatcher (which closes its transient
    // fields. Otherwise, just close the transient fields.
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Pausing processing", this);
    }
    // BUGFIX for BUG#38234
    if (!testAndSetPaused(true) && isPrimary) {
      if (_messageDispatcher != Thread.currentThread()) {
        // don't interrupt ourself to fix bug 40611
        _messageDispatcher.interrupt();
      }
    }

    try {
      // Close transient fields
      closeTransientFields();
    } finally {
      // make sure this gets called if closeTransientFields throws; see bug 40611
      // Start timer
      scheduleDurableExpirationTask();
    }
  }

  private boolean testAndSetPaused(boolean newValue) {

    synchronized (_messageDispatcher._pausedLock) {
      if (_isPaused != newValue) {
        _isPaused = newValue;
        _messageDispatcher._pausedLock.notifyAll();
        return !_isPaused;
      } else {
        _messageDispatcher._pausedLock.notifyAll();
        return _isPaused;
      }
    }
  }

  protected void terminateDispatching(boolean checkQueue) {
    if (_messageDispatcher == null) {
      return;
    }

    boolean closedSocket = false;
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Terminating processing", this);
      }
      if (_messageDispatcher == Thread.currentThread()) {
        // I'm not even sure this is possible but if the dispatcher
        // calls us then at least call stopDispatching
        // the old code did this (I'm not even sure it is safe to do).
        // This needs to be done without testing OR setting "closing".
        _messageDispatcher.stopDispatching(checkQueue);
        cils[RegisterInterestTracker.interestListIndex].clearClientInterestList();
        cils[RegisterInterestTracker.durableInterestListIndex].clearClientInterestList();
        // VJR: bug 37487 fix
        destroyRQ();
        return;
      }

      if (!closing.compareAndSet(false, true)) {
        // must already be closing so just return
        // this is part of the fix for 37684
        return;
      }
      // Unregister interest in all interests (if necessary)
      cils[RegisterInterestTracker.interestListIndex].clearClientInterestList();
      cils[RegisterInterestTracker.durableInterestListIndex].clearClientInterestList();

      // If the message dispatcher is paused, unpause it. The next bit of
      // code will interrupt the waiter.
      if (testAndSetPaused(false)) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Paused but terminating processing", this);
        }
        // Cancel the expiration task
        cancelDurableExpirationTask(false);
      }

      boolean alreadyDestroyed = false;
      boolean gotInterrupt = Thread.interrupted(); // clears the flag
      try {
        // Stop the message dispatcher
        _messageDispatcher.stopDispatching(checkQueue);

        gotInterrupt |= Thread.interrupted(); // clears the flag

        // to fix bug 37684
        // 1. check to see if dispatcher is still alive
        if (_messageDispatcher.isAlive()) {
          closedSocket = closeSocket();
          destroyRQ();
          alreadyDestroyed = true;
          _messageDispatcher.interrupt();
          if (_messageDispatcher.isAlive()) {
            try {
              _messageDispatcher.join(1000);
            } catch (InterruptedException ex) {
              gotInterrupt = true;
            }
            // if it is still alive then warn and move on
            if (_messageDispatcher.isAlive()) {
              // org.apache.geode.logging.internal.OSProcess.printStacks(org.apache.geode.logging.internal.OSProcess.getId());
              logger.warn("{}: Could not stop message dispatcher thread.",
                  this);
            }
          }
        }
      } finally {
        if (gotInterrupt) {
          Thread.currentThread().interrupt();
        }
        if (!alreadyDestroyed) {
          destroyRQ();
        }
      }
    } finally {
      // Close the statistics
      _statistics.close(); // fix for bug 40105
      if (closedSocket) {
        closeOtherTransientFields();
      } else {
        closeTransientFields(); // make sure this happens
      }
    }
  }

  private boolean closeSocket() {
    String remoteHostAddress = _remoteHostAddress;
    if (_socketClosed.compareAndSet(false, true) && remoteHostAddress != null) {
      // Only one thread is expected to close the socket
      _cacheClientNotifier.getSocketCloser().asyncClose(_socket, remoteHostAddress,
          () -> {
          });
      getCacheClientNotifier().getAcceptorStats().decCurrentQueueConnections();
      return true;
    }
    return false;
  }

  private void closeTransientFields() {
    if (!closeSocket()) {
      // The thread who closed the socket will be responsible to
      // releaseResourcesForAddress and clearClientInterestList
      return;
    }

    closeOtherTransientFields();
  }

  private void closeOtherTransientFields() {
    // Null out comm buffer, host address, ports and proxy id. All will be
    // replaced when the client reconnects.
    releaseCommBuffer();
    {
      String remoteHostAddress = _remoteHostAddress;
      if (remoteHostAddress != null) {
        _cacheClientNotifier.getSocketCloser().releaseResourcesForAddress(remoteHostAddress);
        _remoteHostAddress = null;
      }
    }
    try {
      cils[RegisterInterestTracker.interestListIndex].clearClientInterestList();
    } catch (CancelException e) {
      // ignore if cache is shutting down
    }
    // Commented to fix bug 40259
    // this.clientVersion = null;
    closeNonDurableCqs();

    // Logout the subject
    if (subject != null) {
      subject.logout();
    }
  }

  private void releaseCommBuffer() {
    ByteBuffer bb = _commBuffer;
    if (bb != null) {
      _commBuffer = null;
      ServerConnection.releaseCommBuffer(bb);
    }
  }

  private void closeNonDurableCqs() {
    CqService cqService = getCache().getCqService();
    if (cqService != null) {
      try {
        cqService.closeNonDurableClientCqs(getProxyID());
      } catch (CqException ex) {
        logger.warn("CqException while closing non durable Cqs. {}",
            ex.getLocalizedMessage());
      }
    }
  }

  private void destroyRQ() {
    if (_messageDispatcher == null) {
      return;
    }
    try {
      // Using Destroy Region bcoz this method is modified in HARegion so as
      // not to distribute.
      // For normal Regions , even the localDestroyRegion actually propagates
      HARegionQueue rq = _messageDispatcher._messageQueue;
      rq.destroy();
    } catch (RegionDestroyedException | CancelException ignored) {
    } catch (Exception warning) {
      logger.warn(
          String.format("%s: Exception in closing the underlying HARegion of the HARegionQueue",
              this),
          warning);
    }
  }

  @Override
  public void registerInterestRegex(String regionName, String regex, boolean isDurable) {
    registerInterestRegex(regionName, regex, isDurable, true);
  }

  @Override
  public void registerInterestRegex(String regionName, String regex, boolean isDurable,
      boolean receiveValues) {
    if (isPrimary) {
      // Notify all secondaries and client of change in interest
      notifySecondariesAndClient(regionName, regex, InterestResultPolicy.NONE, isDurable,
          receiveValues, InterestType.REGULAR_EXPRESSION);
    } else {
      throw new IllegalStateException(
          "This process is not the primary server for the given client");
    }
  }

  @Override
  public void registerInterest(String regionName, Object keyOfInterest, InterestResultPolicy policy,
      boolean isDurable) {
    registerInterest(regionName, keyOfInterest, policy, isDurable, true);
  }

  @Override
  public void registerInterest(String regionName, Object keyOfInterest, InterestResultPolicy policy,
      boolean isDurable, boolean receiveValues) {
    if (keyOfInterest instanceof String && keyOfInterest.equals("ALL_KEYS")) {
      registerInterestRegex(regionName, ".*", isDurable, receiveValues);
    } else if (keyOfInterest instanceof List) {
      if (isPrimary) {
        notifySecondariesAndClient(regionName, keyOfInterest, policy, isDurable, receiveValues,
            InterestType.KEY);
      } else {
        throw new IllegalStateException(
            "This process is not the primary server for the given client");
      }
    } else {
      if (isPrimary) {
        // Notify all secondaries and client of change in interest
        notifySecondariesAndClient(regionName, keyOfInterest, policy, isDurable, receiveValues,
            InterestType.KEY);

        // Enqueue the initial value message for the client if necessary
        if (policy == InterestResultPolicy.KEYS_VALUES) {
          enqueueInitialValue(null, regionName, keyOfInterest);
        }
        // Add the client to the region's filters
        // addFilterRegisteredClients(regionName, keyOfInterest);
      } else {
        throw new IllegalStateException(
            "This process is not the primary server for the given client");
      }
    }
  }

  private void notifySecondariesAndClient(String regionName, Object keyOfInterest,
      InterestResultPolicy policy, boolean isDurable, boolean receiveValues, int interestType) {
    // Create a client interest message for the keyOfInterest
    ClientInterestMessageImpl message = new ClientInterestMessageImpl(
        new EventID(_cache.getDistributedSystem()), regionName, keyOfInterest, interestType,
        policy.getOrdinal(), isDurable, !receiveValues, ClientInterestMessageImpl.REGISTER);

    // Notify all secondary proxies of a change in interest
    notifySecondariesOfInterestChange(message);

    // Modify interest registration
    if (keyOfInterest instanceof List) {
      registerClientInterestList(regionName, (List<?>) keyOfInterest, isDurable, !receiveValues,
          true);
    } else {
      registerClientInterest(regionName, keyOfInterest, interestType, isDurable, !receiveValues,
          true);
    }

    // Enqueue the interest registration message for the client.
    enqueueInterestRegistrationMessage(message);
  }

  private void enqueueInitialValue(ClientInterestMessageImpl clientInterestMessage,
      String regionName, Object keyOfInterest) {
    // Get the initial value
    Get70 request = (Get70) Get70.getCommand();
    LocalRegion lr = (LocalRegion) _cache.getRegion(regionName);
    Get70.Entry entry = request.getValueAndIsObject(lr, keyOfInterest, null, null);
    boolean isObject = entry.isObject;
    byte[] value = null;

    // If the initial value is not null, add it to the client's queue
    if (entry.value != null) {
      if (entry.value instanceof byte[]) {
        value = (byte[]) entry.value;
      } else {
        try {
          value = CacheServerHelper.serialize(entry.value);
        } catch (IOException e) {
          logger.warn(
              String.format("The following exception occurred while attempting to serialize %s",
                  entry.value),
              e);
        }
      }
      VersionTag tag = entry.versionTag;

      // Initialize the event id.
      final EventID eventId;
      if (clientInterestMessage == null) {
        // If the clientInterestMessage is null, create a new event id
        eventId = new EventID(_cache.getDistributedSystem());
      } else {
        // If the clientInterestMessage is not null, base the event id off its event id to fix
        // GEM-794.
        // This will cause the updateMessage created below to have the same event id as the one
        // created
        // in the primary.
        eventId = new EventID(clientInterestMessage.getEventId(), 1);
      }
      ClientUpdateMessage updateMessage =
          new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_CREATE, lr, keyOfInterest, value,
              null, (isObject ? (byte) 0x01 : (byte) 0x00), null, proxyID, eventId, tag);
      CacheClientNotifier.routeSingleClientMessage(updateMessage, proxyID);
    }
  }

  private void enqueueInterestRegistrationMessage(ClientInterestMessageImpl message) {
    _messageDispatcher.enqueueMessage(message);
  }

  @Override
  public void unregisterInterestRegex(String regionName, String regex, boolean isDurable) {
    unregisterInterestRegex(regionName, regex, isDurable, true);
  }

  @Override
  public void unregisterInterestRegex(String regionName, String regex, boolean isDurable,
      boolean receiveValues) {
    if (isPrimary) {
      notifySecondariesAndClient(regionName, regex, isDurable, receiveValues,
          InterestType.REGULAR_EXPRESSION);
    } else {
      throw new IllegalStateException(
          "This process is not the primary server for the given client");
    }
  }

  @Override
  public void unregisterInterest(String regionName, Object keyOfInterest, boolean isDurable) {
    unregisterInterest(regionName, keyOfInterest, isDurable, true);
  }

  @Override
  public void unregisterInterest(String regionName, Object keyOfInterest, boolean isDurable,
      boolean receiveValues) {
    if (keyOfInterest instanceof String && keyOfInterest.equals("ALL_KEYS")) {
      unregisterInterestRegex(regionName, ".*", isDurable, receiveValues);
    } else {
      if (isPrimary) {
        notifySecondariesAndClient(regionName, keyOfInterest, isDurable, receiveValues,
            InterestType.KEY);
      } else {
        throw new IllegalStateException(
            "This process is not the primary server for the given client");
      }
    }
  }

  private void notifySecondariesAndClient(String regionName, Object keyOfInterest,
      boolean isDurable, boolean receiveValues, int interestType) {
    // Notify all secondary proxies of a change in interest
    ClientInterestMessageImpl message = new ClientInterestMessageImpl(
        new EventID(_cache.getDistributedSystem()), regionName, keyOfInterest, interestType,
        (byte) 0, isDurable, !receiveValues, ClientInterestMessageImpl.UNREGISTER);
    notifySecondariesOfInterestChange(message);

    // Modify interest registration
    if (keyOfInterest instanceof List) {
      unregisterClientInterest(regionName, (List<?>) keyOfInterest, false);
    } else {
      unregisterClientInterest(regionName, keyOfInterest, interestType, false);
    }

    // Enqueue the interest unregistration message for the client.
    enqueueInterestRegistrationMessage(message);
  }

  protected void notifySecondariesOfInterestChange(ClientInterestMessageImpl message) {
    if (logger.isDebugEnabled()) {
      StringBuilder subBuffer = new StringBuilder();
      if (message.isRegister()) {
        subBuffer.append("register ").append(message.getIsDurable() ? "" : "non-")
            .append("durable interest in ");
      } else {
        subBuffer.append("unregister interest in ");
      }
      final String buffer = this + ": Notifying secondary proxies to " + subBuffer
          + message.getRegionName() + "->" + message.getKeyOfInterest()
          + "->" + InterestType.getString(message.getInterestType());
      logger.debug(buffer);
    }
    _cacheClientNotifier.deliverInterestChange(proxyID, message);
  }

  /**
   * Registers interest in the input region name and key
   *
   * @param regionName The fully-qualified name of the region in which to register interest
   * @param keyOfInterest The key in which to register interest
   */
  protected void registerClientInterest(String regionName, Object keyOfInterest, int interestType,
      boolean isDurable, boolean sendUpdatesAsInvalidates, boolean flushState) {
    ClientInterestList cil =
        cils[RegisterInterestTracker.getInterestLookupIndex(isDurable, false)];
    cil.registerClientInterest(regionName, keyOfInterest, interestType, sendUpdatesAsInvalidates);
    if (flushState) {
      flushForInterestRegistration(regionName,
          _cache.getDistributedSystem().getDistributedMember());
    }
    HARegionQueue queue = getHARegionQueue();
    if (queue != null) { // queue is null during initialization
      queue.setHasRegisteredInterest(true);
    }
  }

  /**
   * flush other regions to the given target. This is usually the member that is registering the
   * interest. During queue creation it is the queue's image provider.
   */
  public void flushForInterestRegistration(String regionName, DistributedMember target) {
    Region<?, ?> r = _cache.getRegion(regionName);
    if (r == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to find region '{}' to flush for interest registration", regionName);
      }
    } else if (r.getAttributes().getScope().isDistributed()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Flushing region '{}' for interest registration", regionName);
      }
      CacheDistributionAdvisee cd = (CacheDistributionAdvisee) r;
      final StateFlushOperation sfo;
      if (r instanceof PartitionedRegion) {
        // need to flush all buckets. SFO should be changed to target buckets
        // belonging to a particular PR, but it doesn't have that option right now
        sfo = new StateFlushOperation(
            _cache.getInternalDistributedSystem().getDistributionManager());
      } else {
        sfo = new StateFlushOperation((DistributedRegion) r);
      }
      try {
        // bug 41681 - we need to flush any member that may have a cache operation
        // in progress so that the changes are received there before returning
        // from this method
        InitialImageAdvice advice = cd.getCacheDistributionAdvisor().adviseInitialImage(null);
        HashSet<InternalDistributedMember> recips = new HashSet<>(advice.getReplicates());
        recips.addAll(advice.getUninitialized());
        recips.addAll(advice.getEmpties());
        recips.addAll(advice.getPreloaded());
        recips.addAll(advice.getOthers());
        sfo.flush(recips, target, OperationExecutors.HIGH_PRIORITY_EXECUTOR, true);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Unregisters interest in the input region name and key
   *
   * @param regionName The fully-qualified name of the region in which to unregister interest
   * @param keyOfInterest The key in which to unregister interest
   * @param isClosing Whether the caller is closing
   */
  protected void unregisterClientInterest(String regionName, Object keyOfInterest, int interestType,
      boolean isClosing) {
    // only unregister durable interest if isClosing and !keepalive
    if (!isClosing /* explicit unregister */
        || !getDurableKeepAlive() /* close and no keepAlive */) {
      cils[RegisterInterestTracker.durableInterestListIndex]
          .unregisterClientInterest(regionName, keyOfInterest, interestType);
    }
    // always unregister non durable interest
    cils[RegisterInterestTracker.interestListIndex].unregisterClientInterest(regionName,
        keyOfInterest, interestType);
  }

  /**
   * Registers interest in the input region name and list of keys
   *
   * @param regionName The fully-qualified name of the region in which to register interest
   * @param keysOfInterest The list of keys in which to register interest
   */
  protected void registerClientInterestList(String regionName, List<?> keysOfInterest,
      boolean isDurable, boolean sendUpdatesAsInvalidates, boolean flushState) {
    // we only use two interest lists to map the non-durable and durable
    // identifiers to their interest settings
    ClientInterestList cil = cils[RegisterInterestTracker.getInterestLookupIndex(isDurable,
        false/* sendUpdatesAsInvalidates */)];
    cil.registerClientInterestList(regionName, keysOfInterest, sendUpdatesAsInvalidates);
    if (getHARegionQueue() != null) {
      if (flushState) {
        flushForInterestRegistration(regionName,
            _cache.getDistributedSystem().getDistributedMember());
      }
      getHARegionQueue().setHasRegisteredInterest(true);
    }
  }

  /**
   * Unregisters interest in the input region name and list of keys
   *
   * @param regionName The fully-qualified name of the region in which to unregister interest
   * @param keysOfInterest The list of keys in which to unregister interest
   * @param isClosing Whether the caller is closing
   */
  protected void unregisterClientInterest(String regionName, List<?> keysOfInterest,
      boolean isClosing) {
    // only unregister durable interest if isClosing and !keepalive
    if (!isClosing /* explicit unregister */
        || !getDurableKeepAlive() /* close and no keepAlive */) {
      cils[RegisterInterestTracker.durableInterestListIndex]
          .unregisterClientInterestList(regionName, keysOfInterest);
    }
    // always unregister non durable interest
    cils[RegisterInterestTracker.interestListIndex].unregisterClientInterestList(regionName,
        keysOfInterest);
  }


  /** sent by the cache client notifier when there is an interest registration change */
  protected void processInterestMessage(ClientInterestMessageImpl message) {
    // Register or unregister interest depending on the interest type
    int interestType = message.getInterestType();
    String regionName = message.getRegionName();
    Object key = message.getKeyOfInterest();
    if (message.isRegister()) {
      // Register interest in this region->key
      if (key instanceof List) {
        registerClientInterestList(regionName, (List<?>) key, message.getIsDurable(),
            message.getForUpdatesAsInvalidates(), true);
      } else {
        registerClientInterest(regionName, key, interestType, message.getIsDurable(),
            message.getForUpdatesAsInvalidates(), true);
      }

      // Add the client to the region's filters
      // addFilterRegisteredClients(regionName, key);

      if (logger.isDebugEnabled()) {
        final String buffer = this + ": Interest listener registered "
            + (message.getIsDurable() ? "" : "non-") + "durable interest in "
            + message.getRegionName() + "->" + message.getKeyOfInterest()
            + "->" + InterestType.getString(message.getInterestType());
        logger.debug(buffer);
      }
    } else {
      // Unregister interest in this region->key
      if (key instanceof List) {
        unregisterClientInterest(regionName, (List<?>) key, false);
      } else {
        unregisterClientInterest(regionName, key, interestType, false);
      }

      if (logger.isDebugEnabled()) {
        final String buffer = this + ": Interest listener unregistered interest in "
            + message.getRegionName() + "->" + message.getKeyOfInterest()
            + "->" + InterestType.getString(message.getInterestType());
        logger.debug(buffer);
      }
    }

    // Enqueue the interest message in this secondary proxy (fix for bug #52088)
    enqueueInterestRegistrationMessage(message);

    // Enqueue the initial value if the message is register on a key that is not a list (fix for bug
    // #52088)
    if (message.isRegister() && message.getInterestType() == InterestType.KEY
        && !(key instanceof List) && InterestResultPolicy
            .fromOrdinal(message.getInterestResultPolicy()) == InterestResultPolicy.KEYS_VALUES) {
      enqueueInitialValue(message, regionName, key);
    }
  }

  private boolean postDeliverAuthCheckPassed(ClientUpdateMessage clientMessage) {
    // Before adding it in the queue for dispatching, check for post
    // process authorization
    if (AcceptorImpl.isAuthenticationRequired() && postAuthzCallback == null
        && AcceptorImpl.isPostAuthzCallbackPresent()) {
      // security is on and callback is null: it means multiuser mode.
      ClientUpdateMessageImpl cumi = (ClientUpdateMessageImpl) clientMessage;

      CqNameToOp clientCq = cumi.getClientCq(proxyID);

      if (clientCq != null && !clientCq.isEmpty()) {
        if (logger.isDebugEnabled()) {
          logger.debug("CCP clientCq size before processing auth {}", clientCq.size());
        }
        String[] regionNameHolder = new String[1];
        OperationContext opctxt = getOperationContext(clientMessage, regionNameHolder);
        if (opctxt == null) {
          logger.warn(
              "{}: Not Adding message to queue: {} because the operation context object could not be obtained for this client message.",
              this, clientMessage);
          return false;
        }

        String[] cqNames = clientCq.getNames();
        if (logger.isDebugEnabled()) {
          logger.debug("CCP clientCq names array size {}", cqNames.length);
        }
        for (final String cqName : cqNames) {
          try {
            if (logger.isDebugEnabled()) {
              logger.debug("CCP clientCq name {}", cqName);
            }
            boolean isAuthorized = false;

            if (proxyID.isDurable() && getDurableKeepAlive() && _isPaused) {
              // need to take lock as we may be reinitializing proxy cache
              synchronized (clientUserAuthsLock) {
                AuthorizeRequestPP postAuthCallback =
                    clientUserAuths.getUserAuthAttributes(cqName).getPostAuthzRequest();
                if (logger.isDebugEnabled() && postAuthCallback == null) {
                  logger.debug("CCP clientCq post callback is null");
                }
                if (postAuthCallback != null && postAuthCallback.getPostAuthzCallback()
                    .authorizeOperation(regionNameHolder[0], opctxt)) {
                  isAuthorized = true;
                }
              }
            } else {
              UserAuthAttributes userAuthAttributes =
                  clientUserAuths.getUserAuthAttributes(cqName);

              AuthorizeRequestPP postAuthCallback = userAuthAttributes.getPostAuthzRequest();
              if (postAuthCallback == null && logger.isDebugEnabled()) {
                logger.debug("CCP clientCq post callback is null");
              }
              if (postAuthCallback != null && postAuthCallback.getPostAuthzCallback()
                  .authorizeOperation(regionNameHolder[0], opctxt)) {
                isAuthorized = true;
              }
            }

            if (!isAuthorized) {
              logger.warn("{}: Not Adding CQ message to queue {} because authorization failed.",
                  this, clientMessage);
              clientCq.delete(cqName);
            }
          } catch (Exception ex) {
            // ignore...
          }
          if (logger.isDebugEnabled()) {
            logger.debug("CCP clientCq size after processing auth {}", clientCq.size());
          }
        }
        // again need to check as there may be no CQ available
        if (!clientMessage.hasCqs(proxyID)) {
          _statistics.incMessagesNotQueuedNotInterested();
          if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
            logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE,
                "{}: Not adding message to queue. It is not interested in this region and key: {}",
                this, clientMessage);
          }
          return false;
        }
      }
    } else if (postAuthzCallback != null) {
      String[] regionNameHolder = new String[1];
      OperationContext opctxt = getOperationContext(clientMessage, regionNameHolder);
      if (opctxt == null) {
        logger.warn(
            "{}: Not Adding message to queue: {} because the operation context object could not be obtained for this client message.",
            this, clientMessage);
        return false;
      }
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Invoking authorizeOperation for message: {}", this, clientMessage);
      }

      final boolean isAuthorize;
      if (proxyID.isDurable() && getDurableKeepAlive() && _isPaused) {
        synchronized (clientUserAuthsLock) {
          isAuthorize = postAuthzCallback.authorizeOperation(regionNameHolder[0], opctxt);
        }
      } else {
        isAuthorize = postAuthzCallback.authorizeOperation(regionNameHolder[0], opctxt);
      }
      if (!isAuthorize) {
        logger.warn("{}: Not Adding message to queue {} because authorization failed.",
            this, clientMessage);
        return false;
      }
    }

    return true;
  }

  /**
   * Delivers the message to the client representing this client proxy.
   *
   */
  protected void deliverMessage(Conflatable conflatable) {
    final ClientUpdateMessage clientMessage;
    if (conflatable instanceof HAEventWrapper) {
      clientMessage = ((HAEventWrapper) conflatable).getClientUpdateMessage();
    } else {
      clientMessage = (ClientUpdateMessage) conflatable;
    }

    _statistics.incMessagesReceived();

    // post process for single-user mode. We don't do post process for multi-user mode
    if (subject != null) {
      ThreadState state = securityService.bindSubject(subject);
      try {
        if (securityService.needPostProcess()) {
          Object oldValue = clientMessage.getValue();
          Object newValue = securityService.postProcess(clientMessage.getRegionName(),
              clientMessage.getKeyOfInterest(), oldValue, clientMessage.valueIsObject());
          clientMessage.setLatestValue(newValue);
        }
      } finally {
        if (state != null) {
          state.clear();
        }
      }
    }

    if (clientMessage.needsNoAuthorizationCheck() || postDeliverAuthCheckPassed(clientMessage)) {
      if (_messageDispatcher != null) {
        _messageDispatcher.enqueueMessage(conflatable);
      } else {
        _statistics.incMessagesFailedQueued();

        if (logger.isDebugEnabled()) {
          logger.debug(
              "Message was not added to the queue. Message dispatcher was null for proxy: " + this
                  + ". Event ID hash code: " + conflatable.hashCode() + "; System ID hash code: "
                  + System.identityHashCode(conflatable) + "; Conflatable details: " + conflatable);
        }
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Message was not added to the queue. Event ID hash code: " + conflatable.hashCode()
                + "; System ID hash code: "
                + System.identityHashCode(conflatable) + "; Conflatable details: " + conflatable);
      }

      _statistics.incMessagesFailedQueued();
    }
  }

  protected void sendMessageDirectly(ClientMessage message) {
    // Send the message directly if the connection exists
    // (do not go through the queue).
    if (logger.isDebugEnabled()) {
      logger.debug("About to send message directly to {}", this);
    }
    if (_messageDispatcher != null && _socket != null && !_socket.isClosed()) {
      // If the socket is open, send the message to it
      _messageDispatcher.sendMessageDirectly(message);
      if (logger.isDebugEnabled()) {
        logger.debug("Sent message directly to {}", this);
      }
    } else {
      // Otherwise just reset the ping counter
      resetPingCounter();
      if (logger.isDebugEnabled()) {
        logger.debug("Skipped sending message directly to {}", this);
      }
    }
  }

  private OperationContext getOperationContext(ClientMessage cmsg, String[] regionNameHolder) {
    ClientUpdateMessageImpl cmsgimpl = (ClientUpdateMessageImpl) cmsg;
    OperationContext opctxt = null;
    // TODO SW: Special handling for DynamicRegions; this should be reworked
    // when DynamicRegion API is deprecated
    String regionName = cmsgimpl.getRegionName();
    regionNameHolder[0] = regionName;
    if (cmsgimpl.isCreate()) {
      if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
        regionNameHolder[0] = (String) cmsgimpl.getKeyOfInterest();
        opctxt = new RegionCreateOperationContext(true);
      } else {
        PutOperationContext tmp = new PutOperationContext(cmsgimpl.getKeyOfInterest(),
            cmsgimpl.getValue(), cmsgimpl.valueIsObject(), PutOperationContext.CREATE, true);
        tmp.setCallbackArg(cmsgimpl.getCallbackArgument());
        opctxt = tmp;
      }
    } else if (cmsgimpl.isUpdate()) {
      if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
        regionNameHolder[0] = (String) cmsgimpl.getKeyOfInterest();
        opctxt = new RegionCreateOperationContext(true);
      } else {
        PutOperationContext tmp = new PutOperationContext(cmsgimpl.getKeyOfInterest(),
            cmsgimpl.getValue(), cmsgimpl.valueIsObject(), PutOperationContext.UPDATE, true);
        tmp.setCallbackArg(cmsgimpl.getCallbackArgument());
        opctxt = tmp;
      }
    } else if (cmsgimpl.isDestroy()) {
      if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
        regionNameHolder[0] = (String) cmsgimpl.getKeyOfInterest();
        opctxt = new RegionDestroyOperationContext(true);
      } else {
        DestroyOperationContext tmp =
            new DestroyOperationContext(cmsgimpl.getKeyOfInterest(), true);
        tmp.setCallbackArg(cmsgimpl.getCallbackArgument());
        opctxt = tmp;
      }
    } else if (cmsgimpl.isDestroyRegion()) {
      opctxt = new RegionDestroyOperationContext(true);
    } else if (cmsgimpl.isInvalidate()) {
      InvalidateOperationContext tmp =
          new InvalidateOperationContext(cmsgimpl.getKeyOfInterest(), true);
      tmp.setCallbackArg(cmsgimpl.getCallbackArgument());
      opctxt = tmp;
    } else if (cmsgimpl.isClearRegion()) {
      RegionClearOperationContext tmp = new RegionClearOperationContext(true);
      tmp.setCallbackArg(cmsgimpl.getCallbackArgument());
      opctxt = tmp;
    }
    return opctxt;
  }

  /**
   * Initializes the message dispatcher thread. The <code>MessageDispatcher</code> processes the
   * message queue.
   *
   */
  public void initializeMessageDispatcher() throws CacheException {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Initializing message dispatcher with capacity of {} entries", this,
            _maximumMessageCount);
      }
      String name = "Client Message Dispatcher for " + getProxyID().getDistributedMember()
          + (isDurable() ? " (" + getDurableId() + ")" : "");
      _messageDispatcher = createMessageDispatcher(name);
    } catch (final Exception ex) {
      _statistics.close();
      throw ex;
    }
  }

  MessageDispatcher createMessageDispatcher(String name) {
    return messageDispatcherFactory.create(this, name, statisticsClock);
  }

  protected void startOrResumeMessageDispatcher(boolean processedMarker) {
    // Only start or resume the dispatcher if it is Primary
    if (isPrimary) {
      // Add the marker to the queue
      if (!processedMarker) {
        EventID eventId = new EventID(_cache.getDistributedSystem());
        _messageDispatcher.enqueueMarker(new ClientMarkerMessageImpl(eventId));
      }

      // Set the message queue to primary.
      _messageDispatcher._messageQueue.setPrimary(true);

      // Start or resume the dispatcher
      synchronized (_messageDispatcher._pausedLock) {
        if (isPaused()) {
          // It is paused, resume it
          setPaused(false);
          if (_messageDispatcher.isStopped()) {
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Starting dispatcher", this);
            }
            _messageDispatcher.start();
          } else {
            // ARB: Initialize transient fields.
            _messageDispatcher.initializeTransients();
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Resuming dispatcher", this);
            }
            _messageDispatcher.resumeDispatching();
          }
        } else if (!_messageDispatcher.isAlive()) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Starting dispatcher", this);
          }
          _messageDispatcher.start();
        }
      }
    }
  }

  /*
   * Returns whether the client represented by this <code> CacheClientProxy </code> has registered
   * interest in anything. @return whether the client represented by this <code> CacheClientProxy
   * </code> has registered interest in anything
   */
  protected boolean hasRegisteredInterested() {
    return cils[RegisterInterestTracker.interestListIndex].hasInterest()
        || cils[RegisterInterestTracker.durableInterestListIndex].hasInterest();
  }

  /**
   * Returns a string representation of the proxy
   */
  @Override
  public String toString() {
    return "CacheClientProxy["
        + proxyID
        + "; port=" + _socket.getPort() + "; primary=" + isPrimary
        + "; version=" + clientVersion + "]";
  }

  public String getState() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("CacheClientProxy[")
        .append(proxyID)
        .append("; port=").append(_socket.getPort()).append("; primary=").append(isPrimary)
        .append("; version=").append(clientVersion).append("; paused=").append(isPaused())
        .append("; alive=").append(isAlive()).append("; connected=").append(isConnected())
        .append("; isMarkedForRemoval=").append(isMarkedForRemoval).append("]");

    if (_messageDispatcher != null && isAlive()) {
      buffer.append(LogWriterImpl.getStackTrace(_messageDispatcher));
    }

    return buffer.toString();
  }

  @Override
  public boolean isPrimary() {
    return isPrimary;
  }

  protected void setPrimary(boolean isPrimary) {
    this.isPrimary = isPrimary;
  }

  /*
   * Return this client's HA region queue
   *
   * @returns - HARegionQueue of the client
   */
  public HARegionQueue getHARegionQueue() {
    if (_messageDispatcher != null) {
      return _messageDispatcher._messageQueue;
    }
    return null;
  }


  /**
   * Reinitialize a durable <code>CacheClientProxy</code> with a new client.
   *
   * @param socket The socket between the server and the client
   * @param ip whether this proxy represents the primary
   */
  protected void reinitialize(Socket socket, ClientProxyMembershipID proxyId, boolean ip, byte cc,
      KnownVersion ver) {
    // Re-initialize transient fields
    initializeTransientFields(socket, proxyId, ip, cc, ver);
    getCacheClientNotifier().getAcceptorStats().incCurrentQueueConnections();


    // Cancel expiration task
    cancelDurableExpirationTask(true);

    // Set the message dispatcher's primary flag. This could go from primary
    // to secondary
    _messageDispatcher._messageQueue.setPrimary(ip);
    _messageDispatcher._messageQueue.setClientConflation(cc);

    // Reset the _socketClosed AtomicBoolean
    _socketClosed.compareAndSet(true, false);

    reinitializeClientAuths();
    creationDate = new Date();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Has been reinitialized", this);
    }
  }

  public boolean isDurable() {
    return getProxyID().isDurable();
  }

  public String getDurableId() {
    return getProxyID().getDurableId();
  }

  public int getDurableTimeout() {
    return getProxyID().getDurableTimeout();
  }

  private boolean getDurableKeepAlive() {
    return keepalive;
  }

  public String getHARegionName() {
    return getProxyID().getHARegionName();
  }

  public Region<?, ?> getHARegion() {
    return _messageDispatcher._messageQueue.getRegion();
  }

  public KnownVersion getVersion() {
    return clientVersion;
  }

  @VisibleForTesting
  protected Subject getSubject() {
    return subject;
  }

  protected void scheduleDurableExpirationTask() {
    SystemTimer.SystemTimerTask task = new SystemTimer.SystemTimerTask() {
      @Override
      public void run2() {
        _durableExpirationTask.compareAndSet(this, null);
        logger.warn("{} : The expiration task has fired, so this proxy is being terminated.",
            CacheClientProxy.this);
        // Remove the proxy from the CacheClientNofier's registry
        getCacheClientNotifier().removeClientProxy(CacheClientProxy.this);
        getCacheClientNotifier().durableClientTimedOut(proxyID);

        // Close the proxy
        terminateDispatching(false);
        _cacheClientNotifier.statistics.incQueueDroppedCount();

        /*
         * Setting the expiration task to null again and cancelling existing one, if any.
         * <p/>
         * The message dispatcher may again set the expiry task in below path: <code>
         * org.apache.geode.internal.cache.tier.sockets.CacheClientProxy.
         * scheduleDurableExpirationTask(CacheClientProxy.java:2020)
         * org.apache.geode.internal.cache.tier.sockets.CacheClientProxy.pauseDispatching(
         * CacheClientProxy.java:924)
         * org.apache.geode.internal.cache.tier.sockets.CacheClientProxy$MessageDispatcher.
         * pauseOrUnregisterProxy(CacheClientProxy.java:2813)
         * org.apache.geode.internal.cache.tier.sockets.CacheClientProxy$MessageDispatcher.run(
         * CacheClientProxy.java:2692)
         * </code>
         * <p/>
         * This is because message dispatcher may get an IOException with "Proxy closing due to
         * socket being closed locally" during/after terminateDispatching(false) above.
         */
        SystemTimerTask task = _durableExpirationTask.getAndSet(null);
        if (task != null) {
          if (task.cancel()) {
            _cache.purgeCCPTimer();
          }
        }
      }

    };
    if (_durableExpirationTask.compareAndSet(null, task)) {
      _cache.getCCPTimer().schedule(task, getDurableTimeout() * 1000L);
    }
  }

  protected void cancelDurableExpirationTask(boolean logMessage) {
    SystemTimer.SystemTimerTask task = _durableExpirationTask.getAndSet(null);
    if (task != null) {
      if (logMessage) {
        logger.info("{}: Cancelling expiration task since the client has reconnected.",
            this);
      }
      if (task.cancel()) {
        _cache.purgeCCPTimer();
      }
    }
  }


  /**
   * Returns the current number of CQS the client installed.
   *
   * @return int the current count of CQs for this client
   */
  public int getCqCount() {
    synchronized (this) {
      return _statistics.getCqCount();
    }
  }

  /**
   * Increment the number of CQs the client installed
   *
   */
  public void incCqCount() {
    synchronized (this) {
      _statistics.incCqCount();
    }
  }

  /**
   * Decrement the number of CQs the client installed
   *
   */
  public synchronized void decCqCount() {
    synchronized (this) {
      _statistics.decCqCount();
    }
  }

  /**
   * Returns true if the client has one CQ
   *
   * @return true if the client has one CQ
   */
  public boolean hasOneCq() {
    synchronized (this) {
      return _statistics.getCqCount() == 1;
    }
  }

  /**
   * Returns true if the client has no CQs
   *
   * @return true if the client has no CQs
   */
  public boolean hasNoCq() {
    synchronized (this) {
      return _statistics.getCqCount() == 0;
    }
  }

  /**
   * Get map of regions with empty data policy
   *
   * @since GemFire 6.1
   */
  public Map<String, Integer> getRegionsWithEmptyDataPolicy() {
    return regionsWithEmptyDataPolicy;
  }

  public int incrementAndGetPingCounter() {
    return pingCounter.incrementAndGet();
  }

  public void resetPingCounter() {
    pingCounter.set(0);
  }

  /**
   * Returns the number of seconds that have elapsed since the Client proxy created.
   *
   * @since GemFire 7.0
   */
  public long getUpTime() {
    return (System.currentTimeMillis() - creationDate.getTime()) / 1000;
  }

  public interface TestHook {
    void doTestHook(String spot);
  }

  @MutableForTesting
  public static TestHook testHook;

  @FunctionalInterface
  @VisibleForTesting
  interface CacheClientProxyStatsFactory {

    CacheClientProxyStats create(StatisticsFactory statisticsFactory,
        ClientProxyMembershipID proxyID, String remoteHostAddress);
  }

  @FunctionalInterface
  @VisibleForTesting
  public interface MessageDispatcherFactory {

    MessageDispatcher create(CacheClientProxy proxy, String name, StatisticsClock statisticsClock);
  }
}
