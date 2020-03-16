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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.geode.cache.Cache;
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
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl.CqNameToOp;
import org.apache.geode.internal.cache.tier.sockets.command.Get70;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.Version;
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

  protected final AtomicReference _durableExpirationTask = new AtomicReference();

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
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "MAXIMUM_SHUTDOWN_PEEKS", 50).intValue();

  /**
   * The number of milliseconds to wait for an offering to the message queue
   */
  protected static final int MESSAGE_OFFER_TIME = 0;

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
   * Defaults to true; meaning do some logging of dropped client notification messages. Set the
   * system property to true to cause dropped messages to NOT be logged.
   */
  protected static final boolean LOG_DROPPED_MSGS =
      !Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "disableNotificationWarnings");

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

  private AccessControl postAuthzCallback;
  private Subject subject;

  /**
   * For multiuser environment..
   */
  private ClientUserAuths clientUserAuths;

  private final Object clientUserAuthsLock = new Object();

  /**
   * The version of the client
   */
  private Version clientVersion;

  /**
   * A map of region name as key and integer as its value. Basically, it stores the names of the
   * regions with <code>DataPolicy</code> as EMPTY. If an event's region name is present in this
   * map, it's full value (and not delta) is sent to the client represented by this proxy.
   *
   * @since GemFire 6.1
   */
  private volatile Map regionsWithEmptyDataPolicy = new HashMap();

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
      Version clientVersion, long acceptorId, boolean notifyBySubscription,
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
      Version clientVersion, long acceptorId, boolean notifyBySubscription,
      SecurityService securityService, Subject subject, StatisticsClock statisticsClock,
      StatisticsFactory statisticsFactory,
      CacheClientProxyStatsFactory cacheClientProxyStatsFactory,
      MessageDispatcherFactory messageDispatcherFactory)
      throws CacheException {
    initializeTransientFields(socket, proxyID, isPrimary, clientConflation, clientVersion);
    this._cacheClientNotifier = ccn;
    this._cache = cache;
    this.securityService = securityService;
    this._maximumMessageCount = ccn.getMaximumMessageCount();
    this._messageTimeToLive = ccn.getMessageTimeToLive();
    this._acceptorId = acceptorId;
    this.notifyBySubscription = notifyBySubscription;
    this.statisticsClock = statisticsClock;
    this._statistics =
        cacheClientProxyStatsFactory.create(statisticsFactory, proxyID, _remoteHostAddress);
    this.subject = subject;

    // Create the interest list
    this.cils[RegisterInterestTracker.interestListIndex] =
        new ClientInterestList(this, this.proxyID);
    // Create the durable interest list
    this.cils[RegisterInterestTracker.durableInterestListIndex] =
        new ClientInterestList(this, this.getDurableId());
    this.postAuthzCallback = null;
    this._cacheClientNotifier.getAcceptorStats().incCurrentQueueConnections();
    this.creationDate = new Date();
    this.messageDispatcherFactory = messageDispatcherFactory;
    initializeClientAuths();
  }

  Version getClientVersion() {
    return clientVersion;
  }

  private void initializeClientAuths() {
    if (AcceptorImpl.isPostAuthzCallbackPresent())
      this.clientUserAuths = ServerConnection.getClientUserAuths(this.proxyID);
  }

  private void reinitializeClientAuths() {
    if (this.clientUserAuths != null && AcceptorImpl.isPostAuthzCallbackPresent()) {
      synchronized (this.clientUserAuthsLock) {
        ClientUserAuths newClientAuth = ServerConnection.getClientUserAuths(this.proxyID);
        newClientAuth.fillPreviousCQAuth(this.clientUserAuths);
        this.clientUserAuths = newClientAuth;
      }
    }
  }

  public void setPostAuthzCallback(AccessControl authzCallback) {
    // TODO:hitesh synchronization
    synchronized (this.clientUserAuthsLock) {
      if (this.postAuthzCallback != null)
        this.postAuthzCallback.close();
      this.postAuthzCallback = authzCallback;
    }
  }

  public void setSubject(Subject subject) {
    // TODO:hitesh synchronization
    synchronized (this.clientUserAuthsLock) {
      if (this.subject != null) {
        this.subject.logout();
      }
      this.subject = subject;
    }
  }

  public void setCQVsUserAuth(String cqName, long uniqueId, boolean isDurable) {
    if (postAuthzCallback == null) // only for multiuser
    {
      if (this.clientUserAuths != null)
        this.clientUserAuths.setUserAuthAttributesForCq(cqName, uniqueId, isDurable);
    }
  }

  private void initializeTransientFields(Socket socket, ClientProxyMembershipID pid, boolean ip,
      byte cc, Version vers) {
    this._socket = socket;
    this.proxyID = pid;
    this.connected = true;
    {
      int bufSize = 1024;
      try {
        bufSize = _socket.getSendBufferSize();
        if (bufSize < 1024) {
          bufSize = 1024;
        }
      } catch (SocketException ignore) {
      }
      this._commBuffer = ServerConnection.allocateCommBuffer(bufSize, socket);
    }
    this._remoteHostAddress = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
    this.isPrimary = ip;
    this.clientConflation = cc;
    this.clientVersion = vers;
  }

  public boolean isMarkerEnqueued() {
    return markerEnqueued;
  }

  public void setMarkerEnqueued(boolean bool) {
    markerEnqueued = bool;
  }

  public long getAcceptorId() {
    return this._acceptorId;
  }

  /**
   * @return the notifyBySubscription
   */
  public boolean isNotifyBySubscription() {
    return this.notifyBySubscription;
  }


  /**
   * Returns the DistributedMember represented by this proxy
   */
  public ClientProxyMembershipID getProxyID() {
    return this.proxyID;
  }

  protected boolean isMember(ClientProxyMembershipID memberId) {
    return this.proxyID.equals(memberId);
  }

  protected boolean isSameDSMember(ClientProxyMembershipID memberId) {
    return this.proxyID.isSameDSMember(memberId);
  }

  /**
   * Set the queue keepalive option
   *
   * @param option whether to keep the durable client's queue alive
   */
  protected void setKeepAlive(boolean option) {
    this.keepalive = option;
  }

  /**
   * Returns the socket between the server and the client
   *
   * @return the socket between the server and the client
   */
  protected Socket getSocket() {
    return this._socket;
  }

  public String getSocketHost() {
    return this._socket.getInetAddress().getHostAddress();
  }

  protected ByteBuffer getCommBuffer() {
    return this._commBuffer;
  }

  /**
   * Returns the remote host's IP address string
   *
   * @return the remote host's IP address string
   */
  protected String getRemoteHostAddress() {
    return this._remoteHostAddress;
  }

  /**
   * Returns the remote host's port
   *
   * @return the remote host's port
   */
  public int getRemotePort() {
    return this._socket.getPort();
  }

  /**
   * Returns whether the proxy is connected to a remote client
   *
   * @return whether the proxy is connected to a remote client
   */
  public boolean isConnected() {
    return this.connected;
  }

  /**
   * Mark the receiver as needing removal
   *
   * @return true if it was already marked for removal
   */
  protected boolean startRemoval() {
    boolean result;
    synchronized (this.isMarkedForRemovalLock) {
      result = this.isMarkedForRemoval;
      this.isMarkedForRemoval = true;
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
    synchronized (this.isMarkedForRemovalLock) {
      result = this.isMarkedForRemoval;
      boolean interrupted = false;
      try {
        while (this.isMarkedForRemoval) {
          if (logger.isDebugEnabled()) {
            logger.debug("Waiting for CacheClientProxy removal: {}", this);
          }
          try {
            this.isMarkedForRemovalLock.wait();
          } catch (InterruptedException e) {
            interrupted = true;
            this._cache.getCancelCriterion().checkCancelInProgress(e);
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
    synchronized (this.isMarkedForRemovalLock) {
      this.isMarkedForRemoval = false;
      this.isMarkedForRemovalLock.notifyAll();
    }
  }

  /**
   * Returns the GemFire cache
   *
   * @return the GemFire cache
   */
  public InternalCache getCache() {
    return this._cache;
  }

  public Set<String> getInterestRegisteredRegions() {
    HashSet<String> regions = new HashSet<String>();
    for (int i = 0; i < this.cils.length; i++) {
      if (!this.cils[i].regions.isEmpty()) {
        regions.addAll(this.cils[i].regions);
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
    return this._statistics;
  }

  /**
   * Returns this proxy's <code>CacheClientNotifier</code>.
   *
   * @return this proxy's <code>CacheClientNotifier</code>
   */
  protected CacheClientNotifier getCacheClientNotifier() {
    return this._cacheClientNotifier;
  }

  /**
   * Returns the size of the queue for heuristic purposes. This size may be changing concurrently if
   * puts/gets are occurring at the same time.
   */
  public int getQueueSize() {
    return this._messageDispatcher == null ? 0 : this._messageDispatcher.getQueueSize();
  }

  /**
   * returns the queue size calculated through stats
   */
  public int getQueueSizeStat() {
    return this._messageDispatcher == null ? 0 : this._messageDispatcher.getQueueSizeStat();
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
              cqService.getCq(cqService.constructServerCqName(clientCQName, this.proxyID));
          // close and drain
          if (cqToClose != null) {
            cqService.closeCq(clientCQName, this.proxyID);
            this._messageDispatcher.drainClientCqEvents(this.proxyID, cqToClose);
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
    if (this._messageDispatcher == null) {
      return false;
    }
    return !this._messageDispatcher.isStopped();
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
    return this._isPaused;
  }

  protected void setPaused(boolean isPaused) {
    this._isPaused = isPaused;
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
    boolean pauseDurable = false;
    // If the client is durable and either (a) it hasn't stopped normally or (b) it
    // has stopped normally but it is configured to be kept alive, set pauseDurable
    // to true
    if (isDurable() && (!stoppedNormally || (getDurableKeepAlive() && stoppedNormally))) {
      pauseDurable = true;
    }

    boolean keepProxy = false;
    if (pauseDurable) {
      pauseDispatching();
      keepProxy = true;
    } else {
      terminateDispatching(checkQueue);
      closeTransientFields();
    }

    this.connected = false;

    // Close the Authorization callback (if any)
    try {
      if (!pauseDurable) {
        if (this.postAuthzCallback != null) {// for single user
          this.postAuthzCallback.close();
          this.postAuthzCallback = null;
        } else if (this.clientUserAuths != null) {// for multiple users
          this.clientUserAuths.cleanup(true);
          this.clientUserAuths = null;
        }
      }
    } catch (Exception ex) {
      if (this._cache.getSecurityLogger().warningEnabled()) {
        this._cache.getSecurityLogger().warning(String.format("%s : %s",
            new Object[] {this, ex}));
      }
    }
    // Notify the caller whether to keep this proxy. If the proxy is durable
    // and should be paused, then return true; otherwise return false.
    return keepProxy;
  }

  protected void pauseDispatching() {
    if (this._messageDispatcher == null) {
      return;
    }

    // If this is the primary, pause the dispatcher (which closes its transient
    // fields. Otherwise, just close the transient fields.
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Pausing processing", this);
    }
    // BUGFIX for BUG#38234
    if (!testAndSetPaused(true) && this.isPrimary) {
      if (this._messageDispatcher != Thread.currentThread()) {
        // don't interrupt ourself to fix bug 40611
        this._messageDispatcher.interrupt();
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

    synchronized (this._messageDispatcher._pausedLock) {
      if (this._isPaused != newValue) {
        this._isPaused = newValue;
        this._messageDispatcher._pausedLock.notifyAll();
        return !this._isPaused;
      } else {
        this._messageDispatcher._pausedLock.notifyAll();
        return this._isPaused;
      }
    }
  }

  protected void terminateDispatching(boolean checkQueue) {
    if (this._messageDispatcher == null) {
      return;
    }

    boolean closedSocket = false;
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Terminating processing", this);
      }
      if (this._messageDispatcher == Thread.currentThread()) {
        // I'm not even sure this is possible but if the dispatcher
        // calls us then at least call stopDispatching
        // the old code did this (I'm not even sure it is safe to do).
        // This needs to be done without testing OR setting "closing".
        this._messageDispatcher.stopDispatching(checkQueue);
        this.cils[RegisterInterestTracker.interestListIndex].clearClientInterestList();
        this.cils[RegisterInterestTracker.durableInterestListIndex].clearClientInterestList();
        // VJR: bug 37487 fix
        destroyRQ();
        return;
      }

      if (!this.closing.compareAndSet(false, true)) {
        // must already be closing so just return
        // this is part of the fix for 37684
        return;
      }
      // Unregister interest in all interests (if necessary)
      this.cils[RegisterInterestTracker.interestListIndex].clearClientInterestList();
      this.cils[RegisterInterestTracker.durableInterestListIndex].clearClientInterestList();

      // If the message dispatcher is paused, unpause it. The next bit of
      // code will interrupt the waiter.
      if (this.testAndSetPaused(false)) {
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
        this._messageDispatcher.stopDispatching(checkQueue);

        gotInterrupt |= Thread.interrupted(); // clears the flag

        // to fix bug 37684
        // 1. check to see if dispatcher is still alive
        if (this._messageDispatcher.isAlive()) {
          closedSocket = closeSocket();
          destroyRQ();
          alreadyDestroyed = true;
          this._messageDispatcher.interrupt();
          if (this._messageDispatcher.isAlive()) {
            try {
              this._messageDispatcher.join(1000);
            } catch (InterruptedException ex) {
              gotInterrupt = true;
            }
            // if it is still alive then warn and move on
            if (this._messageDispatcher.isAlive()) {
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
      this._statistics.close(); // fix for bug 40105
      if (closedSocket) {
        closeOtherTransientFields();
      } else {
        closeTransientFields(); // make sure this happens
      }
    }
  }

  private boolean closeSocket() {
    String remoteHostAddress = this._remoteHostAddress;
    if (this._socketClosed.compareAndSet(false, true) && remoteHostAddress != null) {
      // Only one thread is expected to close the socket
      this._cacheClientNotifier.getSocketCloser().asyncClose(this._socket, remoteHostAddress, null);
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
      String remoteHostAddress = this._remoteHostAddress;
      if (remoteHostAddress != null) {
        this._cacheClientNotifier.getSocketCloser().releaseResourcesForAddress(remoteHostAddress);
        this._remoteHostAddress = null;
      }
    }
    try {
      this.cils[RegisterInterestTracker.interestListIndex].clearClientInterestList();
    } catch (CancelException e) {
      // ignore if cache is shutting down
    }
    // Commented to fix bug 40259
    // this.clientVersion = null;
    closeNonDurableCqs();

    // Logout the subject
    if (this.subject != null) {
      this.subject.logout();
    }
  }

  private void releaseCommBuffer() {
    ByteBuffer bb = this._commBuffer;
    if (bb != null) {
      this._commBuffer = null;
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
    if (this._messageDispatcher == null) {
      return;
    }
    try {
      // Using Destroy Region bcoz this method is modified in HARegion so as
      // not to distribute.
      // For normal Regions , even the localDestroyRegion actually propagates
      HARegionQueue rq = this._messageDispatcher._messageQueue;
      rq.destroy();

      // if (!rq.getRegion().isDestroyed()) {
      // rq.getRegion().destroyRegion();
      // }
    } catch (RegionDestroyedException rde) {
      // throw rde;
    } catch (CancelException e) {
      // throw e;
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
    if (this.isPrimary) {
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
      if (this.isPrimary) {
        notifySecondariesAndClient(regionName, keyOfInterest, policy, isDurable, receiveValues,
            InterestType.KEY);
      } else {
        throw new IllegalStateException(
            "This process is not the primary server for the given client");
      }
    } else {
      if (this.isPrimary) {
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
        new EventID(this._cache.getDistributedSystem()), regionName, keyOfInterest, interestType,
        policy.getOrdinal(), isDurable, !receiveValues, ClientInterestMessageImpl.REGISTER);

    // Notify all secondary proxies of a change in interest
    notifySecondariesOfInterestChange(message);

    // Modify interest registration
    if (keyOfInterest instanceof List) {
      registerClientInterestList(regionName, (List) keyOfInterest, isDurable, !receiveValues, true);
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
    LocalRegion lr = (LocalRegion) this._cache.getRegion(regionName);
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
      EventID eventId = null;
      if (clientInterestMessage == null) {
        // If the clientInterestMessage is null, create a new event id
        eventId = new EventID(this._cache.getDistributedSystem());
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
              null, (isObject ? (byte) 0x01 : (byte) 0x00), null, this.proxyID, eventId, tag);
      CacheClientNotifier.routeSingleClientMessage(updateMessage, this.proxyID);
    }
  }

  private void enqueueInterestRegistrationMessage(ClientInterestMessageImpl message) {
    // Enqueue the interest registration message for the client.
    // If the client is not 7.0.1 or greater and the key of interest is a list,
    // then create an individual message for each entry in the list since the
    // client doesn't support a ClientInterestMessageImpl containing a list.
    if (Version.GFE_701.compareTo(this.clientVersion) > 0
        && message.getKeyOfInterest() instanceof List) {
      for (Iterator i = ((List) message.getKeyOfInterest()).iterator(); i.hasNext();) {
        this._messageDispatcher.enqueueMessage(
            new ClientInterestMessageImpl(getCache().getDistributedSystem(), message, i.next()));
      }
    } else {
      this._messageDispatcher.enqueueMessage(message);
    }
  }

  @Override
  public void unregisterInterestRegex(String regionName, String regex, boolean isDurable) {
    unregisterInterestRegex(regionName, regex, isDurable, true);
  }

  @Override
  public void unregisterInterestRegex(String regionName, String regex, boolean isDurable,
      boolean receiveValues) {
    if (this.isPrimary) {
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
      if (this.isPrimary) {
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
        new EventID(this._cache.getDistributedSystem()), regionName, keyOfInterest, interestType,
        (byte) 0, isDurable, !receiveValues, ClientInterestMessageImpl.UNREGISTER);
    notifySecondariesOfInterestChange(message);

    // Modify interest registration
    if (keyOfInterest instanceof List) {
      unregisterClientInterest(regionName, (List) keyOfInterest, false);
    } else {
      unregisterClientInterest(regionName, keyOfInterest, interestType, false);
    }

    // Enqueue the interest unregistration message for the client.
    enqueueInterestRegistrationMessage(message);
  }

  protected void notifySecondariesOfInterestChange(ClientInterestMessageImpl message) {
    if (logger.isDebugEnabled()) {
      StringBuffer subBuffer = new StringBuffer();
      if (message.isRegister()) {
        subBuffer.append("register ").append(message.getIsDurable() ? "" : "non-")
            .append("durable interest in ");
      } else {
        subBuffer.append("unregister interest in ");
      }
      StringBuffer buffer = new StringBuffer();
      buffer.append(this).append(": Notifying secondary proxies to ").append(subBuffer.toString())
          .append(message.getRegionName()).append("->").append(message.getKeyOfInterest())
          .append("->").append(InterestType.getString(message.getInterestType()));
      logger.debug(buffer.toString());
    }
    this._cacheClientNotifier.deliverInterestChange(this.proxyID, message);
  }

  /*
   * protected void addFilterRegisteredClients(String regionName, Object keyOfInterest) { try {
   * this._cacheClientNotifier.addFilterRegisteredClients(regionName, this.proxyID); } catch
   * (RegionDestroyedException e) {
   * logger.warn(LocalizedStrings.CacheClientProxy_0_INTEREST_REG_FOR_0_FAILED, regionName + "->" +
   * keyOfInterest, e); } }
   */

  /**
   * Registers interest in the input region name and key
   *
   * @param regionName The fully-qualified name of the region in which to register interest
   * @param keyOfInterest The key in which to register interest
   */
  protected void registerClientInterest(String regionName, Object keyOfInterest, int interestType,
      boolean isDurable, boolean sendUpdatesAsInvalidates, boolean flushState) {
    ClientInterestList cil =
        this.cils[RegisterInterestTracker.getInterestLookupIndex(isDurable, false)];
    cil.registerClientInterest(regionName, keyOfInterest, interestType, sendUpdatesAsInvalidates);
    if (flushState) {
      flushForInterestRegistration(regionName,
          this._cache.getDistributedSystem().getDistributedMember());
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
    Region r = this._cache.getRegion(regionName);
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
            this._cache.getInternalDistributedSystem().getDistributionManager());
      } else {
        sfo = new StateFlushOperation((DistributedRegion) r);
      }
      try {
        // bug 41681 - we need to flush any member that may have a cache operation
        // in progress so that the changes are received there before returning
        // from this method
        InitialImageAdvice advice = cd.getCacheDistributionAdvisor().adviseInitialImage(null);
        HashSet recips = new HashSet(advice.getReplicates());
        recips.addAll(advice.getUninitialized());
        recips.addAll(advice.getEmpties());
        recips.addAll(advice.getPreloaded());
        recips.addAll(advice.getOthers());
        sfo.flush(recips, target, OperationExecutors.HIGH_PRIORITY_EXECUTOR, true);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
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
      this.cils[RegisterInterestTracker.durableInterestListIndex]
          .unregisterClientInterest(regionName, keyOfInterest, interestType);
    }
    // always unregister non durable interest
    this.cils[RegisterInterestTracker.interestListIndex].unregisterClientInterest(regionName,
        keyOfInterest, interestType);
  }

  /**
   * Registers interest in the input region name and list of keys
   *
   * @param regionName The fully-qualified name of the region in which to register interest
   * @param keysOfInterest The list of keys in which to register interest
   */
  protected void registerClientInterestList(String regionName, List keysOfInterest,
      boolean isDurable, boolean sendUpdatesAsInvalidates, boolean flushState) {
    // we only use two interest lists to map the non-durable and durable
    // identifiers to their interest settings
    ClientInterestList cil = this.cils[RegisterInterestTracker.getInterestLookupIndex(isDurable,
        false/* sendUpdatesAsInvalidates */)];
    cil.registerClientInterestList(regionName, keysOfInterest, sendUpdatesAsInvalidates);
    if (getHARegionQueue() != null) {
      if (flushState) {
        flushForInterestRegistration(regionName,
            this._cache.getDistributedSystem().getDistributedMember());
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
  protected void unregisterClientInterest(String regionName, List keysOfInterest,
      boolean isClosing) {
    // only unregister durable interest if isClosing and !keepalive
    if (!isClosing /* explicit unregister */
        || !getDurableKeepAlive() /* close and no keepAlive */) {
      this.cils[RegisterInterestTracker.durableInterestListIndex]
          .unregisterClientInterestList(regionName, keysOfInterest);
    }
    // always unregister non durable interest
    this.cils[RegisterInterestTracker.interestListIndex].unregisterClientInterestList(regionName,
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
        registerClientInterestList(regionName, (List) key, message.getIsDurable(),
            message.getForUpdatesAsInvalidates(), true);
      } else {
        registerClientInterest(regionName, key, interestType, message.getIsDurable(),
            message.getForUpdatesAsInvalidates(), true);
      }

      // Add the client to the region's filters
      // addFilterRegisteredClients(regionName, key);

      if (logger.isDebugEnabled()) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(this).append(": Interest listener registered ")
            .append(message.getIsDurable() ? "" : "non-").append("durable interest in ")
            .append(message.getRegionName()).append("->").append(message.getKeyOfInterest())
            .append("->").append(InterestType.getString(message.getInterestType()));
        logger.debug(buffer.toString());
      }
    } else {
      // Unregister interest in this region->key
      if (key instanceof List) {
        unregisterClientInterest(regionName, (List) key, false);
      } else {
        unregisterClientInterest(regionName, key, interestType, false);
      }

      if (logger.isDebugEnabled()) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(this).append(": Interest listener unregistered interest in ")
            .append(message.getRegionName()).append("->").append(message.getKeyOfInterest())
            .append("->").append(InterestType.getString(message.getInterestType()));
        logger.debug(buffer.toString());
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
    if (AcceptorImpl.isAuthenticationRequired() && this.postAuthzCallback == null
        && AcceptorImpl.isPostAuthzCallbackPresent()) {
      // security is on and callback is null: it means multiuser mode.
      ClientUpdateMessageImpl cumi = (ClientUpdateMessageImpl) clientMessage;

      CqNameToOp clientCq = cumi.getClientCq(this.proxyID);

      if (clientCq != null && !clientCq.isEmpty()) {
        if (logger.isDebugEnabled()) {
          logger.debug("CCP clientCq size before processing auth {}", clientCq.size());
        }
        String[] regionNameHolder = new String[1];
        OperationContext opctxt = getOperationContext(clientMessage, regionNameHolder);
        if (opctxt == null) {
          logger.warn(
              "{}: Not Adding message to queue: {} because the operation context object could not be obtained for this client message.",
              new Object[] {this, clientMessage});
          return false;
        }

        String[] cqNames = clientCq.getNames();
        if (logger.isDebugEnabled()) {
          logger.debug("CCP clientCq names array size {}", cqNames.length);
        }
        for (int i = 0; i < cqNames.length; i++) {
          try {
            if (logger.isDebugEnabled()) {
              logger.debug("CCP clientCq name {}", cqNames[i]);
            }
            boolean isAuthorized = false;

            if (this.proxyID.isDurable() && this.getDurableKeepAlive() && this._isPaused) {
              // need to take lock as we may be reinitializing proxy cache
              synchronized (this.clientUserAuthsLock) {
                AuthorizeRequestPP postAuthCallback =
                    this.clientUserAuths.getUserAuthAttributes(cqNames[i]).getPostAuthzRequest();
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
                  this.clientUserAuths.getUserAuthAttributes(cqNames[i]);

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
                  new Object[] {this, clientMessage});
              clientCq.delete(cqNames[i]);
            }
          } catch (Exception ex) {
            // ignore...
          }
          if (logger.isDebugEnabled()) {
            logger.debug("CCP clientCq size after processing auth {}", clientCq.size());
          }
        }
        // again need to check as there may be no CQ available
        if (!clientMessage.hasCqs(this.proxyID)) {
          this._statistics.incMessagesNotQueuedNotInterested();
          if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
            logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE,
                "{}: Not adding message to queue. It is not interested in this region and key: {}",
                clientMessage);
          }
          return false;
        }
      }
    } else if (this.postAuthzCallback != null) {
      String[] regionNameHolder = new String[1];
      boolean isAuthorize = false;
      OperationContext opctxt = getOperationContext(clientMessage, regionNameHolder);
      if (opctxt == null) {
        logger.warn(
            "{}: Not Adding message to queue: {} because the operation context object could not be obtained for this client message.",
            new Object[] {this, clientMessage});
        return false;
      }
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Invoking authorizeOperation for message: {}", this, clientMessage);
      }

      if (this.proxyID.isDurable() && this.getDurableKeepAlive() && this._isPaused) {
        synchronized (this.clientUserAuthsLock) {
          isAuthorize = this.postAuthzCallback.authorizeOperation(regionNameHolder[0], opctxt);
        }
      } else {
        isAuthorize = this.postAuthzCallback.authorizeOperation(regionNameHolder[0], opctxt);
      }
      if (!isAuthorize) {
        logger.warn("{}: Not Adding message to queue {} because authorization failed.",
            new Object[] {this, clientMessage});
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
    ThreadState state = this.securityService.bindSubject(this.subject);
    ClientUpdateMessage clientMessage = null;

    if (conflatable instanceof HAEventWrapper) {
      clientMessage = ((HAEventWrapper) conflatable).getClientUpdateMessage();
    } else {
      clientMessage = (ClientUpdateMessage) conflatable;
    }

    this._statistics.incMessagesReceived();

    // post process
    if (this.securityService.needPostProcess()) {
      Object oldValue = clientMessage.getValue();
      Object newValue = securityService.postProcess(clientMessage.getRegionName(),
          clientMessage.getKeyOfInterest(), oldValue, clientMessage.valueIsObject());
      clientMessage.setLatestValue(newValue);
    }

    if (clientMessage.needsNoAuthorizationCheck() || postDeliverAuthCheckPassed(clientMessage)) {
      if (this._messageDispatcher != null) {
        this._messageDispatcher.enqueueMessage(conflatable);
      } else {
        this._statistics.incMessagesFailedQueued();

        if (logger.isDebugEnabled()) {
          logger.debug(
              "Message was not added to the queue. Message dispatcher was null for proxy: " + this
                  + ". Event ID hash code: " + conflatable.hashCode() + "; System ID hash code: "
                  + System.identityHashCode(conflatable) + "; Conflatable details: " + conflatable
                      .toString());
        }
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Message was not added to the queue. Event ID hash code: " + conflatable.hashCode()
                + "; System ID hash code: "
                + System.identityHashCode(conflatable) + "; Conflatable details: " + conflatable
                    .toString());
      }

      this._statistics.incMessagesFailedQueued();
    }

    if (state != null)
      state.clear();
  }

  protected void sendMessageDirectly(ClientMessage message) {
    // Send the message directly if the connection exists
    // (do not go through the queue).
    if (logger.isDebugEnabled()) {
      logger.debug("About to send message directly to {}", this);
    }
    if (this._messageDispatcher != null && this._socket != null && !this._socket.isClosed()) {
      // If the socket is open, send the message to it
      this._messageDispatcher.sendMessageDirectly(message);
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
      this._messageDispatcher = createMessageDispatcher(name);
    } catch (final Exception ex) {
      this._statistics.close();
      throw ex;
    }
  }

  MessageDispatcher createMessageDispatcher(String name) {
    return messageDispatcherFactory.create(this, name, statisticsClock);
  }

  protected void startOrResumeMessageDispatcher(boolean processedMarker) {
    // Only start or resume the dispatcher if it is Primary
    if (this.isPrimary) {
      // Add the marker to the queue
      if (!processedMarker) {
        EventID eventId = new EventID(this._cache.getDistributedSystem());
        this._messageDispatcher.enqueueMarker(new ClientMarkerMessageImpl(eventId));
      }

      // Set the message queue to primary.
      this._messageDispatcher._messageQueue.setPrimary(true);

      // Start or resume the dispatcher
      synchronized (this._messageDispatcher._pausedLock) {
        if (this.isPaused()) {
          // It is paused, resume it
          this.setPaused(false);
          if (this._messageDispatcher.isStopped()) {
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Starting dispatcher", this);
            }
            this._messageDispatcher.start();
          } else {
            // ARB: Initialize transient fields.
            this._messageDispatcher.initializeTransients();
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Resuming dispatcher", this);
            }
            this._messageDispatcher.resumeDispatching();
          }
        } else if (!this._messageDispatcher.isAlive()) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Starting dispatcher", this);
          }
          this._messageDispatcher.start();
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
    return this.cils[RegisterInterestTracker.interestListIndex].hasInterest()
        || this.cils[RegisterInterestTracker.durableInterestListIndex].hasInterest();
  }

  /**
   * Returns a string representation of the proxy
   */
  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("CacheClientProxy[")
        .append(this.proxyID)
        .append("; port=").append(this._socket.getPort()).append("; primary=").append(isPrimary)
        .append("; version=").append(clientVersion).append("]");
    return buffer.toString();
  }

  public String getState() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("CacheClientProxy[")
        .append(this.proxyID)
        .append("; port=").append(this._socket.getPort()).append("; primary=").append(isPrimary)
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
    boolean primary = this.isPrimary;
    return primary;
  }

  protected boolean basicIsPrimary() {
    return this.isPrimary;
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
    if (this._messageDispatcher != null) {
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
  protected void reinitialize(Socket socket, ClientProxyMembershipID proxyId, Cache cache,
      boolean ip, byte cc, Version ver) {
    // Re-initialize transient fields
    initializeTransientFields(socket, proxyId, ip, cc, ver);
    getCacheClientNotifier().getAcceptorStats().incCurrentQueueConnections();


    // Cancel expiration task
    cancelDurableExpirationTask(true);

    // Set the message dispatcher's primary flag. This could go from primary
    // to secondary
    this._messageDispatcher._messageQueue.setPrimary(ip);
    this._messageDispatcher._messageQueue.setClientConflation(cc);

    // Reset the _socketClosed AtomicBoolean
    this._socketClosed.compareAndSet(true, false);

    reinitializeClientAuths();
    this.creationDate = new Date();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Has been reinitialized", this);
    }
  }

  protected boolean isDurable() {
    return getProxyID().isDurable();
  }

  protected String getDurableId() {
    return getProxyID().getDurableId();
  }

  protected int getDurableTimeout() {
    return getProxyID().getDurableTimeout();
  }

  private boolean getDurableKeepAlive() {
    return this.keepalive;
  }

  protected String getHARegionName() {
    return getProxyID().getHARegionName();
  }

  public Region getHARegion() {
    return this._messageDispatcher._messageQueue.getRegion();
  }

  public Version getVersion() {
    return this.clientVersion;
  }

  @VisibleForTesting
  protected Subject getSubject() {
    return this.subject;
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
        getCacheClientNotifier().durableClientTimedOut(CacheClientProxy.this.proxyID);

        // Close the proxy
        terminateDispatching(false);
        _cacheClientNotifier.statistics.incQueueDroppedCount();

        /*
         * Setting the expiration task to null again and cancelling existing one, if any. See
         * #50894.
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
        Object task = _durableExpirationTask.getAndSet(null);
        if (task != null) {
          if (((SystemTimerTask) task).cancel()) {
            _cache.purgeCCPTimer();
          }
        }
      }

    };
    if (this._durableExpirationTask.compareAndSet(null, task)) {
      _cache.getCCPTimer().schedule(task, getDurableTimeout() * 1000L);
    }
  }

  protected void cancelDurableExpirationTask(boolean logMessage) {
    SystemTimer.SystemTimerTask task = (SystemTimerTask) _durableExpirationTask.getAndSet(null);
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
      return this._statistics.getCqCount();
    }
  }

  /**
   * Increment the number of CQs the client installed
   *
   */
  public void incCqCount() {
    synchronized (this) {
      this._statistics.incCqCount();
    }
  }

  /**
   * Decrement the number of CQs the client installed
   *
   */
  public synchronized void decCqCount() {
    synchronized (this) {
      this._statistics.decCqCount();
    }
  }

  /**
   * Returns true if the client has one CQ
   *
   * @return true if the client has one CQ
   */
  public boolean hasOneCq() {
    synchronized (this) {
      return this._statistics.getCqCount() == 1;
    }
  }

  /**
   * Returns true if the client has no CQs
   *
   * @return true if the client has no CQs
   */
  public boolean hasNoCq() {
    synchronized (this) {
      return this._statistics.getCqCount() == 0;
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
    int pingCount = this.pingCounter.incrementAndGet();
    return pingCount;
  }

  public void resetPingCounter() {
    this.pingCounter.set(0);
  }

  /**
   * Returns the number of seconds that have elapsed since the Client proxy created.
   *
   * @since GemFire 7.0
   */
  public long getUpTime() {
    return (long) ((System.currentTimeMillis() - this.creationDate.getTime()) / 1000);
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
