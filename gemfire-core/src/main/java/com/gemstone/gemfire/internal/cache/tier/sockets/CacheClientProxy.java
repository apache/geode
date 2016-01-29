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

package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.ClientSession;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.InterestRegistrationEvent;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.client.internal.RegisterInterestTracker;
import com.gemstone.gemfire.cache.operations.DestroyOperationContext;
import com.gemstone.gemfire.cache.operations.InvalidateOperationContext;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.cache.operations.PutOperationContext;
import com.gemstone.gemfire.cache.operations.RegionClearOperationContext;
import com.gemstone.gemfire.cache.operations.RegionCreateOperationContext;
import com.gemstone.gemfire.cache.operations.RegionDestroyOperationContext;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.query.internal.cq.InternalCqQuery;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.SystemTimer;
import com.gemstone.gemfire.internal.SystemTimer.SystemTimerTask;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.ClientServerObserver;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisee;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;
import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.FilterProfile;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InterestRegistrationEventImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.StateFlushOperation;
import com.gemstone.gemfire.internal.cache.ha.HAContainerWrapper;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueueAttributes;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueueStats;
import com.gemstone.gemfire.internal.cache.tier.InterestType;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessageImpl.CqNameToOp;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.Get70;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.i18n.StringId;

/**
 * Class <code>CacheClientProxy</code> represents the server side of the
 * {@link CacheClientUpdater}. It queues messages to be sent from the server to
 * the client. It then reads those messages from the queue and sends them to the
 * client.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 */
@SuppressWarnings("synthetic-access")
public class CacheClientProxy implements ClientSession {
  private static final Logger logger = LogService.getLogger();
  
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
  protected boolean isMarkedForRemoval = false;
  
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
  protected final GemFireCacheImpl _cache;

  /**
   * The list of keys that the client represented by this proxy is interested in
   * (stored by region)
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
//  /**
//   * A string representing interest in all keys
//   */
//  protected static final String ALL_KEYS = "ALL_KEYS";
//
  /**
   * True if a marker message is still in the ha queue.
   */
  private boolean markerEnqueued = false;

  /**
   * The number of times to peek on shutdown before giving up and shutting down
   */
  protected static final int MAXIMUM_SHUTDOWN_PEEKS = Integer.getInteger("gemfire.MAXIMUM_SHUTDOWN_PEEKS",50).intValue();

  /**
   * The number of milliseconds to wait for an offering to the message queue
   */
  protected static final int MESSAGE_OFFER_TIME = 0;

  /**
   * The default maximum message queue size
   */
//   protected static final int MESSAGE_QUEUE_SIZE_DEFAULT = 230000;

  /** The message queue size */
  protected final int _maximumMessageCount;

  /**
   * The time (in seconds ) after which a message in the client queue will
   * expire.
   */
  protected final int _messageTimeToLive;

  /**
   * The <code>CacheClientNotifier</code> registering this proxy.
   */
  protected final CacheClientNotifier _cacheClientNotifier;

  /**
   * Defaults to true; meaning do some logging of dropped client notification
   * messages. Set the system property to true to cause dropped messages to NOT
   * be logged.
   */
  protected static final boolean LOG_DROPPED_MSGS = !Boolean
      .getBoolean("gemfire.disableNotificationWarnings");

  /**
   * for testing purposes, delays the start of the dispatcher thread
   */
  public static boolean isSlowStartForTesting = false;

  /**
   * Default value for slow starting time of dispatcher
   */
  private static final long DEFAULT_SLOW_STARTING_TIME = 5000;

  /**
   * Key in the system property from which the slow starting time value will be
   * retrieved
   */
  private static final String KEY_SLOW_START_TIME_FOR_TESTING = "slowStartTimeForTesting";

  private boolean isPrimary;
  
  /** @since 5.7 */
  protected byte clientConflation = HandShake.CONFLATION_DEFAULT;
  
  /**
   * Flag to indicate whether to keep a durable client's queue alive
   */
  boolean keepalive = false;

  private AccessControl postAuthzCallback;

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
   * A map of region name as key and integer as its value. Basically, it stores
   * the names of the regions with <code>DataPolicy</code> as EMPTY. If an 
   * event's region name is present in this map, it's full value (and not 
   * delta) is sent to the client represented by this proxy.
   *
   * @since 6.1
   */
  private volatile Map regionsWithEmptyDataPolicy = new HashMap();

  /**
   * A debug flag used for testing Backward compatibility
   */
  public static boolean AFTER_MESSAGE_CREATION_FLAG = false;
  
  /**
   * Notify the region when a client interest registration occurs. This tells
   * the region to update access time when an update is to be pushed to a
   * client. It is enabled only for <code>PartitionedRegion</code>s
   * currently.
   */
  protected static final boolean NOTIFY_REGION_ON_INTEREST = Boolean
      .getBoolean("gemfire.updateAccessTimeOnClientInterest");   
  
  /**
   * The AcceptorImpl identifier to which the proxy is connected.
   */
  private final long _acceptorId;
  
  /** acceptor's setting for notifyBySubscription */
  private final boolean notifyBySubscription;
  
  /** To queue the events arriving during message dispatcher initialization */ 
  private volatile ConcurrentLinkedQueue<Conflatable> queuedEvents = new ConcurrentLinkedQueue<Conflatable>();
  
  private final Object queuedEventsSync = new Object();
  
  private volatile boolean messageDispatcherInit = false;

  /**
   * A counter that keeps track of how many task iterations that have occurred
   * since the last ping or message. The
   * {@linkplain CacheClientNotifier#scheduleClientPingTask ping task}
   * increments it. Normal messages sent to the client reset it. If the counter
   * reaches 3, a ping is sent.
   */
  private final AtomicInteger pingCounter = new AtomicInteger();
  
  
  /** Date on which this instances was created */
  private Date creationDate;

  /** true when the durable client associated with this proxy is being 
   * restarted and prevents cqs from being closed and drained**/
  private boolean drainLocked = false;
  private final Object drainLock = new Object();

  /** number of cq drains that are currently in progress **/
  private int numDrainsInProgress = 0;
  private final Object drainsInProgressLock = new Object();
  
  /**
   * Constructor.
   *
   * @param ccn
   *          The <code>CacheClientNotifier</code> registering this proxy
   * @param socket
   *          The socket between the server and the client
   * @param proxyID
   *          representing the Connection Proxy of the clien
   * @param isPrimary
   *          The boolean stating whether this prozxy is primary
   * @throws CacheException {
   */
  protected CacheClientProxy(CacheClientNotifier ccn, Socket socket,
      ClientProxyMembershipID proxyID, boolean isPrimary, byte clientConflation, 
      Version clientVersion, long acceptorId, boolean notifyBySubscription)
      throws CacheException {
    initializeTransientFields(socket, proxyID, isPrimary, clientConflation, clientVersion);
    this._cacheClientNotifier = ccn;
    this._cache = (GemFireCacheImpl)ccn.getCache();
    this._maximumMessageCount = ccn.getMaximumMessageCount();    
    this._messageTimeToLive = ccn.getMessageTimeToLive();
    this._acceptorId = acceptorId;
    this.notifyBySubscription = notifyBySubscription;
    StatisticsFactory factory = this._cache.getDistributedSystem();
    this._statistics = new CacheClientProxyStats(factory, 
        "id_"+this.proxyID.getDistributedMember().getId()+ "_at_"+ this._remoteHostAddress + ":" + this._socket.getPort());

    // Create the interest list
    this.cils[RegisterInterestTracker.interestListIndex] =
      new ClientInterestList(this, this.proxyID);
    // Create the durable interest list
    this.cils[RegisterInterestTracker.durableInterestListIndex] =
      new ClientInterestList(this, this.getDurableId());
    this.postAuthzCallback = null;
    this._cacheClientNotifier.getAcceptorStats().incCurrentQueueConnections();
    this.creationDate = new Date();
    initializeClientAuths();
  }
  
  private void initializeClientAuths()
  {
    if(AcceptorImpl.isPostAuthzCallbackPresent())
      this.clientUserAuths = ServerConnection.getClientUserAuths(this.proxyID);
  }

  private void reinitializeClientAuths()
  {
    if (this.clientUserAuths != null && AcceptorImpl.isPostAuthzCallbackPresent()) {
      synchronized (this.clientUserAuthsLock) {
        ClientUserAuths newClientAuth = ServerConnection.getClientUserAuths(this.proxyID);
        newClientAuth.fillPreviousCQAuth(this.clientUserAuths);
        this.clientUserAuths = newClientAuth;
      }        
    }
  }
  
  public void setPostAuthzCallback(AccessControl authzCallback) {
    //TODO:hitesh synchronization
    synchronized (this.clientUserAuthsLock) {
      if (this.postAuthzCallback != null)
        this.postAuthzCallback.close();
      this.postAuthzCallback = authzCallback;
    }
  }
  
  public void setCQVsUserAuth(String cqName, long uniqueId, boolean isDurable)
  {
    if(postAuthzCallback == null) //only for multiuser
    {
      if(this.clientUserAuths != null)
        this.clientUserAuths.setUserAuthAttributesForCq(cqName, uniqueId, isDurable);
    }
  }

  private void initializeTransientFields(Socket socket,
      ClientProxyMembershipID pid, boolean ip,  byte cc, Version vers) {
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
    this._remoteHostAddress = socket.getInetAddress().getHostAddress();
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

  public long getAcceptorId(){
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
  public ClientProxyMembershipID getProxyID()
  {
    return this.proxyID;
  }
  
  // the following code was commented out simply because it was not used
//   /**
//    * Determines if the proxy represents the client host (and only the host, not
//    * necessarily the exact VM running on the host)
//    *
//    * @return Whether the proxy represents the client host
//    */
//   protected boolean representsClientHost(String clientHost)
//   {
//     // [bruce] TODO BUGBUGBUG: this should compare InetAddresses, not Strings
//     return this._remoteHostAddress.equals(clientHost);
//   }

//   protected boolean representsClientVM(DistributedMember remoteMember)
//   {
//     // logger.warn("Is input port " + clientPort + " contained in " +
//     // logger.warn("Does input host " + clientHost + " equal " +
//     // this._remoteHostAddress+ ": " + representsClientHost(clientHost));
//     // logger.warn("representsClientVM: " +
//     // (representsClientHost(clientHost) && containsPort(clientPort)));
//     return (proxyID.getDistributedMember().equals(remoteMember));
//   }

//   /**
//    * Determines if the CacheClientUpdater proxied by this instance is listening
//    * on the input clientHost and clientPort
//    *
//    * @param clientHost
//    *          The host name of the client to compare
//    * @param clientPort
//    *          The port number of the client to compare
//    *
//    * @return Whether the CacheClientUpdater proxied by this instance is
//    *         listening on the input clientHost and clientPort
//    */
//   protected boolean representsCacheClientUpdater(String clientHost,
//       int clientPort)
//   {
//     return (clientPort == this._socket.getPort() && representsClientHost(clientHost));
//   }

  protected boolean isMember(ClientProxyMembershipID memberId)
  {
    return this.proxyID.equals(memberId);
  }

  protected boolean isSameDSMember(ClientProxyMembershipID memberId)
  {
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
  protected Socket getSocket()
  {
    return this._socket;
  }
  
  public String getSocketHost()
  {
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
  protected String getRemoteHostAddress()
  {
    return this._remoteHostAddress;
  }

  /**
   * Returns the remote host's port
   *
   * @return the remote host's port
   */
  public int getRemotePort()
  {
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
   * Wait until the receiver's removal has completed before
   * returning.
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
          }
          catch (InterruptedException e) {
            interrupted = true;
            this._cache.getCancelCriterion().checkCancelInProgress(e);
          }
        } // while
      }
      finally {
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
  public GemFireCacheImpl getCache()
  {
    return this._cache;
  }

  public Set<String> getInterestRegisteredRegions() {
    HashSet<String> regions = new HashSet<String>();
    for(int i=0; i < this.cils.length; i++){
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
  public CacheClientProxyStats getStatistics()
  {
    return this._statistics;
  }

  /**
   * Returns this proxy's <code>CacheClientNotifier</code>.
   * @return this proxy's <code>CacheClientNotifier</code>
   */
  protected CacheClientNotifier getCacheClientNotifier() {
    return this._cacheClientNotifier;
  }

  /**
   * Returns the size of the queue for heuristic purposes.  This
   * size may be changing concurrently if puts/gets are occurring
   * at the same time.
   */
  public int getQueueSize() {
    return this._messageDispatcher == null ? 0
        : this._messageDispatcher.getQueueSize();
  }
  
  /** 
   * returns the queue size calculated through stats
   */
  public int getQueueSizeStat() {
    return this._messageDispatcher == null ? 0
        : this._messageDispatcher.getQueueSizeStat();  
  }
  
      
  public boolean drainInProgress() {
    synchronized(drainsInProgressLock) {
      return numDrainsInProgress > 0;
    }
  }
  
  //Called from CacheClientNotifier when attempting to restart paused proxy
  //locking the drain lock requires that no drains are in progress 
  //when the lock was acquired.
  public boolean lockDrain() {
    synchronized(drainsInProgressLock) {
      if (!drainInProgress()) {
        synchronized(drainLock) {
          if (testHook != null) {
            testHook.doTestHook("PRE_ACQUIRE_DRAIN_LOCK_UNDER_SYNC");
          }
          //prevent multiple lockings of drain lock
          if (!drainLocked) {
            drainLocked = true;
            return true;
          }
        }
      }
    }
    return false;
  }
  
  //Called from CacheClientNotifier when completed restart of proxy
  public void unlockDrain() {
    if (testHook != null) {
      testHook.doTestHook("PRE_RELEASE_DRAIN_LOCK");
    }
    synchronized(drainLock) {
      drainLocked = false;
    }
  }
  
  //Only close the client cq if it is paused and no one is attempting to restart the proxy
  public boolean closeClientCq(String clientCQName) throws CqException {
    if (testHook != null) {
      testHook.doTestHook("PRE_DRAIN_IN_PROGRESS");
    }
    synchronized(drainsInProgressLock) {
      numDrainsInProgress ++;
    }
    if (testHook != null) {
      testHook.doTestHook("DRAIN_IN_PROGRESS_BEFORE_DRAIN_LOCK_CHECK");
    }
    try {
      //If the drain lock was acquired, the other thread did so before we could bump up
      //the numDrainsInProgress.  That means we need to stop.
      if (drainLocked) {
        // someone is trying to restart a paused proxy
        String msg = LocalizedStrings.CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_RESTARTING_DURABLE_CLIENT.toLocalizedString(clientCQName, proxyID.getDurableId());
        logger.info(msg);
        throw new CqException(msg);
      }
      //isConnected is to protect against the case where a durable client has reconnected
      //but has not yet sent a ready for events message
      //we can probably remove the isPaused check
      if (isPaused() && !isConnected()) {
        CqService cqService = getCache().getCqService();
        if (cqService != null) {
          InternalCqQuery  cqToClose = cqService.getCq(cqService.constructServerCqName(
              clientCQName, this.proxyID));
          // close and drain
          if (cqToClose != null) {
            cqService.closeCq(clientCQName, this.proxyID);
            this._messageDispatcher.drainClientCqEvents(this.proxyID, cqToClose);
          }
          else {
            String msg = LocalizedStrings.CqService_CQ_NOT_FOUND_FAILED_TO_CLOSE_THE_SPECIFIED_CQ_0.toLocalizedString(clientCQName);
            logger.info(msg);
            throw new CqException(msg);
          }
        }
      } else {
        String msg = LocalizedStrings.CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_ACTIVE_DURABLE_CLIENT.toLocalizedString(clientCQName, proxyID.getDurableId());
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
   * Returns whether the proxy is alive. It is alive if its message dispatcher
   * is processing messages.
   *
   * @return whether the proxy is alive
   */
  protected boolean isAlive()
  {
    if (this._messageDispatcher == null) {
      return false;
    }
    return !this._messageDispatcher.isStopped();
  }

  /**
   * Returns whether the proxy is paused. It is paused if its message dispatcher
   * is paused. This only applies to durable clients.
   *
   * @return whether the proxy is paused
   *
   * @since 5.5
   */
  protected boolean isPaused() {
    return this._isPaused;
  }

  protected void setPaused(boolean isPaused) {
    this._isPaused = isPaused;
  }

  /**
   * Closes the proxy. This method checks the message queue for any unprocessed
   * messages and processes them for MAXIMUM_SHUTDOWN_PEEKS.
   *
   * @see CacheClientProxy#MAXIMUM_SHUTDOWN_PEEKS
   */
  protected void close()
  {
    close(true, false);
  }

  /**
   * Set to true once this proxy starts being closed.
   * Remains true for the rest of its existence.
   */
  private final AtomicBoolean closing = new AtomicBoolean(false);

  /**
   * Close the <code>CacheClientProxy</code>.
   *
   * @param checkQueue
   *          Whether to message check the queue and process any contained
   *          messages (up to MAXIMUM_SHUTDOWN_PEEKS).
   * @param stoppedNormally
   *          Whether client stopped normally
   *
   * @return whether to keep this <code>CacheClientProxy</code>
   * @see CacheClientProxy#MAXIMUM_SHUTDOWN_PEEKS
   */
  protected boolean close(boolean checkQueue, boolean stoppedNormally) {
    boolean pauseDurable = false;
    // If the client is durable and either (a) it hasn't stopped normally or (b) it
    // has stopped normally but it is configured to be kept alive, set pauseDurable
    // to true
    if (isDurable()
        && (!stoppedNormally || (getDurableKeepAlive() && stoppedNormally))) {
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
        if (this.postAuthzCallback != null) {//for single user
          this.postAuthzCallback.close();
          this.postAuthzCallback = null;
        }else if(this.clientUserAuths != null) {//for multiple users
          this.clientUserAuths.cleanup(true);
          this.clientUserAuths = null;
        }
      }
    }
    catch (Exception ex) {
      if (this._cache.getSecurityLoggerI18n().warningEnabled()) {
        this._cache.getSecurityLoggerI18n().warning(LocalizedStrings.TWO_ARG_COLON, new Object[] {this, ex});
      }
    }
    // Notify the caller whether to keep this proxy. If the proxy is durable
    // and should be paused, then return true; otherwise return false.
    return keepProxy;
  }

  protected void pauseDispatching() {
    if (this._messageDispatcher == null){
      return;
    }

    // If this is the primary, pause the dispatcher (which closes its transient
    // fields. Otherwise, just close the transient fields.
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Pausing processing", this);
    }
    //BUGFIX for BUG#38234
    if(!testAndSetPaused(true) && this.isPrimary) {
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
    
    synchronized(this._messageDispatcher._pausedLock) {
      if (this._isPaused != newValue) {
        this._isPaused = newValue;
        this._messageDispatcher._pausedLock.notifyAll();
        return !this._isPaused;
      }
      else {
        this._messageDispatcher._pausedLock.notifyAll();
        return this._isPaused;
      }
    }
  }
  protected void terminateDispatching(boolean checkQueue) {
    if (this._messageDispatcher == null){
      return;
    }
    
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
        closeSocket();
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
            //com.gemstone.gemfire.internal.OSProcess.printStacks(com.gemstone.gemfire.internal.OSProcess.getId());
            logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_COULD_NOT_STOP_MESSAGE_DISPATCHER_THREAD, this));
          }
        }
      }
    }
    finally {
      if (gotInterrupt) {
        Thread.currentThread().interrupt();
      }
      if (!alreadyDestroyed) {
        destroyRQ();
      }
    }
    } finally {
      //  Close the statistics
      this._statistics.close(); // fix for bug 40105
      closeTransientFields(); // make sure this happens
    }
  }

  private void closeSocket() {
    if (this._socketClosed.compareAndSet(false, true)) {
      // Close the socket
      this._cacheClientNotifier.getSocketCloser().asyncClose(this._socket, this._remoteHostAddress, null);
      getCacheClientNotifier().getAcceptorStats().decCurrentQueueConnections();
    }
  }
  
  private void closeTransientFields() {
    closeSocket();

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
    } catch (CacheClosedException e) {
      // ignore if cache is shutting down
    }
    // Commented to fix bug 40259
    //this.clientVersion = null;
    closeNonDurableCqs();    
  }
  
  private void releaseCommBuffer() {
    ByteBuffer bb = this._commBuffer;
    if (bb != null) {
      this._commBuffer = null;
      ServerConnection.releaseCommBuffer(bb);
    }
  }

  private void closeNonDurableCqs(){
    CqService cqService = getCache().getCqService();
    if (cqService != null) {
      try {
        cqService.closeNonDurableClientCqs(getProxyID());
      }
      catch (CqException ex) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_CQEXCEPTION_WHILE_CLOSING_NON_DURABLE_CQS_0, ex.getLocalizedMessage()));
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
    }
    catch (RegionDestroyedException rde) {
//      throw rde;
    }
    catch (CancelException e) {
//      throw e;
    }
    catch (Exception warning) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_EXCEPTION_IN_CLOSING_THE_UNDERLYING_HAREGION_OF_THE_HAREGIONQUEUE, this), warning);
    }
  }

  public void registerInterestRegex(String regionName, String regex,
      boolean isDurable) {
    registerInterestRegex(regionName, regex, isDurable, true);
  }
  
  public void registerInterestRegex(String regionName, String regex,
      boolean isDurable, boolean receiveValues) {
    if (this.isPrimary) {
      // Notify all secondaries and client of change in interest
      notifySecondariesAndClient(regionName, regex, InterestResultPolicy.NONE,
          isDurable, receiveValues, InterestType.REGULAR_EXPRESSION);
    } else {
      throw new IllegalStateException(LocalizedStrings.CacheClientProxy_NOT_PRIMARY.toLocalizedString());
    }
  }

  public void registerInterest(String regionName, Object keyOfInterest,
      InterestResultPolicy policy, boolean isDurable) {
    registerInterest(regionName, keyOfInterest, policy, isDurable, true);
  }
  
  public void registerInterest(String regionName, Object keyOfInterest,
      InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues) {
    if (keyOfInterest instanceof String && keyOfInterest.equals("ALL_KEYS")) {
      registerInterestRegex(regionName, ".*", isDurable, receiveValues);
    } else if (keyOfInterest instanceof List) {
      if (this.isPrimary) {
        notifySecondariesAndClient(regionName, keyOfInterest, policy,
            isDurable, receiveValues, InterestType.KEY);
      } else {
        throw new IllegalStateException(LocalizedStrings.CacheClientProxy_NOT_PRIMARY.toLocalizedString());
      }
    } else {
      if (this.isPrimary) {
        // Notify all secondaries and client of change in interest
        notifySecondariesAndClient(regionName, keyOfInterest, policy,
            isDurable, receiveValues, InterestType.KEY);
        
        // Enqueue the initial value message for the client if necessary
        if (policy == InterestResultPolicy.KEYS_VALUES) {
          Get70 request = (Get70)Get70.getCommand();
          LocalRegion lr = (LocalRegion) this._cache.getRegion(regionName);
          Get70.Entry entry = request.getValueAndIsObject(lr, keyOfInterest, null,
              null);
          boolean isObject = entry.isObject;
          byte[] value = null;
          if (entry.value instanceof byte[]) {
            value = (byte[])entry.value;
          } else {
            try {
              value = CacheServerHelper.serialize(entry.value);
            } catch (IOException e) {
              logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_THE_FOLLOWING_EXCEPTION_OCCURRED_0, entry.value), e);
            }
          }
          VersionTag tag = entry.versionTag;
          ClientUpdateMessage updateMessage = new ClientUpdateMessageImpl(
              EnumListenerEvent.AFTER_CREATE, lr, keyOfInterest, value, null,
              (isObject ? (byte) 0x01 : (byte) 0x00), null, this.proxyID,
              new EventID(this._cache.getDistributedSystem()), tag);
          CacheClientNotifier.routeSingleClientMessage(updateMessage, this.proxyID);
        }
        // Add the client to the region's filters
        //addFilterRegisteredClients(regionName, keyOfInterest);
      } else {
        throw new IllegalStateException(LocalizedStrings.CacheClientProxy_NOT_PRIMARY.toLocalizedString());
      }
    }
  }

  private void notifySecondariesAndClient(String regionName,
      Object keyOfInterest, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues, int interestType) {
    // Create a client interest message for the keyOfInterest
    ClientInterestMessageImpl message = new ClientInterestMessageImpl(
        new EventID(this._cache.getDistributedSystem()), regionName,
        keyOfInterest, interestType, policy.getOrdinal(), isDurable,
        !receiveValues, ClientInterestMessageImpl.REGISTER);

    // Notify all secondary proxies of a change in interest
    notifySecondariesOfInterestChange(message);

    // Modify interest registration
    if (keyOfInterest instanceof List) {
      registerClientInterestList(regionName, (List) keyOfInterest, isDurable,
          !receiveValues, true);
    } else {
      registerClientInterest(regionName, keyOfInterest, interestType,
          isDurable, !receiveValues, true);
    }

    // Enqueue the interest registration message for the client.
    // If the client is not 7.0.1 or greater and the key of interest is a list,
    // then create an individual message for each entry in the list since the
    // client doesn't support a ClientInterestMessageImpl containing a list.
    if (Version.GFE_701.compareTo(this.clientVersion) > 0
        && keyOfInterest instanceof List) {
      for (Iterator i = ((List) keyOfInterest).iterator(); i.hasNext();) {
        this._messageDispatcher.enqueueMessage(new ClientInterestMessageImpl(
            new EventID(this._cache.getDistributedSystem()), regionName,
            i.next(), interestType, policy.getOrdinal(), isDurable, !receiveValues,
            ClientInterestMessageImpl.REGISTER));
     }
    } else {
      this._messageDispatcher.enqueueMessage(message);
    }
  }

  public void unregisterInterestRegex(String regionName, String regex,
      boolean isDurable) {
    unregisterInterestRegex(regionName, regex, isDurable, true);
  }
  
  public void unregisterInterestRegex(String regionName, String regex,
      boolean isDurable, boolean receiveValues) {
    if (this.isPrimary) {
      notifySecondariesAndClient(regionName, regex, isDurable, receiveValues,
          InterestType.REGULAR_EXPRESSION);
    } else {
      throw new IllegalStateException(LocalizedStrings.CacheClientProxy_NOT_PRIMARY.toLocalizedString());
    }
  }
  
  public void unregisterInterest(String regionName, Object keyOfInterest,
      boolean isDurable) {
    unregisterInterest(regionName, keyOfInterest, isDurable, true);
  }
  
  public void unregisterInterest(String regionName, Object keyOfInterest,
      boolean isDurable, boolean receiveValues) {
    if (keyOfInterest instanceof String && keyOfInterest.equals("ALL_KEYS")) {
      unregisterInterestRegex(regionName, ".*", isDurable, receiveValues);
    } else {
      if (this.isPrimary) {
        notifySecondariesAndClient(regionName, keyOfInterest, isDurable,
            receiveValues, InterestType.KEY);
      } else {
        throw new IllegalStateException(LocalizedStrings.CacheClientProxy_NOT_PRIMARY.toLocalizedString());
      }
    }
  }

  private void notifySecondariesAndClient(String regionName,
      Object keyOfInterest, boolean isDurable, boolean receiveValues,
      int interestType) {
    // Notify all secondary proxies of a change in interest
    ClientInterestMessageImpl message = new ClientInterestMessageImpl(
        new EventID(this._cache.getDistributedSystem()), regionName,
        keyOfInterest, interestType, (byte) 0, isDurable, !receiveValues,
        ClientInterestMessageImpl.UNREGISTER);
    notifySecondariesOfInterestChange(message);
 
    // Modify interest registration
    if (keyOfInterest instanceof List) {
      unregisterClientInterest(regionName, (List) keyOfInterest, false);
    } else {
      unregisterClientInterest(regionName, keyOfInterest, interestType,
          false);
    }
 
    // Enqueue the interest unregistration message for the client.
    // If the client is not 7.0.1 or greater and the key of interest is a list,
    // then create an individual message for each entry in the list since the
    // client doesn't support a ClientInterestMessageImpl containing a list.
    if (Version.GFE_701.compareTo(this.clientVersion) > 0
        && keyOfInterest instanceof List) {
      for (Iterator i = ((List) keyOfInterest).iterator(); i.hasNext();) {
        this._messageDispatcher.enqueueMessage(new ClientInterestMessageImpl(
            new EventID(this._cache.getDistributedSystem()), regionName,
            i.next(), interestType, (byte) 0, isDurable, !receiveValues,
            ClientInterestMessageImpl.UNREGISTER));
      }
    } else {
      this._messageDispatcher.enqueueMessage(message);
    }
  }
  
  protected void notifySecondariesOfInterestChange(ClientInterestMessageImpl message) {
    if (logger.isDebugEnabled()) {
      StringBuffer subBuffer = new StringBuffer();
      if (message.isRegister()) {
        subBuffer
          .append("register ")
          .append(message.getIsDurable() ? "" : "non-")
          .append("durable interest in ");
      } else {
        subBuffer.append("unregister interest in ");
      }
      StringBuffer buffer = new StringBuffer();
      buffer
        .append(this)
        .append(": Notifying secondary proxies to ")
        .append(subBuffer.toString())
        .append(message.getRegionName())
        .append("->")
        .append(message.getKeyOfInterest())
        .append("->")
        .append(InterestType.getString(message.getInterestType()));
      logger.debug(buffer.toString());
    }
    this._cacheClientNotifier.deliverInterestChange(this.proxyID, message);
  }

  /*
  protected void addFilterRegisteredClients(String regionName,
      Object keyOfInterest) {
    try {
      this._cacheClientNotifier.addFilterRegisteredClients(regionName,
          this.proxyID);
    } catch (RegionDestroyedException e) {
      logger.warn(LocalizedStrings.CacheClientProxy_0_INTEREST_REG_FOR_0_FAILED, regionName + "->" + keyOfInterest, e);
    }
  }
  */
  
  /**
   * Registers interest in the input region name and key
   *
   * @param regionName
   *          The fully-qualified name of the region in which to register
   *          interest
   * @param keyOfInterest
   *          The key in which to register interest
   */
  protected void registerClientInterest(String regionName,
      Object keyOfInterest, int interestType, boolean isDurable,
      boolean sendUpdatesAsInvalidates, boolean flushState)
  {
    ClientInterestList cil =
      this.cils[RegisterInterestTracker.getInterestLookupIndex(
          isDurable, false)];
    cil.registerClientInterest(regionName, keyOfInterest, interestType, sendUpdatesAsInvalidates);
    if (flushState) {
      flushForInterestRegistration(regionName, this._cache.getDistributedSystem().getDistributedMember());
    }
    HARegionQueue queue = getHARegionQueue();
    if (queue != null) { // queue is null during initialization
      queue.setHasRegisteredInterest(true);
    }
  }
  
  /**
   * flush other regions to the given target.  This is usually the member
   * that is registering the interest.  During queue creation it is the
   * queue's image provider.
   */
  public void flushForInterestRegistration(String regionName, DistributedMember target) {
    Region r = this._cache.getRegion(regionName);
    if (r == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to find region '{}' to flush for interest registration", regionName);
      }
    } else if (r.getAttributes().getScope().isDistributed()) {
      if (logger.isDebugEnabled()){
        logger.debug("Flushing region '{}' for interest registration", regionName);
      }
      CacheDistributionAdvisee cd = (CacheDistributionAdvisee)r;
      final StateFlushOperation sfo;
      if (r instanceof PartitionedRegion) {
        // need to flush all buckets.  SFO should be changed to target buckets
        // belonging to a particular PR, but it doesn't have that option right now
        sfo = new StateFlushOperation(
            this._cache.getDistributedSystem().getDistributionManager());
      } else {
        sfo = new StateFlushOperation((DistributedRegion)r);
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
        sfo.flush(recips,
            target,
            DistributionManager.HIGH_PRIORITY_EXECUTOR, true);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /**
   * Unregisters interest in the input region name and key
   *
   * @param regionName
   *          The fully-qualified name of the region in which to unregister
   *          interest
   * @param keyOfInterest
   *          The key in which to unregister interest
   * @param isClosing
   *          Whether the caller is closing
   */
  protected void unregisterClientInterest(String regionName,
      Object keyOfInterest, int interestType, boolean isClosing)
  {
    // only unregister durable interest if isClosing and !keepalive
    if (!isClosing /* explicit unregister */
        || !getDurableKeepAlive() /* close and no keepAlive*/) {
      this.cils[RegisterInterestTracker.durableInterestListIndex].
        unregisterClientInterest(regionName, keyOfInterest, interestType);
    }
    // always unregister non durable interest
    this.cils[RegisterInterestTracker.interestListIndex].
      unregisterClientInterest(regionName, keyOfInterest, interestType);
  }

  /**
   * Registers interest in the input region name and list of keys
   *
   * @param regionName
   *          The fully-qualified name of the region in which to register
   *          interest
   * @param keysOfInterest
   *          The list of keys in which to register interest
   */
  protected void registerClientInterestList(String regionName,
      List keysOfInterest, boolean isDurable, boolean sendUpdatesAsInvalidates,
      boolean flushState)
  {
    // we only use two interest lists to map the non-durable and durable
    // identifiers to their interest settings
    ClientInterestList cil =
      this.cils[RegisterInterestTracker.getInterestLookupIndex(
          isDurable, false/*sendUpdatesAsInvalidates*/)];
    cil.registerClientInterestList(regionName, keysOfInterest, sendUpdatesAsInvalidates);
    if (getHARegionQueue() != null) {
      if (flushState) {
        flushForInterestRegistration(regionName, this._cache.getDistributedSystem().getDistributedMember());
      }
      getHARegionQueue().setHasRegisteredInterest(true);
    }
  }

  /**
   * Unregisters interest in the input region name and list of keys
   *
   * @param regionName
   *          The fully-qualified name of the region in which to unregister
   *          interest
   * @param keysOfInterest
   *          The list of keys in which to unregister interest
   * @param isClosing
   *          Whether the caller is closing
   */
  protected void unregisterClientInterest(String regionName,
      List keysOfInterest, boolean isClosing)
  {
    // only unregister durable interest if isClosing and !keepalive
    if (!isClosing /* explicit unregister */
        || !getDurableKeepAlive() /* close and no keepAlive*/) {
      this.cils[RegisterInterestTracker.durableInterestListIndex].
        unregisterClientInterestList(regionName, keysOfInterest);
    }
    // always unregister non durable interest
    this.cils[RegisterInterestTracker.interestListIndex].
      unregisterClientInterestList(regionName, keysOfInterest);
  }


  /** sent by the cache client notifier when there is an interest registration change */
  protected void processInterestMessage(ClientInterestMessageImpl message) { 
    int interestType = message.getInterestType(); 
    String regionName = message.getRegionName(); 
    Object key = message.getKeyOfInterest(); 
    if (message.isRegister()) {
      // Register interest in this region->key
      if (key instanceof List) {
        registerClientInterestList(regionName, (List) key,
            message.getIsDurable(), message.getForUpdatesAsInvalidates(), true);
      } else {
        registerClientInterest(regionName, key, interestType,
            message.getIsDurable(), message.getForUpdatesAsInvalidates(), true);
      }
       
      // Add the client to the region's filters 
      //addFilterRegisteredClients(regionName, key); 
 
      if (logger.isDebugEnabled()) { 
        StringBuffer buffer = new StringBuffer();
        buffer 
          .append(this) 
          .append(": Interest listener registered ") 
          .append(message.getIsDurable() ? "" : "non-") 
          .append("durable interest in ") 
          .append(message.getRegionName()) 
          .append("->") 
          .append(message.getKeyOfInterest())
          .append("->")
          .append(InterestType.getString(message.getInterestType()));
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
        buffer 
          .append(this) 
          .append(": Interest listener unregistered interest in ") 
          .append(message.getRegionName()) 
          .append("->") 
          .append(message.getKeyOfInterest())
          .append("->")
          .append(InterestType.getString(message.getInterestType()));
        logger.debug(buffer.toString()); 
      } 
    } 
  } 

  private boolean postDeliverAuthCheckPassed(ClientUpdateMessage clientMessage) {
    // Before adding it in the queue for dispatching, check for post
    // process authorization
    if (AcceptorImpl.isAuthenticationRequired()
        && this.postAuthzCallback == null
        && AcceptorImpl.isPostAuthzCallbackPresent()) {
      // security is on and callback is null: it means multiuser mode.
      ClientUpdateMessageImpl cumi = (ClientUpdateMessageImpl)clientMessage;

      CqNameToOp clientCq = cumi.getClientCq(this.proxyID);

      if (clientCq != null && !clientCq.isEmpty()) {
        if (logger.isDebugEnabled()) {
          logger.debug("CCP clientCq size before processing auth {}", clientCq.size());
        }
        String[] regionNameHolder = new String[1];
        OperationContext opctxt = getOperationContext(clientMessage,
            regionNameHolder);
        if (opctxt == null) {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.CacheClientProxy__0_NOT_ADDING_MESSAGE_TO_QUEUE_1_BECAUSE_THE_OPERATION_CONTEXT_OBJECT_COULD_NOT_BE_OBTAINED_FOR_THIS_CLIENT_MESSAGE,
              new Object[] {this, clientMessage}));
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

            if (this.proxyID.isDurable() && this.getDurableKeepAlive()
                && this._isPaused) {
              // need to take lock as we may be reinitializing proxy cache
              synchronized (this.clientUserAuthsLock) {
                AuthorizeRequestPP postAuthCallback = this.clientUserAuths
                    .getUserAuthAttributes(cqNames[i]).getPostAuthzRequest();
                if (logger.isDebugEnabled() && postAuthCallback == null) {
                  logger.debug("CCP clientCq post callback is null");
                }
                if (postAuthCallback != null && postAuthCallback
                    .getPostAuthzCallback().authorizeOperation(
                        regionNameHolder[0], opctxt)) {
                  isAuthorized = true;
                }
              }
            } else {
              UserAuthAttributes userAuthAttributes = this.clientUserAuths
                  .getUserAuthAttributes(cqNames[i]);

              AuthorizeRequestPP postAuthCallback = userAuthAttributes
                  .getPostAuthzRequest();
              if (postAuthCallback == null && logger.isDebugEnabled()) {
                logger.debug("CCP clientCq post callback is null");
              }
              if (postAuthCallback != null && postAuthCallback
                  .getPostAuthzCallback().authorizeOperation(
                      regionNameHolder[0], opctxt)) {
                isAuthorized = true;
              }
            }

            if (!isAuthorized) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.CacheClientProxy__0_NOT_ADDING_CQ_MESSAGE_TO_QUEUE_1_BECAUSE_AUTHORIZATION_FAILED,
                  new Object[] {this, clientMessage}));
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
          if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER)) {
            logger.debug("{}: Not adding message to queue. It is not interested in this region and key: {}", clientMessage);
          }
          return false;
        }
      }
    }
    else if (this.postAuthzCallback != null) {
      String[] regionNameHolder = new String[1];
      boolean isAuthorize = false;
      OperationContext opctxt = getOperationContext(clientMessage,
          regionNameHolder);
      if (opctxt == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy__0_NOT_ADDING_MESSAGE_TO_QUEUE_1_BECAUSE_THE_OPERATION_CONTEXT_OBJECT_COULD_NOT_BE_OBTAINED_FOR_THIS_CLIENT_MESSAGE, new Object[] {this, clientMessage}));
        return false;
      }
      if (logger.isTraceEnabled()){
        logger.trace("{}: Invoking authorizeOperation for message: {}", this, clientMessage);
      }

      if (this.proxyID.isDurable() && this.getDurableKeepAlive()
          && this._isPaused) {
        synchronized (this.clientUserAuthsLock) {
          isAuthorize = this.postAuthzCallback.authorizeOperation(
              regionNameHolder[0], opctxt);
        }
      } else {
        isAuthorize = this.postAuthzCallback.authorizeOperation(
            regionNameHolder[0], opctxt);
      }
      if (!isAuthorize) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy__0_NOT_ADDING_MESSAGE_TO_QUEUE_1_BECAUSE_AUTHORIZATION_FAILED, new Object[] {this, clientMessage}));
        return false;
      }
    }

    return true;
  }

  /**
   * Delivers the message to the client representing this client proxy.
   * @param conflatable 
   */
  protected void deliverMessage(Conflatable conflatable)
  {
    ClientUpdateMessage clientMessage = null;
    if(conflatable instanceof HAEventWrapper) {
      clientMessage = ((HAEventWrapper)conflatable).getClientUpdateMessage();
    } else {
      clientMessage = (ClientUpdateMessage)conflatable;
    } 

    this._statistics.incMessagesReceived();

    if (clientMessage.needsNoAuthorizationCheck() || postDeliverAuthCheckPassed(clientMessage)) {
      // If dispatcher is getting initialized, add the event to temporary queue.
      if (this.messageDispatcherInit) {
        synchronized (this.queuedEventsSync) {
          if (this.messageDispatcherInit) {  // Check to see value did not changed while getting the synchronize lock.
            if (logger.isDebugEnabled()) {
              logger.debug("Message dispatcher for proxy {} is getting initialized. Adding message to the queuedEvents.", this);
            }
            this.queuedEvents.add(conflatable);
            return;
          }
        }
      }
      
      if (this._messageDispatcher != null) {
        this._messageDispatcher.enqueueMessage(conflatable);
      } else {
        this._statistics.incMessagesFailedQueued();
        if (logger.isDebugEnabled()) {
          logger.debug("Message is not added to the queue. Message dispatcher for proxy: {} doesn't exist.", this);
        }
      }
    } else {
      this._statistics.incMessagesFailedQueued();
    }
  }

  protected void sendMessageDirectly(ClientMessage message) {
    // Send the message directly if the connection exists
    // (do not go through the queue).
    if (logger.isDebugEnabled()){
      logger.debug("About to send message directly to {}", this);
    }
    if (this._messageDispatcher != null && this._socket != null && !this._socket.isClosed()) {
      // If the socket is open, send the message to it
      this._messageDispatcher.sendMessageDirectly(message);
      if (logger.isDebugEnabled()){
        logger.debug("Sent message directly to {}", this);
      }
    } else {
      // Otherwise just reset the ping counter
      resetPingCounter();
      if (logger.isDebugEnabled()){
        logger.debug("Skipped sending message directly to {}", this);
      }
    }
  }
  
  private OperationContext getOperationContext(ClientMessage cmsg,
      String[] regionNameHolder) {
    ClientUpdateMessageImpl cmsgimpl = (ClientUpdateMessageImpl)cmsg;
    OperationContext opctxt = null;
    // TODO SW: Special handling for DynamicRegions; this should be reworked
    // when DynamicRegion API is deprecated
    String regionName = cmsgimpl.getRegionName();
    regionNameHolder[0] = regionName;
    if (cmsgimpl.isCreate()) {
      if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
        regionNameHolder[0] = (String)cmsgimpl.getKeyOfInterest();
        opctxt = new RegionCreateOperationContext(true);
      }
      else {
        PutOperationContext tmp = new PutOperationContext(cmsgimpl.getKeyOfInterest(), cmsgimpl
            .getValue(), cmsgimpl.valueIsObject(), PutOperationContext.CREATE,
            true);
        tmp.setCallbackArg(cmsgimpl.getCallbackArgument());
        opctxt = tmp;
      }
    }
    else if (cmsgimpl.isUpdate()) {
      if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
        regionNameHolder[0] = (String)cmsgimpl.getKeyOfInterest();
        opctxt = new RegionCreateOperationContext(true);
      }
      else {
        PutOperationContext tmp = new PutOperationContext(cmsgimpl.getKeyOfInterest(), cmsgimpl
            .getValue(), cmsgimpl.valueIsObject(), PutOperationContext.UPDATE,
            true);
        tmp.setCallbackArg(cmsgimpl.getCallbackArgument());
        opctxt = tmp;
      }
    }
    else if (cmsgimpl.isDestroy()) {
      if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
        regionNameHolder[0] = (String)cmsgimpl.getKeyOfInterest();
        opctxt = new RegionDestroyOperationContext(true);
      }
      else {
        DestroyOperationContext tmp = new DestroyOperationContext(cmsgimpl.getKeyOfInterest(), true);
        tmp.setCallbackArg(cmsgimpl.getCallbackArgument());
        opctxt = tmp;
      }
    }
    else if (cmsgimpl.isDestroyRegion()) {
      opctxt = new RegionDestroyOperationContext(true);
    }
    else if (cmsgimpl.isInvalidate()) {
      InvalidateOperationContext tmp = new InvalidateOperationContext(cmsgimpl.getKeyOfInterest(), true);
      tmp.setCallbackArg(cmsgimpl.getCallbackArgument());
      opctxt = tmp;
    }
    else if (cmsgimpl.isClearRegion()) {
      RegionClearOperationContext tmp = new RegionClearOperationContext(true);
      tmp.setCallbackArg(cmsgimpl.getCallbackArgument());
      opctxt = tmp;
    }
    return opctxt;
  }

  /**
   * Initializes the message dispatcher thread. The
   * <code>MessageDispatcher</code> processes the message queue.
   *
   * @throws CacheException
   */
  public void initializeMessageDispatcher() throws CacheException
  {
    this.messageDispatcherInit = true; // Initialization process.
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Initializing message dispatcher with capacity of {} entries", this, _maximumMessageCount);
      }
      String name = "Client Message Dispatcher for "
        + getProxyID().getDistributedMember() + (isDurable()? " (" + getDurableId()+")" : "");
      this._messageDispatcher = new MessageDispatcher(this, name);
      
      //Fix for 41375 - drain as many of the queued events
      //as we can without synchronization.
      if (logger.isDebugEnabled()) {
        logger.debug("{} draining {} events from init queue into intialized queue", this, this.queuedEvents.size());
      }
      Conflatable nextEvent;
      while((nextEvent = queuedEvents.poll()) != null) {
        this._messageDispatcher.enqueueMessage(nextEvent);
      }
      
      //Now finish emptying the queue with synchronization to make
      //sure we don't miss any events.
      synchronized (this.queuedEventsSync){
          while((nextEvent = queuedEvents.poll()) != null) {
            this._messageDispatcher.enqueueMessage(nextEvent);
          }
          
          this.messageDispatcherInit = false; // Done initialization.
      }
    } finally {
      if (this.messageDispatcherInit) { // If its not successfully completed.
        this._statistics.close();
      }
    }
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
          }
          else {
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
   * Returns whether the client represented by this <code> CacheClientProxy
   * </code> has registered interest in anything. @return whether the client
   * represented by this <code> CacheClientProxy </code> has registered interest
   * in anything
   */
  protected boolean hasRegisteredInterested()
  {
    return
    this.cils[RegisterInterestTracker.interestListIndex].hasInterest() ||
    this.cils[RegisterInterestTracker.durableInterestListIndex].hasInterest();
  }

  /**
   * Returns a string representation of the proxy
   */
  @Override
  public String toString()
  {
    StringBuffer buffer = new StringBuffer();
    buffer.append("CacheClientProxy[")
      // .append("client proxy id=")
      .append(this.proxyID)
      // .append("; client host name=")
      // .append(this._socket.getInetAddress().getCanonicalHostName())
      // .append("; client host address=")
      // .append(this._remoteHostAddress)
      .append("; port=").append(this._socket.getPort())
      .append("; primary=").append(isPrimary)
      .append("; version=").append(clientVersion)
      .append("]");
    return buffer.toString();
  }
  
  public String getState(){
    StringBuffer buffer = new StringBuffer();
    buffer.append("CacheClientProxy[")
      // .append("client proxy id=")
      .append(this.proxyID)
      // .append("; client host name=")
      // .append(this._socket.getInetAddress().getCanonicalHostName())
      // .append("; client host address=")
      // .append(this._remoteHostAddress)
      .append("; port=").append(this._socket.getPort())
      .append("; primary=").append(isPrimary)
      .append("; version=").append(clientVersion)
      .append("; paused=").append(isPaused())
      .append("; alive=").append(isAlive())
      .append("; connected=").append(isConnected())
      .append("; isMarkedForRemoval=").append(isMarkedForRemoval)
      .append("]");
    return buffer.toString();
  }

  public boolean isPrimary()
  {
    //boolean primary = this._messageDispatcher.isAlive()
    //    || this._messageDispatcher._messageQueue.isPrimary();
    boolean primary = this.isPrimary;
    //System.out.println(this + ": DISPATCHER IS ALIVE: " + this._messageDispatcher.isAlive());
    //System.out.println(this + ": DISPATCHER QUEUE IS PRIMARY: " + this._messageDispatcher._messageQueue.isPrimary());
    //System.out.println(this + ": IS PRIMARY: " + primary);
    return primary;
    // return this.isPrimary ;
  }

  protected boolean basicIsPrimary() {
    return this.isPrimary;
  }

  protected void setPrimary(boolean isPrimary) {
    this.isPrimary = isPrimary;
  }

  // private static int nextId = 0;
  // static protected int getNextId() {
  // synchronized (CacheClientProxy.class) {
  // return ++nextId;
  // }
  // }
  /*
   * Return this client's HA region queue
   * @returns - HARegionQueue of the client
   */
   public HARegionQueue getHARegionQueue() {
     if (this._messageDispatcher != null){
       return _messageDispatcher._messageQueue;
     }
     return null;
   }


  /**
   * Reinitialize a durable <code>CacheClientProxy</code> with a new client.
   * @param socket
   *          The socket between the server and the client
   * @param ip
   *          whether this proxy represents the primary
   */
  protected void reinitialize(Socket socket, ClientProxyMembershipID proxyId,
      Cache cache, boolean ip, byte cc, Version ver) {
    // Re-initialize transient fields
    initializeTransientFields(socket, proxyId, ip, cc, ver);
    getCacheClientNotifier().getAcceptorStats().incCurrentQueueConnections();


    // Cancel expiration task
    cancelDurableExpirationTask(true);

    // Set the message dispatcher's primary flag. This could go from primary
    // to secondary
    this._messageDispatcher._messageQueue.setPrimary(ip);
    this._messageDispatcher._messageQueue.setClientConflation(cc);

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
  
  protected void scheduleDurableExpirationTask() {
    SystemTimer.SystemTimerTask task = new SystemTimer.SystemTimerTask() {
      @Override
      public void run2() {
        _durableExpirationTask.compareAndSet(this, null);
        logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0__THE_EXPIRATION_TASK_HAS_FIRED_SO_THIS_PROXY_IS_BEING_TERMINATED, CacheClientProxy.this));
        // Remove the proxy from the CacheClientNofier's registry
        getCacheClientNotifier().removeClientProxy(CacheClientProxy.this);
        getCacheClientNotifier().durableClientTimedOut(CacheClientProxy.this.proxyID);

        // Close the proxy
        terminateDispatching(false);
        _cacheClientNotifier._statistics.incQueueDroppedCount();

        /**
         * Setting the expiration task to null again and cancelling existing
         * one, if any. See #50894.
         * <p/>
         * The message dispatcher may again set the expiry task in below path:
         * <code>
         *  com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy.scheduleDurableExpirationTask(CacheClientProxy.java:2020)
         *  com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy.pauseDispatching(CacheClientProxy.java:924)
         *  com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy$MessageDispatcher.pauseOrUnregisterProxy(CacheClientProxy.java:2813)
         *  com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy$MessageDispatcher.run(CacheClientProxy.java:2692)
         * </code>
         * <p/>
         * This is because message dispatcher may get an IOException with
         * "Proxy closing due to socket being closed locally" during/after
         * terminateDispatching(false) above.
         */
        Object task = _durableExpirationTask.getAndSet(null);
        if (task != null) {
          ((SystemTimerTask)task).cancel();
        }
      }

    };
    if(this._durableExpirationTask.compareAndSet(null, task)) {
      _cache.getCCPTimer().schedule(task,
          getDurableTimeout()*1000L);
    }
  }

  protected void cancelDurableExpirationTask(boolean logMessage) {
    SystemTimer.SystemTimerTask task = (SystemTimerTask) _durableExpirationTask.getAndSet(null);
    if (task != null) {
      if (logMessage) {
        logger.info(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_CANCELLING_EXPIRATION_TASK_SINCE_THE_CLIENT_HAS_RECONNECTED, this));
      }
      task.cancel();
    }
  }

  /**
   * Class <code>ClientInterestList</code> provides a convenient interface
   * for manipulating client interest information.
   */
  static protected class ClientInterestList
   {
    
    final CacheClientProxy ccp;
    
    final Object id;

     /**
     * An object used for synchronizing the interest lists
     */
    final private Object interestListLock = new Object();
    
    /**
     * Regions that this client is interested in
     */
    final protected Set<String> regions = new HashSet<String>();

    /**
     * Constructor.
     */
    protected ClientInterestList(CacheClientProxy ccp, Object interestID) {
      this.ccp = ccp;
      this.id = interestID;
      // this.id = getNextId();
    }

    /**
     * Registers interest in the input region name and key
     */
    protected void registerClientInterest(String regionName,
        Object keyOfInterest, int interestType, boolean sendUpdatesAsInvalidates)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: registerClientInterest region={} key={}", ccp, regionName, keyOfInterest);
      }
      Set keysRegistered = null;
      synchronized(this.interestListLock) {
        LocalRegion r = (LocalRegion)this.ccp._cache.getRegion(regionName, true);
        if (r == null) {
          throw new RegionDestroyedException("Region could not be found for interest registration", regionName);
        }
        if ( ! (r instanceof CacheDistributionAdvisee) ) {
          throw new IllegalArgumentException("region " + regionName + " is not distributed and does not support interest registration");
        }
        FilterProfile p = r.getFilterProfile();
        keysRegistered = p.registerClientInterest(id, keyOfInterest, interestType, sendUpdatesAsInvalidates);
        regions.add(regionName);
      }
      // Perform actions if any keys were registered  
      if ((keysRegistered != null) && containsInterestRegistrationListeners()
          && !keysRegistered.isEmpty()) {
        handleInterestEvent(regionName, keysRegistered, interestType, true);
      } 
    }
    
    
    protected FilterProfile getProfile(String regionName) {
      try {
        return this.ccp._cache.getFilterProfile(regionName);
      } catch (CacheClosedException e) {
        return null;
      }
    }

    /**
     * Unregisters interest in the input region name and key
     *
     * @param regionName
     *          The fully-qualified name of the region in which to unregister
     *          interest
     * @param keyOfInterest
     *          The key in which to unregister interest
     */
    protected void unregisterClientInterest(String regionName,
        Object keyOfInterest, int interestType)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: unregisterClientInterest region={} key={}", ccp, regionName, keyOfInterest);
      }
      FilterProfile p = getProfile(regionName);
      Set keysUnregistered = null;
      synchronized(this.interestListLock) {
        if (p != null) {
          keysUnregistered = p.unregisterClientInterest(
            id, keyOfInterest, interestType);
          if (!p.hasInterestFor(id)) {
            this.regions.remove(regionName);
          }
        } else {
          this.regions.remove(regionName);
        }
      }
      if (keysUnregistered != null && !keysUnregistered.isEmpty()) { 
        handleInterestEvent(regionName, keysUnregistered, interestType, false); 
      } 
    }

    /**
     * Registers interest in the input region name and list of keys
     *
     * @param regionName
     *          The fully-qualified name of the region in which to register
     *          interest
     * @param keysOfInterest
     *          The list of keys in which to register interest
     */
    protected void registerClientInterestList(String regionName,
        List keysOfInterest, boolean sendUpdatesAsInvalidates) {
      FilterProfile p = getProfile(regionName);
      if (p == null) {
        throw new RegionDestroyedException("Region not found during client interest registration", regionName);
      }
      Set keysRegistered = null;
      synchronized(this.interestListLock) {
        keysRegistered = p.registerClientInterestList(id, keysOfInterest, sendUpdatesAsInvalidates);
        regions.add(regionName);
      }
      // Perform actions if any keys were registered 
      if (containsInterestRegistrationListeners() && !keysRegistered.isEmpty()) {
        handleInterestEvent(regionName, keysRegistered, InterestType.KEY, true);
      } 
    }

    /**
     * Unregisters interest in the input region name and list of keys
     *
     * @param regionName
     *          The fully-qualified name of the region in which to unregister
     *          interest
     * @param keysOfInterest
     *          The list of keys in which to unregister interest
     */
    protected void unregisterClientInterestList(String regionName,
        List keysOfInterest)
    {
      FilterProfile p = getProfile(regionName);
      Set keysUnregistered = null;
      synchronized(this.interestListLock) {
        if (p != null) {
          keysUnregistered = p.unregisterClientInterestList(
              id, keysOfInterest);
          if (!p.hasInterestFor(id)) {
            regions.remove(regionName);
          }
        } else {
          regions.remove(regionName);
        }
      }
     // Perform actions if any keys were unregistered
      if (!keysUnregistered.isEmpty()) { 
        handleInterestEvent(regionName, keysUnregistered, InterestType.KEY,false); 
      } 
    }

    /*
     * Returns whether this interest list has any keys, patterns or filters of
     * interest. It answers the question: Are any clients being notified because
     * of this interest list? @return whether this interest list has any keys,
     * patterns or filters of interest
     */
    protected boolean hasInterest() {
      return regions.size() > 0;
    }

    protected void clearClientInterestList() {
      boolean isClosed = ccp.getCache().isClosed();
      
      synchronized(this.interestListLock) {
        for (String regionName: regions) {
          FilterProfile p = getProfile(regionName);
          if (p == null) {
            continue;
          }
          if (!isClosed) {
            if (p.hasAllKeysInterestFor(id)) {
              Set allKeys = new HashSet();
              allKeys.add(".*");
              allKeys = Collections.unmodifiableSet(allKeys);
              handleInterestEvent(regionName, allKeys,
                  InterestType.REGULAR_EXPRESSION, false);
            }
            Set keysOfInterest = p.getKeysOfInterestFor(id);
            if (keysOfInterest != null && keysOfInterest.size() > 0) {
              handleInterestEvent(regionName, keysOfInterest,
                  InterestType.KEY, false);
            }
            Map<String,Pattern> patternsOfInterest =
              p.getPatternsOfInterestFor(id);
            if (patternsOfInterest != null && patternsOfInterest.size() > 0) {
              handleInterestEvent(regionName, patternsOfInterest.keySet(),
                  InterestType.REGULAR_EXPRESSION, false);
            }
          }
          p.clearInterestFor(id);
        }
        regions.clear();
      }
    }


    private void handleInterestEvent(String regionName, Set keysOfInterest,
        int interestType, boolean isRegister) {
      // Notify the region about this register interest event if:
      // - the application has requested it
      // - this is a primary CacheClientProxy (otherwise multiple notifications
      // may occur)
      // - it is a key interest type (regex is currently not supported)
      InterestRegistrationEvent event = null;
      if (NOTIFY_REGION_ON_INTEREST && this.ccp.isPrimary()
          && interestType == InterestType.KEY) {
        event = new InterestRegistrationEventImpl(this.ccp, regionName,
            keysOfInterest, interestType, isRegister);
        try {
          notifyRegionOfInterest(event);
        }
        catch (Exception e) {
          logger.warn(LocalizedStrings.CacheClientProxy_REGION_NOTIFICATION_OF_INTEREST_FAILED, e);
        }
      }
      // Invoke interest registration listeners
      if (containsInterestRegistrationListeners()) {
        if (event == null) {
          event = new InterestRegistrationEventImpl(this.ccp, regionName,
              keysOfInterest, interestType, isRegister);
        }
        notifyInterestRegistrationListeners(event);
      }      
    }

    private void notifyRegionOfInterest(InterestRegistrationEvent event) {
      this.ccp.getCacheClientNotifier().handleInterestEvent(event);
    }

    private void notifyInterestRegistrationListeners(
        InterestRegistrationEvent event) {
      this.ccp.getCacheClientNotifier().notifyInterestRegistrationListeners(
          event);
    }

    private boolean containsInterestRegistrationListeners() {
      return this.ccp.getCacheClientNotifier()
          .containsInterestRegistrationListeners();
    }
  }


  /**
   * Class <code>MessageDispatcher</code> is a <code>Thread</code> that
   * processes messages bound for the client by taking messsages from the
   * message queue and sending them to the client over the socket.
   */
  static class MessageDispatcher extends Thread
   {

    /**
     * The queue of messages to be sent to the client
     */
    protected final HARegionQueue _messageQueue;

//    /**
//     * An int used to keep track of the number of messages dropped for logging
//     * purposes. If greater than zero then a warning has been logged about
//     * messages being dropped.
//     */
//    private int _numberOfMessagesDropped = 0;

    /**
     * The proxy for which this dispatcher is processing messages
     */
    private final CacheClientProxy _proxy;

//    /**
//     * The conflator faciliates message conflation
//     */
//     protected BridgeEventConflator _eventConflator;

    /**
     * Whether the dispatcher is stopped
     */
    private volatile boolean _isStopped = true;

    /**
     * @guarded.By _pausedLock
     */
    //boolean _isPausedDispatcher = false;
    
    /**
     * A lock object used to control pausing this dispatcher
     */
    protected final Object _pausedLock = new Object();

    /**
     * An object used to protect when dispatching is being stopped.
     */
    private final Object _stopDispatchingLock = new Object();

    private final ReadWriteLock socketLock = new ReentrantReadWriteLock();

    private final Lock socketWriteLock = socketLock.writeLock();
//    /**
//     * A boolean verifying whether a warning has already been issued if the
//     * message queue has reached its capacity.
//     */
//    private boolean _messageQueueCapacityReachedWarning = false;

    /**
     * Constructor.
     *
     * @param proxy
     *          The <code>CacheClientProxy</code> for which this dispatcher is
     *          processing messages
     * @param name thread name for this dispatcher
     * @throws CacheException
     */
    protected MessageDispatcher(CacheClientProxy proxy, String name) throws CacheException {
      super(LoggingThreadGroup.createThreadGroup(name, logger), name);

      setDaemon(true);

      this._proxy = proxy;

      // Create the event conflator
      // this._eventConflator = new BridgeEventConflator

      // Create the message queue
      try {
        HARegionQueueAttributes harq= new HARegionQueueAttributes();
        harq.setBlockingQueueCapacity(proxy._maximumMessageCount);
        harq.setExpiryTime(proxy._messageTimeToLive);
        ((HAContainerWrapper)proxy._cacheClientNotifier.getHaContainer())
            .putProxy(HARegionQueue.createRegionName(getProxy()
                .getHARegionName()), getProxy());
        boolean createDurableQueue = proxy.proxyID.isDurable();            
        boolean canHandleDelta = (proxy.clientVersion.compareTo(Version.GFE_61) >= 0)
            && InternalDistributedSystem.getAnyInstance().getConfig()
                .getDeltaPropagation()
            && !(this._proxy.clientConflation == HandShake.CONFLATION_ON);
        if ((createDurableQueue || canHandleDelta) && logger.isDebugEnabled()) {
          logger.debug("Creating a durable HA queue");
        }
        this._messageQueue = 
          HARegionQueue.getHARegionQueueInstance(
              getProxy().getHARegionName(), getCache(), harq,
              HARegionQueue.BLOCKING_HA_QUEUE, createDurableQueue, 
              proxy._cacheClientNotifier.getHaContainer(),
              proxy.getProxyID(),
              this._proxy.clientConflation,
              this._proxy.isPrimary(), canHandleDelta);
        // Check if interests were registered during HARegion GII.
        if (this._proxy.hasRegisteredInterested()) {
          this._messageQueue.setHasRegisteredInterest(true);
        }
      }
      catch (CancelException e) {
        throw e;
      }
      catch (RegionExistsException ree) {
        throw ree;
      }
      catch (Exception e) {
        getCache().getCancelCriterion().checkCancelInProgress(e);
        throw new CacheException(LocalizedStrings.CacheClientProxy_EXCEPTION_OCCURRED_WHILE_TRYING_TO_CREATE_A_MESSAGE_QUEUE.toLocalizedString(), e) {
          private static final long serialVersionUID = 0L;};
      }
    }
    
    private CacheClientProxy getProxy() {
      return this._proxy;
    }
    private GemFireCacheImpl getCache() {
      return getProxy().getCache();
    }
    private Socket getSocket() {
      return getProxy().getSocket();
    }
    private ByteBuffer getCommBuffer() {
      return getProxy().getCommBuffer();
    }
    private CacheClientProxyStats getStatistics() {
      return getProxy().getStatistics();
    }

    private void basicStopDispatching() {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: notified dispatcher to stop", this);
      }
      this._isStopped = true;
      // this.interrupt(); // don't interrupt here. Let close(boolean) do this.
    }

    @Override
    public String toString() {
      return getProxy().toString();
    }

    /**
     * Notifies the dispatcher to stop dispatching.
     *
     * @param checkQueue
     *          Whether to check the message queue for any unprocessed messages
     *          and process them for MAXIMUM_SHUTDOWN_PEEKS.
     *
     * @see CacheClientProxy#MAXIMUM_SHUTDOWN_PEEKS
     */
    protected synchronized void stopDispatching(boolean checkQueue)
    {
      if (isStopped()) {     
        return;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("{}: Stopping dispatching", this);
      }
      if (!checkQueue) {
        basicStopDispatching();
        return;
      }

      // Stay alive until the queue is empty or a number of peeks is reached.
      List events = null;
      try {
        for (int numberOfPeeks = 0; numberOfPeeks < MAXIMUM_SHUTDOWN_PEEKS;
            ++numberOfPeeks) {
          boolean interrupted = Thread.interrupted();
          try {
            events = this._messageQueue.peek(1, -1);
            if (events == null || events.size() == 0) {
              break;
            }
            if (logger.isDebugEnabled()){
           logger.debug("Waiting for client to drain queue: {}", _proxy.proxyID);
            }
            Thread.sleep(500);
          }
          catch (InterruptedException e) {
            interrupted = true;
            /*GemFireCache c = (GemFireCache)_cache;
            c.getDistributedSystem().getCancelCriterion().checkCancelInProgress(e);*/
          }
          catch (CancelException e) {
            break;
          }
          catch (CacheException e) {
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Exception occurred while trying to stop dispatching", this, e);
            }
          }
          finally {
            if (interrupted) Thread.currentThread().interrupt();
          }
        } // for
      }
      finally {
        basicStopDispatching();
      }
    }

    /**
     * Returns whether the dispatcher is stopped
     *
     * @return whether the dispatcher is stopped
     */
    protected boolean isStopped()
    {
      return this._isStopped;
    }

    /**
     * Returns the size of the queue for heuristic purposes. This size may be
     * changing concurrently if puts / gets are occurring at the same time.
     *
     * @return the size of the queue
     */
    protected int getQueueSize()
    {
      return this._messageQueue == null ? 0 : this._messageQueue.size();
    }
    
    /**
     * Returns the size of the queue calculated through stats
     * This includes events that have dispatched but have yet been removed
     * @return the size of the queue
     */
    protected int getQueueSizeStat()
    {
      if (this._messageQueue != null) {
        HARegionQueueStats stats = this._messageQueue.getStatistics();
        return ((int)(stats.getEventsEnqued() - stats.getEventsRemoved() - stats.getEventsConflated() - stats.getMarkerEventsConflated() - stats.getEventsExpired() - stats.getEventsRemovedByQrm() - stats.getEventsTaken() - stats.getNumVoidRemovals()));
      }
      return 0;
    }
    
    protected void drainClientCqEvents(ClientProxyMembershipID clientId, InternalCqQuery cqToClose) {
        this._messageQueue.closeClientCq(clientId, cqToClose);
    }
    
    /**
     * Runs the dispatcher by taking a message from the queue and sending it to
     * the client attached to this proxy.
     */
    @Override
    public void run()
    {
      boolean exceptionOccured = false;
      this._isStopped = false;

      if (logger.isDebugEnabled()) {
        logger.debug("{}: Beginning to process events", this);
      }
      // for testing purposes
      if (isSlowStartForTesting) {
        long slowStartTimeForTesting = Long.getLong(KEY_SLOW_START_TIME_FOR_TESTING,
            DEFAULT_SLOW_STARTING_TIME).longValue();
        long elapsedTime = 0;
        long startTime = System.currentTimeMillis();
        while ((slowStartTimeForTesting > elapsedTime) && isSlowStartForTesting) {
          try {
            Thread.sleep(500);
          }
          catch (InterruptedException ignore) {
            if (logger.isDebugEnabled()) {
              logger.debug("Slow start for testing interrupted");
            }
            break;
          }
          elapsedTime = System.currentTimeMillis() - startTime;
        }
        if(slowStartTimeForTesting < elapsedTime) {
          isSlowStartForTesting = false;
        }
      }

      ClientMessage clientMessage = null;
      while (!isStopped()) {
//        SystemFailure.checkFailure(); DM's stopper does this
        if (this._proxy._cache.getCancelCriterion().cancelInProgress() != null) {
          break;
        }
        try {
          // If paused, wait to be told to resume (or interrupted if stopped)
          if (getProxy().isPaused()) {
            try {
              // ARB: Before waiting for resumption, process acks from client. 
              // This will reduce the number of duplicates that a client receives after
              // reconnecting.
              if (this._messageQueue.size() > 0) {
                Thread.sleep(50);
              }
              while (!this._messageQueue.isEmptyAckList()&& this._messageQueue.isPeekInitialized()) {
                this._messageQueue.remove();
              }
            }
            catch (InterruptedException ex) {
              logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_SLEEP_INTERRUPTED, this));
            }
            waitForResumption();
          }
          try {
            clientMessage = (ClientMessage)this._messageQueue.peek();
          }
          catch (RegionDestroyedException skipped) {
            break;
          }
          getStatistics().setQueueSize(this._messageQueue.size());
          if (isStopped()) {
            break;
          }
          // Process the message
          long start = getStatistics().startTime();
          //// BUGFIX for BUG#38206 and BUG#37791
          boolean isDispatched = dispatchMessage(clientMessage);
          getStatistics().endMessage(start);
          if(isDispatched){
            this._messageQueue.remove();
            if (clientMessage instanceof ClientMarkerMessageImpl) {
              getProxy().markerEnqueued = false;
            }
          }
          clientMessage = null;
        }
        catch (IOException e) {
          // Added the synchronization below to ensure that exception handling
          // does not occur while stopping the dispatcher and vice versa.
          synchronized (this._stopDispatchingLock) {
            // An IOException occurred while sending a message to the
            // client. If the processor is not already stopped, assume
            // the client is dead and stop processing.
            if (!isStopped() && !getProxy().isPaused()) {
              if ("Broken pipe".equals(e.getMessage())) {
                logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_PROXY_CLOSING_DUE_TO_UNEXPECTED_BROKEN_PIPE_ON_SOCKET_CONNECTION, this));
              } else if ("Connection reset".equals(e.getMessage())) {
                logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_PROXY_CLOSING_DUE_TO_UNEXPECTED_RESET_ON_SOCKET_CONNECTION, this));
              }
              else if ("Connection reset by peer".equals(e.getMessage())) {
                logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_PROXY_CLOSING_DUE_TO_UNEXPECTED_RESET_BY_PEER_ON_SOCKET_CONNECTION, this));
              }
              else if ("Socket is closed".equals(e.getMessage()) || "Socket Closed".equals(e.getMessage())) {
                logger.info(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_PROXY_CLOSING_DUE_TO_SOCKET_BEING_CLOSED_LOCALLY, this));
              }
              else {
                logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_AN_UNEXPECTED_IOEXCEPTION_OCCURRED_SO_THE_PROXY_WILL_BE_CLOSED, this), e);
              }
              // Let the CacheClientNotifier discover the proxy is not alive.
              // See isAlive().
              // getProxy().close(false);

              pauseOrUnregisterProxy();
            } // _isStopped
          } // synchronized
          exceptionOccured = true;
        } // IOException
        catch (InterruptedException e) {
          // If the thread is paused, ignore the InterruptedException and
          // continue. The proxy is null if stopDispatching has been called.
          if (getProxy().isPaused()) {
            if (logger.isDebugEnabled()) {
              logger.debug("{}: interrupted because it is being paused. It will continue and wait for resumption.", this);
            }
            Thread.interrupted();
            continue;
          }

          // no need to reset the bit; we're exiting
          if (logger.isDebugEnabled()) {
            logger.debug("{}: interrupted", this);
          }
          break;
        }
        catch (CancelException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}: shutting down due to cancellation", this);
          }
          exceptionOccured = true; // message queue is defunct, don't try to read it.
          break;
        }
        catch (RegionDestroyedException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}: shutting down due to loss of message queue", this);
          }
          exceptionOccured = true; // message queue is defunct, don't try to read it.
          break;
        }
        catch (Exception e) {
          // An exception occured while processing a message. Since it
          // is not an IOException, the client may still be alive, so
          // continue processing.
          if (!isStopped()) {
            logger.fatal(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0__AN_UNEXPECTED_EXCEPTION_OCCURRED, this), e);
          }
        }
      }

      // Processing gets here if isStopped=true. What is this code below doing?
      List list = null;
      if(!exceptionOccured) {
      try {
        // Clear the interrupt status if any,
        Thread.interrupted();
        int size = this._messageQueue.size();
        list = this._messageQueue.peek(size);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: After flagging the dispatcher to stop , the residual List of messages to be dispatched={} size={}", this, list, list.size());
        }
        if (list.size() > 0) {
          long start = getStatistics().startTime();
          Iterator itr = list.iterator();
          while (itr.hasNext()) {
            dispatchMessage((ClientMessage)itr.next());
            getStatistics().endMessage(start);
            // @todo asif: shouldn't we call itr.remove() since the current msg
            //             has been sent? That way list will be more accurate
            //             if we have an exception.
          }
          this._messageQueue.remove();
        }
      }
      catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("CacheClientNotifier stopped due to cancellation");
        }
      }
      catch (Exception ignore) {
        //if (logger.isInfoEnabled()) {
          StringId extraMsg = null;
          
          if ("Broken pipe".equals(ignore.getMessage())) {            
            extraMsg = LocalizedStrings.CacheClientProxy_PROBLEM_CAUSED_BY_BROKEN_PIPE_ON_SOCKET;
          }
          else if (ignore instanceof RegionDestroyedException) {
            extraMsg = LocalizedStrings.CacheClientProxy_PROBLEM_CAUSED_BY_MESSAGE_QUEUE_BEING_CLOSED;
          }
          final Object[] msgArgs = new Object[] {
              ((!isStopped()) ? this.toString() + ": " : ""),
              ((list == null) ? 0 : list.size())};        
          if (extraMsg != null) {
            //Dont print exception details, but add on extraMsg
            logger.info(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_POSSIBILITY_OF_NOT_BEING_ABLE_TO_SEND_SOME_OR_ALL_THE_MESSAGES_TO_CLIENTS_TOTAL_MESSAGES_CURRENTLY_PRESENT_IN_THE_LIST_1, msgArgs));
            logger.info(extraMsg);
          } else {
            //Print full stacktrace
            logger.info(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_POSSIBILITY_OF_NOT_BEING_ABLE_TO_SEND_SOME_OR_ALL_THE_MESSAGES_TO_CLIENTS_TOTAL_MESSAGES_CURRENTLY_PRESENT_IN_THE_LIST_1, msgArgs), ignore);
          }
        }

        if (list != null && logger.isTraceEnabled()) {
          logger.trace("Messages remaining in the list are: {}", list);
        }

      //}
      }
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Dispatcher thread is ending", this);
      }

    }

    private void pauseOrUnregisterProxy() {
      if (getProxy().isDurable()) {
        try {
          getProxy().pauseDispatching();
        } catch (Exception ex) {
          // see bug 40611; we catch Exception here because
          // we once say an InterruptedException here.
          // log a warning saying we couldn't pause?
          if (logger.isDebugEnabled()) {
            logger.debug("{}: {}", this, ex);
          }
        }
      }
      else {
        this._isStopped = true;
      }

      // Stop the ServerConnections. This will force the client to
      // server communication to close.
      ClientHealthMonitor chm = ClientHealthMonitor.getInstance();

      // Note now that _proxy is final the following comment is no
      // longer true. the _isStopped check should be sufficient.
      // Added the test for this._proxy != null to prevent bug 35801.
      // The proxy could have been stopped after this IOException has
      // been caught and here, so the _proxy will be null.
      if (chm != null) {
        ClientProxyMembershipID proxyID = getProxy().proxyID;
        chm.removeAllConnectionsAndUnregisterClient(proxyID);
        if (!getProxy().isDurable()) {
          getProxy().getCacheClientNotifier().unregisterClient(proxyID, false);
        }
      }
    }
    
    /**
     * Sends a message to the client attached to this proxy
     *
     * @param clientMessage
     *          The <code>ClientMessage</code> to send to the client
     *
     * @throws IOException
     */
    protected boolean dispatchMessage(ClientMessage clientMessage)
        throws IOException
    {
      boolean isDispatched = false ;
      if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER)) {
        logger.trace(LogMarker.BRIDGE_SERVER, "Dispatching {}", clientMessage);
      }
      Message message = null;

      // byte[] latestValue =
      // this._eventConflator.getLatestValue(clientMessage);

     if (clientMessage instanceof ClientUpdateMessage) {
        byte[] latestValue = (byte[])((ClientUpdateMessage)clientMessage).getValue();
        if (logger.isTraceEnabled()) {
          StringBuilder msg = new StringBuilder(100);
          msg.append(this).append(": Using latest value: ").append(Arrays.toString(latestValue));
          if (((ClientUpdateMessage)clientMessage).valueIsObject()) {
            if (latestValue != null) {
              msg.append(" (").append(deserialize(latestValue)).append(")");
            }
            msg.append(" for ").append(clientMessage);
          }
          logger.trace(msg.toString());
        }
        
        message = ((ClientUpdateMessageImpl)clientMessage).getMessage(getProxy(),
            latestValue);
        
        if (AFTER_MESSAGE_CREATION_FLAG) {
          ClientServerObserver bo = ClientServerObserverHolder.getInstance();
          bo.afterMessageCreation(message);
        }
     }
     else {
       message = clientMessage.getMessage(getProxy(), true /* notify */);
     }

      // //////////////////////////////
      // TEST CODE BEGIN (Throws exception to test closing proxy)
      // if (true) throw new IOException("test");
      // TEST CODE END
      // //////////////////////////////
     // Message message = ((ClientUpdateMessageImpl)clientMessage).getMessage(getProxy().proxyID, latestValue);
     //Message message = clientMessage.getMessage(); removed during merge.
     // BugFix for BUG#38206 and BUG#37791
     if (!this._proxy.isPaused()) {
       sendMessage(message);

      // //////////////////////////////
      // TEST CODE BEGIN (Throws exception to test closing proxy)
      // if (true) throw new IOException("test");
      // TEST CODE END
      // //////////////////////////////
      //Message message = ((ClientUpdateMessageImpl)clientMessage).getMessage(getProxy().proxyID, latestValue);
      //Message message = clientMessage.getMessage(); removed during merge.
      //message.setComms(getSocket(), getCommBuffer(), getStatistics());
      //message.send();

      // //////////////////////////////
      // TEST CODE BEGIN (Introduces random wait in client)
      // Sleep a random number of ms
      // java.util.Random rand = new java.util.Random();
      // try {Thread.sleep(rand.nextInt(5));} catch (InterruptedException e) {}
      // TEST CODE END
      // //////////////////////////////

      if (logger.isTraceEnabled()) {
        logger.trace("{}: Dispatched {}", this, clientMessage);
      }
      isDispatched = true;
     }
     else {
       if (logger.isDebugEnabled()) {
         logger.debug("Message Dispatcher of a Paused CCProxy is trying to dispatch message");
       }
     }
     if (isDispatched) {
       this._messageQueue.getStatistics().incEventsDispatched();
     }
     return isDispatched;
    }
    
    private void sendMessage(Message message) throws IOException {
      if (message == null) {
        return;
      }
      this.socketWriteLock.lock();
      try {
        message.setComms(getSocket(), getCommBuffer(), getStatistics());
        message.send();
        getProxy().resetPingCounter();
      } finally {
        this.socketWriteLock.unlock();
      }
    }

    /**
     * Add the input client message to the message queue
     *
     * @param clientMessage
     *          The <code>Conflatable</code> to add to the queue
     */
    protected void enqueueMessage(Conflatable clientMessage)
    {
      try {
          this._messageQueue.put(clientMessage);
          if(this._proxy.isPaused() && this._proxy.isDurable()){
            this._proxy._cacheClientNotifier._statistics.incEventEnqueuedWhileClientAwayCount();
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Queued message while Durable Client is away {}", this, clientMessage);
            }
          } else {
// [bruce] we don't really know that it was added, so don't log this
//            if (logger.isDebugEnabled() || BridgeServerImpl.VERBOSE) {
//              logger.debug(LocalizedStrings.DEBUG, this + " added message to queue: " + clientMessage);
//          }
      }
      }
      catch (CancelException e) {
        throw e;
      }
      catch (Exception e) {
        if (!isStopped()) {
          this._proxy._statistics.incMessagesFailedQueued();
          logger.fatal(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0_EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_ADD_MESSAGE_TO_QUEUE, this), e);
        }
      }
    }


    protected void enqueueMarker(ClientMessage message) {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Queueing marker message. <{}>. The queue contains {} entries.", this, message, getQueueSize());
        }
        this._messageQueue.put(message);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Queued marker message. The queue contains {} entries.", this, getQueueSize());
        }
      }
      catch (CancelException e) {
        throw e;
      }
      catch (Exception e) {
        if (!isStopped()) {
          logger.fatal(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0__EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_ADD_MESSAGE_TO_QUEUE, this), e);
        }
      }
    }

    private void sendMessageDirectly(ClientMessage clientMessage) {
      Message message;
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Dispatching directly: {}", this, clientMessage);
        }
        message = clientMessage.getMessage(getProxy(), true);
        sendMessage(message);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Dispatched directly: {}", this, clientMessage);
        }
        // The exception handling code was modeled after the MessageDispatcher
        // run method
      } catch (IOException e) {
        synchronized (this._stopDispatchingLock) {
          // Pause or unregister proxy
          if (!isStopped() && !getProxy().isPaused()) {
            logger.fatal(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0__AN_UNEXPECTED_EXCEPTION_OCCURRED, this), e);
            pauseOrUnregisterProxy();
          }
        }
      } catch (Exception e) {
        if (!isStopped()) {
          logger.fatal(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0__AN_UNEXPECTED_EXCEPTION_OCCURRED, this), e);
        }
      }
    }

    protected void waitForResumption() throws InterruptedException {
      synchronized (this._pausedLock) {
        logger.info(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0__PAUSING_PROCESSING, this));
        if (!getProxy().isPaused()) {
          return;
        }
        while (getProxy().isPaused()) {
          this._pausedLock.wait();
        }
        // Fix for #48571
        _messageQueue.clearPeekedIDs();
      }
    }

    protected void resumeDispatching() {
        logger.info(LocalizedMessage.create(LocalizedStrings.CacheClientProxy_0__RESUMING_PROCESSING, this));

        // Notify thread to resume
        this._pausedLock.notifyAll();
    }

    protected Object deserialize(byte[] serializedBytes)
    {
      Object deserializedObject = serializedBytes;
      // This is a debugging method so ignore all exceptions like
      // ClassNotFoundException
      try {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
            serializedBytes));
        deserializedObject = DataSerializer.readObject(dis);
      }
      catch (Exception e) {
      }
      return deserializedObject;
    }
    
    protected void initializeTransients()
    {
        while (!this._messageQueue.isEmptyAckList()&& this._messageQueue.isPeekInitialized()) {
            try {
                this._messageQueue.remove();
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }
      this._messageQueue.initializeTransients();
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
  public  void incCqCount() {
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
   * @since 6.1
   */
  public Map getRegionsWithEmptyDataPolicy() {
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
   * Returns the number of seconds that have elapsed since the Client proxy
   * created.
   * 
   * @since 7.0
   */
  public long getUpTime() {
    return (long) ((System.currentTimeMillis() - this.creationDate.getTime()) / 1000);
  }

  public interface TestHook {
    public void doTestHook(String spot);
  }
  public static TestHook testHook;
}
