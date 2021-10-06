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

import static org.apache.geode.internal.cache.EntryEventImpl.deserialize;
import static org.apache.geode.internal.cache.tier.sockets.ClientReAuthenticateMessage.RE_AUTHENTICATION_START_VERSION;
import static org.apache.geode.internal.lang.SystemPropertyHelper.RE_AUTHENTICATE_WAIT_TIME;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.ha.HAContainerWrapper;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.ha.HARegionQueueAttributes;
import org.apache.geode.internal.cache.ha.HARegionQueueStats;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.AuthenticationExpiredException;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

/**
 * Class <code>MessageDispatcher</code> is a <code>Thread</code> that processes messages bound for
 * the client by taking messsages from the message queue and sending them to the client over the
 * socket.
 */
public class MessageDispatcher extends LoggingThread {
  private static final Logger logger = LogService.getLogger();

  /**
   * Default value for slow starting time of dispatcher
   */
  private static final long DEFAULT_SLOW_STARTING_TIME = 5000;

  /**
   * Key in the system property from which the slow starting time value will be retrieved
   */
  private static final String KEY_SLOW_START_TIME_FOR_TESTING = "slowStartTimeForTesting";


  /**
   * Default value in milliseconds for waiting for re-authentication
   */
  private static final long DEFAULT_RE_AUTHENTICATE_WAIT_TIME = 5000;

  /**
   * The queue of messages to be sent to the client
   */
  protected final HARegionQueue _messageQueue;

  // /**
  // * An int used to keep track of the number of messages dropped for logging
  // * purposes. If greater than zero then a warning has been logged about
  // * messages being dropped.
  // */
  // private int _numberOfMessagesDropped = 0;

  /**
   * The proxy for which this dispatcher is processing messages
   */
  private final CacheClientProxy _proxy;

  // /**
  // * The conflator faciliates message conflation
  // */
  // protected BridgeEventConflator _eventConflator;

  /**
   * Whether the dispatcher is stopped
   */
  private volatile boolean _isStopped = true;

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
  // /**
  // * A boolean verifying whether a warning has already been issued if the
  // * message queue has reached its capacity.
  // */
  // private boolean _messageQueueCapacityReachedWarning = false;

  /**
   * Constructor.
   *
   * @param proxy The <code>CacheClientProxy</code> for which this dispatcher is processing
   *        messages
   * @param name thread name for this dispatcher
   */
  protected MessageDispatcher(CacheClientProxy proxy, String name,
      StatisticsClock statisticsClock) throws CacheException {
    this(proxy, name, getMessageQueue(proxy, statisticsClock));
  }

  private static HARegionQueue getMessageQueue(CacheClientProxy proxy,
      StatisticsClock statisticsClock) {
    try {
      HARegionQueueAttributes harq = new HARegionQueueAttributes();
      harq.setBlockingQueueCapacity(proxy._maximumMessageCount);
      harq.setExpiryTime(proxy._messageTimeToLive);
      ((HAContainerWrapper) proxy._cacheClientNotifier.getHaContainer())
          .putProxy(HARegionQueue.createRegionName(proxy.getHARegionName()), proxy);
      boolean createDurableQueue = proxy.proxyID.isDurable();
      boolean canHandleDelta =
          InternalDistributedSystem.getAnyInstance().getConfig().getDeltaPropagation()
              && !(proxy.clientConflation == Handshake.CONFLATION_ON);
      if ((createDurableQueue || canHandleDelta) && logger.isDebugEnabled()) {
        logger.debug("Creating a {} subscription queue for {}",
            createDurableQueue ? "durable" : "non-durable",
            proxy.getProxyID());
      }
      return HARegionQueue.getHARegionQueueInstance(proxy.getHARegionName(),
          proxy.getCache(), harq, HARegionQueue.BLOCKING_HA_QUEUE, createDurableQueue,
          proxy._cacheClientNotifier.getHaContainer(), proxy.getProxyID(),
          proxy.clientConflation, proxy.isPrimary(), canHandleDelta, statisticsClock);
    } catch (CancelException | RegionExistsException e) {
      throw e;
    } catch (Exception e) {
      proxy.getCache().getCancelCriterion().checkCancelInProgress(e);
      throw new CacheException(
          "Exception occurred while trying to create a message queue.",
          e) {
        private static final long serialVersionUID = 0L;
      };
    }
  }

  protected MessageDispatcher(CacheClientProxy proxy, String name, HARegionQueue messageQueue)
      throws CacheException {
    super(name);
    _proxy = proxy;
    _messageQueue = messageQueue;
    // Check if interests were registered during HARegion GII.
    if (proxy.hasRegisteredInterested()) {
      _messageQueue.setHasRegisteredInterest(true);
    }
  }

  private CacheClientProxy getProxy() {
    return _proxy;
  }

  private InternalCache getCache() {
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
    _isStopped = true;
    // this.interrupt(); // don't interrupt here. Let close(boolean) do this.
  }

  @Override
  public String toString() {
    return getProxy().toString();
  }

  /**
   * Notifies the dispatcher to stop dispatching.
   *
   * @param checkQueue Whether to check the message queue for any unprocessed messages and process
   *        them for MAXIMUM_SHUTDOWN_PEEKS.
   *
   * @see CacheClientProxy#MAXIMUM_SHUTDOWN_PEEKS
   */
  protected synchronized void stopDispatching(boolean checkQueue) {
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
    try {
      for (int numberOfPeeks =
          0; numberOfPeeks < CacheClientProxy.MAXIMUM_SHUTDOWN_PEEKS; ++numberOfPeeks) {
        boolean interrupted = Thread.interrupted();
        try {
          List<?> events = _messageQueue.peek(1, -1);
          if (events == null || events.size() == 0) {
            break;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Waiting for client to drain queue: {}", _proxy.proxyID);
          }
          Thread.sleep(500);
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (CancelException e) {
          break;
        } catch (CacheException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Exception occurred while trying to stop dispatching", this, e);
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // for
    } finally {
      basicStopDispatching();
    }
  }

  /**
   * Returns whether the dispatcher is stopped
   *
   * @return whether the dispatcher is stopped
   */
  protected boolean isStopped() {
    return _isStopped;
  }

  /**
   * Returns the size of the queue for heuristic purposes. This size may be changing concurrently
   * if puts / gets are occurring at the same time.
   *
   * @return the size of the queue
   */
  protected int getQueueSize() {
    return _messageQueue == null ? 0 : _messageQueue.size();
  }

  /**
   * Returns the size of the queue calculated through stats This includes events that have
   * dispatched but have yet been removed
   *
   * @return the size of the queue
   */
  protected int getQueueSizeStat() {
    if (_messageQueue != null) {
      HARegionQueueStats stats = _messageQueue.getStatistics();
      return ((int) (stats.getEventsEnqued() - stats.getEventsRemoved()
          - stats.getEventsConflated() - stats.getMarkerEventsConflated()
          - stats.getEventsExpired() - stats.getEventsRemovedByQrm() - stats.getEventsTaken()
          - stats.getNumVoidRemovals()));
    }
    return 0;
  }

  protected void drainClientCqEvents(ClientProxyMembershipID clientId,
      InternalCqQuery cqToClose) {
    _messageQueue.closeClientCq(clientId, cqToClose);
  }

  @Override
  public void run() {
    // for testing purposes
    if (CacheClientProxy.isSlowStartForTesting) {
      long slowStartTimeForTesting =
          Long.getLong(KEY_SLOW_START_TIME_FOR_TESTING, DEFAULT_SLOW_STARTING_TIME);
      long elapsedTime = 0;
      long startTime = System.currentTimeMillis();
      while ((slowStartTimeForTesting > elapsedTime) && CacheClientProxy.isSlowStartForTesting) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException ignore) {
          if (logger.isDebugEnabled()) {
            logger.debug("Slow start for testing interrupted");
          }
          break;
        }
        elapsedTime = System.currentTimeMillis() - startTime;
      }
      if (slowStartTimeForTesting < elapsedTime) {
        CacheClientProxy.isSlowStartForTesting = false;
      }
    }

    runDispatcher();
  }

  /**
   * Runs the dispatcher by taking a message from the queue and sending it to the client attached
   * to this proxy.
   */
  @VisibleForTesting
  protected void runDispatcher() {
    boolean exceptionOccurred = false;
    _isStopped = false;

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Beginning to process events", this);
    }

    long reAuthenticateWaitTime =
        getSystemProperty(RE_AUTHENTICATE_WAIT_TIME, DEFAULT_RE_AUTHENTICATE_WAIT_TIME);

    ClientMessage clientMessage = null;
    long wait_for_re_auth_start_time = -1;
    while (!isStopped()) {
      // SystemFailure.checkFailure(); DM's stopper does this
      if (getCache().getCancelCriterion().isCancelInProgress()) {
        break;
      }
      try {
        // If paused, wait to be told to resume (or interrupted if stopped)
        if (getProxy().isPaused()) {
          // ARB: Before waiting for resumption, process acks from client.
          // This will reduce the number of duplicates that a client receives after
          // reconnecting.
          synchronized (_pausedLock) {
            try {
              logger.info("available ids = " + _messageQueue.size() + " , isEmptyAckList ="
                  + _messageQueue.isEmptyAckList() + ", peekInitialized = "
                  + _messageQueue.isPeekInitialized());
              while (!_messageQueue.isEmptyAckList()
                  && _messageQueue.isPeekInitialized()) {
                _messageQueue.remove();
              }
            } catch (InterruptedException ex) {
              logger.warn("{}: sleep interrupted.", this);
            }
          }
          waitForResumption();
        }

        // if message is not delivered due to authentication expiation, this clientMessage
        // would not be null.
        if (clientMessage == null) {
          try {
            clientMessage = (ClientMessage) _messageQueue.peek();
          } catch (RegionDestroyedException skipped) {
            break;
          }
        }

        getStatistics().setQueueSize(_messageQueue.size());
        if (isStopped()) {
          break;
        }

        if (clientMessage == null) {
          _messageQueue.remove();
          continue;
        }

        // Process the message
        long start = getStatistics().startTime();
        try {
          if (dispatchMessage(clientMessage)) {
            getStatistics().endMessage(start);
            _messageQueue.remove();
            if (clientMessage instanceof ClientMarkerMessageImpl) {
              getProxy().setMarkerEnqueued(false);
            }
          }
          clientMessage = null;
          wait_for_re_auth_start_time = -1;
        } catch (NotAuthorizedException notAuthorized) {
          // behave as if the message is dispatched, remove from the queue
          logger.warn("skip delivering message: " + clientMessage, notAuthorized);
          _messageQueue.remove();
          clientMessage = null;
        } catch (AuthenticationExpiredException expired) {
          if (wait_for_re_auth_start_time == -1) {
            wait_for_re_auth_start_time = System.currentTimeMillis();
            // only send the message to clients who can handle the message
            if (getProxy().getVersion().isNewerThanOrEqualTo(RE_AUTHENTICATION_START_VERSION)) {
              EventID eventId = createEventId();
              sendMessageDirectly(new ClientReAuthenticateMessage(eventId));
            }
            // We wait for all versions of clients to re-authenticate. For older clients we still
            // wait, just in case client will perform some operations to
            // trigger credential refresh on its own.
            Thread.sleep(200);
          } else {
            long elapsedTime = System.currentTimeMillis() - wait_for_re_auth_start_time;
            if (elapsedTime > reAuthenticateWaitTime) {
              synchronized (_stopDispatchingLock) {
                logger.warn("Client did not re-authenticate back successfully in " + elapsedTime
                    + "ms. Unregister this client proxy.");
                pauseOrUnregisterProxy(expired);
              }
              exceptionOccurred = true;
            } else {
              Thread.sleep(200);
            }
          }
        }

      } catch (MessageTooLargeException e) {
        logger.warn("Message too large to send to client: {}, {}", clientMessage, e.getMessage());
      } catch (IOException e) {
        // Added the synchronization below to ensure that exception handling
        // does not occur while stopping the dispatcher and vice versa.
        synchronized (_stopDispatchingLock) {
          // An IOException occurred while sending a message to the
          // client. If the processor is not already stopped, assume
          // the client is dead and stop processing.
          if (!isStopped() && !getProxy().isPaused()) {
            if ("Broken pipe".equals(e.getMessage())) {
              logger.warn("{}: Proxy closing due to unexpected broken pipe on socket connection.",
                  this);
            } else if ("Connection reset".equals(e.getMessage())) {
              logger.warn("{}: Proxy closing due to unexpected reset on socket connection.",
                  this);
            } else if ("Connection reset by peer".equals(e.getMessage())) {
              logger.warn(
                  "{}: Proxy closing due to unexpected reset by peer on socket connection.",
                  this);
            } else if ("Socket is closed".equals(e.getMessage())
                || "Socket Closed".equals(e.getMessage())) {
              logger.info("{}: Proxy closing due to socket being closed locally.",
                  this);
            } else {
              logger.warn(String.format(
                  "%s: An unexpected IOException occurred so the proxy will be closed.",
                  this),
                  e);
            }
            // Let the CacheClientNotifier discover the proxy is not alive.
            // See isAlive().
            // getProxy().close(false);

            pauseOrUnregisterProxy(e);
          } // _isStopped
        } // synchronized
        exceptionOccurred = true;
      } // IOException
      catch (InterruptedException e) {
        // If the thread is paused, ignore the InterruptedException and
        // continue. The proxy is null if stopDispatching has been called.
        if (getProxy().isPaused()) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "{}: interrupted because it is being paused. It will continue and wait for resumption.",
                this);
          }
          Thread.interrupted();
          continue;
        }

        // no need to reset the bit; we're exiting
        if (logger.isDebugEnabled()) {
          logger.debug("{}: interrupted", this);
        }
        break;
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: shutting down due to cancellation", this);
        }
        exceptionOccurred = true; // message queue is defunct, don't try to read it.
        break;
      } catch (RegionDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: shutting down due to loss of message queue", this);
        }
        exceptionOccurred = true; // message queue is defunct, don't try to read it.
        break;
      } catch (Exception e) {
        // An exception occurred while processing a message. Since it
        // is not an IOException, the client may still be alive, so
        // continue processing.
        if (!isStopped()) {
          logger.fatal(String.format("%s : An unexpected Exception occurred", this),
              e);
        }
      }
    }

    // Processing gets here if isStopped=true. What is this code below doing?
    if (!exceptionOccurred) {
      dispatchResidualMessages();
    }
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Dispatcher thread is ending", this);
    }
  }

  @VisibleForTesting
  void dispatchResidualMessages() {
    List<ClientMessage> list = new ArrayList<>();
    try {
      // Clear the interrupt status if any,
      Thread.interrupted();
      int size = _messageQueue.size();
      list.addAll(uncheckedCast(_messageQueue.peek(size)));
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: After flagging the dispatcher to stop , the residual List of messages to be dispatched={} size={}",
            this, list, list.size());
      }
      if (list.size() > 0) {
        long start = getStatistics().startTime();
        Iterator<ClientMessage> itr = list.iterator();
        while (itr.hasNext()) {
          dispatchMessage(itr.next());
          getStatistics().endMessage(start);
          itr.remove();
        }
        _messageQueue.remove();
      }
    } catch (CancelException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("CacheClientNotifier stopped due to cancellation");
      }
    } catch (Exception e) {
      String extraMsg = null;

      if ("Broken pipe".equals(e.getMessage())) {
        extraMsg = "Problem caused by broken pipe on socket.";
      } else if (e instanceof RegionDestroyedException) {
        extraMsg = "Problem caused by message queue being closed.";
      }
      if (extraMsg == null) {
        extraMsg = "Problem caused by: " + e.getMessage();
      }
      logger.info(String.format(
          "%s Possibility of not being able to send some or all of the messages to clients. Total messages currently present in the list %s.",
          (!isStopped()) ? toString() + ": " : "", list.size()));
      logger.info(extraMsg);
    }

    if (!list.isEmpty() && logger.isTraceEnabled()) {
      logger.trace("Messages remaining in the list are: {}", list);
    }
  }

  @NotNull
  @VisibleForTesting
  protected Long getSystemProperty(String key, long defaultValue) {
    return SystemPropertyHelper.getProductLongProperty(key, defaultValue);
  }

  @NotNull
  @VisibleForTesting
  protected EventID createEventId() {
    return new EventID(getCache().getDistributedSystem());
  }

  @VisibleForTesting
  protected void pauseOrUnregisterProxy(Throwable t) {
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
    } else {
      _isStopped = true;
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
      chm.removeAllConnectionsAndUnregisterClient(proxyID, t);
      if (!getProxy().isDurable()) {
        getProxy().getCacheClientNotifier().unregisterClient(proxyID, false);
      }
    }
  }

  /**
   * Sends a message to the client attached to this proxy
   *
   * @param clientMessage The <code>ClientMessage</code> to send to the client
   *
   */
  protected boolean dispatchMessage(ClientMessage clientMessage) throws IOException {
    boolean isDispatched = false;
    if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
      logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, "Dispatching {}", clientMessage);
    }

    final Message message;
    if (clientMessage instanceof ClientUpdateMessage) {
      ClientUpdateMessage clientUpdateMessage = (ClientUpdateMessage) clientMessage;
      byte[] latestValue = (byte[]) clientUpdateMessage.getValue();
      if (logger.isTraceEnabled()) {
        StringBuilder msg = new StringBuilder(100);
        msg.append(this).append(": Using latest value: ").append(Arrays.toString(latestValue));
        if (clientUpdateMessage.valueIsObject()) {
          if (latestValue != null) {
            msg.append(" (").append(deserialize(latestValue)).append(")");
          }
          msg.append(" for ").append(clientMessage);
        }
        logger.trace(msg.toString());
      }

      // authorize the message before dispatching
      SecurityService securityService = getCache().getSecurityService();
      // for legacy security, we don't support auth expiration
      if (securityService.isIntegratedSecurity()) {
        if (getProxy().getSubject() != null) {
          securityService.authorize(getResourcePermission(clientUpdateMessage),
              getProxy().getSubject());
        } else {
          // if it's multi-user mode
          ClientUpdateMessageImpl.CqNameToOp clientCq =
              clientUpdateMessage.getClientCq(getProxy().getProxyID());
          if (clientCq != null) {
            String[] names = clientCq.getNames();
            for (String cqName : names) {
              Subject subject = getProxy().getSubject(cqName);
              if (subject != null) {
                securityService.authorize(getResourcePermission(clientUpdateMessage), subject);
              }
            }
          }
        }
      }
      message = ((ClientUpdateMessageImpl) clientMessage).getMessage(getProxy(), latestValue);

      if (CacheClientProxy.AFTER_MESSAGE_CREATION_FLAG) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.afterMessageCreation(message);
      }
    } else {
      message = clientMessage.getMessage(getProxy(), true /* notify */);
    }

    if (!_proxy.isPaused()) {
      sendMessage(message);

      if (logger.isTraceEnabled()) {
        logger.trace("{}: Dispatched {}", this, clientMessage);
      }
      isDispatched = true;
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Message Dispatcher of a Paused CCProxy is trying to dispatch message");
      }
    }
    if (isDispatched) {
      _messageQueue.getStatistics().incEventsDispatched();
    }
    return isDispatched;
  }

  @NotNull
  private ResourcePermission getResourcePermission(ClientUpdateMessage message) {
    String regionName = message.getRegionName();
    Object key = message.getKeyOfInterest();
    return new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ,
        regionName, key == null ? null : key.toString());
  }

  @VisibleForTesting
  protected void sendMessage(Message message) throws IOException {
    if (message == null) {
      return;
    }
    socketWriteLock.lock();
    try {
      message.setComms(getSocket(), getCommBuffer(), getStatistics());
      message.send();
      getProxy().resetPingCounter();
    } finally {
      socketWriteLock.unlock();
    }
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Sent {}", this, message);
    }
  }

  /**
   * Add the input client message to the message queue
   *
   * @param clientMessage The <code>Conflatable</code> to add to the queue
   */
  protected void enqueueMessage(Conflatable clientMessage) {
    try {
      long startTime = _proxy._statistics.startMessageQueueStats();
      _messageQueue.put(clientMessage);
      _proxy._statistics.endMessageQueueStats(startTime);
      _proxy._statistics.setQueueSize(_messageQueue.size());

      if (_proxy.isPaused() && _proxy.isDurable()) {
        _proxy._cacheClientNotifier.statistics.incEventEnqueuedWhileClientAwayCount();
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Queued message while Durable Client is away {}", this, clientMessage);
        }
      }
    } catch (CancelException e) {
      throw e;
    } catch (Exception e) {
      if (!isStopped()) {
        _proxy._statistics.incMessagesFailedQueued();
        logger.fatal(
            String.format("%s: Exception occurred while attempting to add message to queue",
                this),
            e);
      }
    }
  }


  protected void enqueueMarker(ClientMessage message) {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Queueing marker message. <{}>. The queue contains {} entries.", this,
            message, getQueueSize());
      }
      long startTime = _proxy._statistics.startMessageQueueStats();
      _messageQueue.put(message);
      _proxy._statistics.endMessageQueueStats(startTime);
      _proxy._statistics.setQueueSize(_messageQueue.size());
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Queued marker message. The queue contains {} entries.", this,
            getQueueSize());
      }
    } catch (CancelException e) {
      throw e;
    } catch (Exception e) {
      if (!isStopped()) {
        logger.fatal(
            String.format("%s : Exception occurred while attempting to add message to queue",
                this),
            e);
      }
    }
  }

  void sendMessageDirectly(ClientMessage clientMessage) {
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
    } catch (MessageTooLargeException e) {
      logger.warn("Message too large to send to client: {}, {}", clientMessage, e.getMessage());

    } catch (IOException e) {
      synchronized (_stopDispatchingLock) {
        // Pause or unregister proxy
        if (!isStopped() && !getProxy().isPaused()) {
          logger.fatal(String.format("%s : An unexpected Exception occurred", this),
              e);
          pauseOrUnregisterProxy(e);
        }
      }
    } catch (Exception e) {
      if (!isStopped()) {
        logger.fatal(String.format("%s : An unexpected Exception occurred", this), e);
      }
    }
  }

  protected void waitForResumption() throws InterruptedException {
    synchronized (_pausedLock) {
      logger.info("{} : Pausing processing", this);
      if (!getProxy().isPaused()) {
        return;
      }
      while (getProxy().isPaused()) {
        _pausedLock.wait();
      }
      // Fix for #48571
      _messageQueue.clearPeekedIDs();
    }
  }

  protected void resumeDispatching() {
    logger.info("{} : Resuming processing", this);

    // Notify thread to resume
    _pausedLock.notifyAll();
  }

  protected Object deserialize(byte[] serializedBytes) {
    Object deserializedObject = serializedBytes;
    // This is a debugging method so ignore all exceptions like
    // ClassNotFoundException
    try (ByteArrayDataInput dis = new ByteArrayDataInput(serializedBytes)) {
      deserializedObject = DataSerializer.readObject(dis);
    } catch (Exception ignore) {
    }
    return deserializedObject;
  }

  protected void initializeTransients() {
    while (!_messageQueue.isEmptyAckList() && _messageQueue.isPeekInitialized()) {
      try {
        _messageQueue.remove();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    _messageQueue.initializeTransients();
  }
}
