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
package org.apache.geode.cache.query.internal.cq;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.ServerCQProxyImpl;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesMutator;
import org.apache.geode.cache.query.CqClosedException;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqResults;
import org.apache.geode.cache.query.CqStatusListener;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.internal.CqStateImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.security.GemFireSecurityException;

public class ClientCQImpl extends CqQueryImpl implements ClientCQ {
  private static final Logger logger = LogService.getLogger();

  private CqAttributes cqAttributes = null;

  private volatile ServerCQProxyImpl cqProxy;

  private ProxyCache proxyCache = null;

  /**
   * To queue the CQ Events arriving during CQ execution with initial Results.
   */
  private volatile ConcurrentLinkedQueue<CqEventImpl> queuedEvents = null;

  final Object queuedEventsSynchObject = new Object();

  private boolean connected = false;

  public ClientCQImpl(CqServiceImpl cqService, String cqName, String queryString,
      CqAttributes cqAttributes, ServerCQProxyImpl serverProxy, boolean isDurable) {
    super(cqService, cqName, queryString, isDurable);
    this.cqAttributes = cqAttributes;
    this.cqProxy = serverProxy;
  }

  @Override
  public String getServerCqName() {
    return this.cqName;
  }

  ServerCQProxyImpl getCQProxy() {
    return this.cqProxy;
  }

  /**
   * Initializes the connection using the pool from the client region. Also sets the cqBaseRegion
   * value of this CQ.
   */
  private void initConnectionProxy() throws CqException, RegionNotFoundException {
    cqBaseRegion = (LocalRegion) cqService.getCache().getRegion(regionName);
    // Check if the region exists on the local cache.
    // In the current implementation of 5.1 the Server Connection is (ConnectionProxyImpl)
    // is obtained by the Bridge Client/writer/loader on the local region.
    if (cqBaseRegion == null) {
      throw new RegionNotFoundException(
          String.format("Region on which query is specified not found locally, regionName: %s",
              regionName));
    }

    ServerRegionProxy srp = cqBaseRegion.getServerProxy();
    if (srp != null) {
      if (logger.isTraceEnabled()) {
        logger.trace("Found server region proxy on region. RegionName: {}", regionName);
      }
      this.cqProxy = new ServerCQProxyImpl(srp);
      if (!srp.getPool().getSubscriptionEnabled()) {
        throw new CqException("The 'queueEnabled' flag on Pool installed on Region " + regionName
            + " is set to false.");
      }
    } else {
      throw new CqException(
          "Unable to get the connection pool. The Region does not have a pool configured.");
    }
  }

  @Override
  public void close() throws CqClosedException, CqException {
    this.close(true);
  }

  @Override
  public void close(boolean sendRequestToServer) throws CqClosedException, CqException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Started closing CQ CqName: {} SendRequestToServer: {}", cqName,
          sendRequestToServer);
    }
    // Synchronize with stop and execute CQ commands
    synchronized (this.cqState) {
      // Check if the cq is already closed.
      if (this.isClosed()) {
        // throw new CqClosedException("CQ is already closed, CqName : " + this.cqName);
        if (isDebugEnabled) {
          logger.debug("CQ is already closed, CqName: {}", this.cqName);
        }
        return;
      }

      int stateBeforeClosing = this.cqState.getState();
      this.cqState.setState(CqStateImpl.CLOSING);
      boolean isClosed = false;

      // Client Close. Proxy is null in case of server.
      // Check if this has been sent to server, if so send
      // Close request to server.
      Exception exception = null;
      if (this.cqProxy != null && sendRequestToServer) {
        try {
          if (this.proxyCache != null) {
            if (this.proxyCache.isClosed()) {
              throw proxyCache.getCacheClosedException("Cache is closed for this user.");
            }
            UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
          }
          cqProxy.close(this);
          isClosed = true;
        } catch (CancelException e) {
          throw e;
        } catch (Exception ex) {
          if (shutdownInProgress()) {
            return;
          }
          exception = ex;
        } finally {
          UserAttributes.userAttributes.set(null);
        }
      }

      // Cleanup the resource used by cq.
      this.removeFromCqMap();

      if (cqProxy == null || !sendRequestToServer || isClosed) {
        // Stat update.
        if (stateBeforeClosing == CqStateImpl.RUNNING) {
          cqService.stats().decCqsActive();
        } else if (stateBeforeClosing == CqStateImpl.STOPPED) {
          cqService.stats().decCqsStopped();
        }

        // Set the state to close, and update stats
        this.cqState.setState(CqStateImpl.CLOSED);
        cqService.stats().incCqsClosed();
        cqService.stats().decCqsOnClient();
        if (this.stats != null)
          this.stats.close();
      } else {
        if (shutdownInProgress()) {
          return;
        }
        // Hasn't able to send close request to any server.
        if (exception != null) {
          throw new CqException(
              String.format("Failed to close the cq. CqName: %s. Error from last endpoint: %s",
                  this.cqName, exception.getLocalizedMessage()),
              exception.getCause());
        } else {
          throw new CqException(
              String.format(
                  "Failed to close the cq. CqName: %s. The server endpoints on which this cq was registered were not found.",
                  this.cqName));
        }
      }
    }

    // Invoke close on Listeners if any.
    if (this.cqAttributes != null) {
      CqListener[] cqListeners = this.getCqAttributes().getCqListeners();

      if (cqListeners != null) {
        if (isDebugEnabled) {
          logger.debug(
              "Invoking CqListeners close() api for the CQ, CqName: {} Number of CqListeners: {}",
              cqName, cqListeners.length);
        }
        for (int lCnt = 0; lCnt < cqListeners.length; lCnt++) {
          try {
            cqListeners[lCnt].close();
            // Handle client side exceptions.
          } catch (Exception ex) {
            logger.warn("Exception occurred in the CqListener of the CQ, CqName : {} Error : {}",
                new Object[] {cqName, ex.getLocalizedMessage()});
            if (isDebugEnabled) {
              logger.debug(ex.getMessage(), ex);
            }
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
                "RuntimeException occurred in the CqListener of the CQ, CqName : {} Error : {}",
                new Object[] {cqName, t.getLocalizedMessage()});
            if (isDebugEnabled) {
              logger.debug(t.getMessage(), t);
            }
          }
        }
      }
    }
    if (isDebugEnabled) {
      logger.debug("Successfully closed the CQ. {}", cqName);
    }
  }

  /**
   * Clears the resource used by CQ.
   */
  @Override
  protected void cleanup() throws CqException {
    this.cqService.removeFromBaseRegionToCqNameMap(this.regionName, this.getServerCqName());
  }

  @Override
  public CqAttributes getCqAttributes() {
    return cqAttributes;
  }

  /**
   * @return Returns the cqListeners.
   */
  public CqListener[] getCqListeners() {
    return cqAttributes.getCqListeners();
  }

  /**
   * Start or resume executing the query.
   */
  @Override
  public void execute() throws CqClosedException, RegionNotFoundException, CqException {
    executeCqOnRedundantsAndPrimary(false);
  }

  /**
   * Start or resume executing the query. Gets or updates the CQ results and returns them.
   */
  @Override
  public <E> CqResults<E> executeWithInitialResults()
      throws CqClosedException, RegionNotFoundException, CqException {

    synchronized (queuedEventsSynchObject) {
      // Prevents multiple calls to executeWithInitialResults from squishing
      // each others queuedEvents. Forces the second call to wait
      // until first call is completed.
      while (queuedEvents != null) {
        try {
          queuedEventsSynchObject.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      // At this point we know queuedEvents is null and no one is adding to queuedEvents yet.
      this.queuedEvents = new ConcurrentLinkedQueue<CqEventImpl>();
    }

    if (CqQueryImpl.testHook != null) {
      testHook.pauseUntilReady();
    }
    // Send CQ request to servers.
    // If an exception is thrown, we need to clean up the queuedEvents
    // or else client will hang on next executeWithInitialResults
    CqResults initialResults;
    try {
      initialResults = (CqResults) executeCqOnRedundantsAndPrimary(true);
    } catch (RegionNotFoundException | CqException | RuntimeException e) {
      queuedEvents = null;
      throw e;
    }

    // lock was released earlier so that events that are received while executing
    // initial results can be added to the queue.
    synchronized (queuedEventsSynchObject) {
      // Check if any CQ Events are received during query execution.
      // Invoke the CQ Listeners with the received CQ Events.
      try {
        if (!this.queuedEvents.isEmpty()) {
          try {
            Thread thread = new LoggingThread("CQEventHandler For " + cqName, () -> {
              Object[] eventArray = null;
              if (CqQueryImpl.testHook != null) {
                testHook.setEventCount(queuedEvents.size());
              }
              // Synchronization for the executer thread.
              synchronized (queuedEventsSynchObject) {
                try {
                  eventArray = queuedEvents.toArray();

                  // Process through the events
                  for (Object cqEvent : eventArray) {
                    cqService.invokeListeners(cqName, ClientCQImpl.this, (CqEventImpl) cqEvent);
                    stats.decQueuedCqListenerEvents();
                  }
                } finally {
                  // Make sure that we notify waiting threads or else possible dead lock
                  queuedEvents.clear();
                  queuedEvents = null;
                  queuedEventsSynchObject.notify();
                }
              }
            });
            thread.start();
          } catch (Exception ex) {
            if (logger.isDebugEnabled()) {
              logger.debug("Exception while invoking the CQ Listener with queued events.", ex);
            }
          }
        } else {
          queuedEvents = null;
        }
      } finally {
        queuedEventsSynchObject.notify();
      }
      return initialResults;
    }
  }

  /**
   * This executes the CQ first on the redundant server and then on the primary server. This is
   * required to keep the redundancy behavior in accordance with the HAQueue expectation (wherein
   * the events are delivered only from the primary).
   *
   * @param executeWithInitialResults boolean
   * @return Object SelectResults in case of executeWithInitialResults
   */
  private Object executeCqOnRedundantsAndPrimary(boolean executeWithInitialResults)
      throws CqClosedException, RegionNotFoundException, CqException {

    Object initialResults = null;

    synchronized (this.cqState) {
      if (this.isClosed()) {
        throw new CqClosedException(
            String.format("CQ is closed, CqName : %s", this.cqName));
      }
      if (this.isRunning()) {
        throw new IllegalStateException(String.format("CQ is in running state, CqName : %s",
            this.cqName));
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Performing Execute {} request for CQ. CqName: {}",
            ((executeWithInitialResults) ? "WithInitialResult" : ""), this.cqName);
      }
      this.cqBaseRegion = (LocalRegion) cqService.getCache().getRegion(this.regionName);

      // If not server send the request to server.
      if (!cqService.isServer()) {
        // Send execute request to the server.

        // If CqService is initialized using the pool, the connection proxy is set using the
        // pool that initializes the CQ. Else its set using the Region proxy.
        if (this.cqProxy == null) {
          initConnectionProxy();
        }

        boolean success = false;
        try {
          if (this.proxyCache != null) {
            if (this.proxyCache.isClosed()) {
              throw proxyCache.getCacheClosedException("Cache is closed for this user.");
            }
            UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
          }
          if (executeWithInitialResults) {
            initialResults = cqProxy.createWithIR(this);
            if (initialResults == null) {
              String errMsg = "Failed to execute the CQ.  CqName: " + this.cqName
                  + ", Query String is: " + this.queryString;
              throw new CqException(errMsg);
            }
          } else {
            cqProxy.create(this);
          }
          success = true;
        } catch (Exception ex) {
          // Check for system shutdown.
          if (this.shutdownInProgress()) {
            throw new CqException("System shutdown in progress.");
          }
          if (ex.getCause() instanceof GemFireSecurityException) {
            if (securityLogWriter.warningEnabled()) {
              securityLogWriter.warning(
                  String.format("Exception while executing cq Exception: %s", ex));
            }
            throw new CqException(ex.getCause().getMessage(), ex.getCause());
          } else if (ex instanceof CqException) {
            throw (CqException) ex;
          } else {
            String errMsg =
                String.format(
                    "Failed to execute the CQ. CqName: %s, Query String is: %s, Error from last server: %s",
                    this.cqName, this.queryString, ex.getLocalizedMessage());
            if (logger.isDebugEnabled()) {
              logger.debug(errMsg, ex);
            }
            throw new CqException(errMsg, ex);
          }
        } finally {
          if (!success && !this.shutdownInProgress()) {
            try {
              cqProxy.close(this);
            } catch (Exception e) {
              if (logger.isDebugEnabled()) {
                logger.debug("Exception cleaning up failed cq", e);
              }
              UserAttributes.userAttributes.set(null);
            }
          }
          UserAttributes.userAttributes.set(null);
        }
      }
      this.cqState.setState(CqStateImpl.RUNNING);
    }
    // If client side, alert listeners that a cqs have been connected
    if (!cqService.isServer()) {
      connected = true;
      CqListener[] cqListeners = getCqAttributes().getCqListeners();
      for (int lCnt = 0; lCnt < cqListeners.length; lCnt++) {
        if (cqListeners[lCnt] != null) {
          if (cqListeners[lCnt] instanceof CqStatusListener) {
            CqStatusListener listener = (CqStatusListener) cqListeners[lCnt];
            listener.onCqConnected();
          }
        }
      }
    }
    // Update CQ-base region for book keeping.
    this.cqService.stats().incCqsActive();
    this.cqService.stats().decCqsStopped();
    return initialResults;
  }

  /**
   * Check to see if shutdown in progress.
   *
   * @return true if shutdown in progress else false.
   */
  private boolean shutdownInProgress() {
    InternalCache cache = cqService.getInternalCache();
    if (cache == null || cache.isClosed()) {
      return true; // bail, things are shutting down
    }

    String reason = cqProxy.getPool().getCancelCriterion().cancelInProgress();
    if (reason != null) {
      return true;
    }
    return false;
  }

  /**
   * Stop or pause executing the query.
   */
  @Override
  public void stop() throws CqClosedException, CqException {
    boolean isStopped = false;
    synchronized (this.cqState) {
      if (this.isClosed()) {
        throw new CqClosedException(
            String.format("CQ is closed, CqName : %s", this.cqName));
      }

      if (!(this.isRunning())) {
        throw new IllegalStateException(
            String.format("CQ is not in running state, stop CQ does not apply, CqName : %s",
                this.cqName));
      }

      Exception exception = null;
      try {
        if (this.proxyCache != null) {
          if (this.proxyCache.isClosed()) {
            throw proxyCache.getCacheClosedException("Cache is closed for this user.");
          }
          UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
        }
        cqProxy.stop(this);
        isStopped = true;
      } catch (Exception e) {
        exception = e;
      } finally {
        UserAttributes.userAttributes.set(null);
      }
      if (cqProxy == null || isStopped) {
        // Change state and stats on the client side
        this.cqState.setState(CqStateImpl.STOPPED);
        this.cqService.stats().incCqsStopped();
        this.cqService.stats().decCqsActive();
        if (logger.isDebugEnabled()) {
          logger.debug("Successfully stopped the CQ. {}", cqName);
        }
      } else {
        // Hasn't able to send stop request to any server.
        if (exception != null) {
          throw new CqException(
              String.format("Failed to stop the cq. CqName :%s Error from last server: %s",
                  this.cqName, exception.getLocalizedMessage()),
              exception.getCause());
        } else {
          throw new CqException(
              String.format(
                  "Failed to stop the cq. CqName: %s. The server endpoints on which this cq was registered were not found.",
                  this.cqName));
        }
      }
    }
  }

  @Override
  public CqAttributesMutator getCqAttributesMutator() {
    return (CqAttributesMutator) this.cqAttributes;
  }

  ConcurrentLinkedQueue<CqEventImpl> getQueuedEvents() {
    return this.queuedEvents;
  }

  @Override
  public void setProxyCache(ProxyCache proxyCache) {
    this.proxyCache = proxyCache;
  }

  boolean isConnected() {
    return connected;
  }

  void setConnected(boolean connected) {
    this.connected = connected;
  }

  @Override
  public void createOn(Connection conn, boolean isDurable) {
    byte regionDataPolicyOrdinal = getCqBaseRegion() == null ? (byte) 0
        : getCqBaseRegion().getAttributes().getDataPolicy().ordinal;

    int state = this.cqState.getState();
    this.cqProxy.createOn(getName(), conn, getQueryString(), state, isDurable,
        regionDataPolicyOrdinal);
  }
}
