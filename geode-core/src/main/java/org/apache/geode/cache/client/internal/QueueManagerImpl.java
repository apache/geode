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
package org.apache.geode.cache.client.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.GemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.PoolImpl.PoolTask;
import org.apache.geode.cache.client.internal.RegisterInterestTracker.RegionInterestEntry;
import org.apache.geode.cache.client.internal.ServerDenyList.DenyListListener;
import org.apache.geode.cache.client.internal.ServerDenyList.DenyListListenerAdapter;
import org.apache.geode.cache.client.internal.ServerDenyList.FailureTracker;
import org.apache.geode.cache.query.internal.CqStateImpl;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.cq.ClientCQ;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.security.GemFireSecurityException;

/**
 * Manages Client Queues. Responsible for creating callback connections and satisfying redundancy
 * requirements.
 *
 * @since GemFire 5.7
 */
public class QueueManagerImpl implements QueueManager {
  private static final Logger logger = LogService.getLogger();

  private static final Comparator QSIZE_COMPARATOR = new QSizeComparator();

  protected final long redundancyRetryInterval;
  private final EndpointManager endpointManager;
  private final EndpointManager.EndpointListenerAdapter endpointListener;
  private final ConnectionSource source;
  private final int redundancyLevel;
  protected final ConnectionFactory factory;
  private final InternalLogWriter securityLogger;
  private final ClientProxyMembershipID proxyId;
  protected final InternalPool pool;
  private final QueueStateImpl state;
  private boolean printPrimaryNotFoundError;
  private boolean printRedundancyNotSatisfiedError;
  private boolean printRecoveringPrimary;
  private boolean printRecoveringRedundant;
  protected final ServerDenyList denyList;
  // Lock which guards updates to queueConnections.
  // Also threads calling getAllConnections will wait on this
  // lock until there is a primary.
  protected final Object lock = new Object();

  protected final CountDownLatch initializedLatch = new CountDownLatch(1);

  private ScheduledExecutorService recoveryThread;
  private volatile boolean sentClientReady;

  // queueConnections in maintained by using copy-on-write
  protected volatile ConnectionList queueConnections = new ConnectionList();
  protected volatile RedundancySatisfierTask redundancySatisfierTask = null;
  private volatile boolean shuttingDown;

  public QueueManagerImpl(InternalPool pool, EndpointManager endpointManager,
      ConnectionSource source, ConnectionFactory factory, int queueRedundancyLevel,
      long redundancyRetryInterval, InternalLogWriter securityLogger,
      ClientProxyMembershipID proxyId) {
    this.printPrimaryNotFoundError = true;
    this.printRedundancyNotSatisfiedError = true;
    this.printRecoveringRedundant = true;
    this.printRecoveringPrimary = true;
    this.pool = pool;
    this.endpointManager = endpointManager;
    this.source = source;
    this.factory = factory;
    this.redundancyLevel = queueRedundancyLevel;
    this.securityLogger = securityLogger;
    this.proxyId = proxyId;
    this.redundancyRetryInterval = redundancyRetryInterval;
    denyList = new ServerDenyList(redundancyRetryInterval);


    this.endpointListener = new EndpointManager.EndpointListenerAdapter() {
      @Override
      public void endpointCrashed(Endpoint endpoint) {
        QueueManagerImpl.this.endpointCrashed(endpoint);
      }
    };

    this.state = new QueueStateImpl(this);
  }

  public InternalPool getPool() {
    return pool;
  }

  public boolean isPrimaryUpdaterAlive() {
    boolean result = false;
    QueueConnectionImpl primary = (QueueConnectionImpl) queueConnections.getPrimary();
    if (primary != null) {
      ClientUpdater cu = primary.getUpdater();
      if (cu != null) {
        result = cu.isAlive();
      }
    }
    return result;
  }

  public QueueConnections getAllConnectionsNoWait() {
    return queueConnections;
  }

  public QueueConnections getAllConnections() {

    ConnectionList snapshot = queueConnections;
    if (snapshot.getPrimary() == null) {
      // wait for a new primary to become available.
      synchronized (lock) {
        snapshot = queueConnections;
        while (snapshot.getPrimary() == null && !snapshot.primaryDiscoveryFailed() && !shuttingDown
            && pool.getPoolOrCacheCancelInProgress() == null) {
          try {
            lock.wait();
          } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
            break;
          }
          snapshot = queueConnections;
        }
      }
    }

    if (snapshot.getPrimary() == null) {
      pool.getCancelCriterion().checkCancelInProgress(null);
      GemFireException exception = snapshot.getPrimaryDiscoveryException();
      if (exception == null || exception instanceof NoSubscriptionServersAvailableException) {
        exception = new NoSubscriptionServersAvailableException(exception);
      } else {
        exception = new ServerConnectivityException(exception.getMessage(), exception);
      }
      throw exception;
    }

    return snapshot;
  }

  public InternalLogWriter getSecurityLogger() {
    return securityLogger;
  }

  public void close(boolean keepAlive) {
    endpointManager.removeListener(endpointListener);
    synchronized (lock) {
      shuttingDown = true;
      if (redundancySatisfierTask != null) {
        redundancySatisfierTask.cancel();
      }
      lock.notifyAll();
    }
    if (recoveryThread != null) {
      // it will be null if we never called start
      recoveryThread.shutdown();
    }
    if (recoveryThread != null) {
      try {
        if (!recoveryThread.awaitTermination(PoolImpl.SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
          logger.warn("Timeout waiting for recovery thread to complete");
        }
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        logger.debug("Interrupted waiting for recovery thread termination");
      }
    }

    QueueConnectionImpl primary = (QueueConnectionImpl) queueConnections.getPrimary();
    if (logger.isDebugEnabled()) {
      logger.debug("QueueManagerImpl - closing connections with keepAlive={}", keepAlive);
    }
    if (primary != null) {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("QueueManagerImpl - closing primary {}", primary);
        }
        primary.internalClose(keepAlive);
      } catch (Exception e) {
        logger.warn("Error closing primary connection to " +
            primary.getEndpoint(),
            e);
      }
    }

    List backups = queueConnections.getBackups();
    for (Iterator itr = backups.iterator(); itr.hasNext();) {
      QueueConnectionImpl backup = (QueueConnectionImpl) itr.next();
      if (backup != null) {
        try {
          if (logger.isDebugEnabled()) {
            logger.debug("QueueManagerImpl - closing backup {}", backup);
          }
          backup.internalClose(keepAlive);
        } catch (Exception e) {
          logger.warn("Error closing backup connection to " +
              backup.getEndpoint(),
              e);
        }
      }
    }
  }


  public void emergencyClose() {
    shuttingDown = true;
    queueConnections.getPrimary().emergencyClose();
    List backups = queueConnections.getBackups();
    for (int i = 0; i < backups.size(); i++) {
      Connection backup = (Connection) backups.get(i);
      backup.emergencyClose();
    }
  }

  public void start(ScheduledExecutorService background) {
    try {
      denyList.start(background);
      endpointManager.addListener(endpointListener);

      // Use a separate timer for queue management tasks
      // We don't want primary recovery (and therefore user threads) to wait for
      // things like pinging connections for health checks.
      final String name = "queueTimer-" + this.pool.getName();
      this.recoveryThread = LoggingExecutors.newScheduledThreadPool(name, 1, false);

      getState().start(background, getPool().getSubscriptionAckInterval());

      // initialize connections
      initializeConnections();

      scheduleRedundancySatisfierIfNeeded(redundancyRetryInterval);

      // When a server is removed from the denylist, try again
      // to establish redundancy (if we need to)
      DenyListListener denyListListener = new DenyListListenerAdapter() {
        @Override
        public void serverRemoved(ServerLocation location) {
          QueueManagerImpl.this.scheduleRedundancySatisfierIfNeeded(0);
        }
      };

      denyList.addListener(denyListListener);
      factory.getDenyList().addListener(denyListListener);
    } finally {
      initializedLatch.countDown();
    }
  }



  public void readyForEvents(InternalDistributedSystem system) {
    synchronized (lock) {
      this.sentClientReady = true;
    }

    QueueConnectionImpl primary = null;
    while (primary == null) {
      try {
        primary = (QueueConnectionImpl) getAllConnections().getPrimary();
      } catch (NoSubscriptionServersAvailableException ignore) {
        primary = null;
        break;
      }
      if (primary.sendClientReady()) {
        try {
          logger.info("Sending ready for events to primary: {}", primary);
          ReadyForEventsOp.execute(pool, primary);
        } catch (Exception e) {
          if (logger.isDebugEnabled()) {
            logger.debug("Error sending ready for events to {}", primary, e);
          }
          primary.destroy();
          primary = null;
        }
      }
    }
  }

  public void readyForEventsAfterFailover(QueueConnectionImpl primary) {
    try {
      logger.info("Sending ready for events to primary: {}", primary);
      ReadyForEventsOp.execute(pool, primary);
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Error sending ready for events to {}", primary, e);
      }
      primary.destroy();
    }
  }

  void connectionCrashed(Connection con) {
    // the endpoint has not crashed but this method does all the work
    // we need to do
    endpointCrashed(con.getEndpoint());
  }

  void endpointCrashed(Endpoint endpoint) {
    QueueConnectionImpl deadConnection = null;
    // We must be synchronized while checking to see if we have a queue connection for the endpoint,
    // because when we need to prevent a race between adding a queue connection to the map
    // and the endpoint for that connection crashing.
    synchronized (lock) {
      deadConnection = queueConnections.getConnection(endpoint);
      if (deadConnection != null) {
        queueConnections = queueConnections.removeConnection(deadConnection);
      }
    }
    if (deadConnection != null) {
      logger
          .info("{} subscription endpoint {} crashed. Scheduling recovery.",
              new Object[] {deadConnection.getUpdater() != null
                  ? (deadConnection.getUpdater().isPrimary() ? "Primary" : "Redundant")
                  : "Queue",
                  endpoint});
      scheduleRedundancySatisfierIfNeeded(0);
      deadConnection.internalDestroy();
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Ignoring crashed endpoint {} it does not have a queue.", endpoint);
      }
    }
  }

  /**
   * This method checks whether queue connection exist on this endpoint or not. if its there then it
   * just destroys connection as clientUpdate thread is not there to read that connection.
   */
  public void checkEndpoint(ClientUpdater ccu, Endpoint endpoint) {
    QueueConnectionImpl deadConnection = null;

    synchronized (lock) {
      if (shuttingDown)
        return;
      // if same client updater then only remove as we don't know whether it has created new
      // updater/connection on same endpoint or not..
      deadConnection = queueConnections.getConnection(endpoint);
      if (deadConnection != null && ccu.equals(deadConnection.getUpdater())) {
        queueConnections = queueConnections.removeConnection(deadConnection);
        try {
          deadConnection.internalClose(pool.getKeepAlive());
        } catch (Exception e) {
          logger.warn("Error destroying client to server connection to {}",
              deadConnection.getEndpoint(), e);
        }
      }
    }

    logger
        .info("Cache client updater for {} on endpoint {} exiting. Scheduling recovery.",
            (deadConnection != null && deadConnection.getUpdater() != null)
                ? (deadConnection.getUpdater().isPrimary() ? "Primary" : "Redundant")
                : "Queue",
            endpoint);
    scheduleRedundancySatisfierIfNeeded(0);// one more chance
  }

  private void initializeConnections() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("SubscriptionManager - intitializing connections");
    }

    int queuesNeeded = redundancyLevel == -1 ? -1 : redundancyLevel + 1;
    Set excludedServers = new HashSet(denyList.getBadServers());
    List servers = findQueueServers(excludedServers, queuesNeeded, true, false, null);

    if (servers == null || servers.isEmpty()) {
      logger.warn(
          "Could not create a queue. No queue servers available.");
      scheduleRedundancySatisfierIfNeeded(redundancyRetryInterval);
      synchronized (lock) {
        queueConnections = queueConnections.setPrimaryDiscoveryFailed(null);
        lock.notifyAll();
      }
      return;
    }

    if (isDebugEnabled) {
      logger.debug("SubscriptionManager - discovered subscription servers {}", servers);
    }

    SortedMap/* <ServerQueueStatus,Connection> */ oldQueueServers = new TreeMap(QSIZE_COMPARATOR);
    List nonRedundantServers = new ArrayList();

    for (Iterator itr = servers.iterator(); itr.hasNext();) {
      ServerLocation server = (ServerLocation) itr.next();
      Connection connection = null;
      try {
        connection = factory.createClientToServerConnection(server, true);
      } catch (GemFireSecurityException e) {
        throw e;
      } catch (GemFireConfigException e) {
        throw e;
      } catch (Exception e) {
        if (isDebugEnabled) {
          logger.debug("SubscriptionManager - Error connected to server: {}", server, e);
        }
      }
      if (connection != null) {
        ServerQueueStatus status = connection.getQueueStatus();
        if (status.isRedundant() || status.isPrimary()) {
          oldQueueServers.put(status, connection);
        } else {
          nonRedundantServers.add(connection);
        }
      }
    }

    // This ordering was determined from the old ConnectionProxyImpl code
    //
    // initialization order of the new redundant and primary server is
    // old redundant w/ second largest queue
    // old redundant w/ third largest queue
    // ...
    // old primary
    // non redundants in no particular order
    //
    // The primary is then chosen as
    // redundant with the largest queue
    // primary if there are no redundants
    // a non redundant

    // if the redundant with the largest queue fails, then we go and
    // make a new server a primary.

    Connection newPrimary = null;
    if (!oldQueueServers.isEmpty()) {
      newPrimary = (Connection) oldQueueServers.remove(oldQueueServers.lastKey());
    } else if (!nonRedundantServers.isEmpty()) {
      newPrimary = (Connection) nonRedundantServers.remove(0);
    }

    nonRedundantServers.addAll(0, oldQueueServers.values());

    for (Iterator itr = nonRedundantServers.iterator(); itr.hasNext();) {
      Connection connection = (Connection) itr.next();
      QueueConnectionImpl queueConnection = initializeQueueConnection(connection, false, null);
      if (queueConnection != null) {
        addToConnectionList(queueConnection, false);
      }
    }

    QueueConnectionImpl primaryQueue = null;
    if (newPrimary != null) {
      primaryQueue = initializeQueueConnection(newPrimary, true, null);
      if (primaryQueue == null) {
        newPrimary.destroy();
      } else {
        if (!addToConnectionList(primaryQueue, true)) {
          primaryQueue = null;
        }
      }
    }


    excludedServers.addAll(servers);

    // Make sure we have enough redundant copies. Some of the connections may
    // have failed
    // above.
    if (redundancyLevel != -1 && getCurrentRedundancy() < redundancyLevel) {
      if (isDebugEnabled) {
        logger.debug(
            "SubscriptionManager - Some initial connections failed. Trying to create redundant queues");
      }
      recoverRedundancy(excludedServers, false);
    }

    if (redundancyLevel != -1 && primaryQueue == null) {
      if (isDebugEnabled) {
        logger.debug(
            "SubscriptionManager - Intial primary creation failed. Trying to create a new primary");
      }
      while (primaryQueue == null) {
        primaryQueue = createNewPrimary(excludedServers);
        if (primaryQueue == null) {
          // couldn't find a server to make primary
          break;
        }
        if (!addToConnectionList(primaryQueue, true)) {
          excludedServers.add(primaryQueue.getServer());
          primaryQueue = null;
        }
      }
    }

    if (primaryQueue == null) {
      if (isDebugEnabled) {
        logger.debug(
            "SubscriptionManager - Unable to create a new primary queue, using one of the redundant queues");
      }
      while (primaryQueue == null) {
        primaryQueue = promoteBackupToPrimary(queueConnections.getBackups());
        if (primaryQueue == null) {
          // no backup servers available
          break;
        }
        if (!addToConnectionList(primaryQueue, true)) {
          synchronized (lock) {
            // make sure we don't retry this same connection.
            queueConnections = queueConnections.removeConnection(primaryQueue);
          }
          primaryQueue = null;
        }
      }
    }

    if (primaryQueue == null) {
      logger.error("Could not initialize a primary queue on startup. No queue servers available.");
      synchronized (lock) {
        queueConnections =
            queueConnections.setPrimaryDiscoveryFailed(new NoSubscriptionServersAvailableException(
                "Could not initialize a primary queue on startup. No queue servers available."));
        lock.notifyAll();
      }
      cqsDisconnected();
    } else {
      cqsConnected();
    }

    if (getCurrentRedundancy() < redundancyLevel) {
      logger.warn(
          "Unable to initialize enough redundant queues on startup. The redundancy count is currently {}.",
          getCurrentRedundancy());
    }
  }

  private void cqsConnected() {
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      CqService cqService = cache.getCqService();
      // Primary queue was found, alert the affected cqs if necessary
      cqService.cqsConnected(pool);
    }
  }

  private void cqsDisconnected() {
    // No primary queue was found, alert the affected cqs if necessary
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      CqService cqService = cache.getCqService();
      cqService.cqsDisconnected(pool);
    }
  }

  private int getCurrentRedundancy() {
    return queueConnections.getBackups().size();
  }

  /**
   * Make sure that we have enough backup servers.
   *
   * Add any servers we fail to connect to to the excluded servers list.
   */
  protected boolean recoverRedundancy(Set excludedServers, boolean recoverInterest) {
    if (pool.getPoolOrCacheCancelInProgress() != null) {
      return true;
    }
    int additionalBackups;
    while (pool.getPoolOrCacheCancelInProgress() == null
        && ((additionalBackups = redundancyLevel - getCurrentRedundancy()) > 0
            || redundancyLevel == -1)) {


      if (redundancyLevel != -1 && printRecoveringRedundant) {
        logger.info(
            "SubscriptionManager redundancy satisfier - redundant endpoint has been lost. Attempting to recover.");
        printRecoveringRedundant = false;
      }

      List servers = findQueueServers(excludedServers,
          redundancyLevel == -1 ? -1 : additionalBackups, false,
          (redundancyLevel == -1 ? false : printRedundancyNotSatisfiedError),
          "Could not find any server to host redundant client queue. Number of excluded servers is %s and exception is %s");

      if (servers == null || servers.isEmpty()) {
        if (redundancyLevel != -1) {

          if (printRedundancyNotSatisfiedError) {
            logger.info(
                "Redundancy level {} is not satisfied, but there are no more servers available. Redundancy is currently {}.",
                new Object[] {redundancyLevel, getCurrentRedundancy()});
          }
        }
        printRedundancyNotSatisfiedError = false;// printed above
        return false;
      }
      excludedServers.addAll(servers);

      final boolean isDebugEnabled = logger.isDebugEnabled();
      for (Iterator itr = servers.iterator(); itr.hasNext();) {
        ServerLocation server = (ServerLocation) itr.next();
        Connection connection = null;
        try {
          connection = factory.createClientToServerConnection(server, true);
        } catch (GemFireSecurityException e) {
          throw e;
        } catch (Exception e) {
          if (isDebugEnabled) {
            logger.debug("SubscriptionManager - Error connecting to server: ()", server, e);
          }
        }
        if (connection == null) {
          continue;
        }

        QueueConnectionImpl queueConnection = initializeQueueConnection(connection, false, null);
        if (queueConnection != null) {
          boolean isFirstNewConnection = false;
          synchronized (lock) {
            if (recoverInterest && queueConnections.getPrimary() == null
                && queueConnections.getBackups().isEmpty()) {
              // we lost our queue at some point. We Need to recover
              // interest. This server will be made primary after this method
              // finishes
              // because whoever killed the primary when this method started
              // should
              // have scheduled a task to recover the primary.
              isFirstNewConnection = true;
              // TODO - Actually, we need a better check than the above. There's
              // still a chance
              // that we haven't realized that the primary has died but it is
              // already gone. We should
              // get some information from the queue server about whether it was
              // able to copy the
              // queue from another server and decide if we need to recover our
              // interest based on
              // that information.
            }
          }
          boolean promotionFailed = false;
          if (isFirstNewConnection) {
            if (!promoteBackupCnxToPrimary(queueConnection)) {
              promotionFailed = true;
            }
          }
          if (!promotionFailed) {
            if (addToConnectionList(queueConnection, isFirstNewConnection)) {
              // redundancy satisfied
              printRedundancyNotSatisfiedError = true;
              printRecoveringRedundant = true;
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "SubscriptionManager redundancy satisfier - created a queue on server {}",
                    queueConnection.getEndpoint());
              }
              // Even though the new redundant queue will usually recover
              // subscription information (see bug #39014) from its initial
              // image provider, in bug #42280 we found that this is not always
              // the case, so clients must always register interest with the new
              // redundant server.
              if (recoverInterest) {
                recoverInterest(queueConnection, isFirstNewConnection);
              }
            }
          }
        }
      }
    }
    return true;
  }

  private QueueConnectionImpl promoteBackupToPrimary(List backups) {
    QueueConnectionImpl primary = null;
    for (int i = 0; primary == null && i < backups.size(); i++) {
      QueueConnectionImpl lastConnection = (QueueConnectionImpl) backups.get(i);
      if (promoteBackupCnxToPrimary(lastConnection)) {
        primary = lastConnection;
      }
    }
    return primary;
  }

  private boolean promoteBackupCnxToPrimary(QueueConnectionImpl cnx) {
    boolean result = false;
    if (PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG) {
      ClientServerObserver bo = ClientServerObserverHolder.getInstance();
      bo.beforePrimaryIdentificationFromBackup();
    }
    try {
      boolean haveSentClientReady = this.sentClientReady;
      if (haveSentClientReady) {
        cnx.sendClientReady();
      }
      ClientUpdater updater = cnx.getUpdater();
      if (updater == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("backup connection was destroyed before it could become the primary.");
        }
        Assert.assertTrue(cnx.isDestroyed());
      } else {
        updater.setFailedUpdater(queueConnections.getFailedUpdater());
        MakePrimaryOp.execute(pool, cnx, haveSentClientReady);
        result = true;
        if (PoolImpl.AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG) {
          ClientServerObserver bo = ClientServerObserverHolder.getInstance();
          bo.afterPrimaryIdentificationFromBackup(cnx.getServer());
        }
      }
    } catch (Exception e) {
      if (pool.getPoolOrCacheCancelInProgress() == null && logger.isDebugEnabled()) {
        logger.debug("Error making a backup server the primary server for client subscriptions", e);
      }
    }
    return result;
  }

  /**
   * Create a new primary server from a non-redundant server.
   *
   * Add any failed servers to the excludedServers set.
   */
  private QueueConnectionImpl createNewPrimary(Set excludedServers) {
    QueueConnectionImpl primary = null;
    while (primary == null && pool.getPoolOrCacheCancelInProgress() == null) {
      List servers = findQueueServers(excludedServers, 1, false, printPrimaryNotFoundError,
          "Could not find any server to host primary client queue. Number of excluded servers is %s and exception is %s");
      printPrimaryNotFoundError = false; // printed above
      if (servers == null || servers.isEmpty()) {
        break;
      }

      Connection connection = null;
      try {
        connection = factory.createClientToServerConnection((ServerLocation) servers.get(0), true);
      } catch (GemFireSecurityException e) {
        throw e;
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("SubscriptionManagerImpl - error creating a connection to server {}",
              servers.get(0));
        }
      }
      if (connection != null) {
        primary = initializeQueueConnection(connection, true, queueConnections.getFailedUpdater());
      }
      excludedServers.addAll(servers);
    }

    if (primary != null && sentClientReady && primary.sendClientReady()) {
      readyForEventsAfterFailover(primary);
    }
    return primary;
  }

  private List findQueueServers(Set excludedServers, int count, boolean findDurable,
      boolean printErrorMessage, String msg) {
    List servers = null;
    Exception ex = null;
    try {
      if (pool.getPoolOrCacheCancelInProgress() != null) {
        return null;
      }
      servers = source.findServersForQueue(excludedServers, count, proxyId, findDurable);
    } catch (GemFireSecurityException e) {
      // propagate the security exception immediately.
      throw e;
    } catch (Exception e) {
      /*
       * logger .warning(
       * LocalizedStrings.QueueManagerImpl_COULD_NOT_RETRIEVE_LIST_OF_SERVERS_FOR_SUBSCRIPTION_0,
       * new Object[] { e.getMessage() });
       */
      ex = e;
      if (logger.isDebugEnabled()) {
        logger.debug("SubscriptionManager - Error getting the list of servers: {}", e);
      }
    }

    if (printErrorMessage) {
      if (servers == null || servers.isEmpty()) {
        logger.error(String.format(msg,
            new Object[] {(excludedServers != null ? excludedServers.size() : 0),
                (ex != null ? ex.getMessage() : "no exception")}));
      }
    }
    return servers;
  }

  /**
   * Find a new primary, adding any failed servers we encounter to the excluded servers list
   *
   * First we try to make a backup server the primary, but if run out of backup servers we will try
   * to find a new server.
   */
  protected void recoverPrimary(Set excludedServers) {
    if (pool.getPoolOrCacheCancelInProgress() != null) {
      return;
    }
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (queueConnections.getPrimary() != null) {
      if (isDebugEnabled) {
        logger.debug("Primary recovery not needed");
      }
      return;
    }

    if (isDebugEnabled) {
      logger.debug(
          "SubscriptionManager redundancy satisfier - primary endpoint has been lost. Attempting to recover");
    }

    if (printRecoveringPrimary) {
      logger.info(
          "SubscriptionManager redundancy satisfier - primary endpoint has been lost. Attempting to recover.");
      printRecoveringPrimary = false;
    }

    QueueConnectionImpl newPrimary = null;
    while (newPrimary == null && pool.getPoolOrCacheCancelInProgress() == null) {
      List backups = queueConnections.getBackups();
      newPrimary = promoteBackupToPrimary(backups);
      // Hitesh now lets say that server crashed
      if (newPrimary == null) {
        // could not find a backup to promote
        break;
      }
      if (!addToConnectionList(newPrimary, true)) {
        synchronized (lock) {
          // make sure we don't retry the same backup server
          queueConnections = queueConnections.removeConnection(newPrimary);
        }
        newPrimary = null;
      }

    }

    if (newPrimary != null) {
      if (isDebugEnabled) {
        logger.debug(
            "SubscriptionManager redundancy satisfier - Switched backup server to primary: {}",
            newPrimary.getEndpoint());
      }
      if (PoolImpl.AFTER_PRIMARY_RECOVERED_CALLBACK_FLAG) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.afterPrimaryRecovered(newPrimary.getServer());
      }

      // new primary from back up server was found, alert affected cqs if necessary
      cqsConnected();
      printPrimaryNotFoundError = true;
      printRecoveringPrimary = true;
      return;
    }

    while (newPrimary == null) {
      newPrimary = createNewPrimary(excludedServers);
      if (newPrimary == null) {
        // could not find a new primary to create
        break;
      }
      if (!addToConnectionList(newPrimary, true)) {
        excludedServers.add(newPrimary.getServer());
        newPrimary = null;
      }

      if (newPrimary != null) {
        if (isDebugEnabled) {
          logger.debug(
              "SubscriptionManager redundancy satisfier - Non backup server was made primary. Recovering interest {}",
              newPrimary.getEndpoint());
        }

        if (!recoverInterest(newPrimary, true)) {
          excludedServers.add(newPrimary.getServer());
          newPrimary = null;
        }
        // New primary queue was found from a non backup, alert the affected cqs
        cqsConnected();
      }

      if (newPrimary != null && PoolImpl.AFTER_PRIMARY_RECOVERED_CALLBACK_FLAG) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.afterPrimaryRecovered(newPrimary.getServer());
      }
      printPrimaryNotFoundError = true;
      printRecoveringPrimary = true;
      return;
    }
    // No primary queue was found, alert the affected cqs
    cqsDisconnected();
    if (isDebugEnabled) {
      logger.debug("SubscriptionManager redundancy satisfier - Could not recover a new primary");
    }
    synchronized (lock) {
      queueConnections = queueConnections.setPrimaryDiscoveryFailed(null);
      lock.notifyAll();
    }
  }

  private QueueConnectionImpl initializeQueueConnection(Connection connection, boolean isPrimary,
      ClientUpdater failedUpdater) {
    QueueConnectionImpl queueConnection = null;
    FailureTracker failureTracker = denyList.getFailureTracker(connection.getServer());
    try {
      ClientUpdater updater = factory.createServerToClientConnection(connection.getEndpoint(), this,
          isPrimary, failedUpdater);
      if (updater != null) {
        queueConnection = new QueueConnectionImpl(this, connection, updater, failureTracker);
      } else {
        logger.warn("unable to create a subscription connection to server {}",
            connection.getEndpoint());
      }
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("error creating subscription connection to server {}",
            connection.getEndpoint(), e);
      }
    }
    if (queueConnection == null) {
      failureTracker.addFailure();
      connection.destroy();
    }
    return queueConnection;
  }

  // need flag whether primary is created from backup
  // for backuup queue lets assume before we add connection, endpoint crashed, now we put in
  // connection but CCU may died as endpoint closed....
  // so before putting connection need to see if something(crash) happen we should be able to
  // recover from it
  private boolean addToConnectionList(QueueConnectionImpl connection, boolean isPrimary) {
    boolean isBadConnection;
    synchronized (lock) {
      ClientUpdater cu = connection.getUpdater();
      if (cu == null || (!cu.isAlive()) || (!cu.isProcessing()))
        return false;// don't add
      // now still CCU can died but then it will execute Checkendpoint with lock it will remove
      // connection connection and it will reschedule it.
      if (connection.getEndpoint().isClosed() || shuttingDown
          || pool.getPoolOrCacheCancelInProgress() != null) {
        isBadConnection = true;
      } else {
        isBadConnection = false;
        if (isPrimary) {
          queueConnections = queueConnections.setPrimary(connection);
          lock.notifyAll();
        } else {
          queueConnections = queueConnections.addBackup(connection);
        }
      }
    }

    if (isBadConnection) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Endpoint {} crashed while creating a connection. The connection will be destroyed",
            connection.getEndpoint());
      }
      try {
        connection.internalClose(pool.getKeepAlive());
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Error destroying client to server connection to {}",
              connection.getEndpoint(), e);
        }
      }
    }

    return !isBadConnection;
  }

  protected void scheduleRedundancySatisfierIfNeeded(long delay) {
    if (shuttingDown) {
      return;
    }

    synchronized (lock) {
      if (shuttingDown) {
        return;
      }
      if (queueConnections.getPrimary() == null || getCurrentRedundancy() < redundancyLevel
          || redundancyLevel == -1 || queueConnections.primaryDiscoveryFailed()) {
        if (redundancySatisfierTask != null) {
          if (redundancySatisfierTask.getRemainingDelay() > delay) {
            redundancySatisfierTask.cancel();
          } else {
            return;
          }
        }

        redundancySatisfierTask = new RedundancySatisfierTask();
        try {
          ScheduledFuture future =
              recoveryThread.schedule(redundancySatisfierTask, delay, TimeUnit.MILLISECONDS);
          redundancySatisfierTask.setFuture(future);
        } catch (RejectedExecutionException e) {
          // ignore, the timer has been cancelled, which means we're shutting down.
        }
      }
    }
  }


  private boolean recoverInterest(final QueueConnectionImpl newConnection,
      final boolean isFirstNewConnection) {

    if (pool.getPoolOrCacheCancelInProgress() != null) {
      return true;
    }

    // recover interest
    try {
      recoverAllInterestTypes(newConnection, isFirstNewConnection);
      newConnection.getFailureTracker().reset();
      return true;
    } catch (CancelException ignore) {
      return true;
      // ok to ignore we are being shutdown
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      pool.getCancelCriterion().checkCancelInProgress(t);
      logger.warn("QueueManagerImpl failed to recover interest to server " +
          newConnection.getServer(),
          t);
      newConnection.getFailureTracker().addFailure();
      newConnection.destroy();
      return false;
    }
  }

  public QueueState getState() {
    return this.state;
  }

  private void recoverSingleList(int interestType, Connection recoveredConnection,
      boolean isDurable, boolean receiveValues, boolean isFirstNewConnection) {
    Iterator i = this.getPool().getRITracker()
        .getRegionToInterestsMap(interestType, isDurable, !receiveValues).values().iterator();
    while (i.hasNext()) { // restore a region
      RegionInterestEntry e = (RegionInterestEntry) i.next();
      recoverSingleRegion(e.getRegion(), e.getInterests(), interestType, recoveredConnection,
          isDurable, receiveValues, isFirstNewConnection);
    } // restore a region
  }

  private void recoverCqs(Connection recoveredConnection, boolean isDurable) {
    Map cqs = this.getPool().getRITracker().getCqsMap();
    Iterator i = cqs.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry e = (Map.Entry) i.next();
      ClientCQ cqi = (ClientCQ) e.getKey();
      String name = cqi.getName();
      if (this.pool.getMultiuserAuthentication()) {
        UserAttributes.userAttributes
            .set(((DefaultQueryService) this.pool.getQueryService()).getUserAttributes(name));
      }
      try {
        if (((CqStateImpl) cqi.getState()).getState() != CqStateImpl.INIT) {
          cqi.createOn(recoveredConnection, isDurable);
        }
      } finally {
        UserAttributes.userAttributes.set(null);
      }
    }
  }

  // TODO this is distressingly similar to LocalRegion#processSingleInterest
  private void recoverSingleRegion(LocalRegion r, Map keys, int interestType,
      Connection recoveredConnection, boolean isDurable, boolean receiveValues,
      boolean isFirstNewConnection) {

    if (logger.isDebugEnabled()) {
      logger.debug("{}.recoverSingleRegion starting kind={} region={}: {}", this,
          InterestType.getString(interestType), r.getFullPath(), keys);
    }

    // build a HashMap, key is policy, value is list
    HashMap policyMap = new HashMap();
    Iterator keysIter = keys.entrySet().iterator();
    while (keysIter.hasNext()) { // restore and commit an interest
      Map.Entry me = (Map.Entry) keysIter.next();
      Object key = me.getKey();
      InterestResultPolicy pol = (InterestResultPolicy) me.getValue();

      if (interestType == InterestType.KEY) {
        // Gester: we only consolidate the key into list for InterestType.KEY
        LinkedList keyList = (LinkedList) policyMap.get(pol);
        if (keyList == null) {

          keyList = new LinkedList();
        }
        keyList.add(key);
        policyMap.put(pol, keyList);
      } else {
        // for other Interest type, do it one by one
        recoverSingleKey(r, key, pol, interestType, recoveredConnection, isDurable, receiveValues,
            isFirstNewConnection);
      }
    }

    // Process InterestType.KEY: Iterator list for each each policy
    Iterator polIter = policyMap.entrySet().iterator();
    while (polIter.hasNext()) {
      Map.Entry me = (Map.Entry) polIter.next();
      LinkedList keyList = (LinkedList) me.getValue();
      InterestResultPolicy pol = (InterestResultPolicy) me.getKey();
      recoverSingleKey(r, keyList, pol, interestType, recoveredConnection, isDurable, receiveValues,
          isFirstNewConnection);
    }
  }

  private void recoverSingleKey(LocalRegion r, Object keys, InterestResultPolicy policy,
      int interestType, Connection recoveredConnection, boolean isDurable, boolean receiveValues,
      boolean isFirstNewConnection) {
    r.startRegisterInterest();
    try {
      // Remove all matching values from local cache
      if (isFirstNewConnection) { // only if this recoveredEP
        // becomes primaryEndpoint
        r.clearKeysOfInterest(keys, interestType, policy);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{}.recoverSingleRegion :Endpoint recovered is primary so clearing the keys of interest starting kind={} region={}: {}",
              this, InterestType.getString(interestType), r.getFullPath(), keys);
        }
      }
      // Register interest, get new values back
      List serverKeys;
      if (policy != InterestResultPolicy.KEYS_VALUES) {
        serverKeys = r.getServerProxy().registerInterestOn(recoveredConnection, keys, interestType,
            policy, isDurable, !receiveValues, r.getAttributes().getDataPolicy().ordinal);
        // Restore keys based on server's response
        if (isFirstNewConnection) {
          // only if this recoveredEP becomes primaryEndpoint
          r.refreshEntriesFromServerKeys(recoveredConnection, serverKeys, policy);
        }
      } else {
        if (!isFirstNewConnection) {
          // InterestResultPolicy.KEYS_VALUES now fetches values in
          // RegisterInterestOp's response itself and in this case
          // refreshEntriesFromServerKeys(...) does not explicitly fetch values
          // but only updates keys-values to region. To not fetch values, we
          // need to use policy NONE or KEYS.
          serverKeys = r.getServerProxy().registerInterestOn(recoveredConnection, keys,
              interestType, InterestResultPolicy.NONE, isDurable, !receiveValues,
              r.getAttributes().getDataPolicy().ordinal);
        } else {
          serverKeys =
              r.getServerProxy().registerInterestOn(recoveredConnection, keys, interestType, policy,
                  isDurable, !receiveValues, r.getAttributes().getDataPolicy().ordinal);
          r.refreshEntriesFromServerKeys(recoveredConnection, serverKeys, policy);
        }
      }
    } finally {
      r.finishRegisterInterest();
    }
  }

  private void recoverInterestList(final Connection recoveredConnection, boolean durable,
      boolean receiveValues, boolean isFirstNewConnection) {
    recoverSingleList(InterestType.KEY, recoveredConnection, durable, receiveValues,
        isFirstNewConnection);
    recoverSingleList(InterestType.REGULAR_EXPRESSION, recoveredConnection, durable, receiveValues,
        isFirstNewConnection);
    recoverSingleList(InterestType.FILTER_CLASS, recoveredConnection, durable, receiveValues,
        isFirstNewConnection);
    recoverSingleList(InterestType.OQL_QUERY, recoveredConnection, durable, receiveValues,
        isFirstNewConnection);
  }

  protected void recoverAllInterestTypes(final Connection recoveredConnection,
      boolean isFirstNewConnection) {
    if (PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG) {
      ClientServerObserver bo = ClientServerObserverHolder.getInstance();
      bo.beforeInterestRecovery();
    }
    recoverInterestList(recoveredConnection, false, true, isFirstNewConnection);
    recoverInterestList(recoveredConnection, false, false, isFirstNewConnection);
    recoverCqs(recoveredConnection, false);
    if (getPool().isDurableClient()) {
      recoverInterestList(recoveredConnection, true, true, isFirstNewConnection);
      recoverInterestList(recoveredConnection, true, false, isFirstNewConnection);
      recoverCqs(recoveredConnection, true);
    }
  }


  /**
   * A comparator which sorts queue elements in the order of primary first redundant with smallest
   * queue size ... redundant with largest queue size
   *
   *
   */
  protected static class QSizeComparator implements java.util.Comparator {
    public int compare(Object o1, Object o2) {
      ServerQueueStatus s1 = (ServerQueueStatus) o1;
      ServerQueueStatus s2 = (ServerQueueStatus) o2;
      // sort primaries to the front of the list
      if (s1.isPrimary() && !s2.isPrimary()) {
        return -1;
      } else if (!s1.isPrimary() && s2.isPrimary()) {
        return 1;
      } else {
        int diff = s1.getServerQueueSize() - s2.getServerQueueSize();
        if (diff != 0) {
          return diff;
        } else {
          return s1.getMemberId().compareTo(s2.getMemberId());
        }
      }
    }
  }

  /**
   * A data structure for holding the current set of connections the queueConnections reference
   * should be maintained by making a copy of this data structure for each change.
   *
   * Note the the order of the backups is significant. The first backup in the list is the first
   * server that will be become primary after the primary fails, etc.
   *
   * The order of backups in this list is the reverse of the order or endpoints from the old
   * ConnectionProxyImpl .
   */
  public class ConnectionList implements QueueConnections {
    private final QueueConnectionImpl primary;
    private final Map/* <Endpoint, QueueConnection> */ connectionMap;
    private final List/* <QueueConnection> */ backups;
    /**
     * The primaryDiscoveryException flag is stronger than just not having any queue connections It
     * also means we tried all of the possible queue servers and we'ren't able to connect.
     */
    private final GemFireException primaryDiscoveryException;
    private final QueueConnectionImpl failedPrimary;

    public ConnectionList() {
      primary = null;
      connectionMap = Collections.EMPTY_MAP;
      backups = Collections.EMPTY_LIST;
      primaryDiscoveryException = null;
      failedPrimary = null;
    }

    private ConnectionList(QueueConnectionImpl primary, List backups,
        GemFireException discoveryException, QueueConnectionImpl failedPrimary) {
      this.primary = primary;
      Map allConnectionsTmp = new HashMap();
      for (Iterator itr = backups.iterator(); itr.hasNext();) {
        QueueConnectionImpl nextConnection = (QueueConnectionImpl) itr.next();
        allConnectionsTmp.put(nextConnection.getEndpoint(), nextConnection);
      }
      if (primary != null) {
        allConnectionsTmp.put(primary.getEndpoint(), primary);
      }
      this.connectionMap = Collections.unmodifiableMap(allConnectionsTmp);
      this.backups = Collections.unmodifiableList(new ArrayList(backups));
      pool.getStats().setSubscriptionCount(connectionMap.size());
      this.primaryDiscoveryException = discoveryException;
      this.failedPrimary = failedPrimary;
    }

    public ConnectionList setPrimary(QueueConnectionImpl newPrimary) {
      List newBackups = backups;
      if (backups.contains(newPrimary)) {
        newBackups = new ArrayList(backups);
        newBackups.remove(newPrimary);
      }
      return new ConnectionList(newPrimary, newBackups, null, null);
    }

    public ConnectionList setPrimaryDiscoveryFailed(GemFireException p_discoveryException) {
      GemFireException discoveryException = p_discoveryException;
      if (discoveryException == null) {
        discoveryException =
            new NoSubscriptionServersAvailableException("Primary discovery failed.");
      }
      return new ConnectionList(primary, backups, discoveryException, failedPrimary);
    }

    public ConnectionList addBackup(QueueConnectionImpl queueConnection) {
      ArrayList newBackups = new ArrayList(backups);
      newBackups.add(queueConnection);
      return new ConnectionList(primary, newBackups, primaryDiscoveryException, failedPrimary);
    }

    public ConnectionList removeConnection(QueueConnectionImpl connection) {
      if (primary == connection) {
        return new ConnectionList(null, backups, primaryDiscoveryException, primary);
      } else {
        ArrayList newBackups = new ArrayList(backups);
        newBackups.remove(connection);
        return new ConnectionList(primary, newBackups, primaryDiscoveryException, failedPrimary);
      }
    }

    public Connection getPrimary() {
      return primary;
    }

    public List/* <QueueConnection> */ getBackups() {
      return backups;
    }

    /**
     * Return the cache client updater from the previously failed primary
     *
     * @return the previous updater or null if there is no previous updater
     */
    public ClientUpdater getFailedUpdater() {
      if (failedPrimary != null) {
        return failedPrimary.getUpdater();
      } else {
        return null;
      }
    }

    public boolean primaryDiscoveryFailed() {
      return primaryDiscoveryException != null;
    }

    public GemFireException getPrimaryDiscoveryException() {
      return primaryDiscoveryException;
    }

    public QueueConnectionImpl getConnection(Endpoint endpoint) {
      return (QueueConnectionImpl) connectionMap.get(endpoint);
    }

    /** return a copy of the list of all server locations */
    public Set/* <ServerLocation> */ getAllLocations() {
      HashSet locations = new HashSet();
      for (Iterator itr = connectionMap.keySet().iterator(); itr.hasNext();) {
        org.apache.geode.cache.client.internal.Endpoint endpoint =
            (org.apache.geode.cache.client.internal.Endpoint) itr.next();
        locations.add(endpoint.getLocation());
      }

      return locations;
    }
  }

  protected void logError(String message, Throwable t) {
    if (t instanceof GemFireSecurityException) {
      securityLogger.error(message, t);
    } else {
      logger.error(message, t);
    }
  }

  /**
   * Asynchronous task which tries to restablish a primary connection and satisfy redundant
   * requirements.
   *
   * This task should only be running in a single thread at a time. This task is the only way that
   * new queue servers will be added, and the only way that a backup server can transistion to a
   * primary server.
   *
   */
  protected class RedundancySatisfierTask extends PoolTask {
    private boolean isCancelled;
    private ScheduledFuture future;

    public void setFuture(ScheduledFuture future) {
      this.future = future;
    }

    public long getRemainingDelay() {
      return future.getDelay(TimeUnit.MILLISECONDS);
    }

    @Override
    public void run2() {
      try {
        initializedLatch.await();
        synchronized (lock) {
          if (isCancelled) {
            return;
          } else {
            redundancySatisfierTask = null;
          }
          if (pool.getPoolOrCacheCancelInProgress() != null) {
            /* wake up waiters so they can detect cancel */
            lock.notifyAll();
            return;
          }
        }
        Set excludedServers = queueConnections.getAllLocations();
        excludedServers.addAll(denyList.getBadServers());
        excludedServers.addAll(factory.getDenyList().getBadServers());
        recoverPrimary(excludedServers);
        recoverRedundancy(excludedServers, true);
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (CancelException e) {
        throw e;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        synchronized (lock) {
          if (t instanceof GemFireSecurityException) {
            queueConnections =
                queueConnections.setPrimaryDiscoveryFailed((GemFireSecurityException) t);
          } else {
            queueConnections = queueConnections.setPrimaryDiscoveryFailed(null);
          }
          lock.notifyAll();
          pool.getCancelCriterion().checkCancelInProgress(t);
          logError("Error in redundancy satisfier", t);
        }
      }

      scheduleRedundancySatisfierIfNeeded(redundancyRetryInterval);
    }

    public boolean cancel() {
      synchronized (lock) {
        if (isCancelled) {
          return false;
        }
        isCancelled = true;
        future.cancel(false);
        redundancySatisfierTask = null;
        return true;
      }
    }

  }

  public static void loadEmergencyClasses() {
    QueueConnectionImpl.loadEmergencyClasses();
  }
}
