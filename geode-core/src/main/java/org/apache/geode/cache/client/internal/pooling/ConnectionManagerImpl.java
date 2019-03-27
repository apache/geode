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
package org.apache.geode.cache.client.internal.pooling;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.ConnectionFactory;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.EndpointManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.PoolImpl.PoolTask;
import org.apache.geode.cache.client.internal.QueueConnectionImpl;
import org.apache.geode.distributed.PoolCancelledException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.i18n.StringId;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.security.GemFireSecurityException;

/**
 * Manages client to server connections for the connection pool. This class contains all of the
 * pooling logic to checkout/checkin connections.
 *
 * @since GemFire 5.7
 *
 */
public class ConnectionManagerImpl implements ConnectionManager {
  private static final Logger logger = LogService.getLogger();
  public static final int NOT_WAITING = -1;

  private final String poolName;
  private final PoolStats poolStats;
  protected final long prefillRetry; // ms
  private final AvailableConnectionManager availableConnectionManager =
      new AvailableConnectionManager();
  protected final ConnectionMap allConnectionsMap = new ConnectionMap();
  private final EndpointManager endpointManager;
  private final long idleTimeout; // make this an int
  protected final long idleTimeoutNanos;
  final int lifetimeTimeout;
  final long lifetimeTimeoutNanos;
  private final InternalLogWriter securityLogWriter;
  protected final CancelCriterion cancelCriterion;

  private final ConnectionAccounting connectionAccounting;
  protected ScheduledExecutorService backgroundProcessor;
  protected ScheduledExecutorService loadConditioningProcessor;

  private ConnectionFactory connectionFactory;
  protected boolean haveIdleExpireConnectionsTask;
  protected final AtomicBoolean havePrefillTask = new AtomicBoolean(false);
  private boolean keepAlive = false;
  protected final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private EndpointManager.EndpointListenerAdapter endpointListener;

  /**
   * Adds an arbitrary variance to a positive temporal interval. Where possible, 10% of the interval
   * is added or subtracted from the interval. Otherwise, 1 is added or subtracted from the
   * interval. For all positive intervals, the returned value will <bold>not</bold> equal the
   * supplied interval.
   *
   * @param interval Positive temporal interval.
   * @return Adjusted interval including the variance for positive intervals; the unmodified
   *         interval for non-positive intervals.
   */
  static int addVarianceToInterval(int interval) {
    if (1 <= interval) {
      final SplittableRandom random = new SplittableRandom();
      final int variance = (interval < 10) ? 1 : (1 + random.nextInt((interval / 10) - 1));
      final int sign = random.nextBoolean() ? 1 : -1;
      return interval + (sign * variance);
    }
    return interval;
  }

  /**
   * Create a connection manager
   *
   * @param poolName the name of the pool that owns us
   * @param factory the factory for new connections
   * @param maxConnections The maximum number of connections that can exist
   * @param minConnections The minimum number of connections that can exist
   * @param idleTimeout The amount of time to wait to expire idle connections. -1 means that idle
   *        connections are never expired.
   * @param lifetimeTimeout the lifetimeTimeout in ms.
   */
  public ConnectionManagerImpl(String poolName, ConnectionFactory factory,
      EndpointManager endpointManager, int maxConnections, int minConnections, long idleTimeout,
      int lifetimeTimeout, InternalLogWriter securityLogger, long pingInterval,
      CancelCriterion cancelCriterion, PoolStats poolStats) {
    this.poolName = poolName;
    this.poolStats = poolStats;
    if (maxConnections < minConnections && maxConnections != -1) {
      throw new IllegalArgumentException(
          "Max connections " + maxConnections + " is less than minConnections " + minConnections);
    }
    if (maxConnections <= 0 && maxConnections != -1) {
      throw new IllegalArgumentException(
          "Max connections " + maxConnections + " must be greater than 0");
    }
    if (minConnections < 0) {
      throw new IllegalArgumentException(
          "Min connections " + minConnections + " must be greater than or equals to 0");
    }

    this.connectionFactory = factory;
    this.endpointManager = endpointManager;
    this.connectionAccounting = new ConnectionAccounting(minConnections,
        maxConnections == -1 ? Integer.MAX_VALUE : maxConnections);
    this.lifetimeTimeout = addVarianceToInterval(lifetimeTimeout);
    this.lifetimeTimeoutNanos = MILLISECONDS.toNanos(this.lifetimeTimeout);
    if (this.lifetimeTimeout != -1) {
      if (idleTimeout > this.lifetimeTimeout || idleTimeout == -1) {
        // lifetimeTimeout takes precedence over longer idle timeouts
        idleTimeout = this.lifetimeTimeout;
      }
    }
    this.idleTimeout = idleTimeout;
    this.idleTimeoutNanos = MILLISECONDS.toNanos(this.idleTimeout);
    this.securityLogWriter = securityLogger;
    this.prefillRetry = pingInterval;
    this.cancelCriterion = cancelCriterion;
    this.endpointListener = new EndpointManager.EndpointListenerAdapter() {
      @Override
      public void endpointCrashed(Endpoint endpoint) {
        invalidateServer(endpoint);
      }
    };
  }

  private void destroyAndMaybePrefill() {
    destroyAndMaybePrefill(1);
  }

  private void destroyAndMaybePrefill(int count) {
    if (connectionAccounting.destroyAndIsUnderMinimum(count)) {
      startBackgroundPrefill();
    }
  }

  private PooledConnection createPooledConnection()
      throws NoAvailableServersException, ServerOperationException {
    return createPooledConnection(Collections.emptySet());
  }

  private PooledConnection createPooledConnection(Set<ServerLocation> excludedServers)
      throws NoAvailableServersException, ServerOperationException {
    try {
      return addConnection(connectionFactory.createClientToServerConnection(excludedServers));
    } catch (GemFireSecurityException e) {
      throw new ServerOperationException(e);
    } catch (ServerRefusedConnectionException e) {
      throw new NoAvailableServersException(e);
    }
  }

  private PooledConnection createPooledConnection(ServerLocation serverLocation)
      throws ServerRefusedConnectionException, GemFireSecurityException {
    return addConnection(connectionFactory.createClientToServerConnection(serverLocation, false));
  }

  /**
   * Always creates a connection and may cause {@link #connectionCount} to exceed
   * {@link #maxConnections}.
   */
  private PooledConnection forceCreateConnection(ServerLocation serverLocation)
      throws ServerRefusedConnectionException, ServerOperationException {
    connectionAccounting.create();
    try {
      return createPooledConnection(serverLocation);
    } catch (GemFireSecurityException e) {
      throw new ServerOperationException(e);
    }
  }

  /**
   * Always creates a connection and may cause {@link #connectionCount} to exceed
   * {@link #maxConnections}.
   */
  private PooledConnection forceCreateConnection(Set<ServerLocation> excludedServers)
      throws NoAvailableServersException, ServerOperationException {
    connectionAccounting.create();
    return createPooledConnection(excludedServers);
  }

  private boolean checkShutdownInterruptedOrTimeout(final long timeout)
      throws PoolCancelledException {
    if (shuttingDown.get()) {
      throw new PoolCancelledException();
    }

    if (Thread.currentThread().isInterrupted()) {
      return true;
    }

    if (timeout < System.nanoTime()) {
      return true;
    }

    return false;
  }

  private long beginConnectionWaitStatIfNotStarted(final long waitStart) {
    if (NOT_WAITING == waitStart) {
      return getPoolStats().beginConnectionWait();
    }

    return waitStart;
  }

  private void endConnectionWaitStatIfStarted(final long waitStart) {
    if (NOT_WAITING != waitStart) {
      getPoolStats().endConnectionWait(waitStart);
    }
  }

  // TODO reevaluate this
  @Override
  public Connection borrowConnection(long acquireTimeout)
      throws AllConnectionsInUseException, NoAvailableServersException, ServerOperationException {
    long waitStart = NOT_WAITING;
    try {
      long timeout = System.nanoTime() + MILLISECONDS.toNanos(acquireTimeout);
      while (true) {
        PooledConnection connection = availableConnectionManager.useFirst();
        if (null != connection) {
          return connection;
        }

        if (connectionAccounting.tryCreate()) {
          try {
            connection = createPooledConnection();
            if (null != connection) {
              return connection;
            }
            throw new NoAvailableServersException();
          } finally {
            if (connection == null) {
              int currentCount = connectionAccounting.cancelTryCreate();
              if (currentCount < connectionAccounting.getMinimum()) {
                startBackgroundPrefill();
              }
            }
          }
        }

        if (checkShutdownInterruptedOrTimeout(timeout)) {
          break;
        }

        waitStart = beginConnectionWaitStatIfNotStarted(waitStart);

        Thread.yield();
      }
    } finally {
      endConnectionWaitStatIfStarted(waitStart);
    }

    this.cancelCriterion.checkCancelInProgress(null);
    throw new AllConnectionsInUseException();
  }

  /**
   * Borrow a connection to a specific server. This task currently allows us to break the connection
   * limit, because it is used by tasks from the background thread that shouldn't be constrained by
   * the limit. They will only violate the limit by 1 connection, and that connection will be
   * destroyed when returned to the pool.
   */
  @Override
  public Connection borrowConnection(ServerLocation server, long acquireTimeout,
      boolean onlyUseExistingCnx) throws AllConnectionsInUseException, NoAvailableServersException {
    PooledConnection connection =
        availableConnectionManager.useFirst((c) -> c.getServer().equals(server));
    if (null != connection) {
      return connection;
    }

    if (onlyUseExistingCnx) {
      throw new AllConnectionsInUseException();
    }

    connection = forceCreateConnection(server);
    if (null != connection) {
      return connection;
    }

    throw new ServerConnectivityException(
        "Could not create a new connection to server " + server);
  }

  @Override
  public Connection exchangeConnection(final Connection oldConnection,
      final Set/* <ServerLocation> */ excludedServers, final long acquireTimeout)
      throws AllConnectionsInUseException {

    try {
      PooledConnection connection = availableConnectionManager
          .useFirst((c) -> !excludedServers.contains(c.getServer()));
      if (null != connection) {
        return connection;
      }

      connection = forceCreateConnection(excludedServers);
      if (null != connection) {
        return connection;
      }

      throw new NoAvailableServersException();
    } finally {
      returnConnection(oldConnection, true, true);
    }
  }

  protected/* GemStoneAddition */ String getPoolName() {
    return this.poolName;
  }

  private PooledConnection addConnection(Connection conn) {

    if (conn == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to create a connection in the allowed time");
      }
      return null;
    }
    PooledConnection pooledConn = new PooledConnection(this, conn);
    allConnectionsMap.addConnection(pooledConn);
    if (logger.isDebugEnabled()) {
      logger.debug("Created a new connection. {} Connection count is now {}", pooledConn,
          connectionAccounting.getCount());
    }
    return pooledConn;
  }

  private void destroyConnection(PooledConnection connection) {
    if (allConnectionsMap.removeConnection(connection)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Invalidating connection {} connection count is now {}", connection,
            connectionAccounting.getCount());
      }

      destroyAndMaybePrefill();
    }

    connection.internalDestroy();
  }


  protected void invalidateServer(Endpoint endpoint) {
    Set badConnections = allConnectionsMap.removeEndpoint(endpoint);
    if (badConnections == null) {
      return;
    }

    if (shuttingDown.get()) {
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Invalidating {} connections to server {}", badConnections.size(), endpoint);
    }

    for (Iterator itr = badConnections.iterator(); itr.hasNext();) {
      PooledConnection conn = (PooledConnection) itr.next();
      // TODO is this right?
      if (!conn.isDestroyed()) {
        conn.setShouldDestroy();
        availableConnectionManager.remove(conn);
        destroyAndMaybePrefill();
        conn.internalDestroy();
      }
    }
  }

  @Override
  public void returnConnection(Connection connection) {
    returnConnection(connection, true);
  }

  @Override
  public void returnConnection(Connection connection, boolean accessed) {
    returnConnection(connection, accessed, false);
  }

  private void returnConnection(Connection connection, boolean accessed, boolean addLast) {
    assert connection instanceof PooledConnection;
    PooledConnection pooledConn = (PooledConnection) connection;

    if (pooledConn.isDestroyed()) {
      return;
    }

    if (pooledConn.shouldDestroy()) {
      destroyConnection(pooledConn);
    } else if (!destroyIfOverLimit(pooledConn)) {
      if (addLast) {
        availableConnectionManager.addLast(pooledConn, accessed);
      } else {
        availableConnectionManager.addFirst(pooledConn, accessed);
      }
    }
  }

  /**
   * Destroys connection if and only if {@link #connectionCount} exceeds {@link #maxConnections}.
   *
   * @return true if connection is destroyed, otherwise false.
   */
  private boolean destroyIfOverLimit(PooledConnection connection) {
    if (connectionAccounting.tryDestroy()) {
      if (allConnectionsMap.removeConnection(connection)) {
        try {
          PoolImpl localpool = (PoolImpl) PoolManagerImpl.getPMI().find(poolName);
          boolean durable = false;
          if (localpool != null) {
            durable = localpool.isDurableClient();
          }
          connection.internalClose(durable || this.keepAlive);
        } catch (Exception e) {
          logger.warn(String.format("Error closing connection %s", connection), e);
        }
      } else {
        // Not a pooled connection so undo the decrement.
        connectionAccounting.cancelTryDestroy();
      }

      return true;
    }

    return false;
  }

  @Override
  public void start(ScheduledExecutorService backgroundProcessor) {
    this.backgroundProcessor = backgroundProcessor;
    String name = "poolLoadConditioningMonitor-" + getPoolName();
    this.loadConditioningProcessor =
        LoggingExecutors.newScheduledThreadPool(name, 1, false);

    endpointManager.addListener(endpointListener);

    startBackgroundPrefill();
  }

  @Override
  public void close(boolean keepAlive) {
    if (logger.isDebugEnabled()) {
      logger.debug("Shutting down connection manager with keepAlive {}", keepAlive);
    }
    this.keepAlive = keepAlive;
    endpointManager.removeListener(endpointListener);

    if (!shuttingDown.compareAndSet(false, true)) {
      return;
    }

    try {
      if (this.loadConditioningProcessor != null) {
        this.loadConditioningProcessor.shutdown();
        if (!this.loadConditioningProcessor.awaitTermination(PoolImpl.SHUTDOWN_TIMEOUT,
            MILLISECONDS)) {
          logger.warn("Timeout waiting for load conditioning tasks to complete");
        }
      }
    } catch (RuntimeException e) {
      logger.error("Error stopping loadConditioningProcessor", e);
    } catch (InterruptedException e) {
      logger.error(
          "Interrupted stopping loadConditioningProcessor",
          e);
    }
    allConnectionsMap.close(keepAlive);
  }

  @Override
  public void emergencyClose() {
    shuttingDown.set(true);
    if (this.loadConditioningProcessor != null) {
      this.loadConditioningProcessor.shutdown();
    }
    allConnectionsMap.emergencyClose();
  }

  protected void startBackgroundExpiration() {
    if (idleTimeout >= 0) {
      synchronized (this.allConnectionsMap) {
        if (!haveIdleExpireConnectionsTask) {
          haveIdleExpireConnectionsTask = true;
          try {
            backgroundProcessor.schedule(new IdleExpireConnectionsTask(), idleTimeout,
                MILLISECONDS);
          } catch (RejectedExecutionException e) {
            // ignore, the timer has been cancelled, which means we're shutting
            // down.
          }
        }
      }
    }
  }

  protected void startBackgroundPrefill() {
    if (havePrefillTask.compareAndSet(false, true)) {
      try {
        backgroundProcessor.execute(new PrefillConnectionsTask());
      } catch (RejectedExecutionException e) {
        // ignore, the timer has been cancelled, which means we're shutting
        // down.
      }
    }
  }

  protected boolean prefill() {
    try {
      while (connectionAccounting.isUnderMinimum()) {
        if (cancelCriterion.isCancelInProgress()) {
          return true;
        }
        boolean createdConnection = prefillConnection();
        if (!createdConnection) {
          return false;
        }
      }
    } catch (Throwable t) {
      cancelCriterion.checkCancelInProgress(t);
      if (t.getCause() != null) {
        t = t.getCause();
      }
      logInfo("Error prefilling connections", t);
      return false;
    }

    return true;
  }

  @Override
  public int getConnectionCount() {
    return connectionAccounting.getCount();
  }

  protected PoolStats getPoolStats() {
    return this.poolStats;
  }

  @Override
  public Connection getConnection(Connection conn) {
    if (conn instanceof PooledConnection) {
      return ((PooledConnection) conn).getConnection();
    } else if (conn instanceof QueueConnectionImpl) {
      return ((QueueConnectionImpl) conn).getConnection();
    } else {
      return conn;
    }
  }


  private boolean prefillConnection() {
    if (shuttingDown.get()) {
      return false;
    }

    if (connectionAccounting.tryPrefill()) {
      PooledConnection connection = null;
      try {
        connection = createPooledConnection();
        if (connection == null) {
          return false;
        }
        getPoolStats().incPrefillConnect();
        availableConnectionManager.addLast(connection, false);
        if (logger.isDebugEnabled()) {
          logger.debug("Prefilled connection {} connection count is now {}", connection,
              connectionAccounting.getCount());
        }
        return true;
      } catch (ServerConnectivityException ex) {
        logger.info(
            String.format("Unable to prefill pool to minimum because: %s", ex.getMessage()));
        return false;
      } finally {
        if (connection == null) {
          connectionAccounting.cancelTryPrefill();

          if (logger.isDebugEnabled()) {
            logger.debug("Unable to prefill pool to minimum, connection count is now {}",
                this::getConnectionCount);
          }
        }
      }
    }

    return false;
  }

  public static void loadEmergencyClasses() {
    PooledConnection.loadEmergencyClasses();
  }

  protected class LifetimeExpireConnectionsTask implements Runnable {
    @Override
    public void run() {
      try {
        allConnectionsMap.checkLifetimes();
      } catch (CancelException ignore) {
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        SystemFailure.checkFailure();
        logger.warn(String.format("LoadConditioningTask <%s> encountered exception",
            this),
            t);
        // Don't rethrow, it will just get eaten and kill the timer
      }
    }
  }

  protected class IdleExpireConnectionsTask implements Runnable {
    @Override
    public void run() {
      try {
        getPoolStats().incIdleCheck();
        allConnectionsMap.expireIdleConnections();
      } catch (CancelException ignore) {
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        // NOTREACHED
        throw e; // for safety
      } catch (Throwable t) {
        SystemFailure.checkFailure();
        logger.warn(String.format("IdleExpireConnectionsTask <%s> encountered exception",
            this),
            t);
        // Don't rethrow, it will just get eaten and kill the timer
      }
    }
  }

  protected class PrefillConnectionsTask extends PoolTask {

    @Override
    public void run2() {
      if (logger.isTraceEnabled()) {
        logger.trace("Prefill Connections task running");
      }
      prefill();
      if (connectionAccounting.isUnderMinimum() && !cancelCriterion.isCancelInProgress()) {
        try {
          backgroundProcessor.schedule(new PrefillConnectionsTask(), prefillRetry,
              MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
          // ignore, the timer has been cancelled, which means we're shutting down.
        }
      } else {
        havePrefillTask.set(false);
      }
    }
  }

  /**
   * Offer the replacement "con" to any cnx currently connected to "currentServer".
   *
   * @return true if someone takes our offer; false if not
   */
  private boolean offerReplacementConnection(Connection con, ServerLocation currentServer) {
    boolean retry;
    do {
      retry = false;
      PooledConnection target = this.allConnectionsMap.findReplacementTarget(currentServer);
      if (target != null) {
        final Endpoint targetEP = target.getEndpoint();
        boolean interrupted = false;
        try {
          if (target.switchConnection(con)) {
            getPoolStats().incLoadConditioningDisconnect();
            this.allConnectionsMap.addReplacedCnx(target, targetEP);
            return true;
          } else {
            retry = true;
          }
        } catch (InterruptedException e) {
          // thrown by switchConnection
          interrupted = true;
          cancelCriterion.checkCancelInProgress(e);
          retry = false;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    } while (retry);
    getPoolStats().incLoadConditioningReplaceTimeouts();
    con.destroy();
    return false;
  }

  /**
   * An existing connections lifetime has expired. We only want to create one replacement connection
   * at a time so this should block until this connection replaces an existing one. Note that if
   * a connection is created here it must not count against the pool max and its idle time and
   * lifetime must not begin until it actually replaces the existing one.
   *
   * @param currentServer the server the candidate connection is connected to
   * @param idlePossible true if we have more cnxs than minPoolSize
   * @return true if caller should recheck for expired lifetimes; false if a background check was
   *         scheduled or no expirations are possible.
   */
  public boolean createLifetimeReplacementConnection(ServerLocation currentServer,
      boolean idlePossible) {
    HashSet<ServerLocation> excludedServers = new HashSet<>();
    while (true) {
      ServerLocation sl = connectionFactory.findBestServer(currentServer, excludedServers);
      if (sl == null || sl.equals(currentServer)) {
        // we didn't find a server to create a replacement cnx on so
        // extends the currentServers life
        allConnectionsMap.extendLifeOfCnxToServer(currentServer);
        break;
      }
      if (!allConnectionsMap.hasExpiredCnxToServer(currentServer)) {
        break;
      }
      Connection con = null;
      try {
        con = connectionFactory.createClientToServerConnection(sl, false);
        if (con != null) {
          getPoolStats().incLoadConditioningConnect();
          if (allConnectionsMap.hasExpiredCnxToServer(currentServer)) {
            offerReplacementConnection(con, currentServer);
          } else {
            getPoolStats().incLoadConditioningReplaceTimeouts();
            con.destroy();
          }
          break;
        }
      } catch (GemFireSecurityException e) {
        securityLogWriter.warning(
            String.format("Security exception connecting to server '%s': %s",
                new Object[] {sl, e}));
      } catch (ServerRefusedConnectionException srce) {
        logger.warn("Server '{}' refused new connection: {}",
            new Object[] {sl, srce});
      }
      excludedServers.add(sl);
    }
    return allConnectionsMap.checkForReschedule(true);
  }

  protected class ConnectionMap {
    private final Map<Endpoint, Set<PooledConnection>> map = new HashMap<>();
    private List<PooledConnection> allConnections = new LinkedList<>();
    private boolean haveLifetimeExpireConnectionsTask;
    volatile boolean closing;

    public synchronized boolean isIdleExpirePossible() {
      return this.allConnections.size() > connectionAccounting.getMinimum();
    }

    @Override
    public synchronized String toString() {
      final long now = System.nanoTime();
      StringBuffer sb = new StringBuffer();
      sb.append("<");
      for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
        PooledConnection pc = (PooledConnection) it.next();
        sb.append(pc.getServer());
        if (pc.shouldDestroy()) {
          sb.append("-DESTROYED");
        } else if (pc.hasIdleExpired(now, idleTimeoutNanos)) {
          sb.append("-IDLE");
        } else if (pc.remainingLife(now, lifetimeTimeoutNanos) <= 0) {
          sb.append("-EOL");
        }
        if (it.hasNext()) {
          sb.append(",");
        }
      }
      sb.append(">");
      return sb.toString();
    }

    public synchronized void addConnection(PooledConnection connection) {
      if (this.closing) {
        throw new CacheClosedException("This pool is closing");
      }

      getPoolStats().incPoolConnections(1);

      // we want the smallest birthDate (e.g. oldest cnx) at the front of the list
      this.allConnections.add(connection);

      addToEndpointMap(connection);

      if (isIdleExpirePossible()) {
        startBackgroundExpiration();
      }
      if (lifetimeTimeout != -1 && !haveLifetimeExpireConnectionsTask) {
        if (checkForReschedule(true)) {
          // something has already expired so start processing with no delay
          startBackgroundLifetimeExpiration(0);
        } else {
          // either no possible lifetime expires or we scheduled one
        }
      }
    }

    public synchronized void addReplacedCnx(PooledConnection con, Endpoint oldEndpoint) {
      if (this.closing) {
        throw new CacheClosedException("This pool is closing");
      }
      if (this.allConnections.remove(con)) {
        // otherwise someone else has removed it and closed it
        removeFromEndpointMap(oldEndpoint, con);
        addToEndpointMap(con);
        this.allConnections.add(con);
        if (isIdleExpirePossible()) {
          startBackgroundExpiration();
        }
      }
    }

    public synchronized Set removeEndpoint(Endpoint endpoint) {
      final Set endpointConnections = (Set) this.map.remove(endpoint);
      if (endpointConnections != null) {
        int count = 0;
        for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
          if (endpointConnections.contains(it.next())) {
            count++;
            it.remove();
          }
        }
        if (count != 0) {
          getPoolStats().incPoolConnections(-count);
        }
      }
      return endpointConnections;
    }

    public synchronized boolean removeConnection(PooledConnection connection) {
      boolean result = this.allConnections.remove(connection);
      if (result) {
        getPoolStats().incPoolConnections(-1);
      }

      removeFromEndpointMap(connection);
      return result;
    }

    private synchronized void addToEndpointMap(PooledConnection connection) {
      Set<PooledConnection> endpointConnections = map.get(connection.getEndpoint());
      if (endpointConnections == null) {
        endpointConnections = new HashSet();
        map.put(connection.getEndpoint(), endpointConnections);
      }
      endpointConnections.add(connection);
    }

    private void removeFromEndpointMap(PooledConnection connection) {
      removeFromEndpointMap(connection.getEndpoint(), connection);
    }

    private synchronized void removeFromEndpointMap(Endpoint endpoint,
        PooledConnection connection) {
      Set<PooledConnection> endpointConnections = this.map.get(endpoint);
      if (endpointConnections != null) {
        endpointConnections.remove(connection);
        if (endpointConnections.size() == 0) {
          this.map.remove(endpoint);
        }
      }
    }

    public void close(boolean keepAlive) {
      List<PooledConnection> connections;
      int count = 0;

      synchronized (this) {
        if (closing) {
          return;
        }
        closing = true;
        map.clear();
        connections = allConnections;
        allConnections = new ClosedPoolConnectionList();
      }

      for (PooledConnection pc : connections) {
        count++;
        if (!pc.isDestroyed()) {
          try {
            pc.internalClose(keepAlive);
          } catch (SocketException se) {
            logger.info("Error closing connection to server " +
                pc.getServer(),
                se);
          } catch (Exception e) {
            logger.warn("Error closing connection to server " +
                pc.getServer(),
                e);
          }
        }
      }
      if (count != 0) {
        getPoolStats().incPoolConnections(-count);
      }
    }

    public synchronized void emergencyClose() {
      closing = true;
      map.clear();
      while (!this.allConnections.isEmpty()) {
        PooledConnection pc = (PooledConnection) this.allConnections.remove(0);
        pc.emergencyClose();
      }
    }

    /**
     * Returns a pooled connection that can have its underlying cnx to currentServer replaced by a
     * new connection.
     *
     * @return null if a target could not be found
     */
    public synchronized PooledConnection findReplacementTarget(ServerLocation currentServer) {
      final long now = System.nanoTime();
      for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
        PooledConnection pc = (PooledConnection) it.next();
        if (currentServer.equals(pc.getServer())) {
          if (!pc.shouldDestroy() && pc.remainingLife(now, lifetimeTimeoutNanos) <= 0) {
            removeFromEndpointMap(pc);
            return pc;
          }
        }
      }
      return null;
    }

    /**
     * Return true if we have a connection to the currentServer whose lifetime has expired.
     * Otherwise return false;
     */
    public synchronized boolean hasExpiredCnxToServer(ServerLocation currentServer) {
      if (!this.allConnections.isEmpty()) {
        final long now = System.nanoTime();
        for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
          PooledConnection pc = (PooledConnection) it.next();
          if (pc.shouldDestroy()) {
            // this con has already been destroyed so ignore it
            continue;
          } else if (currentServer.equals(pc.getServer())) {
            {
              long life = pc.remainingLife(now, lifetimeTimeoutNanos);
              if (life <= 0) {
                return true;
              }
            }
          }
        }
      }
      return false;
    }

    /**
     * Returns true if caller should recheck for expired lifetimes Returns false if a background
     * check was scheduled or no expirations are possible.
     */
    public synchronized boolean checkForReschedule(boolean rescheduleOk) {
      if (!this.allConnections.isEmpty()) {
        final long now = System.nanoTime();
        for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
          PooledConnection pc = (PooledConnection) it.next();
          if (pc.hasIdleExpired(now, idleTimeoutNanos)) {
            // this con has already idle expired so ignore it
            continue;
          } else if (pc.shouldDestroy()) {
            // this con has already been destroyed so ignore it
            continue;
          } else {
            long life = pc.remainingLife(now, lifetimeTimeoutNanos);
            if (life > 0) {
              if (rescheduleOk) {
                startBackgroundLifetimeExpiration(life);
                return false;
              } else {
                return false;
              }
            } else {
              return true;
            }
          }
        }
      }
      return false;
    }

    /**
     * Extend the life of the first expired connection to sl.
     */
    public synchronized void extendLifeOfCnxToServer(ServerLocation sl) {
      if (!this.allConnections.isEmpty()) {
        final long now = System.nanoTime();
        for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
          PooledConnection pc = (PooledConnection) it.next();
          if (pc.remainingLife(now, lifetimeTimeoutNanos) > 0) {
            // no more connections whose lifetime could have expired
            break;
            // note don't ignore idle connections because they are still connected
            // } else if (pc.remainingIdle(now, idleTimeoutNanos) <= 0) {
            // // this con has already idle expired so ignore it
          } else if (pc.shouldDestroy()) {
            // this con has already been destroyed so ignore it
          } else if (sl.equals(pc.getEndpoint().getLocation())) {
            // we found a connection to whose lifetime we can extend
            it.remove();
            pc.setBirthDate(now);
            getPoolStats().incLoadConditioningExtensions();
            this.allConnections.add(pc);
            // break so we only do this to the oldest connection
            break;
          }
        }
      }
    }

    public synchronized void startBackgroundLifetimeExpiration(long delay) {
      if (!this.haveLifetimeExpireConnectionsTask) {
        this.haveLifetimeExpireConnectionsTask = true;
        try {
          LifetimeExpireConnectionsTask task = new LifetimeExpireConnectionsTask();
          loadConditioningProcessor.schedule(task, delay, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException e) {
          // ignore, the timer has been cancelled, which means we're shutting down.
        }
      }
    }

    public void expireIdleConnections() {
      int expireCount = 0;
      List<PooledConnection> toClose = null;
      synchronized (this) {
        haveIdleExpireConnectionsTask = false;
        if (shuttingDown.get()) {
          return;
        }
        if (logger.isTraceEnabled()) {
          logger.trace("Looking for connections to expire");
        }

        // find connections which have idle expired
        if (!connectionAccounting.isOverMinimum()) {
          return;
        }

        long minRemainingIdle = Long.MAX_VALUE;
        int conCount = connectionAccounting.getCount();
        toClose = new ArrayList<>(conCount - connectionAccounting.getMinimum());

        // because we expire thread local connections we need to scan allConnections
        for (Iterator it = allConnections.iterator(); it.hasNext()
            && conCount > connectionAccounting.getMinimum();) {
          PooledConnection pc = (PooledConnection) it.next();
          if (pc.shouldDestroy()) {
            // ignore these connections
            conCount--;
          } else {
            long remainingIdle = pc.doIdleTimeout(System.nanoTime(), idleTimeoutNanos);
            if (remainingIdle >= 0) {
              if (remainingIdle == 0) {
                // someone else already destroyed pc so ignore it
                conCount--;
              } else if (remainingIdle < minRemainingIdle) {
                minRemainingIdle = remainingIdle;
              }
            } else /* (remainingIdle < 0) */ {
              // this means that we idleExpired the connection
              expireCount++;
              conCount--;
              removeFromEndpointMap(pc);
              toClose.add(pc);
              it.remove();
            }
          }
        }
        if (conCount > connectionAccounting.getMinimum()
            && minRemainingIdle < Long.MAX_VALUE) {
          try {
            backgroundProcessor.schedule(new IdleExpireConnectionsTask(), minRemainingIdle,
                TimeUnit.NANOSECONDS);
          } catch (RejectedExecutionException e) {
            // ignore, the timer has been cancelled, which means we're shutting down.
          }
          haveIdleExpireConnectionsTask = true;
        }
      }

      if (expireCount > 0) {
        getPoolStats().incIdleExpire(expireCount);
        getPoolStats().incPoolConnections(-expireCount);
        destroyAndMaybePrefill(expireCount);
      }

      // now destroy all of the connections, outside the sync
      final boolean isDebugEnabled = logger.isDebugEnabled();
      for (PooledConnection connection : toClose) {
        if (isDebugEnabled) {
          logger.debug("Idle connection detected. Expiring connection {}", connection);
        }
        try {
          connection.internalClose(false);
        } catch (Exception e) {
          logger.warn("Error expiring connection {}", connection);
        }
      }
    }

    public void checkLifetimes() {
      boolean done;
      synchronized (this) {
        this.haveLifetimeExpireConnectionsTask = false;
        if (shuttingDown.get()) {
          return;
        }
      }
      do {
        getPoolStats().incLoadConditioningCheck();
        long firstLife = -1;
        ServerLocation candidate = null;
        boolean idlePossible = true;

        synchronized (this) {
          if (shuttingDown.get()) {
            return;
          }
          // find a connection whose lifetime has expired
          // and who is not already being replaced
          long now = System.nanoTime();
          long life = 0;
          idlePossible = isIdleExpirePossible();
          for (Iterator it = this.allConnections.iterator(); it.hasNext() && life <= 0
              && (candidate == null);) {
            PooledConnection pc = (PooledConnection) it.next();
            // skip over idle expired and destroyed
            life = pc.remainingLife(now, lifetimeTimeoutNanos);
            if (life <= 0) {
              boolean idleTimedOut =
                  idlePossible ? pc.hasIdleExpired(now, idleTimeoutNanos) : false;
              boolean destroyed = pc.shouldDestroy();
              if (!idleTimedOut && !destroyed) {
                candidate = pc.getServer();
              }
            } else if (firstLife == -1) {
              firstLife = life;
            }
          }
        }
        if (candidate != null) {
          done = !createLifetimeReplacementConnection(candidate, idlePossible);
        } else {
          if (firstLife >= 0) {
            // reschedule
            startBackgroundLifetimeExpiration(firstLife);
          }
          done = true; // just to be clear
        }
      } while (!done);
      // If a lifetimeExpire task is not scheduled at this point then
      // schedule one that will do a check in our configured lifetimeExpire.
      // this should not be needed but seems to currently help.
      startBackgroundLifetimeExpiration(lifetimeTimeoutNanos);
    }

  }

  private void logInfo(String message, Throwable t) {
    if (t instanceof GemFireSecurityException) {
      securityLogWriter.info(String.format("%s : %s",
          message, t), t);
    } else {
      logger.info(String.format("%s : %s",
          message, t), t);
    }
  }

  private void logError(StringId message, Throwable t) {
    if (t instanceof GemFireSecurityException) {
      securityLogWriter.error(message, t);
    } else {
      logger.error(message, t);
    }
  }

  @Override
  public boolean activate(Connection conn) {
    assert conn instanceof PooledConnection;
    return ((PooledConnection) conn).activate();
  }

  @Override
  public void passivate(Connection conn, boolean accessed) {
    assert conn instanceof PooledConnection;
    ((PooledConnection) conn).passivate(accessed);
  }

  private static class ClosedPoolConnectionList extends ArrayList {
    @Override
    public Object set(int index, Object element) {
      throw new CacheClosedException("This pool has been closed");
    }

    @Override
    public boolean add(Object element) {
      throw new CacheClosedException("This pool has been closed");
    }

    @Override
    public void add(int index, Object element) {
      throw new CacheClosedException("This pool has been closed");
    }

    @Override
    public Object remove(int index) {
      throw new CacheClosedException("This pool has been closed");
    }

    @Override
    public boolean addAll(Collection c) {
      throw new CacheClosedException("This pool has been closed");
    }

    @Override
    public boolean addAll(int index, Collection c) {
      throw new CacheClosedException("This pool has been closed");
    }
  }
}
