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

import java.io.IOException;
import java.io.NotSerializableException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.BufferUnderflowException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.CopyException;
import org.apache.geode.GemFireException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.cache.client.SubscriptionNotEnabledException;
import org.apache.geode.cache.client.internal.ExecuteFunctionOp.ExecuteFunctionOpImpl;
import org.apache.geode.cache.client.internal.ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl;
import org.apache.geode.cache.client.internal.QueueManager.QueueConnections;
import org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException;
import org.apache.geode.cache.client.internal.pooling.ConnectionManager;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.cache.PutAllPartialResultException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.tier.BatchException;
import org.apache.geode.internal.cache.tier.sockets.MessageTooLargeException;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Called from the client and execute client to server requests against servers. Handles retrying to
 * different servers, and marking servers dead if we get exception from them.
 *
 * @since GemFire 5.7
 */
public class OpExecutorImpl implements ExecutablePool {
  private static final Logger logger = LogService.getLogger();

  private static final boolean TRY_SERVERS_ONCE =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.TRY_SERVERS_ONCE");
  static final int TX_RETRY_ATTEMPT =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "txRetryAttempt", 500);

  private final ConnectionManager connectionManager;
  private final int retryAttempts;
  private final long serverTimeout;
  private final long singleServerTimeout;

  private final EndpointManager endpointManager;
  private final RegisterInterestTracker riTracker;
  private final QueueManager queueManager;
  private final CancelCriterion cancelCriterion;
  private /* final */ PoolImpl pool;
  private final ThreadLocal<Boolean> serverAffinity = ThreadLocal.withInitial(() -> Boolean.FALSE);
  private boolean serverAffinityFailover = false;
  private final ThreadLocal<ServerLocation> affinityServerLocation = new ThreadLocal<>();

  private final ThreadLocal<Integer> affinityRetryCount = ThreadLocal.withInitial(() -> 0);

  public OpExecutorImpl(ConnectionManager connectionManager, QueueManager queueManager,
      EndpointManager endpointManager, RegisterInterestTracker riTracker, int retryAttempts,
      long serverTimeout, long singleServerTimeout, CancelCriterion cancelCriterion,
      PoolImpl pool) {
    this.connectionManager = connectionManager;
    this.queueManager = queueManager;
    this.endpointManager = endpointManager;
    this.riTracker = riTracker;
    this.retryAttempts = retryAttempts;
    this.serverTimeout = serverTimeout;
    this.singleServerTimeout = singleServerTimeout;
    this.cancelCriterion = cancelCriterion;
    this.pool = pool;
  }

  @Override
  public Object execute(Op op) {
    return execute(op, retryAttempts);
  }

  @Override
  public Object execute(Op op, int retries) {
    if (serverAffinity.get()) {
      ServerLocation loc = affinityServerLocation.get();
      if (loc == null) {
        loc = getNextOpServerLocation();
        affinityServerLocation.set(loc);
        if (logger.isDebugEnabled()) {
          logger.debug("setting server affinity to {}", affinityServerLocation.get());
        }
      }
      return executeWithServerAffinity(loc, op);
    }

    Connection conn = connectionManager.borrowConnection(serverTimeout);
    try {
      Set<ServerLocation> attemptedServers = null;

      for (int attempt = 0; true; attempt++) {
        // when an op is retried we may need to try to recover the previous
        // attempt's version stamp
        if (attempt == 1 && (op instanceof AbstractOp)) {
          AbstractOp absOp = (AbstractOp) op;
          absOp.getMessage().setIsRetry();
        }
        try {
          authenticateIfRequired(conn, op);
          return executeWithPossibleReAuthentication(conn, op);
        } catch (MessageTooLargeException e) {
          throw new GemFireIOException("unable to transmit message to server", e);
        } catch (Exception e) {
          handleException(e, conn, attempt, attempt >= retries && retries != -1);
          if (null == attemptedServers) {
            // don't allocate this until we need it
            attemptedServers = new HashSet<>();
          }
          attemptedServers.add(conn.getServer());
          try {
            conn = connectionManager.exchangeConnection(conn, attemptedServers);
          } catch (NoAvailableServersException nse) {
            // if retries is -1, don't try again after the last server has failed
            if (retries == -1 || TRY_SERVERS_ONCE) {
              handleException(e, conn, attempt, true);
            } else {
              // try one of the failed servers again, until we exceed the retry attempts.
              attemptedServers.clear();
              try {
                conn = connectionManager.exchangeConnection(conn, attemptedServers);
              } catch (NoAvailableServersException nse2) {
                handleException(e, conn, attempt, true);
              }
            }
          }
        }
      }
    } finally {
      connectionManager.returnConnection(conn);
    }
  }

  /**
   * execute the given op on the given server. If the server cannot be reached, sends a
   * TXFailoverOp, then retries the given op
   *
   * @param loc the server to execute the op on
   * @param op the op to execute
   * @return the result of execution
   */
  Object executeWithServerAffinity(ServerLocation loc, Op op) {
    final int initialRetryCount = getAffinityRetryCount();
    try {
      try {
        return executeOnServer(loc, op, true, false);
      } catch (ServerConnectivityException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("caught exception while executing with affinity:{}", e.getMessage(), e);
        }
        if (!serverAffinityFailover || e instanceof ServerOperationException) {
          throw e;
        }
        int retryCount = getAffinityRetryCount();
        if ((retryAttempts != -1 && retryCount >= retryAttempts)
            || retryCount > TX_RETRY_ATTEMPT) {
          // prevent stack overflow
          throw e;
        }
        setAffinityRetryCount(retryCount + 1);
      }
      affinityServerLocation.set(null);
      if (logger.isDebugEnabled()) {
        logger.debug("reset server affinity: attempting txFailover");
      }
      // send TXFailoverOp, so that new server can
      // do bootstrapping, then re-execute original op
      AbstractOp absOp = (AbstractOp) op;
      absOp.getMessage().setIsRetry();
      int transactionId = absOp.getMessage().getTransactionId();
      // for CommitOp we do not have transactionId in AbstractOp
      // so set it explicitly for TXFailoverOp
      TXFailoverOp.execute(pool, transactionId);

      if (op instanceof ExecuteRegionFunctionOpImpl) {
        op = new ExecuteRegionFunctionOpImpl((ExecuteRegionFunctionOpImpl) op, (byte) 1,
            new HashSet<>());
        ((ExecuteRegionFunctionOpImpl) op).getMessage().setTransactionId(transactionId);
      } else if (op instanceof ExecuteFunctionOpImpl) {
        op = new ExecuteFunctionOpImpl((ExecuteFunctionOpImpl) op, (byte) 1/* isReExecute */);
        ((ExecuteFunctionOpImpl) op).getMessage().setTransactionId(transactionId);
      }
      return pool.execute(op);
    } finally {
      if (initialRetryCount == 0) {
        setAffinityRetryCount(0);
      }
    }
  }

  @Override
  public void setupServerAffinity(boolean allowFailover) {
    if (logger.isDebugEnabled()) {
      logger.debug("setting up server affinity");
    }
    serverAffinityFailover = allowFailover;
    serverAffinity.set(Boolean.TRUE);
  }

  @Override
  public void releaseServerAffinity() {
    if (logger.isDebugEnabled()) {
      logger.debug("reset server affinity");
    }
    serverAffinity.set(Boolean.FALSE);
    affinityServerLocation.set(null);
  }

  @Override
  public ServerLocation getServerAffinityLocation() {
    return affinityServerLocation.get();
  }

  int getAffinityRetryCount() {
    return affinityRetryCount.get();
  }

  void setAffinityRetryCount(int retryCount) {
    affinityRetryCount.set(retryCount);
  }

  @Override
  public void setServerAffinityLocation(ServerLocation serverLocation) {
    assert affinityServerLocation.get() == null;
    affinityServerLocation.set(serverLocation);
  }

  public ServerLocation getNextOpServerLocation() {
    Connection conn = connectionManager.borrowConnection(serverTimeout);
    try {
      return conn.getServer();
    } finally {
      connectionManager.returnConnection(conn);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.client.internal.OpExecutor#executeOn(org.apache.geode.distributed.
   * internal.ServerLocation, org.apache.geode.cache.client.internal.Op)
   */
  @Override
  public Object executeOn(ServerLocation server, Op op) {
    return executeOn(server, op, true, false);
  }

  @Override
  public Object executeOn(ServerLocation p_server, Op op, boolean accessed,
      boolean onlyUseExistingCnx) {
    ServerLocation server = p_server;
    if (serverAffinity.get()) {
      ServerLocation affinityserver = affinityServerLocation.get();
      if (affinityserver != null) {
        server = affinityserver;
      } else {
        affinityServerLocation.set(server);
      }
      // redirect to executeWithServerAffinity so that we
      // can send a TXFailoverOp.
      return executeWithServerAffinity(server, op);
    }
    return executeOnServer(server, op, accessed, onlyUseExistingCnx);
  }

  protected Object executeOnServer(ServerLocation p_server, Op op, boolean accessed,
      boolean onlyUseExistingCnx) {
    boolean returnCnx = true;
    boolean pingOp = (op instanceof PingOp.PingOpImpl);
    Connection conn = null;
    if (pingOp) {
      // currently for pings we prefer to queue clientToServer cnx so that we will
      // not create a pooled cnx when all we have is queue cnxs.
      if (queueManager != null) {
        // see if our QueueManager has a connection to this server that we can send
        // the ping on.
        Endpoint ep = endpointManager.getEndpointMap().get(p_server);
        if (ep != null) {
          QueueConnections qcs = queueManager.getAllConnectionsNoWait();
          conn = qcs.getConnection(ep);
          if (conn != null) {
            // we found one to do the ping on
            returnCnx = false;
          }
        }
      }
    }
    if (conn == null) {
      conn = connectionManager.borrowConnection(p_server, singleServerTimeout, onlyUseExistingCnx);
    }
    try {
      return executeWithPossibleReAuthentication(conn, op);
    } catch (Exception e) {
      handleException(e, conn, 0, true);
      // this shouldn't actually be reached, handle exception will throw something
      throw new ServerConnectivityException("Received error connecting to server", e);
    } finally {
      if (serverAffinity.get() && affinityServerLocation.get() == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("setting server affinity to {} server:{}", conn.getEndpoint().getMemberId(),
              conn.getServer());
        }
        affinityServerLocation.set(conn.getServer());
      }
      if (returnCnx) {
        connectionManager.returnConnection(conn, accessed);
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.client.internal.ExecutablePool#executeOnPrimary(org.apache.geode.cache.
   * client.internal.Op)
   */
  @Override
  public Object executeOnPrimary(Op op) {
    if (queueManager == null) {
      throw new SubscriptionNotEnabledException();
    }

    HashSet<ServerLocation> attemptedPrimaries = new HashSet<>();
    while (true) {
      Connection primary = queueManager.getAllConnections().getPrimary();
      try {
        return executeWithPossibleReAuthentication(primary, op);
      } catch (Exception e) {
        boolean finalAttempt = !attemptedPrimaries.add(primary.getServer());
        handleException(e, primary, 0, finalAttempt);
        // we shouldn't reach this code, but just in case
        if (finalAttempt) {
          throw new ServerConnectivityException("Tried the same primary server twice.", e);
        }
      }
    }
  }

  @Override
  public void executeOnAllQueueServers(Op op) {
    if (queueManager == null) {
      throw new SubscriptionNotEnabledException();
    }

    RuntimeException lastException = null;

    QueueConnections connections = queueManager.getAllConnectionsNoWait();
    Connection primary = connections.getPrimary();
    if (primary != null) {
      try {
        executeWithPossibleReAuthentication(primary, op);
      } catch (Exception e) {
        try {
          handleException(e, primary, 0, false);
        } catch (RuntimeException e2) {
          lastException = e2;
        }
      }
    }

    List<Connection> backups = connections.getBackups();
    for (Connection conn : backups) {
      try {
        executeWithPossibleReAuthentication(conn, op);
      } catch (Exception e) {
        try {
          handleException(e, conn, 0, false);
        } catch (RuntimeException e2) {
          lastException = e2;
        }
      }
    }

    if (lastException != null) {
      throw lastException;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.client.internal.ExecutablePool#executeOnAllQueueServers(org.apache.geode
   * .cache.client.internal.Op)
   */
  @Override
  public Object executeOnQueuesAndReturnPrimaryResult(Op op) {
    if (queueManager == null) {
      throw new SubscriptionNotEnabledException();
    }
    QueueConnections connections = queueManager.getAllConnections();

    List backups = connections.getBackups();
    if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
      logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, "sending {} to backups: {}", op, backups);
    }
    for (int i = backups.size() - 1; i >= 0; i--) {
      Connection conn = (Connection) backups.get(i);
      try {
        executeWithPossibleReAuthentication(conn, op);
      } catch (Exception e) {
        handleException(e, conn, 0, false);
      }
    }

    Connection primary = connections.getPrimary();
    HashSet<ServerLocation> attemptedPrimaries = new HashSet<>();
    while (true) {
      try {
        if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
          logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, "sending {} to primary: {}", op, primary);
        }
        return executeWithPossibleReAuthentication(primary, op);
      } catch (Exception e) {
        if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
          logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, "caught exception sending to primary {}",
              e.getMessage(), e);
        }
        boolean finalAttempt = !attemptedPrimaries.add(primary.getServer());
        handleException(e, primary, 0, finalAttempt);
        primary = queueManager.getAllConnections().getPrimary();
        // we shouldn't reach this code, but just in case
        if (finalAttempt) {
          throw new ServerConnectivityException("Tried the same primary server twice.", e);
        }
      }
    }
  }

  /**
   * Used by GatewayBatchOp
   */
  @Override
  public Object executeOn(Connection conn, Op op, boolean timeoutFatal) {
    try {
      return executeWithPossibleReAuthentication(conn, op);
    } catch (Exception e) {
      handleException(op, e, conn, 0, true, timeoutFatal);
      // this shouldn't actually be reached, handle exception will throw something
      throw new ServerConnectivityException("Received error connecting to server", e);
    }
  }

  /**
   * This is used by unit tests
   */
  @Override
  public Object executeOn(Connection conn, Op op) {
    return executeOn(conn, op, false);
  }

  @Override
  public RegisterInterestTracker getRITracker() {
    return riTracker;
  }

  /**
   * This method will throw an exception if we need to stop the connection manager if there are
   * failures.
   */
  protected void handleException(Throwable e, Connection conn, int retryCount,
      boolean finalAttempt) {
    handleException(e, conn, retryCount, finalAttempt, false/* timeoutFatal */);
  }

  protected void handleException(Op op, Throwable e, Connection conn, int retryCount,
      boolean finalAttempt, boolean timeoutFatal) throws CacheRuntimeException {
    if (op instanceof AuthenticateUserOp.AuthenticateUserOpImpl) {
      if (e instanceof GemFireSecurityException) {
        throw (GemFireSecurityException) e;
      } else if (e instanceof ServerRefusedConnectionException) {
        throw (ServerRefusedConnectionException) e;
      }
    }
    handleException(e, conn, retryCount, finalAttempt, timeoutFatal);
  }

  protected void handleException(Throwable e, Connection conn, int retryCount, boolean finalAttempt,
      boolean timeoutFatal) throws CacheRuntimeException {
    GemFireException exToThrow = null;
    String title;
    boolean invalidateServer = true;
    boolean warn = true;
    boolean forceThrow = false;
    Throwable cause = e;

    cancelCriterion.checkCancelInProgress(e);

    if (logger.isDebugEnabled() && !(e instanceof java.io.EOFException)) {
      if (e instanceof java.net.SocketTimeoutException) {
        logger.debug("OpExecutor.handleException on Connection to {} read timed out",
            conn.getServer());
      } else {
        logger.debug("OpExecutor.handleException on Connection to {}", conn.getServer(), e);
      }
    }

    // first take care of all exceptions that should not invalidate the
    // connection and do not need to be logged

    if (e instanceof MessageTooLargeException) {
      title = null;
      exToThrow = new GemFireIOException("message is too large to transmit", e);
    } else if (e instanceof NotSerializableException) {
      title = null; // no message
      exToThrow = new SerializationException("Pool message failure", e);
    } else if (e instanceof BatchException || e instanceof BatchException70) {
      title = null; // no message
      exToThrow = new ServerOperationException(e);
    } else if (e instanceof RegionDestroyedException) {
      invalidateServer = false;
      title = null;
      exToThrow = (RegionDestroyedException) e;
    } else if (e instanceof GemFireSecurityException) {
      title = null;
      exToThrow = new ServerOperationException(e);
    } else if (e instanceof SerializationException) {
      title = null; // no message
      exToThrow = new ServerOperationException(e);
    } else if (e instanceof CopyException) {
      title = null; // no message
      exToThrow = new ServerOperationException(e);
    } else if (e instanceof ClassNotFoundException) {
      title = null; // no message
      exToThrow = new ServerOperationException(e);
    } else if (e instanceof TransactionException) {
      title = null; // no message
      exToThrow = (TransactionException) e;
      invalidateServer = false;
    } else if (e instanceof SynchronizationCommitConflictException) {
      title = null;
      exToThrow = (SynchronizationCommitConflictException) e;
      invalidateServer = false;
    } else if (e instanceof SocketException) {
      if ("Socket closed".equals(e.getMessage()) || "Connection reset".equals(e.getMessage())
          || "Connection refused: connect".equals(e.getMessage())
          || "Connection refused".equals(e.getMessage())) {
        title = e.getMessage();
      } else {
        title = "SocketException";
      }
    } else if (e instanceof SocketTimeoutException) {
      invalidateServer = timeoutFatal;
      title = "socket timed out on client";
      cause = null;
    } else if (e instanceof ConnectionDestroyedException) {
      invalidateServer = false;
      title = "connection was asynchronously destroyed";
      cause = null;
    } else if (e instanceof java.io.EOFException) {
      title = "closed socket on server";
    } else if (e instanceof IOException) {
      title = "IOException";
    } else if (e instanceof BufferUnderflowException) {
      title = "buffer underflow reading from server";
    } else if (e instanceof CancelException) {
      title = "Cancelled";
      warn = false;
    } else if (e instanceof InternalFunctionInvocationTargetException) {
      // In this case, function will be re executed
      title = null;
      exToThrow = (InternalFunctionInvocationTargetException) e;
    } else if (e instanceof FunctionInvocationTargetException) {
      // in this case function will not be re executed
      title = null;
      exToThrow = (GemFireException) e;
    } else if (e instanceof PutAllPartialResultException) {
      title = null;
      exToThrow = (PutAllPartialResultException) e;
      invalidateServer = false;
    } else {
      Throwable t = e.getCause();
      if ((t instanceof IOException)
          || (t instanceof SerializationException) || (t instanceof CopyException)
          || (t instanceof GemFireSecurityException) || (t instanceof ServerOperationException)
          || (t instanceof TransactionException) || (t instanceof CancelException)) {
        handleException(t, conn, retryCount, finalAttempt, timeoutFatal);
        return;
      } else if (e instanceof ServerOperationException) {
        title = null; // no message
        exToThrow = (ServerOperationException) e;
        invalidateServer = false; // fix for bug #42225
      } else if (e instanceof FunctionException) {
        if (t instanceof InternalFunctionInvocationTargetException) {
          // Client server to re-execute for node failure
          handleException(t, conn, retryCount, finalAttempt, timeoutFatal);
          return;
        } else {
          title = null; // no message
          exToThrow = (FunctionException) e;
        }
      } else if (e instanceof ServerConnectivityException
          && e.getMessage().equals("Connection error while authenticating user")) {
        title = null;
        if (logger.isDebugEnabled()) {
          logger.debug(e.getMessage(), e);
        }
      } else {
        title = e.toString();
        forceThrow = true;
      }
    }
    if (title != null) {
      conn.destroy();
      if (invalidateServer) {
        endpointManager.serverCrashed(conn.getEndpoint());
      }
      boolean logEnabled = warn ? logger.isWarnEnabled() : logger.isDebugEnabled();
      boolean msgNeeded = logEnabled || finalAttempt;
      if (msgNeeded) {
        final StringBuffer sb = getExceptionMessage(title, retryCount, finalAttempt, conn);
        final String msg = sb.toString();
        if (logEnabled) {
          if (warn) {
            logger.warn(msg);
          } else {
            logger.debug(msg, e);
          }
        }
        if (forceThrow || finalAttempt) {
          exToThrow = new ServerConnectivityException(msg, cause);
        }
      }
    }
    if (exToThrow != null) {
      throw exToThrow;
    }
  }

  private StringBuffer getExceptionMessage(String exceptionName, int retryCount,
      boolean finalAttempt, Connection connection) {
    StringBuffer message = new StringBuffer(200);
    message.append("Pool unexpected ").append(exceptionName);
    if (connection != null) {
      message.append(" connection=").append(connection);
    }
    if (retryCount > 0) {
      message.append(" attempt=").append(retryCount + 1);
    }
    message.append(')');
    if (finalAttempt) {
      message.append(". Server unreachable: could not connect after ").append(retryCount + 1)
          .append(" attempts");
    }
    return message;
  }

  private void authenticateIfRequired(Connection conn, Op op) {
    if (!conn.getServer().getRequiresCredentials()) {
      return;
    }

    if (pool == null) {
      PoolImpl poolImpl =
          (PoolImpl) PoolManagerImpl.getPMI().find(endpointManager.getPoolName());
      if (poolImpl == null) {
        return;
      }
      pool = poolImpl;
    }
    if (pool.getMultiuserAuthentication()) {
      if (((AbstractOp) op).needsUserId()) {
        UserAttributes ua = UserAttributes.userAttributes.get();
        if (ua != null) {
          if (!ua.getServerToId().containsKey(conn.getServer())) {
            authenticateMultiuser(pool, conn, ua);
          }
        }
      }
    } else if (((AbstractOp) op).needsUserId()) {
      // This should not be reached, but keeping this code here in case it is
      // reached.
      if (conn.getServer().getUserId() == -1) {
        Connection connImpl = conn.getWrappedConnection();
        conn.getServer().setUserId((Long) AuthenticateUserOp.executeOn(connImpl, pool));
        if (logger.isDebugEnabled()) {
          logger.debug(
              "OpExecutorImpl.execute() - single user mode - authenticated this user on {}", conn);
        }
      }
    }
  }

  private void authenticateMultiuser(PoolImpl pool, Connection conn, UserAttributes ua) {
    try {
      Long userId =
          (Long) AuthenticateUserOp.executeOn(conn.getServer(), pool, ua.getCredentials());
      if (userId != null) {
        ua.setServerToId(conn.getServer(), userId);
        if (logger.isDebugEnabled()) {
          logger.debug("OpExecutorImpl.execute() - multiuser mode - authenticated this user on {}",
              conn);
        }
      }
    } catch (ServerConnectivityException sce) {
      Throwable cause = sce.getCause();
      if (cause instanceof IOException || cause instanceof BufferUnderflowException
          || cause instanceof CancelException
          || (sce.getMessage() != null
              && (sce.getMessage().contains("Could not create a new connection to server")
                  || sce.getMessage().contains("socket timed out on client")
                  || sce.getMessage().contains("connection was asynchronously destroyed")))) {
        throw new ServerConnectivityException("Connection error while authenticating user");
      } else {
        throw sce;
      }
    }
  }

  private Object executeWithPossibleReAuthentication(Connection conn, Op op) throws Exception {
    try {
      return conn.execute(op);

    } catch (ServerConnectivityException sce) {
      Throwable cause = sce.getCause();
      if ((cause instanceof AuthenticationRequiredException
          && "User authorization attributes not found.".equals(cause.getMessage()))
          || sce.getMessage().contains("Connection error while authenticating user")) {
        // (ashetkar) Need a cleaner way of doing above check.
        // 2nd exception-message above is from AbstractOp.sendMessage()

        PoolImpl pool =
            (PoolImpl) PoolManagerImpl.getPMI().find(endpointManager.getPoolName());
        if (!pool.getMultiuserAuthentication()) {
          Connection connImpl = conn.getWrappedConnection();
          conn.getServer().setUserId((Long) AuthenticateUserOp.executeOn(connImpl, this));
          return conn.execute(op);
        } else {
          UserAttributes ua = UserAttributes.userAttributes.get();
          if (ua != null) {
            authenticateMultiuser(pool, conn, ua);
          }
          return conn.execute(op);
        }

      } else {
        throw sce;
      }
    }
  }

}
