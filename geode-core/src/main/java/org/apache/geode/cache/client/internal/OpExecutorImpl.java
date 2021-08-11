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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
import org.apache.geode.distributed.internal.ServerLocationAndMemberId;
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
  private final PoolImpl pool;
  private final ThreadLocal<Boolean> serverAffinity = ThreadLocal.withInitial(() -> Boolean.FALSE);
  private boolean serverAffinityFailover = false;
  private final ThreadLocal<ServerLocation> affinityServerLocation = new ThreadLocal<>();

  private final ThreadLocal<Integer> affinityRetryCount = ThreadLocal.withInitial(() -> 0);

  public OpExecutorImpl(final @NotNull ConnectionManager connectionManager,
      final @Nullable QueueManager queueManager,
      final @NotNull EndpointManager endpointManager,
      final @NotNull RegisterInterestTracker riTracker, final int retryAttempts,
      final long serverTimeout, final long singleServerTimeout,
      final @NotNull CancelCriterion cancelCriterion,
      final @NotNull PoolImpl pool) {
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
  public Object executeOn(final ServerLocation server, final Op op) {
    return executeOn(server, op, true, false);
  }

  @Override
  public Object executeOn(ServerLocation server, final Op op, final boolean accessed,
      final boolean onlyUseExistingCnx) {
    if (serverAffinity.get()) {
      final ServerLocation affinityServer = affinityServerLocation.get();
      if (affinityServer != null) {
        server = affinityServer;
      } else {
        affinityServerLocation.set(server);
      }
      // redirect to executeWithServerAffinity so that we
      // can send a TXFailoverOp.
      return executeWithServerAffinity(server, op);
    }
    return executeOnServer(server, op, accessed, onlyUseExistingCnx);
  }

  protected Object executeOnServer(ServerLocation server, Op op, boolean accessed,
      boolean onlyUseExistingConnection) {
    boolean returnConnection = true;
    Connection connection = null;
    if (op instanceof PingOp.PingOpImpl) {
      // currently for pings we prefer to queue clientToServer cnx so that we will
      // not create a pooled cnx when all we have is queue connections.
      if (queueManager != null) {
        // see if our QueueManager has a connection to this server that we can send
        // the ping on.
        final ServerLocationAndMemberId slAndMId = new ServerLocationAndMemberId(server,
            ((PingOp.PingOpImpl) op).getServerID().getUniqueId());
        final Endpoint endpoint = endpointManager.getEndpointMap().get(slAndMId);
        if (endpoint != null) {
          QueueConnections queueConnections = queueManager.getAllConnectionsNoWait();
          connection = queueConnections.getConnection(endpoint);
          if (connection != null) {
            // we found one to do the ping on
            returnConnection = false;
          }
        }
      }
    }
    if (connection == null) {
      connection = connectionManager.borrowConnection(server, singleServerTimeout,
          onlyUseExistingConnection);
    }
    try {
      return executeWithPossibleReAuthentication(connection, op);
    } catch (final Exception e) {
      handleException(e, connection, 0, true);
      // this shouldn't actually be reached, handle exception will throw something
      throw new ServerConnectivityException("Received error connecting to server", e);
    } finally {
      if (serverAffinity.get() && affinityServerLocation.get() == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("setting server affinity to {} server:{}",
              connection.getEndpoint().getMemberId(),
              connection.getServer());
        }
        affinityServerLocation.set(connection.getServer());
      }
      if (returnConnection) {
        connectionManager.returnConnection(connection, accessed);
      }
    }
  }

  @Override
  public Object executeOnPrimary(final Op op) {
    if (queueManager == null) {
      throw new SubscriptionNotEnabledException();
    }

    final HashSet<ServerLocation> attemptedPrimaries = new HashSet<>();
    while (true) {
      final Connection primary = queueManager.getAllConnections().getPrimary();
      try {
        return executeWithPossibleReAuthentication(primary, op);
      } catch (final Exception e) {
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

    final QueueConnections connections = queueManager.getAllConnectionsNoWait();
    final Connection primary = connections.getPrimary();
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

    final List<Connection> backups = connections.getBackups();
    for (final Connection connection : backups) {
      try {
        executeWithPossibleReAuthentication(connection, op);
      } catch (Exception e) {
        try {
          handleException(e, connection, 0, false);
        } catch (RuntimeException e2) {
          lastException = e2;
        }
      }
    }

    if (lastException != null) {
      throw lastException;
    }
  }

  @Override
  public Object executeOnQueuesAndReturnPrimaryResult(final Op op) {
    if (queueManager == null) {
      throw new SubscriptionNotEnabledException();
    }
    final QueueConnections connections = queueManager.getAllConnections();

    final List<Connection> backups = connections.getBackups();
    if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
      logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, "sending {} to backups: {}", op, backups);
    }
    for (int i = backups.size() - 1; i >= 0; i--) {
      final Connection connection = backups.get(i);
      try {
        executeWithPossibleReAuthentication(connection, op);
      } catch (Exception e) {
        handleException(e, connection, 0, false);
      }
    }

    Connection primary = connections.getPrimary();
    final HashSet<ServerLocation> attemptedPrimaries = new HashSet<>();
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
        final boolean finalAttempt = !attemptedPrimaries.add(primary.getServer());
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
  public Object executeOn(final Connection connection, final Op op, final boolean timeoutFatal) {
    try {
      return executeWithPossibleReAuthentication(connection, op);
    } catch (Exception e) {
      handleException(op, e, connection, 0, true, timeoutFatal);
      // this shouldn't actually be reached, handle exception will throw something
      throw new ServerConnectivityException("Received error connecting to server", e);
    }
  }

  /**
   * This is used by unit tests
   */
  @Override
  public Object executeOn(final Connection connection, final Op op) {
    return executeOn(connection, op, false);
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
    handleException(e, conn, retryCount, finalAttempt, false);
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

  protected void handleException(final Throwable throwable, final Connection connection,
      final int retryCount, final boolean finalAttempt,
      final boolean timeoutFatal) throws CacheRuntimeException {

    cancelCriterion.checkCancelInProgress(throwable);

    if (logger.isDebugEnabled() && !(throwable instanceof java.io.EOFException)) {
      if (throwable instanceof java.net.SocketTimeoutException) {
        logger.debug("OpExecutor.handleException on Connection to {} read timed out",
            connection.getServer());
      } else {
        logger.debug("OpExecutor.handleException on Connection to {}", connection.getServer(),
            throwable);
      }
    }

    // first take care of all exceptions that should not invalidate the
    // connection and do not need to be logged

    GemFireException exceptionToThrow = null;
    String title;
    boolean invalidateServer = true;
    boolean warn = true;
    boolean forceThrow = false;
    if (throwable instanceof MessageTooLargeException) {
      title = null;
      exceptionToThrow = new GemFireIOException("message is too large to transmit", throwable);
    } else if (throwable instanceof NotSerializableException) {
      title = null; // no message
      exceptionToThrow = new SerializationException("Pool message failure", throwable);
    } else if (throwable instanceof BatchException || throwable instanceof BatchException70) {
      title = null; // no message
      exceptionToThrow = new ServerOperationException(throwable);
    } else if (throwable instanceof RegionDestroyedException) {
      invalidateServer = false;
      title = null;
      exceptionToThrow = (RegionDestroyedException) throwable;
    } else if (throwable instanceof GemFireSecurityException) {
      title = null;
      exceptionToThrow = new ServerOperationException(throwable);
    } else if (throwable instanceof SerializationException) {
      title = null; // no message
      exceptionToThrow = new ServerOperationException(throwable);
    } else if (throwable instanceof CopyException) {
      title = null; // no message
      exceptionToThrow = new ServerOperationException(throwable);
    } else if (throwable instanceof ClassNotFoundException) {
      title = null; // no message
      exceptionToThrow = new ServerOperationException(throwable);
    } else if (throwable instanceof TransactionException) {
      title = null; // no message
      exceptionToThrow = (TransactionException) throwable;
      invalidateServer = false;
    } else if (throwable instanceof SynchronizationCommitConflictException) {
      title = null;
      exceptionToThrow = (SynchronizationCommitConflictException) throwable;
      invalidateServer = false;
    } else if (throwable instanceof SocketException) {
      if ("Socket closed".equals(throwable.getMessage())
          || "Connection reset".equals(throwable.getMessage())
          || "Connection refused: connect".equals(throwable.getMessage())
          || "Connection refused".equals(throwable.getMessage())) {
        title = throwable.getMessage();
      } else {
        title = "SocketException";
      }
    } else if (throwable instanceof SocketTimeoutException) {
      invalidateServer = timeoutFatal;
      title = "socket timed out on client";
    } else if (throwable instanceof ConnectionDestroyedException) {
      invalidateServer = false;
      title = "connection was asynchronously destroyed";
    } else if (throwable instanceof java.io.EOFException) {
      title = "closed socket on server";
    } else if (throwable instanceof IOException) {
      title = "IOException";
    } else if (throwable instanceof BufferUnderflowException) {
      title = "buffer underflow reading from server";
    } else if (throwable instanceof CancelException) {
      title = "Cancelled";
      warn = false;
    } else if (throwable instanceof InternalFunctionInvocationTargetException) {
      // In this case, function will be re executed
      title = null;
      exceptionToThrow = (InternalFunctionInvocationTargetException) throwable;
    } else if (throwable instanceof FunctionInvocationTargetException) {
      // in this case function will not be re executed
      title = null;
      exceptionToThrow = (GemFireException) throwable;
    } else if (throwable instanceof PutAllPartialResultException) {
      title = null;
      exceptionToThrow = (PutAllPartialResultException) throwable;
      invalidateServer = false;
    } else {
      Throwable t = throwable.getCause();
      if ((t instanceof IOException)
          || (t instanceof SerializationException) || (t instanceof CopyException)
          || (t instanceof GemFireSecurityException) || (t instanceof ServerOperationException)
          || (t instanceof TransactionException) || (t instanceof CancelException)) {
        handleException(t, connection, retryCount, finalAttempt, timeoutFatal);
        return;
      } else if (throwable instanceof ServerOperationException) {
        title = null; // no message
        exceptionToThrow = (ServerOperationException) throwable;
        invalidateServer = false; // fix for bug #42225
      } else if (throwable instanceof FunctionException) {
        if (t instanceof InternalFunctionInvocationTargetException) {
          // Client server to re-execute for node failure
          handleException(t, connection, retryCount, finalAttempt, timeoutFatal);
          return;
        } else {
          title = null; // no message
          exceptionToThrow = (FunctionException) throwable;
        }
      } else if (throwable instanceof ServerConnectivityException
          && throwable.getMessage().equals("Connection error while authenticating user")) {
        title = null;
        if (logger.isDebugEnabled()) {
          logger.debug(throwable.getMessage(), throwable);
        }
      } else {
        title = throwable.toString();
        forceThrow = true;
      }
    }
    if (title != null) {
      connection.destroy();
      if (invalidateServer) {
        endpointManager.serverCrashed(connection.getEndpoint());
      }
      boolean logEnabled = warn ? logger.isWarnEnabled() : logger.isDebugEnabled();
      boolean msgNeeded = logEnabled || finalAttempt;
      if (msgNeeded) {
        final StringBuilder sb = getExceptionMessage(title, retryCount, finalAttempt, connection);
        final String msg = sb.toString();
        if (logEnabled) {
          if (warn) {
            logger.warn(msg);
          } else {
            logger.debug(msg, throwable);
          }
        }
        if (forceThrow || finalAttempt) {
          exceptionToThrow = new ServerConnectivityException(msg, throwable);
        }
      }
    }
    if (exceptionToThrow != null) {
      throw exceptionToThrow;
    }
  }

  private StringBuilder getExceptionMessage(final String exceptionName, final int retryCount,
      final boolean finalAttempt, final Connection connection) {
    final StringBuilder message = new StringBuilder(200);
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

  private void authenticateIfRequired(final Connection connection, final Op op) {
    if (!connection.getServer().getRequiresCredentials()) {
      return;
    }

    if (pool.getMultiuserAuthentication()) {
      if (((AbstractOp) op).needsUserId()) {
        final UserAttributes ua = UserAttributes.userAttributes.get();
        if (ua != null) {
          if (!ua.getServerToId().containsKey(connection.getServer())) {
            authenticateMultiuser(pool, connection, ua);
          }
        }
      }
    } else if (((AbstractOp) op).needsUserId()) {
      // This should not be reached, but keeping this code here in case it is reached.
      if (connection.getServer().getUserId() == -1) {
        final Connection wrappedConnection = connection.getWrappedConnection();
        connection.getServer().setUserId(AuthenticateUserOp.executeOn(wrappedConnection, pool));
        if (logger.isDebugEnabled()) {
          logger.debug(
              "OpExecutorImpl.execute() - single user mode - authenticated this user on {}",
              connection);
        }
      }
    }
  }

  private void authenticateMultiuser(final PoolImpl pool, final Connection conn,
      final UserAttributes ua) {
    try {
      final Long userId = AuthenticateUserOp.executeOn(conn.getServer(), pool, ua.getCredentials());
      if (userId != null) {
        ua.setServerToId(conn.getServer(), userId);
        if (logger.isDebugEnabled()) {
          logger.debug("OpExecutorImpl.execute() - multiuser mode - authenticated this user on {}",
              conn);
        }
      }
    } catch (ServerConnectivityException sce) {
      final Throwable cause = sce.getCause();
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

  private Object executeWithPossibleReAuthentication(final Connection conn, final Op op)
      throws Exception {
    try {
      return conn.execute(op);
    } catch (final ServerConnectivityException sce) {
      final Throwable cause = sce.getCause();
      if ((cause instanceof AuthenticationRequiredException
          && "User authorization attributes not found.".equals(cause.getMessage()))
          || sce.getMessage().contains("Connection error while authenticating user")) {
        // 2nd exception-message above is from AbstractOp.sendMessage()

        if (pool.getMultiuserAuthentication()) {
          final UserAttributes ua = UserAttributes.userAttributes.get();
          if (ua != null) {
            authenticateMultiuser(pool, conn, ua);
          }
        } else {
          final Connection wrappedConnection = conn.getWrappedConnection();
          conn.getServer().setUserId(AuthenticateUserOp.executeOn(wrappedConnection, this));
        }

        return conn.execute(op);
      } else {
        throw sce;
      }
    }
  }

}
