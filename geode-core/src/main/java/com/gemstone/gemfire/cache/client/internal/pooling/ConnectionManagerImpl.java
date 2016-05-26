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
package com.gemstone.gemfire.cache.client.internal.pooling;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.GatewayConfigurationException;
import com.gemstone.gemfire.cache.client.AllConnectionsInUseException;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.ServerRefusedConnectionException;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.ConnectionFactory;
import com.gemstone.gemfire.cache.client.internal.Endpoint;
import com.gemstone.gemfire.cache.client.internal.EndpointManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.PoolImpl.PoolTask;
import com.gemstone.gemfire.cache.client.internal.QueueConnectionImpl;
import com.gemstone.gemfire.distributed.PoolCancelledException;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.PoolManagerImpl;
import com.gemstone.gemfire.internal.cache.PoolStats;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.security.GemFireSecurityException;
import com.gemstone.gemfire.i18n.StringId;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;

/**
 * Manages client to server connections for the connection pool. This class contains
 * all of the pooling logic to checkout/checkin connections.
 * 
 * @since GemFire 5.7
 *
 */
public class ConnectionManagerImpl implements ConnectionManager {
  private static final Logger logger = LogService.getLogger();
  
  static long AQUIRE_TIMEOUT = Long.getLong("gemfire.ConnectionManager.AQUIRE_TIMEOUT", 10 * 1000).longValue();
  private final String poolName;
  private final PoolStats poolStats;
  protected final long prefillRetry; // ms // make this an int
//  private final long pingInterval; // ms // make this an int
  private final LinkedList/*<PooledConnection>*/ availableConnections = new LinkedList/*<PooledConnection>*/();
  protected final ConnectionMap allConnectionsMap = new ConnectionMap();
  private final EndpointManager endpointManager;
  private final int maxConnections;
  protected final int minConnections;
  private final long idleTimeout; // make this an int
  protected final long idleTimeoutNanos;
  final int lifetimeTimeout;
  final long lifetimeTimeoutNanos;
  private final InternalLogWriter securityLogWriter;
  protected final CancelCriterion cancelCriterion;

  protected volatile int connectionCount;
  protected ScheduledExecutorService backgroundProcessor;
  protected ScheduledThreadPoolExecutor loadConditioningProcessor;
  
  protected ReentrantLock lock = new ReentrantLock();
  protected Condition freeConnection = lock.newCondition();
  private ConnectionFactory connectionFactory;
  protected boolean haveIdleExpireConnectionsTask;
  protected boolean havePrefillTask;
  private boolean keepAlive=false;
  protected volatile boolean shuttingDown;
  private EndpointManager.EndpointListenerAdapter endpointListener;

  private static final long NANOS_PER_MS = 1000000L;

  /**
   * Create a connection manager
   * @param poolName the name of the pool that owns us
   * @param factory the factory for new connections
   * @param maxConnections The maximum number of connections that can exist
   * @param minConnections The minimum number of connections that can exist
   * @param idleTimeout The amount of time to wait to expire idle connections. -1 means that
   * idle connections are never expired.
   * @param lifetimeTimeout the lifetimeTimeout in ms.
   * @param securityLogger 
   */
  public ConnectionManagerImpl(String poolName,
                               ConnectionFactory factory,
                               EndpointManager endpointManager,
                               int maxConnections, int minConnections,
                               long idleTimeout, int lifetimeTimeout,
                               InternalLogWriter securityLogger, long pingInterval,
                               CancelCriterion cancelCriterion, PoolStats poolStats) {
    this.poolName = poolName;
    this.poolStats = poolStats;
    if(maxConnections < minConnections && maxConnections != -1) {
      throw new IllegalArgumentException("Max connections " + maxConnections + " is less than minConnections " + minConnections);
    }
    if(maxConnections <= 0 && maxConnections != -1) {
      throw new IllegalArgumentException("Max connections " + maxConnections + " must be greater than 0");
    }
    if(minConnections < 0) {
      throw new IllegalArgumentException("Min connections " + minConnections + " must be greater than or equals to 0");
    }
    
    this.connectionFactory = factory;
    this.endpointManager = endpointManager;
    this.maxConnections = maxConnections == -1 ? Integer.MAX_VALUE : maxConnections;
    this.minConnections = minConnections;
    this.lifetimeTimeout = lifetimeTimeout;
    this.lifetimeTimeoutNanos = lifetimeTimeout * NANOS_PER_MS;
    if (lifetimeTimeout != -1) {
      if (idleTimeout > lifetimeTimeout || idleTimeout == -1) {
        // lifetimeTimeout takes precedence over longer idle timeouts
        idleTimeout = lifetimeTimeout;
      }
    }
    this.idleTimeout = idleTimeout;
    this.idleTimeoutNanos = this.idleTimeout * NANOS_PER_MS;
    this.securityLogWriter = securityLogger;
    this.prefillRetry = pingInterval;
//    this.pingInterval = pingInterval;
    this.cancelCriterion = cancelCriterion;
    this.endpointListener = new EndpointManager.EndpointListenerAdapter() {
      @Override
      public void endpointCrashed(Endpoint endpoint) {
        invalidateServer(endpoint);
      }
    };
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.internal.pooling.ConnectionManager#borrowConnection(long)
   */
  public Connection borrowConnection(long acquireTimeout) throws AllConnectionsInUseException, NoAvailableServersException {
    
    long startTime = System.currentTimeMillis();
    long remainingTime = acquireTimeout;
    
    //wait for a connection to become free
    lock.lock();
    try {
      while(connectionCount >= maxConnections && availableConnections.isEmpty() &&remainingTime > 0 && !shuttingDown) {
        final long start = getPoolStats().beginConnectionWait();
        boolean interrupted = false;
        try {
          freeConnection.await(remainingTime, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
          interrupted = true;
          cancelCriterion.checkCancelInProgress(e);
          throw new AllConnectionsInUseException();
        }
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
          getPoolStats().endConnectionWait(start);
        }
        remainingTime = acquireTimeout - (System.currentTimeMillis() - startTime);
      }
      if(shuttingDown) {
        throw new PoolCancelledException();
      }

      while (!availableConnections.isEmpty()) {
        PooledConnection connection = (PooledConnection) availableConnections.removeFirst();
        try {
          connection.activate();
          return connection;
        }
        catch (ConnectionDestroyedException ex) {
          // whoever destroyed it already decremented connectionCount
        }
      }
      if (connectionCount >= maxConnections) {
        throw new AllConnectionsInUseException();
      }
      else {
        //We need to create a connection. Reserve space for it.
        connectionCount++;
//         logger.info("DEBUG: borrowConnection conCount(+1)->" + connectionCount);
//         getPoolStats().incConCount(1);
      }
      
    }
    finally {
      lock.unlock();
    }
    
    PooledConnection connection = null;
    try {
      Connection plainConnection = connectionFactory.createClientToServerConnection(Collections.EMPTY_SET);
      
      connection = addConnection(plainConnection);
    }
    catch(GemFireSecurityException e) {
      throw new ServerOperationException(e);
    }
    catch(GatewayConfigurationException e) {
      throw new ServerOperationException(e);
    }
    catch(ServerRefusedConnectionException srce) {
      throw new NoAvailableServersException(srce);
    }
    finally {
      //if we failed, release the space we reserved for our connection
      if(connection == null) {
        lock.lock();
        try {
//           getPoolStats().incConCount(-1);
          --connectionCount;
//           logger.info("DEBUG: borrowConnection conCount(-1)->" + connectionCount);
          if(connectionCount < minConnections) {
            startBackgroundPrefill();
          }
        }
        finally {
          lock.unlock();
        }
      }
    }
    
    if(connection == null) {
      this.cancelCriterion.checkCancelInProgress(null);
      throw new NoAvailableServersException();
    }
    
    return connection;
  }
  
//   public Connection borrowConnection(ServerLocation server, long acquireTimeout)
//       throws AllConnectionsInUseException, NoAvailableServersException {
//     return borrowConnection(server, acquireTimeout, false);
//   }

//   /**
//    * Used to tell a caller of borrowConnection that it did not find an existing connnection.
//    */
//   public static final Connection NO_EXISTING_CONNECTION = new ConnectionImpl(null, null);
  
  /**
   * Borrow a connection to a specific server. This task currently
   * allows us to break the connection limit, because it is used by
   * tasks from the background thread that shouldn't be constrained
   * by the limit. They will only violate the limit by 1 connection, and 
   * that connection will be destroyed when returned to the pool.
   */
  public Connection borrowConnection(ServerLocation server, long acquireTimeout
                                     , boolean onlyUseExistingCnx)
    throws AllConnectionsInUseException, NoAvailableServersException {
    lock.lock();
    try {
      if(shuttingDown) {
        throw new PoolCancelledException();
      }
      for(Iterator itr = availableConnections.iterator(); itr.hasNext(); ) {
        PooledConnection nextConnection = (PooledConnection) itr.next();
        try {
          nextConnection.activate();
          if(nextConnection.getServer().equals(server)) {
            itr.remove();
            return nextConnection;
          }
          nextConnection.passivate(false);
        } catch (ConnectionDestroyedException ex) {
          // someone else already destroyed this connection so ignore it
          // but remove it from availableConnections
        }
        //Fix for 41516. Before we let this method exceed the max connections
        //by creating a new connection, we need to make sure that they're
        //aren't bogus connections sitting in the available connection list
        //otherwise, the length of that list might exceed max connections,
        //but with some bad connections. That can cause members to 
        //get a bad connection but have no permits to create a new connection.
        if(nextConnection.shouldDestroy()) {
          itr.remove();
        }
      }

      if (onlyUseExistingCnx) {
        throw new AllConnectionsInUseException();
      }

      // We need to create a connection. Reserve space for it.
      connectionCount++;
//       logger.info("DEBUG: borrowConnection conCount(+1)->" + connectionCount);
//       getPoolStats().incConCount(1);
    } finally {
      lock.unlock();
    }
    
    PooledConnection connection = null;
    try {
      Connection plainConnection = connectionFactory.createClientToServerConnection(server, false);
      connection = addConnection(plainConnection);
    } catch(GemFireSecurityException e) {
      throw new ServerOperationException(e);
    } finally {
      //if we failed, release the space we reserved for our connection
      if(connection == null) {
        lock.lock();
        try {
//           getPoolStats().incConCount(-1);
          --connectionCount;
//           logger.info("DEBUG: borrowConnection conCount(-1)->" + connectionCount);
          if(connectionCount < minConnections) {
            startBackgroundPrefill();
          }
        } finally {
          lock.unlock();
        }
      }
    }
    if(connection == null) {
      throw new ServerConnectivityException("Could not create a new connection to server " + server);
    }
    return connection;
  }
  
  public Connection exchangeConnection(Connection oldConnection,
      Set/* <ServerLocation> */excludedServers, long acquireTimeout)
      throws AllConnectionsInUseException {
    assert oldConnection instanceof PooledConnection;
    PooledConnection newConnection = null;
    PooledConnection oldPC = (PooledConnection) oldConnection;
//     while (!allConnectionsMap.containsConnection(oldPC)) {
//       // ok the connection has already been removed so we really can't do an
//       // exchange yet.
//       // As a quick hack lets just get a connection using borrow.
//       // If it turns out to be in our excludedServer set then
//       // we can loop and try to exchange it.
//       // But first make sure oldPC's socket gets closed.
//       oldPC.internalDestroy();
//       newConnection = (PooledConnection)borrowConnection(acquireTimeout);
//       if (excludedServers.contains(newConnection.getServer())) {
//         oldPC = newConnection; // loop and try to exchange it
//         newConnection = null;
//       } else {
//         // we found one so we can just return it
//         return newConnection;
//       }
//     }

    boolean needToUndoEstimate = false;
    lock.lock();
    try {
      if(shuttingDown) {
        throw new PoolCancelledException();
      }
      for(Iterator itr = availableConnections.iterator(); itr.hasNext(); ) {
        PooledConnection nextConnection = (PooledConnection) itr.next();
        if(!excludedServers.contains(nextConnection.getServer())) {
          itr.remove();
          try {
            nextConnection.activate();
            newConnection = nextConnection;
//             logger.info("DEBUG: exchangeConnection removeCon(" + oldPC +")");
            if (allConnectionsMap.removeConnection(oldPC)) {
//               getPoolStats().incConCount(-1);
              --connectionCount;
//               logger.info("DEBUG: exchangeConnection conCount(-1)->" + connectionCount + " oldPC=" + oldPC);
              if(connectionCount < minConnections) {
                startBackgroundPrefill();
              }
            }
            break;
          }
          catch (ConnectionDestroyedException ex) {
            // someone else already destroyed this connection so ignore it
            // but remove it from availableConnections
          }
        }
      }
      if (newConnection == null) {
        if (!allConnectionsMap.removeConnection(oldPC)) {
          // need to reserve space for the following create
//           if (connectionCount >= maxConnections) {
//             throw new AllConnectionsInUseException();
//           } else {
          // WARNING: we may be going over maxConnections here
          // @todo grid: this needs to be fixed
            //We need to create a connection. Reserve space for it.
            needToUndoEstimate = true;
            connectionCount++;
//             logger.info("DEBUG: exchangeConnection conCount(+1)->" + connectionCount);
//             getPoolStats().incConCount(1);
//           }
        }
      }
    }
    finally {
      lock.unlock();
    }

    if(newConnection == null) {
      try {
        Connection plainConnection = connectionFactory.createClientToServerConnection(excludedServers);
        newConnection = addConnection(plainConnection);
//         logger.info("DEBUG: exchangeConnection newConnection=" + newConnection);
      }
      catch(GemFireSecurityException e) {
        throw new ServerOperationException(e);
      }
      catch(ServerRefusedConnectionException srce) {
        throw new NoAvailableServersException(srce);
      }
      finally {
        if (needToUndoEstimate && newConnection == null) {
          lock.lock();
          try {
//             getPoolStats().incConCount(-1);
            --connectionCount;
//             logger.info("DEBUG: exchangeConnection conCount(-1)->" + connectionCount);
            if(connectionCount < minConnections) {
              startBackgroundPrefill();
            }
          }
          finally {
            lock.unlock();
          }
        }
      }
    }
    
    if(newConnection == null) {
      throw new NoAvailableServersException();
    }

//     logger.info("DEBUG: exchangeConnection internalDestroy(" + oldPC +")");
    oldPC.internalDestroy();
    
    return newConnection;
  }

  protected/*GemStoneAddition*/ String getPoolName() {
    return this.poolName;
  }
  
  private PooledConnection addConnection(Connection conn) {
      
    if(conn == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to create a connection in the allowed time");
      }
      return null;
    }
    PooledConnection pooledConn= new PooledConnection(this, conn);
    allConnectionsMap.addConnection(pooledConn);
    if(logger.isDebugEnabled()) {
      logger.debug("Created a new connection. {} Connection count is now {}", pooledConn, connectionCount);
    }
    return pooledConn;
  }
  
  private void destroyConnection(PooledConnection connection) {
    lock.lock();
    try {
      if (allConnectionsMap.removeConnection(connection)) {
        if(logger.isDebugEnabled()) {
          logger.debug("Invalidating connection {} connection count is now {}", connection, connectionCount);
        }
      
//         getPoolStats().incConCount(-1);
//         logger.info("DEBUG: destroyConnection conCount(-1)->" + connectionCount);
        if(connectionCount < minConnections) {
          startBackgroundPrefill();
        }
        freeConnection.signalAll();
      }
      --connectionCount; // fix for bug #50333
    }
    finally {
      lock.unlock();
    }
    
    connection.internalDestroy();
  }
  

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.internal.pooling.ConnectionManager#invalidateServer(com.gemstone.gemfire.distributed.internal.ServerLocation)
   */
  protected void invalidateServer(Endpoint endpoint) {
    Set badConnections = allConnectionsMap.removeEndpoint(endpoint);
    if(badConnections == null) {
      return;
    }
    
    lock.lock();
    try {
      if(shuttingDown) {
        return;
      }
      if(logger.isDebugEnabled()) {
        logger.debug("Invalidating {} connections to server {}", badConnections.size(), endpoint);
      }

      //mark connections for destruction now, so if anyone tries
      //to return a connection they'll get an exception
      for(Iterator itr = badConnections.iterator(); itr.hasNext(); ) {
        PooledConnection conn = (PooledConnection) itr.next();
        if (!conn.setShouldDestroy()) {
          // this might not be true; they make have just had an exception
//           itr.remove(); // someone else is destroying it
        }
      }
      
      for(Iterator itr = availableConnections.iterator(); itr.hasNext(); ) {
        PooledConnection conn = (PooledConnection) itr.next();
        if(badConnections.contains(conn)) {
          itr.remove();
        }
      }
      
//       getPoolStats().incConCount(-badConnections.size());
      connectionCount -= badConnections.size();
//       logger.info("DEBUG: invalidateServer conCount(" + (-badConnections.size()) + ")->" + connectionCount);
      
      if(connectionCount < minConnections) {
        startBackgroundPrefill();
      }

      // TODO (ashetkar) This for loop may well be outside the lock. But this
      // change was tested thoroughly for #42185 and also it may not impact perf
      // because this method gets called only when a server goes down.
      for(Iterator itr = badConnections.iterator(); itr.hasNext(); ) {
        PooledConnection conn = (PooledConnection) itr.next();
        conn.internalDestroy();
      }

      if(connectionCount < maxConnections) {
        freeConnection.signalAll();
      }
    }
    finally {
      lock.unlock();
    }
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.internal.pooling.ConnectionManager#returnConnection(com.gemstone.gemfire.cache.client.internal.Connection)
   */
  public void returnConnection(Connection connection) {
    returnConnection(connection, true);
  }
  
  public void returnConnection(Connection connection, boolean accessed) {

    assert connection instanceof PooledConnection;
    PooledConnection pooledConn = (PooledConnection)connection;

    boolean shouldClose = false;

    lock.lock();
    try {
      if (pooledConn.isDestroyed()) {
        return;
      }

      if (pooledConn.shouldDestroy()) {
        destroyConnection(pooledConn);
      } else {
        // thread local connections are already passive at this point
        if (pooledConn.isActive()) {
          pooledConn.passivate(accessed);
        }

        // borrowConnection(ServerLocation, long) allows us to break the
        // connection limit in order to get a connection to a server. So we need
        // to get our pool back to size if we're above the limit
        if (connectionCount > maxConnections) {
          if (allConnectionsMap.removeConnection(pooledConn)) {
            shouldClose = true;
            // getPoolStats().incConCount(-1);
            --connectionCount;
            // logger.info("DEBUG: returnConnection conCount(-1)->" + connectionCount);
          }
        } else {
          availableConnections.addFirst(pooledConn);
          freeConnection.signalAll();
        }
      }
    } finally {
      lock.unlock();
    }

    if (shouldClose) {
      try {
          PoolImpl localpool=(PoolImpl)PoolManagerImpl.getPMI().find(poolName);
          Boolean durable=false;
          if(localpool!=null){
            durable=localpool.isDurableClient();
            }
        pooledConn.internalClose(durable||this.keepAlive);
      } catch (Exception e) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.ConnectionManagerImpl_ERROR_CLOSING_CONNECTION_0,
            pooledConn), e);
      }
    }
  }  

  /* (non-Javadoc)
   */
  public void start(ScheduledExecutorService backgroundProcessor) {
    this.backgroundProcessor = backgroundProcessor;
    this.loadConditioningProcessor = new ScheduledThreadPoolExecutor(1/*why not 0?*/, new ThreadFactory() {
        public Thread newThread(final Runnable r) {
          Thread result = new Thread(r, "poolLoadConditioningMonitor-" + getPoolName());
          result.setDaemon(true);
          return result;
        }
      });
    this.loadConditioningProcessor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    
    endpointManager.addListener(endpointListener);
    
    lock.lock();
    try {
      startBackgroundPrefill();
    }
    finally {
      lock.unlock();
    }
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.internal.pooling.ConnectionManager#close(boolean, long)
   */
  public void close(boolean keepAlive) {
    if(logger.isDebugEnabled()) {
      logger.debug("Shutting down connection manager with keepAlive {}", keepAlive);
    }
    this.keepAlive=keepAlive;
    endpointManager.removeListener(endpointListener);
    
    lock.lock();
    try {
      if(shuttingDown) {
        return;
      }
      shuttingDown = true;
    }
    finally {
      lock.unlock();
    }

    // do this early as it might help lifetimeProcessor shutdown
//     closeReplacementConnection();
    try {
      if (this.loadConditioningProcessor != null) {
        this.loadConditioningProcessor.shutdown();
        if(!this.loadConditioningProcessor.awaitTermination(PoolImpl.SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.ConnectionManagerImpl_TIMEOUT_WAITING_FOR_LOAD_CONDITIONING_TASKS_TO_COMPLETE));
        }
      }
    }
    catch (RuntimeException e) {
      logger.error(LocalizedMessage.create(LocalizedStrings.ConnectionManagerImpl_ERROR_STOPPING_LOADCONDITIONINGPROCESSOR), e);
    }
    catch (InterruptedException e) {
      logger.error(LocalizedMessage.create(LocalizedStrings.ConnectionManagerImpl_INTERRUPTED_STOPPING_LOADCONDITIONINGPROCESSOR), e);
    }
    // one more time in case of race with lifetimeProcessor
//     closeReplacementConnection();
    allConnectionsMap.close(keepAlive);
  }
  
  public void emergencyClose() {
    shuttingDown = true;
    if (this.loadConditioningProcessor != null) {
      this.loadConditioningProcessor.shutdown();
    }
//     closeReplacementConnection();
    allConnectionsMap.emergencyClose();
  }

  protected void startBackgroundExpiration() {
    if (idleTimeout >= 0) {
      synchronized (this.allConnectionsMap) {
        if(!haveIdleExpireConnectionsTask) {
          haveIdleExpireConnectionsTask = true;
          try {
            backgroundProcessor.schedule(new IdleExpireConnectionsTask(), idleTimeout, TimeUnit.MILLISECONDS);
          }
          catch (RejectedExecutionException e) {
            // ignore, the timer has been cancelled, which means we're shutting
            // down.
          }
        }
      }
    }
  }

  /** Always called with lock held */
  protected void startBackgroundPrefill() {
    if(!havePrefillTask) {
      havePrefillTask = true;
      try {
        backgroundProcessor.execute(new PrefillConnectionsTask());
      }
      catch (RejectedExecutionException e) {
        // ignore, the timer has been cancelled, which means we're shutting
        // down.
      }
    }
  }
  
  protected boolean prefill() {
    try {
      while (connectionCount < minConnections) {
        if (cancelCriterion.cancelInProgress() != null) {
          return true;
        }
        boolean createdConnection= prefillConnection();
        if (!createdConnection) {
          return false;
        }
      }
    }
    catch(Throwable t) {
      cancelCriterion.checkCancelInProgress(t);
      if(t.getCause()!=null) {
        t = t.getCause();
      }
      logInfo(LocalizedStrings.ConnectionManagerImpl_ERROR_PREFILLING_CONNECTIONS, t);
      return false;
    }
    
    return true;
  }

  public int getConnectionCount() {
    return this.connectionCount;
  }

  protected PoolStats getPoolStats() {
    return this.poolStats;
  }
  
  public Connection getConnection(Connection conn) {
    if (conn instanceof PooledConnection) {
      return ((PooledConnection)conn).getConnection();
    } else if (conn instanceof QueueConnectionImpl) { 
      return ((QueueConnectionImpl)conn).getConnection();
    } else {
      return conn;
    }
  }

  private boolean prefillConnection() {
    boolean createConnection = false;
    lock.lock();
    try {
      if (shuttingDown) { 
        return false;
      }
      if (connectionCount < minConnections) {
//         getPoolStats().incConCount(1);
        connectionCount++;
//         logger.info("DEBUG: prefillConnection conCount(+1)->" + connectionCount);
        createConnection = true;
      }
    }
    finally {
      lock.unlock();
    }
    
    if (createConnection) {
      PooledConnection connection= null;
      try {
        Connection plainConnection = connectionFactory.createClientToServerConnection(Collections.EMPTY_SET);
        if(plainConnection == null) {
          return false;
        }
        connection = addConnection(plainConnection);
        connection.passivate(false);
        getPoolStats().incPrefillConnect();
      }
      catch (ServerConnectivityException ex) {
        logger.info(LocalizedStrings.ConnectionManagerImpl_UNABLE_TO_PREFILL_POOL_TO_MINIMUM_BECAUSE_0.toLocalizedString(ex.getMessage()));
        return false;
      }
      finally {
        lock.lock();
        try {
          if(connection == null) {
//             getPoolStats().incConCount(-1);
            connectionCount--;
//             logger.info("DEBUG: prefillConnection conCount(-1)->" + connectionCount);
            if(logger.isDebugEnabled()) {
              logger.debug("Unable to prefill pool to minimum, connection count is now {}", connectionCount);
            }
          }
          else {
            availableConnections.addFirst(connection);
            freeConnection.signalAll();
            if(logger.isDebugEnabled()) {
              logger.debug("Prefilled connection {} connection count is now {}", connection, connectionCount);
            }
          }
        }
        finally {
          lock.unlock();
        }
      }
    }
    
    return true;
  }
  
  public static void loadEmergencyClasses() {
    PooledConnection.loadEmergencyClasses();
  }
  
  protected class LifetimeExpireConnectionsTask implements Runnable {
    public void run() {
      try {
//         logger.info("DEBUG: lifetimeTask=" + this);
        allConnectionsMap.checkLifetimes();
      }
      catch (CancelException ignore) {
      }
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        // NOTREACHED
        throw e; // for safety
      }
      catch (Throwable t) {
        SystemFailure.checkFailure();
        logger.warn(LocalizedMessage.create(LocalizedStrings.ConnectionManagerImpl_LOADCONDITIONINGTASK_0_ENCOUNTERED_EXCEPTION, this), t);
        // Don't rethrow, it will just get eaten and kill the timer
      }
    }
  }
  
  protected class IdleExpireConnectionsTask implements Runnable {
    public void run() {
      try {
        getPoolStats().incIdleCheck();
        allConnectionsMap.checkIdleExpiration();
      }
      catch (CancelException ignore) {
      }
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        // NOTREACHED
        throw e; // for safety
      }
      catch (Throwable t) {
        SystemFailure.checkFailure();
        logger.warn(LocalizedMessage.create(LocalizedStrings.ConnectionManagerImpl_IDLEEXPIRECONNECTIONSTASK_0_ENCOUNTERED_EXCEPTION, this), t);
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
      lock.lock();
      try {
        if(connectionCount < minConnections && cancelCriterion.cancelInProgress() == null) {
          try {
            backgroundProcessor.schedule(new PrefillConnectionsTask(), prefillRetry, TimeUnit.MILLISECONDS);
          } catch(RejectedExecutionException e) {
            //ignore, the timer has been cancelled, which means we're shutting down.
          }
        }
        else {
          havePrefillTask = false;
        }
      }
      finally {
        lock.unlock();
      }
    }
  }

//   private final AR/*<ReplacementConnection>*/ replacement = CFactory.createAR();

//   private void closeReplacementConnection() {
//     ReplacementConnection rc = (ReplacementConnection)this.replacement.getAndSet(null);
//     if (rc != null) {
//       rc.getConnection().destroy();
//     }
//   }

  /**
   * Offer the replacement "con" to any cnx currently connected to "currentServer".
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
          }
          else {
//             // target was destroyed; we have already removed it from
//             // allConnectionsMap but didn't dec the stat
//             getPoolStats().incPoolConnections(-1);
//             logger.info("DEBUG: offerReplacementConnection incPoolConnections(-1)->" + getPoolStats().getPoolConnections());
            retry = true;
          }
        }
        catch (InterruptedException e) {
          // thrown by switchConnection
          interrupted = true;
          cancelCriterion.checkCancelInProgress(e);
          retry = false;
        }
        finally {
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
   * An existing connections lifetime has expired.
   * We only want to create one replacement connection at a time
   * so this guy should block until this connection replaces an existing one.
   * Note that if a connection is created here it must not count against
   * the pool max and its idle time and lifetime must not begin until
   * it actually replaces the existing one.
   * @param currentServer the server the candidate connection is connected to
   * @param idlePossible true if we have more cnxs than minPoolSize
   * @return true if caller should recheck for expired lifetimes;
   *         false if a background check was scheduled or no expirations are possible.
   */
  public boolean createLifetimeReplacementConnection(ServerLocation currentServer,
                                                      boolean idlePossible) {
    HashSet excludedServers = new HashSet();
    ServerLocation sl = this.connectionFactory.findBestServer(currentServer, excludedServers);

//    boolean replacementConsumed = false;
    while (sl != null) {
      if (sl.equals(currentServer)) {
        this.allConnectionsMap.extendLifeOfCnxToServer(currentServer);
        break;
      }
      else {
        if (!this.allConnectionsMap.hasExpiredCnxToServer(currentServer)) {
          break;
        }
        Connection con = null;
        try {
          //           logger.fine("DEBUG: creating replacement connection to " + sl);
          con = this.connectionFactory.createClientToServerConnection(sl, false);
          //           logger.fine("DEBUG: created replacement connection: " + con);
        }
        catch (GemFireSecurityException e) {
          securityLogWriter.warning(
              LocalizedStrings.ConnectionManagerImpl_SECURITY_EXCEPTION_CONNECTING_TO_SERVER_0_1,
              new Object[] {sl, e});
        }
        catch (ServerRefusedConnectionException srce) {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.ConnectionManagerImpl_SERVER_0_REFUSED_NEW_CONNECTION_1,
              new Object[] {sl, srce}));
        } 
        if (con == null) {
          excludedServers.add(sl);
          sl = this.connectionFactory.findBestServer(currentServer, excludedServers);
        }
        else {
          getPoolStats().incLoadConditioningConnect();
          if (!this.allConnectionsMap.hasExpiredCnxToServer(currentServer)) {
            getPoolStats().incLoadConditioningReplaceTimeouts();
            con.destroy();
            break;
          }
          offerReplacementConnection(con, currentServer);
          break;
        }
      }
    }
    if (sl == null) {
      // we didn't find a server to create a replacement cnx on so
      // extends the currentServers life
      this.allConnectionsMap.extendLifeOfCnxToServer(currentServer);
    }
    return this.allConnectionsMap.checkForReschedule(true);
  }
  
  protected class ConnectionMap {
    private final HashMap/*<Endpoint, HashSet<PooledConnection>*/ map = new HashMap();
    private final LinkedList/*<PooledConnection>*/ allConnections = new LinkedList/*<PooledConnection>*/(); // in the order they were created
    private boolean haveLifetimeExpireConnectionsTask;

    public synchronized boolean isIdleExpirePossible() {
      return this.allConnections.size() > minConnections;
    }

    @Override
    public synchronized String toString() {
      final long now = System.nanoTime();
      StringBuffer sb = new StringBuffer();
      sb.append("<");
      for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
        PooledConnection pc = (PooledConnection)it.next();
        sb.append(pc.getServer());
        if (pc.shouldDestroy()) {
          sb.append("-DESTROYED");
        }
        else if ( pc.hasIdleExpired(now, idleTimeoutNanos) ) {
          sb.append("-IDLE");
        }
        else if ( pc.remainingLife(now, lifetimeTimeoutNanos) <= 0 ) {
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
      addToEndpointMap(connection);

      // we want the smallest birthDate (e.g. oldest cnx) at the front of the list
      getPoolStats().incPoolConnections(1);
//       logger.info("DEBUG: addConnection incPoolConnections(1)->" + getPoolStats().getPoolConnections() + " con="+connection,
//                   new RuntimeException("STACK"));
      this.allConnections.addLast(connection);
      if (isIdleExpirePossible()) {
        startBackgroundExpiration();
      }
      if (lifetimeTimeout != -1 && !haveLifetimeExpireConnectionsTask) {
        if (checkForReschedule(true)) {
          // something has already expired so start processing with no delay
//           logger.info("DEBUG: rescheduling lifetime expire to be now");
          startBackgroundLifetimeExpiration(0);
        }
        else {
          // either no possible lifetime expires or we scheduled one
        }
      }
    }

    public synchronized void addReplacedCnx(PooledConnection con, Endpoint oldEndpoint) {
      if (this.allConnections.remove(con)) {
        // otherwise someone else has removed it and closed it
        removeFromEndpointMap(oldEndpoint, con);
        addToEndpointMap(con);
        this.allConnections.addLast(con);
        if (isIdleExpirePossible()) {
          startBackgroundExpiration();
        }
      }
    }
    
    public synchronized Set removeEndpoint(Endpoint endpoint) {
      final Set endpointConnections = (Set) this.map.remove(endpoint);
      if(endpointConnections != null) {
        int count = 0;
        for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
          if (endpointConnections.contains(it.next())) {
            count++;
            it.remove();
          }
        }
        if (count != 0) {
          getPoolStats().incPoolConnections(-count);
//           logger.info("DEBUG: removedEndpoint incPoolConnections(" + (-count) + ")->" + getPoolStats().getPoolConnections() + " cons.size=" + endpointConnections.size() + " cons=" + endpointConnections);
        }
      }
      return endpointConnections;
    }

    public synchronized boolean containsConnection(PooledConnection connection) {
      return this.allConnections.contains(connection);
    }
    
    public synchronized boolean removeConnection(PooledConnection connection) {
      // @todo darrel: allConnections.remove could be optimized by making
      // allConnections a linkedHashSet
      boolean result = this.allConnections.remove(connection);
      if (result) {
        getPoolStats().incPoolConnections(-1);
//         logger.info("DEBUG: removedConnection incPoolConnections(-1)->" + getPoolStats().getPoolConnections() + " con="+connection);
      }

      removeFromEndpointMap(connection);
      return result;
    }

    private synchronized void addToEndpointMap(PooledConnection connection) {
      Set endpointConnections = (Set) map.get(connection.getEndpoint());
      if(endpointConnections == null) {
        endpointConnections = new HashSet();
        map.put(connection.getEndpoint(), endpointConnections);
      }
      endpointConnections.add(connection);
    }
    
    private void removeFromEndpointMap(PooledConnection connection) {
      removeFromEndpointMap(connection.getEndpoint(), connection);
    }
    
    private synchronized void removeFromEndpointMap(Endpoint endpoint, PooledConnection connection) {
      Set endpointConnections = (Set) this.map.get(endpoint);
      if (endpointConnections != null) {
        endpointConnections.remove(connection);
        if(endpointConnections.size() == 0) {
          this.map.remove(endpoint);
        }
      }
    }

    public synchronized void close(boolean keepAlive) {
      map.clear();
      int count = 0;
      while (!this.allConnections.isEmpty()) {
        PooledConnection pc = (PooledConnection)this.allConnections.removeFirst();
        count++;
        if (!pc.isDestroyed()) {
          try {
            pc.internalClose(keepAlive);
          } catch(SocketException se) {
              logger.info(LocalizedMessage.create(
                      LocalizedStrings.ConnectionManagerImpl_ERROR_CLOSING_CONNECTION_TO_SERVER_0,
                      pc.getServer()), se);
          } catch(Exception e) {
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.ConnectionManagerImpl_ERROR_CLOSING_CONNECTION_TO_SERVER_0, 
                pc.getServer()), e);
          }
        }
      }
      if (count != 0) {
        getPoolStats().incPoolConnections(-count);
//         logger.info("DEBUG: close incPoolConnections(" + (-count) + ")->" + getPoolStats().getPoolConnections());
      }
    }

    public synchronized void emergencyClose() {
      map.clear();
      while (!this.allConnections.isEmpty()) {
        PooledConnection pc = (PooledConnection)this.allConnections.removeFirst();
        pc.emergencyClose();
      }
    }

    /**
     * Returns a pooled connection that can have its underlying cnx
     * to currentServer replaced by a new connection.
     * @return null if a target could not be found
     */
    public synchronized PooledConnection findReplacementTarget(ServerLocation currentServer) {
      final long now = System.nanoTime();
      for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
        PooledConnection pc = (PooledConnection)it.next();
        if (currentServer.equals(pc.getServer())) {
          if (!pc.shouldDestroy()
              && pc.remainingLife(now, lifetimeTimeoutNanos) <= 0) {
            removeFromEndpointMap(pc);
            return pc;
          }
        }
      }
      return null;
    }
    
    /**
     * Return true if we have a connection to the currentServer whose
     * lifetime has expired.
     * Otherwise return false;
     */
    public synchronized boolean hasExpiredCnxToServer(ServerLocation currentServer) {
      if (!this.allConnections.isEmpty()) {
        //boolean idlePossible = isIdleExpirePossible();
        final long now = System.nanoTime();
        for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
          PooledConnection pc = (PooledConnection)it.next();
          if (pc.shouldDestroy()) {
            // this con has already been destroyed so ignore it
            continue;
          }
          else if ( currentServer.equals(pc.getServer()) ) {
            /*if (idlePossible && pc.hasIdleExpired(now, idleTimeoutNanos)) {
              // this con has already idle expired so ignore it
              continue;
              } else*/ {
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
     * Returns true if caller should recheck for expired lifetimes
     * Returns false if a background check was scheduled or no expirations are possible.
     */
    public synchronized boolean checkForReschedule(boolean rescheduleOk) {
      if (!this.allConnections.isEmpty()) {
        final long now = System.nanoTime();
        for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
          PooledConnection pc = (PooledConnection)it.next();
          if (pc.hasIdleExpired(now, idleTimeoutNanos)) {
            // this con has already idle expired so ignore it
            continue;
          }
          else if ( pc.shouldDestroy() ) {
            // this con has already been destroyed so ignore it
            continue;
          }
          else {
            long life = pc.remainingLife(now, lifetimeTimeoutNanos);
            if (life > 0) {
              if (rescheduleOk) {
//                 logger.info("DEBUG: 2 rescheduling lifetime expire to be in: "
//                             + life + " nanos");
                startBackgroundLifetimeExpiration(life);
                return false;
              }
              else {
                return false;
              }
            }
            else {
              return true;
            }
          }
        }
      }
      return false;
    }

    /**
     * See if any of the expired connections (that have not idle expired)
     * are already connected to this sl and have not idle expired.
     * If so then just update them in-place to simulate a replace.
     * @param sl the location of the server we should see if we are connected to
     * @return true if we were able to extend an existing connection's lifetime
     *         or if we have no connection's whose lifetime has expired.
     *         false if we need to create a replacement connection.
     */
    public synchronized boolean tryToExtendLifeTime(ServerLocation sl) {
      // a better approach might be to get the most loaded server
      // (if they are not balanced) and then scan through and extend the lifetime
      // of everyone not connected to that server and do a replace on just one
      // of the guys who has lifetime expired to the most loaded server
      boolean result = true;
      if (!this.allConnections.isEmpty()) {
        final long now = System.nanoTime();
        for (Iterator it = this.allConnections.iterator(); it.hasNext();) {
          PooledConnection pc = (PooledConnection)it.next();
          if (pc.remainingLife(now, lifetimeTimeoutNanos) > 0) {
            // no more connections whose lifetime could have expired
            break;
            // note don't ignore idle guys because they are still connected
//           } else if (pc.remainingIdle(now, idleTimeoutNanos) <= 0) {
//             // this con has already idle expired so ignore it
          }
          else if ( pc.shouldDestroy() ) {
            // this con has already been destroyed so ignore it
          }
          else if ( sl.equals(pc.getEndpoint().getLocation()) ) {
            // we found a guy to whose lifetime we can extend
            it.remove();
//             logger.fine("DEBUG: tryToExtendLifeTime extending life of: " + pc);
            pc.setBirthDate(now);
            getPoolStats().incLoadConditioningExtensions();
            this.allConnections.addLast(pc);
            return true;
          }
          else {
            // the current pc is a candidate for reconnection to another server
            // so set result to false which will stick unless we find another con
            // whose life can be extended.
            result = false;
          }
        }
      }
//       if (result) {
//         logger.fine("DEBUG: tryToExtendLifeTime found no one to extend");
//       }
      return result;
    }

    /**
     * Extend the life of the first expired connection to sl.
     */
    public synchronized void extendLifeOfCnxToServer(ServerLocation sl) {
      if (!this.allConnections.isEmpty()) {
        final long now = System.nanoTime();
        for (Iterator it = this.allConnections.iterator(); it.hasNext() ;) {
          PooledConnection pc = (PooledConnection)it.next();
          if (pc.remainingLife(now, lifetimeTimeoutNanos) > 0) {
            // no more connections whose lifetime could have expired
            break;
            // note don't ignore idle guys because they are still connected
//           } else if (pc.remainingIdle(now, idleTimeoutNanos) <= 0) {
//             // this con has already idle expired so ignore it
          }
          else if ( pc.shouldDestroy() ) {
            // this con has already been destroyed so ignore it
          }
          else if ( sl.equals(pc.getEndpoint().getLocation()) ) {
            // we found a guy to whose lifetime we can extend
            it.remove();
//             logger.fine("DEBUG: tryToExtendLifeTime extending life of: " + pc);
            pc.setBirthDate(now);
            getPoolStats().incLoadConditioningExtensions();
            this.allConnections.addLast(pc);
            // break so we only do this to the oldest guy
            break;
          }
        }
      }
    }
    
    public synchronized void startBackgroundLifetimeExpiration(long delay) {
      if(!this.haveLifetimeExpireConnectionsTask) {
        this.haveLifetimeExpireConnectionsTask = true;
        try {
//           logger.info("DEBUG: scheduling lifetime expire check in: " + delay + " ns");
          LifetimeExpireConnectionsTask task = new LifetimeExpireConnectionsTask();
//           logger.info("DEBUG: scheduling lifetimeTask=" + task);
          loadConditioningProcessor.schedule(task, delay, TimeUnit.NANOSECONDS);
        }
        catch (RejectedExecutionException e) {
          //ignore, the timer has been cancelled, which means we're shutting down.
        }
      }
    }

    public void checkIdleExpiration() {
      int expireCount = 0;
      List<PooledConnection> toClose = null;
      synchronized (this) {
        haveIdleExpireConnectionsTask = false;
        if(shuttingDown) {
          return;
        }
        if (logger.isTraceEnabled()) {
          logger.trace("Looking for connections to expire");
        }

        // because we expire thread local connections we need to scan allConnections

        //find connections which have idle expired
        int conCount = this.allConnections.size();
        if (conCount <= minConnections) {
          return;
        }
        final long now = System.nanoTime();
        long minRemainingIdle = Long.MAX_VALUE;
        toClose = new ArrayList<PooledConnection>(conCount-minConnections);
        for (Iterator it = this.allConnections.iterator();
             it.hasNext() && conCount > minConnections;) {
          PooledConnection pc = (PooledConnection)it.next();
          if (pc.shouldDestroy()) {
            // ignore these connections
            conCount--;
          }
          else {
            long remainingIdle = pc.doIdleTimeout(now, idleTimeoutNanos);
            if (remainingIdle >= 0) {
              if (remainingIdle == 0) {
                // someone else already destroyed pc so ignore it
                conCount--;
              }
              else if ( remainingIdle < minRemainingIdle ) {
                minRemainingIdle = remainingIdle;
              }
            }
            else /* (remainingIdle < 0) */{
              // this means that we idleExpired the connection
              expireCount++;
              conCount--;
              removeFromEndpointMap(pc);
              toClose.add(pc);
              it.remove();
            }
          }
        }
        if (conCount > minConnections && minRemainingIdle < Long.MAX_VALUE) {
          try {
            backgroundProcessor.schedule(new IdleExpireConnectionsTask(),
                                         minRemainingIdle,
                                         TimeUnit.NANOSECONDS);
          }
          catch (RejectedExecutionException e) {
            //ignore, the timer has been cancelled, which means we're shutting down.
          }
          haveIdleExpireConnectionsTask = true;
        }
      }

      if (expireCount > 0) {
        getPoolStats().incIdleExpire(expireCount);
        getPoolStats().incPoolConnections(-expireCount);
//         logger.info("DEBUG: checkIdleExpiration incPoolConnections(" + (-expireCount) + ")->" + getPoolStats().getPoolConnections());
        // do this outside the above sync
        lock.lock();
        try {
//           getPoolStats().incConCount(-expireCount);
          connectionCount -= expireCount;
//           logger.info("DEBUG: checkIdleExpiration conCount(" + (-expireCount) + ")->" + connectionCount);
          freeConnection.signalAll();
          if(connectionCount < minConnections) {
            startBackgroundPrefill();
          }
        }
        finally {
          lock.unlock();
        }
      }
      //now destroy all of the connections, outside the sync
//      if (toClose != null) (cannot be null) 
      final boolean isDebugEnabled = logger.isDebugEnabled();
      {
        for (Iterator itr = toClose.iterator(); itr.hasNext(); ) {
          PooledConnection connection = (PooledConnection) itr.next();
          if (isDebugEnabled) {
            logger.debug("Idle connection detected. Expiring connection {}", connection);
          }
          try {
            connection.internalClose(false);
          }
          catch (Exception e) {
            logger.warn(LocalizedMessage.create(LocalizedStrings.ConnectionManagerImpl_ERROR_EXPIRING_CONNECTION_0, 
                connection));
          }
        }
      }
    }

    public void checkLifetimes() {
        //      logger.info("DEBUG: Looking for connections whose lifetime has expired");
      boolean done;
      synchronized (this) {
        this.haveLifetimeExpireConnectionsTask = false;
        if(shuttingDown) {
          return;
        }
      }
      do {
        getPoolStats().incLoadConditioningCheck();
        long firstLife = -1;
        done = true;
        ServerLocation candidate = null;
        boolean idlePossible = true;
        
        synchronized (this) {
          if(shuttingDown) {
            return;
          }
          // find a connection whose lifetime has expired
          // and who is not already being replaced
          long now = System.nanoTime();
          long life = 0;
          idlePossible = isIdleExpirePossible();
          for (Iterator it = this.allConnections.iterator();
               it.hasNext() && life <= 0 && (candidate == null);) {
            PooledConnection pc = (PooledConnection)it.next();
            // skip over idle expired and destroyed
            life = pc.remainingLife(now, lifetimeTimeoutNanos);
//             logger.fine("DEBUG: life remaining in " + pc + " is: " + life);
            if (life <= 0) {
               boolean idleTimedOut = idlePossible
                 ? pc.hasIdleExpired(now, idleTimeoutNanos)
                 : false;
              boolean destroyed = pc.shouldDestroy();
//               logger.fine("DEBUG: idleTimedOut=" + idleTimedOut
//                           + " destroyed=" + destroyed);
              if (!idleTimedOut && !destroyed) {
                candidate = pc.getServer();
              }
            }
            else if ( firstLife == -1 ) {
              firstLife = life;
            }
          }
        }
        if (candidate != null) {
//           logger.fine("DEBUG: calling createLifetimeReplacementConnection");
          done = !createLifetimeReplacementConnection(candidate, idlePossible);
//           logger.fine("DEBUG: createLifetimeReplacementConnection returned " + !done);
        }
        else {
//           logger.fine("DEBUG: reschedule " + firstLife);
          if (firstLife >= 0) {
            // reschedule
//             logger.info("DEBUG: rescheduling lifetime expire to be in: "
//                         + firstLife + " nanos");
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

  private void logInfo(StringId message, Throwable t) {
    if(t instanceof GemFireSecurityException) {
      securityLogWriter.info(LocalizedStrings.TWO_ARG_COLON, 
            new Object[] {message.toLocalizedString(), t}, t);
    } else {
      logger.info(LocalizedMessage.create(LocalizedStrings.TWO_ARG_COLON,
         new Object[] {message.toLocalizedString(), t}), t);
    }
  }
  
  private void logError(StringId message, Throwable t) {
    if(t instanceof GemFireSecurityException) {
      securityLogWriter.error(message, t);
    }
    else { 
      logger.error(message, t);
    }
  }
  
  public void activate(Connection conn) {
    assert conn instanceof PooledConnection;
    ((PooledConnection)conn).activate();
  }
  public void passivate(Connection conn, boolean accessed) {
    assert conn instanceof PooledConnection;
    ((PooledConnection)conn).passivate(accessed);
  }
}
