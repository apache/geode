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
package org.apache.geode.internal.datasource;

import java.io.Serializable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThread;

/**
 * AbstractPoolCache implements the ConnectionPoolCache interface. This is base class for the all
 * connection pools. The class also implements the Serializable interface. The pool maintain a list
 * for keeping the available connections(not assigned to user) and the active connections(assigned
 * to user) This is a thread safe class.
 */
public abstract class AbstractPoolCache implements ConnectionPoolCache, Serializable {

  private static final Logger logger = LogService.getLogger();

  protected int INIT_LIMIT;
  private int MAX_LIMIT;
  protected transient Map availableCache;
  protected transient Map activeCache;
  protected EventListener connEventListner;
  // private String error = "";
  protected ConfiguredDataSourceProperties configProps;
  // expirationTime is for the available connection which are expired in milliseconds
  protected int expirationTime;
  // timeOut is for the Active connection which are time out in milliseconds
  protected int timeOut;
  // Client Timeout in milliseconds
  protected int loginTimeOut;
  // private final boolean DEBUG = false;
  public transient ConnectionCleanUpThread cleaner;
  private int totalConnections = 0;
  private int activeConnections = 0;
  private List expiredConns = null;
  protected long sleepTime = -1;
  private Thread th = null;// cleaner thread

  /**
   * Constructor initializes the AbstractPoolCache properties.
   *
   * @param eventListner The event listner for the database connections.
   * @param configs The ConfiguredDataSourceProperties object containing the configuration for the
   *        pool.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "SC_START_IN_CTOR",
      justification = "the thread started is a cleanup thread and is not active until there is a timeout tx")
  public AbstractPoolCache(EventListener eventListner, ConfiguredDataSourceProperties configs)
      throws PoolException {
    availableCache = new HashMap();
    activeCache = Collections.synchronizedMap(new LinkedHashMap());
    connEventListner = eventListner;
    expiredConns = Collections.synchronizedList(new ArrayList());
    MAX_LIMIT = configs.getMaxPoolSize();
    expirationTime = configs.getConnectionExpirationTime() * 1000;
    timeOut = configs.getConnectionTimeOut() * 1000;
    loginTimeOut = configs.getLoginTimeOut() * 1000;
    INIT_LIMIT = Math.min(configs.getInitialPoolSize(), MAX_LIMIT);
    configProps = configs;
    cleaner = this.new ConnectionCleanUpThread();
    th = new LoggingThread("ConnectionCleanUpThread", cleaner);
    th.start();
  }

  protected void initializePool() {
    if (INIT_LIMIT > 0) {
      long currTime = System.currentTimeMillis();
      for (int count = 0; count < INIT_LIMIT; count++) {
        try {
          availableCache.put(getNewPoolConnection(), Long.valueOf(currTime));
          ++totalConnections;
        } catch (Exception ex) {
          if (logger.isDebugEnabled()) {
            logger.debug("AbstractPoolCache::initializePool:Error in creating connection",
                ex.getCause());
          }
        }
      }
    }
  }

  /**
   * Authenticates the username and password for the database connection.
   *
   * @param user The username for the database connection
   * @param pass The password for the database connection
   *
   *        public abstract void checkCredentials(String user, String pass)
   */
  /**
   * @return ???
   */
  public abstract Object getNewPoolConnection() throws PoolException;

  /**
   * Returns the connection to the available pool Asif: When the connection is returned to the pool,
   * notify any one thread waiting on availableCache so that it can go into connection seeking
   * state.
   *
   * @param connectionObject PooledConnection object for return.
   *
   */
  public void returnPooledConnectionToPool(Object connectionObject) {
    boolean returnedHappened = false;
    if (connectionObject != null) {
      // Asif: Take a lock on activeCache while removing the Connection
      // from the map. It is possible that while the connection is
      // being returned the cleaner thread migh have also removed
      // it from the activeMap. If that is the case we don't do
      // anything bcoz the cleaner thread will take care of decrementing
      // the total & available connection. We wil take a lock on the
      // individual connection rather than complete activeMap
      // bcoz there is no point in blocking other threads from
      // returning their own connections
      synchronized (connectionObject) {
        if (this.activeCache.containsKey(connectionObject)) {
          // Asif: Remove the connection from activeCache
          // Don't add it to availabel pool now. Add it when we have
          // taken a lock on availableCache & then we will modify the
          // count.
          this.activeCache.remove(connectionObject);
          returnedHappened = true;
        }
      }
    }
    if (returnedHappened) {
      // Asif: take the lock on availableCache & decrement the
      // count of active connections. Call notify on availableConnections
      // so that any waiting client thread will go into connection
      // seeking state
      synchronized (this.availableCache) {
        --this.activeConnections;
        this.availableCache.put(connectionObject, Long.valueOf(System.currentTimeMillis()));
        this.availableCache.notify();
      }
    }
  }

  /**
   * Expires connection in the available pool.
   *
   */
  abstract void destroyPooledConnection(Object connectionObject);

  /**
   * Returns number of connections currently in use.
   *
   * @return int Number of active connections
   */
  public int getActiveCacheSize() {
    return this.activeConnections;
  }

  /**
   * Returns number of connections available for use
   *
   * @return Size of available cache.
   */
  public int getAvailableCacheSize() {
    return (this.totalConnections - this.activeConnections);
  }

  /**
   * @see org.apache.geode.internal.datasource.ConnectionPoolCache#expirePooledConnection(Object)
   * @param connectionObject Asif: This function will set the timestamp associated with the
   *        Connection object such that it will get timed out by the cleaner thread. Normally when
   *        this function is called, the connection object will be present in activeCache as there
   *        is no way client can return the object. But it is possible that the cleaner thread may
   *        have already picked it up ,so we still have to check whether it is contained in the map
   *        or not
   *
   */
  public void expirePooledConnection(Object connectionObject) {
    synchronized (connectionObject) {
      if (this.activeCache.containsKey(connectionObject)) {
        // Change the time stamp associated with the object
        long prev = ((Long) this.activeCache.get(connectionObject)).longValue();
        prev = prev - this.timeOut - 1000;
        this.activeCache.put(connectionObject, Long.valueOf(prev));
      }
    }
  }

  /**
   * Gets the connection from the pool. The specified user and password are used. If the available
   * pool is not empty a connection is fetched from the available pool. If the available pool is
   * empty and the total connections in the pool(available + active) are less then the Max limit, a
   * new connection is created and returned to the client. If the Max limit is reached the client
   * waits for the connection to be available.
   *
   * Asif: The client thread checks if the total number of active connections have exhausted the
   * limit. If yes it waits on the availableCache. When another thread returns the connection to the
   * pool, the waiting thread gets notified. If a thread experiences timeout while waiting , a
   * SQLException will be thrown. Other case is that there are available connections in map. Now
   * while getting connection from avaialbel map , we may or may not obtain connetion bcoz the
   * connection in available map may have expired. But if the checkOutConnection() returns null ,
   * this iteslf guarantees that atleast one connection expired so the current thread can safely
   * demand one new connection.
   *
   * @return Object connection object from the pool.
   */
  public Object getPooledConnectionFromPool() throws PoolException {
    Object poolConn = null;
    // checkCredentials(username, password);
    /*
     * if (availableCache == null) { synchronized (this) { if (availableCache == null) {
     * initializePool(); } } }
     */
    long now = System.currentTimeMillis();
    synchronized (this.availableCache) {
      while ((totalConnections - activeConnections) == 0 && totalConnections == MAX_LIMIT) {
        try {
          this.availableCache.wait(loginTimeOut);
          long newtime = System.currentTimeMillis();
          long duration = newtime - now;
          if (duration > loginTimeOut)
            throw new PoolException(
                "AbstractPooledCache::getPooledConnectionFromPool:Login time-out exceeded");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // TODO add a cancellation check?
          if (logger.isDebugEnabled()) {
            logger.debug(
                "AbstractPooledCache::getPooledConnectionFromPool:InterruptedException in waiting thread");
          }
          throw new PoolException(
              "AbstractPooledCache::getPooledConnectionFromPool:InterruptedException in waiting thread");
        }
      }
      if ((totalConnections - activeConnections) > 0) {
        poolConn = checkOutConnection(now);
        if (poolConn != null) {
          activeCache.put(poolConn, Long.valueOf(now));
          ++activeConnections;
        }
      }
      if (poolConn == null) {
        poolConn = getNewPoolConnection();
        activeCache.put(poolConn, Long.valueOf(now));
        ++totalConnections;
        ++activeConnections;
      }
    }
    // Asif: Notify the cleaner thread if it is waiting
    // on the active Cache as there is an entry now
    synchronized (activeCache) {
      activeCache.notify();
    }
    return poolConn;
  }

  /**
   * Returns the max pool limit.
   *
   */
  public int getMaxLimit() {
    return MAX_LIMIT;
  }

  /**
   * Remove a connection from the pool. Modified by Asif : This function is called from a synch
   * block where the lock is taken on this.availableCache
   *
   * @param now Current time in milliseconds
   * @return Object Connection object from the pool.
   */
  private Object checkOutConnection(long now) throws PoolException {
    // boolean expiryCheck = false;
    Object retConn = null;
    Set entryset = availableCache.entrySet();
    Iterator itr = entryset.iterator();
    Map.Entry entry = null;
    while (itr.hasNext()) {
      entry = (Map.Entry) itr.next();
      long time = ((Long) entry.getValue()).longValue();
      if ((now - time) <= expirationTime) {
        retConn = entry.getKey();
        itr.remove();
        break;
      } else {
        // Asif : Take a lock on expiredConns, so that clean up thread
        // does not miss it while emptying.
        synchronized (this.expiredConns) {
          this.expiredConns.add(entry.getKey());
        }
        itr.remove();
        // Asif :Reduce the total number of available connections
        // We will not notify the cleaner thread. It will clean
        // the list next time when the thread awakes. The cleaner thread
        // will be either waiting on the activeCache or sleeping.
        // It will get notified whenever there is an elemnt added
        // is added in the activeCache. If there is atleast one
        // object in the activeMap it will sleep rather than wait
        --totalConnections;
      }
    }
    return retConn;
  }

  // Code for Clean up thread
  /**
   * Asif: The Cleaner thread calls this function periodically to clear the expired connection list
   * & also to add those connections to expiry list , which have timed out while being active. The
   * thread first iterates over the map of active connections. It does by getting array of keys
   * contained in the map so that there is no backing of Map. It collects all the actiev connections
   * timeout & if there is atleast one such connection , it will issue a notify or notify all ( if
   * more than one connections have timed out). After that it will just clear the list of expired
   * connections *
   */
  protected void cleanUp() {
    // Asif Get an array of keys in the activeConnection map
    // Since we are using LinkedHashMap the keys are guaranteed to be in order
    // of oldest conn as the first element
    // PooledConnection[] activeConnArr = (PooledConnection[])
    // this.activeCache.keySet().toArray(new PooledConnection[0]);
    int numConnTimedOut = 0;
    long now = System.currentTimeMillis();
    sleepTime = -1;
    boolean toContinue = true;
    // int len = activeConnArr.length;
    // for (int i = 0; i < len; ++i) {
    while (toContinue) {
      Object conn = null;
      Long associatedValue = null;
      // Get the connection which is the oldest
      synchronized (this.activeCache) {
        Set set = this.activeCache.entrySet();
        Iterator itr = set.iterator();
        if (itr.hasNext()) {
          Map.Entry entry = (Map.Entry) itr.next();
          conn = entry.getKey();
          associatedValue = (Long) entry.getValue();
        } else {
          toContinue = false;
          continue;
        }
      }
      synchronized (conn) {
        if (this.activeCache.containsKey(conn)) {
          // Asif: We need to again get the assocaited value & check
          // if it is same as before. As it is possibel that by the time
          // we reach here , the connection was returned to available map
          // and again put in active map , but this time it will be
          // at the extreme end !!! . So we cannot use it.
          // If that is the case we need to skip the entry
          // Check the timeout
          Long associatedValueInWindow = (Long) this.activeCache.get(conn);
          long then = associatedValueInWindow.longValue();
          if (associatedValueInWindow.longValue() == associatedValue.longValue()) {
            if ((now - then) > timeOut) {
              // Asif :remove the connection from activeMap so
              // that destroy can be called on it.
              // We will first collect all the connections
              // which have timed out & then expire them.
              // In that gap even if the client genuinely closes
              // the connection , it will not be returned to the
              // pool as the active map no longer contains it
              this.activeCache.remove(conn);
              this.expiredConns.add(conn);
              ++numConnTimedOut;
            } else {
              // AsifTODO: Just keep a final expitry time
              sleepTime = then + timeOut - now;
              toContinue = false;
            }
          }
        }
      }
    }
    if (numConnTimedOut > 0) {
      // Asif : In case only one connection timed out , take a lock
      // on availableCache & call notify . If more than one timed out
      // call notify all . Delete the total connections & number of active
      // connections by the right amount
      synchronized (this.availableCache) {
        this.activeConnections -= numConnTimedOut;
        this.totalConnections -= numConnTimedOut;
        if (numConnTimedOut == 1)
          this.availableCache.notify();
        else
          this.availableCache.notifyAll();
      }
    }
    // Asif : Create a temp list which copies the connections in
    // the expired list & releases the lock on expired list
    // immediately . Then it is safe to clear the expiredConnList
    List temp = new ArrayList();
    synchronized (this.expiredConns) {
      temp.addAll(this.expiredConns);
      this.expiredConns.clear();
    }
    // Asif: destroy the connections contained in the temp list
    // & clear it
    int size = temp.size();
    for (int i = 0; i < size; ++i) {
      Object conn = temp.get(i);
      this.destroyPooledConnection(conn);
    }
    temp.clear();
  }

  /**
   * It will clean all the thread
   */
  public void clearUp() {
    cleaner.toContinueRunning = false;
    try {
      th.interrupt();
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("AbstractPoolCache::clearUp: Exception in interrupting the thread", e);
      }
    }
    // closing all the connection
    try {
      Iterator availableCacheItr = availableCache.keySet().iterator();
      Iterator activeCacheItr = activeCache.keySet().iterator();
      while (activeCacheItr.hasNext()) {
        ((Connection) activeCacheItr.next()).close();
      }
      while (availableCacheItr.hasNext()) {
        ((Connection) availableCacheItr.next()).close();
      }
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "AbstractPoolCache::clearUp: Exception in closing connections. Ignoring this exception)");
      }
    }
    /*
     * try { th.join(); } catch (Exception e) { e.printStackTrace(); }
     */
  }

  /**
   * Inner class for Clean up thread. Asif: The inner class implements Runnable
   */
  class ConnectionCleanUpThread implements Runnable {

    // private AbstractPoolCache poolCache;
    // private int sleepTime;
    protected volatile boolean toContinueRunning = true;

    /*
     * public ConnectionCleanUpThread(int time) { // poolCache = pool; sleepTime = time;
     */
    public void run() {
      while (toContinueRunning) {
        SystemFailure.checkFailure();
        try {
          cleanUp();
          if (sleepTime != -1)
            Thread.sleep(sleepTime);
          // Asif : The cleaner thread will wait on activeCache if it is
          // empty . Else it will sleep for a while & again do the
          // clean up. If the activeMap is empty cleaner thread will
          // wait till some cleint obtains the connection. At this point
          // the thread will awaken .It will sleep for stipulated time
          // & then check on the connections
          synchronized (activeCache) {
            if (activeCache.isEmpty() && toContinueRunning) {
              activeCache.wait();
            }
          } // synchronized
        } catch (InterruptedException e) {
          // No need to reset the bit, we'll exit.
          if (toContinueRunning) {
            logger.debug("ConnectionCleanupThread: interrupted", e);
          }
          break;
        } catch (CancelException e) {
          if (toContinueRunning) {
            logger.debug("ConnectionCleanupThread: cancelled", e);
          }
          break;
        } catch (Exception e) {
          if (logger.isDebugEnabled() && toContinueRunning) {
            logger.debug(
                "ConnectionCleanUpThread::run: Thread encountered Exception. e={}. Ignoring the exception",
                e.getMessage(), e);
          }
        }
      } // while
    } // method

  } // CleanupThread
}
