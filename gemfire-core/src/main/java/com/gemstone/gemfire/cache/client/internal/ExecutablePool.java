/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;


import com.gemstone.gemfire.cache.NoSubscriptionServersAvailableException;
import com.gemstone.gemfire.cache.client.SubscriptionNotEnabledException;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

/**
 * Provides methods to execute AbstractOp instances on a client pool.
 * @author darrel
 * @since 5.7
 */
public interface ExecutablePool {
  /**
   * Execute the given op on the servers that this pool connects to.
   * This method is responsible for retrying the op if an attempt fails.
   * It will only execute it once and on one server.
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   * @since 5.7
   */
  public Object execute(Op op);
  
  /**
   * Execute the given op on the servers that this pool connects to.
   * This method is responsible for retrying the op if an attempt fails.
   * It will only execute it once and on one server.
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   * @since 5.7
   */
  public Object execute(Op op, int retryAttempts);
  
  /**
   * Execute the given op on all the servers that have server-to-client queues
   * for this pool The last exception from any server will be thrown if the op fails.
   * The op is executed with the primary first, followed by the backups.
   * 
   * @param op
   *                the operation to execute.
   * @throws NoSubscriptionServersAvailableException if we have no queue server
   * @throws SubscriptionNotEnabledException If the pool does not have queues enabled
   */
  public void executeOnAllQueueServers(Op op) throws NoSubscriptionServersAvailableException, SubscriptionNotEnabledException;
  /**
   * Execute the given op on all the servers that have server-to-client queues
   * for this pool. The op will be executed on all backups, and then the primary.
   * This method will block until a primary is available.
   * 
   * @param op
   *                the operation to execute
   * @return The result from the primary server.
   * @throws NoSubscriptionServersAvailableException if we have no queue server
   * @throws SubscriptionNotEnabledException If the pool does not have queues enabled
   * @since 5.7
   */
  public Object executeOnQueuesAndReturnPrimaryResult(Op op)  throws NoSubscriptionServersAvailableException, SubscriptionNotEnabledException;
  /**
   * Execute the given op on the given server.
   * @param server the server to do the execution on
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  public Object executeOn(ServerLocation server, Op op);
  /**
   * Execute the given op on the given server.
   * @param server the server to do the execution on
   * @param op the operation to execute
   * @param accessed true if the connection is accessed by this execute
   * @return the result of execution if any; null if not
   */
  public Object executeOn(ServerLocation server, Op op, boolean accessed,boolean onlyUseExistingCnx);
  /**
   * Execute the given op on the given connection.
   * @param con the connection to do the execution on
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  public Object executeOn(Connection con, Op op);
  /**
   * Execute the given op on the given connection.
   * @param con the connection to do the execution on
   * @param op the operation to execute
   * @param timeoutFatal true if a timeout exception should be treated as a fatal one
   * @return the result of execution if any; null if not
   */
  public Object executeOn(Connection con, Op op, boolean timeoutFatal);
  /**
   * Execute the given op on the current primary server.
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  public Object executeOnPrimary(Op op);
  public RegisterInterestTracker getRITracker();
  
  /**
   * Release the connection held by the calling
   * thread if we're using thread local connections
   */
  void releaseThreadLocalConnection();
  
  /**
   * The calling thread will connect to only one server for
   * executing all ops until it calls {@link #releaseServerAffinity()}
   * @param allowFailover true if we want to failover to another
   * server when the first server is unreachable. Affinity to the
   * new server will be maintained
   * @since 6.6
   */
  public void setupServerAffinity(boolean allowFailover);
  
  /**
   * Release the server affinity established
   * by {@link #setupServerAffinity(boolean)}
   * @since 6.6
   */
  public void releaseServerAffinity();
  
  /**
   * When server affinity is enabled by this thread, returns the server against which all ops in this thread are performed 
   * @return location of the affinity server
   * @since 6.6
   * @see ExecutablePool#setupServerAffinity(boolean) 
   */
  public ServerLocation getServerAffinityLocation();
  
  /**
   * All subsequent operations by this thread will be performed on
   * the given ServerLocation. Used for resuming suspended transactions.
   * @param serverLocation
   * @since 6.6
   */
  public void setServerAffinityLocation(ServerLocation serverLocation);
}
