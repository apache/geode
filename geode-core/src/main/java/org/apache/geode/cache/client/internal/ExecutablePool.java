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

import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.client.SubscriptionNotEnabledException;
import org.apache.geode.distributed.internal.ServerLocation;

/**
 * Provides methods to execute AbstractOp instances on a client pool.
 *
 * @since GemFire 5.7
 */
public interface ExecutablePool {
  /**
   * Execute the given op on the servers that this pool connects to. This method is responsible for
   * retrying the op if an attempt fails. It will only execute it once and on one server.
   *
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   * @since GemFire 5.7
   */
  Object execute(Op op);

  /**
   * Execute the given op on the servers that this pool connects to. This method is responsible for
   * retrying the op if an attempt fails. It will only execute it once and on one server.
   *
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   * @since GemFire 5.7
   */
  Object execute(Op op, int retryAttempts);

  /**
   * Execute the given op on all the servers that have server-to-client queues for this pool The
   * last exception from any server will be thrown if the op fails. The op is executed with the
   * primary first, followed by the backups.
   *
   * @param op the operation to execute.
   * @throws NoSubscriptionServersAvailableException if we have no queue server
   * @throws SubscriptionNotEnabledException If the pool does not have queues enabled
   */
  void executeOnAllQueueServers(Op op)
      throws NoSubscriptionServersAvailableException, SubscriptionNotEnabledException;

  /**
   * Execute the given op on all the servers that have server-to-client queues for this pool. The op
   * will be executed on all backups, and then the primary. This method will block until a primary
   * is available.
   *
   * @param op the operation to execute
   * @return The result from the primary server.
   * @throws NoSubscriptionServersAvailableException if we have no queue server
   * @throws SubscriptionNotEnabledException If the pool does not have queues enabled
   * @since GemFire 5.7
   */
  Object executeOnQueuesAndReturnPrimaryResult(Op op)
      throws NoSubscriptionServersAvailableException, SubscriptionNotEnabledException;

  /**
   * Execute the given op on the given server.
   *
   * @param server the server to do the execution on
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  Object executeOn(ServerLocation server, Op op);

  /**
   * Execute the given op on the given server.
   *
   * @param server the server to do the execution on
   * @param op the operation to execute
   * @param accessed true if the connection is accessed by this execute
   * @return the result of execution if any; null if not
   */
  Object executeOn(ServerLocation server, Op op, boolean accessed, boolean onlyUseExistingCnx);

  /**
   * Execute the given op on the given connection.
   *
   * @param con the connection to do the execution on
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  Object executeOn(ClientCacheConnection con, Op op);

  /**
   * Execute the given op on the given connection.
   *
   * @param con the connection to do the execution on
   * @param op the operation to execute
   * @param timeoutFatal true if a timeout exception should be treated as a fatal one
   * @return the result of execution if any; null if not
   */
  Object executeOn(ClientCacheConnection con, Op op, boolean timeoutFatal);

  /**
   * Execute the given op on the current primary server.
   *
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  Object executeOnPrimary(Op op);

  RegisterInterestTracker getRITracker();

  /**
   * The calling thread will connect to only one server for executing all ops until it calls
   * {@link #releaseServerAffinity()}
   *
   * @param allowFailover true if we want to failover to another server when the first server is
   *        unreachable. Affinity to the new server will be maintained
   * @since GemFire 6.6
   */
  void setupServerAffinity(boolean allowFailover);

  /**
   * Release the server affinity established by {@link #setupServerAffinity(boolean)}
   *
   * @since GemFire 6.6
   */
  void releaseServerAffinity();

  /**
   * When server affinity is enabled by this thread, returns the server against which all ops in
   * this thread are performed
   *
   * @return location of the affinity server
   * @since GemFire 6.6
   * @see ExecutablePool#setupServerAffinity(boolean)
   */
  ServerLocation getServerAffinityLocation();

  /**
   * All subsequent operations by this thread will be performed on the given ServerLocation. Used
   * for resuming suspended transactions.
   *
   * @since GemFire 6.6
   */
  void setServerAffinityLocation(ServerLocation serverLocation);
}
