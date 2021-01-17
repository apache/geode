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

package org.apache.geode.cache.query;


/**
 * Interface for continuous query. This provides methods for managing a CQ once it is created
 * through the QueryService. The methods allow you to retrieve CQ related information, operate on CQ
 * like execute, stop, close and get the state of the CQ.
 *
 * @since GemFire 5.5
 */

public interface CqQuery {

  /**
   * Get the original query string that was specified with CQ.
   *
   * @return String the query string associated with CQ.
   */
  String getQueryString();

  /**
   * Get the query object generated for this CQs query.
   *
   * @return Query query object for the query string.
   */
  Query getQuery();

  /**
   * Get the name of the CQ.
   *
   * @return the name of the CQ.
   */
  String getName();

  /**
   * Get statistics information for this CQ.
   *
   * @return CqStatistics CQ statistics object.
   */
  CqStatistics getStatistics();

  /**
   * Get CqAttributes for this CQ.
   *
   * @see CqAttributes
   * @return CqAttributes cqAttribute set with this CqQuery.
   */
  CqAttributes getCqAttributes();

  /**
   * Get CqAttributesMutator for this CQ.
   *
   * @see CqAttributesMutator
   * @return CqAttributesMutator.
   */
  CqAttributesMutator getCqAttributesMutator();

  /**
   * Starts executing the CQ or, if the CQ is in a stopped state, resumes execution. Gets the
   * current resultset for the CQ query. The CQ is executed on the primary server, and then
   * redundant servers as needed. If execution fails on all servers, a CqException is thrown. If the
   * query is complex and the data set is large, this execution may take a long time, which may
   * cause a socket read timeout in the client. To allow adequate time, you may need to set a longer
   * pool read-timeout in the client.
   *
   * @throws CqClosedException if this CqQuery is closed.
   * @throws RegionNotFoundException if the specified region in the query string is not found.
   * @throws IllegalStateException if the CqQuery is in the RUNNING state already.
   * @throws CqException if failed to execute and get initial results.
   * @return SelectResults resultset obtained by executing the query.
   */
  <E> CqResults<E> executeWithInitialResults()
      throws CqClosedException, RegionNotFoundException, CqException;

  /**
   * Start executing the CQ or if this CQ is stopped earlier, resumes execution of the CQ. The CQ is
   * executed on primary and redundant servers, if CQ execution fails on all the server then a
   * CqException is thrown.
   *
   * @throws CqClosedException if this CqQuery is closed.
   * @throws RegionNotFoundException if the specified region in the query string is not found.
   * @throws IllegalStateException if the CqQuery is in the RUNNING state already.
   * @throws CqException if failed to execute.
   */
  void execute() throws CqClosedException, RegionNotFoundException, CqException;

  /**
   * Stops this CqQuery without releasing resources. Puts the CqQuery into the STOPPED state. Can be
   * resumed by calling execute or executeWithInitialResults.
   *
   * @throws IllegalStateException if the CqQuery is in the STOPPED state already.
   * @throws CqClosedException if the CQ is CLOSED.
   */
  void stop() throws CqClosedException, CqException;

  /**
   * Get the state of the CQ in CqState object form. CqState supports methods like isClosed(),
   * isRunning(), isStopped().
   *
   * @see CqState
   * @return CqState state object of the CQ.
   */
  CqState getState();

  /**
   * Close the CQ and stop execution. Releases the resources associated with this CqQuery.
   *
   * @throws CqClosedException Further calls on this CqQuery instance except for getState() or
   *         getName().
   * @throws CqException - if failure during cleanup of CQ resources.
   */
  void close() throws CqClosedException, CqException;

  /**
   * This allows to check if the CQ is in running or active.
   *
   * @return boolean true if running, false otherwise
   */
  boolean isRunning();

  /**
   * This allows to check if the CQ is in stopped.
   *
   * @return boolean true if stopped, false otherwise
   */
  boolean isStopped();

  /**
   * This allows to check if the CQ is closed.
   *
   * @return boolean true if closed, false otherwise
   */
  boolean isClosed();

  /**
   * This allows to check if the CQ is durable.
   *
   * @return boolean true if durable, false otherwise
   * @since GemFire 5.5
   */
  boolean isDurable();

  /**
   * This allows to check if the CQ use option to suppress CQ create notification.
   *
   * @return boolean true if create is suppressed, false otherwise
   * @since Geode 14.0
   */
  boolean isCreateSuppressed();

  /**
   * This allows to check if the CQ use option to suppress CQ update notification.
   *
   * @return boolean true if update is suppressed, false otherwise
   * @since Geode 14.0
   */
  boolean isUpdateSuppressed();

  /**
   * This allows to check if the CQ use option to suppress CQ destroy notification.
   *
   * @return boolean true if destroy is suppressed, false otherwise
   * @since Geode 14.0
   */
  boolean isDestroySuppressed();

}
