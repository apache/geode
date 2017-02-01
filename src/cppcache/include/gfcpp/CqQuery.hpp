#pragma once

#ifndef GEODE_GFCPP_CQQUERY_H_
#define GEODE_GFCPP_CQQUERY_H_

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

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

#include "CqResults.hpp"
#include "CqStatistics.hpp"
#include "CqAttributes.hpp"
#include "CqAttributesMutator.hpp"
#include "CqState.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

/**
 * @class CqQuery CqQuery.hpp
 *
 * A Query is obtained from a QueryService which in turn is obtained from the
 * Cache.
 * This can be executed to return SelectResults which can be either
 * a ResultSet or a StructSet, or it can be just registered on the java server
 * without returning results immediately rather only the incremental results.
 *
 * This class is intentionally not thread-safe. So multiple threads should not
 * operate on the same <code>CqQuery</code> object concurrently rather should
 * have their own <code>CqQuery</code> objects.
 */
class CPPCACHE_EXPORT CqQuery : public SharedBase {
 public:
  /**
   * Get the query string provided when a new Query was created from a
   * QueryService.
   * @returns The query string.
   */
  virtual const char* getQueryString() const = 0;
  /**
   * Get teh query object generated for this CQs query.
   * @return Query object fort he query string
   */
  virtual QueryPtr getQuery() const = 0;
  /**
   * Get the name of the CQ.
   * @return the name of the CQ.
   */
  virtual const char* getName() const = 0;
  /**
   * Get the statistics information of this CQ.
   * @return CqStatistics, the CqStatistics object.
   */
  virtual const CqStatisticsPtr getStatistics() const = 0;
  /**
   * Get the Attributes of this CQ.
   * @return CqAttributes, the CqAttributes object.
   */
  virtual const CqAttributesPtr getCqAttributes() const = 0;
  /**
   * Get the AttributesMutator of this CQ.
   * @return CqAttributesMutator, the CqAttributesMutator object.
   */
  virtual const CqAttributesMutatorPtr getCqAttributesMutator() const = 0;
  /**
   * Start executing the CQ or if this CQ is stopped earlier, resumes execution
   * of the CQ.
   * Get the resultset associated with CQ query.
   * The CQ is executed on primary and redundant servers, if CQ execution fails
   * on all the
   * server then a CqException is thrown.
   *
   * @param timeout The time (in seconds) to wait for query response, optional.
   *        This should be less than or equal to 2^31/1000 i.e. 2147483.
   *
   * @throws IllegalArgumentException if timeout parameter is greater than
   * 2^31/1000.
   * @throws CqClosedException if this CqQuery is closed.
   * @throws RegionNotFoundException if the specified region in the
   *         query string is not found.
   * @throws IllegalStateException if the CqQuery is in the RUNNING state
   *         already.
   * @throws CqException if failed to execute and get initial results.
   * @return CqResults resultset obtained by executing the query.
   */
  virtual CqResultsPtr executeWithInitialResults(
      uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT) = 0;

  /**
   * @notsupported_cacheserver
   * @nativeclient
   * Executes the OQL Query on the cache server and returns the results.
   *
   * @throws RegionNotFoundException if the specified region in the
   *         query string is not found.
   * @throws CqClosedException if this CqQuery is closed.
   * @throws CqException if some query error occurred at the server.
   * @throws IllegalStateException if some error occurred.
   * @throws NotConnectedException if no java cache server is available. For
   * pools
   * configured with locators, if no locators are available, the cause of
   * NotConnectedException
   * is set to NoAvailableLocatorsException.
   * @endnativeclient
   */
  virtual void execute() = 0;
  /**
   *  Stops this CqQuery without releasing resources. Puts the CqQuery into
   *  the STOPPED state. Can be resumed by calling execute or
   *  executeWithInitialResults.
   *  @throws IllegalStateException if the CqQuery is in the STOPPED state
   *          already.
   *  @throws CqClosedException if the CQ is CLOSED.
   */
  virtual void stop() = 0;

  /**
   * Get the state of the CQ in CqState object form.
   * CqState supports methods like isClosed(), isRunning(), isStopped().
   * @see CqState
   * @return CqState state object of the CQ.
   */
  virtual CqState::StateType getState() = 0;

  /**
   * Close the CQ and stop execution.
   * Releases the resources associated with this CqQuery.
   * @throws CqClosedException Further calls on this CqQuery instance except
   *         for getState() or getName().
   * @throws CqException - if failure during cleanup of CQ resources.
   */
  virtual void close() = 0;

  /**
   * This allows to check if the CQ is in running or active.
   * @return boolean true if running, false otherwise
   */
  virtual bool isRunning() = 0;

  /**
   * This allows to check if the CQ is in stopped.
   * @return boolean true if stopped, false otherwise
   */
  virtual bool isStopped() = 0;

  /**
   * This allows to check if the CQ is closed.
   * @return boolean true if closed, false otherwise
   */
  virtual bool isClosed() = 0;

  /**
   * This allows to check if the CQ is durable.
   * @return boolean true if durable, false otherwise
   * @since 5.5
   */
  virtual bool isDurable() = 0;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CQQUERY_H_
