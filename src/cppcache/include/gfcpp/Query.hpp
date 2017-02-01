#pragma once

#ifndef GEODE_GFCPP_QUERY_H_
#define GEODE_GFCPP_QUERY_H_

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

#include "SelectResults.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

/**
 * @class Query Query.hpp
 *
 * A Query is obtained from a QueryService which in turn is obtained from the
 * Cache.
 * This can be executed to return SelectResults which can be either a ResultSet
 * or a StructSet.
 *
 * This class is intentionally not thread-safe. So multiple threads should not
 * operate on the same <code>Query</code> object concurrently rather should
 * have their own <code>Query</code> objects.
 */
class CPPCACHE_EXPORT Query : public SharedBase {
 public:
  /**
   * Executes the OQL Query on the cache server and returns the results.
   *
   * @param timeout The time (in seconds) to wait for query response, optional.
   *        This should be less than or equal to 2^31/1000 i.e. 2147483.
   *
   * @throws IllegalArgumentException if timeout parameter is greater than
   * 2^31/1000.
   * @throws QueryException if some query error occurred at the server.
   * @throws IllegalStateException if some error occurred.
   * @throws NotConnectedException if no java cache server is available.  For
   * pools
   * configured with locators, if no locators are available, the cause of
   * NotConnectedException
   * is set to NoAvailableLocatorsException.
   * @returns A smart pointer to the SelectResults which can either be a
   * ResultSet or a StructSet.
   */
  virtual SelectResultsPtr execute(
      uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT) = 0;

  /**
   * Executes the parameterized OQL Query on the cache server and returns the
   * results.
   *
   * @param paramList The query parameters list
   * @param timeout The time (in seconds) to wait for query response, optional.
   *       This should be less than or equal to 2^31/1000 i.e. 2147483.
   *
   * @throws IllegalArgumentException if timeout parameter is greater than
   * 2^31/1000.
   * @throws QueryException if some query error occurred at the server.
   * @throws IllegalStateException if some error occurred.
   * @throws NotConnectedException if no java cache server is available.  For
   * pools
   * configured with locators, if no locators are available, the cause of
   * NotConnectedException
   * is set to NoAvailableLocatorsException.
   * returns A smart pointer to the SelectResults which can either be a
   * ResultSet or a StructSet.
   */

  virtual SelectResultsPtr execute(
      CacheableVectorPtr paramList,
      uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT) = 0;
  /**
   * Get the query string provided when a new Query was created from a
   * QueryService.
   *
   * @returns The query string.
   */
  virtual const char* getQueryString() const = 0;

  /**
   * Compile the Query - client side query compilation is not supported.
   *
   * @throws UnsupportedOperationException because this is not currently
   * supported.
   */
  virtual void compile() = 0;

  /**
   * Check whether the Query is compiled - client side query compilation
   * is not supported.
   *
   * @throws UnsupportedOperationException because this is not currently
   * supported.
   */
  virtual bool isCompiled() = 0;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_QUERY_H_
