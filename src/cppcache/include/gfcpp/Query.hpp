#ifndef __GEMFIRE_QUERY_H__
#define __GEMFIRE_QUERY_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

#include "SelectResults.hpp"

/**
 * @file
 */

namespace gemfire {

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

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_QUERY_H__
