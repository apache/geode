#ifndef __GEMFIRE_QUERYSERVICE_H__
#define __GEMFIRE_QUERYSERVICE_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "ExceptionTypes.hpp"
#include "CqQuery.hpp"
#include "CqAttributes.hpp"
#include "CqServiceStatistics.hpp"

#include "Query.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @class QueryService QueryService.hpp
 * QueryService is the class obtained from a Cache.
 * A Query is created from a QueryService and executed on the server
 * returning a SelectResults which can be either a ResultSet or a StructSet.
 */
class CPPCACHE_EXPORT QueryService : public SharedBase {
 public:
  /**
   * Get a new Query with the specified query string.
   *
   * @param querystr The query string with which to create a new Query.
   * @returns A smart pointer to the Query.
   */
  virtual QueryPtr newQuery(const char* querystr) = 0;

  /**
   * @nativeclient
   * Constructs a new named continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed, however, until the execute method
   * is invoked on the CqQuery. The name of the query will be used
   * to identify this query in statistics archival.
   *
   * @param cqName the String name for this query
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @param isDurable true if the CQ is durable
   * @return the newly created CqQuery object
   * @throws CqExistsException if a CQ by this name already exists on this
   * client
   * @throws IllegalArgumentException if queryString is null, or cqAttr is
   * NULLPTR
   * @throws IllegalStateException if this method is called from a cache
   *         server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating
   *         managing cq metadata info.
   * @throws CqInvalidException if the query doesnot meet the CQ constraints.
   *   E.g.: Query string should refer only one region, join not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a
   * region path.
   *         Bind parameters in the query are not supported for the initial
   * release.
   *
   * @endnativeclient
   */
  virtual CqQueryPtr newCq(const char* name, const char* querystr,
                           CqAttributesPtr& cqAttr, bool isDurable = false) = 0;
  /**
   * @nativeclient
   * Constructs a new named continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed, however, until the execute method
   * is invoked on the CqQuery. The name of the query will be used
   * to identify this query in statistics archival.
   *
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @param isDurable true if the CQ is durable
   * @return the newly created CqQuery object
   * @throws CqExistsException if a CQ by this name already exists on this
   * client
   * @throws IllegalArgumentException if queryString is null, or cqAttr is
   * NULLPTR
   * @throws IllegalStateException if this method is called from a cache
   *         server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating
   *         managing cq metadata info.
   * @throws CqInvalidException if the query doesnot meet the CQ constraints.
   *   E.g.: Query string should refer only one region, join not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a
   * region path.
   *         Bind parameters in the query are not supported for the initial
   * release.
   *
   * @endnativeclient
   */
  virtual CqQueryPtr newCq(const char* querystr, CqAttributesPtr& cqAttr,
                           bool isDurable = false) = 0;
  /**
   * @nativeclient
   * Close all CQs, and release resources
   * associated with executing CQs.
   * @endnativeclient
   */
  virtual void closeCqs() = 0;
  /**
   * @nativeclient
   * Retrieve  all registered CQs
   * @endnativeclient
   */
  virtual void getCqs(VectorOfCqQuery& vec) = 0;
  /**
   * @nativeclient
   * Retrieve a CqQuery by name.
   * @return the CqQuery or NULLPTR if not found
   * @endnativeclient
   */
  virtual CqQueryPtr getCq(const char* name) = 0;
  /**
   * @nativeclient
   * Executes all the cqs on this client.
   * @endnativeclient
   */
  virtual void executeCqs() = 0;
  /**
   * @nativeclient
   * Stops all the cqs on this client.
   * @endnativeclient
   */
  virtual void stopCqs() = 0;
  /**
   * @nativeclient
   * Get statistics information for all CQs
   * @return the CqServiceStatistics
   * @endnativeclient
   */
  virtual CqServiceStatisticsPtr getCqServiceStatistics() = 0;

  /**
   * Gets all the durable CQs registered by this client.
   *
   * @return List of names of registered durable CQs, empty list if no durable
   * cqs.
   */
  virtual CacheableArrayListPtr getAllDurableCqsFromServer() = 0;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_QUERYSERVICE_H__
