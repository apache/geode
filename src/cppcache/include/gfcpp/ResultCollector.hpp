#ifndef __GEMFIRE_RESULTCOLLECTOR_H__
#define __GEMFIRE_RESULTCOLLECTOR_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "SharedPtr.hpp"
#include "VectorT.hpp"
#include "CacheableBuiltins.hpp"

/**
 * @file
 */

namespace gemfire {
/**
 * @class ResultCollector ResultCollector.hpp
 * Defines the interface for a container that gathers results from function
 * execution.<br>
 * GemFire provides a default implementation for ResultCollector.
 * Applications can choose to implement their own custom ResultCollector.
 * A custom ResultCollector facilitates result sorting or aggregation.
 * Aggregation
 * functions like sum, minimum, maximum and average can also be applied to the
 * result using
 * a custom ResultCollector.
 *  Example:
 *  <br>
 *  <pre>
 *  ResultCollectorPtr rc = FunctionService::onRegion(region)
 *                                      ->withArgs(args)
 *                                      ->withFilter(keySet)
 *                                      ->withCollector(new
 * MyCustomResultCollector())
 *                                      .execute(Function);
 *  //Application can do something else here before retrieving the result
 *  CacheableVectorPtr functionResult = rc.getResult();
 * </pre>
 *
 * @see FunctionService
 */

class CPPCACHE_EXPORT ResultCollector : public SharedBase {
  /**
    * @brief public methods
    */
 public:
  ResultCollector();
  virtual ~ResultCollector();
  /**
   * Returns the result of function execution, potentially blocking until all
   * the results are available.
   * If gemfire sendException is called then {@link ResultCollector.getResult}
   * will not
   * throw exception but will have exception {@link
   * UserFunctionExecutionException} as a part of results received.
   * @param timeout in seconds, if result is not ready within this time,
   * exception will be thrown
   * @return the result
   * @throws FunctionException if result retrieval fails
   * @see UserFunctionExecutionException
   */
  virtual CacheableVectorPtr getResult(
      uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  /**
   * Adds a single function execution result to the ResultCollector
   *
   * @param resultOfSingleExecution
   * @since 5.8LA
   */
  virtual void addResult(CacheablePtr& resultOfSingleExecution);
  /**
   * GemFire will invoke this method when function execution has completed
   * and all results for the execution have been obtained and  added to the
   * ResultCollector}
   */
  virtual void endResults();
  /**
   * GemFire will invoke this method before re-executing function (in case of
   * Function Execution HA) This is to clear the previous execution results from
   * the result collector
   * @since 6.5
  */
  virtual void clearResults();

 private:
  CacheableVectorPtr m_resultList;
  volatile bool m_isResultReady;
};

}  // namespace gemfire
#endif  // ifndef __GEMFIRE_RESULTCOLLECTOR_H__
