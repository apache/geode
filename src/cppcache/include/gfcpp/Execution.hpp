#ifndef __GEMFIRE_EXECUTION_H__
#define __GEMFIRE_EXECUTION_H__

/*=========================================================================
  * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
  *
  * The specification of function behaviors is found in the corresponding .cpp
 fil
  e.
  *
  *========================================================================
  */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "VectorT.hpp"
#include "SharedPtr.hpp"
#include "CacheableBuiltins.hpp"
#include "ResultCollector.hpp"

/**
 * @file
 */

namespace gemfire {
/**
 * @class Execution Execution.hpp
 * gathers results from function execution
 * @see FunctionService
 */

class CPPCACHE_EXPORT Execution : public SharedBase {
 public:
  /**
   * Specifies a data filter of routing objects for selecting the GemFire
   * members
   * to execute the function.
   * <p>
   * If the filter set is empty the function is executed on all members
   * that have the  FunctionService::onRegion(Region).</p>
   * @param routingObj Set defining the data filter to be used for executing the
   * function
   * @return an Execution with the filter
   * @throws IllegalArgumentException if filter passed is NULLPTR.
   * @throws UnsupportedOperationException if not called after
   *    FunctionService::onRegion(Region).
   */
  virtual ExecutionPtr withFilter(CacheableVectorPtr routingObj) = 0;
  /**
   * Specifies the user data passed to the function when it is executed.
   * @param args user data passed to the function execution
   * @return an Execution with args
   * @throws IllegalArgumentException if the input parameter is NULLPTR
   *
   */
  virtual ExecutionPtr withArgs(CacheablePtr args) = 0;
  /**
   * Specifies the {@link ResultCollector} that will receive the results after
   * the function has been executed.
   * @return an Execution with a collector
   * @throws IllegalArgumentException if {@link ResultCollector} is NULLPTR
   * @see ResultCollector
   */
  virtual ExecutionPtr withCollector(ResultCollectorPtr rs) = 0;
  /**
   * Executes the function using its name
   * <p>
   * @param func the name of the function to be executed
   * @param timeout value to wait for the operation to finish before timing out.
   * @throws Exception if there is an error during function execution
   * @return either a default result collector or one specified by {@link
   * #withCollector(ResultCollector)}
   */
  virtual ResultCollectorPtr execute(
      const char* func, uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT) = 0;

  /**
   * Executes the function using its name
   * <p>
   * @param routingObj Set defining the data filter to be used for executing the
   * function
   * @param args user data passed to the function execution
   * @param rs * Specifies the {@link ResultCollector} that will receive the
   * results after
   * the function has been executed.
   * @param func the name of the function to be executed
   * @param timeout value to wait for the operation to finish before timing out.
   * @throws Exception if there is an error during function execution
   * @return either a default result collector or one specified by {@link
   * #withCollector(ResultCollector)}
   */
  virtual ResultCollectorPtr execute(CacheableVectorPtr& routingObj,
                                     CacheablePtr& args, ResultCollectorPtr& rs,
                                     const char* func, uint32_t timeout) = 0;
};
}  // namespace gemfire
#endif  //__GEMFIRE_EXECUTION_H__
