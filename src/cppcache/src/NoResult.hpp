/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_NO_RESULT_HPP__
#define __GEMFIRE_NO_RESULT_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/ResultCollector.hpp>

/**
 * @file
 */
namespace gemfire {

/**
 * A Special ResultCollector implementation. Functions having
 * {@link Function#hasResult()} false, this ResultCollector will be returned.
 * <br>
 * Calling getResult on this NoResult will throw
 * {@link FunctionException}
 *
 *
 */
class CPPCACHE_EXPORT NoResult : public ResultCollector {
 public:
  NoResult() {}
  ~NoResult() {}
  inline void addResult(CacheablePtr& resultOfSingleExecution) {
    throw UnsupportedOperationException("can not add to NoResult");
  }

  inline void endResults() {
    throw UnsupportedOperationException("can not close on NoResult");
  }

  inline CacheableVectorPtr getResult(
      uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT) {
    throw FunctionExecutionException(
        "Cannot return any result, as Function.hasResult() is false");
  }
  inline void clearResults() {
    throw UnsupportedOperationException("can not clear results on NoResult");
  }
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_NO_RESULT_HPP__
