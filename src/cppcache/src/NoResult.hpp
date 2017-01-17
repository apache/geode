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
