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
#include <gfcpp/ResultCollector.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <TimeoutTimer.hpp>

using namespace apache::geode::client;
ResultCollector::ResultCollector()
    : m_resultList(CacheableVector::create()), m_isResultReady(false) {}
ResultCollector::~ResultCollector() {}
CacheableVectorPtr ResultCollector::getResult(uint32_t timeout) {
  if (m_isResultReady == true) {
    return m_resultList;
  } else {
    TimeoutTimer ttimer;
    for (uint32_t i = 0; i < timeout; i++) {
      ttimer.untilTimeout(1);
      if (m_isResultReady == true) return m_resultList;
    }
    throw FunctionExecutionException(
        "Result is not ready, endResults callback is called before invoking "
        "getResult() method");
  }
}
void ResultCollector::addResult(CacheablePtr& result) {
  m_resultList->push_back(result);
}
void ResultCollector::endResults() { m_isResultReady = true; }

void ResultCollector::clearResults() { m_resultList->clear(); }
