/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/ResultCollector.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <TimeoutTimer.hpp>

using namespace gemfire;
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
