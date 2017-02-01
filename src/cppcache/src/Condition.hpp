#pragma once

#ifndef GEODE_CONDITION_H_
#define GEODE_CONDITION_H_

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

#include <gfcpp/gfcpp_globals.hpp>
#include <ace/Condition_Recursive_Thread_Mutex.h>
#include <ace/OS_NS_sys_time.h>

namespace apache {
namespace geode {
namespace client {

/**
 * Condition wrapper when you want an absolute signal or timeout condition.
 */
class CPPCACHE_EXPORT Condition {
 private:
  ACE_Condition<ACE_Recursive_Thread_Mutex> m_cond;
  bool m_signaled;

 public:
  /**
   * Create a Condition protected by the mutex provided.
   */
  Condition(ACE_Recursive_Thread_Mutex& mutex);

  ~Condition() {}

  /**
   * Release the given mutex, and wait for condition to be signaled, then
   * re-acquire the mutex.
   * Returns true only if the condition is signaled.
   * Returns false only if the time is passed.
   */
  bool waitUntil(ACE_Time_Value* absoluteStopTime);
  /** Convenience to derive absolute time.
  */
  inline bool waitFor(uint32_t seconds) {
    ACE_Time_Value stopAt = ACE_OS::gettimeofday();
    stopAt += seconds;
    return waitUntil(&stopAt);
  }

  /**
   * The mutex for this condition should be locked/held by the thread calling
   * this signal method.
   */
  inline void signal() {
    m_signaled = true;
    m_cond.signal();
  }

  inline ACE_Recursive_Thread_Mutex& mutex() { return m_cond.mutex(); }

  /** Must be called with the mutex held. */
  inline void reset() { m_signaled = false; }
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_CONDITION_H_
