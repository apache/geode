#pragma once

#ifndef GEODE_TIMEOUTTIMER_H_
#define GEODE_TIMEOUTTIMER_H_

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
#include <ace/Condition_T.h>
#include <ace/Guard_T.h>
#include <ace/Time_Value.h>
#include <ace/OS_NS_sys_time.h>

namespace apache {
namespace geode {
namespace client {

class CPPCACHE_EXPORT TimeoutTimer {
 private:
  ACE_Recursive_Thread_Mutex m_mutex;
  ACE_Condition<ACE_Recursive_Thread_Mutex> m_cond;
  volatile bool m_reset;

 public:
  TimeoutTimer() : m_mutex(), m_cond(m_mutex), m_reset(false) {}

  /**
   * Return only after seconds have passed without receiving a reset.
   */
  void untilTimeout(int seconds) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_mutex);

    ACE_OS::last_error(0);
    while ((ACE_OS::last_error() != ETIME) || m_reset) {
      m_reset = false;
      ACE_Time_Value stopTime = ACE_OS::gettimeofday();
      stopTime += seconds;
      ACE_OS::last_error(0);
      m_cond.wait(&stopTime);
    }
  }

  /**
   * Reset the timeout interval so that it restarts now.
   */
  void reset() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_mutex);
    // m_cond.signal();
    m_reset = true;
  }
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_TIMEOUTTIMER_H_
