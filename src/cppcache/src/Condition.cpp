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

#include "Condition.hpp"

namespace gemfire {

/**
 * Create a Condition protected by the mutex provided.
 */
Condition::Condition(ACE_Recursive_Thread_Mutex& mutex)
    : m_cond(mutex), m_signaled(false) {}

bool Condition::waitUntil(ACE_Time_Value* absoluteStopTime) {
  ACE_OS::last_error(0);

  ACE_Time_Value stopAt = *absoluteStopTime;
  while ((!m_signaled) && (m_cond.wait(&stopAt) != 0) &&
         (ACE_OS::last_error() != ETIME)) {
    ACE_OS::last_error(0);
    stopAt = *absoluteStopTime;
    ACE_Time_Value now = ACE_OS::gettimeofday();
    if (now >= stopAt) {
      break;
    }
  }
  bool result = m_signaled;
  m_signaled = false;
  return result;
}
}  // namespace gemfire
