/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
