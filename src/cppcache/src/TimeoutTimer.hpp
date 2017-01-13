/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_TIMEOUT_HPP_
#define _GEMFIRE_TIMEOUT_HPP_

#include <gfcpp/gfcpp_globals.hpp>

#include <ace/Condition_Recursive_Thread_Mutex.h>
#include <ace/Condition_T.h>
#include <ace/Guard_T.h>
#include <ace/Time_Value.h>
#include <ace/OS_NS_sys_time.h>

namespace gemfire {

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
}

#endif
