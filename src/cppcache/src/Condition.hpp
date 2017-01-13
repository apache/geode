/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_IMPL_CONDITION_HPP_
#define _GEMFIRE_IMPL_CONDITION_HPP_ 1

#include <gfcpp/gfcpp_globals.hpp>
#include <ace/Condition_Recursive_Thread_Mutex.h>
#include <ace/OS_NS_sys_time.h>

namespace gemfire {

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
}

#endif
