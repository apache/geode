/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_IMPL_ATOMICINC_HPP_
#define _GEMFIRE_IMPL_ATOMICINC_HPP_

#include <gfcpp/gfcpp_globals.hpp>

#include "HostAsm.hpp"

namespace gemfire {

/**
 * @brief Atomic type wrapper for thread safe arithmetic limited to addition.
 */
class CPPCACHE_EXPORT AtomicInc {
 private:
  volatile int32_t m_value;

 public:
  /** @brief Initialize m_value to c. */
  AtomicInc(const int32_t c = 0) : m_value(c) {}
  /** @brief reset m_value to c. */
  void resetValue(const int32_t c = 0) { m_value = c; }

  /** @brief  Atomically pre-increment m_value. */
  int32_t operator++(void) {
    // return ++m_value;
    return HostAsm::atomicAdd(m_value, 1);
  }

  /** @brief  Atomically post-increment m_value. */
  int32_t operator++(int) {
    // return m_value++;
    return HostAsm::atomicAddPostfix(m_value, 1);
  }

  /** @brief  Atomically increment m_value by rhs. */
  int32_t operator+=(const int32_t &rhs) {
    // return m_value += rhs;
    return HostAsm::atomicAdd(m_value, rhs);
  }

  /** @brief  Atomically pre-decrement m_value. */
  int32_t operator--(void) {
    // return --m_value;
    return HostAsm::atomicAdd(m_value, -1);
  }

  /** @brief  Atomically post-decrement m_value. */
  int32_t operator--(int) {
    // return m_value--;
    return HostAsm::atomicAddPostfix(m_value, -1);
  }

  /** @brief  Atomically decrement m_value by rhs. */
  int32_t operator-=(const int32_t &rhs) {
    // return m_value -= rhs;
    return HostAsm::atomicAdd(m_value, -1 * rhs);
  }

  /** @brief  Explicitly return m_value. */
  int32_t value(void) const {
    // return m_value;
    return m_value;
  }
};
}

#endif
