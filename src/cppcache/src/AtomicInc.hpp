#pragma once

#ifndef GEODE_ATOMICINC_H_
#define GEODE_ATOMICINC_H_

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

#include "HostAsm.hpp"

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_ATOMICINC_H_
