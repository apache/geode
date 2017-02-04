#pragma once

#ifndef GEODE_GFCPP_CQSTATE_H_
#define GEODE_GFCPP_CQSTATE_H_

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

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

/**
 * @class CqState CqState.hpp
 *
 * This interface gives information on the state of a CqQuery.
 * It is provided by the getState method of the CqQuery instance.
 */
class CPPCACHE_EXPORT CqState {
 public:
  // corresponding to geode.cache.query.internal.CqStateImpl
  typedef enum {
    STOPPED = 0,
    RUNNING = 1,
    CLOSED = 2,
    CLOSING = 3,
    INVALID
  } StateType;
  /**
   * Returns the state in string form.
   */
  const char* toString() const;

  /**
   * Returns true if the CQ is in Running state.
   */
  bool isRunning() const;

  /**
   * Returns true if the CQ is in Stopped state.
   */
  bool isStopped() const;

  /**
   * Returns true if the CQ is in Closed state.
   */
  bool isClosed() const;

  /**
   * Returns true if the CQ is in Closing state.
   */
  bool isClosing() const;
  void setState(CqState::StateType state);
  CqState::StateType getState();

 private:
  StateType m_state;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CQSTATE_H_
