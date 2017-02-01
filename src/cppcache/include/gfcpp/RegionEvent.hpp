#pragma once

#ifndef GEODE_GFCPP_REGIONEVENT_H_
#define GEODE_GFCPP_REGIONEVENT_H_

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
#include "Region.hpp"
#include "CacheableKey.hpp"
#include "UserData.hpp"

/** @file
*/

namespace apache {
namespace geode {
namespace client {

/** Declares region events.
*/

class CPPCACHE_EXPORT RegionEvent {
 protected:
  const RegionPtr m_region; /**< Region for this event. */
  const UserDataPtr
      m_callbackArgument;    /**< Callback argument for this event, if any. */
  const bool m_remoteOrigin; /**< True if from a remote process. */

 public:
  /** Constructor. */
  RegionEvent();
  /** Constructor, given the values. */
  RegionEvent(const RegionPtr& region, const UserDataPtr& aCallbackArgument,
              const bool remoteOrigin);

  /** Destructor. */
  ~RegionEvent();

  /** Return the region this event occurred in. */
  inline RegionPtr getRegion() const { return m_region; }

  /**
   * Returns the callbackArgument passed to the method that generated
   * this event. See the {@link Region} interface methods that take
   * a callbackArgument parameter.
   */
  inline UserDataPtr getCallbackArgument() const { return m_callbackArgument; }

  /** If the event originated in a remote process, returns true. */
  inline bool remoteOrigin() const { return m_remoteOrigin; }

 private:
  // never implemented.
  RegionEvent(const RegionEvent& other);
  void operator=(const RegionEvent& other);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_REGIONEVENT_H_
