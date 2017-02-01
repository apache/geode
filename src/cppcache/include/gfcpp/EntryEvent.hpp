#pragma once

#ifndef GEODE_GFCPP_ENTRYEVENT_H_
#define GEODE_GFCPP_ENTRYEVENT_H_

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

/** Represents an entry event affecting an entry, including its identity and the
 * the circumstances of the event. */
class CPPCACHE_EXPORT EntryEvent : public apache::geode::client::SharedBase {
 protected:
  RegionPtr m_region;      /**< Region */
  CacheableKeyPtr m_key;   /**< Cacheable key */
  CacheablePtr m_oldValue; /**< Old value */
  CacheablePtr m_newValue; /**< New value */
  UserDataPtr
      m_callbackArgument; /**< Callback argument for this event, if any. */
  bool m_remoteOrigin;    /**< True if from a remote (non-local) process */

 public:
  /** Constructor, given all values. */
  EntryEvent(const RegionPtr& region, const CacheableKeyPtr& key,
             const CacheablePtr& oldValue, const CacheablePtr& newValue,
             const UserDataPtr& aCallbackArgument, const bool remoteOrigin);

  /** Destructor. */
  virtual ~EntryEvent();

  /** Constructor. */
  EntryEvent();

  /** @return the region this event occurred in. */
  inline RegionPtr getRegion() const { return m_region; }

  /** @return the key this event describes. */
  inline CacheableKeyPtr getKey() const { return m_key; }

  /** If the prior state of the entry was invalid, or non-existent/destroyed,
   * then the old value will be NULLPTR.
   * @return the old value in the cache.
   */
  inline CacheablePtr getOldValue() const { return m_oldValue; }

  /** If the event is a destroy or invalidate operation, then the new value
   * will be NULLPTR.
   * @return the updated value from this event
   */
  inline CacheablePtr getNewValue() const { return m_newValue; }

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
  EntryEvent(const EntryEvent& other);
  void operator=(const EntryEvent& other);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_ENTRYEVENT_H_
