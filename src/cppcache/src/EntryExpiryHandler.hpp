#pragma once

#ifndef GEODE_ENTRYEXPIRYHANDLER_H_
#define GEODE_ENTRYEXPIRYHANDLER_H_

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
#include <gfcpp/Cache.hpp>
#include <gfcpp/Region.hpp>
#include <gfcpp/ExpirationAction.hpp>
#include "ExpMapEntry.hpp"
#include "RegionInternal.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {
/**
 * @class EntryExpiryTask EntryExpiryTask.hpp
 *
 * The task object which contains the handler which gets triggered
 * when an entry expires.
 *
 * TODO: TODO: cleanup region entry nodes and handlers from expiry task
 * manager when region is destroyed
 */
class CPPCACHE_EXPORT EntryExpiryHandler : public ACE_Event_Handler {
 public:
  /**
   * Constructor
   */
  EntryExpiryHandler(RegionInternalPtr& rptr, MapEntryImplPtr& entryPtr,
                     ExpirationAction::Action action, uint32_t duration);

  /** This task object will be registered with the Timer Queue.
   *  When the timer expires the handle_timeout is invoked.
   */
  int handle_timeout(const ACE_Time_Value& current_time, const void* arg);
  /**
   * This is called when the task object needs to be cleaned up..
   */
  int handle_close(ACE_HANDLE handle, ACE_Reactor_Mask close_mask);

 private:
  // The region which contains the entry
  RegionInternalPtr m_regionPtr;
  // The ExpMapEntry contained in the ConcurrentMap against the key.
  MapEntryImplPtr m_entryPtr;
  // Action to be taken on expiry
  ExpirationAction::Action m_action;
  // Duration after which the task should be reset in case of
  // modification.
  uint32_t m_duration;
  // perform the actual expiration action
  void DoTheExpirationAction(const CacheableKeyPtr& key);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_ENTRYEXPIRYHANDLER_H_
