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
#include "ace/Timer_Queue.h"
#include "ace/Timer_Heap.h"
#include "ace/Reactor.h"
#include "ace/svc_export.h"
#include "ace/Timer_Heap_T.h"
#include "ace/Timer_Queue_Adapters.h"

#include "CacheImpl.hpp"
#include "ExpiryTaskManager.hpp"
#include "TombstoneExpiryHandler.hpp"
#include "MapEntry.hpp"
#include "RegionInternal.hpp"

using namespace apache::geode::client;

TombstoneExpiryHandler::TombstoneExpiryHandler(TombstoneEntryPtr entryPtr,
                                               TombstoneList* tombstoneList,
                                               uint32_t duration)
    : m_entryPtr(entryPtr),
      m_duration(duration),
      m_tombstoneList(tombstoneList) {}

int TombstoneExpiryHandler::handle_timeout(const ACE_Time_Value& current_time,
                                           const void* arg) {
  CacheableKeyPtr key;
  m_entryPtr->getEntry()->getKeyI(key);
  int64_t creationTime = m_entryPtr->getTombstoneCreationTime();
  int64_t curr_time = static_cast<int64_t>(current_time.get_msec());
  int64_t expiryTaskId = m_entryPtr->getExpiryTaskId();
  int64_t sec = curr_time - creationTime - m_duration * 1000;
  try {
    LOGDEBUG(
        "Entered entry expiry task handler for tombstone of key [%s]: "
        "%lld,%lld,%d,%lld",
        Utils::getCacheableKeyString(key)->asChar(), curr_time, creationTime,
        m_duration, sec);
    if (sec >= 0) {
      DoTheExpirationAction(key);
    } else {
      // reset the task after
      // (lastAccessTime + entryExpiryDuration - curr_time) in seconds
      LOGDEBUG(
          "Resetting expiry task %d secs later for key "
          "[%s]",
          -sec / 1000 + 1, Utils::getCacheableKeyString(key)->asChar());
      CacheImpl::expiryTaskManager->resetTask(
          static_cast<long>(m_entryPtr->getExpiryTaskId()),
          uint32_t(-sec / 1000 + 1));
      return 0;
    }
  } catch (...) {
    // Ignore whatever exception comes
  }
  LOGDEBUG("Removing expiry task for key [%s]",
           Utils::getCacheableKeyString(key)->asChar());
  // we now delete the handler in GF_Timer_Heap_ImmediateReset_T
  // and always return success.
  CacheImpl::expiryTaskManager->resetTask(static_cast<long>(expiryTaskId), 0);
  return 0;
}

int TombstoneExpiryHandler::handle_close(ACE_HANDLE, ACE_Reactor_Mask) {
  // we now delete the handler in GF_Timer_Heap_ImmediateReset_T
  return 0;
}

inline void TombstoneExpiryHandler::DoTheExpirationAction(
    const CacheableKeyPtr& key) {
  LOGDEBUG(
      "EntryExpiryHandler::DoTheExpirationAction LOCAL_DESTROY "
      "for region entry with key %s",
      Utils::getCacheableKeyString(key)->asChar());
  m_tombstoneList->removeEntryFromMapSegment(key);
}
