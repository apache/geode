/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "ace/Timer_Queue.h"
#include "ace/Timer_Heap.h"
#include "ace/Reactor.h"
#include "ace/svc_export.h"
#include "ace/Timer_Heap_T.h"
#include "ace/Timer_Queue_Adapters.h"

#include "CacheImpl.hpp"
#include "ExpiryTaskManager.hpp"
#include "EntryExpiryHandler.hpp"

#include "RegionInternal.hpp"

using namespace gemfire;

EntryExpiryHandler::EntryExpiryHandler(RegionInternalPtr& rptr,
                                       MapEntryImplPtr& entryPtr,
                                       ExpirationAction::Action action,
                                       uint32_t duration)
    : m_regionPtr(rptr),
      m_entryPtr(entryPtr),
      m_action(action),
      m_duration(duration) {}

int EntryExpiryHandler::handle_timeout(const ACE_Time_Value& current_time,
                                       const void* arg) {
  CacheableKeyPtr key;
  m_entryPtr->getKeyI(key);
  ExpEntryProperties& expProps = m_entryPtr->getExpProperties();
  try {
    uint32_t curr_time = static_cast<uint32_t>(current_time.sec());

    uint32_t lastTimeForExp = expProps.getLastAccessTime();
    if (m_regionPtr->getAttributes()->getEntryTimeToLive() > 0) {
      lastTimeForExp = expProps.getLastModifiedTime();
    }

    int32_t sec = curr_time - lastTimeForExp - m_duration;
    LOGDEBUG(
        "Entered entry expiry task handler for key [%s] of region [%s]: "
        "%d,%d,%d,%d",
        Utils::getCacheableKeyString(key)->asChar(), m_regionPtr->getFullPath(),
        curr_time, lastTimeForExp, m_duration, sec);
    if (sec >= 0) {
      DoTheExpirationAction(key);
    } else {
      // reset the task after
      // (lastAccessTime + entryExpiryDuration - curr_time) in seconds
      LOGDEBUG(
          "Resetting expiry task %d secs later for key [%s] of region "
          "[%s]",
          -sec, Utils::getCacheableKeyString(key)->asChar(),
          m_regionPtr->getFullPath());
      CacheImpl::expiryTaskManager->resetTask(expProps.getExpiryTaskId(), -sec);
      return 0;
    }
  } catch (...) {
    // Ignore whatever exception comes
  }
  LOGDEBUG("Removing expiry task for key [%s] of region [%s]",
           Utils::getCacheableKeyString(key)->asChar(),
           m_regionPtr->getFullPath());
  CacheImpl::expiryTaskManager->resetTask(expProps.getExpiryTaskId(), 0);
  //  we now delete the handler in GF_Timer_Heap_ImmediateReset_T
  // and always return success.

  // set the invalid taskid as we have removed the expiry task
  expProps.setExpiryTaskId(-1);
  return 0;
}

int EntryExpiryHandler::handle_close(ACE_HANDLE, ACE_Reactor_Mask) {
  //  we now delete the handler in GF_Timer_Heap_ImmediateReset_T
  return 0;
}

inline void EntryExpiryHandler::DoTheExpirationAction(
    const CacheableKeyPtr& key) {
  // Pass a blank version tag.
  VersionTagPtr versionTag;
  switch (m_action) {
    case ExpirationAction::INVALIDATE: {
      LOGDEBUG(
          "EntryExpiryHandler::DoTheExpirationAction INVALIDATE "
          "for region %s entry with key %s",
          m_regionPtr->getFullPath(),
          Utils::getCacheableKeyString(key)->asChar());
      m_regionPtr->invalidateNoThrow(key, NULLPTR, -1,
                                     CacheEventFlags::EXPIRATION, versionTag);
      break;
    }
    case ExpirationAction::LOCAL_INVALIDATE: {
      LOGDEBUG(
          "EntryExpiryHandler::DoTheExpirationAction LOCAL_INVALIDATE "
          "for region %s entry with key %s",
          m_regionPtr->getFullPath(),
          Utils::getCacheableKeyString(key)->asChar());
      m_regionPtr->invalidateNoThrow(
          key, NULLPTR, -1,
          CacheEventFlags::EXPIRATION | CacheEventFlags::LOCAL, versionTag);
      break;
    }
    case ExpirationAction::DESTROY: {
      LOGDEBUG(
          "EntryExpiryHandler::DoTheExpirationAction DESTROY "
          "for region %s entry with key %s",
          m_regionPtr->getFullPath(),
          Utils::getCacheableKeyString(key)->asChar());
      m_regionPtr->destroyNoThrow(key, NULLPTR, -1, CacheEventFlags::EXPIRATION,
                                  versionTag);
      break;
    }
    case ExpirationAction::LOCAL_DESTROY: {
      LOGDEBUG(
          "EntryExpiryHandler::DoTheExpirationAction LOCAL_DESTROY "
          "for region %s entry with key %s",
          m_regionPtr->getFullPath(),
          Utils::getCacheableKeyString(key)->asChar());
      m_regionPtr->destroyNoThrow(
          key, NULLPTR, -1,
          CacheEventFlags::EXPIRATION | CacheEventFlags::LOCAL, versionTag);
      break;
    }
    default: {
      LOGERROR(
          "Unknown expiration action "
          "%d for region %s for key %s",
          m_action, m_regionPtr->getFullPath(),
          Utils::getCacheableKeyString(key)->asChar());
      break;
    }
  }
}
