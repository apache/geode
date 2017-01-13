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

#include "RegionExpiryHandler.hpp"
#include "RegionInternal.hpp"

using namespace gemfire;

RegionExpiryHandler::RegionExpiryHandler(RegionInternalPtr& rptr,
                                         ExpirationAction::Action action,
                                         uint32_t duration)
    : m_regionPtr(rptr),
      m_action(action),
      m_duration(duration),
      /* adongre
       * CID 28941: Uninitialized scalar field (UNINIT_CTOR)
       */
      m_expiryTaskId(0) {}

int RegionExpiryHandler::handle_timeout(const ACE_Time_Value& current_time,
                                        const void* arg) {
  time_t curr_time = current_time.sec();
  try {
    CacheStatisticsPtr ptr = m_regionPtr->getStatistics();
    uint32_t lastTimeForExp = ptr->getLastAccessedTime();
    if (m_regionPtr->getAttributes()->getRegionTimeToLive() > 0) {
      lastTimeForExp = ptr->getLastModifiedTime();
    }

    int32_t sec = static_cast<int32_t>(curr_time) - lastTimeForExp - m_duration;
    LOGDEBUG("Entered region expiry task handler for region [%s]: %d,%d,%d,%d",
             m_regionPtr->getFullPath(), curr_time, lastTimeForExp, m_duration,
             sec);
    if (sec >= 0) {
      DoTheExpirationAction();
    } else {
      // reset the task after
      // (lastAccessTime + entryExpiryDuration - curr_time) in seconds
      LOGDEBUG("Resetting expiry task for region [%s] after %d sec",
               m_regionPtr->getFullPath(), -sec);
      CacheImpl::expiryTaskManager->resetTask(m_expiryTaskId, -sec);
      return 0;
    }
  } catch (...) {
    // Ignore whatever exception comes
  }
  LOGDEBUG("Removing expiry task for region [%s]", m_regionPtr->getFullPath());
  CacheImpl::expiryTaskManager->resetTask(m_expiryTaskId, 0);
  //  we now delete the handler in GF_Timer_Heap_ImmediateReset_T
  // and always return success.
  return 0;
}

int RegionExpiryHandler::handle_close(ACE_HANDLE handle,
                                      ACE_Reactor_Mask close_mask) {
  //  we now delete the handler in GF_Timer_Heap_ImmediateReset_T
  // delete this;
  return 0;
}

void RegionExpiryHandler::DoTheExpirationAction() {
  switch (m_action) {
    case ExpirationAction::INVALIDATE: {
      LOGDEBUG(
          "RegionExpiryHandler::DoTheExpirationAction INVALIDATE "
          "region [%s]",
          m_regionPtr->getFullPath());
      m_regionPtr->invalidateRegionNoThrow(NULLPTR,
                                           CacheEventFlags::EXPIRATION);
      break;
    }
    case ExpirationAction::LOCAL_INVALIDATE: {
      LOGDEBUG(
          "RegionExpiryHandler::DoTheExpirationAction LOCAL_INVALIDATE "
          "region [%s]",
          m_regionPtr->getFullPath());
      m_regionPtr->invalidateRegionNoThrow(
          NULLPTR, CacheEventFlags::EXPIRATION | CacheEventFlags::LOCAL);
      break;
    }
    case ExpirationAction::DESTROY: {
      LOGDEBUG(
          "RegionExpiryHandler::DoTheExpirationAction DESTROY "
          "region [%s]",
          m_regionPtr->getFullPath());
      m_regionPtr->destroyRegionNoThrow(NULLPTR, true,
                                        CacheEventFlags::EXPIRATION);
      break;
    }
    case ExpirationAction::LOCAL_DESTROY: {
      LOGDEBUG(
          "RegionExpiryHandler::DoTheExpirationAction LOCAL_DESTROY "
          "region [%s]",
          m_regionPtr->getFullPath());
      m_regionPtr->destroyRegionNoThrow(
          NULLPTR, true, CacheEventFlags::EXPIRATION | CacheEventFlags::LOCAL);
      break;
    }
    default: {
      LOGERROR(
          "Unknown expiration action "
          "%d for region [%s]",
          m_action, m_regionPtr->getFullPath());
      break;
    }
  }
}
