/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_TOMBSTONEEXPIRYTASK_H__
#define __GEMFIRE_TOMBSTONEEXPIRYTASK_H__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Region.hpp>
#include <gfcpp/ExpirationAction.hpp>
#include "RegionInternal.hpp"
#include "TombstoneList.hpp"

/**
 * @file
 */

namespace gemfire {
/**
 * @class TombstoneExpiryHandler TombstoneExpiryHandler.hpp
 *
 * The task object which contains the handler which gets triggered
 * when a tombstone expires.
 *
 */
class CPPCACHE_EXPORT TombstoneExpiryHandler : public ACE_Event_Handler {
 public:
  /**
* Constructor
*/
  TombstoneExpiryHandler(TombstoneEntryPtr entryPtr,
                         TombstoneList* tombstoneList, uint32_t duration);

  /** This task object will be registered with the Timer Queue.
   *  When the timer expires the handle_timeout is invoked.
   */
  int handle_timeout(const ACE_Time_Value& current_time, const void* arg);
  /**
   * This is called when the task object needs to be cleaned up..
   */
  int handle_close(ACE_HANDLE handle, ACE_Reactor_Mask close_mask);

  void setTombstoneEntry(TombstoneEntryPtr entryPtr) { m_entryPtr = entryPtr; }

 private:
  // The entry contained in the tombstone list
  TombstoneEntryPtr m_entryPtr;
  // Duration after which the task should be reset in case of
  // modification.
  uint32_t m_duration;
  // perform the actual expiration action
  void DoTheExpirationAction(const CacheableKeyPtr& key);

  TombstoneList* m_tombstoneList;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_TOMBSTONEEXPIRYTASK_H__
