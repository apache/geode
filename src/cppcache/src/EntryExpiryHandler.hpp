#ifndef _GEMFIRE_ENTRYEXPIRYTASK_H__
#define _GEMFIRE_ENTRYEXPIRYTASK_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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

namespace gemfire {
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
};      // namespace gemfire
#endif  // ifndef _GEMFIRE_ENTRYEXPIRYTASK_H__
