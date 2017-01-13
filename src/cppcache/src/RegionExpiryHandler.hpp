#ifndef __GEMFIRE_REGIONEXPIRYTASK_H__
#define __GEMFIRE_REGIONEXPIRYTASK_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Region.hpp>
#include <gfcpp/ExpirationAction.hpp>
#include "RegionInternal.hpp"

/**
 * @file
 */

namespace gemfire {
/**
 * @class RegionExpiryHandler RegionExpiryHandler.hpp
 *
 * The task object which contains the handler which gets triggered
 * when a region expires.
 *
 * TODO: cleanup region entry node and handler from expiry task
 * manager when region is destroyed
 *
 */
class CPPCACHE_EXPORT RegionExpiryHandler : public ACE_Event_Handler {
 public:
  /**
   * Constructor
   */
  RegionExpiryHandler(RegionInternalPtr& rptr, ExpirationAction::Action action,
                      uint32_t duration);

  /** This handler object will be registered with the Timer Queue.
   *  When the timer expires the handle_timeout is invoked.
   */
  int handle_timeout(const ACE_Time_Value& current_time, const void* arg);
  /**
   * This is called when the task object needs to be cleaned up..
   */
  int handle_close(ACE_HANDLE handle, ACE_Reactor_Mask close_mask);
  void setExpiryTaskId(long expiryTaskId) { m_expiryTaskId = expiryTaskId; }

 private:
  RegionInternalPtr m_regionPtr;
  ExpirationAction::Action m_action;
  uint32_t m_duration;
  long m_expiryTaskId;
  // perform the actual expiration action
  void DoTheExpirationAction();
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_REGIONEXPIRYTASK_H__
