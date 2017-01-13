/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PreservedDataExpiryHandler.hpp
 *
 *  Created on: Apr 5, 2012
 *      Author: npatel
 */

#ifndef __PRESERVEDDATAEXPIRYHANDLER_HPP__
#define __PRESERVEDDATAEXPIRYHANDLER_HPP__
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Cache.hpp>
#include <gfcpp/PdxSerializable.hpp>
#include "CacheImpl.hpp"
#include "ExpiryTaskManager.hpp"
//#include "CacheTransactionManagerImpl.hpp"

/**
 * @file
 */

namespace gemfire {
// class CacheTransactionManagerImpl;
/**
 * @class PreservedDataExpiryHandler
 *
 * The task object which contains the handler which gets triggered
 * when a preserved data expires.
 *
 */
class CPPCACHE_EXPORT PreservedDataExpiryHandler : public ACE_Event_Handler {
 public:
  /**
   * Constructor
   */
  PreservedDataExpiryHandler(PdxSerializablePtr pdxObjectPtr,
                             uint32_t duration);

  /** This task object will be registered with the Timer Queue.
   *  When the timer expires the handle_timeout is invoked.
   */
  int handle_timeout(const ACE_Time_Value &current_time, const void *arg);
  /**
   * This is called when the task object needs to be cleaned up..
   */
  int handle_close(ACE_HANDLE handle, ACE_Reactor_Mask close_mask);

 private:
  // Duration after which the task should be reset in case of
  // modification.
  // UNUSED uint32_t m_duration;
  PdxSerializablePtr m_pdxObjectPtr;
};
}

#endif /* __PRESERVEDDATAEXPIRYHANDLER_HPP__*/
