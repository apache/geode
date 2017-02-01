#pragma once

#ifndef GEODE_PRESERVEDDATAEXPIRYHANDLER_H_
#define GEODE_PRESERVEDDATAEXPIRYHANDLER_H_

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
/*
 * PreservedDataExpiryHandler.hpp
 *
 *  Created on: Apr 5, 2012
 *      Author: npatel
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Cache.hpp>
#include <gfcpp/PdxSerializable.hpp>
#include "CacheImpl.hpp"
#include "ExpiryTaskManager.hpp"
//#include "CacheTransactionManagerImpl.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {
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
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_PRESERVEDDATAEXPIRYHANDLER_H_
