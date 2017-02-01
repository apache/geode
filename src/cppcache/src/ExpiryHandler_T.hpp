#pragma once

#ifndef GEODE_EXPIRYHANDLER_T_H_
#define GEODE_EXPIRYHANDLER_T_H_

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
/**
 *@file ExpiryHandler_T.hpp
 *@since   1.0
 *@version 1.0
 */


#include "ace/Event_Handler.h"
#include <gfcpp/Log.hpp>
#include "StackTrace.hpp"
namespace apache {
namespace geode {
namespace client {
/**
 * This class manages deletion of the handler so this class should only be
 * used for cases that are cancelled explicitly and never expire otherwise,
 * since GF_Timer_Heap_ImmediateReset_T also deletes handlers that expire
 */
template <class T>
class CPPCACHE_EXPORT ExpiryHandler_T : public ACE_Event_Handler {
 public:
  /// Handle timeout events.
  typedef int (T::*TO_HANDLER)(const ACE_Time_Value &, const void *);

  // op_handler is the receiver of the timeout event. timeout is the method
  // to be executed by op_handler_
  ExpiryHandler_T(T *op_handler, TO_HANDLER timeout)
      : op_handler_(op_handler), to_handler_(timeout) {}

  virtual ~ExpiryHandler_T() {}

  virtual int handle_timeout(const ACE_Time_Value &tv, const void *arg) {
    return (this->to_handler_ == 0 ? 0 : (this->op_handler_->*to_handler_)(
                                             tv, arg));
  }

  virtual int handle_close(ACE_HANDLE fd, ACE_Reactor_Mask close_mask) {
    //  delete the handler so this class can only be used for cases
    // that do not handle deletion themselves and those that never expire
    // otherwise, since GF_Timer_Heap_ImmediateReset_T also deletes handlers
    // that fall out of the queue
    delete this;
    return 0;
  }

 private:
  T *op_handler_;
  /// Handle timeout events.
  TO_HANDLER to_handler_;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_EXPIRYHANDLER_T_H_
