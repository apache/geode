/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 *@file ExpiryHandler_T.hpp
 *@since   1.0
 *@version 1.0
 */

#ifndef EXPIRYHANDLER_T_HPP
#define EXPIRYHANDLER_T_HPP

#include "ace/Event_Handler.h"
#include <gfcpp/Log.hpp>
#include "StackTrace.hpp"
namespace gemfire {
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

}  // end namespace
#endif  // !defined (EXPIRYHANDLER_T_HPP)
