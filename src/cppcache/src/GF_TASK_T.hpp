/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/
/**
*@file GF_TASK_T.hpp
*@since   1.0
*@version 1.0
*/

#ifndef __GF_TASK_T_HPP__
#define __GF_TASK_T_HPP__

#include "DistributedSystemImpl.hpp"
#include <ace/Task.h>
namespace gemfire {
const char NC_thread[] = "NC thread";
template <class T>
class CPPCACHE_EXPORT GF_TASK_T : public ACE_Task_Base {
 public:
  /// Handle timeout events.
  typedef int (T::*OPERATION)(volatile bool& isRunning);

  // op_handler is the receiver of the timeout event. timeout is the method to
  // be executed by op_handler_
  GF_TASK_T(T* op_handler, OPERATION op)
      : op_handler_(op_handler),
        m_op(op),
        m_run(false),
        m_threadName(NC_thread) {}

  // op_handler is the receiver of the timeout event. timeout is the method to
  // be executed by op_handler_
  GF_TASK_T(T* op_handler, OPERATION op, const char* tn)
      : op_handler_(op_handler), m_op(op), m_run(false), m_threadName(tn) {}

  ~GF_TASK_T() {}

  void start() {
    m_run = true;
    activate();
  }

  void stop() {
    if (m_run) {
      m_run = false;
      wait();
    }
  }

  void stopNoblock() { m_run = false; }

  int svc(void) {
    DistributedSystemImpl::setThreadName(m_threadName);
    return ((this->op_handler_->*m_op)(m_run));
  }

 private:
  T* op_handler_;
  /// Handle timeout events.
  OPERATION m_op;
  volatile bool m_run;
  const char* m_threadName;
};

}  // end namespace
#endif  // __GF_TASK_T_HPP__
