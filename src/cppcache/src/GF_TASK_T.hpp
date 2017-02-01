#pragma once

#ifndef GEODE_GF_TASK_T_H_
#define GEODE_GF_TASK_T_H_

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
*@file GF_TASK_T.hpp
*@since   1.0
*@version 1.0
*/


#include "DistributedSystemImpl.hpp"
#include <ace/Task.h>
namespace apache {
namespace geode {
namespace client {
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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GF_TASK_T_H_
