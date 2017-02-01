#pragma once

#ifndef APACHE_GEODE_GUARD_6024bf86dd8d0d4aad8f988f5ba1655a
#define APACHE_GEODE_GUARD_6024bf86dd8d0d4aad8f988f5ba1655a

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


#include <gfcpp/gf_base.hpp>
#include <AtomicInc.hpp>
#include "fwklib/FwkLog.hpp"

#include "ace/Task.h"
#include <ace/Condition_T.h>

#include <string>

namespace apache {
namespace geode {
namespace client {
namespace testframework {

class SharedTaskObject {
 public:
  SharedTaskObject() {}

  virtual ~SharedTaskObject() {}

  virtual void initialize() = 0;
  virtual void finalize() = 0;
};

class ServiceTask {
 protected:
  volatile bool* m_run;
  SharedTaskObject* m_shared;

 public:
  ServiceTask(SharedTaskObject* shared) : m_run(NULL), m_shared(shared) {}

  virtual ~ServiceTask() {}

  void setRunFlag(volatile bool* run) { m_run = run; }
  virtual int32_t doTask() = 0;
  virtual void initialize() = 0;
  virtual void finalize() = 0;
};

class Service : public ACE_Task_Base {
 private:
  uint32_t m_ThreadCount;
  volatile bool m_run;
  AtomicInc m_busy;
  ACE_Thread_Mutex m_Mutex;
  ACE_DLList<ServiceTask> m_TaskQueue;

  int32_t svc();

  inline void putQ(ServiceTask* task, uint32_t cnt = 1) {
    ACE_Guard<ACE_Thread_Mutex> guard(m_Mutex);
    m_busy += cnt;
    for (uint32_t i = 0; i < cnt; i++) m_TaskQueue.insert_tail(task);
  }

  inline ServiceTask* getQ() {
    ACE_Guard<ACE_Thread_Mutex> guard(m_Mutex);
    return m_TaskQueue.delete_head();
  }

 public:
  Service(int32_t threadCnt);

  inline ~Service() { stopThreads(); }

  int32_t runThreaded(ServiceTask* task, uint32_t threads);

  inline uint32_t getBusyCount() {
    return static_cast<uint32_t>(m_busy.value());
  }
  inline uint32_t getIdleCount() {
    return m_ThreadCount - static_cast<uint32_t>(m_busy.value());
  }

  inline void stopThreads() {
    m_run = false;
    wait();
  }
};

template <class T>
class SafeQueue {
  ACE_Thread_Mutex m_mutex;
  ACE_DLList<T> m_queue;
  ACE_Condition<ACE_Thread_Mutex> m_cond;

 public:
  SafeQueue() : m_mutex(), m_cond(m_mutex) {}
  ~SafeQueue() {}

  void enqueue(T* val) {
    ACE_Guard<ACE_Thread_Mutex> guard(m_mutex);
    m_queue.insert_tail(val);
    m_cond.signal();
  }

  T* dequeue() {
    ACE_Guard<ACE_Thread_Mutex> guard(m_mutex);
    if (m_queue.size() == 0) {
      ACE_Time_Value until(2);
      until += ACE_OS::gettimeofday();
      ;
      int32_t res = m_cond.wait(&until);
      if (res == -1) return NULL;
    }
    return m_queue.delete_head();
  }

  bool isEmpty() {
    ACE_Guard<ACE_Thread_Mutex> guard(m_mutex);
    return m_queue.isEmpty();
  }

  uint32_t size() {
    ACE_Guard<ACE_Thread_Mutex> guard(m_mutex);
    return static_cast<uint32_t>(m_queue.size());
  }
};

class IPCMessage {
 protected:
  std::string m_msg;

 public:
  IPCMessage() {}
  IPCMessage(std::string content) { m_msg = content; }

  virtual ~IPCMessage() {}

  std::string& getMessage() { return m_msg; }

  uint32_t length() { return static_cast<uint32_t>(m_msg.size()); }

  const char* getContent() { return m_msg.c_str(); }

  void setMessage(std::string& content) { m_msg = content; }

  virtual void clear() { m_msg.clear(); }
};

}  // namespace testframework
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // APACHE_GEODE_GUARD_6024bf86dd8d0d4aad8f988f5ba1655a
