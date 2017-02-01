#pragma once

#ifndef GEODE_INTQUEUE_H_
#define GEODE_INTQUEUE_H_

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

#include <deque>
#include <ace/ACE.h>
#include <ace/Condition_Recursive_Thread_Mutex.h>
#include <ace/Time_Value.h>
#include <ace/Guard_T.h>
#include <ace/Recursive_Thread_Mutex.h>

namespace apache {
namespace geode {
namespace client {

template <class T>

class CPPCACHE_EXPORT IntQueue {
 public:
  IntQueue() : m_cond(m_mutex) {}

  ~IntQueue() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);

    while (m_queue.size() > 0) {
      m_queue.pop_back();
    }
  }

  /** wait usec time until notified */
  T get(long usec) {
    ACE_Time_Value interval(usec / 1000000, usec % 1000000);
    return getUntil(interval);
  }

  T get() {
    T mp = 0;

    getInternal(mp);
    return mp;
  }

  void put(T mp) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
    m_queue.push_front(mp);
    m_cond.signal();
  }

  uint32_t size() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
    return static_cast<uint32_t>(m_queue.size());
  }

  void clear() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
    m_queue.clear();
    m_cond.signal();
  }

  bool empty() { return size() == 0; }

 private:
  inline bool getInternal(T& val) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);

    if (m_queue.size() > 0) {
      val = m_queue.back();
      m_queue.pop_back();
      return true;
    }

    return false;
  }

  T getUntil(const ACE_Time_Value& interval) {
    T mp = 0;
    bool found = getInternal(mp);

    if (!found) {
      ACE_Time_Value stopAt(ACE_OS::gettimeofday());
      stopAt += interval;

      while (!found && ACE_OS::gettimeofday() < stopAt) {
        ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
        m_cond.wait(&stopAt);
        if (m_queue.size() > 0) {
          mp = m_queue.back();
          m_queue.pop_back();
          found = true;
        }
      }
    }
    return mp;
  }

  typedef std::deque<T> LocalQueue;
  LocalQueue m_queue;
  ACE_Recursive_Thread_Mutex m_mutex;
  ACE_Condition<ACE_Recursive_Thread_Mutex> m_cond;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_INTQUEUE_H_
