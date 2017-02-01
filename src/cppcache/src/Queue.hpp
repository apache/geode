#pragma once

#ifndef GEODE_QUEUE_H_
#define GEODE_QUEUE_H_

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
#include <ace/Time_Value.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Condition_Recursive_Thread_Mutex.h>
#include <ace/Guard_T.h>

namespace apache {
namespace geode {
namespace client {

template <class T>
class CPPCACHE_EXPORT Queue {
 public:
  /**
   * Constructor with parameter to specify whether the contained objects
   * should be deleted in the destructor, and maximum size of queue.
   */
  Queue(bool deleteObjs = true, const uint32_t maxSize = 0)
      : m_cond(m_mutex),
        m_deleteObjs(deleteObjs),
        m_maxSize(maxSize),
        m_closed(false) {}

  ~Queue() { close(); }

  T* get() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
    return getNoLock();
  }

  /** wait "sec" secs, "usec" micros time until notified */
  T* getUntil(uint32_t sec, uint32_t usec = 0) {
    T* mp = get();

    if (mp == 0) {
      ACE_Time_Value interval(sec + usec / 1000000, usec % 1000000);
      ACE_Time_Value stopAt(ACE_OS::gettimeofday());
      stopAt += interval;

      while (!m_closed && mp == 0 && ACE_OS::gettimeofday() < stopAt) {
        ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
        if (m_cond.wait(&stopAt) != -1) mp = getNoLock();
      }
    }
    return mp;
  }

  bool put(T* mp) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
    if (m_maxSize > 0 && m_queue.size() >= m_maxSize) {
      return false;
    }
    return putNoLock(mp);
  }

  bool putUntil(T* mp, uint32_t sec, uint32_t usec = 0) {
    if (m_maxSize > 0) {
      {
        ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
        if (m_queue.size() < m_maxSize) {
          return putNoLock(mp);
        }
      }
      ACE_Time_Value interval(sec + usec / 1000000, usec % 1000000);
      ACE_Time_Value stopAt(ACE_OS::gettimeofday());
      stopAt += interval;

      while (ACE_OS::gettimeofday() < stopAt) {
        ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
        m_cond.wait(&stopAt);
        if (m_queue.size() < m_maxSize) {
          return putNoLock(mp);
        }
      }
      return false;
    } else {
      ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
      return putNoLock(mp);
    }
  }

  void open() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
    m_closed = false;
  }

  void close() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);

    if (m_deleteObjs) {
      while (m_queue.size() > 0) {
        T* mp = m_queue.back();
        m_queue.pop_back();
        delete mp;
      }
    } else {
      m_queue.clear();
    }
    m_closed = true;
    m_cond.signal();
  }

  uint32_t size() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
    return static_cast<uint32_t>(m_queue.size());
  }

  bool empty() { return (size() == 0); }

 private:
  inline T* getNoLock() {
    T* mp = 0;

    uint32_t queueSize = static_cast<uint32_t>(m_queue.size());
    if (queueSize > 0) {
      mp = m_queue.back();
      m_queue.pop_back();
      // signal the waiting putter threads, if any
      if (m_maxSize > 0 && queueSize == m_maxSize) {
        m_cond.signal();
      }
    }
    return mp;
  }

  inline bool putNoLock(T* mp) {
    if (!m_closed) {
      m_queue.push_front(mp);
      // signal the waiting getter threads, if any
      if (m_queue.size() == 1) {
        m_cond.signal();
      }
      return true;
    }
    return false;
  }

  std::deque<T*> m_queue;
  ACE_Recursive_Thread_Mutex m_mutex;
  ACE_Condition<ACE_Recursive_Thread_Mutex> m_cond;
  bool m_deleteObjs;
  const uint32_t m_maxSize;
  bool m_closed;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_QUEUE_H_
