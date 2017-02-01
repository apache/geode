#pragma once

#ifndef GEODE_SET_H_
#define GEODE_SET_H_

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

#include <unordered_set>
#include <ace/Guard_T.h>
#include <ace/Recursive_Thread_Mutex.h>

#include "NonCopyable.hpp"

namespace apache {
namespace geode {
namespace client {

// A synchronized Set using std::unordered_set<T>

/* adongre
 * CID 28616: Other violation (COPY_WITHOUT_ASSIGN)
 * Class "apache::geode::client::Set<unsigned short>::Iterator" has user-written
 * copyi
 * `constructor "apache::geode::client::Set<unsigned
 * short>::Iterator::Iterator(apache::geode::client::Set<unsigned
 * short>::Iterator const &)" i
 * but no corresponding user-written assignment operator.
 *
 * FIX : Make the class non copyable
 */
template <typename T>
class CPPCACHE_EXPORT Set : private NonAssignable {
 public:
  // Iterator for a synchronized Set
  class Iterator {
   private:
    Set<T>& m_set;
    typename std::unordered_set<T>::const_iterator m_iter;

    Iterator(Set<T>& set) : m_set(set) {
      m_set.m_mutex.acquire();
      m_iter = set.m_set.begin();
    }
    // Never defined.
    Iterator();

   public:
    Iterator(const Iterator& other) : m_set(other.m_set) {
      m_set.m_mutex.acquire();
      m_iter = other.m_iter;
    }

    inline const T& next() { return *(m_iter++); }

    inline bool hasNext() const { return (m_iter != m_set.m_set.end()); }

    ~Iterator() { m_set.m_mutex.release(); }

    friend class Set;
  };

  Set() : m_set() {}

  ~Set() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);

    m_set.clear();
  }

  inline bool insert(const T& key) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);

    return m_set.insert(key).second;
  }

  inline bool find(const T& key) const {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);

    return (m_set.find(key) != m_set.end());
  }

  inline bool erase(const T& key) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);

    return (m_set.erase(key) > 0);
  }

  inline size_t size() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);

    return m_set.size();
  }

  inline void clear() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);

    m_set.clear();
  }

  inline bool empty() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);

    return m_set.empty();
  }

  inline Iterator iterator() { return Iterator(*this); }

 private:
  std::unordered_set<T> m_set;
  ACE_Recursive_Thread_Mutex m_mutex;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_SET_H_
