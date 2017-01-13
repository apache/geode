/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_SET_HPP_
#define _GEMFIRE_SET_HPP_

#include <unordered_set>
#include <ace/Guard_T.h>
#include <ace/Recursive_Thread_Mutex.h>

#include "NonCopyable.hpp"

namespace gemfire {

// A synchronized Set using std::unordered_set<T>

/* adongre
 * CID 28616: Other violation (COPY_WITHOUT_ASSIGN)
 * Class "gemfire::Set<unsigned short>::Iterator" has user-written copyi
 * `constructor "gemfire::Set<unsigned
 * short>::Iterator::Iterator(gemfire::Set<unsigned short>::Iterator const &)" i
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
}  // end namespace

#endif  // !defined _GEMFIRE_SET_HPP_
