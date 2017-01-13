/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_IMPL_POOLEDBASEPOOL_HPP_
#define _GEMFIRE_IMPL_POOLEDBASEPOOL_HPP_

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>
#include "SpinLock.hpp"
#include "PooledBase.hpp"
#include <deque>

namespace gemfire {

class CPPCACHE_EXPORT PooledBasePool {
  SpinLock m_poolLock;
  std::deque<PooledBase*> m_pooldata;

 public:
  PooledBasePool() : m_poolLock(), m_pooldata() {}

  ~PooledBasePool() {
    SpinLockGuard guard(m_poolLock);
    while (!m_pooldata.empty()) {
      PooledBase* item = m_pooldata.front();
      m_pooldata.pop_front();
      delete item;
    }
  }

  inline void returnToPool(PooledBase* poolable) {
    poolable->prePool();
    {
      SpinLockGuard guard(m_poolLock);
      m_pooldata.push_back(const_cast<PooledBase*>(poolable));
    }
  }

  inline PooledBase* takeFromPool() {
    PooledBase* result = NULL;
    {
      SpinLockGuard guard(m_poolLock);
      if (!m_pooldata.empty()) {
        result = m_pooldata.front();
        m_pooldata.pop_front();
      }
    }
    if (result != NULL) {
      result->postPool();
    }
    return result;
  }

  inline void clear() {
    SpinLockGuard guard(m_poolLock);
    while (!m_pooldata.empty()) {
      PooledBase* item = m_pooldata.front();
      m_pooldata.pop_front();
      delete item;
    }
  }
};
}

#endif
