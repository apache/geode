#ifndef _GEMFIRE_PooledBase_HPP_
#define _GEMFIRE_PooledBase_HPP_

// PooledBase.hpp     -*- mode: c++ -*-

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

namespace gemfire {

class PooledBasePool;

/**
 * @class PooledBase PooledBase.hpp
 *
 * This abstract base class is the base class of all user objects
 * that have the shared capability of reference counting.
 */
class CPPCACHE_EXPORT PooledBase {
 public:
  PooledBase(PooledBasePool* pool);

  void preserveSB() const;
  void releaseSB() const;

  inline int32_t refCount() { return m_refCount; }

  virtual ~PooledBase();

  /** called just prior to inserting an object back into the pool. */
  virtual void prePool();

  /** called just after removing an object from the pool. */
  virtual void postPool();

 private:
  volatile int32_t m_refCount;
  PooledBasePool* m_pool;

  void operator=(const PooledBase& rhs);
};
}

#endif

// the end...
