

// util/PooledBase.cpp		-*- mode: c++ -*-

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "PooledBase.hpp"
#include "HostAsm.hpp"
#include "PooledBasePool.hpp"

#include <typeinfo>

namespace gemfire {

PooledBase::PooledBase(PooledBasePool* pool) : m_refCount(0), m_pool(pool) {
  GF_D_ASSERT(m_pool != NULL);
}

PooledBase::~PooledBase() { m_pool = NULL; }

void PooledBase::preserveSB() const {
  PooledBase* self = const_cast<PooledBase*>(this);
  HostAsm::atomicAdd(self->m_refCount, 1);
}

void PooledBase::releaseSB() const {
  PooledBase* self = const_cast<PooledBase*>(this);
  if (HostAsm::atomicAdd(self->m_refCount, -1) == 0) {
    m_pool->returnToPool(self);
  }
}

void PooledBase::prePool() {}

void PooledBase::postPool() {}
}  // namespace gemfire
