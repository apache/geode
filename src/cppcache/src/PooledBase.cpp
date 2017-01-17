

// util/PooledBase.cpp		-*- mode: c++ -*-

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
