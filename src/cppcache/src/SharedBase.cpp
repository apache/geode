/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/SharedBase.hpp>
#include <HostAsm.hpp>

#include <typeinfo>

namespace gemfire {

void SharedBase::preserveSB() const { HostAsm::atomicAdd(m_refCount, 1); }

void SharedBase::releaseSB() const {
  if (HostAsm::atomicAdd(m_refCount, -1) == 0) {
    delete this;
  }
}

// dummy instance to use for NULLPTR
const NullSharedBase* const NullSharedBase::s_instancePtr = NULL;
}  // namespace gemfire
