/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxInstantiator.cpp
 *
 *  Created on: Dec 28, 2011
 *      Author: npatel
 */

#include "PdxInstantiator.hpp"
#include "GemfireTypeIdsImpl.hpp"
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/DataInput.hpp>
#include "PdxHelper.hpp"
#include <gfcpp/PdxSerializable.hpp>

namespace gemfire {

PdxInstantiator::PdxInstantiator() {}

PdxInstantiator::~PdxInstantiator() {}

int8_t PdxInstantiator::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::PDX);
}

void PdxInstantiator::toData(DataOutput& output) const {
  throw UnsupportedOperationException(
      "operation PdxInstantiator::toData() is not supported ");
}

Serializable* PdxInstantiator::fromData(DataInput& input) {
  m_userObject = PdxHelper::deserializePdx(input, false);
  return m_userObject.ptr();
}

CacheableStringPtr PdxInstantiator::toString() const {
  return CacheableString::create("PdxInstantiator");
}
}  // namespace gemfire
