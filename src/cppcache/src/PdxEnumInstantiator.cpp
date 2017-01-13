/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* PdxEnumInstantiator.cpp
*/

#include "PdxEnumInstantiator.hpp"
#include "GemfireTypeIdsImpl.hpp"
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/DataInput.hpp>

namespace gemfire {

PdxEnumInstantiator::PdxEnumInstantiator() {}

PdxEnumInstantiator::~PdxEnumInstantiator() {}

int8_t PdxEnumInstantiator::typeId() const {
  return static_cast<int8_t>(GemfireTypeIds::CacheableEnum);
}

void PdxEnumInstantiator::toData(DataOutput& output) const {
  throw UnsupportedOperationException(
      "operation PdxEnumInstantiator::toData() is not supported ");
}

Serializable* PdxEnumInstantiator::fromData(DataInput& input) {
  m_enumObject = CacheableEnum::create(" ", " ", 0);
  m_enumObject->fromData(input);
  return m_enumObject.ptr();
}

CacheableStringPtr PdxEnumInstantiator::toString() const {
  return CacheableString::create("PdxEnumInstantiator");
}
}  // namespace gemfire
