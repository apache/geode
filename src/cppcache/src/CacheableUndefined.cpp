/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/CacheableUndefined.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/GemfireTypeIds.hpp>
#include <GemfireTypeIdsImpl.hpp>

namespace gemfire {

void CacheableUndefined::toData(DataOutput& output) const {}

Serializable* CacheableUndefined::fromData(DataInput& input) { return this; }

int32_t CacheableUndefined::classId() const { return 0; }

int8_t CacheableUndefined::typeId() const {
  return GemfireTypeIds::CacheableUndefined;
}

int8_t CacheableUndefined::DSFID() const {
  return GemfireTypeIdsImpl::FixedIDByte;
}

uint32_t CacheableUndefined::objectSize() const { return 0; }
}  // namespace gemfire
