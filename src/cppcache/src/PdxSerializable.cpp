/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxSerializable.cpp
 *
 *  Created on: Sep 29, 2011
 *      Author: npatel
 */

#include <gfcpp/PdxSerializable.hpp>
#include <GemfireTypeIdsImpl.hpp>
#include <gfcpp/CacheableString.hpp>
#include <PdxHelper.hpp>
#include <gfcpp/CacheableKeys.hpp>

namespace gemfire {
PdxSerializable::PdxSerializable() {}

PdxSerializable::~PdxSerializable() {}

int8_t PdxSerializable::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::PDX);
}

void PdxSerializable::toData(DataOutput& output) const {
  LOGDEBUG("SerRegistry.cpp:serializePdx:86: PDX Object Type = %s",
           typeid(*this).name());
  PdxHelper::serializePdx(output, *this);
}

Serializable* PdxSerializable::fromData(DataInput& input) {
  throw UnsupportedOperationException(
      "operation PdxSerializable::fromData() is not supported ");
}

CacheableStringPtr PdxSerializable::toString() const {
  return CacheableString::create("PdxSerializable");
}

bool PdxSerializable::operator==(const CacheableKey& other) const {
  return (this == &other);
}

uint32_t PdxSerializable::hashcode() const {
  uint64_t hash = static_cast<uint64_t>((intptr_t)this);
  return gemfire::serializer::hashcode(hash);
}
}  // namespace gemfire
