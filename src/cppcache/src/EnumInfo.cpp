/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "EnumInfo.hpp"
#include "Utils.hpp"
#include "GemfireTypeIdsImpl.hpp"

namespace gemfire {

EnumInfo::~EnumInfo() {}

EnumInfo::EnumInfo()
    : m_enumClassName(NULLPTR), m_enumName(NULLPTR), m_ordinal(-1) {}

EnumInfo::EnumInfo(const char *enumClassName, const char *enumName,
                   int32_t ordinal)
    : m_ordinal(ordinal) {
  m_enumClassName = CacheableString::create(enumClassName);
  m_enumName = CacheableString::create(enumName);
}

uint32_t EnumInfo::hashcode() const {
  return ((m_enumClassName != NULLPTR ? m_enumClassName->hashcode() : 0) +
          (m_enumName != NULLPTR ? m_enumName->hashcode() : 0));
}

bool EnumInfo::operator==(const CacheableKey &other) const {
  if (other.typeId() != typeId()) {
    return false;
  }
  const EnumInfo &otherEnum = static_cast<const EnumInfo &>(other);
  if (m_ordinal != otherEnum.m_ordinal) {
    return false;
  }
  if (m_enumClassName == NULLPTR) {
    return (otherEnum.m_enumClassName == NULLPTR);
  }
  if (m_enumName == NULLPTR) {
    return (otherEnum.m_enumName == NULLPTR);
  }
  if (strcmp(m_enumClassName->asChar(), otherEnum.m_enumClassName->asChar()) !=
      0) {
    return false;
  }
  if (strcmp(m_enumName->asChar(), otherEnum.m_enumName->asChar()) != 0) {
    return false;
  }
  return true;
}

void EnumInfo::toData(gemfire::DataOutput &output) const {
  output.writeObject(m_enumClassName);
  output.writeObject(m_enumName);
  output.writeInt(m_ordinal);
}

Serializable *EnumInfo::fromData(gemfire::DataInput &input) {
  input.readObject(m_enumClassName);
  input.readObject(m_enumName);
  input.readInt(&m_ordinal);
  return this;
}

int8_t EnumInfo::DSFID() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::FixedIDByte);
}
}  // namespace gemfire
