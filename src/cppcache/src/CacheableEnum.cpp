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
#include <gfcpp/CacheableEnum.hpp>
#include <Utils.hpp>
#include <PdxHelper.hpp>
#include <GemfireTypeIdsImpl.hpp>
#include <EnumInfo.hpp>

namespace gemfire {

CacheableEnum::~CacheableEnum() {}

CacheableEnum::CacheableEnum()
    : m_enumClassName(NULLPTR),
      m_enumName(NULLPTR),
      m_ordinal(-1),
      m_hashcode(0) {}

CacheableEnum::CacheableEnum(const char* enumClassName, const char* enumName,
                             int32_t ordinal)
    : m_ordinal(ordinal), m_hashcode(0) {
  m_enumClassName = CacheableString::create(enumClassName);
  m_enumName = CacheableString::create(enumName);
}

void CacheableEnum::toData(gemfire::DataOutput& output) const {
  int enumVal = PdxHelper::getEnumValue(m_enumClassName->asChar(),
                                        m_enumName->asChar(), m_ordinal);
  output.write(static_cast<int8_t>(GemfireTypeIds::CacheableEnum));
  output.write(int8_t(enumVal >> 24));
  output.writeArrayLen(enumVal & 0xFFFFFF);
}

Serializable* CacheableEnum::fromData(gemfire::DataInput& input) {
  int8_t dsId;
  input.read(&dsId);
  int32_t arrLen;
  input.readArrayLen(&arrLen);
  int enumId = (dsId << 24) | (arrLen & 0xFFFFFF);
  EnumInfoPtr enumVal = PdxHelper::getEnum(enumId);

  m_enumClassName = enumVal->getEnumClassName();
  m_enumName = enumVal->getEnumName();
  m_ordinal = enumVal->getEnumOrdinal();
  return enumVal.ptr();
}

uint32_t CacheableEnum::hashcode() const {
  int localHash = 1;
  if (m_hashcode == 0) {
    int prime = 31;
    localHash =
        prime * localHash +
        ((m_enumClassName != NULLPTR ? m_enumClassName->hashcode() : 0));
    localHash = prime * localHash +
                ((m_enumName != NULLPTR ? m_enumName->hashcode() : 0));
    m_hashcode = localHash;
  }
  return m_hashcode;
}

bool CacheableEnum::operator==(const CacheableKey& other) const {
  if (other.typeId() != typeId()) {
    return false;
  }
  CacheableKey& temp = const_cast<CacheableKey&>(other);
  CacheableEnum* otherEnum = static_cast<CacheableEnum*>(&temp);
  if (otherEnum == NULL) {
    return false;
  }
  if (m_ordinal != otherEnum->m_ordinal) {
    return false;
  }
  if (m_enumClassName == NULLPTR) {
    return (otherEnum->m_enumClassName == NULLPTR);
  }
  if (m_enumName == NULLPTR) {
    return (otherEnum->m_enumName == NULLPTR);
  }
  if (strcmp(m_enumClassName->asChar(), otherEnum->m_enumClassName->asChar()) !=
      0) {
    return false;
  }
  if (strcmp(m_enumName->asChar(), otherEnum->m_enumName->asChar()) != 0) {
    return false;
  }
  return true;
}
}  // namespace gemfire
