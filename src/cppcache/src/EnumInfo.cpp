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
#include "EnumInfo.hpp"
#include "Utils.hpp"
#include "GeodeTypeIdsImpl.hpp"

namespace apache {
namespace geode {
namespace client {

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

void EnumInfo::toData(apache::geode::client::DataOutput &output) const {
  output.writeObject(m_enumClassName);
  output.writeObject(m_enumName);
  output.writeInt(m_ordinal);
}

Serializable *EnumInfo::fromData(apache::geode::client::DataInput &input) {
  input.readObject(m_enumClassName);
  input.readObject(m_enumName);
  input.readInt(&m_ordinal);
  return this;
}

int8_t EnumInfo::DSFID() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::FixedIDByte);
}
}  // namespace client
}  // namespace geode
}  // namespace apache
