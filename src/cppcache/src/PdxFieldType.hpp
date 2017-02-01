#pragma once

#ifndef GEODE_PDXFIELDTYPE_H_
#define GEODE_PDXFIELDTYPE_H_

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

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Serializable.hpp>
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include <string>

namespace apache {
namespace geode {
namespace client {
class PdxFieldType;
typedef SharedPtr<PdxFieldType> PdxFieldTypePtr;
class CPPCACHE_EXPORT PdxFieldType : public Serializable {
 private:
  std::string m_fieldName;
  std::string m_className;
  uint8_t m_typeId;
  uint32_t m_sequenceId;

  bool m_isVariableLengthType;
  bool m_isIdentityField;
  int32 m_fixedSize;
  int32 m_varLenFieldIdx;

  int32 m_vlOffsetIndex;
  int32 m_relativeOffset;

  int32 getFixedTypeSize() const;

 public:
  PdxFieldType(const char* fieldName, const char* className, uint8_t typeId,
               int32 sequenceId, bool isVariableLengthType, int32 fixedSize,
               int32 varLenFieldIdx);

  PdxFieldType();

  inline const char* getFieldName() { return m_fieldName.c_str(); }

  inline const char* getClassName() { return m_className.c_str(); }

  inline uint8_t getTypeId() { return m_typeId; }

  inline uint8_t getSequenceId() { return m_sequenceId; }

  inline bool IsVariableLengthType() { return m_isVariableLengthType; }

  bool getIdentityField() const { return m_isIdentityField; }

  int32 getVarLenFieldIdx() const { return m_varLenFieldIdx; }

  void setVarLenOffsetIndex(int32 value) { m_vlOffsetIndex = value; }

  void setRelativeOffset(int32 value) { m_relativeOffset = value; }

  int32 getFixedSize() const { return m_fixedSize; }
  void setIdentityField(bool identityField) {
    m_isIdentityField = identityField;
  }

  // TODO:add more getters for the remaining members.

  virtual void toData(DataOutput& output) const;

  virtual Serializable* fromData(DataInput& input);

  virtual int32_t classId() const { return m_typeId; }

  virtual uint32_t objectSize() const {
    uint32_t size = sizeof(PdxFieldType);
    size += static_cast<uint32_t>(m_className.length());
    size += static_cast<uint32_t>(m_fieldName.length());
    return size;
  }

  CacheableStringPtr toString() const;

  virtual ~PdxFieldType();

  bool equals(PdxFieldTypePtr otherObj);

  int32 getVarLenOffsetIndex() const { return m_vlOffsetIndex; }

  int32 getRelativeOffset() const { return m_relativeOffset; }
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_PDXFIELDTYPE_H_
