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
/*
 * PdxFieldType.cpp
 *
 *  Created on: Nov 3, 2011
 *      Author: npatel
 */

#include "PdxFieldType.hpp"
#include "GemfireTypeIdsImpl.hpp"
#include "PdxTypes.hpp"
#include <gfcpp/PdxFieldTypes.hpp>
//#include <malloc.h>

#include "ace/OS.h"

namespace gemfire {

PdxFieldType::PdxFieldType() : Serializable() {
  // m_fieldName = NULL;
  // m_className = NULL;
  m_typeId = 0;
  m_sequenceId = 0;
  m_isVariableLengthType = false;
  m_fixedSize = 0;
  m_varLenFieldIdx = 0;
  m_isIdentityField = false;
  m_relativeOffset = 0;
  m_vlOffsetIndex = 0;
}

PdxFieldType::PdxFieldType(const char* fieldName, const char* className,
                           uint8_t typeId, int32 sequenceId,
                           bool isVariableLengthType, int32 fixedSize,
                           int32 varLenFieldIdx)
    : Serializable() {
  m_fieldName = fieldName;
  m_className = className;
  m_typeId = typeId;
  m_sequenceId = sequenceId;  // start with 0
  m_isVariableLengthType = isVariableLengthType;
  m_fixedSize = fixedSize;
  m_varLenFieldIdx = varLenFieldIdx;  // start with 0
  m_isIdentityField = false;
  m_relativeOffset = 0;
  m_vlOffsetIndex = 0;
}

PdxFieldType::~PdxFieldType() {}

void PdxFieldType::toData(DataOutput& output) const {
  // LOGINFO("DEBUG:PdxFieldType::toData:LN_28");
  output.write(static_cast<int8_t>(GemfireTypeIds::CacheableASCIIString));
  output.writeUTF(m_fieldName.c_str());
  output.writeInt(m_sequenceId);
  output.writeInt(m_varLenFieldIdx);
  output.write(m_typeId);

  output.writeInt(m_relativeOffset);
  output.writeInt(m_vlOffsetIndex);
  output.writeBoolean(m_isIdentityField);
}

Serializable* PdxFieldType::fromData(DataInput& input) {
  int8_t typeId;
  input.read(&typeId);
  char* fname = NULL;
  input.readUTF(&fname);
  m_fieldName = fname;
  input.freeUTFMemory(fname);  // freeing fname

  input.readInt(&m_sequenceId);
  input.readInt(&m_varLenFieldIdx);
  input.read(&m_typeId);
  input.readInt(&m_relativeOffset);
  input.readInt(&m_vlOffsetIndex);
  input.readBoolean(&m_isIdentityField);
  m_fixedSize = getFixedTypeSize();
  if (m_fixedSize != -1) {
    m_isVariableLengthType = false;
  } else {
    m_isVariableLengthType = true;
  }
  return this;
}

bool PdxFieldType::equals(PdxFieldTypePtr otherObj) {
  if (otherObj == NULLPTR) return false;

  PdxFieldType* otherFieldType = dynamic_cast<PdxFieldType*>(otherObj.ptr());

  if (otherFieldType == NULL) return false;

  if (otherFieldType == this) return true;

  if (otherFieldType->m_fieldName.compare(m_fieldName) == 0 &&
      otherFieldType->m_typeId == m_typeId) {
    return true;
  }

  return false;
}

int32_t PdxFieldType::getFixedTypeSize() const {
  switch (m_typeId) {
    case PdxFieldTypes::BYTE:
    case PdxFieldTypes::BOOLEAN: {
      return PdxTypes::BOOLEAN_SIZE;
    }
    case PdxFieldTypes::SHORT:
    case PdxFieldTypes::CHAR:
      // case gemfire::GemfireTypeIds::CacheableChar: //TODO
      { return PdxTypes::CHAR_SIZE; }
    case PdxFieldTypes::INT:
    case PdxFieldTypes::FLOAT:
      // case DSCODE.ENUM:
      { return PdxTypes::INTEGER_SIZE; }
    case PdxFieldTypes::LONG:
    case PdxFieldTypes::DOUBLE: {
      return PdxTypes::LONG_SIZE;
    }
    case PdxFieldTypes::DATE: {
      return PdxTypes::DATE_SIZE;
    }
    default:
      return -1;
  }
  return -1;
}

CacheableStringPtr PdxFieldType::toString() const {
  char stringBuf[1024];
  /* adongre  - Coverity II
   * CID 29208: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
   * "sprintf" can cause a
   * buffer overflow when done incorrectly. Because sprintf() assumes an
   * arbitrarily long string,
   * callers must be careful not to overflow the actual space of the
   * destination.
   * Use snprintf() instead, or correct precision specifiers.
   * Fix : using ACE_OS::snprintf
   */
  ACE_OS::snprintf(
      stringBuf, 1024,
      " PdxFieldName=%s TypeId=%d VarLenFieldIdx=%d sequenceid=%d\n",
      this->m_fieldName.c_str(), this->m_typeId, this->m_varLenFieldIdx,
      this->m_sequenceId);
  return CacheableString::create(stringBuf);
}
}  // namespace gemfire
