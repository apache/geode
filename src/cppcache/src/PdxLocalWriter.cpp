/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxLocalWriter.cpp
 *
 *  Created on: Nov 3, 2011
 *      Author: npatel
 */

#include "PdxLocalWriter.hpp"
#include "PdxHelper.hpp"
#include "PdxTypeRegistry.hpp"
#include <gfcpp/CacheableEnum.hpp>
#include "GemfireTypeIdsImpl.hpp"

namespace gemfire {

/* adongre  - Coverity II
 * Non-static class member "m_currentOffsetIndex" is not initialized in this
 * constructor nor in any functions that it calls.
 * Non-static class member "m_startPositionOffset" is not initialized in this
 * constructor nor in any functions that it calls.
 */
PdxLocalWriter::PdxLocalWriter()
    : m_dataOutput(NULL),
      m_pdxType(NULLPTR),
      m_startPosition(NULL),
      m_startPositionOffset(0),
      m_domainClassName(NULL),
      m_currentOffsetIndex(0),
      m_pdxClassName(NULL) {  // COVERITY --> 29282 Uninitialized pointer field
  // m_dataOutput = NULL;
  // m_pdxType =NULLPTR;
}

PdxLocalWriter::PdxLocalWriter(DataOutput& output, PdxTypePtr pdxType) {
  m_dataOutput = &output;
  m_pdxType = pdxType;
  m_currentOffsetIndex = 0;
  m_preserveData = NULLPTR;
  m_pdxClassName = NULL;
  if (pdxType != NULLPTR) m_pdxClassName = pdxType->getPdxClassName();
  ;
  initialize();
  /* adongre  - Coverity II
   * CID 29281: Uninitialized pointer field (UNINIT_CTOR)
   * Non-static class member "m_domainClassName" is not initialized in this
   * constructor nor in any functions that it calls.
   * Fix :
   */
  m_domainClassName = NULL;
}

PdxLocalWriter::PdxLocalWriter(DataOutput& dataOutput, PdxTypePtr pdxType,
                               const char* pdxClassName) {
  m_dataOutput = &dataOutput;
  m_pdxType = pdxType;
  m_currentOffsetIndex = 0;
  m_preserveData = NULLPTR;
  m_pdxClassName = pdxClassName;
  initialize();
  /* adongre  - Coverity II
   * CID 29281: Uninitialized pointer field (UNINIT_CTOR)
   * Non-static class member "m_domainClassName" is not initialized in this
   * constructor nor in any functions that it calls.
   * Fix :
   */
  m_domainClassName = NULL;
}

PdxLocalWriter::~PdxLocalWriter() {
  /*if (m_dataOutput != NULL) {
    delete m_dataOutput;
    m_dataOutput = NULL;
  }
  */
  /*if (m_startPosition != NULL) {
    delete m_startPosition;
    m_startPosition = NULL;
  }*/

  /*if (m_domainClassName != NULL) {
    delete m_domainClassName;
    m_domainClassName = NULL;
  }*/
}

void PdxLocalWriter::initialize() {
  if (m_pdxType != NULLPTR) {
    m_currentOffsetIndex = 0;
  }

  // start position, this should start of dataoutput buffer and then use
  // bufferlen
  m_startPosition = m_dataOutput->getBuffer();

  // data has been write
  m_startPositionOffset = m_dataOutput->getBufferLength();

  // Advance cursor to write pdx header
  m_dataOutput->advanceCursor(PdxHelper::PdxHeader);
}

void PdxLocalWriter::addOffset() {
  // bufferLen gives lenght which has been written to DataOutput
  // m_startPositionOffset: from where pdx header length starts
  int bufferLen = m_dataOutput->getBufferLength() - m_startPositionOffset;

  int offset = bufferLen - PdxHelper::PdxHeader;

  m_offsets.push_back(offset);
}

void PdxLocalWriter::endObjectWriting() {
  // Write header for pdx.
  writePdxHeader();
}

void PdxLocalWriter::writePdxHeader() {
  int32 len = calculateLenWithOffsets();
  int32 typeId = m_pdxType->getTypeId();

  const uint8_t* starpos = m_dataOutput->getBuffer() + m_startPositionOffset;
  PdxHelper::writeInt32(const_cast<uint8_t*>(starpos), len);
  PdxHelper::writeInt32(const_cast<uint8_t*>(starpos + 4), typeId);

  writeOffsets(len);
}

void PdxLocalWriter::writeOffsets(int32 len) {
  if (len <= 0xff) {
    for (int i = static_cast<int>(m_offsets.size()) - 1; i > 0; i--) {
      m_dataOutput->write(static_cast<uint8_t>(m_offsets[i]));
    }
  } else if (len <= 0xffff) {
    for (int i = static_cast<int>(m_offsets.size()) - 1; i > 0; i--) {
      m_dataOutput->writeInt(static_cast<uint16_t>(m_offsets[i]));
    }
  } else {
    for (int i = static_cast<int>(m_offsets.size()) - 1; i > 0; i--) {
      m_dataOutput->writeInt(static_cast<uint32_t>(m_offsets[i]));
    }
  }
}

PdxWriterPtr PdxLocalWriter::writeUnreadFields(PdxUnreadFieldsPtr unread) {
  if (isFieldWritingStarted()) {
    throw IllegalStateException(
        "WriteUnreadFields must be called before any other fields are "
        "written.");
  }

  if (unread != NULLPTR) {
    m_preserveData = dynCast<PdxRemotePreservedDataPtr>(unread);
    if (m_preserveData != NULLPTR) {
      m_pdxType =
          PdxTypeRegistry::getPdxType(m_preserveData->getMergedTypeId());
      if (m_pdxType == NULLPTR) {
        // its local type
        // this needs to fix for IPdxTypemapper
        m_pdxType = PdxTypeRegistry::getLocalPdxType(m_pdxClassName);
      }
    } else {
      throw IllegalStateException(
          "PdxLocalWriter::writeUnreadFields: m_preserveData should not be "
          "NULL");
    }
  }
  return PdxWriterPtr(this);
}

int32 PdxLocalWriter::calculateLenWithOffsets() {
  int bufferLen = m_dataOutput->getBufferLength() - m_startPositionOffset;
  int32 totalOffsets = 0;
  if (m_pdxType->getNumberOfVarLenFields() > 0) {
    totalOffsets = m_pdxType->getNumberOfVarLenFields() -
                   1;  // for first var len no need to append offset
  }
  int32 totalLen = bufferLen - PdxHelper::PdxHeader + totalOffsets;

  if (totalLen <= 0xff) {  // 1 byte
    return totalLen;
  } else if (totalLen + totalOffsets <= 0xffff) {  // 2 byte
    return totalLen + totalOffsets;
  } else {  // 4 byte
    return totalLen + totalOffsets * 3;
  }
}

bool PdxLocalWriter::isFieldWritingStarted() { return true; }

PdxWriterPtr PdxLocalWriter::writeChar(const char* fieldName, char value) {
  m_dataOutput->writeChar(static_cast<uint16_t>(value));
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeWideChar(const char* fieldName,
                                           wchar_t value) {
  m_dataOutput->writeChar(static_cast<uint16_t>(value));
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeBoolean(const char* fieldName, bool value) {
  m_dataOutput->writeBoolean(value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeByte(const char* fieldName, int8_t value) {
  m_dataOutput->write(value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeShort(const char* fieldName, int16_t value) {
  m_dataOutput->writeInt(value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeInt(const char* fieldName, int32_t value) {
  m_dataOutput->writeInt(value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeLong(const char* fieldName, int64_t value) {
  m_dataOutput->writeInt(value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeFloat(const char* fieldName, float value) {
  m_dataOutput->writeFloat(value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeDouble(const char* fieldName, double value) {
  m_dataOutput->writeDouble(value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeDate(const char* fieldName,
                                       CacheableDatePtr date) {
  // m_dataOutput->writeObject(date.ptr());
  if (date != NULLPTR) {
    date->toData(*m_dataOutput);
  } else {
    m_dataOutput->writeInt(static_cast<uint64_t>(-1L));
  }
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeString(const char* fieldName,
                                         const char* value) {
  addOffset();
  if (value == NULL) {
    m_dataOutput->write(
        static_cast<int8_t>(GemfireTypeIds::CacheableNullString));
  } else {
    int32_t len = DataOutput::getEncodedLength(value);
    if (len > 0xffff) {
      // write HugUTF
      m_dataOutput->write(
          static_cast<int8_t>(GemfireTypeIds::CacheableStringHuge));
      m_dataOutput->writeUTFHuge(value);
    } else {
      // Write normal UTF
      m_dataOutput->write(static_cast<int8_t>(GemfireTypeIds::CacheableString));
      m_dataOutput->writeUTF(value);
    }
  }
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeWideString(const char* fieldName,
                                             const wchar_t* value) {
  addOffset();
  if (value == NULL) {
    m_dataOutput->write(
        static_cast<int8_t>(GemfireTypeIds::CacheableNullString));
  } else {
    int32_t len = DataOutput::getEncodedLength(value);
    if (len > 0xffff) {
      // write HugUTF
      m_dataOutput->write(
          static_cast<int8_t>(GemfireTypeIds::CacheableStringHuge));
      m_dataOutput->writeUTFHuge(value);
    } else {
      // Write normal UTF
      m_dataOutput->write(static_cast<int8_t>(GemfireTypeIds::CacheableString));
      m_dataOutput->writeUTF(value);
    }
  }
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeStringwithoutOffset(const char* value) {
  if (value == NULL) {
    m_dataOutput->write(
        static_cast<int8_t>(GemfireTypeIds::CacheableNullString));
  } else {
    int32_t len = DataOutput::getEncodedLength(value);
    if (len > 0xffff) {
      // write HugUTF
      m_dataOutput->write(
          static_cast<int8_t>(GemfireTypeIds::CacheableStringHuge));
      m_dataOutput->writeUTFHuge(value);
    } else {
      // Write normal UTF
      m_dataOutput->write(static_cast<int8_t>(GemfireTypeIds::CacheableString));
      m_dataOutput->writeUTF(value);
    }
  }
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeWideStringwithoutOffset(
    const wchar_t* value) {
  if (value == NULL) {
    m_dataOutput->write(
        static_cast<int8_t>(GemfireTypeIds::CacheableNullString));
  } else {
    int32_t len = DataOutput::getEncodedLength(value);
    if (len > 0xffff) {
      // write HugUTF
      m_dataOutput->write(
          static_cast<int8_t>(GemfireTypeIds::CacheableStringHuge));
      m_dataOutput->writeUTFHuge(value);
    } else {
      // Write normal UTF
      m_dataOutput->write(static_cast<int8_t>(GemfireTypeIds::CacheableString));
      m_dataOutput->writeUTF(value);
    }
  }
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeStringArray(const char* fieldName,
                                              char** array, int length) {
  addOffset();
  if (array == NULL) {
    m_dataOutput->write(static_cast<int8_t>(-1));
    // WriteByte(-1);
  } else {
    m_dataOutput->writeArrayLen(length);
    for (int i = 0; i < length; i++) {
      writeStringwithoutOffset(array[i]);
    }
  }
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeWideStringArray(const char* fieldName,
                                                  wchar_t** array, int length) {
  addOffset();
  if (array == NULL) {
    m_dataOutput->write(static_cast<int8_t>(-1));
  } else {
    m_dataOutput->writeArrayLen(length);
    for (int i = 0; i < length; i++) {
      writeWideStringwithoutOffset(array[i]);
    }
  }
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeObject(const char* fieldName,
                                         SerializablePtr value) {
  addOffset();
  CacheableEnumPtr enumValPtr = NULLPTR;
  CacheableObjectArrayPtr objArrPtr = NULLPTR;
  /*if (value != NULLPTR) {
    try {
      enumValPtr = dynCast<CacheableEnumPtr>(value);
    }
    catch (const ClassCastException&) {
      //ignore
    }
  }*/

  if (value != NULLPTR &&
      value->typeId() == static_cast<int8_t>(GemfireTypeIds::CacheableEnum)) {
    enumValPtr = dynCast<CacheableEnumPtr>(value);
  }

  if (enumValPtr != NULLPTR) {
    enumValPtr->toData(*m_dataOutput);
  } else {
    if (value != NULLPTR &&
        value->typeId() == GemfireTypeIds::CacheableObjectArray) {
      objArrPtr = dynCast<CacheableObjectArrayPtr>(value);
      m_dataOutput->write(
          static_cast<int8_t>(GemfireTypeIds::CacheableObjectArray));
      m_dataOutput->writeArrayLen(objArrPtr->length());
      m_dataOutput->write(static_cast<int8_t>(GemfireTypeIdsImpl::Class));

      _VectorOfCacheable::Iterator iter = objArrPtr->begin();
      PdxSerializablePtr actualObjPtr = dynCast<PdxSerializablePtr>(*iter);

      m_dataOutput->write(
          static_cast<int8_t>(GemfireTypeIds::CacheableASCIIString));
      m_dataOutput->writeASCII(actualObjPtr->getClassName());

      for (; iter != objArrPtr->end(); ++iter) {
        m_dataOutput->writeObject(*iter);
      }
    } else {
      m_dataOutput->writeObject(value);
    }
  }
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeBooleanArray(const char* fieldName,
                                               bool* array, int length) {
  addOffset();
  writeObject(array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeCharArray(const char* fieldName, char* array,
                                            int length) {
  addOffset();
  writePdxCharArray(array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeWideCharArray(const char* fieldName,
                                                wchar_t* array, int length) {
  addOffset();
  writeObject(array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeByteArray(const char* fieldName,
                                            int8_t* array, int length) {
  addOffset();
  writeObject(array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeShortArray(const char* fieldName,
                                             int16_t* array, int length) {
  addOffset();
  writeObject(array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeIntArray(const char* fieldName,
                                           int32_t* array, int length) {
  addOffset();
  writeObject(array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeLongArray(const char* fieldName,
                                            int64_t* array, int length) {
  addOffset();
  writeObject(array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeFloatArray(const char* fieldName,
                                             float* array, int length) {
  addOffset();
  writeObject(array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeDoubleArray(const char* fieldName,
                                              double* array, int length) {
  addOffset();
  writeObject(array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeObjectArray(const char* fieldName,
                                              CacheableObjectArrayPtr array) {
  addOffset();
  if (array != NULLPTR) {
    array->toData(*m_dataOutput);
  } else {
    m_dataOutput->write(static_cast<int8_t>(-1));
  }
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::writeArrayOfByteArrays(const char* fieldName,
                                                    int8_t** byteArrays,
                                                    int arrayLength,
                                                    int* elementLength) {
  addOffset();
  if (byteArrays != NULL) {
    m_dataOutput->writeArrayLen(arrayLength);
    for (int i = 0; i < arrayLength; i++) {
      m_dataOutput->writeBytes(byteArrays[i], elementLength[i]);
    }
  } else {
    m_dataOutput->write(static_cast<int8_t>(-1));
  }

  return PdxWriterPtr(this);
}

PdxWriterPtr PdxLocalWriter::markIdentityField(const char* fieldName) {
  return PdxWriterPtr(this);
}

uint8_t* PdxLocalWriter::getPdxStream(int& pdxLen) {
  uint8_t* stPos =
      const_cast<uint8_t*>(m_dataOutput->getBuffer()) + m_startPositionOffset;
  int len = PdxHelper::readInt32 /*readByte*/ (stPos);
  pdxLen = len;
  // ignore len and typeid
  return m_dataOutput->getBufferCopyFrom(stPos + 8, len);
}

void PdxLocalWriter::writeByte(int8_t byte) { m_dataOutput->write(byte); }
}  // namespace gemfire
