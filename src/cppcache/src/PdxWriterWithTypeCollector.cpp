/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxWriterWithTypeCollector.cpp
 *
 *  Created on: Nov 3, 2011
 *      Author: npatel
 */

#include "PdxWriterWithTypeCollector.hpp"
#include "PdxType.hpp"
#include "PdxHelper.hpp"
#include "PdxTypes.hpp"
#include <gfcpp/PdxFieldTypes.hpp>

namespace gemfire {

PdxWriterWithTypeCollector::PdxWriterWithTypeCollector() {}

PdxWriterWithTypeCollector::PdxWriterWithTypeCollector(
    DataOutput& output, const char* domainClassName)
    : PdxLocalWriter(output, NULL) {
  m_domainClassName = domainClassName;
  initialize();
}

void PdxWriterWithTypeCollector::initialize() {
  m_pdxType = new PdxType(m_domainClassName, true);
}

PdxWriterWithTypeCollector::~PdxWriterWithTypeCollector() {}

void PdxWriterWithTypeCollector::endObjectWriting() {
  // Write header for PDX
  PdxLocalWriter::writePdxHeader();
}

void PdxWriterWithTypeCollector::writeOffsets(int32 len) {
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

int32 PdxWriterWithTypeCollector::calculateLenWithOffsets() {
  int bufferLen = m_dataOutput->getBufferLength() - m_startPositionOffset;
  int32 totalOffsets = 0;
  if (m_offsets.size() > 0) {
    totalOffsets = static_cast<int32>(m_offsets.size()) -
                   1;  // for first var len no need to append offset
  }
  int32 totalLen = bufferLen - PdxHelper::PdxHeader + totalOffsets;

  if (totalLen <= 0xff) {
    return totalLen;
  } else if (totalLen + totalOffsets <= 0xffff) {
    return totalLen + totalOffsets;
  } else {
    return totalLen + totalOffsets * 3;
  }
}

void PdxWriterWithTypeCollector::addOffset() {
  int bufferLen = m_dataOutput->getBufferLength() - m_startPositionOffset;
  int offset = bufferLen - PdxHelper::PdxHeader;

  m_offsets.push_back(offset);
}

bool PdxWriterWithTypeCollector::isFieldWritingStarted() {
  return m_pdxType->getTotalFields() > 0;
}

PdxWriterPtr PdxWriterWithTypeCollector::writeUnreadFields(
    PdxUnreadFieldsPtr unread) {
  PdxLocalWriter::writeUnreadFields(unread);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeChar(const char* fieldName,
                                                   char value) {
  m_pdxType->addFixedLengthTypeField(fieldName, "char", PdxFieldTypes::CHAR,
                                     PdxTypes::CHAR_SIZE);
  PdxLocalWriter::writeChar(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeWideChar(const char* fieldName,
                                                       wchar_t value) {
  m_pdxType->addFixedLengthTypeField(fieldName, "char", PdxFieldTypes::CHAR,
                                     PdxTypes::CHAR_SIZE);
  PdxLocalWriter::writeWideChar(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeBoolean(const char* fieldName,
                                                      bool value) {
  m_pdxType->addFixedLengthTypeField(
      fieldName, "boolean", PdxFieldTypes::BOOLEAN, PdxTypes::BOOLEAN_SIZE);
  PdxLocalWriter::writeBoolean(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeByte(const char* fieldName,
                                                   int8_t value) {
  m_pdxType->addFixedLengthTypeField(fieldName, "byte", PdxFieldTypes::BYTE,
                                     PdxTypes::BYTE_SIZE);
  PdxLocalWriter::writeByte(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeShort(const char* fieldName,
                                                    int16_t value) {
  m_pdxType->addFixedLengthTypeField(fieldName, "short", PdxFieldTypes::SHORT,
                                     PdxTypes::SHORT_SIZE);
  PdxLocalWriter::writeShort(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeInt(const char* fieldName,
                                                  int32_t value) {
  m_pdxType->addFixedLengthTypeField(fieldName, "int", PdxFieldTypes::INT,
                                     PdxTypes::INTEGER_SIZE);
  PdxLocalWriter::writeInt(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeLong(const char* fieldName,
                                                   int64_t value) {
  m_pdxType->addFixedLengthTypeField(fieldName, "long", PdxFieldTypes::LONG,
                                     PdxTypes::LONG_SIZE);
  PdxLocalWriter::writeLong(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeFloat(const char* fieldName,
                                                    float value) {
  m_pdxType->addFixedLengthTypeField(fieldName, "float", PdxFieldTypes::FLOAT,
                                     PdxTypes::FLOAT_SIZE);
  PdxLocalWriter::writeFloat(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeDouble(const char* fieldName,
                                                     double value) {
  m_pdxType->addFixedLengthTypeField(fieldName, "double", PdxFieldTypes::DOUBLE,
                                     PdxTypes::DOUBLE_SIZE);
  PdxLocalWriter::writeDouble(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeDate(const char* fieldName,
                                                   CacheableDatePtr date) {
  m_pdxType->addFixedLengthTypeField(fieldName, "Date", PdxFieldTypes::DATE,
                                     PdxTypes::DATE_SIZE);
  PdxLocalWriter::writeDate(fieldName, date);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeString(const char* fieldName,
                                                     const char* value) {
  m_pdxType->addVariableLengthTypeField(fieldName, "String",
                                        PdxFieldTypes::STRING);
  PdxLocalWriter::writeString(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeWideString(const char* fieldName,
                                                         const wchar_t* value) {
  m_pdxType->addVariableLengthTypeField(fieldName, "String",
                                        PdxFieldTypes::STRING);
  PdxLocalWriter::writeWideString(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeObject(const char* fieldName,
                                                     SerializablePtr value) {
  m_pdxType->addVariableLengthTypeField(fieldName, "Serializable",
                                        PdxFieldTypes::OBJECT);
  PdxLocalWriter::writeObject(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeBooleanArray(
    const char* fieldName, bool* array, int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "bool[]",
                                        PdxFieldTypes::BOOLEAN_ARRAY);
  PdxLocalWriter::writeBooleanArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeCharArray(const char* fieldName,
                                                        char* array,
                                                        int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "char[]",
                                        PdxFieldTypes::CHAR_ARRAY);
  PdxLocalWriter::writeCharArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeWideCharArray(
    const char* fieldName, wchar_t* array, int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "char[]",
                                        PdxFieldTypes::CHAR_ARRAY);
  PdxLocalWriter::writeWideCharArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeByteArray(const char* fieldName,
                                                        int8_t* array,
                                                        int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "byte[]",
                                        PdxFieldTypes::BYTE_ARRAY);
  PdxLocalWriter::writeByteArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeShortArray(const char* fieldName,
                                                         int16_t* array,
                                                         int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "short[]",
                                        PdxFieldTypes::SHORT_ARRAY);
  PdxLocalWriter::writeShortArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeIntArray(const char* fieldName,
                                                       int32_t* array,
                                                       int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "int[]",
                                        PdxFieldTypes::INT_ARRAY);
  PdxLocalWriter::writeIntArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeLongArray(const char* fieldName,
                                                        int64_t* array,
                                                        int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "long[]",
                                        PdxFieldTypes::LONG_ARRAY);
  PdxLocalWriter::writeLongArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeFloatArray(const char* fieldName,
                                                         float* array,
                                                         int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "float[]",
                                        PdxFieldTypes::FLOAT_ARRAY);
  PdxLocalWriter::writeFloatArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeDoubleArray(const char* fieldName,
                                                          double* array,
                                                          int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "double[]",
                                        PdxFieldTypes::DOUBLE_ARRAY);
  PdxLocalWriter::writeDoubleArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeStringArray(const char* fieldName,
                                                          char** array,
                                                          int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "String[]",
                                        PdxFieldTypes::STRING_ARRAY);
  PdxLocalWriter::writeStringArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeWideStringArray(
    const char* fieldName, wchar_t** array, int length) {
  m_pdxType->addVariableLengthTypeField(fieldName, "String[]",
                                        PdxFieldTypes::STRING_ARRAY);
  PdxLocalWriter::writeWideStringArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeObjectArray(
    const char* fieldName, CacheableObjectArrayPtr array) {
  m_pdxType->addVariableLengthTypeField(fieldName, "Object[]",
                                        PdxFieldTypes::OBJECT_ARRAY);
  PdxLocalWriter::writeObjectArray(fieldName, array);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::writeArrayOfByteArrays(
    const char* fieldName, int8_t** byteArrays, int arrayLength,
    int* elementLength) {
  m_pdxType->addVariableLengthTypeField(fieldName, "byte[][]",
                                        PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS);
  PdxLocalWriter::writeArrayOfByteArrays(fieldName, byteArrays, arrayLength,
                                         elementLength);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxWriterWithTypeCollector::markIdentityField(
    const char* fieldName) {
  PdxFieldTypePtr pft = m_pdxType->getPdxField(fieldName);
  if (pft == NULLPTR) {
    throw IllegalStateException(
        "Field, must be written to PdxWriter before calling "
        "MarkIdentityField ");
  }
  pft->setIdentityField(true);
  return PdxWriterPtr(this);
}
}  // namespace gemfire
