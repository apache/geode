/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* PdxReaderWithTypeCollector.cpp
*
*  Created on: Nov 3, 2011
*      Author: npatel
*/

#include "PdxReaderWithTypeCollector.hpp"
#include "PdxTypes.hpp"
#include <gfcpp/PdxFieldTypes.hpp>
#include <ace/OS_NS_stdio.h>

namespace gemfire {

PdxReaderWithTypeCollector::PdxReaderWithTypeCollector(DataInput& dataInput,
                                                       PdxTypePtr pdxType,
                                                       int32_t pdxlen)
    : PdxLocalReader(dataInput, pdxType, pdxlen) {
  m_newPdxType = new PdxType(pdxType->getPdxClassName(), true);
}

PdxReaderWithTypeCollector::PdxReaderWithTypeCollector() {}

PdxReaderWithTypeCollector::~PdxReaderWithTypeCollector() {}

void PdxReaderWithTypeCollector::checkType(const char* fieldName, int8_t typeId,
                                           const char* fieldType) {
  // Check for Empty Field.
  if (fieldName == NULL) {
    throw IllegalStateException("Field name is null");
  }

  PdxFieldTypePtr pft = m_pdxType->getPdxField(fieldName);
  if (pft != NULLPTR) {
    if (typeId != pft->getTypeId()) {
      char excpStr[128] = {0};
      ACE_OS::snprintf(
          excpStr, 128,
          "Expected %s fieldType field but found field of type %s ", fieldType,
          pft->toString()->asChar());
      throw IllegalStateException(excpStr);
    }
  }
}

char PdxReaderWithTypeCollector::readChar(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::CHAR, "char");
  m_newPdxType->addFixedLengthTypeField(fieldName, "char", PdxFieldTypes::CHAR,
                                        PdxTypes::CHAR_SIZE);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readChar()position = %d", position);
  if (position != -1) {
    m_dataInput->advanceCursor(position);
    char retVal = PdxLocalReader::readChar(fieldName);
    m_dataInput->rewindCursor(position + PdxTypes::CHAR_SIZE);
    return retVal;
  } else {
    return 0;
  }
}

wchar_t PdxReaderWithTypeCollector::readWideChar(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::CHAR, "char");
  m_newPdxType->addFixedLengthTypeField(fieldName, "char", PdxFieldTypes::CHAR,
                                        PdxTypes::CHAR_SIZE);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readWideChar()position = %d", position);
  if (position != -1) {
    m_dataInput->advanceCursor(position);
    wchar_t retVal = PdxLocalReader::readWideChar(fieldName);
    m_dataInput->rewindCursor(position + PdxTypes::CHAR_SIZE);
    return retVal;
  } else {
    return 0;
  }
}

bool PdxReaderWithTypeCollector::readBoolean(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::BOOLEAN, "boolean");
  m_newPdxType->addFixedLengthTypeField(
      fieldName, "boolean", PdxFieldTypes::BOOLEAN, PdxTypes::BOOLEAN_SIZE);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readBoolean():position = %d", position);
  if (position != -1) {
    m_dataInput->advanceCursor(position);
    bool retVal = PdxLocalReader::readBoolean(fieldName);
    m_dataInput->rewindCursor(position + PdxTypes::BOOLEAN_SIZE);
    return retVal;
  } else {
    return 0;
  }
}

int8_t PdxReaderWithTypeCollector::readByte(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::BYTE, "byte");
  m_newPdxType->addFixedLengthTypeField(fieldName, "byte", PdxFieldTypes::BYTE,
                                        PdxTypes::BYTE_SIZE);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readByte(): position = %d", position);
  if (position != -1) {
    m_dataInput->advanceCursor(position);
    int8_t retVal;
    retVal = PdxLocalReader::readByte(fieldName);
    m_dataInput->rewindCursor(position + PdxTypes::BYTE_SIZE);
    return retVal;
  } else {
    return 0;
  }
}

int16_t PdxReaderWithTypeCollector::readShort(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::SHORT, "short");
  m_newPdxType->addFixedLengthTypeField(
      fieldName, "short", PdxFieldTypes::SHORT, PdxTypes::SHORT_SIZE);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readShort(): position = %d", position);
  if (position != -1) {
    int16_t value;
    m_dataInput->advanceCursor(position);
    value = PdxLocalReader::readShort(fieldName);
    m_dataInput->rewindCursor(position + PdxTypes::SHORT_SIZE);
    return value;
  } else {
    return 0;
  }
}

int32_t PdxReaderWithTypeCollector::readInt(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::INT, "int");
  m_newPdxType->addFixedLengthTypeField(fieldName, "int", PdxFieldTypes::INT,
                                        PdxTypes::INTEGER_SIZE);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readInt():position = %d", position);
  if (position != -1) {
    int32_t value;
    m_dataInput->advanceCursor(position);
    value = PdxLocalReader::readInt(fieldName);
    m_dataInput->rewindCursor(position + PdxTypes::INTEGER_SIZE);
    return value;
  } else {
    return 0;
  }
}

int64_t PdxReaderWithTypeCollector::readLong(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::LONG, "long");
  m_newPdxType->addFixedLengthTypeField(fieldName, "long", PdxFieldTypes::LONG,
                                        PdxTypes::LONG_SIZE);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readLong(): position = %d", position);
  if (position != -1) {
    int64_t value;
    m_dataInput->advanceCursor(position);
    value = PdxLocalReader::readLong(fieldName);
    m_dataInput->rewindCursor(position + PdxTypes::LONG_SIZE);
    return value;
  } else {
    return 0;
  }
}

float PdxReaderWithTypeCollector::readFloat(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::FLOAT, "float");
  m_newPdxType->addFixedLengthTypeField(
      fieldName, "float", PdxFieldTypes::FLOAT, PdxTypes::FLOAT_SIZE);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readFloat():position = %d", position);
  if (position != -1) {
    float value;
    m_dataInput->advanceCursor(position);
    value = PdxLocalReader::readFloat(fieldName);
    m_dataInput->rewindCursor(position + PdxTypes::FLOAT_SIZE);
    return value;
  } else {
    return 0.0f;
  }
}

double PdxReaderWithTypeCollector::readDouble(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::DOUBLE, "double");
  m_newPdxType->addFixedLengthTypeField(
      fieldName, "double", PdxFieldTypes::DOUBLE, PdxTypes::DOUBLE_SIZE);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readDouble():position = %d", position);
  if (position != -1) {
    double value;
    m_dataInput->advanceCursor(position);
    value = PdxLocalReader::readDouble(fieldName);
    m_dataInput->rewindCursor(position + PdxTypes::DOUBLE_SIZE);
    return value;
  } else {
    return 0.0;
  }
}

char* PdxReaderWithTypeCollector::readString(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::STRING, "String");
  m_newPdxType->addVariableLengthTypeField(fieldName, "String",
                                           PdxFieldTypes::STRING);
  int32_t position = m_pdxType->getFieldPosition(
      fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readString():position = %d", position);

  if (position != -1) {
    char* str;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    str = PdxLocalReader::readString(fieldName);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return str;
  } else {
    static char emptyString[] = {static_cast<char>(0)};
    return emptyString;
  }
}

wchar_t* PdxReaderWithTypeCollector::readWideString(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::STRING, "String");
  m_newPdxType->addVariableLengthTypeField(fieldName, "String",
                                           PdxFieldTypes::STRING);
  int32_t position = m_pdxType->getFieldPosition(
      fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readWideString():position = %d",
           position);

  if (position != -1) {
    wchar_t* str;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    str = PdxLocalReader::readWideString(fieldName);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return str;
  } else {
    static wchar_t emptyString[] = {static_cast<wchar_t>(0)};
    return emptyString;
  }
}

SerializablePtr PdxReaderWithTypeCollector::readObject(const char* fieldName) {
  // field is collected after reading
  checkType(fieldName, PdxFieldTypes::OBJECT, "Serializable");
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readObject():position = %d", position);
  if (position != -1) {
    SerializablePtr ptr;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    ptr = PdxLocalReader::readObject(fieldName);
    m_newPdxType->addVariableLengthTypeField(fieldName, "Serializable",
                                             PdxFieldTypes::OBJECT);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return ptr;
  } else {
    return NULLPTR;
  }
}

char* PdxReaderWithTypeCollector::readCharArray(const char* fieldName,
                                                int32_t& length) {
  checkType(fieldName, PdxFieldTypes::CHAR_ARRAY, "char[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "char[]",
                                           PdxFieldTypes::CHAR_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readCharArray():position = %d",
           position);
  if (position != -1) {
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    char* retVal = PdxLocalReader::readCharArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return retVal;
  } else {
    return NULL;
  }
}

wchar_t* PdxReaderWithTypeCollector::readWideCharArray(const char* fieldName,
                                                       int32_t& length) {
  checkType(fieldName, PdxFieldTypes::CHAR_ARRAY, "char[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "char[]",
                                           PdxFieldTypes::CHAR_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readCharArray():position = %d",
           position);
  if (position != -1) {
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    wchar_t* retVal = PdxLocalReader::readWideCharArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return retVal;
  } else {
    return NULL;
  }
}

bool* PdxReaderWithTypeCollector::readBooleanArray(const char* fieldName,
                                                   int32_t& length) {
  checkType(fieldName, PdxFieldTypes::BOOLEAN_ARRAY, "boolean[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "boolean[]",
                                           PdxFieldTypes::BOOLEAN_ARRAY);
  LOGDEBUG("NIL:293: PdxReaderWithTypeCollector::readBooleanArray Test-1");
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG(
      "NIL:293: PdxReaderWithTypeCollector::readBooleanArray(): Position =%d ",
      position);

  if (position != -1) {
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    bool* retVal = PdxLocalReader::readBooleanArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return retVal;
  }
  return NULL;
}

int8_t* PdxReaderWithTypeCollector::readByteArray(const char* fieldName,
                                                  int32_t& length) {
  checkType(fieldName, PdxFieldTypes::BYTE_ARRAY, "byte[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "byte[]",
                                           PdxFieldTypes::BYTE_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readByteArray(): position = %d",
           position);
  if (position != -1) {
    int8_t* byteArrptr;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    byteArrptr = PdxLocalReader::readByteArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return byteArrptr;
  } else {
    return NULL;
  }
}

int16_t* PdxReaderWithTypeCollector::readShortArray(const char* fieldName,
                                                    int32_t& length) {
  checkType(fieldName, PdxFieldTypes::SHORT_ARRAY, "short[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "short[]",
                                           PdxFieldTypes::SHORT_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readShortArray():position = %d",
           position);
  if (position != -1) {
    int16_t* shortArrptr;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    shortArrptr = PdxLocalReader::readShortArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return shortArrptr;
  } else {
    return NULL;
  }
}

int32_t* PdxReaderWithTypeCollector::readIntArray(const char* fieldName,
                                                  int32_t& length) {
  checkType(fieldName, PdxFieldTypes::INT_ARRAY, "int[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "int[]",
                                           PdxFieldTypes::INT_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readIntArray():position = %d",
           position);
  if (position != -1) {
    int32_t* intArrayptr;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    intArrayptr = PdxLocalReader::readIntArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return intArrayptr;
  } else {
    return NULL;
  }
}

int64_t* PdxReaderWithTypeCollector::readLongArray(const char* fieldName,
                                                   int32_t& length) {
  checkType(fieldName, PdxFieldTypes::LONG_ARRAY, "long[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "long[]",
                                           PdxFieldTypes::LONG_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readLongArray():position = %d",
           position);
  if (position != -1) {
    int64_t* longArrptr;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    longArrptr = PdxLocalReader::readLongArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return longArrptr;
  } else {
    return NULL;
  }
}

float* PdxReaderWithTypeCollector::readFloatArray(const char* fieldName,
                                                  int32_t& length) {
  checkType(fieldName, PdxFieldTypes::FLOAT_ARRAY, "float[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "float[]",
                                           PdxFieldTypes::FLOAT_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readFloatArray(): position = %d",
           position);
  if (position != -1) {
    float* floatArrptr;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    floatArrptr = PdxLocalReader::readFloatArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return floatArrptr;
  } else {
    return NULL;
  }
}

double* PdxReaderWithTypeCollector::readDoubleArray(const char* fieldName,
                                                    int32_t& length) {
  checkType(fieldName, PdxFieldTypes::DOUBLE_ARRAY, "double[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "double[]",
                                           PdxFieldTypes::DOUBLE_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readDoubleArray():position = %d",
           position);
  if (position != -1) {
    double* doubleArrptr;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    doubleArrptr = PdxLocalReader::readDoubleArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return doubleArrptr;
  } else {
    return NULL;
  }
}

char** PdxReaderWithTypeCollector::readStringArray(const char* fieldName,
                                                   int32_t& length) {
  checkType(fieldName, PdxFieldTypes::STRING_ARRAY, "String[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "String[]",
                                           PdxFieldTypes::STRING_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readStringArray(): position = %d",
           position);
  if (position != -1) {
    char** strArray;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    strArray = PdxLocalReader::readStringArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return strArray;
  } else {
    return NULL;
  }
}

wchar_t** PdxReaderWithTypeCollector::readWideStringArray(const char* fieldName,
                                                          int32_t& length) {
  checkType(fieldName, PdxFieldTypes::STRING_ARRAY, "String[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "String[]",
                                           PdxFieldTypes::STRING_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readWideStringArray(): position = %d",
           position);
  if (position != -1) {
    wchar_t** strArray;
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    strArray = PdxLocalReader::readWideStringArray(fieldName, length);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return strArray;
  } else {
    return NULL;
  }
}

CacheableObjectArrayPtr PdxReaderWithTypeCollector::readObjectArray(
    const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::OBJECT_ARRAY, "Object[]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "Object[]",
                                           PdxFieldTypes::OBJECT_ARRAY);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readObjectArray():position = %d",
           position);
  if (position != -1) {
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    CacheableObjectArrayPtr retVal = PdxLocalReader::readObjectArray(fieldName);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return retVal;
  } else {
    return NULLPTR;
  }
}

int8_t** PdxReaderWithTypeCollector::readArrayOfByteArrays(
    const char* fieldName, int32_t& arrayLength, int32_t** elementLength) {
  checkType(fieldName, PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS, "byte[][]");
  m_newPdxType->addVariableLengthTypeField(fieldName, "byte[][]",
                                           PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readArrayOfByteArrays() position = %d",
           position);
  if (position != -1) {
    m_dataInput->advanceCursor(position);
    const uint8_t* startLoc = m_dataInput->currentBufferPosition();
    int8_t** retVal = PdxLocalReader::readArrayOfByteArrays(
        fieldName, arrayLength, elementLength);
    int32_t strSize =
        static_cast<int32_t>(m_dataInput->currentBufferPosition() - startLoc);
    m_dataInput->rewindCursor(strSize + position);
    startLoc = NULL;
    return retVal;
  } else {
    return NULL;
  }
}

CacheableDatePtr PdxReaderWithTypeCollector::readDate(const char* fieldName) {
  checkType(fieldName, PdxFieldTypes::DATE, "Date");
  m_newPdxType->addFixedLengthTypeField(fieldName, "Date", PdxFieldTypes::DATE,
                                        PdxTypes::DATE_SIZE);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readDate() position = %d", position);
  if (position != -1) {
    m_dataInput->advanceCursor(position);
    CacheableDatePtr retVal = PdxLocalReader::readDate(fieldName);
    m_dataInput->rewindCursor(position + PdxTypes::DATE_SIZE);
    return retVal;
  } else {
    return NULLPTR;
  }
}

void PdxReaderWithTypeCollector::readCollection(
    const char* fieldName, CacheableArrayListPtr& collection) {
  checkType(fieldName, gemfire::GemfireTypeIds::CacheableArrayList,
            "Collection");
  m_newPdxType->addVariableLengthTypeField(
      fieldName, "Collection", gemfire::GemfireTypeIds::CacheableArrayList);
  int position = m_pdxType->getFieldPosition(fieldName, m_offsetsBuffer,
                                             m_offsetSize, m_serializedLength);
  LOGDEBUG("PdxReaderWithTypeCollector::readCollection() position = %d",
           position);
  if (position != -1) {
    m_dataInput->advanceCursor(position);
    PdxLocalReader::readCollection(fieldName, collection);
    m_dataInput->rewindCursor(position);
  } else {
    collection = NULLPTR;
  }
}
}  // namespace gemfire
