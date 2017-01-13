/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* PdxLocalReader.cpp
* Created on: Nov 3, 2011
*      Author: npatel
*/

#include "PdxLocalReader.hpp"
#include "PdxTypeRegistry.hpp"

namespace gemfire {

PdxLocalReader::PdxLocalReader()
    : m_dataInput(NULL),
      m_startBuffer(NULL),
      m_startPosition(0),
      m_serializedLength(0),
      m_serializedLengthWithOffsets(0),
      m_offsetSize(0),
      m_offsetsBuffer(NULL),
      m_isDataNeedToPreserve(false),
      m_localToRemoteMap(NULL),
      m_remoteToLocalMap(NULL),
      m_remoteToLocalMapSize(0) {}

PdxLocalReader::PdxLocalReader(DataInput& input, PdxTypePtr remoteType,
                               int32_t pdxLen) {
  m_dataInput = &input;
  m_pdxType = remoteType;
  m_serializedLengthWithOffsets = pdxLen;

  m_localToRemoteMap = remoteType->getLocalToRemoteMap();
  m_remoteToLocalMap = remoteType->getRemoteToLocalMap();
  m_remoteToLocalMapSize = remoteType->getTotalFields();

  m_pdxRemotePreserveData = new PdxRemotePreservedData();
  m_isDataNeedToPreserve = true;
  initialize();
}

PdxLocalReader::~PdxLocalReader() {}

void PdxLocalReader::resettoPdxHead() {
  int32_t pdxHeadOffset = static_cast<int32_t>(
      m_dataInput->currentBufferPosition() - m_startBuffer);
  m_dataInput->rewindCursor(pdxHeadOffset);
}

void PdxLocalReader::initialize() {
  m_startBuffer = const_cast<uint8_t*>(m_dataInput->currentBufferPosition());
  m_startPosition = m_dataInput->getBytesRead();  // number of bytes read in
                                                  // c++;

  if (m_serializedLengthWithOffsets <= 0xff) {
    m_offsetSize = 1;
  } else if (m_serializedLengthWithOffsets <= 0xffff) {
    m_offsetSize = 2;
  } else {
    m_offsetSize = 4;
  }

  if (m_pdxType->getNumberOfVarLenFields() > 0) {
    m_serializedLength =
        m_serializedLengthWithOffsets -
        ((m_pdxType->getNumberOfVarLenFields() - 1) * m_offsetSize);
  } else {
    m_serializedLength = m_serializedLengthWithOffsets;
  }
  m_offsetsBuffer = m_startBuffer + m_serializedLength;
}

void PdxLocalReader::MoveStream() {
  // this will reset unmaged datainput as well
  m_dataInput->reset(m_startPosition + m_serializedLengthWithOffsets);
}

void PdxLocalReader::checkEmptyFieldName(const char* fieldName) {
  if (fieldName == NULL) {
    throw IllegalStateException("Field name is null");
  }
}

char PdxLocalReader::readChar(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  uint16_t value = 0;
  m_dataInput->readInt(&value);
  return (static_cast<char>(value));
}

wchar_t PdxLocalReader::readWideChar(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  uint16_t value = 0;
  m_dataInput->readInt(&value);
  return static_cast<wchar_t>(value);
}

bool PdxLocalReader::readBoolean(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  bool value;
  m_dataInput->readBoolean(&value);
  return value;
}

int8_t PdxLocalReader::readByte(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  int8_t value;
  m_dataInput->read(&value);
  return value;
}

int16_t PdxLocalReader::readShort(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  int16_t value;
  m_dataInput->readInt(&value);
  return value;
}

int32_t PdxLocalReader::readInt(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  int32_t value;
  m_dataInput->readInt(&value);
  return value;
}

int64_t PdxLocalReader::readLong(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  int64_t value;
  m_dataInput->readInt(&value);
  return value;
}

float PdxLocalReader::readFloat(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  float value;
  m_dataInput->readFloat(&value);
  return value;
}

double PdxLocalReader::readDouble(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  double value;
  m_dataInput->readDouble(&value);
  return value;
}

char* PdxLocalReader::readString(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  char* str;
  m_dataInput->readString(&str);
  return str;
}

wchar_t* PdxLocalReader::readWideString(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  wchar_t* str;
  m_dataInput->readWideString(&str);
  return str;
}

SerializablePtr PdxLocalReader::readObject(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  SerializablePtr ptr;
  m_dataInput->readObject(ptr);
  if (ptr != NULLPTR) {
    return ptr;
  } else {
    return NULLPTR;
  }
}

char* PdxLocalReader::readCharArray(const char* fieldName,
                                    int32_t& length) {  // TODO:: need to return
                                                        // Length to user for
                                                        // all primitive arrays
  checkEmptyFieldName(fieldName);
  char* charArray = NULL;
  m_dataInput->readCharArray(&charArray, length);
  return charArray;
}

wchar_t* PdxLocalReader::readWideCharArray(
    const char* fieldName,
    int32_t& length) {  // TODO:: need to return Length to user for all
                        // primitive arrays
  checkEmptyFieldName(fieldName);
  wchar_t* charArray = NULL;
  m_dataInput->readWideCharArray(&charArray, length);
  return charArray;
}
bool* PdxLocalReader::readBooleanArray(const char* fieldName, int32_t& length) {
  checkEmptyFieldName(fieldName);
  bool* boolArray = NULL;
  m_dataInput->readBooleanArray(&boolArray, length);
  return boolArray;
}

int8_t* PdxLocalReader::readByteArray(const char* fieldName, int32_t& length) {
  checkEmptyFieldName(fieldName);
  int8_t* byteArray = NULL;
  m_dataInput->readByteArray(&byteArray, length);
  return byteArray;
}

int16_t* PdxLocalReader::readShortArray(const char* fieldName,
                                        int32_t& length) {
  checkEmptyFieldName(fieldName);
  int16_t* shortArray = NULL;
  m_dataInput->readShortArray(&shortArray, length);
  return shortArray;
}

int32_t* PdxLocalReader::readIntArray(const char* fieldName, int32_t& length) {
  checkEmptyFieldName(fieldName);
  int32_t* intArray = NULL;
  m_dataInput->readIntArray(&intArray, length);
  return intArray;
}

int64_t* PdxLocalReader::readLongArray(const char* fieldName, int32_t& length) {
  checkEmptyFieldName(fieldName);
  int64_t* longArray = NULL;
  m_dataInput->readLongArray(&longArray, length);
  return longArray;
}

float* PdxLocalReader::readFloatArray(const char* fieldName, int32_t& length) {
  checkEmptyFieldName(fieldName);
  float* floatArray = NULL;
  m_dataInput->readFloatArray(&floatArray, length);
  return floatArray;
}

double* PdxLocalReader::readDoubleArray(const char* fieldName,
                                        int32_t& length) {
  checkEmptyFieldName(fieldName);
  double* doubleArray = NULL;
  m_dataInput->readDoubleArray(&doubleArray, length);
  return doubleArray;
}

char** PdxLocalReader::readStringArray(const char* fieldName, int32_t& length) {
  checkEmptyFieldName(fieldName);
  char** stringArray = NULL;
  m_dataInput->readStringArray(&stringArray, length);
  return stringArray;
}

wchar_t** PdxLocalReader::readWideStringArray(const char* fieldName,
                                              int32_t& length) {
  checkEmptyFieldName(fieldName);
  wchar_t** stringArray = NULL;
  m_dataInput->readWideStringArray(&stringArray, length);
  return stringArray;
}

CacheableObjectArrayPtr PdxLocalReader::readObjectArray(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  CacheableObjectArrayPtr coa = CacheableObjectArray::create();
  coa->fromData(*m_dataInput);
  LOGDEBUG("PdxLocalReader::readObjectArray coa->size() = %d", coa->size());
  if (coa->size() <= 0) {
    coa = NULLPTR;
  }
  return coa;
}

int8_t** PdxLocalReader::readArrayOfByteArrays(const char* fieldName,
                                               int32_t& arrayLength,
                                               int32_t** elementLength) {
  checkEmptyFieldName(fieldName);
  int8_t** arrofBytearr = NULL;
  m_dataInput->readArrayOfByteArrays(&arrofBytearr, arrayLength, elementLength);
  return arrofBytearr;
}

CacheableDatePtr PdxLocalReader::readDate(const char* fieldName) {
  checkEmptyFieldName(fieldName);
  CacheableDatePtr cd = CacheableDate::create();
  cd->fromData(*m_dataInput);
  return cd;
}

PdxRemotePreservedDataPtr PdxLocalReader::getPreservedData(
    PdxTypePtr mergedVersion, PdxSerializablePtr pdxObject) {
  int nFieldExtra = m_pdxType->getNumberOfExtraFields();
  LOGDEBUG(
      "PdxLocalReader::getPreservedData::nFieldExtra = %d AND "
      "PdxTypeRegistry::getPdxIgnoreUnreadFields = %d ",
      nFieldExtra, PdxTypeRegistry::getPdxIgnoreUnreadFields());
  if (nFieldExtra > 0 && PdxTypeRegistry::getPdxIgnoreUnreadFields() == false) {
    m_pdxRemotePreserveData->initialize(
        m_pdxType != NULLPTR ? m_pdxType->getTypeId() : 0,
        mergedVersion->getTypeId(), nFieldExtra, pdxObject);
    LOGDEBUG("PdxLocalReader::getPreservedData - 1");

    m_localToRemoteMap = m_pdxType->getLocalToRemoteMap();
    m_remoteToLocalMap = m_pdxType->getRemoteToLocalMap();

    int currentIdx = 0;
    std::vector<int8_t> pdVector;
    for (int i = 0; i < m_remoteToLocalMapSize; i++) {
      if (m_remoteToLocalMap[i] == -1 ||
          m_remoteToLocalMap[i] == -2)  // this field needs to preserve
      {
        int pos = m_pdxType->getFieldPosition(i, m_offsetsBuffer, m_offsetSize,
                                              m_serializedLength);
        int nFieldPos = 0;

        if (i == m_remoteToLocalMapSize - 1) {
          nFieldPos = m_serializedLength;
        } else {
          nFieldPos = m_pdxType->getFieldPosition(
              i + 1, m_offsetsBuffer, m_offsetSize, m_serializedLength);
        }

        resettoPdxHead();
        m_dataInput->advanceCursor(pos);
        uint8_t dataByte = 0;

        for (int i = 0; i < (nFieldPos - pos); i++) {
          m_dataInput->read(&dataByte);
          pdVector.push_back(dataByte);
        }
        resettoPdxHead();

        m_pdxRemotePreserveData->setPreservedData(pdVector);
        currentIdx++;
        pdVector.erase(pdVector.begin(), pdVector.end());
      } else {
        LOGDEBUG("PdxLocalReader::getPreservedData No need to preserve");
      }
    }

    if (m_isDataNeedToPreserve) {
      return m_pdxRemotePreserveData;
    } else {
      LOGDEBUG(
          "PdxLocalReader::GetPreservedData m_isDataNeedToPreserve is false");
    }
  }
  return NULLPTR;
}

bool PdxLocalReader::hasField(const char* fieldName) {
  return m_pdxType->getPdxField(fieldName) != NULLPTR;
}

bool PdxLocalReader::isIdentityField(const char* fieldName) {
  PdxFieldTypePtr pft = m_pdxType->getPdxField(fieldName);
  return (pft != NULLPTR) && (pft->getIdentityField());
}

void PdxLocalReader::readCollection(const char* fieldName,
                                    CacheableArrayListPtr& collection) {
  m_dataInput->readObject(collection);
}

PdxUnreadFieldsPtr PdxLocalReader::readUnreadFields() {
  LOGDEBUG("readUnreadFields:: %d ignore property %d", m_isDataNeedToPreserve,
           PdxTypeRegistry::getPdxIgnoreUnreadFields());
  if (PdxTypeRegistry::getPdxIgnoreUnreadFields() == true) return NULLPTR;
  m_isDataNeedToPreserve = false;
  return m_pdxRemotePreserveData;
}
}  // namespace gemfire
