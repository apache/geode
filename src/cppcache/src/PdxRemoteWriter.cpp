/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxRemoteWriter.cpp
 *
 *  Created on: Nov 3, 2011
 *      Author: npatel
 */

#include "PdxRemoteWriter.hpp"
#include "PdxTypeRegistry.hpp"

namespace gemfire {
/* adongre
 * Coverity - II
 * Non-static class member "m_currentDataIdx" is not initialized in this
 * constructor nor in any functions that it calls.
 * Non-static class member "m_preserveDataIdx" is not initialized in this
 * constructor nor in any functions that it calls.
 * Non-static class member "m_remoteTolocalMapLength" is not initialized in this
 * constructor nor in any functions that it calls.
 * Fix : Initialize the members
 */
PdxRemoteWriter::PdxRemoteWriter()
    : m_preserveDataIdx(0), m_currentDataIdx(-1), m_remoteTolocalMapLength(0) {
  if (m_pdxType != NULLPTR) {
    m_remoteTolocalMap =
        m_pdxType->getRemoteToLocalMap();  // COVERITY --> 29286 Uninitialized
                                           // pointer field
    m_remoteTolocalMapLength = m_pdxType->getTotalFields();
  }
}

PdxRemoteWriter::PdxRemoteWriter(DataOutput& output, PdxTypePtr pdxType,
                                 PdxRemotePreservedDataPtr preservedData)
    : PdxLocalWriter(output, pdxType),
      m_preserveDataIdx(0),
      m_currentDataIdx(-1),
      m_remoteTolocalMapLength(0) {
  m_preserveData = preservedData;
  if (m_pdxType != NULLPTR) {
    m_remoteTolocalMap = m_pdxType->getRemoteToLocalMap();
    m_remoteTolocalMapLength = m_pdxType->getTotalFields();
  }
  m_pdxClassName = pdxType->getPdxClassName();

  initialize();
}

PdxRemoteWriter::PdxRemoteWriter(DataOutput& output, const char* pdxClassName)
    : PdxLocalWriter(output, NULLPTR, pdxClassName),
      m_preserveDataIdx(0),
      m_currentDataIdx(-1),
      m_remoteTolocalMapLength(0) {
  m_preserveData = NULLPTR;
  if (m_pdxType != NULLPTR) {
    m_remoteTolocalMapLength = m_pdxType->getTotalFields();
    m_remoteTolocalMap =
        m_pdxType->getRemoteToLocalMap();  // COVERITY --> 29285 Uninitialized
                                           // pointer field
  }
  m_pdxClassName = pdxClassName;
  initialize();
}

PdxRemoteWriter::~PdxRemoteWriter() {}

void PdxRemoteWriter::endObjectWriting() {
  writePreserveData();
  // write header
  PdxLocalWriter::writePdxHeader();
}

void PdxRemoteWriter::writePreserveData() {
  m_currentDataIdx++;  // it starts from -1
  LOGDEBUG("PdxRemoteWriter::writePreserveData m_currentDataIdx = %d",
           m_currentDataIdx);
  LOGDEBUG("PdxRemoteWriter::writePreserveData m_remoteTolocalMap->Length = %d",
           m_remoteTolocalMapLength);

  if (m_preserveData != NULLPTR) {
    while (m_currentDataIdx < m_remoteTolocalMapLength) {
      if (m_remoteTolocalMap[m_currentDataIdx] ==
          -1)  // need to add preserve data with offset
      {
        PdxLocalWriter::addOffset();
        for (size_t i = 0;
             i < (m_preserveData->getPreservedData(m_preserveDataIdx)).size();
             i++) {
          m_dataOutput->write(
              (m_preserveData->getPreservedData(m_preserveDataIdx))[i]);
        }
        m_preserveDataIdx++;
        m_currentDataIdx++;
      } else if (m_remoteTolocalMap[m_currentDataIdx] ==
                 -2)  // need to add preserve data WITHOUT offset
      {
        for (size_t i = 0;
             i < (m_preserveData->getPreservedData(m_preserveDataIdx)).size();
             i++) {
          m_dataOutput->write(
              (m_preserveData->getPreservedData(m_preserveDataIdx))[i]);
        }
        m_preserveDataIdx++;
        m_currentDataIdx++;

      } else {
        break;  // continue writing local data..
      }
    }
  }
}

void PdxRemoteWriter::initialize() {
  // this is default case
  if (m_preserveData == NULLPTR) {
    m_pdxType = PdxTypeRegistry::getLocalPdxType(m_pdxClassName);
  }
}

bool PdxRemoteWriter::isFieldWritingStarted() {
  return m_currentDataIdx != -1;  // field writing NOT started. do we need
                                  // this??
}

PdxWriterPtr PdxRemoteWriter::writeUnreadFields(PdxUnreadFieldsPtr unread) {
  PdxLocalWriter::writeUnreadFields(unread);
  m_remoteTolocalMap = m_pdxType->getRemoteToLocalMap();
  m_remoteTolocalMapLength = m_pdxType->getTotalFields();
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeChar(const char* fieldName, char value) {
  writePreserveData();
  PdxLocalWriter::writeChar(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeWideChar(const char* fieldName,
                                            wchar_t value) {
  writePreserveData();
  PdxLocalWriter::writeWideChar(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeBoolean(const char* fieldName, bool value) {
  writePreserveData();
  PdxLocalWriter::writeBoolean(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeByte(const char* fieldName, int8_t value) {
  writePreserveData();
  PdxLocalWriter::writeByte(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeShort(const char* fieldName, int16_t value) {
  writePreserveData();
  PdxLocalWriter::writeShort(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeInt(const char* fieldName, int32_t value) {
  writePreserveData();
  PdxLocalWriter::writeInt(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeLong(const char* fieldName, int64_t value) {
  writePreserveData();
  PdxLocalWriter::writeLong(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeFloat(const char* fieldName, float value) {
  writePreserveData();
  PdxLocalWriter::writeFloat(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeDouble(const char* fieldName, double value) {
  writePreserveData();
  PdxLocalWriter::writeDouble(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeDate(const char* fieldName,
                                        CacheableDatePtr date) {
  writePreserveData();
  PdxLocalWriter::writeDate(fieldName, date);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeString(const char* fieldName,
                                          const char* value) {
  writePreserveData();
  PdxLocalWriter::writeString(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeWideString(const char* fieldName,
                                              const wchar_t* value) {
  writePreserveData();
  PdxLocalWriter::writeWideString(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeStringArray(const char* fieldName,
                                               char** array, int length) {
  writePreserveData();
  PdxLocalWriter::writeStringArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeWideStringArray(const char* fieldName,
                                                   wchar_t** array,
                                                   int length) {
  writePreserveData();
  PdxLocalWriter::writeWideStringArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeObject(const char* fieldName,
                                          SerializablePtr value) {
  writePreserveData();
  PdxLocalWriter::writeObject(fieldName, value);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeBooleanArray(const char* fieldName,
                                                bool* array, int length) {
  writePreserveData();
  PdxLocalWriter::writeBooleanArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeCharArray(const char* fieldName, char* array,
                                             int length) {
  writePreserveData();
  PdxLocalWriter::writeCharArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeWideCharArray(const char* fieldName,
                                                 wchar_t* array, int length) {
  writePreserveData();
  PdxLocalWriter::writeWideCharArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeByteArray(const char* fieldName,
                                             int8_t* array, int length) {
  writePreserveData();
  PdxLocalWriter::writeByteArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeShortArray(const char* fieldName,
                                              int16_t* array, int length) {
  writePreserveData();
  PdxLocalWriter::writeShortArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeIntArray(const char* fieldName,
                                            int32_t* array, int length) {
  writePreserveData();
  PdxLocalWriter::writeIntArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeLongArray(const char* fieldName,
                                             int64_t* array, int length) {
  writePreserveData();
  PdxLocalWriter::writeLongArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeFloatArray(const char* fieldName,
                                              float* array, int length) {
  writePreserveData();
  PdxLocalWriter::writeFloatArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeDoubleArray(const char* fieldName,
                                               double* array, int length) {
  writePreserveData();
  PdxLocalWriter::writeDoubleArray(fieldName, array, length);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeObjectArray(const char* fieldName,
                                               CacheableObjectArrayPtr array) {
  writePreserveData();
  PdxLocalWriter::writeObjectArray(fieldName, array);
  return PdxWriterPtr(this);
}

PdxWriterPtr PdxRemoteWriter::writeArrayOfByteArrays(const char* fieldName,
                                                     int8_t** byteArrays,
                                                     int arrayLength,
                                                     int* elementLength) {
  writePreserveData();
  PdxLocalWriter::writeArrayOfByteArrays(fieldName, byteArrays, arrayLength,
                                         elementLength);
  return PdxWriterPtr(this);
}
}  // namespace gemfire
