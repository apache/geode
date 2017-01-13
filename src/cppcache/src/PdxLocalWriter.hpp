/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxLocalWriter.hpp
 *
 *  Created on: Nov 3, 2011
 *      Author: npatel
 */

#ifndef _GEMFIRE_IMPL_PDXLOCALWRITER_HPP_
#define _GEMFIRE_IMPL_PDXLOCALWRITER_HPP_

#include <gfcpp/PdxWriter.hpp>
#include "PdxType.hpp"
#include <gfcpp/DataOutput.hpp>
#include <vector>
#include "PdxRemotePreservedData.hpp"
#include <gfcpp/CacheableObjectArray.hpp>

namespace gemfire {

class PdxLocalWriter : public PdxWriter {
 protected:
  DataOutput* m_dataOutput;
  PdxTypePtr m_pdxType;
  const uint8_t* m_startPosition;
  int32 m_startPositionOffset;
  const char* m_domainClassName;
  std::vector<int32> m_offsets;
  int32 m_currentOffsetIndex;

  PdxRemotePreservedDataPtr m_preserveData;
  const char* m_pdxClassName;

  PdxWriterPtr writeStringwithoutOffset(const char* value);

  PdxWriterPtr writeWideStringwithoutOffset(const wchar_t* value);

 public:
  PdxLocalWriter();

  PdxLocalWriter(DataOutput& output, PdxTypePtr pdxType);

  PdxLocalWriter(DataOutput& output, PdxTypePtr pdxType,
                 const char* pdxDomainType);

  virtual ~PdxLocalWriter();

  void initialize();

  virtual void addOffset();

  virtual void endObjectWriting();

  void writePdxHeader();

  virtual void writeOffsets(int32 len);

  virtual int32 calculateLenWithOffsets();

  virtual bool isFieldWritingStarted();

  inline void writeObject(bool value) { m_dataOutput->writeBoolean(value); }

  inline void writeObject(wchar_t value) {
    m_dataOutput->writeInt((uint16_t)value);
  }

  inline void writePdxChar(char value) {
    m_dataOutput->writeInt((uint16_t)value);
  }

  inline void writePdxCharArray(char* objArray, int arrayLen) {
    if (objArray != NULL) {
      m_dataOutput->writeArrayLen(arrayLen);
      if (arrayLen > 0) {
        char* ptr = objArray;
        int i = 0;
        for (i = 0; i < arrayLen; i++) {
          writePdxChar(*ptr++);
        }
      }
    } else {
      m_dataOutput->write((uint8_t)0xff);
    }
  }

  inline void writeObject(int8_t value) { m_dataOutput->write(value); }

  inline void writeObject(int16_t value) { m_dataOutput->writeInt(value); }

  inline void writeObject(int32_t value) { m_dataOutput->writeInt(value); }

  inline void writeObject(int64_t value) { m_dataOutput->writeInt(value); }

  inline void writeObject(float value) { m_dataOutput->writeFloat(value); }

  inline void writeObject(double value) { m_dataOutput->writeDouble(value); }

  template <typename mType>
  void writeObject(mType* objArray, int arrayLen) {
    if (objArray != NULL) {
      m_dataOutput->writeArrayLen(arrayLen);
      if (arrayLen > 0) {
        mType* ptr = objArray;
        int i = 0;
        for (i = 0; i < arrayLen; i++) {
          writeObject(*ptr++);
        }
      }
    } else {
      m_dataOutput->write((uint8_t)0xff);
    }
  }

  /**
   *Write a wide char to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The wide char value to write
   */
  virtual PdxWriterPtr writeChar(const char* fieldName, char value);

  virtual PdxWriterPtr writeWideChar(const char* fieldName, wchar_t value);

  /**
   *Write a boolean value to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The boolean value to write
   */
  virtual PdxWriterPtr writeBoolean(const char* fieldName, bool value);

  /**
   *Write a 8-bit integer or byte to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The 8-bit integer or byte to write
   */
  virtual PdxWriterPtr writeByte(const char* fieldName, int8_t value);

  /**
   *Write a 16-bit integer to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The 16-bit integer to write
   */
  virtual PdxWriterPtr writeShort(const char* fieldName, int16_t value);

  /**
   *Write a 32-bit integer to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The 32-bit integer to write
   */
  virtual PdxWriterPtr writeInt(const char* fieldName, int32_t value);

  /**
   *Write a long integer to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The long integer to write
   */
  virtual PdxWriterPtr writeLong(const char* fieldName, int64_t value);

  /**
   *Write a Float to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The float value to write
   */
  virtual PdxWriterPtr writeFloat(const char* fieldName, float value);

  /**
   *Write a Double to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The double value to write
   */
  virtual PdxWriterPtr writeDouble(const char* fieldName, double value);

  /**
   *Write a Date to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The date value to write
   */
  virtual PdxWriterPtr writeDate(const char* fieldName, CacheableDatePtr date);

  /**
   *Write a string to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The string to write
   */
  virtual PdxWriterPtr writeString(const char* fieldName, const char* value);

  virtual PdxWriterPtr writeWideString(const char* fieldName,
                                       const wchar_t* value);
  /**
   *Write a object to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The object to write
   */
  virtual PdxWriterPtr writeObject(const char* fieldName,
                                   SerializablePtr value);

  /**
   *Write a boolean array to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The boolean array value to write
   */
  virtual PdxWriterPtr writeBooleanArray(const char* fieldName, bool* array,
                                         int length);

  /**
   *Write a Char array to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The char array value to write
   */
  virtual PdxWriterPtr writeCharArray(const char* fieldName, char* array,
                                      int length);

  virtual PdxWriterPtr writeWideCharArray(const char* fieldName, wchar_t* array,
                                          int length);
  /**
   *Write a Byte array to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The byte array value to write
   */
  virtual PdxWriterPtr writeByteArray(const char* fieldName, int8_t* array,
                                      int length);

  /**
   *Write a 16-bit integer array to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The array value to write
   */
  virtual PdxWriterPtr writeShortArray(const char* fieldName, int16_t* array,
                                       int length);

  /**
   *Write a 32-bit integer array to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The integer array value to write
   */
  virtual PdxWriterPtr writeIntArray(const char* fieldName, int32_t* array,
                                     int length);

  /**
   *Write a long integer array to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The long integer array value to write
   */
  virtual PdxWriterPtr writeLongArray(const char* fieldName, int64_t* array,
                                      int length);

  /**
   *Write a Float array to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The float array value to write
   */
  virtual PdxWriterPtr writeFloatArray(const char* fieldName, float* array,
                                       int length);

  /**
   *Write a double array to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The double array value to write
   */
  virtual PdxWriterPtr writeDoubleArray(const char* fieldName, double* array,
                                        int length);

  /**
   *Write a string array to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The string array value to write
   */
  virtual PdxWriterPtr writeStringArray(const char* fieldName, char** array,
                                        int length);

  virtual PdxWriterPtr writeWideStringArray(const char* fieldName,
                                            wchar_t** array, int length);
  /**
   *Write a object array to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param value The object array value to write
   */
  virtual PdxWriterPtr writeObjectArray(const char* fieldName,
                                        CacheableObjectArrayPtr array);

  /**
   *Write a array of byte arrays to the PdxWriter.
   *@param fieldName The name of the field associated with the value.
   *@param array The arrayOfbytearray value to write
   */
  virtual PdxWriterPtr writeArrayOfByteArrays(const char* fieldName,
                                              int8_t** array, int arrayLength,
                                              int* elementLength);

  /**
   *Indicate that the given field name should be included in hashCode and equals
   *checks
   *of this object on a server that is using {@link
   *CacheFactory#setPdxReadSerialized(boolean)} or when a client executes a
   *query on a server.
   *The fields that are marked as identity fields are used to generate the
   *hashCode and
   *equals methods of {@link PdxInstance}. Because of this, the identity fields
   *should themselves
   *either be primatives, or implement hashCode and equals.
   *
   *If no fields are set as identity fields, then all fields will be used in
   *hashCode and equal checks.
   *
   *The identity fields should make marked after they are written using a write*
   *method.
   *
   *@param fieldName The name of the field that should be used in the as part of
   *the identity.
   *@eturns this PdxWriterPtr
   */
  virtual PdxWriterPtr markIdentityField(const char* fieldName);

  virtual PdxWriterPtr writeUnreadFields(PdxUnreadFieldsPtr unread);

  // this is used to get pdx stream when WriteablePdxStream udpadates the field
  // It should be called after pdx stream has been written to output
  uint8_t* getPdxStream(int& pdxLen);

  void writeByte(int8_t byte);

  inline int32_t getStartPositionOffset() { return m_startPositionOffset; }
};
typedef SharedPtr<PdxLocalWriter> PdxLocalWriterPtr;
}
#endif /* PDXLOCALWRITER_HPP_ */
