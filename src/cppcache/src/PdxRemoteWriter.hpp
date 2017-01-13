/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxRemoteWriter.hpp
 *
 *  Created on: Nov 3, 2011
 *      Author: npatel
 */

#ifndef PDXREMOTEWRITER_HPP_
#define PDXREMOTEWRITER_HPP_

#include "PdxLocalWriter.hpp"

namespace gemfire {

class PdxRemoteWriter : public PdxLocalWriter {
 private:
  int32* m_remoteTolocalMap;
  int32 m_preserveDataIdx;
  int32 m_currentDataIdx;

  int32 m_remoteTolocalMapLength;

  void initialize();
  void writePreserveData();

 public:
  PdxRemoteWriter();

  virtual ~PdxRemoteWriter();

  PdxRemoteWriter(DataOutput& output, PdxTypePtr pdxType,
                  PdxRemotePreservedDataPtr preservedData);

  PdxRemoteWriter(DataOutput& output, const char* pdxClassName);

  virtual void endObjectWriting();

  virtual bool isFieldWritingStarted();

  virtual PdxWriterPtr writeUnreadFields(PdxUnreadFieldsPtr unread);

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
};
typedef SharedPtr<PdxRemoteWriter> PdxRemoteWriterPtr;
}
#endif /* PDXREMOTEWRITER_HPP_ */
