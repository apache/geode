/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* PdxLocalReader.hpp
* Created on: Nov 3, 2011
*      Author: npatel
*/

#ifndef _GEMFIRE_IMPL_PDXLOCALREADER_HPP_
#define _GEMFIRE_IMPL_PDXLOCALREADER_HPP_

#include <gfcpp/PdxReader.hpp>
#include "PdxType.hpp"
#include <gfcpp/DataInput.hpp>
#include <gfcpp/CacheableObjectArray.hpp>
#include <gfcpp/CacheableDate.hpp>
#include "PdxRemotePreservedData.hpp"
namespace gemfire {

class PdxLocalReader : public PdxReader {
 protected:
  DataInput* m_dataInput;
  PdxTypePtr m_pdxType;
  uint8_t* m_startBuffer;
  int32_t m_startPosition;
  int32_t m_serializedLength;
  int32_t m_serializedLengthWithOffsets;
  int32_t m_offsetSize;
  uint8_t* m_offsetsBuffer;
  bool m_isDataNeedToPreserve;
  PdxRemotePreservedDataPtr m_pdxRemotePreserveData;
  int32_t* m_localToRemoteMap;
  int32_t* m_remoteToLocalMap;
  int32 m_remoteToLocalMapSize;

  void initialize();
  void resettoPdxHead();
  void checkEmptyFieldName(const char* fieldName);

 public:
  PdxLocalReader();

  PdxLocalReader(DataInput& input, PdxTypePtr remoteType, int32_t pdxLen);

  virtual ~PdxLocalReader();

  void MoveStream();

  virtual PdxRemotePreservedDataPtr getPreservedData(
      PdxTypePtr mergedVersion, PdxSerializablePtr pdxObject);

  /**
   * Read a char value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns char value
   */
  virtual char readChar(const char* fieldName);

  /**
   * Read a wide char value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns wide char value
   */
  virtual wchar_t readWideChar(const char* fieldName);

  /**
   * Read a boolean value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns bool value
   */
  virtual bool readBoolean(const char* fieldName);

  /**
   * Read a byte value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns byte value
   */
  virtual int8_t readByte(const char* fieldName);

  /**
   * Read a 16-bit integer value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns short value
   */
  virtual int16_t readShort(const char* fieldName);

  /**
   * Read a 32-bit integer value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Int value
   */
  virtual int32_t readInt(const char* fieldName);

  /**
   * Read a 64-bit long value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Long value
   */
  virtual int64_t readLong(const char* fieldName);

  /**
   * Read a float value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Float value
   */
  virtual float readFloat(const char* fieldName);

  /**
   * Read a double value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns double value
   */
  virtual double readDouble(const char* fieldName);

  /**
   * Read a string from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns string value
   */
  virtual char* readString(const char* fieldName);

  virtual wchar_t* readWideString(const char* fieldName);
  /**
   * Read a object from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns SerializablePtr
   */
  virtual SerializablePtr readObject(const char* fieldName);

  virtual char* readCharArray(const char* fieldName, int32_t& length);

  virtual wchar_t* readWideCharArray(const char* fieldName, int32_t& length);

  virtual bool* readBooleanArray(const char* fieldName, int32_t& length);

  /**
   * Read a 8bit-Integer Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Byte Array
   */
  virtual int8_t* readByteArray(const char* fieldName, int32_t& length);

  /**
   * Read a 16bit-Integer Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Short Array
   */
  virtual int16_t* readShortArray(const char* fieldName, int32_t& length);

  /**
   * Read a 32bit-Integer Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Int Array
   */
  virtual int32_t* readIntArray(const char* fieldName, int32_t& length);

  /**
   * Read a Long integer Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Long Array
   */
  virtual int64_t* readLongArray(const char* fieldName, int32_t& length);

  /**
   * Read a Float Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Float Array
   */
  virtual float* readFloatArray(const char* fieldName, int32_t& length);

  /**
   * Read a Double Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns double Array
   */
  virtual double* readDoubleArray(const char* fieldName, int32_t& length);

  /**
   * Read a String Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns String Array
   */
  virtual char** readStringArray(const char* fieldName, int32_t& length);

  virtual wchar_t** readWideStringArray(const char* fieldName, int32_t& length);

  virtual CacheableObjectArrayPtr readObjectArray(const char* fieldName);

  virtual int8_t** readArrayOfByteArrays(const char* fieldName,
                                         int32_t& arrayLength,
                                         int32_t** elementLength);

  virtual CacheableDatePtr readDate(const char* fieldName);

  virtual bool hasField(const char* fieldName);

  virtual bool isIdentityField(const char* fieldName);

  virtual void readCollection(const char* fieldName,
                              CacheableArrayListPtr& collection);

  virtual PdxUnreadFieldsPtr readUnreadFields();
};
typedef SharedPtr<PdxLocalReader> PdxLocalReaderPtr;
}
#endif /* PDXLOCALREADER_HPP_ */
