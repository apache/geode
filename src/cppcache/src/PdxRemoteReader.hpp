#pragma once

#ifndef GEODE_PDXREMOTEREADER_H_
#define GEODE_PDXREMOTEREADER_H_

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

#include "PdxLocalReader.hpp"

namespace apache {
namespace geode {
namespace client {

class PdxRemoteReader : public PdxLocalReader {
 private:
  int32_t m_currentIndex;

 public:
  PdxRemoteReader(DataInput& dataInput, PdxTypePtr remoteType, int32_t pdxLen)
      : PdxLocalReader(dataInput, remoteType, pdxLen) {
    m_currentIndex = 0;
  }

  virtual ~PdxRemoteReader();

  virtual char readChar(const char* fieldName);

  virtual wchar_t readWideChar(const char* fieldName);

  /**
   * Read a boolean value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Bool value
   */
  virtual bool readBoolean(const char* fieldName);

  /**
   * Read a byte value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Byte value
   */
  virtual int8_t readByte(const char* fieldName);

  /**
   * Read a 16-bit integer value from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Short value
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
   * Returns Double value
   */
  virtual double readDouble(const char* fieldName);

  /**
   * Read a ASCII string from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * @param value value of the field which needs to serialize

  virtual void readASCII(const char* fieldName, char** value, uint16_t* len =
  NULL);

  *
   * Read a ASCII Huge string from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * @param value value of the field which needs to serialize

  virtual void readASCIIHuge(const char* fieldName, char** value, uint32_t* len
  = NULL);

  *
  * Read a UTF string from the <code>PdxReader</code>.
  * @param fieldName name of the field which needs to serialize
  * @param value value of the field which needs to serialize

  virtual void readUTF(const char* fieldName, char** value, uint16_t* len =
  NULL);

  *
  * Read a string from the <code>PdxReader</code>.
  * @param fieldName name of the field which needs to serialize
  * Returns String value

  virtual void readUTFHuge(const char* fieldName, char** value, uint32_t* len =
  NULL);
*/
  virtual char* readString(const char* fieldName);

  virtual wchar_t* readWideString(const char* fieldName);
  /**
   * Read a object from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * @param value value of the field which needs to serialize
   */
  virtual SerializablePtr readObject(const char* fieldName);

  virtual char* readCharArray(const char* fieldName, int32_t& length);

  virtual wchar_t* readWideCharArray(const char* fieldName, int32_t& length);

  virtual bool* readBooleanArray(const char* fieldName, int32_t& length);

  /**
   * Read a 8bit-Integer Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Byte array value
   */
  virtual int8_t* readByteArray(const char* fieldName, int32_t& length);

  /**
   * Read a 16bit-Integer Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Short
   */
  virtual int16_t* readShortArray(const char* fieldName, int32_t& length);

  /**
   * Read a 32bit-Integer Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Int value
   */
  virtual int32_t* readIntArray(const char* fieldName, int32_t& length);

  /**
   * Read a Long integer Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Long array
   */
  virtual int64_t* readLongArray(const char* fieldName, int32_t& length);

  /**
   * Read a Float Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Float array
   */
  virtual float* readFloatArray(const char* fieldName, int32_t& length);

  /**
   * Read a Double Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns Double array
   */
  virtual double* readDoubleArray(const char* fieldName, int32_t& length);

  /**
   * Read a String Array from the <code>PdxReader</code>.
   * @param fieldName name of the field which needs to serialize
   * Returns String array
   */
  virtual char** readStringArray(const char* fieldName, int32_t& length);

  virtual wchar_t** readWideStringArray(const char* fieldName, int32_t& length);

  virtual CacheableObjectArrayPtr readObjectArray(const char* fieldName);

  virtual int8_t** readArrayOfByteArrays(const char* fieldName,
                                         int32_t& arrayLength,
                                         int32_t** elementLength);

  virtual CacheableDatePtr readDate(const char* fieldName);

  /*virtual void readBytes(const char* fieldName, uint8_t*& bytes,
    int32_t& len);

  virtual void readBytes(const char* fieldName, int8_t*& bytes,
    int32_t& len );

  virtual void readASCIIChar(const char* fieldName, wchar_t& value);*/

  virtual void readCollection(const char* fieldName,
                              CacheableArrayListPtr& collection);
};
typedef SharedPtr<PdxRemoteReader> PdxRemoteReaderPtr;
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_PDXREMOTEREADER_H_
