/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxObject.hpp
 *
 *  Created on: Sep 29, 2011
 *      Author: npatel
 */

#ifndef __PDXVERSIONED1OBJECT_HPP__
#define __PDXVERSIONED1OBJECT_HPP__

#include <gfcpp/PdxSerializable.hpp>
#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/PdxWriter.hpp>
#include <gfcpp/PdxReader.hpp>
#include "fwklib/FwkExport.hpp"

#ifdef _WIN32
#ifdef BUILD_TESTOBJECT
#define TESTOBJECT_EXPORT LIBEXP
#else
#define TESTOBJECT_EXPORT LIBIMP
#endif
#else
#define TESTOBJECT_EXPORT
#endif

using namespace gemfire;

namespace PdxTests {

class TESTOBJECT_EXPORT PdxVersioned1 : public PdxSerializable {
 private:
  wchar_t m_char;
  bool m_bool;
  int8_t m_byte;
  int8_t m_sbyte;  //
  int16_t m_int16;
  int16_t m_uint16;  ///
  int32_t m_int32;
  // int32_t m_uint32;///
  // int64_t m_long;
  int64_t m_ulong;  ///
  float m_float;
  double m_double;

  char* m_string;

  bool* m_boolArray;
  int8_t* m_byteArray;
  int8_t* m_sbyteArray;  ///

  wchar_t* m_charArray;

  CacheableDatePtr m_date;

  int16_t* m_int16Array;
  int16_t* m_uint16Array;

  int32_t* m_int32Array;
  int32_t* m_uint32Array;

  int64_t* m_longArray;
  int64_t* m_ulongArray;

  float* m_floatArray;
  double* m_doubleArray;

  int8_t** m_byteByteArray;

  char** m_stringArray;
  CacheableArrayListPtr m_arraylist;
  CacheableHashMapPtr m_map;

  int8_t* m_byte252;
  int8_t* m_byte253;
  int8_t* m_byte65535;
  int8_t* m_byte65536;
  enum pdxEnumTest { pdx1, pdx2, pdx3, pdx4 };
  CacheablePtr m_pdxEnum;

  int32_t boolArrayLen;
  int32_t charArrayLen;
  int32_t byteArrayLen;
  int32_t shortArrayLen;
  int32_t intArrayLen;
  int32_t longArrayLen;
  int32_t doubleArrayLen;
  int32_t floatArrayLen;
  int32_t strLenArray;
  int32_t m_byte252Len;
  int32_t m_byte253Len;
  int32_t m_byte65535Len;
  int32_t m_byte65536Len;
  int32_t byteByteArrayLen;

  int* lengthArr;

 public:
  /*  PdxVersioned1(const char* key) {
            LOGINFO("rjk:inside testobject pdxType");
      init(key);
    }*/
  PdxVersioned1() { init("def"); }
  PdxVersioned1(const char* key);
  void init(const char* key);
  inline bool compareBool(bool b, bool b2) {
    if (b == b2) return b;
    throw IllegalStateException("Not got expected value for bool type: ");
  }

  virtual ~PdxVersioned1() {}

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(PdxVersioned1);
    return objectSize;
  }
  // void checkNullAndDelete(void *data);
  wchar_t getChar() { return m_char; }

  wchar_t* getCharArray() { return m_charArray; }

  int8_t** getArrayOfByteArrays() { return m_byteByteArray; }

  bool getBool() { return m_bool; }

  CacheableHashMapPtr getHashMap() { return m_map; }

  int8_t getSByte() { return m_sbyte; }

  int16_t getUint16() { return m_uint16; }

  // int32_t getUInt() {
  //   return m_uint32;
  //}

  int64_t getULong() { return m_ulong; }

  int16_t* getUInt16Array() { return m_uint16Array; }

  int32_t* getUIntArray() { return m_uint32Array; }

  int64_t* getULongArray() { return m_ulongArray; }

  int8_t* getByte252() { return m_byte252; }

  int8_t* getByte253() { return m_byte253; }

  int8_t* getByte65535() { return m_byte65535; }

  int8_t* getByte65536() { return m_byte65536; }

  int8_t* getSByteArray() { return m_sbyteArray; }

  CacheableArrayListPtr getArrayList() { return m_arraylist; }

  int8_t getByte() { return m_byte; }

  int16_t getShort() { return m_int16; }

  int32_t getInt() { return m_int32; }

  // int64_t getLong(){
  //   return m_long;
  // }

  float getFloat() { return m_float; }

  double getDouble() { return m_double; }

  const char* getString() { return m_string; }

  bool* getBoolArray() { return m_boolArray; }

  int8_t* getByteArray() { return m_byteArray; }

  int16_t* getShortArray() { return m_int16Array; }

  int32_t* getIntArray() { return m_int32Array; }

  int64_t* getLongArray() { return m_longArray; }

  double* getDoubleArray() { return m_doubleArray; }

  float* getFloatArray() { return m_floatArray; }

  char** getStringArray() { return m_stringArray; }

  CacheableDatePtr getDate() { return m_date; }

  CacheableEnumPtr getEnum() { return m_pdxEnum; }

  int32_t getByteArrayLength() { return byteArrayLen; }

  int32_t getBoolArrayLength() { return boolArrayLen; }

  int32_t getShortArrayLength() { return shortArrayLen; }

  int32_t getStringArrayLength() { return strLenArray; }

  int32_t getIntArrayLength() { return intArrayLen; }

  int32_t getLongArrayLength() { return longArrayLen; }

  int32_t getFloatArrayLength() { return floatArrayLen; }

  int32_t getDoubleArrayLength() { return doubleArrayLen; }

  int32_t getbyteByteArrayLength() { return byteByteArrayLen; }

  int32_t getCharArrayLength() { return charArrayLen; }

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void toData(PdxWriterPtr pw) /*const*/;

  virtual void fromData(PdxReaderPtr pr);

  CacheableStringPtr toString() const;

  const char* getClassName() const { return "PdxTests.PdxVersioned"; }

  static PdxSerializable* createDeserializable() { return new PdxVersioned1(); }

  bool equals(PdxTests::PdxVersioned1& other, bool isPdxReadSerialized) const;

  template <typename T1, typename T2>
  bool genericValCompare(T1 value1, T2 value2) const;

  template <typename T1, typename T2>
  bool genericCompare(T1* value1, T2* value2, int length) const;

  template <typename T1, typename T2>
  bool generic2DCompare(T1** value1, T2** value2, int length,
                        int* arrLengths) const;
};
typedef SharedPtr<PdxTests::PdxVersioned1> PdxVersioned1Ptr;
}
#endif /* __PDXVERSIONED1OBJECT_HPP__ */
