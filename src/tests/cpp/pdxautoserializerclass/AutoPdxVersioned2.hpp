/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef __AUTOPDXVERSIONED2OBJECT_HPP__
#define __AUTOPDXVERSIONED2OBJECT_HPP__

#include <gfcpp/PdxSerializable.hpp>
#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/PdxWriter.hpp>
#include <gfcpp/PdxReader.hpp>

#ifdef _WIN32
#ifdef BUILD_TESTOBJECT
#define TESTOBJECT_EXPORT LIBEXP
#else
#define TESTOBJECT_EXPORT LIBIMP
#endif
#else
#define TESTOBJECT_EXPORT
#endif

#ifdef TESTTASK
#undef TESTTASK
#endif
#ifdef TEST_EXPORT
#undef TEST_EXPORT
#endif

#if defined(_WIN32)
#define TESTTASK extern "C" __declspec(dllexport) int32_t
#define TEST_EXPORT extern "C" __declspec(dllexport)
#else
#define TESTTASK extern "C" int32_t
#define TEST_EXPORT extern "C"
#endif

/*
#if defined(_WIN32)
  #define FUNC_RETURN_INT int
#else
  #define FUNC_RETURN_INT int32_t
#endif
*/

#define GFIGNORE(X) X
#define GFEXCLUDE
#define GFID
#define GFARRAYSIZE(X)
#define GFARRAYELEMSIZE(X)

using namespace gemfire;

namespace AutoPdxTests {

class GFIGNORE(TESTOBJECT_EXPORT) AutoPdxVersioned2 : public PdxSerializable {
 private:
  int8_t** m_byteByteArray;
  wchar_t m_char;
  bool m_bool;
  bool* m_boolArray;
  int8_t m_byte;
  int8_t* m_byteArray;
  wchar_t* m_charArray;
  CacheableArrayListPtr m_arraylist;
  CacheableHashMapPtr m_map;
  char* m_string;
  CacheableDatePtr m_date;
  double m_double;
  double* m_doubleArray;
  float m_float;
  float* m_floatArray;
  int16_t m_int16;
  int32_t m_int32;
  int32_t* m_int32Array;
  int64_t* m_longArray;
  int16_t* m_int16Array;
  int8_t m_sbyte;        //
  int8_t* m_sbyteArray;  ///
  char** m_stringArray;
  int16_t m_uint16;  ///
  int32_t m_uint32;  ///
  int64_t m_long;
  int64_t m_ulong;  ///
  int32_t* m_uint32Array;
  int64_t* m_ulongArray;
  int16_t* m_uint16Array;
  int8_t *m_byte252, *m_byte253, *m_byte65535, *m_byte65536;
  enum pdxEnumTest { pdx1, pdx2, pdx3, pdx4 };
  CacheablePtr m_pdxEnum;
  // GFEXCLUDE double m_double1,m_double2; // bug #909
  // wchar_t pdxType_Wchar1;
  // GFARRAYSIZE(pdxType_Wchar1) int32_t pdxType_pdxType_Wchar1_Size;  // bug
  // #908

  GFARRAYSIZE(m_boolArray) int32_t boolArrayLen;
  GFARRAYSIZE(m_charArray) int32_t charArrayLen;
  GFARRAYSIZE(m_byteArray) int32_t byteArrayLen;
  GFARRAYSIZE(m_sbyteArray) int32_t shortArrayLen;
  GFARRAYSIZE(m_int16Array) int32_t int16Array;
  GFARRAYSIZE(m_uint16Array) int32_t uint16Array;
  GFARRAYSIZE(m_int32Array) int32_t intArrayLen;
  GFARRAYSIZE(m_uint32Array) int32_t uintArrayLen;
  GFARRAYSIZE(m_longArray) int32_t longArrayLen;
  GFARRAYSIZE(m_ulongArray) int32_t ulongArrayLen;
  GFARRAYSIZE(m_doubleArray) int32_t doubleArrayLen;
  GFARRAYSIZE(m_floatArray) int32_t floatArrayLen;
  GFARRAYSIZE(m_stringArray) int32_t strLenArray;
  GFARRAYSIZE(m_byte252) int32_t m_byte252Len;
  GFARRAYSIZE(m_byte253) int32_t m_byte253Len;
  GFARRAYSIZE(m_byte65535) int32_t m_byte65535Len;
  GFARRAYSIZE(m_byte65536) int32_t m_byte65536Len;
  GFARRAYSIZE(m_byteByteArray) int32_t byteByteArrayLen;
  GFARRAYELEMSIZE(m_byteByteArray) int* lengthArr;

 public:
  // inline void init(const char* key = "abc"){

  /*
    AutoPdxVersioned2(const char* key) {
            LOGINFO("rjk:inside testpdxobject pdxType");
      init(key);
    }
  */
  AutoPdxVersioned2() { init("abc"); }
  AutoPdxVersioned2(const char* key);
  void init(const char* key);
  inline bool compareBool(bool b, bool b2) {
    if (b == b2) return b;
    throw IllegalStateException("Not got expected value for bool type: ");
  }

  virtual ~AutoPdxVersioned2() {}

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(AutoPdxVersioned2);
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

  int32_t getUInt() { return m_uint32; }

  int64_t getULong() { return m_ulong; }

  int16_t* getUInt16Array() { return m_uint16Array; }

  int32_t* getUIntArray() { return m_uint32Array; }

  int64_t* getULongArray() { return m_ulongArray; }

  int8_t* getByte252() { return m_byte252; }

  int8_t* getByte253() { return m_byte253; }

  int8_t* getByte65535() { return m_byte65535; }

  int8_t* getByte65536() { return m_byte65536; }

  int8_t* getSByteArray() { return m_sbyteArray; }

  int8_t getByte() { return m_byte; }

  int16_t getShort() { return m_int16; }

  int32_t getInt() { return m_int32; }

  int64_t getLong() { return m_long; }

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

  CacheableEnumPtr getPdxEnum() { return m_pdxEnum; }

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

  const char* getClassName() const;
  /*AutoPdxVersioned2(){
   LOGINFO("rjk:inside testpdxobject default pdxType");
   init();
  }*/
  static PdxSerializable* createDeserializable();

  bool equals(AutoPdxTests::AutoPdxVersioned2& other,
              bool isPdxReadSerialized) const;

  template <typename T1, typename T2>
  bool genericValCompare(T1 value1, T2 value2) const;

  template <typename T1, typename T2>
  bool genericCompare(T1* value1, T2* value2, int length) const;

  template <typename T1, typename T2>
  bool generic2DCompare(T1** value1, T2** value2, int length,
                        int* arrLengths) const;
};
typedef SharedPtr<AutoPdxTests::AutoPdxVersioned2> AutoPdxVersioned2Ptr;
}
#endif /* __AUTOPDXVERSIONED2OBJECT_HPP__ */
