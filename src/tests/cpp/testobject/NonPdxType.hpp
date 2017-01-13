/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * NonPdxType.hpp
 *
 *  Created on: Apr 30, 2012
 *      Author: vrao
 */

#ifndef __NONPDXTYPE_HPP__
#define __NONPDXTYPE_HPP__

#include <gfcpp/GemfireCppCache.hpp>

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

class TESTOBJECT_EXPORT NonPdxAddress {
 public:
  int32_t _aptNumber;
  const char* _street;
  const char* _city;

 public:
  NonPdxAddress() {}

  NonPdxAddress(int32_t aptN, const char* street, const char* city) {
    _aptNumber = aptN;
    _street = street;
    _city = city;
  }

  bool equals(NonPdxAddress& other) const {
    LOGDEBUG("Inside NonPdxAddress equals");
    NonPdxAddress* ot = dynamic_cast<NonPdxAddress*>(&other);
    if (ot == NULL) {
      return false;
    }
    if (ot == this) {
      return true;
    }
    if (ot->_aptNumber != _aptNumber) {
      return false;
    }
    if (strcmp(ot->_street, _street) != 0) {
      return false;
    }
    if (strcmp(ot->_city, _city) != 0) {
      return false;
    }

    return true;
  }
};

enum pdxEnumTest { pdx1, pdx2, pdx3 };

class TESTOBJECT_EXPORT NonPdxType {
 public:
  char m_char;
  bool m_bool;
  int8_t m_byte;
  int8_t m_sbyte;  //
  int16_t m_int16;
  int16_t m_uint16;  ///
  int32_t m_int32;
  int32_t m_uint32;  ///
  int64_t m_long;
  int64_t m_ulong;  ///
  float m_float;
  double m_double;

  const char* m_string;

  bool* m_boolArray;
  int8_t* m_byteArray;
  int8_t* m_sbyteArray;  ///

  char* m_charArray;

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

  SerializablePtr m_address;

  NonPdxAddress* m_add[10];

  CacheableArrayListPtr m_arraylist;
  CacheableHashMapPtr m_map;
  CacheableHashTablePtr m_hashtable;
  CacheableVectorPtr m_vector;

  CacheableHashSetPtr m_chs;
  CacheableLinkedHashSetPtr m_clhs;

  int8_t* m_byte252;
  int8_t* m_byte253;
  int8_t* m_byte65535;
  int8_t* m_byte65536;
  CacheablePtr m_pdxEnum;

  CacheableObjectArrayPtr m_objectArray;

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
  bool selfCheck();

  inline void init() {
    m_char = 'C';
    m_bool = true;
    m_byte = 0x74;
    m_sbyte = 0x67;
    m_int16 = 0xab;
    m_uint16 = 0x2dd5;
    m_int32 = 0x2345abdc;
    m_uint32 = 0x2a65c434;
    m_long = 324897980;
    m_ulong = 238749898;
    m_float = 23324.324f;
    m_double = 3243298498.00;

    m_string = "gfestring";

    m_boolArray = new bool[3];
    m_boolArray[0] = true;
    m_boolArray[1] = false;
    m_boolArray[2] = true;
    /*for(int i=0; i<3; i++){
      m_boolArray[i] = true;
    };*/

    m_byteArray = new int8_t[2];
    m_byteArray[0] = 0x34;
    m_byteArray[1] = 0x64;

    m_sbyteArray = new int8_t[2];
    m_sbyteArray[0] = 0x34;
    m_sbyteArray[1] = 0x64;

    m_charArray = new char[2];
    m_charArray[0] = 'c';
    m_charArray[1] = 'v';

    // time_t offset = 1310447869154L;
    // m_date = CacheableDate::create(offset);
    struct timeval now;
    now.tv_sec = 1310447869;
    now.tv_usec = 154000;
    m_date = CacheableDate::create(now);

    m_int16Array = new int16_t[2];
    m_int16Array[0] = 0x2332;
    m_int16Array[1] = 0x4545;

    m_uint16Array = new int16_t[2];
    m_uint16Array[0] = 0x3243;
    m_uint16Array[1] = 0x3232;

    m_int32Array = new int32_t[4];
    m_int32Array[0] = 23;
    m_int32Array[1] = 676868;
    m_int32Array[2] = 34343;
    m_int32Array[3] = 2323;

    m_uint32Array = new int32_t[4];
    m_uint32Array[0] = 435;
    m_uint32Array[1] = 234324;
    m_uint32Array[2] = 324324;
    m_uint32Array[3] = 23432432;

    m_longArray = new int64_t[2];
    m_longArray[0] = 324324L;
    m_longArray[1] = 23434545L;

    m_ulongArray = new int64_t[2];
    m_ulongArray[0] = 3245435;
    m_ulongArray[1] = 3425435;

    m_floatArray = new float[2];
    m_floatArray[0] = 232.565f;
    m_floatArray[1] = 2343254.67f;

    m_doubleArray = new double[2];
    m_doubleArray[0] = 23423432;
    m_doubleArray[1] = 4324235435.00;

    m_byteByteArray = new int8_t*[2];
    // for(int i=0; i<2; i++){
    //  m_byteByteArray[i] = new int8_t[1];
    //}
    m_byteByteArray[0] = new int8_t[1];
    m_byteByteArray[1] = new int8_t[2];
    m_byteByteArray[0][0] = 0x23;
    m_byteByteArray[1][0] = 0x34;
    m_byteByteArray[1][1] = 0x55;

    m_stringArray = new char*[2];
    const char* str1 = "one";
    const char* str2 = "two";

    int size = (int)strlen(str1);
    for (int i = 0; i < 2; i++) {
      m_stringArray[i] = new char[size];
    }
    m_stringArray[0] = (char*)str1;
    m_stringArray[1] = (char*)str2;

    m_arraylist = CacheableArrayList::create();
    m_arraylist->push_back(CacheableInt32::create(1));
    m_arraylist->push_back(CacheableInt32::create(2));

    m_map = CacheableHashMap::create();
    m_map->insert(CacheableInt32::create(1), CacheableInt32::create(1));
    m_map->insert(CacheableInt32::create(2), CacheableInt32::create(2));

    m_hashtable = CacheableHashTable::create();
    m_hashtable->insert(CacheableInt32::create(1),
                        CacheableString::create("1111111111111111"));
    m_hashtable->insert(
        CacheableInt32::create(2),
        CacheableString::create("2222222222221111111111111111"));

    m_vector = CacheableVector::create();
    m_vector->push_back(CacheableInt32::create(1));
    m_vector->push_back(CacheableInt32::create(2));
    m_vector->push_back(CacheableInt32::create(3));

    m_chs = CacheableHashSet::create();
    m_chs->insert(CacheableInt32::create(1));

    m_clhs = CacheableLinkedHashSet::create();
    m_clhs->insert(CacheableInt32::create(1));
    m_clhs->insert(CacheableInt32::create(2));

    m_pdxEnum = CacheableEnum::create("PdxTests.pdxEnumTest", "pdx2", pdx2);

    m_add[0] = new NonPdxAddress(1, "street0", "city0");
    m_add[1] = new NonPdxAddress(2, "street1", "city1");
    m_add[2] = new NonPdxAddress(3, "street2", "city2");
    m_add[3] = new NonPdxAddress(4, "street3", "city3");
    m_add[4] = new NonPdxAddress(5, "street4", "city4");
    m_add[5] = new NonPdxAddress(6, "street5", "city5");
    m_add[6] = new NonPdxAddress(7, "street6", "city6");
    m_add[7] = new NonPdxAddress(8, "street7", "city7");
    m_add[8] = new NonPdxAddress(9, "street8", "city8");
    m_add[9] = new NonPdxAddress(10, "street9", "city9");

    m_objectArray = CacheableObjectArray::create();

    m_objectArray->push_back(PdxWrapperPtr(new PdxWrapper(
        new NonPdxAddress(1, "street0", "city0"), "PdxTests.Address")));
    m_objectArray->push_back(PdxWrapperPtr(new PdxWrapper(
        new NonPdxAddress(2, "street1", "city1"), "PdxTests.Address")));
    m_objectArray->push_back(PdxWrapperPtr(new PdxWrapper(
        new NonPdxAddress(3, "street2", "city2"), "PdxTests.Address")));
    m_objectArray->push_back(PdxWrapperPtr(new PdxWrapper(
        new NonPdxAddress(4, "street3", "city3"), "PdxTests.Address")));
    m_objectArray->push_back(PdxWrapperPtr(new PdxWrapper(
        new NonPdxAddress(5, "street4", "city4"), "PdxTests.Address")));
    m_objectArray->push_back(PdxWrapperPtr(new PdxWrapper(
        new NonPdxAddress(6, "street5", "city5"), "PdxTests.Address")));
    m_objectArray->push_back(PdxWrapperPtr(new PdxWrapper(
        new NonPdxAddress(7, "street6", "city6"), "PdxTests.Address")));
    m_objectArray->push_back(PdxWrapperPtr(new PdxWrapper(
        new NonPdxAddress(8, "street7", "city7"), "PdxTests.Address")));
    m_objectArray->push_back(PdxWrapperPtr(new PdxWrapper(
        new NonPdxAddress(9, "street8", "city8"), "PdxTests.Address")));
    m_objectArray->push_back(PdxWrapperPtr(new PdxWrapper(
        new NonPdxAddress(10, "street9", "city9"), "PdxTests.Address")));

    m_byte252 = new int8_t[252];
    for (int i = 0; i < 252; i++) {
      m_byte252[i] = 0;
    }

    m_byte253 = new int8_t[253];
    for (int i = 0; i < 253; i++) {
      m_byte253[i] = 0;
    }

    m_byte65535 = new int8_t[65535];
    for (int i = 0; i < 65535; i++) {
      m_byte65535[i] = 0;
    }

    m_byte65536 = new int8_t[65536];
    for (int i = 0; i < 65536; i++) {
      m_byte65536[i] = 0;
    }

    boolArrayLen = 3;
    byteArrayLen = 2;
    shortArrayLen = 2;
    intArrayLen = 4;
    longArrayLen = 2;
    doubleArrayLen = 2;
    floatArrayLen = 2;
    strLenArray = 2;
    charArrayLen = 2;
    byteByteArrayLen = 2;

    lengthArr = new int[2];

    lengthArr[0] = 1;
    lengthArr[1] = 2;
  }

  NonPdxType() { init(); }

  inline bool compareBool(bool b, bool b2) {
    if (b == b2) return b;
    throw IllegalStateException("Not got expected value for bool type: ");
  }

  virtual ~NonPdxType() {}

  char getChar() { return m_char; }

  char* getCharArray() { return m_charArray; }

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

  CacheableHashSetPtr getHashSet() { return m_chs; }

  CacheableLinkedHashSetPtr getLinkedHashSet() { return m_clhs; }

  CacheableArrayListPtr getArrayList() { return m_arraylist; }

  CacheableHashTablePtr getHashTable() { return m_hashtable; }

  CacheableVectorPtr getVector() { return m_vector; }

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

  CacheableObjectArrayPtr getCacheableObjectArray() { return m_objectArray; }

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

  bool equals(PdxTests::NonPdxType& other, bool isPdxReadSerialized) const;

  template <typename T1, typename T2>
  bool genericValCompare(T1 value1, T2 value2) const;

  template <typename T1, typename T2>
  bool genericCompare(T1* value1, T2* value2, int length) const;

  template <typename T1, typename T2>
  bool generic2DCompare(T1** value1, T2** value2, int length,
                        int* arrLengths) const;
};
}
#endif /* __NONPDXTYPE_HPP__ */
