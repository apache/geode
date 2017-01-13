/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __PDX_OBJECT_WITH_AUTO_HPP__
#define __PDX_OBJECT_WITH_AUTO_HPP__

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

using namespace gemfire;

#define GFIGNORE(X) X
#define GFID
#define GFARRAYSIZE(X)
#define GFARRAYELEMSIZE(X)
#define GFEXCLUDE

namespace PdxTestsAuto {

class GFIGNORE(TESTOBJECT_EXPORT) GrandParent {
 public:
  int32_t m_a;
  int32_t m_b;

  inline void init() {
    this->m_a = 1;
    this->m_b = 2;
  }

  GrandParent() { init(); }

  int32_t getMember_a() { return m_a; }

  int32_t getMember_b() { return m_b; }
};

class GFIGNORE(TESTOBJECT_EXPORT) Parent : public GrandParent {
 public:
  int32_t m_c;
  int32_t m_d;

  inline void init() {
    this->m_c = 3;
    this->m_d = 4;
  }

  int32_t getMember_c() { return m_c; }

  int32_t getMember_d() { return m_d; }

  Parent() { init(); }
};

class Child;
typedef SharedPtr<Child> ChildPtr;

class GFIGNORE(TESTOBJECT_EXPORT) Child : public Parent,
                                          public PdxSerializable {
 private:
  int32_t m_e;
  int32_t m_f;

 public:
  inline void init() {
    this->m_e = 5;
    this->m_f = 6;
  }

  Child() { init(); }

  int32_t getMember_e() { return m_e; }

  int32_t getMember_f() { return m_f; }

  CacheableStringPtr toString() const {
    char idbuf[512];
    sprintf(idbuf, " Child:: %d %d %d %d %d %d ", m_a, m_b, m_c, m_d, m_e, m_f);
    return CacheableString::create(idbuf);
  }

  const char* getClassName() const; /* {
      return "PdxTestsAuto.Child";
    }*/

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  void toData(PdxWriterPtr pw); /* {
  pw->writeInt("m_a", m_a);
  pw->writeInt("m_b", m_b);
  pw->writeInt("m_c", m_c);
  pw->writeInt("m_d", m_d);
  pw->writeInt("m_e", m_e);
  pw->writeInt("m_f", m_f);
}*/

  void fromData(PdxReaderPtr pr);                 /*
                 {
                   m_a = pr->readInt("m_a");
                   m_b = pr->readInt("m_b");
                   m_c = pr->readInt("m_c");
                   m_d = pr->readInt("m_d");
                   m_e = pr->readInt("m_e");
                   m_f = pr->readInt("m_f");
                 }
             */
  static PdxSerializable* createDeserializable(); /* {
     return new Child();
   }*/

  bool equals(PdxSerializablePtr obj) {
    if (obj == NULLPTR) return false;

    ChildPtr pap = dynCast<ChildPtr>(obj);
    if (pap == NULLPTR) return false;

    if (pap == this) return true;

    if (m_a == pap->m_a && m_b == pap->m_b && m_c == pap->m_c &&
        m_d == pap->m_d && m_e == pap->m_e && m_f == pap->m_f)
      return true;

    return false;
  }
};

class GFIGNORE(TESTOBJECT_EXPORT) CharTypes : public PdxSerializable {
 private:
  char m_ch;
  wchar_t m_widechar;
  char* m_chArray;
  wchar_t* m_widecharArray;

  GFARRAYSIZE(m_chArray) int32_t m_charArrayLen;
  GFARRAYSIZE(m_widecharArray) int32_t m_wcharArrayLen;

 public:
  inline void init() {
    m_ch = 'C';
    m_widechar = L'a';

    m_chArray = new char[2];
    m_chArray[0] = 'X';
    m_chArray[1] = 'Y';

    m_widecharArray = new wchar_t[2];
    m_widecharArray[0] = L'x';
    m_widecharArray[1] = L'y';

    m_charArrayLen = 2;
    m_wcharArrayLen = 2;
  }

  CharTypes() { init(); }

  CacheableStringPtr toString() const {
    char idbuf[1024];
    sprintf(idbuf, "%c %lc %c %c %lc %lc", m_ch, m_widechar, m_chArray[0],
            m_chArray[1], m_widecharArray[0], m_widecharArray[1]);
    return CacheableString::create(idbuf);
  }

  bool equals(CharTypes& other) const {
    LOGDEBUG("Inside CharTypes equals");
    CharTypes* ot = dynamic_cast<CharTypes*>(&other);
    if (ot == NULL) {
      return false;
    }
    if (ot == this) {
      return true;
    }
    LOGINFO(
        "CharTypes::equals ot->m_ch = %c m_ch = %c AND ot->m_widechar = %lc "
        "m_widechar = %lc",
        ot->m_ch, m_ch, ot->m_widechar, m_widechar);
    if (ot->m_ch != m_ch || ot->m_widechar != m_widechar) {
      return false;
    }

    int i = 0;
    while (i < 2) {
      LOGINFO(
          "CharTypes::equals Normal char array values ot->m_chArray[%d] = %c "
          "m_chArray[%d] = %c",
          i, ot->m_chArray[i], i, m_chArray[i]);
      LOGINFO(
          "CharTypes::equals Wide char array values ot->m_widecharArray[%d] = "
          "%lc m_widecharArray[%d] = %lc",
          i, ot->m_widecharArray[i], i, m_widecharArray[i]);
      if (ot->m_chArray[i] != m_chArray[i] ||
          ot->m_widecharArray[i] != m_widecharArray[i])
        return false;
      else
        i++;
    }

    return true;
  }

  const char* getClassName() const; /* {
                                    return "PdxTestsAuto.CharTypes";
                                    }*/

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  void toData(PdxWriterPtr pw); /*{
                                pw->writeChar("m_ch", m_ch);
                                pw->writeWideChar("m_widechar", m_widechar);
                                pw->writeCharArray("m_chArray", m_chArray, 2);
                                pw->writeWideCharArray("m_widecharArray",
                                m_widecharArray, 2);
                                }*/

  void fromData(PdxReaderPtr pr); /*
                                  {
                                  m_ch = pr->readChar("m_ch");
                                  m_widechar = pr->readWideChar("m_widechar");
                                  m_chArray = pr->readCharArray("m_chArray",
                                  m_wcharArrayLen);
                                  m_widecharArray =
                                  pr->readWideCharArray("m_widecharArray",
                                  m_charArrayLen);
                                  }*/

  static PdxSerializable* createDeserializable(); /*{
                                                  return new CharTypes();
                                                  }*/
};
typedef SharedPtr<CharTypes> CharTypesPtr;

/**********/
class GFIGNORE(TESTOBJECT_EXPORT) Address : public PdxSerializable {
 private:
  int32_t _aptNumber;
  char* _street;
  char* _city;

 public:
  Address() {}

  CacheableStringPtr toString() const {
    char idbuf[1024];
    sprintf(idbuf, "%d %s %s", _aptNumber, _street, _city);
    return CacheableString::create(idbuf);
  }

  Address(int32_t aptN, const char* street, const char* city) {
    _aptNumber = aptN;
    _street = (char*)street;
    _city = (char*)city;
  }

  bool equals(Address& other) const {
    LOGDEBUG("Inside Address equals");
    Address* ot = dynamic_cast<Address*>(&other);
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

  const char* getClassName() const; /* {
     return "PdxTestsAuto.Address";
   }*/

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  void toData(PdxWriterPtr pw); /*const*/  //{
  //  pw->writeInt("_aptNumber", _aptNumber);//4
  //  pw->writeString("_street", _street);
  //  pw->writeString("_city", _city);
  //}

  void fromData(PdxReaderPtr pr);
  //{
  //  _aptNumber = pr->readInt("_aptNumber");
  //  _street = pr->readString("_street");
  //  _city = pr->readString("_city");
  //}

  static PdxSerializable* createDeserializable(); /*{
    return new Address();
  }*/

  int32_t getAptNum() { return _aptNumber; }

  const char* getStreet() { return _street; }

  const char* getCity() { return _city; }

  /*static AddressPtr create(int32_t aptN, char* street, char* city)
  {
  AddressPtr str = NULLPTR;
  if (value != NULL) {
  str = new Address();
  }
  return str;
  }*/
};
typedef SharedPtr<Address> AddressPtr;
enum pdxEnumTest { pdx1, pdx2, pdx3 };
class GFIGNORE(TESTOBJECT_EXPORT) PdxType : public PdxSerializable {
 private:
  int8_t** m_byteByteArray;
  GFARRAYSIZE(m_byteByteArray) int32_t byteByteArrayLen;
  GFARRAYELEMSIZE(m_byteByteArray) int* lengthArr;

  GFID wchar_t m_char;
  GFID bool m_bool;

  GFID bool* m_boolArray;
  GFARRAYSIZE(m_boolArray) int32_t boolArrayLen;
  GFID int8_t m_byte;
  GFID int8_t* m_byteArray;
  GFARRAYSIZE(m_byteArray) int32_t byteArrayLen;
  GFID wchar_t* m_charArray;
  GFARRAYSIZE(m_charArray) int32_t charArrayLen;
  GFID CacheableArrayListPtr m_arraylist;
  GFID CacheableHashMapPtr m_map;
  GFID CacheableHashTablePtr m_hashtable;
  GFID CacheableVectorPtr m_vector;
  GFID CacheableHashSetPtr m_chs;
  GFID CacheableLinkedHashSetPtr m_clhs;
  GFID char* m_string;
  GFID CacheableDatePtr m_dateTime;
  GFID double m_double;
  GFID double* m_doubleArray;
  GFARRAYSIZE(m_doubleArray) int32_t doubleArrayLen;
  GFID float m_float;
  GFID float* m_floatArray;
  GFARRAYSIZE(m_floatArray) int32_t floatArrayLen;
  GFID int16_t m_int16;
  GFID int32_t m_int32;
  GFID int64_t m_long;
  GFID int32_t* m_int32Array;
  GFARRAYSIZE(m_int32Array) int32_t intArrayLen;
  GFID int64_t* m_longArray;
  GFARRAYSIZE(m_longArray) int32_t longArrayLen;
  GFID int16_t* m_int16Array;
  GFARRAYSIZE(m_int16Array) int32_t shortArrayLen;
  GFID int8_t m_sbyte;
  GFID int8_t* m_sbyteArray;
  GFARRAYSIZE(m_sbyteArray) int32_t m_sbyteArrayLen;
  GFID char** m_stringArray;
  GFARRAYSIZE(m_stringArray) int32_t strLenArray;
  GFID int16_t m_uint16;
  GFID int32_t m_uint32;
  GFID int64_t m_ulong;
  GFID int32_t* m_uint32Array;
  GFARRAYSIZE(m_uint32Array) int32_t m_uint32ArrayLen;
  GFID int64_t* m_ulongArray;
  GFARRAYSIZE(m_ulongArray) int32_t m_ulongArrayLen;
  GFID int16_t* m_uint16Array;
  GFARRAYSIZE(m_uint16Array) int32_t m_uint16ArrayLen;
  GFID int8_t* m_byte252;
  GFARRAYSIZE(m_byte252) int32_t m_byte252Len;
  GFID int8_t* m_byte253;
  GFARRAYSIZE(m_byte253) int32_t m_byte253Len;
  GFID int8_t* m_byte65535;
  GFARRAYSIZE(m_byte65535) int32_t m_byte65535Len;
  GFID int8_t* m_byte65536;
  GFARRAYSIZE(m_byte65536) int32_t m_byte65536Len;
  GFID CacheablePtr m_pdxEnum;
  GFEXCLUDE Address* m_add[10];
  CacheableObjectArrayPtr m_address;

 public:
  inline void init() {
    lengthArr = new int[2];
    lengthArr[0] = 1;
    lengthArr[1] = 2;

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

    m_string = (char*)"gfestring";

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

    m_charArray = new wchar_t[2];
    m_charArray[0] = (wchar_t)L'c';
    m_charArray[1] = (wchar_t)L'v';

    // time_t offset = 1310447869154L;
    // m_date = CacheableDate::create(offset);
    struct timeval now;
    now.tv_sec = 1310447869;
    now.tv_usec = 154000;
    m_dateTime = CacheableDate::create(now);

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

    m_add[0] = new Address(1, "street0", "city0");
    m_add[1] = new Address(2, "street1", "city1");
    m_add[2] = new Address(3, "street2", "city2");
    m_add[3] = new Address(4, "street3", "city3");
    m_add[4] = new Address(5, "street4", "city4");
    m_add[5] = new Address(6, "street5", "city5");
    m_add[6] = new Address(7, "street6", "city6");
    m_add[7] = new Address(8, "street7", "city7");
    m_add[8] = new Address(9, "street8", "city8");
    m_add[9] = new Address(10, "street9", "city9");

    m_address = NULLPTR;

    m_address = CacheableObjectArray::create();
    m_address->push_back(AddressPtr(new Address(1, "street0", "city0")));
    m_address->push_back(AddressPtr(new Address(2, "street1", "city1")));
    m_address->push_back(AddressPtr(new Address(3, "street2", "city2")));
    m_address->push_back(AddressPtr(new Address(4, "street3", "city3")));
    m_address->push_back(AddressPtr(new Address(5, "street4", "city4")));
    m_address->push_back(AddressPtr(new Address(6, "street5", "city5")));
    m_address->push_back(AddressPtr(new Address(7, "street6", "city6")));
    m_address->push_back(AddressPtr(new Address(8, "street7", "city7")));
    m_address->push_back(AddressPtr(new Address(9, "street8", "city8")));
    m_address->push_back(AddressPtr(new Address(10, "street9", "city9")));

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
    shortArrayLen = m_uint16ArrayLen = 2;
    intArrayLen = m_uint32ArrayLen = 4;
    longArrayLen = m_ulongArrayLen = 2;
    doubleArrayLen = 2;
    floatArrayLen = 2;
    strLenArray = 2;
    charArrayLen = 2;
    byteByteArrayLen = 2;
    m_sbyteArrayLen = 2;

    lengthArr = new int[2];
    lengthArr[0] = 1;
    lengthArr[1] = 2;

    m_byte252Len = 252;
    m_byte253Len = 253;
    m_byte65535Len = 65535;
    m_byte65536Len = 65536;
  }

  PdxType() { init(); }

  inline bool compareBool(bool b, bool b2) {
    if (b == b2) return b;
    throw IllegalStateException("Not got expected value for bool type: ");
  }

  virtual ~PdxType() {}

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(PdxType);
    return objectSize;
  }

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

  CacheableDatePtr getDate() { return m_dateTime; }

  CacheableObjectArrayPtr getCacheableObjectArray() { return m_address; }

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

  const char* getClassName() const; /* {
                                    return "PdxTestsAuto.PdxType";
                                    }*/

  static PdxSerializable* createDeserializable(); /* {
                                                  return new PdxType();
                                                  }*/

  bool equals(PdxTestsAuto::PdxType& other, bool isPdxReadSerialized) const;

  template <typename T1, typename T2>
  bool genericValCompare(T1 value1, T2 value2) const;

  template <typename T1, typename T2>
  bool genericCompare(T1* value1, T2* value2, int length) const;

  template <typename T1, typename T2>
  bool generic2DCompare(T1** value1, T2** value2, int length,
                        int* arrLengths) const;
};
typedef SharedPtr<PdxTestsAuto::PdxType> PdxTypePtr;
}
#endif /* PDXOBJECT_HPP_ */
