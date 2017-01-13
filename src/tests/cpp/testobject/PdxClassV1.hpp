/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxClassV1.hpp
 *
 *  Created on: Feb 3, 2012
 *      Author: npatel
 */
#ifndef __PDXCLASSV1_HPP__
#define __PDXCLASSV1_HPP__

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

namespace PdxTests {

/************************************************************
 *  PdxType1V1
 * *********************************************************/
class TESTOBJECT_EXPORT PdxType1V1 : public PdxSerializable {
 private:
  static int32_t m_diffInSameFields;
  static bool m_useWeakHashMap;
  PdxUnreadFieldsPtr m_pdxUreadFields;

  int32_t m_i1;  // = 34324;
  int32_t m_i2;  // = 2144;
  int32_t m_i3;  // = 4645734;
  int32_t m_i4;  // = 73567;

 public:
  PdxType1V1();

  virtual ~PdxType1V1();

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const { return "PdxTests::PdxTypes1V"; }

  static PdxSerializable* createDeserializable() { return new PdxType1V1(); }
};
typedef SharedPtr<PdxType1V1> PdxType1V1Ptr;

/************************************************************
 *  PdxType2V1
 * *********************************************************/
class TESTOBJECT_EXPORT PdxType2V1 : public PdxSerializable {
 private:
  static int m_diffInSameFields;
  static bool m_useWeakHashMap;
  PdxUnreadFieldsPtr m_unreadFields;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  PdxType2V1();

  virtual ~PdxType2V1();

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const { return "PdxTests::PdxTypes2V"; }

  static PdxSerializable* createDeserializable() { return new PdxType2V1(); }
};
typedef SharedPtr<PdxType2V1> PdxType2V1Ptr;

/************************************************************
 *  PdxType3V1
 * *********************************************************/

class TESTOBJECT_EXPORT PdxType3V1 : public PdxSerializable {
 private:
  static int m_diffInSameFields;
  static int m_diffInExtraFields;
  static bool m_useWeakHashMap;
  PdxUnreadFieldsPtr m_unreadFields;
  int32_t m_i1;
  int32_t m_i2;
  char* m_str1;
  int32_t m_i3;
  int32_t m_i4;
  int32_t m_i5;
  char* m_str2;

 public:
  PdxType3V1();

  virtual ~PdxType3V1();

  CacheableStringPtr toString() const;

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const { return "PdxTests::PdxTypes3V"; }

  static PdxSerializable* createDeserializable() { return new PdxType3V1(); }
};
typedef SharedPtr<PdxType3V1> PdxType3V1Ptr;

/************************************************************
 *  PdxTypesV1R1
 * *********************************************************/
class TESTOBJECT_EXPORT PdxTypesV1R1 : public PdxSerializable {
 private:
  static int m_diffInSameFields;
  static bool m_useWeakHashMap;
  PdxUnreadFieldsPtr m_pdxUreadFields;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  PdxTypesV1R1();

  virtual ~PdxTypesV1R1();

  CacheableStringPtr toString() const;

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const { return "PdxTests::PdxTypesR1"; }

  static PdxSerializable* createDeserializable() { return new PdxTypesV1R1(); }
};
typedef SharedPtr<PdxTypesV1R1> PdxTypesV1R1Ptr;

/************************************************************
 *  PdxTypesV1R2
 * *********************************************************/
class TESTOBJECT_EXPORT PdxTypesV1R2 : public PdxSerializable {
 private:
  static int m_diffInSameFields;
  static bool m_useWeakHashMap;
  PdxUnreadFieldsPtr m_pdxUreadFields;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  PdxTypesV1R2();

  virtual ~PdxTypesV1R2();

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const { return "PdxTests::PdxTypesR2"; }

  static PdxSerializable* createDeserializable() { return new PdxTypesV1R2(); }
};
typedef SharedPtr<PdxTypesV1R2> PdxTypesV1R2Ptr;

/************************************************************
 *  PdxTypesIgnoreUnreadFieldsV1
 * *********************************************************/
class TESTOBJECT_EXPORT PdxTypesIgnoreUnreadFieldsV1 : public PdxSerializable {
 private:
  static int m_diffInSameFields;
  static bool m_useWeakHashMap;
  PdxUnreadFieldsPtr m_unreadFields;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  PdxTypesIgnoreUnreadFieldsV1();

  virtual ~PdxTypesIgnoreUnreadFieldsV1();

  CacheableStringPtr toString() const;

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const {
    return "PdxTests::PdxTypesIgnoreUnreadFields";
  }

  static PdxSerializable* createDeserializable() {
    return new PdxTypesIgnoreUnreadFieldsV1();
  }
};
typedef SharedPtr<PdxTypesIgnoreUnreadFieldsV1> PdxTypesIgnoreUnreadFieldsV1Ptr;

/************************************************************
 *  PdxVersionedV1
 * *********************************************************/
class TESTOBJECT_EXPORT PdxVersionedV1 : public PdxSerializable {
 private:
  char m_char;
  bool m_bool;
  int8_t m_byte;
  int16_t m_int16;
  int32_t m_int32;
  int64_t m_long;
  float m_float;
  double m_double;
  char* m_string;
  bool* m_boolArray;
  char* m_charArray;
  CacheableDatePtr m_dateTime;
  int16_t* m_int16Array;
  int32_t* m_int32Array;
  int64_t* m_longArray;
  float* m_floatArray;
  double* m_doubleArray;

  // IDictionary<object, object> m_map;
  // List<object> m_list;

  int32_t boolArrayLen;
  int32_t byteArrayLen;
  int32_t shortArrayLen;
  int32_t intArrayLen;
  int32_t longArrayLen;
  int32_t doubleArrayLen;
  int32_t floatArrayLen;
  int32_t strLenArray;

 public:
  PdxVersionedV1();

  PdxVersionedV1(int32_t size);

  virtual ~PdxVersionedV1();

  void init(int32_t size);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const { return "PdxTests::PdxVersioned"; }

  static PdxSerializable* createDeserializable() {
    return new PdxVersionedV1();
  }
};
typedef SharedPtr<PdxVersionedV1> PdxVersionedV1Ptr;

/************************************************************
 *  TestKey
 * *********************************************************/

class TESTOBJECT_EXPORT TestKeyV1 {
 public:
  char* _id;

 public:
  TestKeyV1();

  TestKeyV1(char* id);
};

/************************************************************
 *  TestKey
 * *********************************************************/

class TESTOBJECT_EXPORT TestDiffTypePdxSV1 {
 public:
  char* _id;
  char* _name;

 public:
  TestDiffTypePdxSV1();

  TestDiffTypePdxSV1(bool init);

  bool equals(TestDiffTypePdxSV1* obj);
};

/************************************************************
 *  TestPdxSerializerForV1
 * *********************************************************/
static const char* V1CLASSNAME1 = "PdxTests.TestKey";
static const char* V1CLASSNAME2 = "PdxTests.TestDiffTypePdxS";

class TestPdxSerializerForV1 : public PdxSerializer {
 public:
  static void deallocate(void* testObject, const char* className) {
    // ASSERT(strcmp(className, V1CLASSNAME1) == 0 || strcmp(className,
    // V1CLASSNAME2) == 0 , "Unexpected classname in deallocate()");
    LOGINFO("TestPdxSerializer::deallocate called");
    if (strcmp(className, V1CLASSNAME1) == 0) {
      PdxTests::TestKeyV1* tkv1 =
          reinterpret_cast<PdxTests::TestKeyV1*>(testObject);
      delete tkv1;
    } else if (strcmp(className, V1CLASSNAME2) == 0) {
      PdxTests::TestDiffTypePdxSV1* dtpv1 =
          reinterpret_cast<PdxTests::TestDiffTypePdxSV1*>(testObject);
      delete dtpv1;
    } else {
      LOGINFO("TestPdxSerializerForV1::deallocate Invalid Class Name");
    }
  }

  static uint32_t objectSize(void* testObject, const char* className) {
    // ASSERT(strcmp(className, V1CLASSNAME1) == 0 || strcmp(className,
    // V1CLASSNAME2) == 0, "Unexpected classname in objectSize()");
    LOGINFO("TestPdxSerializer::objectSize called");
    return 12345;  // dummy value
  }

  UserDeallocator getDeallocator(const char* className) {
    // ASSERT(strcmp(className, V1CLASSNAME1) == 0 || strcmp(className,
    // V1CLASSNAME2) == 0, "Unexpected classname in getDeallocator");
    return deallocate;
  }

  UserObjectSizer getObjectSizer(const char* className) {
    // ASSERT(strcmp(className, V1CLASSNAME1) == 0 || strcmp(className,
    // V1CLASSNAME2) == 0, "Unexpected classname in getObjectSizer");
    return objectSize;
  }

  void* fromDataForTestKeyV1(PdxReaderPtr pr) {
    try {
      PdxTests::TestKeyV1* tkv1 = new PdxTests::TestKeyV1;
      tkv1->_id = pr->readString("_id");
      return (void*)tkv1;
    } catch (...) {
      return NULL;
    }
  }

  bool toDataForTestKeyV1(void* testObject, PdxWriterPtr pw) {
    try {
      PdxTests::TestKeyV1* tkv1 =
          reinterpret_cast<PdxTests::TestKeyV1*>(testObject);
      pw->writeString("_id", tkv1->_id);

      return true;
    } catch (...) {
      return false;
    }
  }

  void* fromDataForTestDiffTypePdxSV1(PdxReaderPtr pr) {
    try {
      PdxTests::TestDiffTypePdxSV1* dtpv1 = new PdxTests::TestDiffTypePdxSV1;
      dtpv1->_id = pr->readString("_id");
      dtpv1->_name = pr->readString("_name");
      return (void*)dtpv1;
    } catch (...) {
      return NULL;
    }
  }

  bool toDataForTestDiffTypePdxSV1(void* testObject, PdxWriterPtr pw) {
    try {
      PdxTests::TestDiffTypePdxSV1* dtpv1 =
          reinterpret_cast<PdxTests::TestDiffTypePdxSV1*>(testObject);
      pw->writeString("_id", dtpv1->_id);
      pw->writeString("_name", dtpv1->_name);

      return true;
    } catch (...) {
      return false;
    }
  }

  void* fromData(const char* className, PdxReaderPtr pr) {
    // ASSERT(strcmp(className, V1CLASSNAME1) == 0 || strcmp(className,
    // V1CLASSNAME2) == 0, "Unexpected classname in fromData");

    if (strcmp(className, V1CLASSNAME2) == 0) {
      return fromDataForTestDiffTypePdxSV1(pr);

    } else if (strcmp(className, V1CLASSNAME1) == 0) {
      return fromDataForTestKeyV1(pr);

    } else {
      LOGINFO("TestPdxSerializerForV1::fromdata() Invalid Class Name");
      return NULL;
    }
  }

  bool toData(void* testObject, const char* className, PdxWriterPtr pw) {
    // ASSERT(strcmp(className, V1CLASSNAME1) == 0 || strcmp(className,
    // V1CLASSNAME2) == 0, "Unexpected classname in toData");

    if (strcmp(className, V1CLASSNAME2) == 0) {
      return toDataForTestDiffTypePdxSV1(testObject, pw);

    } else if (strcmp(className, V1CLASSNAME1) == 0) {
      return toDataForTestKeyV1(testObject, pw);

    } else {
      LOGINFO("TestPdxSerializerForV1::fromdata() Invalid Class Name");
      return false;
    }
  }
};

/************************************************************
 *  TestEqualsV1
 * *********************************************************/

class TESTOBJECT_EXPORT TestEqualsV1 : public PdxSerializable {
 private:
  int32_t i1;
  int32_t i2;
  char* s1;

 public:
  TestEqualsV1();

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const { return "PdxTests::TestEquals"; }

  static PdxSerializable* createDeserializable() { return new TestEqualsV1(); }
};

} /* namespace PdxTests */
#endif /* PDXCLASSV1_HPP_ */
