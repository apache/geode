/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __PDX_CLASSV2_WITH_AUTO_HPP__
#define __PDX_CLASSV2_WITH_AUTO_HPP__

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
#define GFUNREAD

namespace PdxTestsAuto {

/************************************************************
 *  PdxTypes1V2
 * *********************************************************/
class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes1V2 : public PdxSerializable {
 private:
  GFEXCLUDE static int32_t m_diffInSameFields;
  GFEXCLUDE static int32_t m_diffInExtraFields;
  GFEXCLUDE static bool m_useWeakHashMap;
  GFUNREAD PdxUnreadFieldsPtr m_pdxUreadFields;

  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;
  int32_t m_i5;
  int32_t m_i6;

 public:
  PdxTypes1V2();

  virtual ~PdxTypes1V2();

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
     return "PdxTestsAuto::PdxTypes1V";
   }*/

  static PdxSerializable* createDeserializable(); /*{
    return new PdxTypes1V2();
  }*/
};
typedef SharedPtr<PdxTypes1V2> PdxTypes1V2Ptr;

/************************************************************
 *  PdxTypes2V2
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes2V2 : public PdxSerializable {
 private:
  GFEXCLUDE static int m_diffInSameFields;
  GFEXCLUDE static int m_diffInExtraFields;
  GFEXCLUDE static bool m_useWeakHashMap;
  GFUNREAD PdxUnreadFieldsPtr m_unreadFields;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;
  int32_t m_i5;
  int32_t m_i6;

 public:
  PdxTypes2V2();

  virtual ~PdxTypes2V2();

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /*{
     return "PdxTestsAuto::PdxTypes2V";
         }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes2V2();
   }*/
};
typedef SharedPtr<PdxTypes2V2> PdxTypes2V2Ptr;

/************************************************************
 *  PdxTypes3V2
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes3V2 : public PdxSerializable {
 private:
  GFEXCLUDE static int m_diffInSameFields;
  GFEXCLUDE static int m_diffInExtraFields;
  GFEXCLUDE static bool m_useWeakHashMap;
  GFUNREAD PdxUnreadFieldsPtr m_unreadFields;
  int32_t m_i1;
  int32_t m_i2;
  char* m_str1;
  int32_t m_i4;
  int32_t m_i3;
  int32_t m_i6;
  char* m_str3;

 public:
  PdxTypes3V2();

  virtual ~PdxTypes3V2();

  CacheableStringPtr toString() const;

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto::PdxTypes3V";
          }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes3V2();
   }*/
};
typedef SharedPtr<PdxTypes3V2> PdxTypes3V2Ptr;

/************************************************************
 *  PdxTypesR1V2
 **********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxTypesR1V2 : public PdxSerializable {
 private:
  GFEXCLUDE static int32_t m_diffInSameFields;
  GFEXCLUDE static int32_t m_diffInExtraFields;
  GFEXCLUDE static bool m_useWeakHashMap;
  GFUNREAD PdxUnreadFieldsPtr m_pdxUnreadFields;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

  // Fields not in V1.
  int32_t m_i5;
  int32_t m_i6;

 public:
  PdxTypesR1V2();

  virtual ~PdxTypesR1V2();

  CacheableStringPtr toString() const;

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto::PdxTypesR1";
          }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypesR1V2();
   }*/
};
typedef SharedPtr<PdxTypesR1V2> PdxTypesR1V2Ptr;

/************************************************************
 *  PdxTypesR2V2
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxTypesR2V2 : public PdxSerializable {
 private:
  GFEXCLUDE static int m_diffInSameFields;
  GFEXCLUDE static int m_diffInExtraFields;
  GFEXCLUDE static bool m_useWeakHashMap;
  GFUNREAD PdxUnreadFieldsPtr m_pdxunreadFields;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;
  int32_t m_i5;
  int32_t m_i6;

  char* m_str1;

 public:
  PdxTypesR2V2();

  virtual ~PdxTypesR2V2();

  static void reset(bool useWeakHashMap);

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto::PdxTypesR2";
          }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypesR2V2();
   }*/
};
typedef SharedPtr<PdxTypesR2V2> PdxTypesR2V2Ptr;

/************************************************************
 *  PdxTypesIgnoreUnreadFieldsV2
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxTypesIgnoreUnreadFieldsV2
    : public PdxSerializable {
 private:
  GFEXCLUDE static int m_diffInSameFields;
  GFEXCLUDE static int m_diffInExtraFields;
  GFEXCLUDE static bool m_useWeakHashMap;
  GFUNREAD PdxUnreadFieldsPtr m_unreadFields;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;
  int32_t m_i5;
  int32_t m_i6;

 public:
  PdxTypesIgnoreUnreadFieldsV2();

  virtual ~PdxTypesIgnoreUnreadFieldsV2();

  CacheableStringPtr toString() const;

  static void reset(bool useWeakHashMap);

  void updateMembers();

  int getHashCode();

  bool equals(PdxSerializablePtr obj);

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto::PdxTypesIgnoreUnreadFields";
          }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypesIgnoreUnreadFieldsV2();
   }*/
};
typedef SharedPtr<PdxTypesIgnoreUnreadFieldsV2> PdxTypesIgnoreUnreadFieldsV2Ptr;

/************************************************************
 *  PdxVersionedV2
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxVersionedV2 : public PdxSerializable {
 private:
  GFEXCLUDE char m_char;
  bool m_bool;
  int8_t m_byte;
  int16_t m_int16;
  int32_t m_int32;
  int64_t m_long;
  float m_float;
  double m_double;
  char* m_string;
  bool* m_boolArray;
  GFEXCLUDE GFEXCLUDE char* m_charArray;
  CacheableDatePtr m_dateTime;
  int16_t* m_int16Array;
  int32_t* m_int32Array;
  int64_t* m_longArray;
  float* m_floatArray;
  double* m_doubleArray;
  GFEXCLUDE int8_t** m_byteByteArray;  // byte[][]
  GFEXCLUDE char** m_stringArray;      // string[]

  // IDictionary<object, object> m_map;
  // List<object> m_list;

  GFARRAYSIZE(m_boolArray) int32_t boolArrayLen;
  int32_t byteArrayLen;
  GFARRAYSIZE(m_int16Array) int32_t shortArrayLen;
  GFARRAYSIZE(m_int32Array) int32_t intArrayLen;
  GFARRAYSIZE(m_longArray) int32_t longArrayLen;
  GFARRAYSIZE(m_doubleArray) int32_t doubleArrayLen;
  GFARRAYSIZE(m_floatArray) int32_t floatArrayLen;
  int32_t strLenArray;

 public:
  PdxVersionedV2();

  PdxVersionedV2(int32_t size);

  virtual ~PdxVersionedV2();

  void init(int32_t size);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /*{
     return "PdxTestsAuto::PdxVersioned";
   }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxVersionedV2();
   }*/
};
typedef SharedPtr<PdxVersionedV2> PdxVersionedV2Ptr;

/************************************************************
 *  TestKey
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) TestKeyV2 {
 public:
  char* _id;

 public:
  TestKeyV2();

  TestKeyV2(char* id);
};

/************************************************************
 *  TestDiffTypePdxSV2
 * *********************************************************/
// TODO: Enable it once the PdxSerializer is done

class GFIGNORE(TESTOBJECT_EXPORT) TestDiffTypePdxSV2 {
 public:
  char* _id;
  char* _name;
  int _count;

 public:
  TestDiffTypePdxSV2();

  TestDiffTypePdxSV2(bool init);

  bool equals(TestDiffTypePdxSV2* obj);
};

/************************************************************
 *  TestPdxSerializerForV2
 * *********************************************************/
static const char* V2CLASSNAME3 = "PdxTestsAuto.TestKey";
static const char* V2CLASSNAME4 = "PdxTestsAuto.TestDiffTypePdxS";

class TestPdxSerializerForV2 : public PdxSerializer {
 public:
  static void deallocate(void* testObject, const char* className) {
    // ASSERT(strcmp(className, V2CLASSNAME3) == 0 || strcmp(className,
    // V2CLASSNAME4) == 0 , "Unexpected classname in deallocate()");
    LOGINFO("TestPdxSerializerForV2::deallocate called");
    if (strcmp(className, V2CLASSNAME3) == 0) {
      PdxTestsAuto::TestKeyV2* tkv1 =
          reinterpret_cast<PdxTestsAuto::TestKeyV2*>(testObject);
      delete tkv1;
    } else if (strcmp(className, V2CLASSNAME4) == 0) {
      PdxTestsAuto::TestDiffTypePdxSV2* dtpv1 =
          reinterpret_cast<PdxTestsAuto::TestDiffTypePdxSV2*>(testObject);
      delete dtpv1;
    } else {
      LOGINFO("TestPdxSerializerForV1::deallocate Invalid Class Name");
    }
  }

  static uint32_t objectSize(void* testObject, const char* className) {
    // ASSERT(strcmp(className, V2CLASSNAME3) == 0 || strcmp(className,
    // V2CLASSNAME4) == 0, "Unexpected classname in objectSize()");
    LOGINFO("TestPdxSerializer::objectSize called");
    return 12345;  // dummy value
  }

  UserDeallocator getDeallocator(const char* className) {
    // ASSERT(strcmp(className, V2CLASSNAME3) == 0 || strcmp(className,
    // V2CLASSNAME4) == 0, "Unexpected classname in getDeallocator");
    return deallocate;
  }

  UserObjectSizer getObjectSizer(const char* className) {
    // ASSERT(strcmp(className, V2CLASSNAME3) == 0 || strcmp(className,
    // V2CLASSNAME4) == 0, "Unexpected classname in getObjectSizer");
    return objectSize;
  }

  void* fromDataForTestKeyV2(PdxReaderPtr pr) {
    try {
      PdxTestsAuto::TestKeyV2* tkv1 = new PdxTestsAuto::TestKeyV2;
      tkv1->_id = pr->readString("_id");
      return (void*)tkv1;
    } catch (...) {
      return NULL;
    }
  }

  bool toDataForTestKeyV2(void* testObject, PdxWriterPtr pw) {
    try {
      PdxTestsAuto::TestKeyV2* tkv1 =
          reinterpret_cast<PdxTestsAuto::TestKeyV2*>(testObject);
      pw->writeString("_id", tkv1->_id);

      return true;
    } catch (...) {
      return false;
    }
  }

  void* fromDataForTestDiffTypePdxSV2(PdxReaderPtr pr) {
    try {
      PdxTestsAuto::TestDiffTypePdxSV2* dtpv1 =
          new PdxTestsAuto::TestDiffTypePdxSV2;
      dtpv1->_id = pr->readString("_id");
      dtpv1->_name = pr->readString("_name");
      dtpv1->_count = pr->readInt("_count");
      return (void*)dtpv1;
    } catch (...) {
      return NULL;
    }
  }

  bool toDataForTestDiffTypePdxSV2(void* testObject, PdxWriterPtr pw) {
    try {
      PdxTestsAuto::TestDiffTypePdxSV2* dtpv1 =
          reinterpret_cast<PdxTestsAuto::TestDiffTypePdxSV2*>(testObject);
      pw->writeString("_id", dtpv1->_id);
      pw->markIdentityField("_id");
      pw->writeString("_name", dtpv1->_name);
      pw->writeInt("_count", dtpv1->_count);

      return true;
    } catch (...) {
      return false;
    }
  }

  // using PdxSerializable::toData;
  // using PdxSerializable::fromData;

  // void * fromData(const char * className, PdxReaderPtr pr)
  //{
  //  //ASSERT(strcmp(className, V2CLASSNAME3) == 0 || strcmp(className,
  //  V2CLASSNAME4) == 0, "Unexpected classname in fromData");

  //  if (strcmp(className, V2CLASSNAME4) == 0) {
  //    return fromDataForTestDiffTypePdxSV2(pr);

  //  }else if(strcmp(className, V2CLASSNAME3) == 0){
  //  	return fromDataForTestKeyV2(pr);

  //  }else{
  //    LOGINFO("TestPdxSerializerForV2::fromdata() Invalid Class Name");
  //    return NULL;
  //  }
  //}

  // bool toData(void * testObject, const char * className, PdxWriterPtr pw)
  //{
  //  //ASSERT(strcmp(className, V2CLASSNAME3) == 0 || strcmp(className,
  //  V2CLASSNAME4) == 0, "Unexpected classname in toData");

  //  if (strcmp(className, V2CLASSNAME4) == 0) {
  //    return toDataForTestDiffTypePdxSV2(testObject, pw);

  //  } else if(strcmp(className, V2CLASSNAME3) == 0){
  //  	return toDataForTestKeyV2(testObject, pw);

  //  }else{
  //    LOGINFO("TestPdxSerializerForV1::fromdata() Invalid Class Name");
  //    return false;
  //  }
  //}
};

/************************************************************
 *  TestEqualsV1
 * *********************************************************/
/*
class GFIGNORE(TESTOBJECT_EXPORT) TestEqualsV1 : public PdxSerializable {

private:

        int32_t i1;
        int32_t i2;
        char* s1;
        char** sArr;
        int32_t* intArr;

public:

        TestEqualsV1();

        CacheableStringPtr toString() const;

    using PdxSerializable::toData;
    using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw) ;

  const char* getClassName()const {
    return "PdxTestsAuto::TestEquals";
  }

  static PdxSerializable* createDeserializable() {
    return new TestEqualsV1();
  }
};
*/

} /* namespace PdxTestsAuto */
#endif /* PDXCLASSV1_HPP_ */
