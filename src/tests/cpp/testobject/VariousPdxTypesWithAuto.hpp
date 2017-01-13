/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __VARIOUS_PDX_TYPES_WITH_AUTO_HPP_
#define __VARIOUS_PDX_TYPES_WITH_AUTO_HPP_

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
/************************************************************
 *  PdxTypes1
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes1 : public PdxSerializable {
 private:
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  PdxTypes1();

  virtual ~PdxTypes1();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /*{
    return "PdxTestsAuto.PdxTypes1";
  }*/
  int32_t getm_i1() { return m_i1; }
  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes1();
   }*/
};
typedef SharedPtr<PdxTypes1> PdxTypes1Ptr;

/************************************************************
 *  PdxTypes2
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes2 : public PdxSerializable {
 private:
  char* m_s1;  //"one"
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  PdxTypes2();

  virtual ~PdxTypes2();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto.PdxTypes2";
    }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes2();
   }*/
};
typedef SharedPtr<PdxTypes2> PdxTypes2Ptr;

/************************************************************
 *  PdxTypes3
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes3 : public PdxSerializable {
 private:
  char* m_s1;  //"one"
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  PdxTypes3();

  virtual ~PdxTypes3();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto.PdxTypes3";
    }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes3();
   }*/
};
typedef SharedPtr<PdxTypes3> PdxTypes3Ptr;

/************************************************************
 *  PdxTypes4
 * *********************************************************/
class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes4 : public PdxSerializable {
 private:
  char* m_s1;  //"one"
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  PdxTypes4();

  virtual ~PdxTypes4();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
     return "PdxTestsAuto.PdxTypes4";
   }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes4();
   }*/
};
typedef SharedPtr<PdxTypes4> PdxTypes4Ptr;

/************************************************************
 *  PdxTypes5
 * *********************************************************/
class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes5 : public PdxSerializable {
 private:
  char* m_s1;  //"one"
  char* m_s2;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  PdxTypes5();

  virtual ~PdxTypes5();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
    return "PdxTestsAuto.PdxTypes5";
  }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes5();
   }*/
};
typedef SharedPtr<PdxTypes5> PdxTypes5Ptr;

/************************************************************
 *  PdxTypes6
 * *********************************************************/
class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes6 : public PdxSerializable {
 private:
  char* m_s1;  //"one"
  char* m_s2;
  int8_t* bytes128;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;
  GFARRAYSIZE(bytes128) int32_t bytes128Len;

 public:
  PdxTypes6();

  virtual ~PdxTypes6();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto.PdxTypes6";
    }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes6();
   }*/
};
typedef SharedPtr<PdxTypes6> PdxTypes6Ptr;

/************************************************************
 *  PdxTypes7
 * *********************************************************/
class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes7 : public PdxSerializable {
 private:
  char* m_s1;  //"one"
  char* m_s2;
  int32_t m_i1;
  int8_t* bytes38000;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;
  GFARRAYSIZE(bytes38000) int32_t bytes38000Len;

 public:
  PdxTypes7();

  virtual ~PdxTypes7();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto.PdxTypes7";
    }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes7();
   }*/
};
typedef SharedPtr<PdxTypes7> PdxTypes7Ptr;

/************************************************************
 *  PdxTypes8
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes8 : public PdxSerializable {
 private:
  char* m_s1;  //"one"
  char* m_s2;
  int32_t m_i1;
  int8_t* bytes300;
  CacheablePtr _enum;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;
  GFARRAYSIZE(bytes300) int32_t bytes300Len;

 public:
  PdxTypes8();

  virtual ~PdxTypes8();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto.PdxTypes8";
    }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes8();
   }*/
};
typedef SharedPtr<PdxTypes8> PdxTypes8Ptr;

/************************************************************
 *  PdxTypes9
 * *********************************************************/
class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes9 : public PdxSerializable {
 private:
  char* m_s1;  //"one"
  char* m_s2;
  char* m_s3;
  int32_t m_i1;
  int8_t* m_bytes66000;
  char* m_s4;
  char* m_s5;
  GFARRAYSIZE(m_bytes66000) int32_t m_bytes66000Len;

 public:
  PdxTypes9();

  virtual ~PdxTypes9();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
     return "PdxTestsAuto.PdxTypes9";
   }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes9();
   }*/
};
typedef SharedPtr<PdxTypes9> PdxTypes9Ptr;

/************************************************************
 *  PdxTypes10
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxTypes10 : public PdxSerializable {
 private:
  char* m_s1;  //"one"
  char* m_s2;
  char* m_s3;
  int32_t m_i1;
  int8_t* m_bytes66000;
  char* m_s4;
  char* m_s5;
  GFARRAYSIZE(m_bytes66000) int32_t m_bytes66000Len;

 public:
  PdxTypes10();

  virtual ~PdxTypes10();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto.PdxTypes10";
    }*/

  static PdxSerializable* createDeserializable(); /* {
     return new PdxTypes10();
   }*/
};
typedef SharedPtr<PdxTypes10> PdxTypes10Ptr;

/************************************************************
 *  NestedPdx
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) NestedPdx : public PdxSerializable {
 private:
  PdxTypes1Ptr m_pd1;
  PdxTypes2Ptr m_pd2;
  char* m_s1;  //"one"
  char* m_s2;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  NestedPdx();
  NestedPdx(char* key);

  virtual ~NestedPdx();

  int32_t getHashCode();

  bool equals(PdxSerializablePtr obj);

  CacheableStringPtr toString() const;

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void fromData(PdxReaderPtr pr);

  virtual void toData(PdxWriterPtr pw);

  const char* getClassName() const; /* {
      return "PdxTestsAuto::NestedPdx";
    }*/
  const char* getString() { return m_s1; }

  static PdxSerializable* createDeserializable(); /* {
     return new NestedPdx();
   }*/
};
typedef SharedPtr<NestedPdx> NestedPdxPtr;

/************************************************************
 *  PdxInsideIGFSerializable
 * *********************************************************/

class GFIGNORE(TESTOBJECT_EXPORT) PdxInsideIGFSerializable
    : public Serializable {
 private:
  NestedPdxPtr m_npdx;
  PdxTypes3Ptr m_pdx3;

  char* m_s1;  //"one"
  char* m_s2;
  int32_t m_i1;
  int32_t m_i2;
  int32_t m_i3;
  int32_t m_i4;

 public:
  PdxInsideIGFSerializable();

  virtual ~PdxInsideIGFSerializable();

  int32_t getHashCode();

  bool equals(SerializablePtr obj);

  CacheableStringPtr toString() const;

  virtual Serializable* fromData(DataInput& input);

  virtual void toData(DataOutput& output) const;

  virtual int32_t classId() const { return 0x10; }

  const char* getClassName() const {
    return "PdxTestsAuto::PdxInsideIGFSerializable";
  }

  static Serializable* createDeserializable() {
    return new PdxInsideIGFSerializable();
  }
};
typedef SharedPtr<PdxInsideIGFSerializable> PdxInsideIGFSerializablePtr;

} /* namespace PdxTestsAuto */
#endif /* __VARIOUSPDXTYPES_HPP_ */
