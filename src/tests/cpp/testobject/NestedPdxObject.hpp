/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* NestedPdxObject.hpp
*
*/

#ifndef __NESTEDPDXOBJECT_HPP__
#define __NESTEDPDXOBJECT_HPP__

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

namespace testobject {
enum Gender { male, female, other };
class TESTOBJECT_EXPORT ChildPdx : public PdxSerializable {
 private:
  int m_childId;
  char* m_childName;
  CacheableEnumPtr m_enum;

 public:
  ChildPdx() {}

  ChildPdx(int id) {
    m_childId = id;
    char buf[64] = {0};
    sprintf(buf, "name-%d ", id);
    LOGDEBUG("childPdx buf is %s ", buf);
    size_t strSize = strlen(buf) + 1;
    m_childName = new char[strSize];
    memcpy(m_childName, buf, strSize);
    LOGDEBUG("childPdx again buf is %s ", buf);
    m_enum = CacheableEnum::create("Gender", "female", 5);
  }

  virtual ~ChildPdx();

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(ChildPdx);
    return objectSize;
  }

  int32_t getChildId() { return m_childId; }

  char* getChildName() { return m_childName; }

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void toData(PdxWriterPtr pw);

  virtual void fromData(PdxReaderPtr pr);

  CacheableStringPtr toString() const;

  const char* getClassName() const { return "testobject::ChildPdx"; }

  static PdxSerializable* createDeserializable() { return new ChildPdx(); }

  bool equals(ChildPdx& other) const;
};
typedef SharedPtr<ChildPdx> ChildPdxPtr;

class TESTOBJECT_EXPORT ParentPdx : public PdxSerializable {
 private:
  int m_parentId;
  char* m_parentName;
  const wchar_t* m_wideparentName;
  wchar_t** m_wideparentArrayName;
  CacheablePtr m_childPdx;
  CacheableEnumPtr m_enum;
  int m_arrLength;
  char m_char;
  wchar_t m_wideChar;
  char* m_charArray;
  wchar_t* m_wideCharArray;

  int32_t m_charArrayLen;
  int32_t m_wcharArrayLen;

 public:
  ParentPdx() {}

  ParentPdx(int id) {
    m_parentId = id;
    char buf[64] = {0};
    sprintf(buf, "name-%d ", id);
    LOGDEBUG("buf is %s ", buf);
    size_t strSize = strlen(buf) + 1;
    m_parentName = new char[strSize];
    memcpy(m_parentName, buf, strSize);
    m_childPdx = new ChildPdx(id /** 1393*/);
    LOGDEBUG("parentPdx buf is %s ", buf);
    m_enum = CacheableEnum::create("Gender", "male", 6);
    m_wideparentName = L"Wide Parent name";
    LOGINFO("parentPdx m_wideparentName is %ls ", m_wideparentName);
    m_wideparentArrayName = new wchar_t*[3];
    const wchar_t* wstr1 = L"test1";
    const wchar_t* wstr2 = L"test2";
    const wchar_t* wstr3 = L"test3";
    size_t size = wcslen(wstr1);
    for (size_t i = 0; i < 3; i++) {
      m_wideparentArrayName[i] = new wchar_t[size];
    }
    m_wideparentArrayName[0] = (wchar_t*)wstr1;
    m_wideparentArrayName[1] = (wchar_t*)wstr2;
    m_wideparentArrayName[2] = (wchar_t*)wstr3;
    m_arrLength = 3;

    m_char = 'C';
    m_wideChar = L'a';

    m_charArray = new char[2];
    m_charArray[0] = 'X';
    m_charArray[1] = 'Y';

    m_wideCharArray = new wchar_t[2];
    m_wideCharArray[0] = L'x';
    m_wideCharArray[1] = L'y';

    m_charArrayLen = 2;
    m_wcharArrayLen = 2;
  }

  virtual ~ParentPdx();

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(ParentPdx);
    return objectSize;
  }

  int32_t getParentId() { return m_parentId; }

  const char* getParentName() { return m_parentName; }

  const wchar_t* getWideParentName() { return m_wideparentName; }

  wchar_t** getWideParentArrayName() { return m_wideparentArrayName; }

  ChildPdxPtr getChildPdx() { return m_childPdx; }

  CacheableEnumPtr getEnum() { return m_enum; }

  char getChar() { return m_char; }

  wchar_t getWideChar() { return m_wideChar; }

  char* getCharArray() { return m_charArray; }

  wchar_t* getWideCharArray() { return m_wideCharArray; }

  int32_t getCharArrayLength() { return m_charArrayLen; }

  int32_t getWideCharArrayLength() { return m_wcharArrayLen; }

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  virtual void toData(PdxWriterPtr pw);

  virtual void fromData(PdxReaderPtr pr);

  CacheableStringPtr toString() const;

  const char* getClassName() const { return "testobject::ParentPdx"; }

  static PdxSerializable* createDeserializable() { return new ParentPdx(); }

  bool equals(ParentPdx& other, bool isPdxReadSerialized) const;
};
typedef SharedPtr<ParentPdx> ParentPdxPtr;

enum enumQuerytest { id1, id2, id3 };

class TESTOBJECT_EXPORT PdxEnumTestClass : public PdxSerializable {
 private:
  int m_id;
  CacheableEnumPtr m_enumid;

 public:
  int getID() { return m_id; }

  CacheableEnumPtr getEnumID() { return m_enumid; }

  PdxEnumTestClass(int id) {
    m_id = id;
    switch (m_id) {
      case 0:
        LOGINFO("case 0 id1 = %d ", id1);
        m_enumid = CacheableEnum::create("enumQuerytest", "id1",
                                         /*enumQuerytest::*/ id1);
        break;
      case 1:
        LOGINFO("case 1 id2 = %d ", id2);
        m_enumid = CacheableEnum::create("enumQuerytest", "id2",
                                         /*enumQuerytest::*/ id2);
        break;
      case 2:
        LOGINFO("case 2 id3 = %d ", id3);
        m_enumid = CacheableEnum::create("enumQuerytest", "id3",
                                         /*enumQuerytest::*/ id3);
        break;
      default:
        LOGINFO("case default id1 = %d ", id1);
        m_enumid = CacheableEnum::create("enumQuerytest", "id1",
                                         /*enumQuerytest::*/ id1);
        break;
    }
  }

  PdxEnumTestClass() {}

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  void toData(PdxWriterPtr pw) {
    pw->writeInt("m_id", m_id);
    pw->writeObject("m_enumid", m_enumid);
  }

  void fromData(PdxReaderPtr pr) {
    m_id = pr->readInt("m_id");
    m_enumid = pr->readObject("m_enumid");
  }

  CacheableStringPtr toString() const {
    return CacheableString::create("PdxEnumTestClass");
  }

  const char* getClassName() const { return "testobject::PdxEnumTestClass"; }

  static PdxSerializable* createDeserializable() {
    return new PdxEnumTestClass();
  }
};
typedef SharedPtr<PdxEnumTestClass> PdxEnumTestClassPtr;

class TESTOBJECT_EXPORT SerializePdx : public PdxSerializable {
 private:
  int i1;
  int i2;
  const char* s1;
  const char* s2;

 public:
  SerializePdx() {}

  SerializePdx(bool init) {
    if (init) {
      i1 = 1;
      i2 = 2;
      s1 = "s1";
      s2 = "s2";
    } else {
      i1 = 0;
      i2 = 0;
      s1 = NULL;
      s2 = NULL;
    }
  }

  static PdxSerializable* createDeserializable() {
    return new SerializePdx(false);
  }

  using PdxSerializable::toData;
  using PdxSerializable::fromData;

  void toData(PdxWriterPtr pw) {
    pw->writeInt("i1", i1);
    pw->markIdentityField("i1");
    pw->writeInt("i2", i2);
    pw->writeString("s1", s1);
    pw->markIdentityField("s1");
    pw->writeString("s2", s2);
  }

  void fromData(PdxReaderPtr pr) {
    i1 = pr->readInt("i1");
    i2 = pr->readInt("i2");
    s1 = pr->readString("s1");
    s2 = pr->readString("s2");
  }

  CacheableStringPtr toString() const {
    return CacheableString::create("SerializePdx");
  }

  const char* getClassName() const { return "SerializePdx"; }

  bool equals(SerializePdx& other, bool isPdxReadSerialized) const {
    SerializePdx* ot = dynamic_cast<SerializePdx*>(&other);
    if (ot == NULL) {
      LOGINFO("SerializePdx::equals1");
      return false;
    }

    if (ot->i1 != i1 && ot->i2 != i2 && strcmp(ot->s1, s1) != 0 &&
        strcmp(ot->s2, s2) != 0) {
      LOGINFO("SerializePdx::equals2");
      return false;
    }
    LOGINFO("SerializePdx::equals3");
    return true;
  }
};
typedef SharedPtr<SerializePdx> SerializePdxPtr;
}
#endif /* __NESTEDPDXOBJECT_HPP__ */
