/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* NestedPdxObject.cpp
*
*/

#include "NestedPdxObject.hpp"

using namespace gemfire;
using namespace testobject;

ChildPdx::~ChildPdx() {}

void ChildPdx::toData(PdxWriterPtr pw) {
  LOGDEBUG("ChildPdx::toData() Started......");

  pw->writeInt("m_childId", m_childId);
  pw->markIdentityField("m_childId");
  pw->writeObject("m_enum", m_enum);
  pw->writeString("m_childName", m_childName);

  LOGDEBUG("ChildPdx::toData() Done......");
}

void ChildPdx::fromData(PdxReaderPtr pr) {
  LOGINFO("ChildPdx::fromData() start...");

  m_childId = pr->readInt("m_childId");
  LOGINFO("ChildPdx::fromData() m_childId = %d ", m_childId);
  m_enum = pr->readObject("m_enum");
  m_childName = pr->readString("m_childName");

  LOGINFO("ChildPdx::fromData() end...");
}

CacheableStringPtr ChildPdx::toString() const {
  char idbuf[1024];
  sprintf(idbuf, "ChildPdx: [m_childId=%d] [ m_childName=%s ]", m_childId,
          m_childName);
  return CacheableString::create(idbuf);
}

bool ChildPdx::equals(ChildPdx& other) const {
  LOGINFO("ChildPdx::equals");
  ChildPdx* ot = dynamic_cast<ChildPdx*>(&other);
  // Cacheable* ot = dynamic_cast<Cacheable*>(&other);
  if (ot == NULL) {
    LOGINFO("ChildPdx::equals1");
    return false;
  }
  if ((strcmp(m_childName, other.m_childName) == 0) &&
      (m_childId == other.m_childId) &&
      (m_enum->getEnumOrdinal() == other.m_enum->getEnumOrdinal()) &&
      (strcmp(m_enum->getEnumClassName(), other.m_enum->getEnumClassName()) ==
       0) &&
      (strcmp(m_enum->getEnumName(), other.m_enum->getEnumName()) == 0)) {
    LOGINFO("ChildPdx::equals2");
    return true;
  }
  return false;
}

ParentPdx::~ParentPdx() {}

void ParentPdx::toData(PdxWriterPtr pw) {
  LOGDEBUG("ParentPdx::toData() Started......");

  pw->writeInt("m_parentId", m_parentId);
  LOGDEBUG("ParentPdx::toData() m_parentId......");
  pw->markIdentityField("m_parentId");
  pw->writeObject("m_enum", m_enum);
  LOGDEBUG("ParentPdx::toData() m_enum......");
  pw->writeString("m_parentName", m_parentName);
  LOGDEBUG("ParentPdx::toData() m_parentName......");
  pw->writeWideString("m_wideparentName", m_wideparentName);
  LOGDEBUG("ParentPdx::toData() m_wideparentName......");
  pw->writeWideStringArray("m_wideparentArrayName", m_wideparentArrayName, 3);
  LOGDEBUG("ParentPdx::toData() m_wideparentArrayName......");
  pw->writeObject("m_childPdx", (ChildPdxPtr)m_childPdx);
  LOGDEBUG("ParentPdx::toData() m_childPdx......");
  pw->markIdentityField("m_childPdx");

  pw->writeChar("m_char", m_char);
  pw->writeWideChar("m_wideChar", m_wideChar);
  pw->writeCharArray("m_charArray", m_charArray, 2);
  pw->writeWideCharArray("m_wideCharArray", m_wideCharArray, 2);

  LOGDEBUG("ParentPdx::toData() Done......");
}

void ParentPdx::fromData(PdxReaderPtr pr) {
  LOGINFO("ParentPdx::fromData() start...");

  m_parentId = pr->readInt("m_parentId");
  LOGINFO("ParentPdx::fromData() m_parentId = %d ", m_parentId);
  m_enum = pr->readObject("m_enum");
  LOGINFO("ParentPdx::fromData() read gender ");
  m_parentName = pr->readString("m_parentName");
  LOGINFO("ParentPdx::fromData() m_parentName = %s ", m_parentName);
  m_wideparentName = pr->readWideString("m_wideparentName");
  LOGINFO("ParentPdx::fromData() m_wideparentName = %ls ", m_wideparentName);
  m_wideparentArrayName =
      pr->readWideStringArray("m_wideparentArrayName", m_arrLength);
  for (int i = 0; i < 3; i++) {
    LOGINFO("ParentPdx:: m_wideparentArrayName[i] = %ls ",
            m_wideparentArrayName[i]);
  }
  LOGINFO("ParentPdx::fromData() m_wideparentArrayName done ");
  m_childPdx = /*dynCast<SerializablePtr>*/ (pr->readObject("m_childPdx"));
  LOGINFO("ParentPdx::fromData() start3...");

  m_char = pr->readChar("m_char");
  m_wideChar = pr->readWideChar("m_wideChar");
  m_charArray = pr->readCharArray("m_charArray", m_charArrayLen);
  m_wideCharArray = pr->readWideCharArray("m_wideCharArray", m_wcharArrayLen);

  LOGINFO("ParentPdx::fromData() end...");
}

CacheableStringPtr ParentPdx::toString() const {
  char idbuf[1024];
  sprintf(idbuf,
          "ParentPdx: [m_parentId=%d] [ m_parentName=%s ] [ "
          "m_wideparentName=%ls ] [m_childPdx = %s ] ",
          m_parentId, m_parentName, m_wideparentName,
          m_childPdx->toString()->asChar());
  return CacheableString::create(idbuf);
}

bool ParentPdx::equals(ParentPdx& other, bool isPdxReadSerialized) const {
  LOGINFO("ParentPdx::equals");
  ParentPdx* ot = dynamic_cast<ParentPdx*>(&other);
  if (ot == NULL) {
    LOGINFO("ParentPdx::equals1");
    return false;
  }
  if ((strcmp(m_parentName, other.m_parentName) == 0) &&
      (wcscmp(m_wideparentName, other.m_wideparentName) == 0) &&
      (m_parentId == other.m_parentId) &&
      (m_enum->getEnumOrdinal() == other.m_enum->getEnumOrdinal()) &&
      (strcmp(m_enum->getEnumClassName(), other.m_enum->getEnumClassName()) ==
       0) &&
      (strcmp(m_enum->getEnumName(), other.m_enum->getEnumName()) == 0) &&
      m_arrLength == other.m_arrLength &&
      m_charArrayLen == other.m_charArrayLen &&
      m_wcharArrayLen == other.m_wcharArrayLen && m_char == other.m_char &&
      m_wideChar == other.m_wideChar) {
    LOGINFO("ParentPdx::equals2");

    for (int i = 0; i < m_arrLength; i++) {
      if ((wcscmp(m_wideparentArrayName[i], other.m_wideparentArrayName[i]) !=
           0)) {
        LOGINFO("ParentPdx::equals2 not wcscmp");
        return false;
      }
    }

    for (int j = 0; j < m_charArrayLen; j++) {
      if (m_charArray[j] != other.m_charArray[j]) {
        LOGINFO("ParentPdx::equals not char array ");
        return false;
      }
    }

    for (int k = 0; k < m_wcharArrayLen; k++) {
      if (m_wideCharArray[k] != other.m_wideCharArray[k]) {
        LOGINFO("ParentPdx::equals not wide char array ");
        return false;
      }
    }

    if (!isPdxReadSerialized) {
      ChildPdx* ch1 = dynamic_cast<ChildPdx*>(m_childPdx.ptr());
      ChildPdx* ch2 = dynamic_cast<ChildPdx*>(other.m_childPdx.ptr());

      if (ch1->equals(*ch2)) {
        LOGINFO("ParentPdx::equals3");
        return true;
      }
    }
    return true;
  }
  LOGINFO("ParentPdx:: not equals");
  return false;
}
