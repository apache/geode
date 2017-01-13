/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxClassV1.cpp
 *
 *  Created on: Feb 3, 2012
 *      Author: npatel
 */

#include "PdxClassV2.hpp"

using namespace gemfire;
using namespace PdxTests;

/************************************************************
 *  PdxType1V2
 * *********************************************************/
int32_t PdxTypes1V2::m_diffInSameFields = 0;
int32_t PdxTypes1V2::m_diffInExtraFields = 0;
bool PdxTypes1V2::m_useWeakHashMap = false;

PdxTypes1V2::PdxTypes1V2() {
  m_i1 = 34324;
  m_i2 = 2144;
  m_i3 = 4645734;
  m_i4 = 73567;
  m_i5 = 0;
  m_i6 = 0;
}

PdxTypes1V2::~PdxTypes1V2() {
  // TODO Auto-generated destructor stub
}

void PdxTypes1V2::reset(bool useWeakHashMap) {
  PdxTypes1V2::m_diffInSameFields = 0;
  PdxTypes1V2::m_diffInExtraFields = 0;
  PdxTypes1V2::m_useWeakHashMap = useWeakHashMap;
}

int PdxTypes1V2::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxTypes1V2::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxTypes1V2Ptr pap = dynCast<PdxTypes1V2Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4 &&
      m_i5 + m_diffInExtraFields == pap->m_i5 &&
      m_i6 + m_diffInExtraFields == pap->m_i6) {
    return true;
  }

  return false;
}

void PdxTypes1V2::toData(PdxWriterPtr pw) {
  //
  if (!m_useWeakHashMap) pw->writeUnreadFields(m_pdxUreadFields);

  pw->writeInt("i1", m_i1 + 1);
  pw->markIdentityField("i1");
  pw->writeInt("i2", m_i2 + 1);
  pw->writeInt("i3", m_i3 + 1);
  pw->writeInt("i4", m_i4 + 1);
  pw->writeInt("i5", m_i5 + 1);
  pw->writeInt("i6", m_i6 + 1);

  m_diffInSameFields++;
  m_diffInExtraFields++;
  // LOGDEBUG("PdxObject::toData() Done......");
}

void PdxTypes1V2::fromData(PdxReaderPtr pr) {
  // LOGDEBUG("PdxObject::fromData() start...");
  m_i1 = pr->readInt("i1");
  bool isIdentity = pr->isIdentityField("i2");
  if (isIdentity) throw Exception("i2 is not identity field");
  //
  if (!m_useWeakHashMap) m_pdxUreadFields = pr->readUnreadFields();

  m_i2 = pr->readInt("i2");
  m_i3 = pr->readInt("i3");
  m_i4 = pr->readInt("i4");
  m_i5 = pr->readInt("i5");
  m_i6 = pr->readInt("i6");

  // LOGDEBUG("PdxType1V2::fromData() End...");
}

CacheableStringPtr PdxTypes1V2::toString() const {
  char idbuf[4096];
  sprintf(idbuf,
          "PdxType1V1:[ m_i1=%d ] [ m_i2=%d ] [ m_i3=%d ] [ m_i4=%d ] [ "
          "m_i5=%d ] [ m_i6=%d ] [ m_diffInExtraFields=%d ]",
          m_i1, m_i2, m_i3, m_i4, m_i5, m_i6, m_diffInExtraFields);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxTypes2V2
 * *********************************************************/

int PdxTypes2V2::m_diffInSameFields = 0;
int PdxTypes2V2::m_diffInExtraFields = 0;
bool PdxTypes2V2::m_useWeakHashMap = false;

PdxTypes2V2::PdxTypes2V2() {
  m_i1 = 34324;
  m_i2 = 2144;
  m_i3 = 4645734;
  m_i4 = 73567;
  m_i5 = 0;
  m_i6 = 0;
}

PdxTypes2V2::~PdxTypes2V2() {
  // TODO Auto-generated destructor stub
}

void PdxTypes2V2::reset(bool useWeakHashMap) {
  PdxTypes2V2::m_diffInSameFields = 0;
  PdxTypes2V2::m_diffInExtraFields = 0;
  PdxTypes2V2::m_useWeakHashMap = useWeakHashMap;
}

int PdxTypes2V2::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxTypes2V2::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxTypes2V2Ptr pap = dynCast<PdxTypes2V2Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4 &&
      m_i5 + m_diffInExtraFields == pap->m_i5 &&
      m_i6 + m_diffInExtraFields == pap->m_i6) {
    return true;
  }

  return false;
}

void PdxTypes2V2::toData(PdxWriterPtr pw) {
  if (!m_useWeakHashMap) pw->writeUnreadFields(m_unreadFields);

  pw->writeInt("i1", m_i1 + 1);
  pw->writeInt("i2", m_i2 + 1);
  pw->writeInt("i5", m_i5 + 1);
  pw->writeInt("i6", m_i6 + 1);
  pw->writeInt("i3", m_i3 + 1);
  pw->writeInt("i4", m_i4 + 1);

  m_diffInExtraFields++;
  m_diffInSameFields++;
}

void PdxTypes2V2::fromData(PdxReaderPtr pr) {
  // LOGDEBUG("PdxObject::fromData() start...");
  //
  if (!m_useWeakHashMap) m_unreadFields = pr->readUnreadFields();

  m_i1 = pr->readInt("i1");
  m_i2 = pr->readInt("i2");
  m_i5 = pr->readInt("i5");
  m_i6 = pr->readInt("i6");
  m_i3 = pr->readInt("i3");
  m_i4 = pr->readInt("i4");
}

CacheableStringPtr PdxTypes2V2::toString() const {
  char idbuf[4096];
  sprintf(idbuf,
          "PdxTypes2V2:[ m_i1=%d ] [ m_i2=%d ] [ m_i3=%d ] [ m_i4=%d ] [ "
          "m_i5=%d ] [ m_i6=%d ]",
          m_i1, m_i2, m_i3, m_i4, m_i5, m_i6);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxTypes3V2
 * *********************************************************/

int PdxTypes3V2::m_diffInSameFields = 0;
int PdxTypes3V2::m_diffInExtraFields = 0;
bool PdxTypes3V2::m_useWeakHashMap = false;

PdxTypes3V2::PdxTypes3V2() {
  m_i1 = 1;
  m_i2 = 21;
  m_str1 = (char *)"common";
  m_i4 = 41;
  m_i3 = 31;
  m_i6 = 0;
  m_str3 = (char *)"0";
}

PdxTypes3V2::~PdxTypes3V2() {
  // TODO Auto-generated destructor stub
}

void PdxTypes3V2::reset(bool useWeakHashMap) {
  PdxTypes3V2::m_diffInSameFields = 0;
  PdxTypes3V2::m_diffInExtraFields = 0;
  PdxTypes3V2::m_useWeakHashMap = useWeakHashMap;
}

int PdxTypes3V2::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxTypes3V2::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxTypes3V2Ptr pap = dynCast<PdxTypes3V2Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4 &&
      m_i6 + m_diffInExtraFields == pap->m_i6) {
    if (strcmp(m_str1, pap->m_str1) == 0 && strcmp(m_str3, pap->m_str3) <= 0) {
      return true;
    }
  }
  return false;
}

void PdxTypes3V2::toData(PdxWriterPtr pw) {
  //
  if (!m_useWeakHashMap) pw->writeUnreadFields(m_unreadFields);

  pw->writeInt("i1", m_i1 + 1);
  pw->writeInt("i2", m_i2 + 1);
  pw->writeString("m_str1", m_str1);
  pw->writeInt("i4", m_i4 + 1);
  pw->writeInt("i3", m_i3 + 1);
  pw->writeInt("i6", m_i6 + 1);
  pw->writeString("m_str3", m_str3);

  m_diffInExtraFields++;
  m_diffInSameFields++;
}

void PdxTypes3V2::fromData(PdxReaderPtr pr) {
  // LOGDEBUG("PdxObject::fromData() start...");
  //
  if (!m_useWeakHashMap) m_unreadFields = pr->readUnreadFields();

  m_i1 = pr->readInt("i1");
  m_i2 = pr->readInt("i2");
  m_str1 = pr->readString("m_str1");
  m_i4 = pr->readInt("i4");
  m_i3 = pr->readInt("i3");
  m_i6 = pr->readInt("i6");
  char *tmp = pr->readString("m_str3");

  char extraFieldsStr[20];
  if (tmp == NULL) {
    sprintf(extraFieldsStr, "%d", m_diffInExtraFields);
    size_t strSize = strlen(extraFieldsStr) + 1;
    m_str3 = new char[strSize];
    memcpy(m_str3, extraFieldsStr, strSize);
  } else {
    sprintf(extraFieldsStr,
            "%d"
            "%s",
            m_diffInExtraFields, m_str3);
    size_t strSize = strlen(extraFieldsStr) + 1;
    m_str3 = new char[strSize];
    memcpy(m_str3, extraFieldsStr, strSize);
  }
}

CacheableStringPtr PdxTypes3V2::toString() const {
  char idbuf[4096];
  sprintf(idbuf,
          "PdxTypes3V2:[ m_i1=%d ] [ m_i2=%d ] [m_str1=%s] [ m_i3=%d ] [ "
          "m_i4=%d ] [ m_i6=%d ] [m_str3=%s]",
          m_i1, m_i2, m_str1, m_i3, m_i4, m_i6, m_str3);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxTypesR1V2
 * *********************************************************/

int32_t PdxTypesR1V2::m_diffInSameFields = 0;
int32_t PdxTypesR1V2::m_diffInExtraFields = 0;
bool PdxTypesR1V2::m_useWeakHashMap = false;

PdxTypesR1V2::PdxTypesR1V2() {
  m_i1 = 34324;
  m_i2 = 2144;
  m_i3 = 4645734;
  m_i4 = 73567;

  m_i5 = 721398;
  m_i6 = 45987;
}

PdxTypesR1V2::~PdxTypesR1V2() {
  // TODO Auto-generated destructor stub
}

void PdxTypesR1V2::reset(bool useWeakHashMap) {
  PdxTypesR1V2::m_diffInSameFields = 0;
  PdxTypesR1V2::m_diffInExtraFields = 0;
  PdxTypesR1V2::m_useWeakHashMap = useWeakHashMap;
}

int PdxTypesR1V2::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxTypesR1V2::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxTypesR1V2Ptr pap = dynCast<PdxTypesR1V2Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4) {
    if (((m_i5 + PdxTypesR1V2::m_diffInExtraFields == pap->m_i5) &&
         (m_i6 + PdxTypesR1V2::m_diffInExtraFields == pap->m_i6)) ||
        ((PdxTypesR1V2::m_diffInExtraFields == pap->m_i5) &&
         (PdxTypesR1V2::m_diffInExtraFields == pap->m_i6))) {
      return true;
    }
  }
  return false;
}

void PdxTypesR1V2::toData(PdxWriterPtr pw) {
  if (!m_useWeakHashMap) pw->writeUnreadFields(m_pdxUnreadFields);

  pw->writeInt("i1", m_i1 + 1);
  pw->writeInt("i2", m_i2 + 1);
  pw->writeInt("i3", m_i3 + 1);
  pw->writeInt("i4", m_i4 + 1);

  pw->writeInt("i5", m_i5 + 1);
  pw->writeInt("i6", m_i6 + 1);

  m_diffInSameFields++;
  m_diffInExtraFields++;
}

void PdxTypesR1V2::fromData(PdxReaderPtr pr) {
  if (!m_useWeakHashMap) m_pdxUnreadFields = pr->readUnreadFields();

  m_i1 = pr->readInt("i1");
  m_i2 = pr->readInt("i2");
  m_i3 = pr->readInt("i3");
  m_i4 = pr->readInt("i4");

  m_i5 = pr->readInt("i5");
  m_i6 = pr->readInt("i6");
}

CacheableStringPtr PdxTypesR1V2::toString() const {
  char idbuf[4096];
  sprintf(idbuf,
          "PdxTypesR1V2:[ m_i1=%d ] [ m_i2=%d ] [ m_i3=%d ] [ m_i4=%d ] [ "
          "m_i5=%d ] [ m_i6=%d ]",
          m_i1, m_i2, m_i3, m_i4, m_i5, m_i6);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxTypesR2V2
 * *********************************************************/

int PdxTypesR2V2::m_diffInSameFields = 0;
int PdxTypesR2V2::m_diffInExtraFields = 0;
bool PdxTypesR2V2::m_useWeakHashMap = false;

PdxTypesR2V2::PdxTypesR2V2() {
  m_i1 = 34324;
  m_i2 = 2144;
  m_i3 = 4645734;
  m_i4 = 73567;
  m_i5 = 798243;
  m_i6 = 9900;

  m_str1 = (char *)"0";
}

PdxTypesR2V2::~PdxTypesR2V2() {
  // TODO Auto-generated destructor stub
}

void PdxTypesR2V2::reset(bool useWeakHashMap) {
  PdxTypesR2V2::m_diffInSameFields = 0;
  PdxTypesR2V2::m_diffInExtraFields = 0;
  PdxTypesR2V2::m_useWeakHashMap = useWeakHashMap;
}

int PdxTypesR2V2::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxTypesR2V2::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxTypesR2V2Ptr pap = dynCast<PdxTypesR2V2Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4) {
    char tmp[20] = {0};
    sprintf(tmp, "%d", m_diffInExtraFields);
    if (strcmp(pap->m_str1, tmp) == 0) {
      if (m_i5 + m_diffInExtraFields == pap->m_i5 &&
          m_i6 + m_diffInExtraFields == pap->m_i6) {
        return true;
      }
    }
  }
  return false;
}

void PdxTypesR2V2::toData(PdxWriterPtr pw) {
  if (!m_useWeakHashMap) pw->writeUnreadFields(m_pdxunreadFields);

  pw->writeInt("i1", m_i1 + 1);
  pw->writeInt("i2", m_i2 + 1);
  pw->writeInt("i5", m_i5 + 1);
  pw->writeInt("i6", m_i6 + 1);
  pw->writeInt("i3", m_i3 + 1);
  pw->writeInt("i4", m_i4 + 1);

  m_diffInExtraFields++;
  m_diffInSameFields++;

  char tmpBuf[512] = {0};
  sprintf(tmpBuf, "%d", m_diffInExtraFields);
  pw->writeString("m_str1", tmpBuf);
}

void PdxTypesR2V2::fromData(PdxReaderPtr pr) {
  LOGDEBUG("PdxObject::fromData() start...");

  if (!m_useWeakHashMap) m_pdxunreadFields = pr->readUnreadFields();

  m_i1 = pr->readInt("i1");
  m_i2 = pr->readInt("i2");
  m_i5 = pr->readInt("i5");
  m_i6 = pr->readInt("i6");
  m_i3 = pr->readInt("i3");
  m_i4 = pr->readInt("i4");
  m_str1 = pr->readString("m_str1");
}

CacheableStringPtr PdxTypesR2V2::toString() const {
  char idbuf[4096];
  sprintf(idbuf,
          "PdxTypesR2V2:[ m_i1=%d ] [m_i2=%d] [ m_i3=%d ] [ m_i4=%d ] "
          "[m_i5=%d] [m_i6=%d] [m_str1=%s]",
          m_i1, m_i2, m_i3, m_i4, m_i5, m_i6, m_str1);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxTypesIgnoreUnreadFieldsV2
 * *********************************************************/

int PdxTypesIgnoreUnreadFieldsV2::m_diffInSameFields = 0;
int PdxTypesIgnoreUnreadFieldsV2::m_diffInExtraFields = 0;
bool PdxTypesIgnoreUnreadFieldsV2::m_useWeakHashMap = false;

PdxTypesIgnoreUnreadFieldsV2::PdxTypesIgnoreUnreadFieldsV2() {
  m_i1 = 34324;
  m_i2 = 2144;
  m_i3 = 4645734;
  m_i4 = 73567;
  m_i5 = 0;
  m_i6 = 0;
}

PdxTypesIgnoreUnreadFieldsV2::~PdxTypesIgnoreUnreadFieldsV2() {
  // TODO Auto-generated destructor stub
}

void PdxTypesIgnoreUnreadFieldsV2::reset(bool useWeakHashMap) {
  PdxTypesIgnoreUnreadFieldsV2::m_diffInSameFields = 0;
  PdxTypesIgnoreUnreadFieldsV2::m_diffInExtraFields = 0;
  PdxTypesIgnoreUnreadFieldsV2::m_useWeakHashMap = useWeakHashMap;
}

int PdxTypesIgnoreUnreadFieldsV2::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxTypesIgnoreUnreadFieldsV2::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxTypesIgnoreUnreadFieldsV2Ptr pap =
      dynCast<PdxTypesIgnoreUnreadFieldsV2Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  LOGINFO("PdxTypesIgnoreUnreadFieldsV2::equals");
  LOGINFO("m_i1 =%d m_diffInSameFields=%d pap->m_i1=%d", m_i1,
          m_diffInSameFields, pap->m_i1);
  LOGINFO("m_i2 =%d m_diffInSameFields=%d pap->m_i2=%d", m_i2,
          m_diffInSameFields, pap->m_i2);
  LOGINFO("m_i3 =%d m_diffInSameFields=%d pap->m_i3=%d", m_i3,
          m_diffInSameFields, pap->m_i3);
  LOGINFO("m_i4 =%d m_diffInSameFields=%d pap->m_i4=%d", m_i4,
          m_diffInSameFields, pap->m_i4);
  LOGINFO("m_i5 =%d  pap->m_i5=%d", m_i5, pap->m_i5);
  LOGINFO("m_i6 =%d  pap->m_i6=%d", m_i6, pap->m_i6);

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4 && m_i5 == pap->m_i5 &&
      m_i6 == pap->m_i6) {
    return true;
  }

  return false;
}

void PdxTypesIgnoreUnreadFieldsV2::updateMembers() {
  m_i5 = (int32_t)m_diffInExtraFields;
  m_i6 = (int32_t)m_diffInExtraFields;
}

void PdxTypesIgnoreUnreadFieldsV2::toData(PdxWriterPtr pw) {
  if (!m_useWeakHashMap) pw->writeUnreadFields(m_unreadFields);

  pw->writeInt("i1", m_i1 + 1);
  pw->markIdentityField("i1");
  pw->writeInt("i2", m_i2 + 1);
  pw->writeInt("i3", m_i3 + 1);
  pw->writeInt("i4", m_i4 + 1);
  pw->writeInt("i5", m_diffInExtraFields);
  pw->writeInt("i6", m_diffInExtraFields);

  m_i5 = (int32_t)m_diffInExtraFields;
  m_i6 = (int32_t)m_diffInExtraFields;

  m_diffInSameFields++;
  m_diffInExtraFields++;
}

void PdxTypesIgnoreUnreadFieldsV2::fromData(PdxReaderPtr pr) {
  m_i1 = pr->readInt("i1");
  bool isIdentity = pr->isIdentityField("i2");

  if (isIdentity) {
    throw Exception("i2 is not identity field");
  }

  if (!m_useWeakHashMap) m_unreadFields = pr->readUnreadFields();

  m_i2 = pr->readInt("i2");
  m_i3 = pr->readInt("i3");
  m_i4 = pr->readInt("i4");
  m_i5 = pr->readInt("i5");
  m_i6 = pr->readInt("i6");
}

CacheableStringPtr PdxTypesIgnoreUnreadFieldsV2::toString() const {
  char idbuf[4096];
  sprintf(idbuf,
          "PdxTypesV1R1:[m_i1=%d] [m_i2=%d] [m_i3=%d] [m_i4=%d] [m_i5=%d] "
          "[m_i6=%d]",
          m_i1, m_i2, m_i3, m_i4, m_i5, m_i6);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxVersionedV2
 * *********************************************************/
PdxVersionedV2::PdxVersionedV2() {}

void PdxVersionedV2::init(int32_t size) {
  m_char = 'C';
  m_bool = true;
  m_byte = 0x74;
  m_int16 = 0xab;
  m_int32 = 0x2345abdc;
  m_long = 324897980;
  m_float = 23324.324f;
  m_double = 3243298498;

  m_string = (char *)"gfestring";

  m_boolArray = new bool[3];
  m_boolArray[0] = true;
  m_boolArray[1] = false;
  m_boolArray[2] = true;

  m_charArray = new char[2];
  m_charArray[0] = 'c';
  m_charArray[1] = 'v';

  m_dateTime = NULLPTR;

  m_int16Array = new int16_t[size];
  m_int32Array = new int32_t[size];
  m_longArray = new int64_t[size];
  m_floatArray = new float[size];
  m_doubleArray = new double[size];

  for (int i = 0; i < size; i++) {
    m_int16Array[i] = 0x2332;
    m_int32Array[i] = 34343 + i;
    m_longArray[i] = 324324L + i;
    m_floatArray[i] = 232.565f + i;
    m_doubleArray[i] = 23423432 + i;
    // m_stringArray[i] = String.Format("one{0}", i);
  }
  boolArrayLen = 0;
  byteArrayLen = 0;
  shortArrayLen = 0;
  intArrayLen = 0;
  longArrayLen = 0;
  doubleArrayLen = 0;
  floatArrayLen = 0;
  strLenArray = 0;
}

PdxVersionedV2::PdxVersionedV2(int32_t size) {
  init(size);
  LOGDEBUG("PdxVersioned 1");
}

PdxVersionedV2::~PdxVersionedV2() {
  // TODO Auto-generated destructor stub
}

void PdxVersionedV2::toData(PdxWriterPtr pw) {
  // pw->writeChar("m_char", m_char);
  pw->writeBoolean("m_bool", m_bool);
  pw->writeByte("m_byte", m_byte);
  pw->writeShort("m_int16", m_int16);
  pw->writeInt("m_int32", m_int32);
  pw->writeLong("m_long", m_long);
  pw->writeFloat("m_float", m_float);
  pw->writeDouble("m_double", m_double);
  pw->writeString("m_string", m_string);
  pw->writeBooleanArray("m_boolArray", m_boolArray, 3);
  // pw->writeCharArray("m_charArray", m_charArray, 2);
  pw->writeDate("m_dateTime", m_dateTime);
  pw->writeShortArray("m_int16Array", m_int16Array, 2);
  pw->writeIntArray("m_int32Array", m_int32Array, 2);
  pw->writeLongArray("m_longArray", m_longArray, 2);
  pw->writeFloatArray("m_floatArray", m_floatArray, 2);
  pw->writeDoubleArray("m_doubleArray", m_doubleArray, 2);
}

void PdxVersionedV2::fromData(PdxReaderPtr pr) {
  // m_char = pr->readChar("m_char");
  m_bool = pr->readBoolean("m_bool");
  m_byte = pr->readByte("m_byte");
  m_int16 = pr->readShort("m_int16");
  m_int32 = pr->readInt("m_int32");
  m_long = pr->readLong("m_long");
  m_float = pr->readFloat("m_float");
  m_double = pr->readDouble("m_double");
  m_string = pr->readString("m_string");
  m_boolArray = pr->readBooleanArray("m_boolArray", boolArrayLen);
  // m_charArray = pr->readCharArray("m_charArray");
  m_dateTime = pr->readDate("m_dateTime");
  m_int16Array = pr->readShortArray("m_int16Array", shortArrayLen);
  m_int32Array = pr->readIntArray("m_int32Array", intArrayLen);
  m_longArray = pr->readLongArray("m_longArray", longArrayLen);
  m_floatArray = pr->readFloatArray("m_floatArray", floatArrayLen);
  m_doubleArray = pr->readDoubleArray("m_doubleArray", doubleArrayLen);
}

CacheableStringPtr PdxVersionedV2::toString() const {
  char idbuf[4096];
  // sprintf(idbuf,"PdxTypesV1R1:[ m_i1=%d ] [ m_i2=%d ] [ m_i3=%d ] [ m_i4=%d
  // ]", m_i1, m_i2, m_i3, m_i4 );
  return CacheableString::create(idbuf);
}

/************************************************************
 *  TestKeyV2
 * *********************************************************/

TestKeyV2::TestKeyV2() {}

TestKeyV2::TestKeyV2(char *id) { _id = id; }

/************************************************************
 *  TestDiffTypePdxSV2
 * *********************************************************/

TestDiffTypePdxSV2::TestDiffTypePdxSV2() {}

TestDiffTypePdxSV2::TestDiffTypePdxSV2(bool init) {
  if (init) {
    _id = (char *)"id:100";
    _name = (char *)"HK";
    _count = 100;
  }
}

bool TestDiffTypePdxSV2::equals(TestDiffTypePdxSV2 *obj) {
  if (obj == NULL) return false;

  TestDiffTypePdxSV2 *other = dynamic_cast<TestDiffTypePdxSV2 *>(obj);
  if (other == NULL) return false;

  LOGINFO("TestDiffTypePdxSV2 other->_coun = %d and _count = %d ",
          other->_count, _count);
  LOGINFO("TestDiffTypePdxSV2 other->_id = %s and _id = %s ", other->_id, _id);
  LOGINFO("TestDiffTypePdxSV2 other->_name = %s and _name = %s ", other->_name,
          _name);

  if (other->_count == _count && strcmp(other->_id, _id) == 0 &&
      strcmp(other->_name, _name) == 0) {
    return true;
  }

  return false;
}

/************************************************************
 *  TestEqualsV1
 * *********************************************************/
/****
 TestEqualsV1::TestEqualsV1(){
 i1 = 1;
 i2 = 0;
 s1 = "s1";
 //TODO: Uncomment it.
 //sArr = ;
 //intArr = ;
 }

 void TestEqualsV1::toData( PdxWriterPtr pw )  {
 pw->writeInt("i1", i1);
 pw->writeInt("i2", i2);
 pw->writeString("s1", s1);
 //pw->writeStringArray("sArr", sArr, 2);
 //pw->writeIntArray("intArr", intArr, 2);
 //pw->writeObject("intArrObject", intArr);
 //pw->writeObject("sArrObject", sArr);
 }

 void TestEqualsV1::fromData( PdxReaderPtr pr )
 {
 i1 = pr->readInt("i1");
 i2 = pr->readInt("i2");
 s1 = pr->readString("s1");
 //sArr = pr->readStringArray("sArr");
 //intArr = pr->readIntArray("intArr");
 //intArr = (int[]) reader.ReadObject("intArrObject");
 //sArr = (string[]) reader.ReadObject("sArrObject");
 }

 CacheableStringPtr TestEqualsV1::toString() const {
 char idbuf[1024];
 sprintf(idbuf,"TestEqualsV1:[i1=%d ] [i2=%d] ", i1, i2 );
 return CacheableString::create( idbuf );
 }
 ****/
