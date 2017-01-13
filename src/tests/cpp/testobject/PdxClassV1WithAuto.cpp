/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "PdxClassV1WithAuto.hpp"

using namespace gemfire;
using namespace PdxTestsAuto;

/************************************************************
 *  PdxType1V1
 * *********************************************************/
int32_t PdxType1V1::m_diffInSameFields = 0;
bool PdxType1V1::m_useWeakHashMap = false;

PdxType1V1::PdxType1V1() {
  m_i1 = 34324;
  m_i2 = 2144;
  m_i3 = 4645734;
  m_i4 = 73567;
}

PdxType1V1::~PdxType1V1() {
  // TODO Auto-generated destructor stub
}

void PdxType1V1::reset(bool useWeakHashMap) {
  m_diffInSameFields = 0;
  m_useWeakHashMap = useWeakHashMap;
}

int PdxType1V1::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxType1V1::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxType1V1Ptr pap = dynCast<PdxType1V1Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4) {
    return true;
  }

  return false;
}

// void PdxType1V1::toData( PdxWriterPtr pw )  {
//
//	if (!m_useWeakHashMap)
//    pw->writeUnreadFields(m_pdxUreadFields);
//
//	pw->writeInt("i1", m_i1 + 1);
//	pw->markIdentityField("i1");
//	pw->writeInt("i2", m_i2 + 1);
//	pw->writeInt("i3", m_i3 + 1);
//	pw->writeInt("i4", m_i4 + 1);
//
//	m_diffInSameFields++;
//
//	/*
//	pw->writeBoolean("m_bool", m_bool);
//	pw->writeByte("m_byte", m_byte);
//	pw->writeShort("m_int16", m_int16);
//	pw->writeInt("m_int32", m_int32);
//	pw->writeLong("m_long", m_long);
//	pw->writeFloat("m_float", m_float);
//	pw->writeDouble("m_double", m_double);
//	pw->writeString("m_string", m_string);
//	pw->writeBooleanArray("m_boolArray", m_boolArray, 3);
//	pw->writeByteArray("m_byteArray",m_byteArray, 2);
//	pw->writeShortArray("m_int16Array", m_int16Array, 2);
//	pw->writeIntArray("m_int32Array", m_int32Array, 4);
//	pw->writeLongArray("m_longArray", m_longArray, 2);
//	pw->writeFloatArray("m_floatArray", m_floatArray, 2);
//	pw->writeDoubleArray("m_doubleArray", m_doubleArray, 2);
//  */
//	//LOGDEBUG("PdxObject::toData() Done......");
//
//}
//
// void  PdxType1V1::fromData( PdxReaderPtr pr )
//{
//	//LOGDEBUG("PdxObject::fromData() start...");
//
//  if (!m_useWeakHashMap)
//    m_pdxUreadFields = pr->readUnreadFields();
//
//	m_i1 = pr->readInt("i1");
//	m_i2 = pr->readInt("i2");
//	m_i3 = pr->readInt("i3");
//	m_i4 = pr->readInt("i4");
//
//  bool hasField = pr->hasField("i7");
//
//  if (hasField)
//    throw Exception("i7 is not an valid field");
//
//  hasField = pr->hasField("i4");
//
//	if (!hasField)
//    throw Exception("i4 is an valid field");
//
//	/*
//	m_bool = pr->readBoolean("m_bool");
//	//LOGDEBUG("PdxObject::fromData() -1 m_bool = %d", m_bool);
//	m_byte = pr->readByte("m_byte");
//	//LOGDEBUG("PdxObject::fromData() -2 m_byte =%d", m_byte);
//	m_int16 = pr->readShort("m_int16");
//	//LOGDEBUG("PdxObject::fromData() -3 m_int16=%d", m_int16);
//	m_int32 = pr->readInt("m_int32");
//	//LOGDEBUG("PdxObject::fromData() -4 m_int32=%d", m_int32);
//	m_long = pr->readLong("m_long");
//	//LOGDEBUG("PdxObject::fromData() -5 m_long=%lld", m_long);
//	m_float = pr->readFloat("m_float");
//	//LOGDEBUG("PdxObject::fromData() -6 m_float = %f", m_float);
//	m_double = pr->readDouble("m_double");
//	//LOGDEBUG("PdxObject::fromData() -7  m_double=%llf", m_double);
//	m_string = pr->readString("m_string");
//	//LOGDEBUG("PdxObject::fromData() -8  m_string=%s", m_string);
//	m_boolArray = pr->readBooleanArray("m_boolArray");
//	m_byteArray = pr->readByteArray("m_byteArray");
//	m_int16Array = pr->readShortArray("m_int16Array");
//	m_int32Array = pr->readIntArray("m_int32Array");
//	m_longArray = pr->readLongArray("m_longArray");
//	m_floatArray = pr->readFloatArray("m_floatArray");
//	m_doubleArray = pr->readDoubleArray("m_doubleArray");
//	//LOGDEBUG("PdxObject::fromData() -8  m_boolArray[0]=%d",
// m_boolArray[0]);
//	//m_int32 = pr->readInt("m_int32");
//	*/
//	LOGDEBUG("PdxObject::fromData() End...");
//}

CacheableStringPtr PdxType1V1::toString() const {
  char idbuf[4096];
  // sprintf(idbuf,"PdxObject: [ m_bool=%d ] [m_byte=%d] [m_int16=%d]
  // [m_int32=%d] [m_long=%lld] [m_float=%f] [ m_string=%s ]",m_bool, m_byte,
  // m_int16, m_int32, m_long, m_float, m_string);
  // sprintf(idbuf,"PdxObject: [ m_bool=%d ] [m_byte=%d] [m_int16=%d]
  // [m_int32=%d] [m_long=%lld] [m_float=%f] [m_double=%Lf] [ m_string=%s
  // ]",m_bool, m_byte, m_int16, m_int32, m_long, m_float, m_double, m_string);
  // sprintf(idbuf,"PdxObject: [ m_bool=%d ] [m_byte=%d] [m_int16=%d]
  // [m_int32=%d] [m_float=%f] [m_double=%lf] [ m_string=%s ]",m_bool, m_byte,
  // m_int16, m_int32, m_float, m_double, m_string);
  sprintf(idbuf, "PdxType1V1:[ m_i1=%d ] [ m_i2=%d ] [ m_i3=%d ] [ m_i4=%d ]",
          m_i1, m_i2, m_i3, m_i4);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxType2V1
 * *********************************************************/

int PdxType2V1::m_diffInSameFields = 0;
bool PdxType2V1::m_useWeakHashMap = false;

PdxType2V1::PdxType2V1() {
  m_i1 = 34324;
  m_i2 = 2144;
  m_i3 = 4645734;
  m_i4 = 73567;
}

PdxType2V1::~PdxType2V1() {
  // TODO Auto-generated destructor stub
}

void PdxType2V1::reset(bool useWeakHashMap) {
  PdxType2V1::m_diffInSameFields = 0;
  PdxType2V1::m_useWeakHashMap = useWeakHashMap;
}

int PdxType2V1::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxType2V1::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxType2V1Ptr pap = dynCast<PdxType2V1Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4) {
    return true;
  }

  return false;
}

// void PdxType2V1::toData( PdxWriterPtr pw )  {
//
//	if (!m_useWeakHashMap)
//    pw->writeUnreadFields(m_unreadFields);
//
//	pw->writeInt("i1", m_i1 + 1);
//	pw->writeInt("i2", m_i2 + 1);
//	pw->writeInt("i3", m_i3 + 1);
//	pw->writeInt("i4", m_i4 + 1);
//}
//
// void  PdxType2V1::fromData( PdxReaderPtr pr )
//{
//	//LOGDEBUG("PdxObject::fromData() start...");
//
//	if (!m_useWeakHashMap)
//    m_unreadFields = pr->readUnreadFields();
//
//	m_i1 = pr->readInt("i1");
//	m_i2 = pr->readInt("i2");
//	m_i3 = pr->readInt("i3");
//	m_i4 = pr->readInt("i4");
//}

CacheableStringPtr PdxType2V1::toString() const {
  char idbuf[4096];
  sprintf(idbuf, "PdxType2V1:[ m_i1=%d ] [ m_i2=%d ] [ m_i3=%d ] [ m_i4=%d ]",
          m_i1, m_i2, m_i3, m_i4);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxType3V1
 * *********************************************************/

int PdxType3V1::m_diffInSameFields = 0;
int PdxType3V1::m_diffInExtraFields = 0;
bool PdxType3V1::m_useWeakHashMap = false;

PdxType3V1::PdxType3V1() {
  m_i1 = 1;
  m_i2 = 21;
  m_str1 = (char *)"common";
  m_i3 = 31;
  m_i4 = 41;
  m_i5 = 0;
  m_str2 = (char *)"0";
}

PdxType3V1::~PdxType3V1() {
  // TODO Auto-generated destructor stub
}

void PdxType3V1::reset(bool useWeakHashMap) {
  PdxType3V1::m_diffInSameFields = 0;
  PdxType3V1::m_diffInExtraFields = 0;
  PdxType3V1::m_useWeakHashMap = useWeakHashMap;
}

int PdxType3V1::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxType3V1::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxType3V1Ptr pap = dynCast<PdxType3V1Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4 &&
      m_i5 + m_diffInExtraFields == pap->m_i5) {
    if (strcmp(m_str1, pap->m_str1) == 0 && strcmp(m_str2, pap->m_str2) <= 0) {
      return true;
    }
  }
  return false;
}

// void PdxType3V1::toData( PdxWriterPtr pw ) {
//
//  if (!m_useWeakHashMap)
//    pw->writeUnreadFields(m_unreadFields);
//
//  pw->writeInt("i1", m_i1 + 1);
//  pw->writeInt("i2", m_i2 + 1);
//  pw->writeString("m_str1", m_str1);
//  pw->writeInt("i3", m_i3 + 1);
//  pw->writeInt("i4", m_i4 + 1);
//  pw->writeInt("i5", m_i5 + 1);
//  pw->writeString("m_str2", m_str2);
//
//  m_diffInSameFields++;
//  m_diffInExtraFields++;
//}
//
// void PdxType3V1::fromData( PdxReaderPtr pr )
//{
//	//LOGDEBUG("PdxObject::fromData() start...");
//
//    if (!m_useWeakHashMap)
//    	m_unreadFields = pr->readUnreadFields();
//
//	m_i1 = pr->readInt("i1");
//	m_i2 = pr->readInt("i2");
//	m_str1 = pr->readString("m_str1");
//	m_i3 = pr->readInt("i3");
//	m_i4 = pr->readInt("i4");
//	m_i5 = pr->readInt("i5");
//	char* tmp = pr->readString("m_str2");
//
//  char extraFieldsStr[20];
//  if (tmp == NULL) {
//	sprintf(extraFieldsStr, "%d", m_diffInExtraFields);
//	size_t strSize = strlen(extraFieldsStr) + 1;
//	m_str2 = new char[strSize];
//	memcpy(m_str2, extraFieldsStr, strSize);
//  } else {
//	 sprintf(extraFieldsStr, "%d" "%s", m_diffInExtraFields, m_str2);
//	 size_t strSize = strlen(extraFieldsStr) + 1;
//	 m_str2 = new char[strSize];
//	 memcpy(m_str2, extraFieldsStr, strSize);
//  }
//}

CacheableStringPtr PdxType3V1::toString() const {
  char idbuf[4096];
  sprintf(idbuf,
          "PdxType2V1:[ m_i1=%d ] [ m_i2=%d ] [m_str1=%s] [ m_i3=%d ] [ "
          "m_i4=%d ] [ m_i5=%d ] [m_str2=%s]",
          m_i1, m_i2, m_str1, m_i3, m_i4, m_i5, m_str2);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxTypesV1R1
 * *********************************************************/
int PdxTypesV1R1::m_diffInSameFields = 0;
bool PdxTypesV1R1::m_useWeakHashMap = false;

PdxTypesV1R1::PdxTypesV1R1() {
  m_i1 = 34324;
  m_i2 = 2144;
  m_i3 = 4645734;
  m_i4 = 73567;
}

PdxTypesV1R1::~PdxTypesV1R1() {
  // TODO Auto-generated destructor stub
}

void PdxTypesV1R1::reset(bool useWeakHashMap) {
  PdxTypesV1R1::m_diffInSameFields = 0;
  PdxTypesV1R1::m_useWeakHashMap = useWeakHashMap;
}

int PdxTypesV1R1::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxTypesV1R1::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxTypesV1R1Ptr pap = dynCast<PdxTypesV1R1Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4) {
    return true;
  }
  return false;
}

// void PdxTypesV1R1::toData( PdxWriterPtr pw )  {
//
//	if (!m_useWeakHashMap)
//    pw->writeUnreadFields(m_pdxUreadFields);
//
//	pw->writeInt("i1", m_i1 + 1);
//	pw->writeInt("i2", m_i2 + 1);
//	pw->writeInt("i3", m_i3 + 1);
//	pw->writeInt("i4", m_i4 + 1);
//
//  m_diffInSameFields++;
//}
//
// void PdxTypesV1R1::fromData( PdxReaderPtr pr )
//{
//
//	if (!m_useWeakHashMap)
//    m_pdxUreadFields = pr->readUnreadFields();
//
//	m_i1 = pr->readInt("i1");
//	m_i2 = pr->readInt("i2");
//	m_i3 = pr->readInt("i3");
//	m_i4 = pr->readInt("i4");
//}

CacheableStringPtr PdxTypesV1R1::toString() const {
  char idbuf[4096];
  sprintf(idbuf, "PdxTypesV1R1:[ m_i1=%d ] [ m_i2=%d ] [ m_i3=%d ] [ m_i4=%d ]",
          m_i1, m_i2, m_i3, m_i4);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxTypesV1R2
 * *********************************************************/
int PdxTypesV1R2::m_diffInSameFields = 0;
bool PdxTypesV1R2::m_useWeakHashMap = false;

PdxTypesV1R2::PdxTypesV1R2() {
  m_i1 = 34324;
  m_i2 = 2144;
  m_i3 = 4645734;
  m_i4 = 73567;
}

PdxTypesV1R2::~PdxTypesV1R2() {
  // TODO Auto-generated destructor stub
}

void PdxTypesV1R2::reset(bool useWeakHashMap) {
  PdxTypesV1R2::m_diffInSameFields = 0;
  PdxTypesV1R2::m_useWeakHashMap = useWeakHashMap;
}

int PdxTypesV1R2::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxTypesV1R2::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxTypesV1R2Ptr pap = dynCast<PdxTypesV1R2Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4) {
    return true;
  }
  return false;
}

// void PdxTypesV1R2::toData( PdxWriterPtr pw )  {
//
//
//	if (!m_useWeakHashMap)
//    pw->writeUnreadFields(m_pdxUreadFields);
//
//	pw->writeInt("i1", m_i1 + 1);
//	pw->writeInt("i2", m_i2 + 1);
//	pw->writeInt("i3", m_i3 + 1);
//	pw->writeInt("i4", m_i4 + 1);
//}
//
// void PdxTypesV1R2::fromData( PdxReaderPtr pr )
//{
//	//LOGDEBUG("PdxObject::fromData() start...");
//	if (!m_useWeakHashMap)
//		m_pdxUreadFields = pr->readUnreadFields();
//
//	m_i1 = pr->readInt("i1");
//	m_i2 = pr->readInt("i2");
//	m_i3 = pr->readInt("i3");
//	m_i4 = pr->readInt("i4");
//}

CacheableStringPtr PdxTypesV1R2::toString() const {
  char idbuf[4096];
  sprintf(idbuf, "PdxTypesV1R1:[ m_i1=%d ] [ m_i2=%d ] [ m_i3=%d ] [ m_i4=%d ]",
          m_i1, m_i2, m_i3, m_i4);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxTypesIgnoreUnreadFieldsV1
 * *********************************************************/
int PdxTypesIgnoreUnreadFieldsV1::m_diffInSameFields = 0;
bool PdxTypesIgnoreUnreadFieldsV1::m_useWeakHashMap = false;

PdxTypesIgnoreUnreadFieldsV1::PdxTypesIgnoreUnreadFieldsV1() {
  m_i1 = 34324;
  m_i2 = 2144;
  m_i3 = 4645734;
  m_i4 = 73567;
}

PdxTypesIgnoreUnreadFieldsV1::~PdxTypesIgnoreUnreadFieldsV1() {
  // TODO Auto-generated destructor stub
}

void PdxTypesIgnoreUnreadFieldsV1::reset(bool useWeakHashMap) {
  PdxTypesIgnoreUnreadFieldsV1::m_diffInSameFields = 0;
  PdxTypesIgnoreUnreadFieldsV1::m_useWeakHashMap = useWeakHashMap;
}

int PdxTypesIgnoreUnreadFieldsV1::getHashCode() {
  // TODO:Implement It.
  return 1;
}

bool PdxTypesIgnoreUnreadFieldsV1::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  PdxTypesIgnoreUnreadFieldsV1Ptr pap =
      dynCast<PdxTypesIgnoreUnreadFieldsV1Ptr>(obj);
  if (pap == NULLPTR) return false;

  if (pap == this) return true;

  if (m_i1 + m_diffInSameFields <= pap->m_i1 &&
      m_i2 + m_diffInSameFields <= pap->m_i2 &&
      m_i3 + m_diffInSameFields <= pap->m_i3 &&
      m_i4 + m_diffInSameFields <= pap->m_i4) {
    return true;
  }
  return false;
}

// void PdxTypesIgnoreUnreadFieldsV1::toData( PdxWriterPtr pw )  {
//
//  if (!m_useWeakHashMap)
//    pw->writeUnreadFields(m_unreadFields);
//
//	pw->writeInt("i1", m_i1 + 1);
//	pw->markIdentityField("i1");
//	pw->writeInt("i2", m_i2 + 1);
//	pw->writeInt("i3", m_i3 + 1);
//	pw->writeInt("i4", m_i4 + 1);
//
//  m_diffInSameFields++;
//}
//
// void PdxTypesIgnoreUnreadFieldsV1::fromData( PdxReaderPtr pr )
//{
//	//LOGDEBUG("PdxObject::fromData() start...");
//
//	if (!m_useWeakHashMap)
//    m_unreadFields = pr->readUnreadFields();
//
//	m_i1 = pr->readInt("i1");
//	m_i2 = pr->readInt("i2");
//	m_i3 = pr->readInt("i3");
//	m_i4 = pr->readInt("i4");
//
//	bool hasField = pr->hasField("i7");
//	if(hasField)
//		throw Exception("i7 is not an valid field");
//
//	hasField = pr->hasField("i4");
//  if(!hasField)
//    throw Exception("i4 is an valid field");
//
//}

CacheableStringPtr PdxTypesIgnoreUnreadFieldsV1::toString() const {
  char idbuf[4096];
  sprintf(idbuf, "PdxTypesV1R1:[ m_i1=%d ] [ m_i2=%d ] [ m_i3=%d ] [ m_i4=%d ]",
          m_i1, m_i2, m_i3, m_i4);
  return CacheableString::create(idbuf);
}

/************************************************************
 *  PdxVersionedV1
 * *********************************************************/

PdxVersionedV1::PdxVersionedV1() {}

void PdxVersionedV1::init(int32_t size) {
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

  boolArrayLen = 3;
  byteArrayLen = 0;
  shortArrayLen = size;
  intArrayLen = size;
  longArrayLen = size;
  doubleArrayLen = size;
  floatArrayLen = size;
  strLenArray = 0;
}

PdxVersionedV1::PdxVersionedV1(int32_t size) {
  init(size);
  LOGDEBUG("PdxVersioned 1");
}

PdxVersionedV1::~PdxVersionedV1() {
  // TODO Auto-generated destructor stub
}

// void PdxVersionedV1::toData( PdxWriterPtr pw )  {
//  //pw->writeChar("m_char", m_char);
//  pw->writeBoolean("m_bool", m_bool);
//  pw->writeByte("m_byte", m_byte);
//  pw->writeShort("m_int16", m_int16);
//  pw->writeInt("m_int32", m_int32);
//  pw->writeLong("m_long", m_long);
//  pw->writeFloat("m_float", m_float);
//  pw->writeDouble("m_double", m_double);
//  pw->writeString("m_string", m_string);
//  pw->writeBooleanArray("m_boolArray", m_boolArray, 3);
//  //pw->writeCharArray("m_charArray", m_charArray, 2);
//  pw->writeDate("m_dateTime", m_dateTime);
//  pw->writeShortArray("m_int16Array", m_int16Array, 2);
//  pw->writeIntArray("m_int32Array", m_int32Array, 2);
//  pw->writeLongArray("m_longArray", m_longArray, 2);
//  pw->writeFloatArray("m_floatArray", m_floatArray, 2);
//  pw->writeDoubleArray("m_doubleArray", m_doubleArray, 2);
//}
//
// void PdxVersionedV1::fromData( PdxReaderPtr pr )
//{
//	//m_char = pr->readChar("m_char");
//	m_bool = pr->readBoolean("m_bool");
//  m_byte = pr->readByte("m_byte");
//  m_int16 = pr->readShort("m_int16");
//  m_int32 = pr->readInt("m_int32");
//  m_long = pr->readLong("m_long");
//  m_float = pr->readFloat("m_float");
//  m_double = pr->readDouble("m_double");
//  m_string = pr->readString("m_string");
//  m_boolArray = pr->readBooleanArray("m_boolArray", boolArrayLen);
//  //m_charArray = pr->readCharArray("m_charArray");
//  m_dateTime = pr->readDate("m_dateTime");
//  m_int16Array = pr->readShortArray("m_int16Array", shortArrayLen);
//  m_int32Array = pr->readIntArray("m_int32Array", intArrayLen);
//  m_longArray = pr->readLongArray("m_longArray", longArrayLen);
//  m_floatArray = pr->readFloatArray("m_floatArray", floatArrayLen);
//  m_doubleArray = pr->readDoubleArray("m_doubleArray", doubleArrayLen);
//}

CacheableStringPtr PdxVersionedV1::toString() const {
  char idbuf[4096];
  // sprintf(idbuf,"PdxTypesV1R1:[ m_i1=%d ] [ m_i2=%d ] [ m_i3=%d ] [ m_i4=%d
  // ]", m_i1, m_i2, m_i3, m_i4 );
  return CacheableString::create(idbuf);
}

/************************************************************
 *  TestKey
 * *********************************************************/

TestKeyV1::TestKeyV1() {}

TestKeyV1::TestKeyV1(char *id) {
  size_t strSize = strlen(id) + 1;
  _id = new char[strSize];
  memcpy(_id, id, strSize);
}

/************************************************************
 *  TestDiffTypePdxSV1
 * *********************************************************/

TestDiffTypePdxSV1::TestDiffTypePdxSV1() {}

TestDiffTypePdxSV1::TestDiffTypePdxSV1(bool init) {
  if (init) {
    _id = (char *)"id:100";
    _name = (char *)"HK";
  }
}

bool TestDiffTypePdxSV1::equals(TestDiffTypePdxSV1 *obj) {
  if (obj == NULL) return false;

  TestDiffTypePdxSV1 *other = dynamic_cast<TestDiffTypePdxSV1 *>(obj);
  if (other == NULL) return false;

  if (strcmp(other->_id, _id) == 0 && strcmp(other->_name, _name) == 0) {
    return true;
  }

  return false;
}

/************************************************************
 *  TestEqualsV1
 * *********************************************************/

TestEqualsV1::TestEqualsV1() {
  i1 = 1;
  i2 = 0;
  s1 = (char *)"s1";
  // TODO: Uncomment it.
  // sArr = ;
  // intArr = ;
}

// void TestEqualsV1::toData( PdxWriterPtr pw )  {
//	pw->writeInt("i1", i1);
//	pw->writeInt("i2", i2);
//	pw->writeString("s1", s1);
//	//pw->writeStringArray("sArr", sArr, 2);
//	//pw->writeIntArray("intArr", intArr, 2);
//	//pw->writeObject("intArrObject", intArr);
//	//pw->writeObject("sArrObject", sArr);
//}
//
// void TestEqualsV1::fromData( PdxReaderPtr pr )
//{
//  i1 = pr->readInt("i1");
//  i2 = pr->readInt("i2");
//  s1 = pr->readString("s1");
//  //sArr = pr->readStringArray("sArr");
//  //intArr = pr->readIntArray("intArr");
//  //intArr = (int[]) reader.ReadObject("intArrObject");
//  //sArr = (string[]) reader.ReadObject("sArrObject");
//}

CacheableStringPtr TestEqualsV1::toString() const {
  char idbuf[1024];
  sprintf(idbuf, "TestEqualsV1:[i1=%d ] [i2=%d] ", i1, i2);
  return CacheableString::create(idbuf);
}
