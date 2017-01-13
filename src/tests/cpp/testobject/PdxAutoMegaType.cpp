/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "PdxAutoMegaType.hpp"

using namespace gemfire;
using namespace PdxAutoTests;

// CTOR
PdxAutoMegaType::PdxAutoMegaType() {}

// DTOR
PdxAutoMegaType::~PdxAutoMegaType() {}

void PdxAutoMegaType::populatePrimitives() {
  pdxType_Char = 'a';
  pdxType_Wchar = L'a';
  pdxType_Boolean = true;
  pdxType_Byte = 0x74;
  pdxType_Short = 0xab;
  pdxType_Int = 32;
  pdxType_Long = 324897980;
  pdxType_Float = 23324.324f;
  pdxType_Double = 3243298498.00;

  struct timeval now;
  now.tv_sec = 1310447869;
  now.tv_usec = 154000;
  pdxType_Date = CacheableDate::create(now);

  pdxType_String = (char*)"gfestring";
  pdxType_WideString = (wchar_t*)L"gfestring";
}

bool PdxAutoMegaType::verifyPrimitives(PdxSerializablePtr obj) {
  PdxAutoMegaTypePtr other = dynCast<PdxAutoMegaTypePtr>(obj);
  if (other == NULLPTR) return false;
  LOGINFO("PdxAutoMegaType::equals::[char]");
  if (this->pdxType_Char != other->pdxType_Char) {
    LOGINFO("PdxAutoMegaType::equals::[char] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[wchar]");
  if (this->pdxType_Wchar != other->pdxType_Wchar) {
    LOGINFO("PdxAutoMegaType::equals::[wchar] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[bool]");
  if (this->pdxType_Boolean != other->pdxType_Boolean) {
    LOGINFO("PdxAutoMegaType::equals::[bool] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[byte]");
  if (this->pdxType_Byte != other->pdxType_Byte) {
    LOGINFO("PdxAutoMegaType::equals::[byte] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[int]");
  if (this->pdxType_Int != other->pdxType_Int) {
    LOGINFO("PdxAutoMegaType::equals::[int] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[long]");
  if (this->pdxType_Long != other->pdxType_Long) {
    LOGINFO("PdxAutoMegaType::equals::[long] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[short]");
  if (this->pdxType_Short != other->pdxType_Short) {
    LOGINFO("PdxAutoMegaType::equals::[short] type values mismatch");
    return false;
  }
  LOGINFO("PdxAutoMegaType::equals::[float]");
  if (this->pdxType_Float != other->pdxType_Float) {
    LOGINFO("PdxAutoMegaType::equals::[float] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[double]");
  if (this->pdxType_Double != other->pdxType_Double) {
    LOGINFO("PdxAutoMegaType::equals::[double] type values mismatch");
    return false;
  }
  LOGINFO("PdxAutoMegaType::equals::[CacheableDate]");
  if (!(*(this->pdxType_Date.ptr()) == *(other->pdxType_Date.ptr()))) {
    LOGINFO("PdxAutoMegaType::equals::[CacheableDate] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[string]");
  if (strcmp(this->pdxType_String, other->pdxType_String) != 0) {
    LOGINFO("PdxAutoMegaType::equals::[string] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[wide string]");
  if (wcscmp(this->pdxType_WideString, other->pdxType_WideString) != 0) {
    LOGINFO("PdxAutoMegaType::equals::[wide string] type values mismatch");
    return false;
  }
  return true;
}

void PdxAutoMegaType::populateArrays() {
  pdxType_BoolArray = new bool[3];
  pdxType_BoolArray[0] = true;
  pdxType_BoolArray[1] = false;
  pdxType_BoolArray[2] = true;
  pdxType_BoolArray_Size = 3;

  pdxType_WideCharArray = new wchar_t[2];
  pdxType_WideCharArray[0] = L'c';
  pdxType_WideCharArray[1] = L'v';
  pdxType_WideCharArray_Size = 2;

  pdxType_CharArray = new char*[2];
  const char* str1 = "one";
  const char* str2 = "two";

  int size = static_cast<int>(strlen(str1));
  for (int i = 0; i < 2; i++) {
    pdxType_CharArray[i] = new char[size];
  }
  pdxType_CharArray[0] = const_cast<char*>(str1);
  pdxType_CharArray[1] = const_cast<char*>(str2);
  pdxType_CharArray_Size = 2;

  pdxType_ByteArray = new int8_t[2];
  pdxType_ByteArray[0] = 0x34;
  pdxType_ByteArray[1] = 0x64;
  pdxType_ByteArray_Size = 2;

  pdxType_ShortArray = new int16_t[2];
  pdxType_ShortArray[0] = 0x2332;
  pdxType_ShortArray[1] = 0x4545;
  pdxType_ShortArray_Size = 2;

  pdxType_Int32Array = new int32_t[4];
  pdxType_Int32Array[0] = 23;
  pdxType_Int32Array[1] = 676868;
  pdxType_Int32Array[2] = 34343;
  pdxType_Int32Array[3] = 2323;
  pdxType_Int32Array_Size = 4;

  pdxType_LongArray = new int64_t[2];
  pdxType_LongArray[0] = 324324L;
  pdxType_LongArray[1] = 23434545L;
  pdxType_LongArray_Size = 2;

  pdxType_FloatArray = new float[2];
  pdxType_FloatArray[0] = 232.565f;
  pdxType_FloatArray[1] = 2343254.67f;
  pdxType_FloatArray_Size = 2;

  pdxType_DoubleArray = new double[2];
  pdxType_DoubleArray[0] = 23423432;
  pdxType_DoubleArray[1] = 4324235435.00;
  pdxType_DoubleArray_Size = 2;

  pdxType_WideStringArray = new wchar_t*[2];
  const wchar_t* str11 = L"one";
  const wchar_t* str21 = L"two";

  size_t sizeWide = wcslen(str11);
  for (int i = 0; i < 2; i++) {
    pdxType_WideStringArray[i] = new wchar_t[sizeWide];
  }
  pdxType_WideStringArray[0] = const_cast<wchar_t*>(str11);
  pdxType_WideStringArray[1] = const_cast<wchar_t*>(str21);
  pdxType_WideStringArray_Size = 2;
}

bool PdxAutoMegaType::verifyArrays(PdxSerializablePtr obj) {
  PdxAutoMegaTypePtr other = dynCast<PdxAutoMegaTypePtr>(obj);
  if (other == NULLPTR) return false;
  LOGINFO("PdxAutoMegaType::equals::[Boolean Array]");
  if (other->pdxType_BoolArray[0] != true) {
    LOGINFO("PdxAutoMegaType::equals::[Boolean Array] type values mismatch");
    return false;
  }
  if (other->pdxType_BoolArray[1] != false) {
    LOGINFO("PdxAutoMegaType::equals::[Boolean Array] type values mismatch");
    return false;
  }
  if (other->pdxType_BoolArray[2] != true) {
    LOGINFO("PdxAutoMegaType::equals::[Boolean Array] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[WideChar Array]");
  if (other->pdxType_WideCharArray[0] != L'c') {
    LOGINFO("PdxAutoMegaType::equals::[WideChar Array] type values mismatch");
    return false;
  }
  if (other->pdxType_WideCharArray[1] != L'v') {
    LOGINFO("PdxAutoMegaType::equals::[WideChar Array] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[Char Array]");
  if (strcmp(this->pdxType_CharArray[0], "one") != 0) {
    LOGINFO("PdxAutoMegaType::equals::[Char Array] type values mismatch");
    return false;
  }
  if (strcmp(this->pdxType_CharArray[1], "two") != 0) {
    LOGINFO("PdxAutoMegaType::equals::[Char Array] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[Byte Array]");
  if (other->pdxType_ByteArray[0] != 0x34) {
    LOGINFO("PdxAutoMegaType::equals::[Byte Array] type values mismatch");
    return false;
  }
  if (other->pdxType_ByteArray[1] != 0x64) {
    LOGINFO("PdxAutoMegaType::equals::[Byte Array] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[Short Array]");
  if (other->pdxType_ShortArray[0] != 0x2332) {
    LOGINFO("PdxAutoMegaType::equals::[Short Array] type values mismatch");
    return false;
  }
  if (other->pdxType_ShortArray[1] != 0x4545) {
    LOGINFO("PdxAutoMegaType::equals::[Short Array] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[Int Array]");
  if (other->pdxType_Int32Array[0] != 23) {
    LOGINFO("PdxAutoMegaType::equals::[Int Array][0] type values mismatch");
    return false;
  }
  if (other->pdxType_Int32Array[1] != 676868) {
    LOGINFO("PdxAutoMegaType::equals::[Int Array][1] type values mismatch");
    return false;
  }
  if (other->pdxType_Int32Array[2] != 34343) {
    LOGINFO("PdxAutoMegaType::equals::[Int Array][2] type values mismatch");
    return false;
  }
  if (other->pdxType_Int32Array[3] != 2323) {
    LOGINFO("PdxAutoMegaType::equals::[Int Array][3] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[Long Array]");
  if (other->pdxType_LongArray[0] != 324324L) {
    LOGINFO("PdxAutoMegaType::equals::[Long Array][0] type values mismatch");
    return false;
  }
  if (other->pdxType_LongArray[1] != 23434545L) {
    LOGINFO("PdxAutoMegaType::equals::[Long Array][1] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[Float Array]");
  if (other->pdxType_FloatArray[0] != 232.565f) {
    LOGINFO("PdxAutoMegaType::equals::[Float Array][0] type values mismatch");
    return false;
  }
  if (other->pdxType_FloatArray[1] != 2343254.67f) {
    LOGINFO("PdxAutoMegaType::equals::[Float Array][1] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[Double Array]");
  if (other->pdxType_DoubleArray[0] != 23423432) {
    LOGINFO("PdxAutoMegaType::equals::[Double Array][0] type values mismatch");
    return false;
  }
  if (other->pdxType_DoubleArray[1] != 4324235435.00) {
    LOGINFO("PdxAutoMegaType::equals::[Double Array][1] type values mismatch");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[Wide Character Array]");
  if (wcscmp(other->pdxType_WideStringArray[0], L"one") != 0) {
    LOGINFO(
        "PdxAutoMegaType::equals::[Wide Character Array][0] type values "
        "mismatch");
    return false;
  }
  if (wcscmp(other->pdxType_WideStringArray[1], L"two") != 0) {
    LOGINFO(
        "PdxAutoMegaType::equals::[Wide Character Array][1] type values "
        "mismatch");
    return false;
  }
  return true;
}

void PdxAutoMegaType::populateCacheableContainers() {
  pdxType_CacheableArrayListPtr = CacheableArrayList::create();
  pdxType_CacheableArrayListPtr->push_back(CacheableInt32::create(1));
  pdxType_CacheableArrayListPtr->push_back(CacheableInt32::create(2));

  pdxType_CacheableHashMapPtr = CacheableHashMap::create();
  pdxType_CacheableHashMapPtr->insert(CacheableInt32::create(1),
                                      CacheableInt32::create(1));
  pdxType_CacheableHashMapPtr->insert(CacheableInt32::create(2),
                                      CacheableInt32::create(2));

  PdxType_CacheableHashTablePtr = CacheableHashTable::create();
  PdxType_CacheableHashTablePtr->insert(
      CacheableInt32::create(1), CacheableString::create("1111111111111111"));
  PdxType_CacheableHashTablePtr->insert(
      CacheableInt32::create(2),
      CacheableString::create("2222222222221111111111111111"));

  pdxType_CacheableVectorPtr = CacheableVector::create();
  pdxType_CacheableVectorPtr->push_back(CacheableInt32::create(1));
  pdxType_CacheableVectorPtr->push_back(CacheableInt32::create(2));
  pdxType_CacheableVectorPtr->push_back(CacheableInt32::create(3));

  pdxType_CacheableHashSetPtr = CacheableHashSet::create();
  pdxType_CacheableHashSetPtr->insert(CacheableInt32::create(1));

  pdxType_CacheableLinkedHashSetPtr = CacheableLinkedHashSet::create();
  pdxType_CacheableLinkedHashSetPtr->insert(CacheableInt32::create(1));
  pdxType_CacheableLinkedHashSetPtr->insert(CacheableInt32::create(2));
}
bool PdxAutoMegaType::verifyCacheableContainers(PdxSerializablePtr obj) {
  PdxAutoMegaTypePtr other = dynCast<PdxAutoMegaTypePtr>(obj);
  if (other == NULLPTR) return false;
  LOGINFO("PdxAutoMegaType::equals::[CacheableArrayList]");
  if (other->pdxType_CacheableArrayListPtr == NULLPTR) {
    LOGINFO("CacheableArrayListPtr should not be NULL");
    return false;
  }
  // CacheableArrayList
  LOGINFO("PdxAutoMegaType::equals::[CacheableArrayList]  Size Check");
  if (this->pdxType_CacheableArrayListPtr->size() !=
      other->pdxType_CacheableArrayListPtr->size()) {
    LOGINFO("CacheableArrayListPtr size should be equal");
    return false;
  }

  LOGINFO(
      "PdxAutoMegaType::equals::[CacheableArrayList][0] element Value Check");
  int32_t valLeft = CacheableInt32::create(1)->value();
  int32_t valRight =
      dynCast<CacheableInt32Ptr>(other->pdxType_CacheableArrayListPtr->at(0))
          ->value();

  if (valLeft != valRight) {
    LOGINFO(
        "PdxAutoMegaType::equals::[CacheableArrayList][0] type values "
        "mismatch");
    return false;
  }

  valLeft = CacheableInt32::create(2)->value();
  valRight =
      dynCast<CacheableInt32Ptr>(other->pdxType_CacheableArrayListPtr->at(1))
          ->value();

  LOGINFO(
      "PdxAutoMegaType::equals::[CacheableArrayList][1] element Value Check");
  if (valLeft != valRight) {
    LOGINFO(
        "PdxAutoMegaType::equals::[CacheableArrayList][1] type values "
        "mismatch");
    return false;
  }

  // CacheableHashMap
  LOGINFO("PdxAutoMegaType::equals::[CacheableHashMap]");
  if (other->pdxType_CacheableHashMapPtr == NULLPTR) {
    LOGINFO("CacheableHashMap should not be NULL");
    return false;
  }
  LOGINFO("PdxAutoMegaType::equals::[CacheableHashMap]  Size Check");
  if (this->pdxType_CacheableHashMapPtr->size() !=
      other->pdxType_CacheableHashMapPtr->size()) {
    LOGINFO("CacheableHashMap size should be equal");
    return false;
  }

  valLeft = CacheableInt32::create(1)->value();
  CacheableInt32Ptr mapValRight =
      pdxType_CacheableHashMapPtr->operator[](CacheableInt32::create(1));
  valRight = mapValRight->value();

  LOGINFO("PdxAutoMegaType::equals::[CacheableHashMap][0] element Value Check");
  if (valLeft != valRight) {
    LOGINFO(
        "PdxAutoMegaType::equals::[CacheableHashMap][0] type values mismatch");
    return false;
  }

  valLeft = CacheableInt32::create(2)->value();
  mapValRight =
      pdxType_CacheableHashMapPtr->operator[](CacheableInt32::create(2));
  valRight = mapValRight->value();

  LOGINFO("PdxAutoMegaType::equals::[CacheableHashMap][1] element Value Check");
  if (valLeft != valRight) {
    LOGINFO(
        "PdxAutoMegaType::equals::[CacheableHashMap][1] type values mismatch");
    return false;
  }

  // CacheableHashTable
  LOGINFO("PdxAutoMegaType::equals::[CacheableHashTable]");
  if (other->PdxType_CacheableHashTablePtr == NULLPTR) {
    LOGINFO("CacheableHashTable should not be NULL");
    return false;
  }
  LOGINFO("PdxAutoMegaType::equals::[CacheableHashTable] Size Check");
  if (this->PdxType_CacheableHashTablePtr->size() !=
      other->PdxType_CacheableHashTablePtr->size()) {
    LOGINFO("CacheableHashTable size should be equal");
    return false;
  }

  for (CacheableHashTable::Iterator iter =
           this->PdxType_CacheableHashTablePtr->begin();
       iter != this->PdxType_CacheableHashTablePtr->end(); iter++) {
    if (!other->PdxType_CacheableHashTablePtr->contains(iter.first())) {
      LOGINFO(
          "PdxAutoMegaType::equals::[CacheableHashTable] expected key missing");
      return false;
    }
  }

  // CacheableVector
  LOGINFO("PdxAutoMegaType::equals::[CacheableVector]");
  if (other->pdxType_CacheableVectorPtr == NULLPTR) {
    LOGINFO("CacheableVector should not be NULL");
    return false;
  }
  LOGINFO("PdxAutoMegaType::equals::[CacheableVector] Size Check");
  if (this->pdxType_CacheableVectorPtr->size() !=
      other->pdxType_CacheableVectorPtr->size()) {
    LOGINFO("CacheableVector size should be equal");
    return false;
  }
  LOGINFO("PdxAutoMegaType::equals::[CacheableVector] Element Check");
  int index = 1;
  for (CacheableVector::Iterator itr =
           other->pdxType_CacheableVectorPtr->begin();
       itr != other->pdxType_CacheableVectorPtr->end(); ++itr) {
    valLeft = CacheableInt32::create(index)->value();
    valRight = dynCast<CacheableInt32Ptr>(*itr)->value();
    if (valLeft != valRight) {
      LOGINFO("PdxAutoMegaType::equals::[CacheableVector]type values mismatch");
    }
    index++;
  }

  // CacheableHashSet
  LOGINFO("PdxAutoMegaType::equals::[CacheableHashSet]");
  if (other->pdxType_CacheableHashSetPtr == NULLPTR) {
    LOGINFO("CacheableHashSet should not be NULL");
    return false;
  }
  LOGINFO("PdxAutoMegaType::equals::[CacheableHashSet] Size Check");
  if (this->pdxType_CacheableHashSetPtr->size() !=
      other->pdxType_CacheableHashSetPtr->size()) {
    LOGINFO("CacheableHashSet size should be equal");
    return false;
  }

  LOGINFO("PdxAutoMegaType::equals::[CacheableHashSet] Element Check");
  for (CacheableHashSet::Iterator iter =
           this->pdxType_CacheableHashSetPtr->begin();
       iter != this->pdxType_CacheableHashSetPtr->end(); iter++) {
    if (!other->pdxType_CacheableHashSetPtr->contains(*iter)) {
      LOGINFO("PdxAutoMegaType::equals::[CacheableHashSet] values mismatch");
      return false;
    }
  }

  // CacheableLinkedHashSet
  LOGINFO("PdxAutoMegaType::equals::[CacheableLinkedHashSet]");
  if (other->pdxType_CacheableLinkedHashSetPtr == NULLPTR) {
    LOGINFO("CacheableLinkedHashSet should not be NULL");
    return false;
  }
  LOGINFO("PdxAutoMegaType::equals::[CacheableLinkedHashSet] Size Check");
  if (this->pdxType_CacheableLinkedHashSetPtr->size() !=
      other->pdxType_CacheableLinkedHashSetPtr->size()) {
    LOGINFO("CacheableLinkedHashSet size should be equal");
    return false;
  }
  LOGINFO("PdxAutoMegaType::equals::[CacheableLinkedHashSet] Element Check");
  for (CacheableLinkedHashSet::Iterator iter =
           this->pdxType_CacheableLinkedHashSetPtr->begin();
       iter != this->pdxType_CacheableLinkedHashSetPtr->end(); iter++) {
    if (!other->pdxType_CacheableLinkedHashSetPtr->contains(*iter)) {
      return false;
    }
  }
  return true;
}

void PdxAutoMegaType::initPdxAutoMegaType() {
  populatePrimitives();
  populateArrays();
  populateCacheableContainers();
}

bool PdxAutoMegaType::equals(PdxSerializablePtr obj) {
  if (obj == NULLPTR) return false;

  if (true != verifyPrimitives(obj)) {
    return false;
  }
  if (true != verifyArrays(obj)) {
    return false;
  }
  if (true != verifyCacheableContainers(obj)) {
    return false;
  }
  return true;
}
