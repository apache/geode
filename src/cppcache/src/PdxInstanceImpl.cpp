/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "PdxInstanceImpl.hpp"
#include "PdxTypeRegistry.hpp"
#include "PdxHelper.hpp"
#include "PdxTypes.hpp"
#include <gfcpp/PdxFieldTypes.hpp>
#include "PdxLocalWriter.hpp"
#include <gfcpp/PdxReader.hpp>
#include "CacheRegionHelper.hpp"
#include <gfcpp/Cache.hpp>
#include "CacheImpl.hpp"
#include "Utils.hpp"
#include <algorithm>

/* adongre  - Coverity II
* CID 29255: Calling risky function (SECURE_CODING)[VERY RISKY]. Using "sprintf"
* can cause a
* buffer overflow when done incorrectly. Because sprintf() assumes an
* arbitrarily long string,
* callers must be careful not to overflow the actual space of the destination.
* Use snprintf() instead, or correct precision specifiers.
* Fix : using ACE_OS::snprintf
*/

#define VERIFY_PDX_INSTANCE_FIELD_THROW                                 \
  if (pft == NULLPTR) {                                                 \
    char excpStr[256] = {0};                                            \
    ACE_OS::snprintf(excpStr, 256, "PdxInstance doesn't has field %s ", \
                     fieldname);                                        \
    throw IllegalStateException(excpStr);                               \
  }

namespace gemfire {

int8_t PdxInstanceImpl::m_BooleanDefaultBytes[] = {0};
int8_t PdxInstanceImpl::m_ByteDefaultBytes[] = {0};
int8_t PdxInstanceImpl::m_ShortDefaultBytes[] = {0, 0};
int8_t PdxInstanceImpl::m_CharDefaultBytes[] = {0, 0};
int8_t PdxInstanceImpl::m_IntDefaultBytes[] = {0, 0, 0, 0};
int8_t PdxInstanceImpl::m_LongDefaultBytes[] = {0, 0, 0, 0, 0, 0, 0, 0};
int8_t PdxInstanceImpl::m_FloatDefaultBytes[] = {0, 0, 0, 0};
int8_t PdxInstanceImpl::m_DoubleDefaultBytes[] = {0, 0, 0, 0, 0, 0, 0, 0};
int8_t PdxInstanceImpl::m_DateDefaultBytes[] = {-1, -1, -1, -1, -1, -1, -1, -1};
int8_t PdxInstanceImpl::m_StringDefaultBytes[] = {
    gemfire::GemfireTypeIds::CacheableNullString};
int8_t PdxInstanceImpl::m_ObjectDefaultBytes[] = {
    gemfire::GemfireTypeIds::NullObj};
int8_t PdxInstanceImpl::m_NULLARRAYDefaultBytes[] = {-1};
PdxFieldTypePtr PdxInstanceImpl::m_DefaultPdxFieldType(
    new PdxFieldType("default", "default", static_cast<uint8_t>(-1),
                     -1 /*field index*/, false, 1, -1 /*var len field idx*/));

bool sortFunc(PdxFieldTypePtr field1, PdxFieldTypePtr field2) {
  int diff = ACE_OS::strcmp(field1->getFieldName(), field2->getFieldName());
  if (diff < 0) {
    return true;
  } else {
    return false;
  }
}

PdxInstanceImpl::~PdxInstanceImpl() { GF_SAFE_DELETE_ARRAY(m_buffer); }

PdxInstanceImpl::PdxInstanceImpl(gemfire::FieldVsValues fieldVsValue,
                                 gemfire::PdxTypePtr pdxType) {
  m_pdxType = pdxType;
  m_updatedFields = fieldVsValue;
  m_buffer = NULL;
  m_bufferLength = 0;
  m_typeId = 0;

  m_pdxType->InitializeType();  // to generate static position map

  // gemfire::DataOutput* output = gemfire::DataOutput::getDataOutput();
  DataOutput output;
  PdxHelper::serializePdx(output, *this);
}

PdxInstanceImpl::PdxInstanceImpl() {
  m_pdxType = NULLPTR;
  m_buffer = NULL;
  m_bufferLength = 0;
  m_typeId = 0;
}

void PdxInstanceImpl::writeField(PdxWriterPtr writer, const char* fieldName,
                                 int typeId, CacheablePtr value) {
  switch (typeId) {
    case PdxFieldTypes::INT: {
      CacheableInt32* val = dynamic_cast<CacheableInt32*>(value.ptr());
      if (val != NULL) {
        writer->writeInt(fieldName, val->value());
      }
      break;
    }
    case PdxFieldTypes::STRING: {
      CacheableString* val = dynamic_cast<CacheableString*>(value.ptr());
      if (val != NULL) {
        if (val->isWideString()) {
          writer->writeWideString(fieldName, val->asWChar());
        } else if (val->isCString()) {
          writer->writeString(fieldName, val->asChar());
        }
      }
      break;
    }
    case PdxFieldTypes::BOOLEAN: {
      CacheableBoolean* val = dynamic_cast<CacheableBoolean*>(value.ptr());
      if (val != NULL) {
        writer->writeBoolean(fieldName, val->value());
      }
      break;
    }
    case PdxFieldTypes::FLOAT: {
      CacheableFloat* val = dynamic_cast<CacheableFloat*>(value.ptr());
      if (val != NULL) {
        writer->writeFloat(fieldName, val->value());
      }
      break;
    }
    case PdxFieldTypes::DOUBLE: {
      CacheableDouble* val = dynamic_cast<CacheableDouble*>(value.ptr());
      if (val != NULL) {
        writer->writeDouble(fieldName, val->value());
      }
      break;
    }
    case PdxFieldTypes::CHAR: {
      CacheableWideChar* val = dynamic_cast<CacheableWideChar*>(value.ptr());
      if (val != NULL) {
        writer->writeChar(fieldName, static_cast<char>(val->value()));
      }
      break;
    }
    case PdxFieldTypes::BYTE: {
      CacheableByte* val = dynamic_cast<CacheableByte*>(value.ptr());
      if (val != NULL) {
        writer->writeByte(fieldName, val->value());
      }
      break;
    }
    case PdxFieldTypes::SHORT: {
      CacheableInt16* val = dynamic_cast<CacheableInt16*>(value.ptr());
      if (val != NULL) {
        writer->writeShort(fieldName, val->value());
      }
      break;
    }
    case PdxFieldTypes::LONG: {
      CacheableInt64* val = dynamic_cast<CacheableInt64*>(value.ptr());
      if (val != NULL) {
        writer->writeLong(fieldName, val->value());
      }
      break;
    }
    case PdxFieldTypes::BYTE_ARRAY: {
      CacheableBytes* val = dynamic_cast<CacheableBytes*>(value.ptr());
      if (val != NULL) {
        writer->writeByteArray(fieldName, (int8_t*)val->value(), val->length());
      }
      break;
    }
    case PdxFieldTypes::DOUBLE_ARRAY: {
      CacheableDoubleArray* val =
          dynamic_cast<CacheableDoubleArray*>(value.ptr());
      if (val != NULL) {
        writer->writeDoubleArray(fieldName, const_cast<double*>(val->value()),
                                 val->length());
      }
      break;
    }
    case PdxFieldTypes::FLOAT_ARRAY: {
      CacheableFloatArray* val =
          dynamic_cast<CacheableFloatArray*>(value.ptr());
      if (val != NULL) {
        writer->writeFloatArray(fieldName, const_cast<float*>(val->value()),
                                val->length());
      }
      break;
    }
    case PdxFieldTypes::SHORT_ARRAY: {
      CacheableInt16Array* val =
          dynamic_cast<CacheableInt16Array*>(value.ptr());
      if (val != NULL) {
        writer->writeShortArray(fieldName, const_cast<int16_t*>(val->value()),
                                val->length());
      }
      break;
    }
    case PdxFieldTypes::INT_ARRAY: {
      CacheableInt32Array* val =
          dynamic_cast<CacheableInt32Array*>(value.ptr());
      if (val != NULL) {
        writer->writeIntArray(fieldName, const_cast<int32_t*>(val->value()),
                              val->length());
      }
      break;
    }
    case PdxFieldTypes::LONG_ARRAY: {
      CacheableInt64Array* val =
          dynamic_cast<CacheableInt64Array*>(value.ptr());
      if (val != NULL) {
        writer->writeLongArray(fieldName, const_cast<int64_t*>(val->value()),
                               val->length());
      }
      break;
    }
    case PdxFieldTypes::BOOLEAN_ARRAY: {
      BooleanArray* val = dynamic_cast<BooleanArray*>(value.ptr());
      if (val != NULL) {
        writer->writeBooleanArray(fieldName, const_cast<bool*>(val->value()),
                                  val->length());
      }
      break;
    }
    case PdxFieldTypes::CHAR_ARRAY: {
      CharArray* val = dynamic_cast<CharArray*>(value.ptr());
      if (val != NULL) {
        writer->writeWideCharArray(
            fieldName, const_cast<wchar_t*>(val->value()), val->length());
      }
      break;
    }
    case PdxFieldTypes::STRING_ARRAY: {
      CacheableStringArray* val =
          dynamic_cast<CacheableStringArray*>(value.ptr());
      if (val != NULL) {
        int size = val->length();
        if (val->operator[](0)->isCString()) {
          char** strings = new char*[size];
          for (int item = 0; item < size; item++) {
            strings[item] = const_cast<char*>(val->operator[](item)->asChar());
          }
          writer->writeStringArray(fieldName, strings, size);
          delete[] strings;
        } else if (val->operator[](0)->isWideString()) {
          wchar_t** strings = new wchar_t*[size];
          for (int item = 0; item < size; item++) {
            strings[item] =
                const_cast<wchar_t*>(val->operator[](item)->asWChar());
          }
          writer->writeWideStringArray(fieldName, strings, size);
          delete[] strings;
        }
      }
      break;
    }
    case PdxFieldTypes::DATE: {
      CacheableDatePtr date = dynCast<CacheableDatePtr>(value);
      if (date != NULLPTR) {
        writer->writeDate(fieldName, date);
      }
      break;
    }
    case PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS: {
      CacheableVector* vector = dynamic_cast<CacheableVector*>(value.ptr());

      if (vector != NULL) {
        int size = vector->size();
        int8_t** values = new int8_t*[size];
        int* lengths = new int[size];
        for (int i = 0; i < size; i++) {
          CacheableBytes* val =
              dynamic_cast<CacheableBytes*>(vector->at(i).ptr());
          if (val != NULL) {
            values[i] = (int8_t*)val->value();
            lengths[i] = val->length();
          }
        }
        writer->writeArrayOfByteArrays(fieldName, values, size, lengths);
        delete[] values;
        delete[] lengths;
      }
      break;
    }
    case PdxFieldTypes::OBJECT_ARRAY: {
      CacheableObjectArrayPtr objArray =
          dynCast<CacheableObjectArrayPtr>(value);
      if (objArray != NULLPTR) {
        writer->writeObjectArray(fieldName, objArray);
      }
      break;
    }
    default: { writer->writeObject(fieldName, value); }
  }
}

WritablePdxInstancePtr PdxInstanceImpl::createWriter() {
  LOGDEBUG("PdxInstanceImpl::createWriter m_bufferLength = %d m_typeId = %d ",
           m_bufferLength, m_typeId);
  WritablePdxInstancePtr wlr(
      new PdxInstanceImpl(m_buffer, m_bufferLength,
                          m_typeId));  // need to create duplicate byte stream);
  return wlr;
}

bool PdxInstanceImpl::enumerateObjectArrayEquals(
    CacheableObjectArrayPtr Obj, CacheableObjectArrayPtr OtherObj) {
  if (Obj == NULLPTR && OtherObj == NULLPTR) {
    return true;
  } else if (Obj == NULLPTR && OtherObj != NULLPTR) {
    return false;
  } else if (Obj != NULLPTR && OtherObj == NULLPTR) {
    return false;
  }

  if (Obj->size() != OtherObj->size()) {
    return false;
  }

  for (int i = 0; i < Obj->size(); i++) {
    if (!deepArrayEquals(Obj->at(i), OtherObj->at(i))) {
      return false;
    }
  }
  return true;
}

bool PdxInstanceImpl::enumerateVectorEquals(CacheableVectorPtr Obj,
                                            CacheableVectorPtr OtherObj) {
  if (Obj == NULLPTR && OtherObj == NULLPTR) {
    return true;
  } else if (Obj == NULLPTR && OtherObj != NULLPTR) {
    return false;
  } else if (Obj != NULLPTR && OtherObj == NULLPTR) {
    return false;
  }

  if (Obj->size() != OtherObj->size()) {
    return false;
  }

  for (int i = 0; i < Obj->size(); i++) {
    if (!deepArrayEquals(Obj->at(i), OtherObj->at(i))) {
      return false;
    }
  }
  return true;
}

bool PdxInstanceImpl::enumerateArrayListEquals(CacheableArrayListPtr Obj,
                                               CacheableArrayListPtr OtherObj) {
  if (Obj == NULLPTR && OtherObj == NULLPTR) {
    return true;
  } else if (Obj == NULLPTR && OtherObj != NULLPTR) {
    return false;
  } else if (Obj != NULLPTR && OtherObj == NULLPTR) {
    return false;
  }

  if (Obj->size() != OtherObj->size()) {
    return false;
  }

  for (int i = 0; i < Obj->size(); i++) {
    if (!deepArrayEquals(Obj->at(i), OtherObj->at(i))) {
      return false;
    }
  }
  return true;
}

bool PdxInstanceImpl::enumerateMapEquals(CacheableHashMapPtr Obj,
                                         CacheableHashMapPtr OtherObj) {
  if (Obj == NULLPTR && OtherObj == NULLPTR) {
    return true;
  } else if (Obj == NULLPTR && OtherObj != NULLPTR) {
    return false;
  } else if (Obj != NULLPTR && OtherObj == NULLPTR) {
    return false;
  }

  if (Obj->size() != OtherObj->size()) {
    return false;
  }

  for (CacheableHashMap::Iterator iter = Obj->begin(); iter != Obj->end();
       iter++) {
    if (OtherObj->contains(iter.first())) {
      CacheableHashMap::Iterator otherIter = OtherObj->find(iter.first());
      if (otherIter != OtherObj->end()) {
        if (!deepArrayEquals(iter.second(), otherIter.second())) {
          return false;
        }
      }
    } else {
      return false;
    }
  }
  return true;
}

bool PdxInstanceImpl::enumerateHashTableEquals(CacheableHashTablePtr Obj,
                                               CacheableHashTablePtr OtherObj) {
  if (Obj == NULLPTR && OtherObj == NULLPTR) {
    return true;
  } else if (Obj == NULLPTR && OtherObj != NULLPTR) {
    return false;
  } else if (Obj != NULLPTR && OtherObj == NULLPTR) {
    return false;
  }

  if (Obj->size() != OtherObj->size()) {
    return false;
  }

  for (CacheableHashTable::Iterator iter = Obj->begin(); iter != Obj->end();
       iter++) {
    if (OtherObj->contains(iter.first())) {
      CacheableHashTable::Iterator otherIter = OtherObj->find(iter.first());
      if (otherIter != OtherObj->end()) {
        if (!deepArrayEquals(iter.second(), otherIter.second())) {
          return false;
        }
      }
    } else {
      return false;
    }
  }
  return true;
}

bool PdxInstanceImpl::enumerateSetEquals(CacheableHashSetPtr Obj,
                                         CacheableHashSetPtr OtherObj) {
  if (Obj == NULLPTR && OtherObj == NULLPTR) {
    return true;
  } else if (Obj == NULLPTR && OtherObj != NULLPTR) {
    return false;
  } else if (Obj != NULLPTR && OtherObj == NULLPTR) {
    return false;
  }

  if (Obj->size() != OtherObj->size()) {
    return false;
  }
  for (CacheableHashSet::Iterator iter = Obj->begin(); iter != Obj->end();
       iter++) {
    if (!OtherObj->contains(*iter)) {
      return false;
    }
  }
  return true;
}

bool PdxInstanceImpl::enumerateLinkedSetEquals(
    CacheableLinkedHashSetPtr Obj, CacheableLinkedHashSetPtr OtherObj) {
  if (Obj == NULLPTR && OtherObj == NULLPTR) {
    return true;
  } else if (Obj == NULLPTR && OtherObj != NULLPTR) {
    return false;
  } else if (Obj != NULLPTR && OtherObj == NULLPTR) {
    return false;
  }

  if (Obj->size() != OtherObj->size()) {
    return false;
  }
  for (CacheableLinkedHashSet::Iterator iter = Obj->begin(); iter != Obj->end();
       iter++) {
    if (!OtherObj->contains(*iter)) {
      return false;
    }
  }
  return true;
}

bool PdxInstanceImpl::deepArrayEquals(CacheablePtr obj, CacheablePtr otherObj) {
  if (obj == NULLPTR && otherObj == NULLPTR) {
    return true;
  } else if (obj == NULLPTR && otherObj != NULLPTR) {
    return false;
  } else if (obj != NULLPTR && otherObj == NULLPTR) {
    return false;
  }

  int8_t typeId = obj->typeId();
  switch (typeId) {
    case GemfireTypeIds::CacheableObjectArray: {
      CacheableObjectArrayPtr objArrayPtr =
          dynCast<CacheableObjectArrayPtr>(obj);
      CacheableObjectArrayPtr otherObjArrayPtr =
          dynCast<CacheableObjectArrayPtr>(otherObj);
      return enumerateObjectArrayEquals(objArrayPtr, otherObjArrayPtr);
    }
    case GemfireTypeIds::CacheableVector: {
      CacheableVectorPtr vec = dynCast<CacheableVectorPtr>(obj);
      CacheableVectorPtr otherVec = dynCast<CacheableVectorPtr>(otherObj);
      return enumerateVectorEquals(vec, otherVec);
    }
    case GemfireTypeIds::CacheableArrayList: {
      CacheableArrayListPtr arrList = dynCast<CacheableArrayListPtr>(obj);
      CacheableArrayListPtr otherArrList =
          dynCast<CacheableArrayListPtr>(otherObj);
      return enumerateArrayListEquals(arrList, otherArrList);
    }
    case GemfireTypeIds::CacheableHashMap: {
      CacheableHashMapPtr map = dynCast<CacheableHashMapPtr>(obj);
      CacheableHashMapPtr otherMap = dynCast<CacheableHashMapPtr>(otherObj);
      return enumerateMapEquals(map, otherMap);
    }
    case GemfireTypeIds::CacheableHashSet: {
      CacheableHashSetPtr hashset = dynCast<CacheableHashSetPtr>(obj);
      CacheableHashSetPtr otherHashset = dynCast<CacheableHashSetPtr>(otherObj);
      return enumerateSetEquals(hashset, otherHashset);
    }
    case GemfireTypeIds::CacheableLinkedHashSet: {
      CacheableLinkedHashSetPtr linkedHashset =
          dynCast<CacheableLinkedHashSetPtr>(obj);
      CacheableLinkedHashSetPtr otherLinkedHashset =
          dynCast<CacheableLinkedHashSetPtr>(otherObj);
      return enumerateLinkedSetEquals(linkedHashset, otherLinkedHashset);
    }
    case GemfireTypeIds::CacheableHashTable: {
      CacheableHashTablePtr hashTable = dynCast<CacheableHashTablePtr>(obj);
      CacheableHashTablePtr otherhashTable =
          dynCast<CacheableHashTablePtr>(otherObj);
      return enumerateHashTableEquals(hashTable, otherhashTable);
    }
    default: {
      PdxInstancePtr pdxInstPtr = NULLPTR;
      PdxInstancePtr otherPdxInstPtr = NULLPTR;
      try {
        pdxInstPtr = dynCast<PdxInstancePtr>(obj);
        otherPdxInstPtr = dynCast<PdxInstancePtr>(otherObj);
      } catch (ClassCastException& /*ex*/) {
        // ignore
      }
      if (pdxInstPtr != NULLPTR && otherPdxInstPtr != NULLPTR) {
        return (*pdxInstPtr.ptr() == *otherPdxInstPtr.ptr());
      }
      // Chk if it is of CacheableKeyPtr type, eg: CacheableInt32
      else {
        CacheableKey* keyType = NULL;
        CacheableKey* otherKeyType = NULL;
        try {
          keyType = dynamic_cast<CacheableKey*>(obj.ptr());
          otherKeyType = dynamic_cast<CacheableKey*>(otherObj.ptr());
        } catch (ClassCastException&) {
          // ignore
        }
        if (keyType != NULL && otherKeyType != NULL) {
          return keyType->operator==(*otherKeyType);
        }
      }
      char excpStr[256] = {0};
      /* adongre  - Coverity II
      * CID 29210: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
      * "sprintf" can cause a
      * buffer overflow when done incorrectly. Because sprintf() assumes an
      * arbitrarily long string,
      * callers must be careful not to overflow the actual space of the
      * destination.
      * Use snprintf() instead, or correct precision specifiers.
      * Fix : using ACE_OS::snprintf
      */
      ACE_OS::snprintf(excpStr, 256,
                       "PdxInstance cannot calculate equals of the field %s "
                       "since equals is only supported for CacheableKey "
                       "derived types ",
                       obj->toString()->asChar());
      throw IllegalStateException(excpStr);
    }
  }
}

int PdxInstanceImpl::enumerateMapHashCode(CacheableHashMapPtr map) {
  int h = 0;
  for (CacheableHashMap::Iterator itr = map->begin(); itr != map->end();
       itr++) {
    h = h + ((deepArrayHashCode(itr.first())) ^
             ((itr.second() != NULLPTR) ? deepArrayHashCode(itr.second()) : 0));
  }
  return h;
}

int PdxInstanceImpl::enumerateSetHashCode(CacheableHashSetPtr set) {
  int h = 0;
  for (CacheableHashSet::Iterator itr = set->begin(); itr != set->end();
       itr++) {
    h = h + deepArrayHashCode(*itr);
  }
  return h;
}

int PdxInstanceImpl::enumerateLinkedSetHashCode(CacheableLinkedHashSetPtr set) {
  int h = 0;
  for (CacheableLinkedHashSet::Iterator itr = set->begin(); itr != set->end();
       itr++) {
    h = h + deepArrayHashCode(*itr);
  }
  return h;
}

int PdxInstanceImpl::enumerateHashTableCode(CacheableHashTablePtr hashTable) {
  int h = 0;
  for (CacheableHashTable::Iterator itr = hashTable->begin();
       itr != hashTable->end(); itr++) {
    h = h + ((deepArrayHashCode(itr.first())) ^
             ((itr.second() != NULLPTR) ? deepArrayHashCode(itr.second()) : 0));
  }
  return h;
}

int PdxInstanceImpl::enumerateObjectArrayHashCode(
    CacheableObjectArrayPtr objArray) {
  int h = 1;
  for (int i = 0; i < objArray->size(); i++) {
    h = h * 31 + deepArrayHashCode(objArray->at(i));
  }
  return h;
}

int PdxInstanceImpl::enumerateVectorHashCode(CacheableVectorPtr vec) {
  int h = 1;
  for (int i = 0; i < vec->size(); i++) {
    h = h * 31 + deepArrayHashCode(vec->at(i));
  }
  return h;
}

int PdxInstanceImpl::enumerateArrayListHashCode(CacheableArrayListPtr arrList) {
  int h = 1;
  for (int i = 0; i < arrList->size(); i++) {
    h = h * 31 + deepArrayHashCode(arrList->at(i));
  }
  return h;
}

int PdxInstanceImpl::enumerateLinkedListHashCode(
    CacheableLinkedListPtr linkedList) {
  int h = 1;
  for (int i = 0; i < linkedList->size(); i++) {
    h = h * 31 + deepArrayHashCode(linkedList->at(i));
  }
  return h;
}

int PdxInstanceImpl::deepArrayHashCode(CacheablePtr obj) {
  if (obj == NULLPTR) {
    return 0;
  }

  int8_t typeId = obj->typeId();
  switch (typeId) {
    case GemfireTypeIds::CacheableObjectArray: {
      CacheableObjectArrayPtr objArrayPtr =
          dynCast<CacheableObjectArrayPtr>(obj);
      return enumerateObjectArrayHashCode(objArrayPtr);
    }
    case GemfireTypeIds::CacheableVector: {
      CacheableVectorPtr vec = dynCast<CacheableVectorPtr>(obj);
      return enumerateVectorHashCode(vec);
    }
    case GemfireTypeIds::CacheableArrayList: {
      CacheableArrayListPtr arrList = dynCast<CacheableArrayListPtr>(obj);
      return enumerateArrayListHashCode(arrList);
    }
    case GemfireTypeIds::CacheableLinkedList: {
      CacheableLinkedListPtr linkedList = dynCast<CacheableLinkedListPtr>(obj);
      return enumerateLinkedListHashCode(linkedList);
    }
    case GemfireTypeIds::CacheableHashMap: {
      CacheableHashMapPtr map = dynCast<CacheableHashMapPtr>(obj);
      return enumerateMapHashCode(map);
    }
    case GemfireTypeIds::CacheableHashSet: {
      CacheableHashSetPtr hashset = dynCast<CacheableHashSetPtr>(obj);
      return enumerateSetHashCode(hashset);
    }
    case GemfireTypeIds::CacheableLinkedHashSet: {
      CacheableLinkedHashSetPtr linkedHashSet =
          dynCast<CacheableLinkedHashSetPtr>(obj);
      return enumerateLinkedSetHashCode(linkedHashSet);
    }
    case GemfireTypeIds::CacheableHashTable: {
      CacheableHashTablePtr hashTable = dynCast<CacheableHashTablePtr>(obj);
      return enumerateHashTableCode(hashTable);
    }
    default: {
      PdxInstancePtr pdxInstPtr = NULLPTR;
      try {
        pdxInstPtr = dynCast<PdxInstancePtr>(obj);
      } catch (ClassCastException& /*ex*/) {
        // ignore
      }
      if (pdxInstPtr != NULLPTR) {
        return pdxInstPtr->hashcode();
      }
      // Chk if it is of CacheableKeyPtr type, eg: CacheableInt32
      else {
        CacheableKeyPtr keyType = NULLPTR;
        try {
          keyType = dynCast<CacheableKeyPtr>(obj);
        } catch (ClassCastException&) {
          // ignore
        }
        if (keyType != NULLPTR) {
          return keyType->hashcode();
        }
      }
      // Else throwing exception since no type matched.
      char excpStr[256] = {0};
      /* adongre  - Coverity II
      * CID 29211: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
      * "sprintf" can cause a
      * buffer overflow when done incorrectly. Because sprintf() assumes an
      * arbitrarily long string,
      * callers must be careful not to overflow the actual space of the
      * destination.
      * Use snprintf() instead, or correct precision specifiers.
      * Fix : using ACE_OS::snprintf
      */
      ACE_OS::snprintf(excpStr, 256,
                       "PdxInstance cannot calculate hashcode of the field %s "
                       "since hashcode is only supported for CacheableKey "
                       "derived types ",
                       obj->toString()->asChar());
      throw IllegalStateException(excpStr);
    }
  }
}

uint32_t PdxInstanceImpl::hashcode() const {
  int hashCode = 1;

  PdxTypePtr pt = getPdxType();

  std::vector<PdxFieldTypePtr> pdxIdentityFieldList = getIdentityPdxFields(pt);

  DataInput dataInput(m_buffer, m_bufferLength);

  for (uint32_t i = 0; i < pdxIdentityFieldList.size(); i++) {
    PdxFieldTypePtr pField = pdxIdentityFieldList.at(i);

    LOGDEBUG("hashcode for pdxfield %s  hashcode is %d ",
             pField->getFieldName(), hashCode);
    switch (pField->getTypeId()) {
      case PdxFieldTypes::CHAR:
      case PdxFieldTypes::BOOLEAN:
      case PdxFieldTypes::BYTE:
      case PdxFieldTypes::SHORT:
      case PdxFieldTypes::INT:
      case PdxFieldTypes::LONG:
      case PdxFieldTypes::DATE:
      case PdxFieldTypes::FLOAT:
      case PdxFieldTypes::DOUBLE:
      case PdxFieldTypes::STRING:
      case PdxFieldTypes::BOOLEAN_ARRAY:
      case PdxFieldTypes::CHAR_ARRAY:
      case PdxFieldTypes::BYTE_ARRAY:
      case PdxFieldTypes::SHORT_ARRAY:
      case PdxFieldTypes::INT_ARRAY:
      case PdxFieldTypes::LONG_ARRAY:
      case PdxFieldTypes::FLOAT_ARRAY:
      case PdxFieldTypes::DOUBLE_ARRAY:
      case PdxFieldTypes::STRING_ARRAY:
      case PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS: {
        int retH = getRawHashCode(pt, pField, dataInput);
        if (retH != 0) hashCode = 31 * hashCode + retH;
        break;
      }
      case PdxFieldTypes::OBJECT: {
        setOffsetForObject(dataInput, pt, pField->getSequenceId());
        CacheablePtr object = NULLPTR;
        dataInput.readObject(object);
        if (object != NULLPTR) {
          hashCode = 31 * hashCode + deepArrayHashCode(object);
        }
        break;
      }
      case PdxFieldTypes::OBJECT_ARRAY: {
        setOffsetForObject(dataInput, pt, pField->getSequenceId());
        CacheableObjectArrayPtr objectArray = CacheableObjectArray::create();
        objectArray->fromData(dataInput);
        hashCode =
            31 * hashCode +
            ((objectArray != NULLPTR) ? deepArrayHashCode(objectArray) : 0);
        break;
      }
      default: {
        char excpStr[256] = {0};
        /* adongre  - Coverity II
        * CID 29264: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
        * "sprintf" can cause a
        * buffer overflow when done incorrectly. Because sprintf() assumes an
        * arbitrarily long string,
        * callers must be careful not to overflow the actual space of the
        * destination.
        * Use snprintf() instead, or correct precision specifiers.
        * Fix : using ACE_OS::snprintf
        */
        ACE_OS::snprintf(excpStr, 256, "PdxInstance not found typeid %d ",
                         pField->getTypeId());
        throw IllegalStateException(excpStr);
      }
    }
  }
  return hashCode;
}

void PdxInstanceImpl::updatePdxStream(uint8_t* newPdxStream, int len) {
  m_buffer = DataInput::getBufferCopy(newPdxStream, len);
  m_bufferLength = len;
}

PdxTypePtr PdxInstanceImpl::getPdxType() const {
  if (m_typeId == 0) {
    if (m_pdxType == NULLPTR) {
      throw IllegalStateException("PdxType should not be null..");
    }
    return m_pdxType;
  }
  PdxTypePtr pType = PdxTypeRegistry::getPdxType(m_typeId);
  return pType;
}

bool PdxInstanceImpl::isIdentityField(const char* fieldname) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  if (pft != NULLPTR) {
    return pft->getIdentityField();
  }
  return false;
}

bool PdxInstanceImpl::hasField(const char* fieldname) {
  PdxTypePtr pf = getPdxType();
  PdxFieldTypePtr pft = pf->getPdxField(fieldname);
  return (pft != NULLPTR);
}

void PdxInstanceImpl::getField(const char* fieldname, bool& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);

  VERIFY_PDX_INSTANCE_FIELD_THROW;

  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());

  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readBoolean(&value);
}

void PdxInstanceImpl::getField(const char* fieldname,
                               signed char& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);

  VERIFY_PDX_INSTANCE_FIELD_THROW;

  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());

  dataInput.reset();
  dataInput.advanceCursor(pos);
  int8_t tmp = 0;
  dataInput.read(&tmp);
  value = (signed char)tmp;
}

void PdxInstanceImpl::getField(const char* fieldname,
                               unsigned char& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);

  VERIFY_PDX_INSTANCE_FIELD_THROW;

  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());

  dataInput.reset();
  dataInput.advanceCursor(pos);
  int8_t tmp = 0;
  dataInput.read(&tmp);
  value = static_cast<unsigned char>(tmp);
}

void PdxInstanceImpl::getField(const char* fieldname, int16_t& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readInt(&value);
}

void PdxInstanceImpl::getField(const char* fieldname, int32_t& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());

  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readInt(&value);
}

void PdxInstanceImpl::getField(const char* fieldname, int64_t& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readInt(&value);
}

void PdxInstanceImpl::getField(const char* fieldname, float& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readFloat(&value);
}

void PdxInstanceImpl::getField(const char* fieldname, double& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readDouble(&value);
}

void PdxInstanceImpl::getField(const char* fieldname, wchar_t& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  uint16_t temp = 0;
  dataInput.readInt(&temp);
  value = static_cast<wchar_t>(temp);
}

void PdxInstanceImpl::getField(const char* fieldname, char& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  uint16_t temp = 0;
  dataInput.readInt(&temp);
  value = static_cast<char>(temp);
}

void PdxInstanceImpl::getField(const char* fieldname, bool** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readBooleanArray(value, length);
}

void PdxInstanceImpl::getField(const char* fieldname, signed char** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  int8_t* temp = NULL;
  dataInput.readByteArray(&temp, length);
  *value = (signed char*)temp;
}

void PdxInstanceImpl::getField(const char* fieldname, unsigned char** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  int8_t* temp = NULL;
  dataInput.readByteArray(&temp, length);
  *value = reinterpret_cast<unsigned char*>(temp);
}

void PdxInstanceImpl::getField(const char* fieldname, int16_t** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readShortArray(value, length);
}

void PdxInstanceImpl::getField(const char* fieldname, int32_t** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readIntArray(value, length);
}

void PdxInstanceImpl::getField(const char* fieldname, int64_t** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readLongArray(value, length);
}

void PdxInstanceImpl::getField(const char* fieldname, float** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readFloatArray(value, length);
}

void PdxInstanceImpl::getField(const char* fieldname, double** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readDoubleArray(value, length);
}

void PdxInstanceImpl::getField(const char* fieldname, wchar_t** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readWideCharArray(value, length);
}

void PdxInstanceImpl::getField(const char* fieldname, char** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readCharArray(value, length);
}

void PdxInstanceImpl::getField(const char* fieldname, wchar_t** value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  wchar_t* temp = NULL;
  dataInput.readWideString(&temp);
  *value = temp;
}

void PdxInstanceImpl::getField(const char* fieldname, char** value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  char* temp = NULL;
  dataInput.readString(&temp);
  *value = temp;
}

void PdxInstanceImpl::getField(const char* fieldname, wchar_t*** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readWideStringArray(value, length);
}

void PdxInstanceImpl::getField(const char* fieldname, char*** value,
                               int32_t& length) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readStringArray(value, length);
}

void PdxInstanceImpl::getField(const char* fieldname,
                               CacheableDatePtr& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  value = CacheableDate::create();
  value->fromData(dataInput);
}

void PdxInstanceImpl::getField(const char* fieldname,
                               CacheablePtr& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readObject(value);
}

void PdxInstanceImpl::getField(const char* fieldname,
                               CacheableObjectArrayPtr& value) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  value = CacheableObjectArray::create();
  value->fromData(dataInput);
}

void PdxInstanceImpl::getField(const char* fieldname, int8_t*** value,
                               int32_t& arrayLength,
                               int32_t*& elementLength) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);
  VERIFY_PDX_INSTANCE_FIELD_THROW;
  DataInput dataInput(m_buffer, m_bufferLength);
  int pos = getOffset(dataInput, pt, pft->getSequenceId());
  dataInput.reset();
  dataInput.advanceCursor(pos);
  dataInput.readArrayOfByteArrays(value, arrayLength, &elementLength);
}

CacheableStringPtr PdxInstanceImpl::toString() const {
  /* adongre - Coverity II
  * CID 29265: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
  * "sprintf" can cause a
  * buffer overflow when done incorrectly. Because sprintf() assumes an
  * arbitrarily long string,
  * callers must be careful not to overflow the actual space of the destination.
  * Use snprintf() instead, or correct precision specifiers.
  * Fix : using ACE_OS::snprintf
  */
  PdxTypePtr pt = getPdxType();
  std::string toString = "PDX[";
  char buf[2048];
  ACE_OS::snprintf(buf, 2048, "%d", pt->getTypeId());
  toString += buf;
  toString += ",";
  ACE_OS::snprintf(buf, 2048, "%s", pt->getPdxClassName());
  toString += buf;
  toString += "]{";
  bool firstElement = true;
  std::vector<PdxFieldTypePtr> identityFields = getIdentityPdxFields(pt);
  for (size_t i = 0; i < static_cast<int>(identityFields.size()); i++) {
    if (firstElement) {
      firstElement = false;
    } else {
      toString += ",";
    }
    ACE_OS::snprintf(buf, 2048, "%s", identityFields.at(i)->getFieldName());
    toString += buf;
    toString += "=";

    switch (identityFields.at(i)->getTypeId()) {
      case PdxFieldTypes::BOOLEAN: {
        bool value = false;
        getField(identityFields.at(i)->getFieldName(), value);
        ACE_OS::snprintf(buf, 2048, "%s", value ? "true" : "false");
        toString += buf;
        break;
      }
      case PdxFieldTypes::BYTE: {
        signed char value = 0;
        getField(identityFields.at(i)->getFieldName(), value);
        ACE_OS::snprintf(buf, 2048, "%d", value);
        toString += buf;
        break;
      }
      case PdxFieldTypes::SHORT: {
        int16_t value = 0;
        getField(identityFields.at(i)->getFieldName(), value);
        ACE_OS::snprintf(buf, 2048, "%d", value);
        toString += buf;
        break;
      }
      case PdxFieldTypes::INT: {
        int32_t value = 0;
        getField(identityFields.at(i)->getFieldName(), value);
        ACE_OS::snprintf(buf, 2048, "%d", value);
        toString += buf;
        break;
      }
      case PdxFieldTypes::LONG: {
        int64_t value = 0;
        getField(identityFields.at(i)->getFieldName(), value);
        ACE_OS::snprintf(buf, 2048, "%lld", value);
        toString += buf;
        break;
      }
      case PdxFieldTypes::FLOAT: {
        float value = 0;
        getField(identityFields.at(i)->getFieldName(), value);
        ACE_OS::snprintf(buf, 2048, "%f", value);
        toString += buf;
        break;
      }
      case PdxFieldTypes::DOUBLE: {
        double value = 0;
        getField(identityFields.at(i)->getFieldName(), value);
        ACE_OS::snprintf(buf, 2048, "%f", value);
        toString += buf;
        break;
      }
      case PdxFieldTypes::CHAR: {
        wchar_t value = 0;
        getField(identityFields.at(i)->getFieldName(), value);
        ACE_OS::snprintf(buf, 2048, "%c", value);
        toString += buf;
        break;
      }
      case PdxFieldTypes::STRING: {
        wchar_t* value = 0;
        getField(identityFields.at(i)->getFieldName(), &value);
        ACE_OS::snprintf(buf, 2048, "%ls", value);
        toString += buf;
        break;
      }
      case PdxFieldTypes::CHAR_ARRAY: {
        wchar_t* value = 0;
        int32_t length;
        getField(identityFields.at(i)->getFieldName(), &value, length);
        if (length > 0) {
          for (int i = 0; i < length; i++) {
            ACE_OS::snprintf(buf, 2048, "%c\t", value[i]);
            toString += buf;
          }
          GF_SAFE_DELETE_ARRAY(value);
        }
        break;
      }
      case PdxFieldTypes::STRING_ARRAY: {
        wchar_t** value = 0;
        int32_t length;
        getField(identityFields.at(i)->getFieldName(), &value, length);
        if (length > 0) {
          for (int i = 0; i < length; i++) {
            ACE_OS::snprintf(buf, 2048, "%ls\t", value[i]);
            toString += buf;
          }
        }
        break;
      }
      case PdxFieldTypes::BYTE_ARRAY: {
        signed char* value = 0;
        int32_t length;
        getField(identityFields.at(i)->getFieldName(), &value, length);
        if (length > 0) {
          for (int i = 0; i < length; i++) {
            ACE_OS::snprintf(buf, 2048, "%d\t", value[i]);
            toString += buf;
          }
          GF_SAFE_DELETE_ARRAY(value);
        }
        break;
      }
      case PdxFieldTypes::SHORT_ARRAY: {
        int16_t* value = 0;
        int32_t length;
        getField(identityFields.at(i)->getFieldName(), &value, length);
        if (length > 0) {
          for (int i = 0; i < length; i++) {
            ACE_OS::snprintf(buf, 2048, "%d\t", value[i]);
            toString += buf;
          }
          GF_SAFE_DELETE_ARRAY(value);
        }
        break;
      }
      case PdxFieldTypes::INT_ARRAY: {
        int32_t* value = 0;
        int32_t length;
        getField(identityFields.at(i)->getFieldName(), &value, length);
        if (length > 0) {
          for (int i = 0; i < length; i++) {
            ACE_OS::snprintf(buf, 2048, "%d\t", value[i]);
            toString += buf;
          }
          GF_SAFE_DELETE_ARRAY(value);
        }
        break;
      }
      case PdxFieldTypes::LONG_ARRAY: {
        int64_t* value = 0;
        int32_t length;
        getField(identityFields.at(i)->getFieldName(), &value, length);
        if (length > 0) {
          for (int i = 0; i < length; i++) {
            ACE_OS::snprintf(buf, 2048, "%lld\t", value[i]);
            toString += buf;
          }
        }
        break;
      }
      case PdxFieldTypes::FLOAT_ARRAY: {
        float* value = 0;
        int32_t length;
        getField(identityFields.at(i)->getFieldName(), &value, length);
        if (length > 0) {
          for (int i = 0; i < length; i++) {
            ACE_OS::snprintf(buf, 2048, "%f\t", value[i]);
            toString += buf;
          }
          GF_SAFE_DELETE_ARRAY(value);
        }
        break;
      }
      case PdxFieldTypes::DOUBLE_ARRAY: {
        double* value = 0;
        int32_t length;
        getField(identityFields.at(i)->getFieldName(), &value, length);
        if (length > 0) {
          for (int i = 0; i < length; i++) {
            ACE_OS::snprintf(buf, 2048, "%f\t", value[i]);
            toString += buf;
          }
        }
        break;
      }
      case PdxFieldTypes::DATE: {
        CacheableDatePtr value = NULLPTR;
        getField(identityFields.at(i)->getFieldName(), value);
        if (value != NULLPTR) {
          ACE_OS::snprintf(buf, 2048, "%s", value->toString()->asChar());
          toString += buf;
        }
        break;
      }
      case PdxFieldTypes::BOOLEAN_ARRAY: {
        bool* value = 0;
        int32_t length;
        getField(identityFields.at(i)->getFieldName(), &value, length);
        if (length > 0) {
          for (int i = 0; i < length; i++) {
            ACE_OS::snprintf(buf, 2048, "%s\t", value[i] ? "true" : "false");
            toString += buf;
          }
          GF_SAFE_DELETE_ARRAY(value);
        }
        break;
      }
      case PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS: {
        int8_t** value = 0;
        int32_t arrayLength;
        int32_t* elementLength;
        getField(identityFields.at(i)->getFieldName(), &value, arrayLength,
                 elementLength);
        if (arrayLength > 0) {
          for (int j = 0; j < arrayLength; j++) {
            for (int k = 0; k < elementLength[j]; k++) {
              ACE_OS::snprintf(buf, 2048, "%d\t", value[j][k]);
              toString += buf;
            }
          }
        }
        break;
      }
      case PdxFieldTypes::OBJECT_ARRAY: {
        CacheableObjectArrayPtr value;
        getField(identityFields.at(i)->getFieldName(), value);
        if (value != NULLPTR) {
          ACE_OS::snprintf(buf, 2048, "%s\t", value->toString()->asChar());
          toString += buf;
        }
        break;
      }
      default: {
        CacheablePtr value;
        getField(identityFields.at(i)->getFieldName(), value);
        if (value != NULLPTR) {
          ACE_OS::snprintf(buf, 2048, "%s\t", value->toString()->asChar());
          toString += buf;
        }
      }
    }
  }
  toString += "}";

  return CacheableString::create(toString.c_str());
}

PdxSerializablePtr PdxInstanceImpl::getObject() {
  DataInput dataInput(m_buffer, m_bufferLength);
  int64 sampleStartNanos = Utils::startStatOpTime();
  //[ToDo] do we have to call incPdxDeSerialization here?
  PdxSerializablePtr ret =
      PdxHelper::deserializePdx(dataInput, true, m_typeId, m_bufferLength);
  CachePtr cache = CacheFactory::getAnyInstance();
  if (cache == NULLPTR) {
    throw IllegalStateException("cache has not been created yet.");
    ;
  }
  if (cache->isClosed()) {
    throw IllegalStateException("cache has been closed. ");
  }
  CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cache.ptr());
  if (cacheImpl != NULL) {
    Utils::updateStatOpTime(
        cacheImpl->m_cacheStats->getStat(),
        cacheImpl->m_cacheStats->getPdxInstanceDeserializationTimeId(),
        sampleStartNanos);
    cacheImpl->m_cacheStats->incPdxInstanceDeserializations();
  }
  return ret;
}

void PdxInstanceImpl::equatePdxFields(
    std::vector<PdxFieldTypePtr>& my,
    std::vector<PdxFieldTypePtr>& other) const {
  int otherIdx = -1;
  for (int32_t i = 0; i < static_cast<int32_t>(my.size()); i++) {
    PdxFieldTypePtr myF = my.at(i);
    if (!myF->equals(m_DefaultPdxFieldType)) {
      for (int32_t j = 0; j < static_cast<int32_t>(other.size()); j++) {
        if (myF->equals(other[j])) {
          otherIdx = j;
          break;
        } else {
          otherIdx = -1;
        }
      }

      if (otherIdx == -1)  // field not there
      {
        if (i < static_cast<int32_t>(other.size())) {
          PdxFieldTypePtr tmp = other.at(i);
          other.at(i) = m_DefaultPdxFieldType;
          other.push_back(tmp);
        } else {
          other.push_back(m_DefaultPdxFieldType);
        }
      } else if (otherIdx != i) {
        PdxFieldTypePtr tmp = other.at(i);
        other.at(i) = other.at(otherIdx);
        other.at(otherIdx) = tmp;
      }
    }
  }
}

bool PdxInstanceImpl::operator==(const CacheableKey& other) const {
  CacheableKey& temp = const_cast<CacheableKey&>(other);
  PdxInstanceImpl* otherPdx = dynamic_cast<PdxInstanceImpl*>(&temp);

  if (otherPdx == NULL) {
    return false;
  }

  PdxTypePtr myPdxType = getPdxType();
  PdxTypePtr otherPdxType = otherPdx->getPdxType();

  char* myPdxClassName = myPdxType->getPdxClassName();
  char* otherPdxClassName = otherPdxType->getPdxClassName();

  if (ACE_OS::strcmp(otherPdxClassName, myPdxClassName) != 0) {
    return false;
  }

  std::vector<PdxFieldTypePtr> myPdxIdentityFieldList =
      getIdentityPdxFields(myPdxType);
  std::vector<PdxFieldTypePtr> otherPdxIdentityFieldList =
      otherPdx->getIdentityPdxFields(otherPdxType);

  equatePdxFields(myPdxIdentityFieldList, otherPdxIdentityFieldList);
  equatePdxFields(otherPdxIdentityFieldList, myPdxIdentityFieldList);

  DataInput myDataInput(m_buffer, m_bufferLength);
  DataInput otherDataInput(otherPdx->m_buffer, otherPdx->m_bufferLength);

  int fieldTypeId = -1;
  for (size_t i = 0; i < myPdxIdentityFieldList.size(); i++) {
    PdxFieldTypePtr myPFT = myPdxIdentityFieldList.at(i);
    PdxFieldTypePtr otherPFT = otherPdxIdentityFieldList.at(i);

    LOGDEBUG("pdxfield %s ",
             ((myPFT != m_DefaultPdxFieldType) ? myPFT->getFieldName()
                                               : otherPFT->getFieldName()));
    if (myPFT->equals(m_DefaultPdxFieldType)) {
      fieldTypeId = otherPFT->getTypeId();
    } else if (otherPFT->equals(m_DefaultPdxFieldType)) {
      fieldTypeId = myPFT->getTypeId();
    } else {
      fieldTypeId = myPFT->getTypeId();
    }

    switch (fieldTypeId) {
      case PdxFieldTypes::CHAR:
      case PdxFieldTypes::BOOLEAN:
      case PdxFieldTypes::BYTE:
      case PdxFieldTypes::SHORT:
      case PdxFieldTypes::INT:
      case PdxFieldTypes::LONG:
      case PdxFieldTypes::DATE:
      case PdxFieldTypes::FLOAT:
      case PdxFieldTypes::DOUBLE:
      case PdxFieldTypes::STRING:
      case PdxFieldTypes::BOOLEAN_ARRAY:
      case PdxFieldTypes::CHAR_ARRAY:
      case PdxFieldTypes::BYTE_ARRAY:
      case PdxFieldTypes::SHORT_ARRAY:
      case PdxFieldTypes::INT_ARRAY:
      case PdxFieldTypes::LONG_ARRAY:
      case PdxFieldTypes::FLOAT_ARRAY:
      case PdxFieldTypes::DOUBLE_ARRAY:
      case PdxFieldTypes::STRING_ARRAY:
      case PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS: {
        if (!compareRawBytes(*otherPdx, myPdxType, myPFT, myDataInput,
                             otherPdxType, otherPFT, otherDataInput)) {
          return false;
        }
        break;
      }
      case PdxFieldTypes::OBJECT: {
        CacheablePtr object = NULLPTR;
        CacheablePtr otherObject = NULLPTR;
        if (!myPFT->equals(m_DefaultPdxFieldType)) {
          setOffsetForObject(myDataInput, myPdxType, myPFT->getSequenceId());
          myDataInput.readObject(object);
        }

        if (!otherPFT->equals(m_DefaultPdxFieldType)) {
          otherPdx->setOffsetForObject(otherDataInput, otherPdxType,
                                       otherPFT->getSequenceId());
          otherDataInput.readObject(otherObject);
        }

        if (object != NULLPTR) {
          if (!deepArrayEquals(object, otherObject)) {
            return false;
          }
        } else if (otherObject != NULLPTR) {
          return false;
        }
        break;
      }
      case PdxFieldTypes::OBJECT_ARRAY: {
        CacheableObjectArrayPtr otherObjectArray =
            CacheableObjectArray::create();
        CacheableObjectArrayPtr objectArray = CacheableObjectArray::create();

        if (!myPFT->equals(m_DefaultPdxFieldType)) {
          setOffsetForObject(myDataInput, myPdxType, myPFT->getSequenceId());
          objectArray->fromData(myDataInput);
        }

        if (!otherPFT->equals(m_DefaultPdxFieldType)) {
          otherPdx->setOffsetForObject(otherDataInput, otherPdxType,
                                       otherPFT->getSequenceId());
          otherObjectArray->fromData(otherDataInput);
        }
        if (!deepArrayEquals(objectArray, otherObjectArray)) {
          return false;
        }
        break;
      }
      default: {
        char excpStr[256] = {0};
        /* adongre - Coverity II
        * CID 29267: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
        * "sprintf" can cause a
        * buffer overflow when done incorrectly. Because sprintf() assumes an
        * arbitrarily long string,
        * callers must be careful not to overflow the actual space of the
        * destination.
        * Use snprintf() instead, or correct precision specifiers.
        * Fix : using ACE_OS::snprintf
        */
        ACE_OS::snprintf(excpStr, 256, "PdxInstance not found typeid  %d ",
                         myPFT->getTypeId());
        throw IllegalStateException(excpStr);
      }
    }
  }
  return true;
}

bool PdxInstanceImpl::compareRawBytes(PdxInstanceImpl& other, PdxTypePtr myPT,
                                      PdxFieldTypePtr myF,
                                      DataInput& myDataInput,
                                      PdxTypePtr otherPT,
                                      PdxFieldTypePtr otherF,
                                      DataInput& otherDataInput) const {
  if (!myF->equals(m_DefaultPdxFieldType) &&
      !otherF->equals(m_DefaultPdxFieldType)) {
    int pos = getOffset(myDataInput, myPT, myF->getSequenceId());
    int nextpos =
        getNextFieldPosition(myDataInput, myF->getSequenceId() + 1, myPT);
    myDataInput.reset();
    myDataInput.advanceCursor(pos);

    int otherPos =
        other.getOffset(otherDataInput, otherPT, otherF->getSequenceId());
    int otherNextpos = other.getNextFieldPosition(
        otherDataInput, otherF->getSequenceId() + 1, otherPT);
    otherDataInput.reset();
    otherDataInput.advanceCursor(otherPos);

    if ((nextpos - pos) != (otherNextpos - otherPos)) {
      return false;
    }

    for (int i = pos; i < nextpos; i++) {
      int8_t myByte = 0;
      int8_t otherByte = 0;
      myDataInput.read(&myByte);
      otherDataInput.read(&otherByte);
      if (myByte != otherByte) {
        return false;
      }
    }
    return true;
  } else {
    if (myF->equals(m_DefaultPdxFieldType)) {
      int otherPos =
          other.getOffset(otherDataInput, otherPT, otherF->getSequenceId());
      int otherNextpos = other.getNextFieldPosition(
          otherDataInput, otherF->getSequenceId() + 1, otherPT);
      return hasDefaultBytes(otherF, otherDataInput, otherPos, otherNextpos);
    } else {
      int pos = getOffset(myDataInput, myPT, myF->getSequenceId());
      int nextpos =
          getNextFieldPosition(myDataInput, myF->getSequenceId() + 1, myPT);
      return hasDefaultBytes(myF, myDataInput, pos, nextpos);
    }
  }
}

CacheableStringArrayPtr PdxInstanceImpl::getFieldNames() {
  PdxTypePtr pt = getPdxType();
  std::vector<PdxFieldTypePtr>* vectorOfFieldTypes = pt->getPdxFieldTypes();
  int size = static_cast<int>(vectorOfFieldTypes->size());
  CacheableStringPtr* ptrArr = NULL;
  if (size > 0) {
    ptrArr = new CacheableStringPtr[size];
    for (int i = 0; i < size; i++) {
      ptrArr[i] =
          CacheableString::create((vectorOfFieldTypes->at(i))->getFieldName());
    }
  }

  if (size > 0) {
    return CacheableStringArray::createNoCopy(ptrArr, size);
  }
  return NULLPTR;
}

PdxFieldTypes::PdxFieldType PdxInstanceImpl::getFieldType(
    const char* fieldname) const {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldname);

  VERIFY_PDX_INSTANCE_FIELD_THROW;

  return static_cast<PdxFieldTypes::PdxFieldType>(pft->getTypeId());
}

void PdxInstanceImpl::writeUnmodifieldField(DataInput& dataInput, int startPos,
                                            int endPos,
                                            PdxLocalWriterPtr localWriter) {
  dataInput.reset(startPos);
  for (; startPos < endPos; startPos++) {
    uint8_t byte;
    dataInput.read(&byte);
    localWriter->writeByte(byte);
  }
}

void PdxInstanceImpl::toData(PdxWriterPtr writer) /*const*/ {
  PdxTypePtr pt = getPdxType();
  std::vector<PdxFieldTypePtr>* pdxFieldList = pt->getPdxFieldTypes();
  int position = 0;  // ignore typeid and length
  int nextFieldPosition = 0;
  if (m_buffer != NULL) {
    uint8_t* copy = gemfire::DataInput::getBufferCopy(m_buffer, m_bufferLength);
    DataInput dataInput(copy, m_bufferLength);  // this will delete buffer
    for (size_t i = 0; i < pdxFieldList->size(); i++) {
      PdxFieldTypePtr currPf = pdxFieldList->at(i);
      LOGDEBUG("toData filedname = %s , isVarLengthType = %d ",
               currPf->getFieldName(), currPf->IsVariableLengthType());
      CacheablePtr value = NULLPTR;

      FieldVsValues::iterator iter =
          m_updatedFields.find(currPf->getFieldName());
      if (iter != m_updatedFields.end()) {
        value = ((*iter).second);
      } else {
        value = NULLPTR;
      }
      if (value != NULLPTR) {
        writeField(writer, currPf->getFieldName(), currPf->getTypeId(), value);
        position = getNextFieldPosition(dataInput, static_cast<int>(i) + 1, pt);
      } else {
        if (currPf->IsVariableLengthType()) {
          // need to add offset
          (static_cast<PdxLocalWriterPtr>(writer))->addOffset();
        }
        // write raw byte array...
        nextFieldPosition =
            getNextFieldPosition(dataInput, static_cast<int>(i) + 1, pt);
        writeUnmodifieldField(dataInput, position, nextFieldPosition,
                              static_cast<PdxLocalWriterPtr>(writer));
        position = nextFieldPosition;  // mark next field;
      }
    }
    GF_SAFE_DELETE_ARRAY(copy);
  } else {
    for (size_t i = 0; i < pdxFieldList->size(); i++) {
      PdxFieldTypePtr currPf = pdxFieldList->at(i);
      LOGDEBUG("toData1 filedname = %s , isVarLengthType = %d ",
               currPf->getFieldName(), currPf->IsVariableLengthType());
      CacheablePtr value = m_updatedFields[currPf->getFieldName()];
      writeField(writer, currPf->getFieldName(), currPf->getTypeId(), value);
    }
  }
  m_updatedFields.clear();
}

void PdxInstanceImpl::fromData(PdxReaderPtr input) {
  throw IllegalStateException(
      "PdxInstance::FromData( .. ) shouldn't have called");
}

const char* PdxInstanceImpl::getClassName() const {
  if (m_typeId != 0) {
    PdxTypePtr pdxtype = PdxTypeRegistry::getPdxType(m_typeId);
    if (pdxtype == NULLPTR) {
      char excpStr[256] = {0};
      ACE_OS::snprintf(excpStr, 256,
                       "PdxType is not defined for PdxInstance: %d ", m_typeId);
      throw IllegalStateException(excpStr);
    }
    return pdxtype->getPdxClassName();
  }
  throw IllegalStateException(
      "PdxInstance typeid is not defined yet, to get classname.");
}

void PdxInstanceImpl::setPdxId(int32_t typeId) {
  if (m_typeId == 0) {
    m_typeId = typeId;
    m_pdxType = NULLPTR;
  } else {
    throw IllegalStateException("PdxInstance's typeId is already set.");
  }
}

std::vector<PdxFieldTypePtr> PdxInstanceImpl::getIdentityPdxFields(
    PdxTypePtr pt) const {
  std::vector<PdxFieldTypePtr>* pdxFieldList = pt->getPdxFieldTypes();
  std::vector<PdxFieldTypePtr> retList;
  int size = static_cast<int>(pdxFieldList->size());
  for (int i = 0; i < size; i++) {
    PdxFieldTypePtr pft = pdxFieldList->at(i);
    if (pft->getIdentityField()) retList.push_back(pft);
  }

  if (retList.size() > 0) {
    std::sort(retList.begin(), retList.end(), sortFunc);
    return retList;
  }

  for (int i = 0; i < size; i++) {
    PdxFieldTypePtr pft = pdxFieldList->at(i);
    retList.push_back(pft);
  }

  std::sort(retList.begin(), retList.end(), sortFunc);

  return retList;
}

int PdxInstanceImpl::getOffset(DataInput& dataInput, PdxTypePtr pt,
                               int sequenceId) const {
  dataInput.resetPdx(0);

  int offsetSize = 0;
  int serializedLength = 0;
  int pdxSerializedLength = dataInput.getPdxBytes();
  LOGDEBUG("getOffset pdxSerializedLength = %d ", pdxSerializedLength);
  if (pdxSerializedLength <= 0xff) {
    offsetSize = 1;
  } else if (pdxSerializedLength <= 0xffff) {
    offsetSize = 2;
  } else {
    offsetSize = 4;
  }

  if (pt->getNumberOfVarLenFields() > 0) {
    serializedLength = pdxSerializedLength -
                       ((pt->getNumberOfVarLenFields() - 1) * offsetSize);
  } else {
    serializedLength = pdxSerializedLength;
  }

  //[ToDo see if currentBufferPosition can correctly replace GetCursor]
  uint8_t* offsetsBuffer =
      const_cast<uint8_t*>(dataInput.currentBufferPosition()) +
      serializedLength;
  return pt->getFieldPosition(sequenceId, offsetsBuffer, offsetSize,
                              serializedLength);
}

int PdxInstanceImpl::getRawHashCode(PdxTypePtr pt, PdxFieldTypePtr pField,
                                    DataInput& dataInput) const {
  int pos = getOffset(dataInput, pt, pField->getSequenceId());
  int nextpos =
      getNextFieldPosition(dataInput, pField->getSequenceId() + 1, pt);

  LOGDEBUG("pos = %d nextpos = %d ", pos, nextpos);

  if (hasDefaultBytes(pField, dataInput, pos, nextpos)) {
    return 0;  // matched default bytes
  }

  dataInput.reset();
  dataInput.advanceCursor(nextpos - 1);

  int h = 1;
  int8_t byte = 0;
  for (int i = nextpos - 1; i >= pos; i--) {
    dataInput.read(&byte);
    h = 31 * h + static_cast<int>(byte);
    dataInput.reset();
    dataInput.advanceCursor(i - 1);
  }
  LOGDEBUG("getRawHashCode nbytes = %d, final hashcode = %d ", (nextpos - pos),
           h);
  return h;
}

int PdxInstanceImpl::getNextFieldPosition(DataInput& dataInput, int fieldId,
                                          PdxTypePtr pt) const {
  LOGDEBUG("fieldId = %d pt->getTotalFields() = %d ", fieldId,
           pt->getTotalFields());
  if (fieldId == pt->getTotalFields()) {
    // return serialized length
    return getSerializedLength(dataInput, pt);
  } else {
    return getOffset(dataInput, pt, fieldId);
  }
}

int PdxInstanceImpl::getSerializedLength(DataInput& dataInput,
                                         PdxTypePtr pt) const {
  dataInput.resetPdx(0);

  int offsetSize = 0;
  int serializedLength = 0;
  int pdxSerializedLength = dataInput.getPdxBytes();
  LOGDEBUG("pdxSerializedLength = %d ", pdxSerializedLength);
  if (pdxSerializedLength <= 0xff) {
    offsetSize = 1;
  } else if (pdxSerializedLength <= 0xffff) {
    offsetSize = 2;
  } else {
    offsetSize = 4;
  }

  if (pt->getNumberOfVarLenFields() > 0) {
    serializedLength = pdxSerializedLength -
                       ((pt->getNumberOfVarLenFields() - 1) * offsetSize);
  } else {
    serializedLength = pdxSerializedLength;
  }

  return serializedLength;
}

bool PdxInstanceImpl::compareDefaulBytes(DataInput& dataInput, int start,
                                         int end, int8_t* defaultBytes,
                                         int32_t length) const {
  if ((end - start) != length) return false;

  dataInput.reset();
  dataInput.advanceCursor(start);
  int j = 0;
  for (int i = start; i < end; i++) {
    int8_t byte;
    dataInput.read(&byte);
    if (defaultBytes[j++] != byte) {
      return false;
    }
  }
  return true;
}

bool PdxInstanceImpl::hasDefaultBytes(PdxFieldTypePtr pField,
                                      DataInput& dataInput, int start,
                                      int end) const {
  switch (pField->getTypeId()) {
    case PdxFieldTypes::INT: {
      return compareDefaulBytes(dataInput, start, end, m_IntDefaultBytes, 4);
    }
    case PdxFieldTypes::STRING: {
      return compareDefaulBytes(dataInput, start, end, m_StringDefaultBytes, 1);
    }
    case PdxFieldTypes::BOOLEAN: {
      return compareDefaulBytes(dataInput, start, end, m_BooleanDefaultBytes,
                                1);
    }
    case PdxFieldTypes::FLOAT: {
      return compareDefaulBytes(dataInput, start, end, m_FloatDefaultBytes, 4);
    }
    case PdxFieldTypes::DOUBLE: {
      return compareDefaulBytes(dataInput, start, end, m_DoubleDefaultBytes, 8);
    }
    case PdxFieldTypes::CHAR: {
      return compareDefaulBytes(dataInput, start, end, m_CharDefaultBytes, 2);
    }
    case PdxFieldTypes::BYTE: {
      return compareDefaulBytes(dataInput, start, end, m_ByteDefaultBytes, 1);
    }
    case PdxFieldTypes::SHORT: {
      return compareDefaulBytes(dataInput, start, end, m_ShortDefaultBytes, 2);
    }
    case PdxFieldTypes::LONG: {
      return compareDefaulBytes(dataInput, start, end, m_LongDefaultBytes, 8);
    }
    case PdxFieldTypes::BYTE_ARRAY:
    case PdxFieldTypes::DOUBLE_ARRAY:
    case PdxFieldTypes::FLOAT_ARRAY:
    case PdxFieldTypes::SHORT_ARRAY:
    case PdxFieldTypes::INT_ARRAY:
    case PdxFieldTypes::LONG_ARRAY:
    case PdxFieldTypes::BOOLEAN_ARRAY:
    case PdxFieldTypes::CHAR_ARRAY:
    case PdxFieldTypes::STRING_ARRAY:
    case PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS:
    case PdxFieldTypes::OBJECT_ARRAY: {
      return compareDefaulBytes(dataInput, start, end, m_NULLARRAYDefaultBytes,
                                1);
    }
    case PdxFieldTypes::DATE: {
      return compareDefaulBytes(dataInput, start, end, m_DateDefaultBytes, 8);
    }
    case PdxFieldTypes::OBJECT: {
      return compareDefaulBytes(dataInput, start, end, m_ObjectDefaultBytes, 1);
    }
    default: {
      throw IllegalStateException("hasDefaultBytes unable to find typeID ");
    }
  }
}

void PdxInstanceImpl::setField(const char* fieldName, bool value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::BOOLEAN) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29233: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableBoolean::create(value);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, signed char value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::BYTE) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29233: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableByte::create(value);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, unsigned char value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::BYTE) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29236: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableByte::create(value);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, int16_t value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::SHORT) {
    char excpStr[256] = {0};
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableInt16::create(value);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, int32_t value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::INT) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29234: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableInt32::create(value);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, int64_t value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::LONG) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29235: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableInt64::create(value);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, float value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::FLOAT) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29232: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableFloat::create(value);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, double value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::DOUBLE) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29231: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableDouble::create(value);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, wchar_t value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::CHAR) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29237: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableWideChar::create(value);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, char value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::CHAR) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29230: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  wchar_t tempWideChar = static_cast<wchar_t>(value);
  CacheablePtr cacheableObject = CacheableWideChar::create(tempWideChar);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, CacheableDatePtr value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::DATE) {
    char excpStr[256] = {0};
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = value;
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, CacheablePtr value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::OBJECT) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29212: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  m_updatedFields[fieldName] = value;
}

void PdxInstanceImpl::setField(const char* fieldName,
                               CacheableObjectArrayPtr value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::OBJECT_ARRAY) {
    char excpStr[256] = {0};
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  m_updatedFields[fieldName] = value;
}

void PdxInstanceImpl::setField(const char* fieldName, bool* value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::BOOLEAN_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29218: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = BooleanArray::create(value, length);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, signed char* value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::BYTE_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29217: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject =
      CacheableBytes::create(reinterpret_cast<uint8_t*>(value), length);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, unsigned char* value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::BYTE_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29222: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject =
      CacheableBytes::create((uint8_t*)value, length);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, int16_t* value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::SHORT_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29225: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableInt16Array::create(value, length);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, int32_t* value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::INT_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29223: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableInt32Array::create(value, length);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, int64_t* value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::LONG_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29224: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableInt64Array::create(value, length);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, float* value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::FLOAT_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29221: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableFloatArray::create(value, length);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, double* value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::DOUBLE_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29220: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr cacheableObject = CacheableDoubleArray::create(value, length);
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, wchar_t* value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::CHAR_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29226: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr ptr = CharArray::create(value, length);
  m_updatedFields[fieldName] = ptr;
}

void PdxInstanceImpl::setField(const char* fieldName, char* value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::CHAR_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29219: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  size_t size = strlen(value) + 1;
  wchar_t* tempWideCharArray = new wchar_t[size];
  mbstowcs(tempWideCharArray, value, size);
  CacheablePtr ptr = CharArray::create(tempWideCharArray, length);
  m_updatedFields[fieldName] = ptr;
  delete[] tempWideCharArray;
}

void PdxInstanceImpl::setField(const char* fieldName, const wchar_t* value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::STRING) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29213: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr ptr = CacheableString::create(value);
  m_updatedFields[fieldName] = ptr;
}

void PdxInstanceImpl::setField(const char* fieldName, const char* value) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::STRING) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29227: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheablePtr ptr = CacheableString::create(value);
  m_updatedFields[fieldName] = ptr;
}

void PdxInstanceImpl::setField(const char* fieldName, int8_t** value,
                               int32_t arrayLength, int32_t* elementLength) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR &&
      pft->getTypeId() != PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29214: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheableVectorPtr cacheableObject = CacheableVector::create();
  for (int i = 0; i < arrayLength; i++) {
    CacheableBytesPtr ptr = CacheableBytes::create(
        reinterpret_cast<uint8_t*>(value[i]), elementLength[i]);
    cacheableObject->push_back(ptr);
  }
  m_updatedFields[fieldName] = cacheableObject;
}

void PdxInstanceImpl::setField(const char* fieldName, wchar_t** value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::STRING_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29216: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheableStringPtr* ptrArr = NULL;
  if (length > 0) {
    ptrArr = new CacheableStringPtr[length];
    for (int32_t i = 0; i < length; ++i) {
      ptrArr[i] = CacheableString::create(value[i]);
    }
  }
  if (length > 0) {
    CacheablePtr cacheableObject = CacheableStringArray::create(ptrArr, length);
    m_updatedFields[fieldName] = cacheableObject;
  }

  /* adongre - Coverity II
  * CID 29202: Resource leak (RESOURCE_LEAK)
  */
  /*if ( ptrArr ) {
    delete [] ptrArr ;
  }*/
}

void PdxInstanceImpl::setField(const char* fieldName, char** value,
                               int32_t length) {
  PdxTypePtr pt = getPdxType();
  PdxFieldTypePtr pft = pt->getPdxField(fieldName);

  if (pft != NULLPTR && pft->getTypeId() != PdxFieldTypes::STRING_ARRAY) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
    * CID 29215: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "PdxInstance doesn't has field %s or type of field not matched %s ",
        fieldName, (pft != NULLPTR ? pft->toString()->asChar() : ""));
    throw IllegalStateException(excpStr);
  }
  CacheableStringPtr* ptrArr = NULL;
  if (length > 0) {
    ptrArr = new CacheableStringPtr[length];
    for (int32_t i = 0; i < length; ++i) {
      ptrArr[i] = CacheableString::create(value[i]);
    }
  }
  if (length > 0) {
    CacheablePtr cacheableObject = CacheableStringArray::create(ptrArr, length);
    m_updatedFields[fieldName] = cacheableObject;
  }
  /* adongre - Coverity II
  * CID 29201: Resource leak (RESOURCE_LEAK)
  */
  /*if ( ptrArr ) {
    delete [] ptrArr;
  }*/
}

void PdxInstanceImpl::setOffsetForObject(DataInput& dataInput, PdxTypePtr pt,
                                         int sequenceId) const {
  int pos = getOffset(dataInput, pt, sequenceId);
  dataInput.reset();
  dataInput.advanceCursor(pos);
}

uint32_t PdxInstanceImpl::objectSize() const {
  uint32_t size = sizeof(PdxInstanceImpl);
  size += m_bufferLength;
  size += m_pdxType->objectSize();
  for (FieldVsValues::const_iterator iter = m_updatedFields.begin();
       iter != m_updatedFields.end(); ++iter) {
    size += static_cast<int32_t>(iter->first.length());
    size += iter->second->objectSize();
  }
  return size;
}
}  // namespace gemfire
