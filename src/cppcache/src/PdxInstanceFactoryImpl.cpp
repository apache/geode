/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "PdxInstanceFactoryImpl.hpp"
#include "PdxType.hpp"
#include "PdxTypes.hpp"
#include "PdxInstanceImpl.hpp"
#include <ace/OS_NS_stdio.h>

namespace gemfire {

PdxInstanceFactoryImpl::~PdxInstanceFactoryImpl() {}

PdxInstanceFactoryImpl::PdxInstanceFactoryImpl(const char* className) {
  if (className == NULL ||
      *className == '\0') {  // COVERITY ---> 30289 Same on both sides
    throw IllegalStateException("className should not be null.");
  }
  m_pdxType = new PdxType(className, false);
  m_created = false;
}

PdxInstancePtr PdxInstanceFactoryImpl::create() {
  if (m_created) {
    throw IllegalStateException(
        "The PdxInstanceFactory.Create() method can only be called once.");
  }
  PdxInstancePtr pi(new PdxInstanceImpl(m_FieldVsValues, m_pdxType));
  m_created = true;
  return pi;
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeWideChar(
    const char* fieldName, wchar_t value) {
  isFieldAdded(fieldName);
  m_pdxType->addFixedLengthTypeField(fieldName, "char", PdxFieldTypes::CHAR,
                                     PdxTypes::CHAR_SIZE);
  CacheablePtr cacheableObject = CacheableWideChar::create(value);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeChar(const char* fieldName,
                                                        char value) {
  isFieldAdded(fieldName);
  wchar_t tempWideChar = static_cast<wchar_t>(value);
  m_pdxType->addFixedLengthTypeField(fieldName, "char", PdxFieldTypes::CHAR,
                                     PdxTypes::CHAR_SIZE);
  CacheablePtr cacheableObject = CacheableWideChar::create(tempWideChar);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeBoolean(
    const char* fieldName, bool value) {
  isFieldAdded(fieldName);
  m_pdxType->addFixedLengthTypeField(fieldName, "bool", PdxFieldTypes::BOOLEAN,
                                     PdxTypes::BOOLEAN_SIZE);
  CacheablePtr cacheableObject = CacheableBoolean::create(value);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeByte(const char* fieldName,
                                                        int8_t value) {
  isFieldAdded(fieldName);
  m_pdxType->addFixedLengthTypeField(fieldName, "byte", PdxFieldTypes::BYTE,
                                     PdxTypes::BYTE_SIZE);
  CacheablePtr cacheableObject = CacheableByte::create(value);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeShort(const char* fieldName,
                                                         int16_t value) {
  isFieldAdded(fieldName);
  m_pdxType->addFixedLengthTypeField(fieldName, "short", PdxFieldTypes::SHORT,
                                     PdxTypes::SHORT_SIZE);
  CacheablePtr cacheableObject = CacheableInt16::create(value);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeInt(const char* fieldName,
                                                       int32_t value) {
  isFieldAdded(fieldName);
  m_pdxType->addFixedLengthTypeField(fieldName, "int", PdxFieldTypes::INT,
                                     PdxTypes::INTEGER_SIZE);
  CacheablePtr cacheableObject = CacheableInt32::create(value);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeLong(const char* fieldName,
                                                        int64_t value) {
  isFieldAdded(fieldName);
  m_pdxType->addFixedLengthTypeField(fieldName, "long", PdxFieldTypes::LONG,
                                     PdxTypes::LONG_SIZE);
  CacheablePtr cacheableObject = CacheableInt64::create(value);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeFloat(const char* fieldName,
                                                         float value) {
  isFieldAdded(fieldName);
  m_pdxType->addFixedLengthTypeField(fieldName, "float", PdxFieldTypes::FLOAT,
                                     PdxTypes::FLOAT_SIZE);
  CacheablePtr cacheableObject = CacheableFloat::create(value);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeDouble(const char* fieldName,
                                                          double value) {
  isFieldAdded(fieldName);
  m_pdxType->addFixedLengthTypeField(fieldName, "double", PdxFieldTypes::DOUBLE,
                                     PdxTypes::DOUBLE_SIZE);
  CacheablePtr cacheableObject = CacheableDouble::create(value);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeWideString(
    const char* fieldName, const wchar_t* value) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "string",
                                        PdxFieldTypes::STRING);
  CacheablePtr cacheableObject = CacheableString::create(value);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeString(const char* fieldName,
                                                          const char* value) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "string",
                                        PdxFieldTypes::STRING);
  CacheablePtr cacheableObject = CacheableString::create(value);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeObject(const char* fieldName,
                                                          CacheablePtr value) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "Object",
                                        PdxFieldTypes::OBJECT);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, value));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeObjectArray(
    const char* fieldName, CacheableObjectArrayPtr value) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "Object[]",
                                        PdxFieldTypes::OBJECT_ARRAY);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, value));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeBooleanArray(
    const char* fieldName, bool* value, int32_t length) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "bool[]",
                                        PdxFieldTypes::BOOLEAN_ARRAY);
  CacheablePtr cacheableObject = BooleanArray::create(value, length);
  m_FieldVsValues.insert(
      std::pair<std::string, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeWideCharArray(
    const char* fieldName, wchar_t* value, int32_t length) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "char[]",
                                        PdxFieldTypes::CHAR_ARRAY);
  CacheablePtr cacheableObject = CharArray::create(value, length);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeCharArray(
    const char* fieldName, char* value, int32_t length) {
  isFieldAdded(fieldName);
  size_t size = strlen(value) + 1;
  wchar_t* tempWideCharArray = new wchar_t[size];
  mbstowcs(tempWideCharArray, value, size);
  m_pdxType->addVariableLengthTypeField(fieldName, "char[]",
                                        PdxFieldTypes::CHAR_ARRAY);
  CacheablePtr cacheableObject = CharArray::create(tempWideCharArray, length);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  delete[] tempWideCharArray;
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeByteArray(
    const char* fieldName, int8_t* value, int32_t length) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "byte[]",
                                        PdxFieldTypes::BYTE_ARRAY);
  CacheablePtr cacheableObject =
      CacheableBytes::create(reinterpret_cast<uint8_t*>(value), length);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeShortArray(
    const char* fieldName, int16_t* value, int32_t length) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "short[]",
                                        PdxFieldTypes::SHORT_ARRAY);
  CacheablePtr cacheableObject = CacheableInt16Array::create(value, length);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeIntArray(
    const char* fieldName, int32_t* value, int32_t length) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "int32[]",
                                        PdxFieldTypes::INT_ARRAY);
  CacheablePtr cacheableObject = CacheableInt32Array::create(value, length);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeLongArray(
    const char* fieldName, int64_t* value, int32_t length) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "int64[]",
                                        PdxFieldTypes::LONG_ARRAY);
  CacheablePtr cacheableObject = CacheableInt64Array::create(value, length);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeFloatArray(
    const char* fieldName, float* value, int32_t length) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "float[]",
                                        PdxFieldTypes::FLOAT_ARRAY);
  CacheablePtr cacheableObject = CacheableFloatArray::create(value, length);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeDoubleArray(
    const char* fieldName, double* value, int32_t length) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "double[]",
                                        PdxFieldTypes::DOUBLE_ARRAY);
  CacheablePtr cacheableObject = CacheableDoubleArray::create(value, length);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeStringArray(
    const char* fieldName, char** value, int32_t length) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "string[]",
                                        PdxFieldTypes::STRING_ARRAY);
  CacheableStringPtr* ptrArr = NULL;
  if (length > 0) {
    ptrArr = new CacheableStringPtr[length];
    for (int32_t i = 0; i < length; i++) {
      ptrArr[i] = CacheableString::create(value[i]);
    }
  }
  if (length > 0) {
    CacheablePtr cacheableObject = CacheableStringArray::create(ptrArr, length);
    m_FieldVsValues.insert(
        std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  }
  /* adongre  - Coverity II
   * CID 29199: Resource leak (RESOURCE_LEAK)
   */
  if (ptrArr) {
    delete[] ptrArr;
  }
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeWideStringArray(
    const char* fieldName, wchar_t** value, int32_t length) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "string[]",
                                        PdxFieldTypes::STRING_ARRAY);
  CacheableStringPtr* ptrArr = NULL;
  if (length > 0) {
    ptrArr = new CacheableStringPtr[length];
    for (int32_t i = 0; i < length; i++) {
      ptrArr[i] = CacheableString::create(value[i]);
    }
  }
  if (length > 0) {
    CacheablePtr cacheableObject = CacheableStringArray::create(ptrArr, length);
    m_FieldVsValues.insert(
        std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  }
  /* adongre - Coverity II
   * CID 29200: Resource leak (RESOURCE_LEAK)
   */
  if (ptrArr) {
    delete[] ptrArr;
  }
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeDate(
    const char* fieldName, CacheableDatePtr value) {
  isFieldAdded(fieldName);
  m_pdxType->addFixedLengthTypeField(fieldName, "Date", PdxFieldTypes::DATE,
                                     PdxTypes::DATE_SIZE /*+ 1*/);
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, value));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::writeArrayOfByteArrays(
    const char* fieldName, int8_t** value, int32_t arrayLength,
    int32_t* elementLength) {
  isFieldAdded(fieldName);
  m_pdxType->addVariableLengthTypeField(fieldName, "byte[][]",
                                        PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS);
  CacheableVectorPtr cacheableObject = CacheableVector::create();
  for (int i = 0; i < arrayLength; i++) {
    CacheableBytesPtr ptr = CacheableBytes::create(
        reinterpret_cast<uint8_t*>(value[i]), elementLength[i]);
    cacheableObject->push_back(ptr);
  }
  m_FieldVsValues.insert(
      std::pair<const char*, CacheablePtr>(fieldName, cacheableObject));
  return PdxInstanceFactoryPtr(this);
}

PdxInstanceFactoryPtr PdxInstanceFactoryImpl::markIdentityField(
    const char* fieldName) {
  PdxFieldTypePtr pfType = m_pdxType->getPdxField(fieldName);
  if (pfType == NULLPTR) {
    throw IllegalStateException(
        "Field must be added before calling MarkIdentityField ");
  }
  pfType->setIdentityField(true);
  return PdxInstanceFactoryPtr(this);
}

void PdxInstanceFactoryImpl::isFieldAdded(const char* fieldName) {
  if (fieldName == NULL ||
      /**fieldName == '\0' ||*/ m_FieldVsValues.find(fieldName) !=
          m_FieldVsValues.end()) {
    char excpStr[256] = {0};
    /* adongre - Coverity II
     * CID 29209: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
     * "sprintf" can cause a
     * buffer overflow when done incorrectly. Because sprintf() assumes an
     * arbitrarily long string,
     * callers must be careful not to overflow the actual space of the
     * destination.
     * Use snprintf() instead, or correct precision specifiers.
     * Fix : using ACE_OS::snprintf
     */
    ACE_OS::snprintf(excpStr, 256,
                     "Field: %s is either already added into "
                     "PdxInstanceFactory or it is null ",
                     fieldName);
    throw IllegalStateException(excpStr);
  }
}
}  // namespace gemfire
