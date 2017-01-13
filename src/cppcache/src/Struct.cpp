/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/Struct.hpp>
#include <GemfireTypeIdsImpl.hpp>
#include <gfcpp/DataInput.hpp>

namespace gemfire {

Struct::Struct() : m_parent(NULL), m_lastAccessIndex(0) {}

Struct::Struct(StructSet* ssPtr, VectorT<SerializablePtr>& fieldValues) {
  m_parent = ssPtr;
  int32_t vecsize = fieldValues.size();
  for (int32_t i = 0; i < vecsize; i++) {
    m_fieldValues.push_back(fieldValues[i]);
  }
  m_lastAccessIndex = 0;
}

void Struct::skipClassName(DataInput& input) {
  uint8_t classByte;
  input.read(&classByte);
  if (classByte == GemfireTypeIdsImpl::Class) {
    uint8_t stringType;
    input.read(&stringType);  // ignore string type id - assuming its a normal
                              // (under 64k) string.
    uint16_t len;
    input.readInt(&len);
    input.advanceCursor(len);
  } else {
    throw IllegalStateException(
        "Struct: Did not get expected class header byte");
  }
}

int32_t Struct::classId() const { return 0; }

int8_t Struct::typeId() const { return GemfireTypeIds::Struct; }

int8_t Struct::DSFID() const { return GemfireTypeIdsImpl::FixedIDByte; }

void Struct::toData(DataOutput& output) const {
  throw UnsupportedOperationException("Struct::toData: should not be called.");
}

int32_t Struct::length() const { return m_fieldValues.size(); }

Serializable* Struct::fromData(DataInput& input) {
  int8_t classType;
  input.read(&classType);
  input.read(&classType);
  skipClassName(input);

  int32_t numOfFields;
  input.readArrayLen(&numOfFields);

  m_parent = NULL;
  for (int i = 0; i < numOfFields; i++) {
    CacheableStringPtr fieldName;
    // input.readObject(fieldName);
    input.readNativeString(fieldName);
    m_fieldNames.insert(fieldName, CacheableInt32::create(i));
  }
  int32_t lengthForTypes;
  input.readArrayLen(&lengthForTypes);
  skipClassName(input);
  for (int i = 0; i < lengthForTypes; i++) {
    input.read(&classType);
    input.read(&classType);
    skipClassName(input);
  }
  int32_t numOfSerializedValues;
  input.readArrayLen(&numOfSerializedValues);
  skipClassName(input);
  for (int i = 0; i < numOfSerializedValues; i++) {
    SerializablePtr val;
    input.readObject(val);  // need to look
    m_fieldValues.push_back(val);
  }
  return this;
}

const char* Struct::getFieldName(int32_t index) {
  if (m_parent != NULL) {
    return m_parent->getFieldName(index);
  } else {
    for (HashMapT<CacheableStringPtr, CacheableInt32Ptr>::Iterator iter =
             m_fieldNames.begin();
         iter != m_fieldNames.end(); ++iter) {
      if (*(iter.second()) == index) return iter.first()->asChar();
    }
  }

  return NULL;
}

const SerializablePtr Struct::operator[](int32_t index) const {
  if (index >= m_fieldValues.size()) {
    return NULLPTR;
  }

  return m_fieldValues[index];
}

const SerializablePtr Struct::operator[](const char* fieldName) const {
  int32_t index;
  if (m_parent == NULL) {
    CacheableStringPtr fName = CacheableString::create(fieldName);
    HashMapT<CacheableStringPtr, CacheableInt32Ptr>::Iterator iter =
        m_fieldNames.find(fName);
    if (iter == m_fieldNames.end()) {
      throw OutOfRangeException("Struct: fieldName not found.");
    }
    index = iter.second()->value();
  } else {
    index = m_parent->getFieldIndex(fieldName);
  }
  return m_fieldValues[index];
}

const StructSetPtr Struct::getStructSet() const {
  return StructSetPtr(m_parent);
}

bool Struct::hasNext() const {
  if (m_lastAccessIndex + 1 <= m_fieldValues.size()) {
    return true;
  }
  return false;
}

const SerializablePtr Struct::next() {
  m_lastAccessIndex++;
  return m_fieldValues[m_lastAccessIndex - 1];
}

Serializable* Struct::createDeserializable() { return new Struct(); }

}  // end namespace gemfire
