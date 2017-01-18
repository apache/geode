/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "StructSetImpl.hpp"

/**
 * @file
 */

using namespace apache::geode::client;

StructSetImpl::StructSetImpl(
    const CacheableVectorPtr& response,
    const std::vector<CacheableStringPtr>& fieldNames) {
  m_nextIndex = 0;

  size_t numOfFields = fieldNames.size();

  for (size_t i = 0; i < numOfFields; i++) {
    LOGDEBUG("StructSetImpl: pushing fieldName = %s with index = %d",
             fieldNames[i]->asChar(), i);
    m_fieldNameIndexMap.insert(
        std::make_pair(fieldNames[i]->asChar(), static_cast<int32_t>(i)));
  }

  int32_t numOfValues = response->size();
  int32_t valStoredCnt = 0;

  // LOGDEBUG("FieldNames = %d and Values = %d", numOfFields, numOfValues);
  m_structVector = CacheableVector::create();
  while (valStoredCnt < numOfValues) {
    VectorT<SerializablePtr> tmpVec;
    for (size_t i = 0; i < numOfFields; i++) {
      tmpVec.push_back(response->operator[](valStoredCnt++));
    }
    StructPtr siPtr(new Struct(this, tmpVec));
    m_structVector->push_back(siPtr);
  }
}

bool StructSetImpl::isModifiable() const { return false; }

int32_t StructSetImpl::size() const { return m_structVector->size(); }

const SerializablePtr StructSetImpl::operator[](int32_t index) const {
  if (index >= m_structVector->size()) {
    throw IllegalArgumentException("Index out of bounds");
  }

  return m_structVector->operator[](index);
}

SelectResultsIterator StructSetImpl::getIterator() {
  return SelectResultsIterator(m_structVector, SelectResultsPtr(this));
}

int32_t StructSetImpl::getFieldIndex(const char* fieldname) {
  std::map<std::string, int32_t>::iterator iter =
      m_fieldNameIndexMap.find(fieldname);
  if (iter != m_fieldNameIndexMap.end()) {
    return iter->second;
  } else {
    // std::string tmp = "fieldname ";
    // tmp += fieldname + " not found";
    throw IllegalArgumentException("fieldname not found");
  }
}

const char* StructSetImpl::getFieldName(int32_t index) {
  for (std::map<std::string, int32_t>::iterator iter =
           m_fieldNameIndexMap.begin();
       iter != m_fieldNameIndexMap.end(); ++iter) {
    if (iter->second == index) return iter->first.c_str();
  }
  return NULL;
}

SelectResults::Iterator StructSetImpl::begin() const {
  return m_structVector->begin();
}

SelectResults::Iterator StructSetImpl::end() const {
  return m_structVector->end();
}

StructSetImpl::~StructSetImpl() {}
