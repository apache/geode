#pragma once

#ifndef GEODE_INTEGRATION_TEST_CACHEABLEWRAPPER_H_
#define GEODE_INTEGRATION_TEST_CACHEABLEWRAPPER_H_

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

#include <gfcpp/GeodeCppCache.hpp>
#include <string>
#include <map>
#include <vector>

using namespace apache::geode::client;

class CacheableWrapper {
 protected:
  CacheablePtr m_cacheableObject;

 public:
  CacheableWrapper(const CacheablePtr cacheableObject)
      : m_cacheableObject(cacheableObject) {}

  virtual CacheablePtr getCacheable() const { return m_cacheableObject; }

  virtual int maxKeys() const {
    throw IllegalArgumentException("Cannot call maxKeys.");
  }

  virtual void initKey(int32_t keyIndex, int32_t maxSize) {
    throw IllegalArgumentException("Cannot call initKey.");
  }

  virtual void initRandomValue(int maxSize) = 0;

  virtual uint32_t getCheckSum(const CacheablePtr object) const = 0;

  uint32_t getCheckSum() const { return getCheckSum(m_cacheableObject); }

  virtual ~CacheableWrapper() {}
};

typedef CacheableWrapper* (*CacheableWrapperFunc)(void);

class CacheableWrapperFactory {
 public:
  static CacheableWrapper* createInstance(int8_t typeId);

  static void registerType(int8_t typeId, const std::string wrapperType,
                           const CacheableWrapperFunc wrapperFunc,
                           const bool isKey);

  static std::vector<int8_t> getRegisteredKeyTypes();

  static std::vector<int8_t> getRegisteredValueTypes();

  static std::string getTypeForId(int8_t typeId);

 private:
  static std::map<int8_t, CacheableWrapperFunc> m_registeredKeyMap;
  static std::map<int8_t, CacheableWrapperFunc> m_registeredValueMap;
  static std::map<int8_t, std::string> m_typeIdNameMap;
};

std::map<int8_t, CacheableWrapperFunc>
    CacheableWrapperFactory::m_registeredKeyMap;
std::map<int8_t, CacheableWrapperFunc>
    CacheableWrapperFactory::m_registeredValueMap;
std::map<int8_t, std::string> CacheableWrapperFactory::m_typeIdNameMap;

CacheableWrapper* CacheableWrapperFactory::createInstance(int8_t typeId) {
  if (m_registeredValueMap.find(typeId) != m_registeredValueMap.end()) {
    CacheableWrapperFunc wrapperFunc = m_registeredValueMap[typeId];
    return wrapperFunc();
  }
  return NULL;
}

void CacheableWrapperFactory::registerType(
    int8_t typeId, const std::string wrapperType,
    const CacheableWrapperFunc factoryFunc, const bool isKey) {
  if (isKey) {
    m_registeredKeyMap[typeId] = factoryFunc;
  }
  m_registeredValueMap[typeId] = factoryFunc;
  m_typeIdNameMap[typeId] = wrapperType;
}

std::vector<int8_t> CacheableWrapperFactory::getRegisteredKeyTypes() {
  std::vector<int8_t> keyVector;
  std::map<int8_t, CacheableWrapperFunc>::iterator keyMapIterator;

  for (keyMapIterator = m_registeredKeyMap.begin();
       keyMapIterator != m_registeredKeyMap.end(); ++keyMapIterator) {
    keyVector.push_back(keyMapIterator->first);
  }
  return keyVector;
}

std::vector<int8_t> CacheableWrapperFactory::getRegisteredValueTypes() {
  std::vector<int8_t> valueVector;
  std::map<int8_t, CacheableWrapperFunc>::iterator valueMapIterator;

  for (valueMapIterator = m_registeredValueMap.begin();
       valueMapIterator != m_registeredValueMap.end(); ++valueMapIterator) {
    valueVector.push_back(valueMapIterator->first);
  }
  return valueVector;
}

std::string CacheableWrapperFactory::getTypeForId(int8_t typeId) {
  std::map<int8_t, std::string>::iterator findType =
      m_typeIdNameMap.find(typeId);
  if (findType != m_typeIdNameMap.end()) {
    return findType->second;
  } else {
    return "";
  }
}


#endif // GEODE_INTEGRATION_TEST_CACHEABLEWRAPPER_H_
