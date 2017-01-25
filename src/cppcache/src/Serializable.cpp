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
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Serializable.hpp>
#include <GeodeTypeIdsImpl.hpp>
#include <SerializationRegistry.hpp>
#include <Utils.hpp>
#include <gfcpp/CacheableString.hpp>

namespace apache {
namespace geode {
namespace client {

int8_t Serializable::typeId() const {
  int32_t classIdToCheck = classId();
  if (classIdToCheck <= 127 && classIdToCheck >= -128) {
    return static_cast<int8_t>(GeodeTypeIdsImpl::CacheableUserData);
  } else if (classIdToCheck <= 32767 && classIdToCheck >= -32768) {
    return static_cast<int8_t>(GeodeTypeIdsImpl::CacheableUserData2);
  } else {
    return static_cast<int8_t>(GeodeTypeIdsImpl::CacheableUserData4);
  }
}

int8_t Serializable::DSFID() const {
  return static_cast<int8_t>(GeodeTypeIdsImpl::FixedIDDefault);
}

uint32_t Serializable::objectSize() const { return 0; }

void Serializable::registerType(TypeFactoryMethod creationFunction) {
  SerializationRegistry::addType(creationFunction);
}

void Serializable::registerPdxType(TypeFactoryMethodPdx creationFunction) {
  SerializationRegistry::addPdxType(creationFunction);
}

void Serializable::registerPdxSerializer(PdxSerializerPtr pdxSerializer) {
  SerializationRegistry::setPdxSerializer(pdxSerializer);
}

CacheableStringPtr Serializable::toString() const {
  return Utils::demangleTypeName(typeid(*this).name());
}
}  // namespace client
}  // namespace geode
}  // namespace apache
