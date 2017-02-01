#pragma once

#ifndef GEODE_CACHEDDESERIALIZABLEHELPER_H_
#define GEODE_CACHEDDESERIALIZABLEHELPER_H_

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

#include <gfcpp/Cacheable.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "GeodeTypeIdsImpl.hpp"

namespace apache {
namespace geode {
namespace client {
/**
 * Helper class to deserialize bytes received from GFE in the form of
 * PREFER_BYTES_CACHED_DESERIALIZABLE = -65, or
 * VM_CACHED_DESERIALIZABLE = -64.
 *
 *
 */
class CachedDeserializableHelper : public Cacheable,
                                   private NonCopyable,
                                   private NonAssignable {
 private:
  int8_t m_typeId;
  SerializablePtr m_intermediate;

  // default constructor disabled
  CachedDeserializableHelper();
  CachedDeserializableHelper(int8_t typeId) : m_typeId(typeId) {}

 public:
  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const {
    throw IllegalStateException(
        "CachedDeserializableHelper::toData should never have been called.");
  }

  /**
   *@brief deserialize this object
   **/
  virtual Serializable* fromData(DataInput& input) {
    int32_t arrayLen;
    input.readArrayLen(&arrayLen);
    input.readObject(m_intermediate);
    return m_intermediate.ptr();
  }

  /**
   * @brief creation function
   */
  inline static Serializable* createForPreferBytesDeserializable() {
    return new CachedDeserializableHelper(
        GeodeTypeIdsImpl::PreferBytesCachedDeserializable);
  }

  /**
   * @brief creation function
   */
  inline static Serializable* createForVmCachedDeserializable() {
    return new CachedDeserializableHelper(
        GeodeTypeIdsImpl::VmCachedDeserializable);
  }

  /**
   *@brief return the typeId byte of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and derserialize into.
   */
  virtual int8_t typeId() const { return m_typeId; }

  /**
   * Return the data serializable fixed ID size type for internal use.
   * @since GFE 5.7
   */
  virtual int8_t DSFID() const { return GeodeTypeIdsImpl::FixedIDByte; }

  int32_t classId() const { return 0; }
};

typedef SharedPtr<CachedDeserializableHelper> CachedDeserializableHelperPtr;
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_CACHEDDESERIALIZABLEHELPER_H_
