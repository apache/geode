#pragma once

#ifndef GEODE_SERIALIZATIONREGISTRY_H_
#define GEODE_SERIALIZATIONREGISTRY_H_

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
#include <gfcpp/PdxSerializer.hpp>
#include <ace/Hash_Map_Manager.h>
#include <ace/Thread_Mutex.h>
#include <ace/Null_Mutex.h>
#include <gfcpp/GeodeTypeIds.hpp>
#include "GeodeTypeIdsImpl.hpp"
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/Delta.hpp>
#include <string>

#if defined(_MACOSX)
ACE_BEGIN_VERSIONED_NAMESPACE_DECL
// TODO CMake check type int64_t
template <>
class ACE_Export ACE_Hash<int64_t> {
 public:
  inline unsigned long operator()(int64_t t) const {
    return static_cast<long>(t);
  }
};

ACE_END_VERSIONED_NAMESPACE_DECL
#endif

namespace apache {
namespace geode {
namespace client {

typedef ACE_Hash_Map_Manager<int64_t, TypeFactoryMethod, ACE_Null_Mutex>
    IdToFactoryMap;

typedef ACE_Hash_Map_Manager<std::string, TypeFactoryMethodPdx, ACE_Null_Mutex>
    StrToPdxFactoryMap;

class CPPCACHE_EXPORT SerializationRegistry {
 public:
  /** write the length of the serialization, write the typeId of the object,
   * then write whatever the object's toData requires. The length at the
   * front is backfilled after the serialization.
   */
  inline static void serialize(const Serializable* obj, DataOutput& output,
                               bool isDelta = false) {
    if (obj == NULL) {
      output.write(static_cast<int8_t>(GeodeTypeIds::NullObj));
    } else {
      int8_t typeId = obj->typeId();
      switch (obj->DSFID()) {
        case GeodeTypeIdsImpl::FixedIDByte:
          output.write(static_cast<int8_t>(GeodeTypeIdsImpl::FixedIDByte));
          output.write(typeId);  // write the type ID.
          break;
        case GeodeTypeIdsImpl::FixedIDShort:
          output.write(static_cast<int8_t>(GeodeTypeIdsImpl::FixedIDShort));
          output.writeInt(static_cast<int16_t>(typeId));  // write the type ID.
          break;
        case GeodeTypeIdsImpl::FixedIDInt:
          output.write(static_cast<int8_t>(GeodeTypeIdsImpl::FixedIDInt));
          output.writeInt(static_cast<int32_t>(typeId));  // write the type ID.
          break;
        default:
          output.write(typeId);  // write the type ID.
          break;
      }

      if (static_cast<int32_t>(typeId) == GeodeTypeIdsImpl::CacheableUserData) {
        output.write(static_cast<int8_t>(obj->classId()));
      } else if (static_cast<int32_t>(typeId) ==
                 GeodeTypeIdsImpl::CacheableUserData2) {
        output.writeInt(static_cast<int16_t>(obj->classId()));
      } else if (static_cast<int32_t>(typeId) ==
                 GeodeTypeIdsImpl::CacheableUserData4) {
        output.writeInt(obj->classId());
      }
      if (isDelta) {
        const Delta* ptr = dynamic_cast<const Delta*>(obj);
        ptr->toDelta(output);
      } else {
        obj->toData(output);  // let the obj serialize itself.
      }
    }
  }

  inline static void serialize(const SerializablePtr& obj, DataOutput& output) {
    serialize(obj.ptr(), output);
  }

  /**
   * Read the length, typeid, and run the objs fromData. Returns the New
   * object.
   */
  static SerializablePtr deserialize(DataInput& input, int8_t typeId = -1);

  static void addType(TypeFactoryMethod func);

  static void addType(int64_t compId, TypeFactoryMethod func);

  static void addPdxType(TypeFactoryMethodPdx func);

  static void setPdxSerializer(PdxSerializerPtr pdxSerializer);

  static PdxSerializerPtr getPdxSerializer();

  static void removeType(int64_t compId);

  static void init();

  // following for internal types with Data Serializable Fixed IDs  - since GFE
  // 5.7

  static void addType2(TypeFactoryMethod func);

  static void addType2(int64_t compId, TypeFactoryMethod func);

  static void removeType2(int64_t compId);

  static int32_t GetPDXIdForType(const char* poolName, SerializablePtr pdxType);

  static SerializablePtr GetPDXTypeById(const char* poolName, int32_t typeId);

  static int32_t GetEnumValue(SerializablePtr enumInfo);
  static SerializablePtr GetEnum(int32_t val);

  static PdxSerializablePtr getPdxType(char* className);

 private:
  static PoolPtr getPool();
  static IdToFactoryMap* s_typeMap;
  static PdxSerializerPtr m_pdxSerializer;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_SERIALIZATIONREGISTRY_H_
