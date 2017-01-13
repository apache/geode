/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GEMFIRE_IMPL_SERIALIZATIONREGISTRY_HPP_
#define _GEMFIRE_IMPL_SERIALIZATIONREGISTRY_HPP_

#include <gfcpp/gfcpp_globals.hpp>

#include <gfcpp/Serializable.hpp>
#include <gfcpp/PdxSerializer.hpp>
#include <ace/Hash_Map_Manager.h>
#include <ace/Thread_Mutex.h>
#include <ace/Null_Mutex.h>
#include <gfcpp/GemfireTypeIds.hpp>
#include "GemfireTypeIdsImpl.hpp"
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
  inline unsigned long operator()(int64_t t) const { return (long)t; }
};

ACE_END_VERSIONED_NAMESPACE_DECL
#endif

namespace gemfire {

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
      output.write((int8_t)GemfireTypeIds::NullObj);
    } else {
      int8_t typeId = obj->typeId();
      switch (obj->DSFID()) {
        case GemfireTypeIdsImpl::FixedIDByte:
          output.write((int8_t)GemfireTypeIdsImpl::FixedIDByte);
          output.write(typeId);  // write the type ID.
          break;
        case GemfireTypeIdsImpl::FixedIDShort:
          output.write((int8_t)GemfireTypeIdsImpl::FixedIDShort);
          output.writeInt((int16_t)typeId);  // write the type ID.
          break;
        case GemfireTypeIdsImpl::FixedIDInt:
          output.write((int8_t)GemfireTypeIdsImpl::FixedIDInt);
          output.writeInt((int32_t)typeId);  // write the type ID.
          break;
        default:
          output.write(typeId);  // write the type ID.
          break;
      }

      if ((int32_t)typeId == GemfireTypeIdsImpl::CacheableUserData) {
        output.write((int8_t)obj->classId());
      } else if ((int32_t)typeId == GemfireTypeIdsImpl::CacheableUserData2) {
        output.writeInt((int16_t)obj->classId());
      } else if ((int32_t)typeId == GemfireTypeIdsImpl::CacheableUserData4) {
        output.writeInt((int32_t)obj->classId());
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
}

#endif
