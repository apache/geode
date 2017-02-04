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

#include "config.h"
#include <gfcpp/gfcpp_globals.hpp>
#include "SerializationRegistry.hpp"
#include <gfcpp/CacheableBuiltins.hpp>
#include <gfcpp/CacheableObjectArray.hpp>
#include <gfcpp/CacheableDate.hpp>
#include <gfcpp/CacheableFileName.hpp>
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/CacheableUndefined.hpp>
#include <gfcpp/Struct.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/GeodeTypeIds.hpp>
#include "CacheableToken.hpp"
#include <gfcpp/Region.hpp>
#include "EventId.hpp"
#include <gfcpp/Properties.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "SpinLock.hpp"
#include <gfcpp/RegionAttributes.hpp>
#include "CacheableObjectPartList.hpp"
#include "ClientConnectionResponse.hpp"
#include "QueueConnectionResponse.hpp"
#include "LocatorListResponse.hpp"
#include "GatewayEventCallbackArgument.hpp"
#include "GatewaySenderEventCallbackArgument.hpp"
#include "ClientProxyMembershipID.hpp"
#include <ace/Singleton.h>
#include <ace/Thread_Mutex.h>
#include "GetAllServersResponse.hpp"
#include "TXCommitMessage.hpp"
#include <gfcpp/PoolManager.hpp>
#include "ThinClientPoolDM.hpp"
#include "PdxType.hpp"
#include <gfcpp/PdxWrapper.hpp>
#include <gfcpp/PdxSerializable.hpp>
#include "EnumInfo.hpp"
#include "VersionTag.hpp"
#include "DiskStoreId.hpp"
#include "DiskVersionTag.hpp"
#include "CachedDeserializableHelper.hpp"

#include "NonCopyable.hpp"

namespace apache {
namespace geode {
namespace client {
/* adongre
 * CID 28729: Other violation (MISSING_COPY)
 * Class "apache::geode::client::TheTypeMap" owns resources that are managed in
 * its
 * constructor and destructor but has no user-written copy constructor.
 *
 * CID 28715: Other violation (MISSING_ASSIGN)
 * Class "apache::geode::client::TheTypeMap" owns resources that are managed
 * in its constructor and destructor but has no user-written assignment
 * operator.
 *
 * FIX : Make the class NonCopyable
 */

class TheTypeMap : private NonCopyable, private NonAssignable {
 private:
  IdToFactoryMap* m_map;
  IdToFactoryMap* m_map2;  // to hold Fixed IDs since GFE 5.7.
  StrToPdxFactoryMap* m_pdxTypemap;
  SpinLock m_mapLock;
  SpinLock m_map2Lock;
  SpinLock m_pdxTypemapLock;

 public:
  TheTypeMap();

  virtual ~TheTypeMap() {
    if (m_map != NULL) {
      delete m_map;
    }

    if (m_map2 != NULL) {
      delete m_map2;
    }

    if (m_pdxTypemap != NULL) {
      delete m_pdxTypemap;
    }
  }

  inline void setup() {
    // Register Geode builtins here!!
    // update type ids in GeodeTypeIds.hpp

    bind(CacheableByte::createDeserializable);
    bind(CacheableBoolean::createDeserializable);
    bind(BooleanArray::createDeserializable);
    bind(CacheableBytes::createDeserializable);
    bind(CacheableFloat::createDeserializable);
    bind(CacheableFloatArray::createDeserializable);
    bind(CacheableDouble::createDeserializable);
    bind(CacheableDoubleArray::createDeserializable);
    bind(CacheableDate::createDeserializable);
    bind(CacheableFileName::createDeserializable);
    bind(CacheableHashMap::createDeserializable);
    bind(CacheableHashSet::createDeserializable);
    bind(CacheableHashTable::createDeserializable);
    bind(CacheableIdentityHashMap::createDeserializable);
    bind(CacheableLinkedHashSet::createDeserializable);
    bind(CacheableInt16::createDeserializable);
    bind(CacheableInt16Array::createDeserializable);
    bind(CacheableInt32::createDeserializable);
    bind(CacheableInt32Array::createDeserializable);
    bind(CacheableInt64::createDeserializable);
    bind(CacheableInt64Array::createDeserializable);
    bind(CacheableObjectArray::createDeserializable);
    bind(CacheableString::createDeserializable);
    bind(CacheableString::createDeserializableHuge);
    bind(CacheableString::createUTFDeserializable);
    bind(CacheableString::createUTFDeserializableHuge);
    bind(CacheableStringArray::createDeserializable);
    bind(CacheableVector::createDeserializable);
    bind(CacheableArrayList::createDeserializable);
    bind(CacheableLinkedList::createDeserializable);
    bind(CacheableStack::createDeserializable);
    bind(CacheableWideChar::createDeserializable);
    bind(CharArray::createDeserializable);
    bind(CacheableToken::createDeserializable);
    bind(RegionAttributes::createDeserializable);
    bind(Properties::createDeserializable);
    // bind(CacheableObjectPartList::createDeserializable);
    // bind internal/fixed classes - since GFE 5.7
    bind2(CacheableUndefined::createDeserializable);
    bind2(EventId::createDeserializable);
    bind2(Struct::createDeserializable);
    bind2(ClientConnectionResponse::create);
    bind2(QueueConnectionResponse::create);
    bind2(LocatorListResponse::create);
    bind2(ClientProxyMembershipID::createDeserializable);
    bind2(GatewayEventCallbackArgument::createDeserializable);
    bind2(GatewaySenderEventCallbackArgument::createDeserializable);
    bind2(GetAllServersResponse::create);
    bind2(TXCommitMessage::create);
    bind2(EnumInfo::createDeserializable);
    bind2(VersionTag::createDeserializable);
    rebind2(GeodeTypeIdsImpl::DiskStoreId, DiskStoreId::createDeserializable);
    rebind2(GeodeTypeIdsImpl::DiskVersionTag,
            DiskVersionTag::createDeserializable);
    bind2(CachedDeserializableHelper::createForVmCachedDeserializable);
    bind2(CachedDeserializableHelper::createForPreferBytesDeserializable);
    // bind2(VersionedCacheableObjectPartList::createDeserializable);
  }

  inline void clear() {
    SpinLockGuard guard(m_mapLock);
    m_map->unbind_all();

    SpinLockGuard guard2(m_map2Lock);
    m_map2->unbind_all();

    SpinLockGuard guard3(m_pdxTypemapLock);
    m_pdxTypemap->unbind_all();
  }

  inline void find(int64_t id, TypeFactoryMethod& func) {
    SpinLockGuard guard(m_mapLock);
    m_map->find(id, func);
  }

  inline void find2(int64_t id, TypeFactoryMethod& func) {
    SpinLockGuard guard(m_map2Lock);
    m_map2->find(id, func);
  }

  inline void bind(TypeFactoryMethod func) {
    Serializable* obj = func();
    SpinLockGuard guard(m_mapLock);
    int64_t compId = static_cast<int64_t>(obj->typeId());
    if (compId == GeodeTypeIdsImpl::CacheableUserData ||
        compId == GeodeTypeIdsImpl::CacheableUserData2 ||
        compId == GeodeTypeIdsImpl::CacheableUserData4) {
      compId |= ((static_cast<int64_t>(obj->classId())) << 32);
    }
    delete obj;
    int bindRes = m_map->bind(compId, func);
    if (bindRes == 1) {
      LOGERROR(
          "A class with "
          "ID %d is already registered.",
          compId);
      throw IllegalStateException(
          "A class with "
          "given ID is already registered.");
    } else if (bindRes == -1) {
      LOGERROR(
          "Unknown error "
          "while adding class ID %d to map.",
          compId);
      throw IllegalStateException(
          "Unknown error "
          "while adding type to map.");
    }
  }

  inline void rebind(int64_t compId, TypeFactoryMethod func) {
    SpinLockGuard guard(m_mapLock);
    int bindRes = m_map->rebind(compId, func);
    if (bindRes == -1) {
      LOGERROR(
          "Unknown error "
          "while adding class ID %d to map.",
          compId);
      throw IllegalStateException(
          "Unknown error "
          "while adding type to map.");
    }
  }

  inline void unbind(int64_t compId) {
    SpinLockGuard guard(m_mapLock);
    m_map->unbind(compId);
  }

  inline void bind2(TypeFactoryMethod func) {
    Serializable* obj = func();
    SpinLockGuard guard(m_map2Lock);
    int8_t dsfid = obj->DSFID();

    int64_t compId = 0;
    if (dsfid == GeodeTypeIdsImpl::FixedIDShort) {
      compId = compId = static_cast<int64_t>(obj->classId());
    } else {
      compId = static_cast<int64_t>(obj->typeId());
    }
    delete obj;
    int bindRes = m_map2->bind(compId, func);
    if (bindRes == 1) {
      LOGERROR(
          "A fixed class with "
          "ID %d is already registered.",
          compId);
      throw IllegalStateException(
          "A fixed class with "
          "given ID is already registered.");
    } else if (bindRes == -1) {
      LOGERROR(
          "Unknown error "
          "while adding class ID %d to map2.",
          compId);
      throw IllegalStateException(
          "Unknown error "
          "while adding to map2.");
    }
  }

  inline void rebind2(int64_t compId, TypeFactoryMethod func) {
    SpinLockGuard guard(m_map2Lock);
    m_map2->rebind(compId, func);
  }

  inline void unbind2(int64_t compId) {
    SpinLockGuard guard(m_map2Lock);
    m_map2->unbind(compId);
  }

  inline void bindPdxType(TypeFactoryMethodPdx func) {
    PdxSerializable* obj = func();
    SpinLockGuard guard(m_pdxTypemapLock);
    const char* objFullName = obj->getClassName();

    int bindRes = m_pdxTypemap->bind(objFullName, func);

    delete obj;

    if (bindRes == 1) {
      LOGERROR("A object with FullName %s is already registered.", objFullName);
      throw IllegalStateException(
          "A Object with "
          "given FullName is already registered.");
    } else if (bindRes == -1) {
      LOGERROR(
          "Unknown error "
          "while adding Pdx Object named %s to map.",
          objFullName);
      throw IllegalStateException(
          "Unknown error "
          "while adding type to map.");
    }
  }

  inline void findPdxType(char* objFullName, TypeFactoryMethodPdx& func) {
    SpinLockGuard guard(m_pdxTypemapLock);
    m_pdxTypemap->find(objFullName, func);
  }

  inline void rebindPdxType(char* objFullName, TypeFactoryMethodPdx func) {
    SpinLockGuard guard(m_pdxTypemapLock);
    int bindRes = m_pdxTypemap->rebind(objFullName, func);
    if (bindRes == -1) {
      LOGERROR(
          "Unknown error "
          "while adding Pdx Object FullName %s to map.",
          objFullName);
      throw IllegalStateException(
          "Unknown error "
          "while adding type to map.");
    }
  }

  inline void unbindPdxType(char* objFullName) {
    SpinLockGuard guard(m_pdxTypemapLock);
    m_pdxTypemap->unbind(objFullName);
  }
};

TheTypeMap::TheTypeMap() {
  m_map = new IdToFactoryMap();

  // second map to hold internal Data Serializable Fixed IDs - since GFE 5.7
  m_map2 = new IdToFactoryMap();

  // map to hold PDX types <string, funptr>.
  m_pdxTypemap = new StrToPdxFactoryMap();
}

typedef ACE_Singleton<TheTypeMap, ACE_Thread_Mutex> theTypeMap;

PdxSerializerPtr SerializationRegistry::m_pdxSerializer = NULLPTR;

/** This starts at reading the typeid.. assumes the length has been read. */
SerializablePtr SerializationRegistry::deserialize(DataInput& input,
                                                   int8_t typeId) {
  bool findinternal = false;
  int8_t currentTypeId = typeId;

  if (typeId == -1) input.read(&currentTypeId);
  int64_t compId = currentTypeId;

  LOGDEBUG("SerializationRegistry::deserialize typeid = %d currentTypeId= %d ",
           typeId, currentTypeId);
  if (compId == GeodeTypeIds::NullObj) {
    return NULLPTR;
  } else if (compId == GeodeTypeIds::CacheableNullString) {
    return SerializablePtr(CacheableString::createDeserializable());
  } else if (compId == GeodeTypeIdsImpl::CacheableUserData) {
    int8_t classId = 0;
    input.read(&classId);
    compId |= ((static_cast<int64_t>(classId)) << 32);
  } else if (compId == GeodeTypeIdsImpl::CacheableUserData2) {
    int16_t classId = 0;
    input.readInt(&classId);
    compId |= ((static_cast<int64_t>(classId)) << 32);
  } else if (compId == GeodeTypeIdsImpl::CacheableUserData4) {
    int32_t classId = 0;
    input.readInt(&classId);
    compId |= ((static_cast<int64_t>(classId)) << 32);
  }

  TypeFactoryMethod createType = NULL;

  if (compId == GeodeTypeIdsImpl::FixedIDByte) {
    int8_t fixedId;
    input.read(&fixedId);
    compId = fixedId;
    findinternal = true;
  } else if (compId == GeodeTypeIdsImpl::FixedIDShort) {
    int16_t fixedId;
    input.readInt(&fixedId);
    compId = fixedId;
    findinternal = true;
  } else if (compId == GeodeTypeIdsImpl::FixedIDInt) {
    int32_t fixedId;
    input.readInt(&fixedId);
    compId = fixedId;
    findinternal = true;
  }

  if (findinternal) {
    theTypeMap::instance()->find2(compId, createType);
  } else {
    theTypeMap::instance()->find(compId, createType);
  }
  if (createType == NULL) {
    if (findinternal) {
      LOGERROR(
          "Unregistered class ID %d during deserialization: Did the "
          "application register "
          "serialization types?",
          compId);
    } else {
      LOGERROR(
          "Unregistered class ID %d during deserialization: Did the "
          "application register "
          "serialization types?",
          (compId >> 32));
    }

    // instead of a null key or null value... an Exception should be thrown..
    throw IllegalStateException("Unregistered class ID in deserialization");
  }
  SerializablePtr obj(createType());
  // This assignment allows the fromData method to return a different object.
  return SerializablePtr(obj->fromData(input));
}

void SerializationRegistry::addType(TypeFactoryMethod func) {
  theTypeMap::instance()->bind(func);
}

void SerializationRegistry::addPdxType(TypeFactoryMethodPdx func) {
  theTypeMap::instance()->bindPdxType(func);
}

void SerializationRegistry::addType(int64_t compId, TypeFactoryMethod func) {
  theTypeMap::instance()->rebind(compId, func);
}

void SerializationRegistry::removeType(int64_t compId) {
  theTypeMap::instance()->unbind(compId);
}

void SerializationRegistry::addType2(TypeFactoryMethod func) {
  theTypeMap::instance()->bind2(func);
}

void SerializationRegistry::addType2(int64_t compId, TypeFactoryMethod func) {
  theTypeMap::instance()->rebind2(compId, func);
}

void SerializationRegistry::removeType2(int64_t compId) {
  theTypeMap::instance()->unbind2(compId);
}

void SerializationRegistry::init() {
  // Everything here is done in the constructor for TheTypeMap...
  theTypeMap::instance();
  theTypeMap::instance()->clear();
  theTypeMap::instance()->setup();
}

PdxSerializablePtr SerializationRegistry::getPdxType(char* className) {
  TypeFactoryMethodPdx objectType = NULL;
  theTypeMap::instance()->findPdxType(className, objectType);
  PdxSerializablePtr pdxObj;
  if (objectType == NULL) {
    try {
      pdxObj = new PdxWrapper((const char*)className);
    } catch (const Exception&) {
      LOGERROR(
          "Unregistered class %s during PDX deserialization: Did the "
          "application register the PDX type or serializer?",
          className);
      throw IllegalStateException(
          "Unregistered class or serializer in PDX deserialization");
    }
  } else {
    pdxObj = objectType();
  }
  return pdxObj;
}

void SerializationRegistry::setPdxSerializer(PdxSerializerPtr pdxSerializer) {
  m_pdxSerializer = pdxSerializer;
}

PdxSerializerPtr SerializationRegistry::getPdxSerializer() {
  return m_pdxSerializer;
}

int32_t SerializationRegistry::GetPDXIdForType(const char* poolName,
                                               SerializablePtr pdxType) {
  PoolPtr pool = NULLPTR;

  if (poolName == NULL) {
    const HashMapOfPools& pools = PoolManager::getAll();
    if (pools.size() > 0) {
      for (HashMapOfPools::Iterator iter = pools.begin(); iter != pools.end();
           ++iter) {
        PoolPtr currPool(iter.second());
        pool = currPool;
        break;
      }
    }
  } else {
    pool = PoolManager::find(poolName);
  }

  if (pool == NULLPTR) {
    throw IllegalStateException("Pool not found, Pdx operation failed");
  }

  return static_cast<ThinClientPoolDM*>(pool.ptr())->GetPDXIdForType(pdxType);
}

SerializablePtr SerializationRegistry::GetPDXTypeById(const char* poolName,
                                                      int32_t typeId) {
  PoolPtr pool = NULLPTR;

  if (poolName == NULL) {
    const HashMapOfPools& pools = PoolManager::getAll();
    if (pools.size() > 0) {
      HashMapOfPools::Iterator iter = pools.begin();
      PoolPtr currPool(iter.second());
      pool = currPool;
    }
  } else {
    pool = PoolManager::find(poolName);
  }

  if (pool == NULLPTR) {
    throw IllegalStateException("Pool not found, Pdx operation failed");
  }

  return static_cast<ThinClientPoolDM*>(pool.ptr())->GetPDXTypeById(typeId);
}

int32_t SerializationRegistry::GetEnumValue(SerializablePtr enumInfo) {
  PoolPtr pool = getPool();
  if (pool == NULLPTR) {
    throw IllegalStateException("Pool not found, Pdx operation failed");
  }

  return static_cast<ThinClientPoolDM*>(pool.ptr())->GetEnumValue(enumInfo);
}
SerializablePtr SerializationRegistry::GetEnum(int32_t val) {
  PoolPtr pool = getPool();
  if (pool == NULLPTR) {
    throw IllegalStateException("Pool not found, Pdx operation failed");
  }

  return static_cast<ThinClientPoolDM*>(pool.ptr())->GetEnum(val);
}

PoolPtr SerializationRegistry::getPool() {
  PoolPtr pool = NULLPTR;
  const HashMapOfPools& pools = PoolManager::getAll();
  if (pools.size() > 0) {
    for (HashMapOfPools::Iterator iter = pools.begin(); iter != pools.end();
         ++iter) {
      PoolPtr currPool(iter.second());
      pool = currPool;
      break;
    }
  }
  return pool;
}
}  // namespace client
}  // namespace geode
}  // namespace apache
