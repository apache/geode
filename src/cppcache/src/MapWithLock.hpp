#pragma once

#ifndef GEODE_MAPWITHLOCK_H_
#define GEODE_MAPWITHLOCK_H_

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
#include <gfcpp/CacheableKey.hpp>

#include <ace/Hash_Map_Manager.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/config-lite.h>
#include <ace/Versioned_Namespace.h>

#include <unordered_map>
#include <string>

namespace std {
/** @brief Template specialization of hash<T> for CacheableKeyPtr
 * to enable using CacheableKeyPtr's in std::unordered_map/hash_map.
 */
template <>
struct hash<apache::geode::client::CacheableKeyPtr> {
  size_t operator()(const apache::geode::client::CacheableKeyPtr& key) const {
    return key->hashcode();
  }
};

/** @brief Template specialization of equal_to<T> for CacheableKeyPtr
 * to enable using CacheableKeyPtr's in std::unordered_map/hash_map.
 */
template <>
struct equal_to<apache::geode::client::CacheableKeyPtr> {
  size_t operator()(const apache::geode::client::CacheableKeyPtr& key1,
                    const apache::geode::client::CacheableKeyPtr& key2) const {
    return (*key1.ptr() == *key2.ptr());
  }
};
}  // namespace std

typedef std::unordered_map<apache::geode::client::CacheableKeyPtr, int>
    MapOfUpdateCounters;

namespace apache {
namespace geode {
namespace client {

class Region;
typedef SharedPtr<Region> RegionPtr;

/** Map type used to hold root regions in the Cache, and subRegions. */
typedef ACE_Hash_Map_Manager_Ex<std::string, RegionPtr, ACE_Hash<std::string>,
                                ACE_Equal_To<std::string>,
                                ACE_Recursive_Thread_Mutex>
    MapOfRegionWithLock;
typedef ACE_Hash_Map_Manager_Ex<std::string, CqQueryPtr, ACE_Hash<std::string>,
                                ACE_Equal_To<std::string>,
                                ACE_Recursive_Thread_Mutex>
    MapOfCqQueryWithLock;

/** Guard type for locking a MapOfRegionWithLock while iterating or performing
 * other composite operations. ex.. MapOfRegionGuard guard( map->mutex() );
 */
typedef ACE_Guard<ACE_Recursive_Thread_Mutex> MapOfRegionGuard;
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_MAPWITHLOCK_H_
