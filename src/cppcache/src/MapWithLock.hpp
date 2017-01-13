#ifndef __GEMFIRE_IMPL_MAP_WITHLOCK_H__
#define __GEMFIRE_IMPL_MAP_WITHLOCK_H__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *
 * The specification of function behaviors is found in the corresponding .cpp
 *file.
 *
 *========================================================================
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
struct hash<gemfire::CacheableKeyPtr> {
  size_t operator()(const gemfire::CacheableKeyPtr& key) const {
    return key->hashcode();
  }
};

/** @brief Template specialization of equal_to<T> for CacheableKeyPtr
 * to enable using CacheableKeyPtr's in std::unordered_map/hash_map.
 */
template <>
struct equal_to<gemfire::CacheableKeyPtr> {
  size_t operator()(const gemfire::CacheableKeyPtr& key1,
                    const gemfire::CacheableKeyPtr& key2) const {
    return (*key1.ptr() == *key2.ptr());
  }
};
}

typedef std::unordered_map<gemfire::CacheableKeyPtr, int> MapOfUpdateCounters;

namespace gemfire {

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
}
#endif  // define __GEMFIRE_IMPL_MAP_WITHLOCK_H__
