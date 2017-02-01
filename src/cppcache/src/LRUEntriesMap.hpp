#pragma once

#ifndef GEODE_LRUENTRIESMAP_H_
#define GEODE_LRUENTRIESMAP_H_

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
#include <gfcpp/Cache.hpp>
#include "ConcurrentEntriesMap.hpp"
#include "LRUAction.hpp"
#include "LRUList.hpp"
#include "LRUMapEntry.hpp"
#include "MapEntryT.hpp"
#include "SpinLock.hpp"

#include "NonCopyable.hpp"

namespace apache {
namespace geode {
namespace client {
class EvictionController;

/**
 * @brief Concurrent entries map with LRU behavior.
 * Not designed for subclassing...
 */

/* adongre
 * CID 28728: Other violation (MISSING_COPY)
 * Class "apache::geode::client::LRUEntriesMap" owns resources that are managed
 * in its
 * constructor and destructor but has no user-written copy constructor.
 *
 * FIX : Make the class non copyable
 *
 * CID 28714: Other violation (MISSING_ASSIGN)
 * Class "apache::geode::client::LRUEntriesMap" owns resources that are managed
 * in
 * its constructor and destructor but has no user-written assignment operator.
 * Fix : Make the class Non Assinable
 */
class CPPCACHE_EXPORT LRUEntriesMap : public ConcurrentEntriesMap,
                                      private NonCopyable,
                                      private NonAssignable {
 protected:
  LRUAction* m_action;
  LRUList<MapEntryImpl, MapEntryT<LRUMapEntry, 0, 0> > m_lruList;
  uint32_t m_limit;
  PersistenceManagerPtr m_pmPtr;
  EvictionController* m_evictionControllerPtr;
  int64_t m_currentMapSize;
  SpinLock m_mapInfoLock;
  std::string m_name;
  AtomicInc m_validEntries;
  bool m_heapLRUEnabled;

 public:
  LRUEntriesMap(EntryFactory* entryFactory, RegionInternal* region,
                const LRUAction::Action& lruAction, const uint32_t limit,
                bool concurrencyChecksEnabled, const uint8_t concurrency = 16,
                bool heapLRUEnabled = false);

  virtual ~LRUEntriesMap();

  virtual GfErrType put(const CacheableKeyPtr& key,
                        const CacheablePtr& newValue, MapEntryImplPtr& me,
                        CacheablePtr& oldValue, int updateCount,
                        int destroyTracker, VersionTagPtr versionTag,
                        bool& isUpdate = EntriesMap::boolVal,
                        DataInput* delta = NULL);
  virtual GfErrType invalidate(const CacheableKeyPtr& key, MapEntryImplPtr& me,
                               CacheablePtr& oldValue,
                               VersionTagPtr versionTag);
  virtual GfErrType create(const CacheableKeyPtr& key,
                           const CacheablePtr& newValue, MapEntryImplPtr& me,
                           CacheablePtr& oldValue, int updateCount,
                           int destroyTracker, VersionTagPtr versionTag);
  virtual bool get(const CacheableKeyPtr& key, CacheablePtr& returnPtr,
                   MapEntryImplPtr& me);
  virtual CacheablePtr getFromDisk(const CacheableKeyPtr& key,
                                   MapEntryImpl* me) const;
  GfErrType processLRU();
  void processLRU(int32_t numEntriesToEvict);
  GfErrType evictionHelper();
  void updateMapSize(int64_t size);
  inline void setPersistenceManager(PersistenceManagerPtr& pmPtr) {
    m_pmPtr = pmPtr;
  }

  /**
   * @brief remove an entry, marking it evicted for LRUList maintainance.
   */
  virtual GfErrType remove(const CacheableKeyPtr& key, CacheablePtr& result,
                           MapEntryImplPtr& me, int updateCount,
                           VersionTagPtr versionTag, bool afterRemote);

  virtual void close();

  inline bool mustEvict() const {
    if (m_action == NULL) {
      LOGFINE("Eviction action is NULL");
      return false;
    }
    if (m_action->overflows()) {
      return validEntriesSize() > m_limit;
    } else if ((m_heapLRUEnabled) && (m_limit == 0)) {
      return false;
    } else {
      return size() > m_limit;
    }
  }

  inline uint32_t validEntriesSize() const { return m_validEntries.value(); }

  inline void adjustLimit(uint32_t limit) { m_limit = limit; }

  virtual void clear();

};  // class LRUEntriesMap
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_LRUENTRIESMAP_H_
