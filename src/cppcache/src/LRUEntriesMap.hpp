/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_LRUENTRIESMAP_H__
#define __GEMFIRE_IMPL_LRUENTRIESMAP_H__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Cache.hpp>
#include "ConcurrentEntriesMap.hpp"
#include "LRUAction.hpp"
#include "LRUList.hpp"
#include "LRUMapEntry.hpp"
#include "MapEntryT.hpp"
#include "SpinLock.hpp"

#include "NonCopyable.hpp"

namespace gemfire {
class EvictionController;

/**
 * @brief Concurrent entries map with LRU behavior.
 * Not designed for subclassing...
 */

/* adongre
 * CID 28728: Other violation (MISSING_COPY)
 * Class "gemfire::LRUEntriesMap" owns resources that are managed in its
 * constructor and destructor but has no user-written copy constructor.
 *
 * FIX : Make the class non copyable
 *
 * CID 28714: Other violation (MISSING_ASSIGN)
 * Class "gemfire::LRUEntriesMap" owns resources that are managed in
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

}  // namespace gemfire

#endif  // __GEMFIRE_IMPL_LRUENTRIESMAP_H__
