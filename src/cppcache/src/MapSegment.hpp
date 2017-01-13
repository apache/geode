/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_MAPSEGMENT_H__
#define __GEMFIRE_IMPL_MAPSEGMENT_H__

#include <gfcpp/gfcpp_globals.hpp>

#include <gfcpp/CacheableKey.hpp>
#include "MapEntry.hpp"
#include <gfcpp/RegionEntry.hpp>
#include <gfcpp/VectorT.hpp>
#include "SpinLock.hpp"
#include "MapWithLock.hpp"
#include "CacheableToken.hpp"
#include <gfcpp/Delta.hpp>

#include <ace/Hash_Map_Manager.h>
#include <ace/Functor_T.h>
#include <ace/Null_Mutex.h>
#include <ace/Thread_Mutex.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <vector>
#include <ace/config-lite.h>
#include <ace/Versioned_Namespace.h>
#include "TombstoneList.hpp"
#include <unordered_map>
ACE_BEGIN_VERSIONED_NAMESPACE_DECL

template <>
class ACE_Hash<gemfire::CacheableKeyPtr> {
 public:
  u_long operator()(const gemfire::CacheableKeyPtr& key) {
    return key->hashcode();
  }
};

template <>
class ACE_Equal_To<gemfire::CacheableKeyPtr> {
 public:
  bool operator()(const gemfire::CacheableKeyPtr& key1,
                  const gemfire::CacheableKeyPtr& key2) {
    return key1->operator==(*key2);
  }
};
ACE_END_VERSIONED_NAMESPACE_DECL

namespace gemfire {

class RegionInternal;
typedef ::ACE_Hash_Map_Manager_Ex<
    CacheableKeyPtr, MapEntryPtr, ::ACE_Hash<CacheableKeyPtr>,
    ::ACE_Equal_To<CacheableKeyPtr>, ::ACE_Null_Mutex>
    CacheableKeyHashMap;

/** @brief type wrapper around the ACE map implementation. */
class CPPCACHE_EXPORT MapSegment {
 private:
  // contain
  CacheableKeyHashMap* m_map;
  // refers to object managed by the entries map...
  // does not need deletion here.
  const EntryFactory* m_entryFactory;
  RegionInternal* m_region;

  // index of the current prime in the primes table
  uint32_t m_primeIndex;
  SpinLock m_spinlock;
  ACE_Recursive_Thread_Mutex m_segmentMutex;

  bool m_concurrencyChecksEnabled;
  // number of operations that are tracking destroys
  // the m_destroyedKeys contains the set for which destroys
  // have been received as long as this number is greater than zero
  volatile int* m_numDestroyTrackers;
  MapOfUpdateCounters m_destroyedKeys;

  uint32_t m_rehashCount;
  void rehash();
  TombstoneListPtr m_tombstoneList;

  // increment update counter of the given entry and return true if entry
  // was rebound
  inline bool incrementUpdateCount(const CacheableKeyPtr& key,
                                   MapEntryPtr& entry) {
    // This function is disabled if concurrency checks are enabled. The
    // versioning
    // changes takes care of the version and no need for tracking the entry
    if (m_concurrencyChecksEnabled) return false;
    MapEntryPtr newEntry;
    entry->incrementUpdateCount(newEntry);
    if (newEntry != NULLPTR) {
      m_map->rebind(key, newEntry);
      entry = newEntry;
      return true;
    }
    return false;
  }

  // remove a tracker for the given entry
  inline void removeTrackerForEntry(const CacheableKeyPtr& key,
                                    MapEntryPtr& entry,
                                    MapEntryImpl* entryImpl) {
    // This function is disabled if concurrency checks are enabled. The
    // versioning
    // changes takes care of the version and no need for tracking the entry
    if (m_concurrencyChecksEnabled) return;
    std::pair<bool, int> trackerPair = entry->removeTracker();
    if (trackerPair.second <= 0) {
      CacheablePtr value;
      if (entryImpl == NULL) {
        entryImpl = entry->getImplPtr();
      }
      entryImpl->getValueI(value);
      if (value == NULLPTR) {
        // get rid of an entry marked as destroyed
        m_map->unbind(key);
        return;
      }
    }
    if (trackerPair.first) {
      entry = (entryImpl != NULL ? entryImpl : entry->getImplPtr());
      m_map->rebind(key, entry);
    }
  }

  inline GfErrType putNoEntry(const CacheableKeyPtr& key,
                              const CacheablePtr& newValue,
                              MapEntryImplPtr& newEntry, int updateCount,
                              int destroyTracker, VersionTagPtr versionTag,
                              VersionStamp* versionStamp = NULL) {
    if (!m_concurrencyChecksEnabled) {
      if (updateCount >= 0) {
        // entry was removed while being tracked
        // abort the create and do not change the MapEntry
        return GF_CACHE_ENTRY_UPDATED;
      } else if (destroyTracker > 0 && *m_numDestroyTrackers > 0) {
        MapOfUpdateCounters::iterator pos = m_destroyedKeys.find(key);
        if (pos != m_destroyedKeys.end() && pos->second > destroyTracker) {
          // destroy received for a non-existent entry while being tracked
          // abort the create and do not change the MapEntry
          return GF_CACHE_ENTRY_UPDATED;
        }
      }
    }
    m_entryFactory->newMapEntry(key, newEntry);
    newEntry->setValueI(newValue);
    if (m_concurrencyChecksEnabled) {
      if (versionTag != NULLPTR && versionTag.ptr() != NULL)
        newEntry->getVersionStamp().setVersions(versionTag);
      else if (versionStamp != NULL)
        newEntry->getVersionStamp().setVersions(*versionStamp);
    }
    m_map->bind(key, newEntry);
    return GF_NOERR;
  }

  GfErrType putForTrackedEntry(const CacheableKeyPtr& key,
                               const CacheablePtr& newValue, MapEntryPtr& entry,
                               MapEntryImpl* entryImpl, int updateCount,
                               VersionStamp& versionStamp,
                               DataInput* delta = NULL);

  CacheablePtr getFromDisc(CacheableKeyPtr key, MapEntryImpl* entryImpl);

  GfErrType removeWhenConcurrencyEnabled(
      const CacheableKeyPtr& key, CacheablePtr& oldValue, MapEntryImplPtr& me,
      int updateCount, VersionTagPtr versionTag, bool afterRemote,
      bool& isEntryFound, long expiryTaskID, TombstoneExpiryHandler* handler,
      bool& expTaskSet);

 public:
  MapSegment()
      : m_map(NULL),
        m_entryFactory(NULL),
        m_region(NULL),
        m_primeIndex(0),
        m_spinlock(),
        m_segmentMutex(),
        m_concurrencyChecksEnabled(false),
        m_numDestroyTrackers(NULL),
        m_rehashCount(0)  // COVERITY  --> 30303 Uninitialized scalar field
  {
    m_tombstoneList = new TombstoneList(this);
  }

  ~MapSegment();

  // methods to acquire/release MapSegment mutex (not SpinLock)
  // that allow using MapSegment with ACE_Guard
  int acquire();
  int release();

  /**
   * @brief initialize underlying map structures. Not called by constructor.
   * Used when allocated in arrays by EntriesMap implementations.
   */
  void open(RegionInternal* region, const EntryFactory* entryFactory,
            uint32_t size, volatile int* destroyTrackers,
            bool concurrencyChecksEnabled);

  void close();
  void clear();

  /**
   * @brief put a new value in the map, failing if key already exists.
   * return error code if key already existing or something goes wrong.
   */
  GfErrType create(const CacheableKeyPtr& key, const CacheablePtr& newValue,
                   MapEntryImplPtr& me, CacheablePtr& oldValue, int updateCount,
                   int destroyTracker, VersionTagPtr versionTag);

  /**
   * @brief put a value in the map, replacing if key already exists.
   * return error code is something goes wrong.
   */
  GfErrType put(const CacheableKeyPtr& key, const CacheablePtr& newValue,
                MapEntryImplPtr& me, CacheablePtr& oldValue, int updateCount,
                int destroyTracker, bool& isUpdate, VersionTagPtr versionTag,
                DataInput* delta = NULL);

  GfErrType invalidate(const CacheableKeyPtr& key, MapEntryImplPtr& me,
                       CacheablePtr& oldValue, VersionTagPtr versionTag,
                       bool& isTokenAdded);

  /**
   * @brief remove an entry from the map, setting oldValue.
   */
  GfErrType remove(const CacheableKeyPtr& key, CacheablePtr& oldValue,
                   MapEntryImplPtr& me, int updateCount,
                   VersionTagPtr versionTag, bool afterRemote,
                   bool& isEntryFound);

  /**
   * @brief get a value and MapEntry out of the map.
   * return true if the key is present, false otherwise.
   */
  bool getEntry(const CacheableKeyPtr& key, MapEntryImplPtr& result,
                CacheablePtr& value);

  /**
   * @brief return true if there exists an entry for the key.
   */
  bool containsKey(const CacheableKeyPtr& key);

  /**
   * @brief return the all the keys in the provided list.
   */
  void keys(VectorOfCacheableKey& result);

  /**
   * @brief return all the entries in the provided list.
   */
  void entries(VectorOfRegionEntry& result);

  /**
   * @brief return all values in the provided list.
   */
  void values(VectorOfCacheable& result);

  inline uint32_t rehashCount() { return m_rehashCount; }

  int addTrackerForEntry(const CacheableKeyPtr& key, CacheablePtr& oldValue,
                         bool addIfAbsent, bool failIfPresent,
                         bool incUpdateCount);

  void removeTrackerForEntry(const CacheableKeyPtr& key);

  void addTrackerForAllEntries(MapOfUpdateCounters& updateCounterMap);

  void removeDestroyTracking();

  void reapTombstones(std::map<uint16_t, int64_t>& gcVersions);

  void reapTombstones(CacheableHashSetPtr removedKeys);

  bool removeActualEntry(const CacheableKeyPtr& key, bool cancelTask = true);

  bool unguardedRemoveActualEntryWithoutCancelTask(
      const CacheableKeyPtr& key, TombstoneExpiryHandler*& handler,
      long& taskid);

  bool unguardedRemoveActualEntry(const CacheableKeyPtr& key,
                                  bool cancelTask = true);

  GfErrType isTombstone(CacheableKeyPtr key, MapEntryImplPtr& me, bool& result);

  static bool boolVal;
};

}  // gemfire

#endif  // __GEMFIRE_IMPL_MAPSEGMENT_H__
