/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_CONCURRENTENTRIESMAP_H__
#define __GEMFIRE_IMPL_CONCURRENTENTRIESMAP_H__

#include <gfcpp/gfcpp_globals.hpp>
#include "EntriesMap.hpp"
#include "MapSegment.hpp"
#include "AtomicInc.hpp"
#include "ExpMapEntry.hpp"
#include <gfcpp/RegionEntry.hpp>

namespace gemfire {
class RegionInternal;

/**
 * @brief Concurrent entries map.
 */
class CPPCACHE_EXPORT ConcurrentEntriesMap : public EntriesMap {
 protected:
  uint8_t m_concurrency;
  MapSegment* m_segments;
  AtomicInc m_size;
  RegionInternal* m_region;
  volatile int m_numDestroyTrackers;
  bool m_concurrencyChecksEnabled;
  // TODO:  hashcode() is invoked 3-4 times -- need a better
  // implementation (STLport hash_map?) that will invoke it only once
  /**
   * Return a reference to the segment for which the given key would
   * be stored.
   */
  virtual MapSegment* segmentFor(const CacheableKeyPtr& key) const {
    return &(m_segments[segmentIdx(key)]);
  }

  /**
   * Return the segment index number for the given key.
   */
  inline int segmentIdx(const CacheableKeyPtr& key) const {
    return segmentIdx(key->hashcode());
  }

  /**
   * Return the segment index number for the given hash.
   */
  inline int segmentIdx(uint32_t hash) const { return (hash % m_concurrency); }

 public:
  /**
   * @brief constructor, must call open before using map.
   */
  ConcurrentEntriesMap(EntryFactory* entryFactory,
                       bool concurrencyChecksEnabled,
                       RegionInternal* region = NULL, uint8_t concurrency = 16);

  /**
   * Initialize segments with proper EntryFactory.
   */
  virtual void open(uint32_t initialCapacity);

  virtual void close();

  virtual ~ConcurrentEntriesMap();

  virtual void clear();

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
  virtual bool get(const CacheableKeyPtr& key, CacheablePtr& value,
                   MapEntryImplPtr& me);

  /**
   * @brief get MapEntry for key.
   * TODO: return GfErrType like other methods
   */
  virtual void getEntry(const CacheableKeyPtr& key, MapEntryImplPtr& result,
                        CacheablePtr& value) const;

  /**
   * @brief remove the entry for key from the map.
   */
  virtual GfErrType remove(const CacheableKeyPtr& key, CacheablePtr& result,
                           MapEntryImplPtr& me, int updateCount,
                           VersionTagPtr versionTag, bool afterRemote);

  /**
   * @brief return true if there exists an entry for the key.
   */
  virtual bool containsKey(const CacheableKeyPtr& key) const;

  /**
   * @brief return the all the keys in a list.
   */
  virtual void keys(VectorOfCacheableKey& result) const;

  /**
   * @brief return all the entries in a list.
   */
  virtual void entries(VectorOfRegionEntry& result) const;

  /**
   * @brief return all values in a list.
   */
  virtual void values(VectorOfCacheable& result) const;

  /**
   * @brief return the number of entries in the map.
   */
  virtual uint32_t size() const;

  virtual int addTrackerForEntry(const CacheableKeyPtr& key,
                                 CacheablePtr& oldValue, bool addIfAbsent,
                                 bool failIfPresent, bool incUpdateCount);

  virtual void removeTrackerForEntry(const CacheableKeyPtr& key);

  virtual int addTrackerForAllEntries(MapOfUpdateCounters& updateCounterMap,
                                      bool addDestroyTracking);

  virtual void removeDestroyTracking();
  virtual void reapTombstones(std::map<uint16_t, int64_t>& gcVersions);

  virtual void reapTombstones(CacheableHashSetPtr removedKeys);

  /**
   * for internal testing, returns if an entry is a tombstone
   */
  virtual GfErrType isTombstone(CacheableKeyPtr& key, MapEntryImplPtr& me,
                                bool& result);

  /**
   * for internal testing, return the number of times any segment
   * has rehashed.
   */
  uint32_t totalSegmentRehashes() const;
};  // class EntriesMap

}  // namespace gemfire

#endif  // __GEMFIRE_IMPL_CONCURRENTENTRIESMAP_H__
