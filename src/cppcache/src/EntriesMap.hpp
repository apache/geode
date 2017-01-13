/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_ENTRIESMAP_H__
#define __GEMFIRE_IMPL_ENTRIESMAP_H__

// This needs to be ace free so that the region can include it.

#include <gfcpp/gfcpp_globals.hpp>
#include "MapEntry.hpp"
#include <gfcpp/CacheableKey.hpp>
#include "MapSegment.hpp"
#include <gfcpp/RegionEntry.hpp>

namespace gemfire {

#define SYNCHRONIZE_SEGMENT_FOR_KEY(keyPtr) \
  SegmentMutexGuard _segment_guard(((EntriesMap*)m_entries)->segmentFor(keyPtr))

/**
 * @brief Concurrent entries map. Not designed for subclassing...
 */
class CPPCACHE_EXPORT EntriesMap {
 public:
  EntriesMap(EntryFactory* entryFactory) : m_entryFactory(entryFactory) {}
  virtual ~EntriesMap() {}

  /**
   * Initialize segments with proper EntryFactory.
   */
  virtual void open(uint32_t initialCapacity = 5000) = 0;

  /** @brief Close the map. */
  virtual void close() = 0;

  /**
   * @brief put a value in the map, replacing if key already exists.
   */
  virtual GfErrType put(const CacheableKeyPtr& key,
                        const CacheablePtr& newValue, MapEntryImplPtr& me,
                        CacheablePtr& oldValue, int updateCount,
                        int destroyTracker, VersionTagPtr versionTag,
                        bool& isUpdate = EntriesMap::boolVal,
                        DataInput* delta = NULL) = 0;
  virtual GfErrType invalidate(const CacheableKeyPtr& key, MapEntryImplPtr& me,
                               CacheablePtr& oldValue,
                               VersionTagPtr versionTag) = 0;
  virtual GfErrType create(const CacheableKeyPtr& key,
                           const CacheablePtr& newValue, MapEntryImplPtr& me,
                           CacheablePtr& oldValue, int updateCount,
                           int destroyTracker, VersionTagPtr versionTag) = 0;

  /**
   * @brief get a value out of the map; returns false if absent
   */
  virtual bool get(const CacheableKeyPtr& key, CacheablePtr& value,
                   MapEntryImplPtr& me) = 0;

  /**
   * @brief get MapEntry for key; returns NULL if absent
   */
  virtual void getEntry(const CacheableKeyPtr& key, MapEntryImplPtr& result,
                        CacheablePtr& value) const = 0;
  /** @brief remove all entries in the map. */
  virtual void clear() = 0;

  /**
   * @brief remove the entry for key from the map;
   *   returns false and NULL MapEntry if absent
   */
  virtual GfErrType remove(const CacheableKeyPtr& key, CacheablePtr& result,
                           MapEntryImplPtr& me, int updateCount,
                           VersionTagPtr versionTag, bool afterRemote) = 0;

  /**
   * @brief return true if there exists an entry for the key.
   */
  virtual bool containsKey(const CacheableKeyPtr& key) const = 0;

  /**
   * @brief return the all the keys in a vector.
   */
  virtual void keys(VectorOfCacheableKey& result) const = 0;

  /**
   * @brief return all the entries in a vector.
   */
  virtual void entries(VectorOfRegionEntry& result) const = 0;

  /**
   * @brief return all values in a vector.
   */
  virtual void values(VectorOfCacheable& result) const = 0;

  /** @brief return the number of entries in the map. */
  virtual uint32_t size() const = 0;

  /**
   * Add a watch for updates for the given entry. If the entry is present in
   * the cache then the current update counter for the entry is returned,
   * else a new entry is created with destroyed token and zero is returned.
   * A failure value of -1 is returned if <addIfAbsent> is false and an
   * existing entry is not found or <failIfPresent> parameter is true
   * and an existing entry is found.
   * The parameter <incUpdateCount> should be set to true if this is a write
   * operation and update counter sould be incremented for the entry.
   */
  virtual int addTrackerForEntry(const CacheableKeyPtr& key,
                                 CacheablePtr& oldValue, bool addIfAbsent,
                                 bool failIfPresent, bool incUpdateCount) = 0;

  virtual void removeTrackerForEntry(const CacheableKeyPtr& key) = 0;

  /**
   * Add trackers for all the entries and returns the map of keys to
   * tracking number. The "addDestroyTracking" flag also adds tracking
   * for any destroys (e.g. through notification) to the map so that
   * subsequent creates do not overwrite the destroys.
   */
  virtual int addTrackerForAllEntries(MapOfUpdateCounters& updateCounterMap,
                                      bool addDestroyTracking) = 0;

  /**
   * Decrement destroy tracking flag for this map by one.
   */
  virtual void removeDestroyTracking() = 0;

  /**
   * @brief The MapSegment can be used to protect against concurrency...
   * SYNCHRONIZE_SEGMENT_FOR_KEY( keyPtr ) creates a guard that locks
   * and release the segment.
   */
  virtual MapSegment* segmentFor(const CacheableKeyPtr& key) const = 0;

  virtual CacheablePtr getFromDisk(const CacheableKeyPtr& key,
                                   MapEntryImpl* me) const {
    return NULLPTR;
  }

  virtual void reapTombstones(std::map<uint16_t, int64_t>& gcVersions) = 0;

  virtual void reapTombstones(CacheableHashSetPtr removedKeys) = 0;

  /**
   * for internal testing, returns if an entry is a tombstone
   */
  virtual GfErrType isTombstone(CacheableKeyPtr& key, MapEntryImplPtr& me,
                                bool& result) = 0;

  static bool boolVal;

 protected:
  EntryFactory* m_entryFactory;

  /** @brief return the instance of EntryFactory for the segments to use. */
  inline const EntryFactory* getEntryFactory() const { return m_entryFactory; }

};  // class EntriesMap

}  // namespace gemfire

#endif  // __GEMFIRE_IMPL_ENTRIESMAP_H__
