/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ConcurrentEntriesMap.hpp"
#include "RegionInternal.hpp"
#include "TableOfPrimes.hpp"
#include "HostAsm.hpp"

#include <algorithm>

using namespace gemfire;

bool EntriesMap::boolVal = false;

ConcurrentEntriesMap::ConcurrentEntriesMap(EntryFactory* entryFactory,
                                           bool concurrencyChecksEnabled,
                                           RegionInternal* region,
                                           uint8_t concurrency)
    : EntriesMap(entryFactory),
      m_concurrency(0),
      m_segments((MapSegment*)0),
      m_size(0),
      m_region(region),
      m_numDestroyTrackers(0),
      /* adongre
       * CID 28929: Uninitialized pointer field (UNINIT_CTOR)
       */
      m_concurrencyChecksEnabled(concurrencyChecksEnabled) {
  GF_DEV_ASSERT(entryFactory != NULL);

  uint8_t maxConcurrency = TableOfPrimes::getMaxPrimeForConcurrency();
  if (concurrency > maxConcurrency) {
    m_concurrency = maxConcurrency;
  } else {
    m_concurrency = TableOfPrimes::nextLargerPrimeForConcurrency(concurrency);
  }
}

void ConcurrentEntriesMap::open(uint32_t initialCapacity) {
  uint32_t segSize = 1 + (initialCapacity - 1) / m_concurrency;
  m_segments = new MapSegment[m_concurrency];
  for (int index = 0; index < m_concurrency; ++index) {
    m_segments[index].open(m_region, this->getEntryFactory(), segSize,
                           &m_numDestroyTrackers, m_concurrencyChecksEnabled);
  }
}

void ConcurrentEntriesMap::close() {
  for (int index = 0; index < m_concurrency; ++index) {
    m_segments[index].close();
  }
}
void ConcurrentEntriesMap::clear() {
  for (uint32_t index = 0; index < m_concurrency; index++) {
    m_segments[index].clear();
  }
  m_size = 0;
}

ConcurrentEntriesMap::~ConcurrentEntriesMap() { delete[] m_segments; }

GfErrType ConcurrentEntriesMap::create(const CacheableKeyPtr& key,
                                       const CacheablePtr& newValue,
                                       MapEntryImplPtr& me,
                                       CacheablePtr& oldValue, int updateCount,
                                       int destroyTracker,
                                       VersionTagPtr versionTag) {
  GfErrType err = GF_NOERR;
  if ((err = segmentFor(key)->create(key, newValue, me, oldValue, updateCount,
                                     destroyTracker, versionTag)) == GF_NOERR &&
      oldValue == NULLPTR) {
    ++m_size;
  }
  return err;
}

GfErrType ConcurrentEntriesMap::invalidate(const CacheableKeyPtr& key,
                                           MapEntryImplPtr& me,
                                           CacheablePtr& oldValue,
                                           VersionTagPtr versionTag) {
  bool isTokenAdded = false;
  GfErrType err =
      segmentFor(key)->invalidate(key, me, oldValue, versionTag, isTokenAdded);
  if (isTokenAdded) {
    ++m_size;
  }
  return err;
}

GfErrType ConcurrentEntriesMap::put(const CacheableKeyPtr& key,
                                    const CacheablePtr& newValue,
                                    MapEntryImplPtr& me, CacheablePtr& oldValue,
                                    int updateCount, int destroyTracker,
                                    VersionTagPtr versionTag, bool& isUpdate,
                                    DataInput* delta) {
  GfErrType err = GF_NOERR;
  if ((err = segmentFor(key)->put(key, newValue, me, oldValue, updateCount,
                                  destroyTracker, isUpdate, versionTag,
                                  delta)) != GF_NOERR) {
    return err;
  }
  if (!isUpdate) {
    ++m_size;
  }
  return err;
}

bool ConcurrentEntriesMap::get(const CacheableKeyPtr& key, CacheablePtr& value,
                               MapEntryImplPtr& me) {
  return segmentFor(key)->getEntry(key, me, value);
}

void ConcurrentEntriesMap::getEntry(const CacheableKeyPtr& key,
                                    MapEntryImplPtr& result,
                                    CacheablePtr& value) const {
  segmentFor(key)->getEntry(key, result, value);
}

GfErrType ConcurrentEntriesMap::remove(const CacheableKeyPtr& key,
                                       CacheablePtr& result,
                                       MapEntryImplPtr& me, int updateCount,
                                       VersionTagPtr versionTag,
                                       bool afterRemote) {
  bool isEntryFound = true;
  GfErrType err;
  if ((err = segmentFor(key)->remove(key, result, me, updateCount, versionTag,
                                     afterRemote, isEntryFound)) == GF_NOERR) {
    //  decrement only if entry is present
    if (isEntryFound) --m_size;
  }
  return err;
}

bool ConcurrentEntriesMap::containsKey(const CacheableKeyPtr& key) const {
  //  MapSegment* segment = segmentFor( key );
  return segmentFor(key)->containsKey(key);
}

void ConcurrentEntriesMap::keys(VectorOfCacheableKey& result) const {
  result.clear();
  for (int index = 0; index < m_concurrency; ++index) {
    m_segments[index].keys(result);
  }
}

void ConcurrentEntriesMap::entries(VectorOfRegionEntry& result) const {
  result.clear();
  for (int index = 0; index < m_concurrency; ++index) {
    m_segments[index].entries(result);
  }
}

void ConcurrentEntriesMap::values(VectorOfCacheable& result) const {
  result.clear();
  for (int index = 0; index < m_concurrency; ++index) {
    m_segments[index].values(result);
  }
}

uint32_t ConcurrentEntriesMap::size() const { return m_size.value(); }

int ConcurrentEntriesMap::addTrackerForEntry(const CacheableKeyPtr& key,
                                             CacheablePtr& oldValue,
                                             bool addIfAbsent,
                                             bool failIfPresent,
                                             bool incUpdateCount) {
  // This function is disabled if concurrency checks are enabled. The versioning
  // changes takes care of the version and no need for tracking the entry
  if (m_concurrencyChecksEnabled) return -1;
  return segmentFor(key)->addTrackerForEntry(key, oldValue, addIfAbsent,
                                             failIfPresent, incUpdateCount);
}

void ConcurrentEntriesMap::removeTrackerForEntry(const CacheableKeyPtr& key) {
  // This function is disabled if concurrency checks are enabled. The versioning
  // changes takes care of the version and no need for tracking the entry
  if (m_concurrencyChecksEnabled) return;
  segmentFor(key)->removeTrackerForEntry(key);
}

int ConcurrentEntriesMap::addTrackerForAllEntries(
    MapOfUpdateCounters& updateCounterMap, bool addDestroyTracking) {
  // This function is disabled if concurrency checks are enabled. The versioning
  // changes takes care of the version and no need for tracking the entry
  if (m_concurrencyChecksEnabled) return -1;
  for (int index = 0; index < m_concurrency; ++index) {
    m_segments[index].addTrackerForAllEntries(updateCounterMap);
  }
  if (addDestroyTracking) {
    return HostAsm::atomicAdd(m_numDestroyTrackers, 1);
  }
  return 0;
}

void ConcurrentEntriesMap::removeDestroyTracking() {
  // This function is disabled if concurrency checks are enabled. The versioning
  // changes takes care of the version and no need for tracking the entry
  if (m_concurrencyChecksEnabled) return;
  if (HostAsm::atomicAdd(m_numDestroyTrackers, -1) == 0) {
    for (int index = 0; index < m_concurrency; ++index) {
      m_segments[index].removeDestroyTracking();
    }
  }
}

/**
 * @brief return the number of times any segment has rehashed.
 */
uint32_t ConcurrentEntriesMap::totalSegmentRehashes() const {
  uint32_t result = 0;
  for (int index = 0; index < m_concurrency; ++index) {
    result += m_segments[index].rehashCount();
  }
  return result;
}
void ConcurrentEntriesMap::reapTombstones(
    std::map<uint16_t, int64_t>& gcVersions) {
  for (int index = 0; index < m_concurrency; ++index) {
    m_segments[index].reapTombstones(gcVersions);
  }
}
void ConcurrentEntriesMap::reapTombstones(CacheableHashSetPtr removedKeys) {
  for (int index = 0; index < m_concurrency; ++index) {
    m_segments[index].reapTombstones(removedKeys);
  }
}
GfErrType ConcurrentEntriesMap::isTombstone(CacheableKeyPtr& key,
                                            MapEntryImplPtr& me, bool& result) {
  return segmentFor(key)->isTombstone(key, me, result);
}
