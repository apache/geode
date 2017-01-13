/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "MapSegment.hpp"
#include "MapEntry.hpp"
#include "TrackedMapEntry.hpp"
#include "RegionInternal.hpp"
#include "TableOfPrimes.hpp"
#include "SpinLock.hpp"
#include "Utils.hpp"
#include "ThinClientPoolDM.hpp"
#include "ThinClientRegion.hpp"
#include "TombstoneExpiryHandler.hpp"
#include <ace/OS.h>
#include "ace/Time_Value.h"
using namespace gemfire;

#define _GF_GUARD_SEGMENT SpinLockGuard mapGuard(m_spinlock)
#define _VERSION_TAG_NULL_CHK \
  (versionTag != NULLPTR && versionTag.ptr() != NULL)
bool MapSegment::boolVal = false;
MapSegment::~MapSegment() {
  delete m_map;
  // m_entryFactory will be disposed by the containing EntriesMap impl.
}

void MapSegment::open(RegionInternal* region, const EntryFactory* entryFactory,
                      uint32_t size, volatile int* destroyTrackers,
                      bool concurrencyChecksEnabled) {
  m_map = new CacheableKeyHashMap();
  uint32_t mapSize = TableOfPrimes::nextLargerPrime(size, m_primeIndex);
  LOGFINER("Initializing MapSegment with size %d (given size %d).", mapSize,
           size);
  m_map->open(mapSize);
  m_entryFactory = entryFactory;
  m_region = region;
  m_numDestroyTrackers = destroyTrackers;
  m_concurrencyChecksEnabled = concurrencyChecksEnabled;
}

void MapSegment::close() { m_map->close(); }

void MapSegment::clear() {
  _GF_GUARD_SEGMENT;
  m_map->unbind_all();
}

int MapSegment::acquire() { return m_segmentMutex.acquire(); }

int MapSegment::release() { return m_segmentMutex.release(); }

GfErrType MapSegment::create(const CacheableKeyPtr& key,
                             const CacheablePtr& newValue, MapEntryImplPtr& me,
                             CacheablePtr& oldValue, int updateCount,
                             int destroyTracker, VersionTagPtr versionTag) {
  long taskid = -1;
  TombstoneExpiryHandler* handler = NULL;
  GfErrType err = GF_NOERR;
  {
    _GF_GUARD_SEGMENT;
    // if size is greater than 75 percent of prime, rehash
    uint32_t mapSize = TableOfPrimes::getPrime(m_primeIndex);
    if (((m_map->current_size() * 75) / 100) > mapSize) {
      rehash();
    }
    MapEntryPtr entry;
    int status;
    if ((status = m_map->find(key, entry)) == -1) {
      if ((err = putNoEntry(key, newValue, me, updateCount, destroyTracker,
                            versionTag)) != GF_NOERR) {
        return err;
      }
    } else {
      MapEntryImpl* entryImpl = entry->getImplPtr();
      entryImpl->getValueI(oldValue);
      if (oldValue == NULLPTR || CacheableToken::isTombstone(oldValue)) {
        // pass the version stamp
        VersionStamp versionStamp;
        if (m_concurrencyChecksEnabled) {
          versionStamp = entry->getVersionStamp();
          if (_VERSION_TAG_NULL_CHK) {
            err = versionStamp.processVersionTag(m_region, key, versionTag,
                                                 false);
            if (err != GF_NOERR) return err;
            versionStamp.setVersions(versionTag);
          }
        }
        // good case; go ahead with the create
        if (oldValue == NULLPTR) {
          err = putForTrackedEntry(key, newValue, entry, entryImpl, updateCount,
                                   versionStamp);
        } else {
          unguardedRemoveActualEntryWithoutCancelTask(key, handler, taskid);
          err = putNoEntry(key, newValue, me, updateCount, destroyTracker,
                           versionTag, &versionStamp);
        }

        oldValue = NULLPTR;

      } else {
        err = GF_CACHE_ENTRY_EXISTS;
      }
      if (err == GF_NOERR) {
        me = entryImpl;
      }
    }
  }
  if (taskid != -1) {
    CacheImpl::expiryTaskManager->cancelTask(taskid);
    if (handler != NULL) delete handler;
  }
  return err;
}

/**
 * @brief put a value in the map, replacing if key already exists.
 */
GfErrType MapSegment::put(const CacheableKeyPtr& key,
                          const CacheablePtr& newValue, MapEntryImplPtr& me,
                          CacheablePtr& oldValue, int updateCount,
                          int destroyTracker, bool& isUpdate,
                          VersionTagPtr versionTag, DataInput* delta) {
  long taskid = -1;
  TombstoneExpiryHandler* handler = NULL;
  GfErrType err = GF_NOERR;
  {
    _GF_GUARD_SEGMENT;
    // if size is greater than 75 percent of prime, rehash
    uint32_t mapSize = TableOfPrimes::getPrime(m_primeIndex);
    if (((m_map->current_size() * 75) / 100) > mapSize) {
      rehash();
    }
    MapEntryPtr entry;
    int status;
    if ((status = m_map->find(key, entry)) == -1) {
      if (delta != NULL) {
        return GF_INVALID_DELTA;  // You can not apply delta when there is no
      }
      // entry hence ask for full object
      isUpdate = false;
      err = putNoEntry(key, newValue, me, updateCount, destroyTracker,
                       versionTag);
    } else {
      MapEntryImpl* entryImpl = entry->getImplPtr();
      CacheablePtr meOldValue;
      entryImpl->getValueI(meOldValue);
      // pass the version stamp
      VersionStamp versionStamp;
      if (m_concurrencyChecksEnabled) {
        versionStamp = entry->getVersionStamp();
        if (_VERSION_TAG_NULL_CHK) {
          if (delta == NULL) {
            err = versionStamp.processVersionTag(m_region, key, versionTag,
                                                 false);
          } else {
            err =
                versionStamp.processVersionTag(m_region, key, versionTag, true);
          }

          if (err != GF_NOERR) return err;
          versionStamp.setVersions(versionTag);
        }
      }
      if (CacheableToken::isTombstone(meOldValue)) {
        unguardedRemoveActualEntryWithoutCancelTask(key, handler, taskid);
        err = putNoEntry(key, newValue, me, updateCount, destroyTracker,
                         versionTag, &versionStamp);
        meOldValue = NULLPTR;
        isUpdate = false;
      } else if ((err = putForTrackedEntry(key, newValue, entry, entryImpl,
                                           updateCount, versionStamp, delta)) ==
                 GF_NOERR) {
        me = entryImpl;
        oldValue = meOldValue;
        isUpdate = (meOldValue != NULLPTR);
      }
    }
  }
  if (taskid != -1) {
    CacheImpl::expiryTaskManager->cancelTask(taskid);
    if (handler != NULL) delete handler;
  }
  return err;
}

GfErrType MapSegment::invalidate(const CacheableKeyPtr& key,
                                 MapEntryImplPtr& me, CacheablePtr& oldValue,
                                 VersionTagPtr versionTag, bool& isTokenAdded) {
  _GF_GUARD_SEGMENT;
  int status;
  isTokenAdded = false;
  GfErrType err = GF_NOERR;
  MapEntryPtr entry;
  if ((status = m_map->find(key, entry)) != -1) {
    VersionStamp versionStamp;
    if (m_concurrencyChecksEnabled) {
      versionStamp = entry->getVersionStamp();
      if (_VERSION_TAG_NULL_CHK) {
        err = versionStamp.processVersionTag(m_region, key, versionTag, false);
        if (err != GF_NOERR) return err;
        versionStamp.setVersions(versionTag);
      }
    }
    MapEntryImpl* entryImpl = entry->getImplPtr();
    entryImpl->getValueI(oldValue);
    if (CacheableToken::isTombstone(oldValue)) {
      oldValue = NULLPTR;
      return GF_CACHE_ENTRY_NOT_FOUND;
    }
    entryImpl->setValueI(CacheableToken::invalid());
    if (m_concurrencyChecksEnabled) {
      entryImpl->getVersionStamp().setVersions(versionStamp);
    }
    (void)incrementUpdateCount(key, entry);
    if (oldValue != NULLPTR) {
      me = entryImpl;
    }
  } else {
    // create new entry for the key if concurrencychecksEnabled is true
    if (m_concurrencyChecksEnabled) {
      if ((err = putNoEntry(key, CacheableToken::invalid(), me, -1, -1,
                            versionTag)) != GF_NOERR) {
        return err;
      }
      isTokenAdded = true;
    }
    err = GF_CACHE_ENTRY_NOT_FOUND;
  }
  return err;
}

GfErrType MapSegment::removeWhenConcurrencyEnabled(
    const CacheableKeyPtr& key, CacheablePtr& oldValue, MapEntryImplPtr& me,
    int updateCount, VersionTagPtr versionTag, bool afterRemote,
    bool& isEntryFound, long expiryTaskID, TombstoneExpiryHandler* handler,
    bool& expTaskSet) {
  GfErrType err = GF_NOERR;
  int status;
  MapEntryPtr entry;
  VersionStamp versionStamp;
  // If entry found, else return no entry
  if ((status = m_map->find(key, entry)) != -1) {
    isEntryFound = true;
    // If the version tag is null, use the version tag of
    // the existing entry
    versionStamp = entry->getVersionStamp();
    if (_VERSION_TAG_NULL_CHK) {
      CacheableKeyPtr keyPtr;
      entry->getImplPtr()->getKeyI(keyPtr);
      if ((err = entry->getVersionStamp().processVersionTag(
               m_region, keyPtr, versionTag, false)) != GF_NOERR) {
        return err;
      }
      versionStamp.setVersions(versionTag);
    }
    // Get the old value for returning
    MapEntryImpl* entryImpl = entry->getImplPtr();
    entryImpl->getValueI(oldValue);

    if (oldValue != NULLPTR) me = entryImpl;

    if ((err = putForTrackedEntry(key, CacheableToken::tombstone(), entry,
                                  entryImpl, updateCount, versionStamp)) ==
        GF_NOERR) {
      m_tombstoneList->add(m_region, entryImpl, handler, expiryTaskID);
      expTaskSet = true;
    }
    if (CacheableToken::isTombstone(oldValue)) {
      oldValue = NULLPTR;
      if (afterRemote) {
        return GF_NOERR;  // We are here because a remote op succeeded, no need
                          // to throw an error
      } else {
        return GF_CACHE_ENTRY_NOT_FOUND;
      }
    }
  } else {
    // If entry not found than add a tombstone for this entry
    // so that any future updates for this entry are checked for version
    // no entry
    if (_VERSION_TAG_NULL_CHK) {
      MapEntryImplPtr mapEntry;
      putNoEntry(key, CacheableToken::tombstone(), mapEntry, -1, 0, versionTag);
      m_tombstoneList->add(m_region, mapEntry->getImplPtr(), handler,
                           expiryTaskID);
      expTaskSet = true;
    }
    oldValue = NULLPTR;
    isEntryFound = false;
    if (afterRemote) {
      err = GF_NOERR;  // We are here because a remote op succeeded, no need to
                       // throw an error
    } else {
      err = GF_CACHE_ENTRY_NOT_FOUND;
    }
  }
  return err;
}
/**
 * @brief remove entry, setting oldValue.
 */
GfErrType MapSegment::remove(const CacheableKeyPtr& key, CacheablePtr& oldValue,
                             MapEntryImplPtr& me, int updateCount,
                             VersionTagPtr versionTag, bool afterRemote,
                             bool& isEntryFound) {
  //  _GF_GUARD_SEGMENT;
  int status;
  MapEntryPtr entry;
  if (m_concurrencyChecksEnabled) {
    TombstoneExpiryHandler* handler;
    long id = m_tombstoneList->getExpiryTask(&handler);
    bool expTaskSet = false;
    GfErrType err;
    {
      _GF_GUARD_SEGMENT;
      // if (m_concurrencyChecksEnabled)
      err = removeWhenConcurrencyEnabled(key, oldValue, me, updateCount,
                                         versionTag, afterRemote, isEntryFound,
                                         id, handler, expTaskSet);
    }

    // if (m_concurrencyChecksEnabled){
    if (!expTaskSet) {
      CacheImpl::expiryTaskManager->cancelTask(id);
      delete handler;
    }
    return err;
  }

  _GF_GUARD_SEGMENT;
  CacheablePtr value;
  if ((status = m_map->unbind(key, entry)) == -1) {
    // didn't unbind, probably no entry...
    oldValue = NULLPTR;
    volatile int destroyTrackers = *m_numDestroyTrackers;
    if (destroyTrackers > 0) {
      m_destroyedKeys[key] = destroyTrackers + 1;
    }
    return GF_CACHE_ENTRY_NOT_FOUND;
  }

  if (updateCount >= 0 && updateCount != entry->getUpdateCount()) {
    // this is the case when entry has been updated while being tracked
    return GF_CACHE_ENTRY_UPDATED;
  }
  MapEntryImpl* entryImpl = entry->getImplPtr();
  entryImpl->getValueI(oldValue);
  if (CacheableToken::isTombstone(oldValue)) oldValue = NULLPTR;
  if (oldValue != NULLPTR) {
    me = entryImpl;
  }
  return GF_NOERR;
}

bool MapSegment::unguardedRemoveActualEntry(const CacheableKeyPtr& key,
                                            bool cancelTask) {
  MapEntryPtr entry;
  m_tombstoneList->eraseEntryFromTombstoneList(key, m_region, cancelTask);
  if (m_map->unbind(key, entry) == -1) {
    return false;
  }
  return true;
}

bool MapSegment::unguardedRemoveActualEntryWithoutCancelTask(
    const CacheableKeyPtr& key, TombstoneExpiryHandler*& handler,
    long& taskid) {
  MapEntryPtr entry;
  taskid = m_tombstoneList->eraseEntryFromTombstoneListWithoutCancelTask(
      key, m_region, handler);
  if (m_map->unbind(key, entry) == -1) {
    return false;
  }
  return true;
}

bool MapSegment::removeActualEntry(const CacheableKeyPtr& key,
                                   bool cancelTask) {
  _GF_GUARD_SEGMENT;
  return unguardedRemoveActualEntry(key, cancelTask);
}
/**
 * @brief get MapEntry for key. throws NoEntryException if absent.
 */
bool MapSegment::getEntry(const CacheableKeyPtr& key, MapEntryImplPtr& result,
                          CacheablePtr& value) {
  _GF_GUARD_SEGMENT;
  int status;
  MapEntryPtr entry;
  if ((status = m_map->find(key, entry)) == -1) {
    result = NULLPTR;
    value = NULLPTR;
    return false;
  }

  // If the value is a tombstone return not found
  MapEntryImpl* mePtr = entry->getImplPtr();
  mePtr->getValueI(value);
  if (value == NULLPTR || CacheableToken::isTombstone(value)) {
    result = NULLPTR;
    value = NULLPTR;
    return false;
  }
  result = mePtr;
  return true;
}

/**
 * @brief return true if there exists an entry for the key.
 */
bool MapSegment::containsKey(const CacheableKeyPtr& key) {
  _GF_GUARD_SEGMENT;
  MapEntryPtr mePtr;
  int status;
  if ((status = m_map->find(key, mePtr)) == -1) {
    return false;
  }
  // If the value is a tombstone return not found
  CacheablePtr value;
  MapEntryImpl* mePtr1 = mePtr->getImplPtr();
  mePtr1->getValueI(value);
  if (value != NULLPTR && CacheableToken::isTombstone(value)) return false;

  return true;
}

/**
 * @brief return the all the keys in the provided list.
 */
void MapSegment::keys(VectorOfCacheableKey& result) {
  _GF_GUARD_SEGMENT;
  for (CacheableKeyHashMap::iterator iter = m_map->begin();
       iter != m_map->end(); iter++) {
    CacheablePtr valuePtr;
    (*iter).int_id_->getImplPtr()->getValueI(valuePtr);
    if (!CacheableToken::isTombstone(valuePtr)) {
      result.push_back((*iter).ext_id_);
    }
  }
}

/**
 * @brief return all the entries in the provided list.
 */
void MapSegment::entries(VectorOfRegionEntry& result) {
  _GF_GUARD_SEGMENT;
  // printf("total_size)=%u, current_size=%u\n",
  //  m_map->total_size(), m_map->current_size());
  for (CacheableKeyHashMap::iterator iter = m_map->begin();
       iter != m_map->end(); iter++) {
    CacheableKeyPtr keyPtr;
    CacheablePtr valuePtr;
    MapEntryImpl* me = ((*iter).int_id_)->getImplPtr();
    me->getValueI(valuePtr);
    if (valuePtr != NULLPTR && !CacheableToken::isTombstone(valuePtr)) {
      if (CacheableToken::isInvalid(valuePtr)) {
        valuePtr = NULLPTR;
      }
      me->getKeyI(keyPtr);
      RegionEntryPtr rePtr = m_region->createRegionEntry(keyPtr, valuePtr);
      result.push_back(rePtr);
    }
  }
}

/**
 * @brief return all values in the provided list.
 */
void MapSegment::values(VectorOfCacheable& result) {
  _GF_GUARD_SEGMENT;
  for (CacheableKeyHashMap::iterator iter = m_map->begin();
       iter != m_map->end(); iter++) {
    CacheablePtr valuePtr;
    CacheableKeyPtr keyPtr;
    MapEntryPtr entry;
    int status;

    keyPtr = (*iter).ext_id_;
    (*iter).int_id_->getValue(valuePtr);
    status = m_map->find(keyPtr, entry);

    if (status != -1) {
      MapEntryImpl* entryImpl = entry->getImplPtr();
      if (valuePtr != NULLPTR && !CacheableToken::isInvalid(valuePtr) &&
          !CacheableToken::isDestroyed(valuePtr) &&
          !CacheableToken::isTombstone(valuePtr)) {
        if (CacheableToken::isOverflowed(valuePtr)) {  // get Value from disc.
          valuePtr = getFromDisc(keyPtr, entryImpl);
          entryImpl->setValueI(valuePtr);
        }
        result.push_back(valuePtr);
      }
    }
  }
}

// This function will not get called if concurrency checks are enabled. The
// versioning
// changes takes care of the version and no need for tracking the entry
int MapSegment::addTrackerForEntry(const CacheableKeyPtr& key,
                                   CacheablePtr& oldValue, bool addIfAbsent,
                                   bool failIfPresent, bool incUpdateCount) {
  if (m_concurrencyChecksEnabled) return -1;
  _GF_GUARD_SEGMENT;
  MapEntryPtr entry;
  MapEntryPtr newEntry;
  int status;
  if ((status = m_map->find(key, entry)) == -1) {
    oldValue = NULLPTR;
    if (addIfAbsent) {
      MapEntryImplPtr entryImpl;
      // add a new entry with value as destroyed
      m_entryFactory->newMapEntry(key, entryImpl);
      entryImpl->setValueI(CacheableToken::destroyed());
      entry = entryImpl;
      newEntry = entryImpl;
    } else {
      // return -1 without adding an entry
      return -1;
    }
  } else {
    entry->getValue(oldValue);
    if (failIfPresent) {
      // return -1 without adding an entry; the callee should check on
      // oldValue to distinguish this case from "addIfAbsent==false" case
      return -1;
    }
  }
  int updateCount;
  if (incUpdateCount) {
    (void)entry->addTracker(newEntry);
    updateCount = entry->incrementUpdateCount(newEntry);
  } else {
    updateCount = entry->addTracker(newEntry);
  }
  if (newEntry != NULLPTR) {
    if (status == -1) {
      m_map->bind(key, newEntry);
    } else {
      m_map->rebind(key, newEntry);
    }
  }
  return updateCount;
}

// This function will not get called if concurrency checks are enabled. The
// versioning
// changes takes care of the version and no need for tracking the entry
void MapSegment::removeTrackerForEntry(const CacheableKeyPtr& key) {
  if (m_concurrencyChecksEnabled) return;
  _GF_GUARD_SEGMENT;
  MapEntryPtr entry;
  int status;
  if ((status = m_map->find(key, entry)) != -1) {
    removeTrackerForEntry(key, entry, NULL);
  }
}

// This function will not get called if concurrency checks are enabled. The
// versioning
// changes takes care of the version and no need for tracking the entry
void MapSegment::addTrackerForAllEntries(
    MapOfUpdateCounters& updateCounterMap) {
  if (m_concurrencyChecksEnabled) return;
  _GF_GUARD_SEGMENT;
  MapEntryPtr newEntry;
  CacheableKeyPtr key;
  for (CacheableKeyHashMap::iterator iter = m_map->begin();
       iter != m_map->end(); ++iter) {
    (*iter).int_id_->getKey(key);
    int updateCount = (*iter).int_id_->addTracker(newEntry);
    if (newEntry != NULLPTR) {
      m_map->rebind(key, newEntry);
    }
    updateCounterMap.insert(std::make_pair(key, updateCount));
  }
}

// This function will not get called if concurrency checks are enabled. The
// versioning
// changes takes care of the version and no need for tracking the entry
void MapSegment::removeDestroyTracking() {
  if (m_concurrencyChecksEnabled) return;
  _GF_GUARD_SEGMENT;
  m_destroyedKeys.clear();
}

/**
 * @brief replace the existing hash map with one that is wider
 *   to reduce collision chains.
 */
void MapSegment::rehash() {  // Only called from put, segment must already be
                             // locked...

  uint32_t newMapSize = TableOfPrimes::getPrime(++m_primeIndex);
  LOGFINER("Rehashing MapSegment to size %d.", newMapSize);
  CacheableKeyHashMap* newMap = new CacheableKeyHashMap();
  newMap->open(newMapSize);

  // copy all entries into newMap..
  for (CacheableKeyHashMap::iterator iter = m_map->begin();
       iter != m_map->end(); ++iter) {
    newMap->bind((*iter).ext_id_, (*iter).int_id_);
  }

  // plug newMap into real member.
  CacheableKeyHashMap* oldMap = m_map;
  m_map = newMap;
  // clean up the old map.
  delete oldMap;
  m_rehashCount++;
}

CacheablePtr MapSegment::getFromDisc(CacheableKeyPtr key,
                                     MapEntryImpl* entryImpl) {
  LocalRegion* lregion = static_cast<LocalRegion*>(m_region);
  EntriesMap* em = lregion->getEntryMap();
  return em->getFromDisk(key, entryImpl);
}
GfErrType MapSegment::putForTrackedEntry(
    const CacheableKeyPtr& key, const CacheablePtr& newValue,
    MapEntryPtr& entry, MapEntryImpl* entryImpl, int updateCount,
    VersionStamp& versionStamp, DataInput* delta) {
  if (updateCount < 0 || m_concurrencyChecksEnabled) {
    // for a non-tracked put (e.g. from notification) go ahead with the
    // create/update and increment the update counter
    ThinClientRegion* tcRegion = dynamic_cast<ThinClientRegion*>(m_region);
    ThinClientPoolDM* m_poolDM = NULL;
    if (tcRegion) {
      m_poolDM = dynamic_cast<ThinClientPoolDM*>(tcRegion->getDistMgr());
    }
    if (delta != NULL) {
      CacheablePtr oldValue;
      entryImpl->getValueI(oldValue);
      if (oldValue == NULLPTR || CacheableToken::isDestroyed(oldValue) ||
          CacheableToken::isInvalid(oldValue) ||
          CacheableToken::isTombstone(oldValue)) {
        if (m_poolDM) m_poolDM->updateNotificationStats(false, 0);
        return GF_INVALID_DELTA;
      } else if (CacheableToken::isOverflowed(
                     oldValue)) {  // get Value from disc.
        oldValue = getFromDisc(key, entryImpl);
        if (oldValue == NULLPTR) {
          if (m_poolDM) m_poolDM->updateNotificationStats(false, 0);
          return GF_INVALID_DELTA;
        }
      }
      DeltaPtr valueWithDelta(dynCast<DeltaPtr>(oldValue));
      CacheablePtr& newValue1 = const_cast<CacheablePtr&>(newValue);
      try {
        if (m_region->getAttributes()->getCloningEnabled()) {
          DeltaPtr tempVal = valueWithDelta->clone();
          ACE_Time_Value currTimeBefore = ACE_OS::gettimeofday();
          tempVal->fromDelta(*delta);
          if (m_poolDM) {
            m_poolDM->updateNotificationStats(
                true,
                ((ACE_OS::gettimeofday() - currTimeBefore).msec()) * 1000000);
          }
          newValue1 = tempVal;
          entryImpl->setValueI(newValue1);
        } else {
          ACE_Time_Value currTimeBefore = ACE_OS::gettimeofday();
          valueWithDelta->fromDelta(*delta);
          newValue1 = valueWithDelta;
          if (m_poolDM) {
            m_poolDM->updateNotificationStats(
                true,
                ((ACE_OS::gettimeofday() - currTimeBefore).msec()) * 1000000);
          }
          entryImpl->setValueI(valueWithDelta);
        }
      } catch (InvalidDeltaException&) {
        return GF_INVALID_DELTA;
      }
    } else {
      entryImpl->setValueI(newValue);
    }
    if (m_concurrencyChecksEnabled) {
      // erase if the entry is in tombstone
      m_tombstoneList->eraseEntryFromTombstoneList(key, m_region);
      entryImpl->getVersionStamp().setVersions(versionStamp);
    }
    (void)incrementUpdateCount(key, entry);
    return GF_NOERR;
  } else if (updateCount == entry->getUpdateCount()) {
    // good case; go ahead with the create/update
    entryImpl->setValueI(newValue);
    removeTrackerForEntry(key, entry, entryImpl);
    return GF_NOERR;
  } else {
    // entry updated while tracking was being done
    // abort the create/update and do not change the oldValue or MapEntry
    removeTrackerForEntry(key, entry, entryImpl);
    return GF_CACHE_ENTRY_UPDATED;
  }
}
void MapSegment::reapTombstones(std::map<uint16_t, int64_t>& gcVersions) {
  _GF_GUARD_SEGMENT;
  m_tombstoneList->reapTombstones(gcVersions);
}
void MapSegment::reapTombstones(CacheableHashSetPtr removedKeys) {
  _GF_GUARD_SEGMENT;
  m_tombstoneList->reapTombstones(removedKeys);
}

GfErrType MapSegment::isTombstone(CacheableKeyPtr key, MapEntryImplPtr& me,
                                  bool& result) {
  CacheablePtr value;
  MapEntryPtr entry;
  MapEntryImpl* mePtr;
  if (m_map->find(key, entry) == -1) {
    result = false;
    return GF_NOERR;
  }
  mePtr = entry->getImplPtr();

  /* adongre  - Coverity II
   * CID 29204: Dereference before null check (REVERSE_INULL)
   * Dereferencing pointer "mePtr". [show details]
   * Fix : Aded a check for null ptr
   */

  if (mePtr == (MapEntryImpl*)0) {
    result = false;
    return GF_NOERR;
  }

  mePtr->getValueI(value);
  result = mePtr;

  if (value == NULLPTR || value.ptr() == NULL) {
    result = false;
    return GF_NOERR;
  }

  if (CacheableToken::isTombstone(value)) {
    if (m_tombstoneList->getEntryFromTombstoneList(key)) {
      MapEntryPtr entry;
      if (m_map->find(key, entry) != -1) {
        MapEntryImpl* mePtr = entry->getImplPtr();
        me = mePtr;
      }
      result = true;
      return GF_NOERR;
    } else {
      LOGFINER("1 result= false return GF_CACHE_ILLEGAL_STATE_EXCEPTION");
      result = false;
      return GF_CACHE_ILLEGAL_STATE_EXCEPTION;
    }

  } else {
    if (m_tombstoneList->getEntryFromTombstoneList(key)) {
      LOGFINER(" 2 result= false return GF_CACHE_ILLEGAL_STATE_EXCEPTION");
      result = false;
      return GF_CACHE_ILLEGAL_STATE_EXCEPTION;
    } else {
      result = false;
      return GF_NOERR;
    }
  }
}
