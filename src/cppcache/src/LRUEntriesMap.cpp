/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "LRUEntriesMap.hpp"
#include "LRUList.cpp"
#include "ExpiryTaskManager.hpp"
#include "MapSegment.hpp"
#include "CacheImpl.hpp"

namespace gemfire {
/**
 * @brief LRUAction for testing map outside of a region....
 */
class CPPCACHE_EXPORT TestMapAction : public virtual LRUAction {
 private:
  EntriesMap* m_eMap;

 public:
  explicit TestMapAction(EntriesMap* eMap) : m_eMap(eMap) { m_destroys = true; }

  virtual ~TestMapAction() {}

  virtual bool evict(const MapEntryImplPtr& mePtr) {
    CacheableKeyPtr keyPtr;
    mePtr->getKeyI(keyPtr);
    /** @TODO try catch.... return true or false. */
    CacheablePtr cPtr;  // old value.
    MapEntryImplPtr me;
    VersionTagPtr versionTag;
    return (m_eMap->remove(keyPtr, cPtr, me, 0, versionTag, false) == GF_NOERR);
  }

  virtual LRUAction::Action getType() { return LRUAction::LOCAL_DESTROY; }
  friend class LRUAction;
};
}  // namespace gemfire

using namespace gemfire;

LRUEntriesMap::LRUEntriesMap(EntryFactory* entryFactory, RegionInternal* region,
                             const LRUAction::Action& lruAction,
                             const uint32_t limit,
                             bool concurrencyChecksEnabled,
                             const uint8_t concurrency, bool heapLRUEnabled)
    : ConcurrentEntriesMap(entryFactory, concurrencyChecksEnabled, region,
                           concurrency),
      m_lruList(),
      m_limit(limit),
      m_pmPtr(NULLPTR),
      m_validEntries(0),
      m_heapLRUEnabled(heapLRUEnabled) {
  m_currentMapSize = 0;
  m_action = NULL;
  m_evictionControllerPtr = NULL;
  // translate action type to an instance.
  if (region == NULL) {
    m_action = new TestMapAction(this);
  } else {
    m_action = LRUAction::newLRUAction(lruAction, region, this);
    m_name = region->getName();
    CacheImpl* cImpl = region->getCacheImpl();
    if (cImpl != NULL) {
      m_evictionControllerPtr = cImpl->getEvictionController();
      if (m_evictionControllerPtr != NULL) {
        m_evictionControllerPtr->registerRegion(m_name);
        LOGINFO("Heap LRU eviction controller registered region %s",
                m_name.c_str());
      }
    }
  }
}

void LRUEntriesMap::close() {
  if (m_evictionControllerPtr != NULL) {
    m_evictionControllerPtr->updateRegionHeapInfo((-1 * (m_currentMapSize)));
    m_evictionControllerPtr->deregisterRegion(m_name);
  }
  ConcurrentEntriesMap::close();
}

void LRUEntriesMap::clear() {
  updateMapSize((-1 * (m_currentMapSize)));
  ConcurrentEntriesMap::clear();
}

LRUEntriesMap::~LRUEntriesMap() { delete m_action; }

/**
 * @brief put an item in the map... if it is a new entry, then the LRU may
 * need to be consulted.
 * If LRUAction is LRUInvalidateAction, then increment if old value was absent.
 * If LRUAction is one of Destroys, then increment if old Entry was absent.
 */
GfErrType LRUEntriesMap::create(const CacheableKeyPtr& key,
                                const CacheablePtr& newValue,
                                MapEntryImplPtr& me, CacheablePtr& oldValue,
                                int updateCount, int destroyTracker,
                                VersionTagPtr versionTag) {
  MapSegment* segmentRPtr = segmentFor(key);
  GfErrType err = GF_NOERR;
  {  // SYNCHRONIZE_SEGMENT(segmentRPtr);
    MapEntryImplPtr mePtr;
    if ((err = segmentRPtr->create(key, newValue, me, oldValue, updateCount,
                                   destroyTracker, versionTag)) != GF_NOERR) {
      return err;
    }
    // TODO:  can newValue ever be a token ??
    if (!CacheableToken::isToken(newValue)) {
      m_validEntries++;
    }
    //  oldValue can be an invalid token when "createIfInvalid" is true
    if (!CacheableToken::isInvalid(oldValue)) {
      m_size++;
    }
    CacheablePtr tmpValue;
    segmentRPtr->getEntry(key, mePtr, tmpValue);
    if (mePtr == NULLPTR) {
      return err;
    }
    m_lruList.appendEntry(mePtr);
    me = mePtr;
  }
  if (m_evictionControllerPtr != NULL) {
    int64_t newSize =
        static_cast<int64_t>(Utils::checkAndGetObjectSize(newValue));
    newSize += static_cast<int64_t>(Utils::checkAndGetObjectSize(key));
    if (oldValue != NULLPTR) {
      newSize -= static_cast<int64_t>(oldValue->objectSize());
    } else {
      newSize -= static_cast<int64_t>(sizeof(void*));
    }
    updateMapSize(newSize);
  }
  err = processLRU();
  return err;
}

GfErrType LRUEntriesMap::processLRU() {
  GfErrType canEvict = GF_NOERR;
  while (canEvict == GF_NOERR && mustEvict()) {
    canEvict = evictionHelper();
  }
  return canEvict;
}

GfErrType LRUEntriesMap::evictionHelper() {
  GfErrType err = GF_NOERR;
  //  ACE_Guard< ACE_Recursive_Thread_Mutex > guard( m_mutex );
  MapEntryImplPtr lruEntryPtr;
  m_lruList.getLRUEntry(lruEntryPtr);
  if (lruEntryPtr == NULLPTR) {
    err = GF_ENOENT;
    return err;
  }
  bool IsEvictDone = m_action->evict(lruEntryPtr);
  if (m_action->overflows() && IsEvictDone) {
    --m_validEntries;
    lruEntryPtr->getLRUProperties().setEvicted();
  }
  if (!IsEvictDone) {
    err = GF_DISKFULL;
    return err;
  }
  return err;
}

void LRUEntriesMap::processLRU(int32_t numEntriesToEvict) {
  int32_t evicted = 0;
  for (int32_t i = 0; i < numEntriesToEvict; i++) {
    if (m_validEntries.value() > 0 && static_cast<int32_t>(size()) > 0) {
      if (evictionHelper() == GF_NOERR) {
        evicted++;
      }
    } else {
      break;
    }
  }
}

GfErrType LRUEntriesMap::invalidate(const CacheableKeyPtr& key,
                                    MapEntryImplPtr& me, CacheablePtr& oldValue,
                                    VersionTagPtr versionTag) {
  int64_t newSize = 0;
  MapSegment* segmentRPtr = segmentFor(key);
  bool isTokenAdded = false;
  GfErrType err =
      segmentRPtr->invalidate(key, me, oldValue, versionTag, isTokenAdded);
  if (isTokenAdded) {
    ++m_size;
  }
  if (err != GF_NOERR) {
    return err;
  }
  bool isOldValueToken = CacheableToken::isToken(oldValue);
  //  get the old value first which is required for heapLRU
  // calculation and for listeners; note even though there is a race
  // here between get and destroy, it will not harm if we get a slightly
  // later value
  // TODO: assess any other effects of this race
  if (CacheableToken::isOverflowed(oldValue)) {
    ACE_Guard<MapSegment> _guard(*segmentRPtr);
    void* persistenceInfo = me->getLRUProperties().getPersistenceInfo();
    //  get the old value first which is required for heapLRU
    // calculation and for listeners; note even though there is a race
    // here between get and destroy, it will not harm if we get a slightly
    // older value
    // TODO: there is also a race between segment remove and destroy here
    // need to assess the effect of this; also assess the affect of above
    // mentioned race
    oldValue = m_pmPtr->read(key, persistenceInfo);
    if (oldValue != NULLPTR) {
      m_pmPtr->destroy(key, persistenceInfo);
    }
  }
  if (!isOldValueToken) {
    --m_validEntries;
    me->getLRUProperties().setEvicted();
    newSize = CacheableToken::invalid()->objectSize();
    if (oldValue != NULLPTR) {
      newSize -= oldValue->objectSize();
    } else {
      newSize -= sizeof(void*);
    }
    if (m_evictionControllerPtr != NULL) {
      if (newSize != 0) {
        updateMapSize(newSize);
      }
    }
  }
  return err;
}

GfErrType LRUEntriesMap::put(const CacheableKeyPtr& key,
                             const CacheablePtr& newValue, MapEntryImplPtr& me,
                             CacheablePtr& oldValue, int updateCount,
                             int destroyTracker, VersionTagPtr versionTag,
                             bool& isUpdate, DataInput* delta) {
  MapSegment* segmentRPtr = segmentFor(key);
  GF_D_ASSERT(segmentRPtr != NULL);

  GfErrType err = GF_NOERR;
  bool segmentLocked = false;
  {
    if (m_action != NULL &&
        m_action->getType() == LRUAction::OVERFLOW_TO_DISK) {
      segmentRPtr->acquire();
      segmentLocked = true;
    }
    MapEntryImplPtr mePtr;
    if ((err = segmentRPtr->put(key, newValue, me, oldValue, updateCount,
                                destroyTracker, isUpdate, versionTag, delta)) !=
        GF_NOERR) {
      if (segmentLocked == true) segmentRPtr->release();
      return err;
    }

    bool isOldValueToken = CacheableToken::isToken(oldValue);
    // TODO:  need tests for checking that oldValue is returned
    // correctly to listeners for overflow -- this is for all operations
    // put, invalidate, destroy
    // TODO:  need tests for checking if the overflowed entry is
    // destroyed *from disk* in put
    //  existing overflowed entry should be returned correctly in
    // put and removed from disk
    // TODO: need to verify if segment mutex lock is enough here
    // TODO: This whole class needs to be rethought and reworked for locking
    // and concurrency issues. The basic issue is two concurrent operations
    // on the same key regardless of whether we need to go to the persistence
    // manager or not. So all operations on the same key should be protected
    // unless very careful thought has gone into it.
    if (CacheableToken::isOverflowed(oldValue)) {
      if (!segmentLocked) {
        segmentRPtr->release();
        segmentLocked = true;
      }
      void* persistenceInfo = me->getLRUProperties().getPersistenceInfo();
      oldValue = m_pmPtr->read(key, persistenceInfo);
      if (oldValue != NULLPTR) {
        m_pmPtr->destroy(key, persistenceInfo);
      }
    }
    // SpinLock& lock = segmentRPtr->getSpinLock();
    // SpinLockGuard mapGuard( lock );

    // TODO:  when can newValue be a token ??
    if (CacheableToken::isToken(newValue) && !isOldValueToken) {
      --m_validEntries;
    }
    if (!CacheableToken::isToken(newValue) && isOldValueToken) {
      ++m_validEntries;
    }

    // Add new entry to LRU list
    if (isUpdate == false) {
      ++m_size;
      ++m_validEntries;
      CacheablePtr tmpValue;
      segmentRPtr->getEntry(key, mePtr, tmpValue);
      // mePtr cannot be null, we just put it...
      // must convert to an LRUMapEntryImplPtr...
      GF_D_ASSERT(mePtr != NULLPTR);
      m_lruList.appendEntry(mePtr);
      me = mePtr;
    } else {
      if (!CacheableToken::isToken(newValue) && isOldValueToken) {
        CacheablePtr tmpValue;
        segmentRPtr->getEntry(key, mePtr, tmpValue);
        mePtr->getLRUProperties().clearEvicted();
        m_lruList.appendEntry(MapEntryImplPtr(mePtr->getImplPtr()));
        me = mePtr;
      }
    }
  }
  if (m_evictionControllerPtr != NULL) {
    int64_t newSize =
        static_cast<int64_t>(Utils::checkAndGetObjectSize(newValue));
    /*
    if (newSize == 0) {
      LOGWARN("Object size for class ID %d should not be zero when HeapLRU is
    enabled", newValue->classId());
      LOGDEBUG("Type ID is %d for the object returning zero HeapLRU size",
    newValue->typeId());
    }
    */
    if (isUpdate == false) {
      newSize += static_cast<int64_t>(Utils::checkAndGetObjectSize(key));
    } else {
      if (oldValue != NULLPTR) {
        newSize -= static_cast<int64_t>(oldValue->objectSize());
      } else {
        newSize -= static_cast<int64_t>(sizeof(void*));
      }
    }
    updateMapSize(newSize);
  }

  err = processLRU();

  if (segmentLocked) {
    segmentRPtr->release();
  }
  return err;
}

/**
 * @brief Get the value from an entry, if the entry exists it will be marked
 * as recently used. Note, getEntry, entries, and values do not mark entries
 * as recently used.
 */
bool LRUEntriesMap::get(const CacheableKeyPtr& key, CacheablePtr& returnPtr,
                        MapEntryImplPtr& me) {
  char logkey[2048];
  key->logString(logkey, 2040);
  // LOGDEBUG("key = %s", logkey);
  bool doProcessLRU = false;
  MapSegment* segmentRPtr = segmentFor(key);
  bool segmentLocked = false;
  if (m_action != NULL && m_action->getType() == LRUAction::OVERFLOW_TO_DISK) {
    segmentRPtr->acquire();
    segmentLocked = true;
  }
  {
    returnPtr = NULLPTR;
    MapEntryImplPtr mePtr;
    if (false == segmentRPtr->getEntry(key, mePtr, returnPtr)) {
      if (segmentLocked == true) segmentRPtr->release();
      return false;
    }
    // segmentRPtr->get(key, returnPtr, mePtr);
    MapEntryImplPtr nodeToMark = mePtr;
    LRUEntryProperties& lruProps = nodeToMark->getLRUProperties();
    if (returnPtr != NULLPTR && CacheableToken::isOverflowed(returnPtr)) {
      void* persistenceInfo = lruProps.getPersistenceInfo();
      CacheablePtr tmpObj;
      try {
        tmpObj = m_pmPtr->read(key, persistenceInfo);
      } catch (Exception& ex) {
        LOGERROR("read on the persistence layer failed - %s", ex.getMessage());
        if (segmentLocked == true) segmentRPtr->release();
        return false;
      }
      (m_region->getRegionStats())->incRetrieves();
      (m_region->getCacheImpl())->m_cacheStats->incRetrieves();

      returnPtr = tmpObj;

      CacheablePtr oldValue;
      bool isUpdate;
      VersionTagPtr versionTag;
      if (GF_NOERR ==
          segmentRPtr->put(key, tmpObj, mePtr, oldValue, 0, 0, isUpdate,
                           versionTag, NULL)) {
        // m_entriesRetrieved++;
        ++m_validEntries;
        lruProps.clearEvicted();
        m_lruList.appendEntry(nodeToMark);
      }
      doProcessLRU = true;
      if (m_evictionControllerPtr != NULL) {
        int64_t newSize = 0;
        if (tmpObj != NULLPTR) {
          newSize += static_cast<int64_t>(
              tmpObj->objectSize() - CacheableToken::invalid()->objectSize());
        } else {
          newSize += sizeof(void*);
        }
        updateMapSize(newSize);
      }
    }
    me = mePtr;
    // lruProps.clearEvicted();
    lruProps.setRecentlyUsed();
    if (doProcessLRU) {
      GfErrType IsProcessLru = processLRU();
      if ((IsProcessLru != GF_NOERR)) {
        if (segmentLocked) {
          segmentRPtr->release();
        }
        return false;
      }
    }
    if (segmentLocked) {
      segmentRPtr->release();
    }
    return true;
  }
}

GfErrType LRUEntriesMap::remove(const CacheableKeyPtr& key,
                                CacheablePtr& result, MapEntryImplPtr& me,
                                int updateCount, VersionTagPtr versionTag,
                                bool afterRemote) {
  MapSegment* segmentRPtr = segmentFor(key);
  bool isEntryFound = true;
  GfErrType err;
  if ((err = segmentRPtr->remove(key, result, me, updateCount, versionTag,
                                 afterRemote, isEntryFound)) == GF_NOERR) {
    // ACE_Guard<MapSegment> _guard(*segmentRPtr);
    if (result != NULLPTR && me != NULLPTR) {
      LRUEntryProperties& lruProps = me->getLRUProperties();
      lruProps.setEvicted();
      if (isEntryFound) --m_size;
      if (!CacheableToken::isToken(result)) {
        --m_validEntries;
      }
      if (CacheableToken::isOverflowed(result)) {
        ACE_Guard<MapSegment> _guard(*segmentRPtr);
        void* persistenceInfo = lruProps.getPersistenceInfo();
        //  get the old value first which is required for heapLRU
        // calculation and for listeners; note even though there is a race
        // here between get and destroy, it will not harm if we get a slightly
        // older value
        // TODO: there is also a race between segment remove and destroy here
        // need to assess the effect of this; also assess the affect of above
        // mentioned race
        result = m_pmPtr->read(key, persistenceInfo);
        if (result != NULLPTR) {
          m_pmPtr->destroy(key, persistenceInfo);
        }
      }
      if (m_evictionControllerPtr != NULL) {
        int64_t sizeToRemove = static_cast<int64_t>(key->objectSize());
        sizeToRemove += static_cast<int64_t>(result->objectSize());
        updateMapSize((-1 * sizeToRemove));
      }
    }
  }
  return err;
}

void LRUEntriesMap::updateMapSize(int64_t size) {
  // TODO: check and remove null check since this has already been done
  // by all the callers
  if (m_evictionControllerPtr != NULL) {
    {
      SpinLockGuard __guard(m_mapInfoLock);
      m_currentMapSize += size;
    }
    m_evictionControllerPtr->updateRegionHeapInfo(size);
  }
}

CacheablePtr LRUEntriesMap::getFromDisk(const CacheableKeyPtr& key,
                                        MapEntryImpl* me) const {
  void* persistenceInfo = me->getLRUProperties().getPersistenceInfo();
  CacheablePtr tmpObj;
  try {
    LOGDEBUG("Reading value from persistence layer for key: %s",
             key->toString()->asChar());
    tmpObj = m_pmPtr->read(key, persistenceInfo);
  } catch (Exception& ex) {
    LOGERROR("read on the persistence layer failed - %s", ex.getMessage());
    return NULLPTR;
  }
  return tmpObj;
}
