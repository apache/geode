/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "TombstoneList.hpp"
#include "TombstoneExpiryHandler.hpp"
#include "MapSegment.hpp"
#include <unordered_map>

using namespace gemfire;

#define SIZEOF_PTR (sizeof(void*))
#define SIZEOF_SHAREDPTR (SIZEOF_PTR + 4)
// 3 variables in expiry handler, two variables for ace_reactor expiry, one
// pointer to expiry handle
#define SIZEOF_EXPIRYHANDLER ((SIZEOF_PTR * 5) + 4)
#define SIZEOF_TOMBSTONEENTRY (SIZEOF_PTR + 8 + 8)
// one shared ptr for map entry, one sharedPtr for tombstone entry, one
// sharedptr for key, one shared ptr for tombstone value,
// one ptr for tombstone list, one ptr for mapsegment, one tombstone entry
#define SIZEOF_TOMBSTONELISTENTRY \
  (SIZEOF_SHAREDPTR * 4 + SIZEOF_PTR * 2 + SIZEOF_TOMBSTONEENTRY)
#define SIZEOF_TOMBSTONEOVERHEAD \
  (SIZEOF_EXPIRYHANDLER + SIZEOF_TOMBSTONELISTENTRY)

long TombstoneList::getExpiryTask(TombstoneExpiryHandler** handler) {
  // This function is not guarded as all functions of this class are called from
  // MapSegment
  // read TombstoneTImeout from systemProperties.
  uint32_t duration =
      DistributedSystem::getSystemProperties()->tombstoneTimeoutInMSec() / 1000;
  ACE_Time_Value currTime(ACE_OS::gettimeofday());
  TombstoneEntryPtr tombstoneEntryPtr = TombstoneEntryPtr(
      new TombstoneEntry(NULL, static_cast<int64_t>(currTime.get_msec())));
  *handler = new TombstoneExpiryHandler(tombstoneEntryPtr, this, duration);
  tombstoneEntryPtr->setHandler(*handler);
  long id =
      CacheImpl::expiryTaskManager->scheduleExpiryTask(*handler, duration, 0);
  return id;
}

void TombstoneList::add(RegionInternal* rptr, MapEntryImpl* entry,
                        TombstoneExpiryHandler* handler, long taskid) {
  // This function is not guarded as all functions of this class are called from
  // MapSegment
  // read TombstoneTImeout from systemProperties.
  // uint32_t duration =
  // DistributedSystem::getSystemProperties()->tombstoneTimeoutInMSec()/1000;
  ACE_Time_Value currTime(ACE_OS::gettimeofday());
  TombstoneEntryPtr tombstoneEntryPtr = TombstoneEntryPtr(
      new TombstoneEntry(entry, static_cast<int64_t>(currTime.get_msec())));
  // TombstoneExpiryHandler* handler = new
  // TombstoneExpiryHandler(tombstoneEntryPtr, this, duration);
  handler->setTombstoneEntry(tombstoneEntryPtr);
  tombstoneEntryPtr->setHandler(handler);
  // long id = CacheImpl::expiryTaskManager->scheduleExpiryTask(
  //  handler, duration, 0);
  CacheableKeyPtr key;
  entry->getKeyI(key);
  /*if (Log::finestEnabled()) {
    LOGFINEST("tombstone expiry for key [%s], task id = %d, "
        "duration = %d",
        Utils::getCacheableKeyString(key)->asChar(), id, duration);
  }*/
  tombstoneEntryPtr->setExpiryTaskId(taskid);
  if (!m_tombstoneMap.insert(key, tombstoneEntryPtr)) {
    m_tombstoneMap[key] = tombstoneEntryPtr;
  }
  rptr->getCacheImpl()->m_cacheStats->incTombstoneCount();
  int32_t tombstonesize = key->objectSize() + SIZEOF_TOMBSTONEOVERHEAD;
  rptr->getCacheImpl()->m_cacheStats->incTombstoneSize(tombstonesize);
}

// Reaps the tombstones which have been gc'ed on server.
// A map that has identifier for ClientProxyMembershipID as key
// and server version of the tombstone with highest version as the
// value is passed as paramter
void TombstoneList::reapTombstones(std::map<uint16_t, int64_t>& gcVersions) {
  // This function is not guarded as all functions of this class are called from
  // MapSegment
  std::unordered_set<CacheableKeyPtr> tobeDeleted;
  for (HashMapT<CacheableKeyPtr, TombstoneEntryPtr>::Iterator queIter =
           m_tombstoneMap.begin();
       queIter != m_tombstoneMap.end(); ++queIter) {
    std::map<uint16_t, int64_t>::iterator mapIter = gcVersions.find(
        queIter.second()->getEntry()->getVersionStamp().getMemberId());

    if (mapIter == gcVersions.end()) {
      continue;
    }
    int64_t version = (*mapIter).second;
    if (version >=
        queIter.second()->getEntry()->getVersionStamp().getRegionVersion()) {
      tobeDeleted.insert(queIter.first());
    }
  }
  for (std::unordered_set<CacheableKeyPtr>::iterator itr = tobeDeleted.begin();
       itr != tobeDeleted.end(); itr++) {
    unguardedRemoveEntryFromMapSegment(*itr);
  }
}

// Reaps the tombstones whose keys are specified in the hash set .
void TombstoneList::reapTombstones(CacheableHashSetPtr removedKeys) {
  // This function is not guarded as all functions of this class are called from
  // MapSegment
  for (HashSetT<CacheableKeyPtr>::Iterator queIter = removedKeys->begin();
       queIter != removedKeys->end(); ++queIter) {
    unguardedRemoveEntryFromMapSegment(*queIter);
  }
}
// Call this when the lock of MapSegment has not been taken
void TombstoneList::removeEntryFromMapSegment(CacheableKeyPtr key) {
  m_mapSegment->removeActualEntry(key, false);
}

// Call this when the lock of MapSegment has already been taken
void TombstoneList::unguardedRemoveEntryFromMapSegment(CacheableKeyPtr key) {
  m_mapSegment->unguardedRemoveActualEntry(key);
}

void TombstoneList::eraseEntryFromTombstoneList(CacheableKeyPtr key,
                                                RegionInternal* region,
                                                bool cancelTask) {
  // This function is not guarded as all functions of this class are called from
  // MapSegment
  bool exists = (key != NULLPTR) ? m_tombstoneMap.contains(key) : false;
  if (cancelTask && exists) {
    CacheImpl::expiryTaskManager->cancelTask(
        static_cast<long>(m_tombstoneMap[key]->getExpiryTaskId()));
    delete m_tombstoneMap[key]->getHandler();
  }
  if (exists) {
    region->getCacheImpl()->m_cacheStats->decTombstoneCount();
    int32_t tombstonesize = key->objectSize() + SIZEOF_TOMBSTONEOVERHEAD;
    region->getCacheImpl()->m_cacheStats->decTombstoneSize(tombstonesize);
    m_tombstoneMap.erase(key);
  }
}

long TombstoneList::eraseEntryFromTombstoneListWithoutCancelTask(
    CacheableKeyPtr key, RegionInternal* region,
    TombstoneExpiryHandler*& handler) {
  // This function is not guarded as all functions of this class are called from
  // MapSegment
  bool exists = (key != NULLPTR) ? m_tombstoneMap.contains(key) : false;
  long taskid = -1;
  if (exists) {
    taskid = static_cast<long>(m_tombstoneMap[key]->getExpiryTaskId());
    handler = m_tombstoneMap[key]->getHandler();
    region->getCacheImpl()->m_cacheStats->decTombstoneCount();
    int32_t tombstonesize = key->objectSize() + SIZEOF_TOMBSTONEOVERHEAD;
    region->getCacheImpl()->m_cacheStats->decTombstoneSize(tombstonesize);
    m_tombstoneMap.erase(key);
  }
  return taskid;
}

bool TombstoneList::getEntryFromTombstoneList(CacheableKeyPtr key) {
  // This function is not guarded as all functions of this class are called from
  // MapSegment
  return m_tombstoneMap.contains(key);
}

void TombstoneList::cleanUp() {
  // This function is not guarded as all functions of this class are called from
  // MapSegment
  for (HashMapT<CacheableKeyPtr, TombstoneEntryPtr>::Iterator queIter =
           m_tombstoneMap.begin();
       queIter != m_tombstoneMap.end(); ++queIter) {
    CacheImpl::expiryTaskManager->cancelTask(
        static_cast<long>(queIter.second()->getExpiryTaskId()));
    delete queIter.second()->getHandler();
  }
}
