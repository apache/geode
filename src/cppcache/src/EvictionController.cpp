/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "EvictionController.hpp"
#include "CacheImpl.hpp"
#include "CacheRegionHelper.hpp"
#include "RegionInternal.hpp"
#include <gfcpp/DistributedSystem.hpp>
#include "ReadWriteLock.hpp"
#include <string>

using namespace gemfire;

const char* EvictionController::NC_EC_Thread = "NC EC Thread";
EvictionController::EvictionController(size_t maxHeapSize,
                                       int32_t heapSizeDelta, CacheImpl* cache)
    : m_run(false),
      m_maxHeapSize(maxHeapSize * 1024 * 1024),
      m_heapSizeDelta(heapSizeDelta),
      m_cacheImpl(cache),
      m_currentHeapSize(0) {
  evictionThreadPtr = new EvictionThread(this);
  LOGINFO("Maximum heap size for Heap LRU set to %ld bytes", m_maxHeapSize);
  //  m_currentHeapSize =
  //  DistributedSystem::getSystemProperties()->gfHighWaterMark(),
  //  DistributedSystem::getSystemProperties()->gfMessageSize();
}

EvictionController::~EvictionController() { GF_SAFE_DELETE(evictionThreadPtr); }

void EvictionController::updateRegionHeapInfo(int64_t info) {
  // LOGINFO("updateRegionHeapInfo is %d", info);
  m_queue.put(info);
  // We could block here if we wanted to prevent any further memory use
  // until the evictions had been completed.
}

int EvictionController::svc() {
  DistributedSystemImpl::setThreadName(NC_EC_Thread);
  int64_t pendingEvictions = 0;
  while (m_run) {
    int64_t readInfo = 0;
    readInfo = (int64_t)m_queue.get(1500);
    if (readInfo == 0) continue;

    processHeapInfo(readInfo, pendingEvictions);
  }
  int32_t size = m_queue.size();
  for (int i = 0; i < size; i++) {
    int64_t readInfo = 0;
    readInfo = (int64_t)m_queue.get();
    if (readInfo == 0) continue;
    processHeapInfo(readInfo, pendingEvictions);
  }
  return 1;
}

void EvictionController::processHeapInfo(int64_t& readInfo,
                                         int64_t& pendingEvictions) {
  m_currentHeapSize += readInfo;

  // Waiting for evictions to catch up.Negative numbers
  // are attributed to evictions that were triggered by the
  // EvictionController
  int64_t sizeToCompare = 0;
  if (readInfo < 0 && pendingEvictions > 0) {
    pendingEvictions += readInfo;
    if (pendingEvictions < 0) pendingEvictions = 0;
    return;  // as long as you are still evicting, don't do the rest of the work
  } else {
    sizeToCompare = m_currentHeapSize - pendingEvictions;
  }

  if (sizeToCompare > m_maxHeapSize) {
    // Check if overflow is above the delta
    int64_t sizeOverflow = sizeToCompare - m_maxHeapSize;

    // Calculate the percentage that we are over the limit.
    int32_t fractionalOverflow =
        static_cast<int32_t>(((sizeOverflow * 100) % m_maxHeapSize) > 0) ? 1
                                                                         : 0;
    int32_t percentage =
        static_cast<int32_t>((sizeOverflow * 100) / m_maxHeapSize) +
        fractionalOverflow;
    // need to evict
    int32_t evictionPercentage =
        static_cast<int32_t>(percentage + m_heapSizeDelta);
    int32_t bytesToEvict =
        static_cast<int32_t>((sizeToCompare * evictionPercentage) / 100);
    pendingEvictions += bytesToEvict;
    if (evictionPercentage > 100) evictionPercentage = 100;
    orderEvictions(evictionPercentage);
    // Sleep for 10 seconds to allow the evictions to catch up
    //   gemfire::millisleep(10);  //taken this out for now
  }
}

void EvictionController::registerRegion(std::string& name) {
  WriteGuard guard(m_regionLock);
  m_regions.push_back(name);
  LOGFINE("Registered region with Heap LRU eviction controller: name is %s",
          name.c_str());
}

void EvictionController::deregisterRegion(std::string& name) {
  // Iterate over regions vector and remove the one that we need to remove
  WriteGuard guard(m_regionLock);
  for (size_t i = 0; i < m_regions.size(); i++) {
    std::string str = (std::string)m_regions.at(i);
    if (str == name) {
      std::vector<std::string>::iterator iter = m_regions.begin();
      m_regions.erase(iter + i);
      LOGFINE(
          "Deregistered region with Heap LRU eviction controller: name is %s",
          name.c_str());
      break;
    }
  }
}

void EvictionController::orderEvictions(int32_t percentage) {
  evictionThreadPtr->putEvictionInfo(percentage);
}

void EvictionController::evict(int32_t percentage) {
  // TODO:  Shouldn't we take the CacheImpl::m_regions
  // lock here? Otherwise we might invoke eviction on a region
  // that has been destroyed or is being destroyed.
  // Its important to not hold this lock for too long
  // because it prevents new regions from getting created or destroyed
  // On the flip side, this requires a copy of the registered region list
  // every time eviction is ordered and that might not be cheap
  //@TODO: Discuss with team
  VectorOfString regionTmpVector;
  {
    ReadGuard guard(m_regionLock);
    for (size_t i = 0; i < m_regions.size(); i++) {
      regionTmpVector.push_back((std::string)m_regions.at(i));
    }
  }

  for (size_t i = 0; i < regionTmpVector.size(); i++) {
    std::string str = (std::string)regionTmpVector.at(i);
    RegionPtr rptr;
    m_cacheImpl->getRegion(str.c_str(), rptr);
    if (rptr != NULLPTR) {
      RegionInternal* rimpl = dynamic_cast<RegionInternal*>(rptr.ptr());
      if (rimpl != NULL) {
        rimpl->evict(percentage);
      }
    }
  }
}
