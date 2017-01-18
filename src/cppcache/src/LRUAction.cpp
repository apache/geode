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
#include <gfcpp/Cache.hpp>
#include "LRUAction.hpp"
#include "LRULocalDestroyAction.hpp"
#include "LRUEntriesMap.hpp"
#include "CacheImpl.hpp"

using namespace apache::geode::client;

LRUAction* LRUAction::newLRUAction(const LRUAction::Action& actionType,
                                   RegionInternal* regionPtr,
                                   LRUEntriesMap* entriesMapPtr) {
  LRUAction* result = NULL;

  switch (actionType) {
    case LRUAction::INVALIDATE:
      result = new LRULocalInvalidateAction(regionPtr, entriesMapPtr);
      break;
    case LRUAction::LOCAL_DESTROY:
      result = new LRULocalDestroyAction(regionPtr, entriesMapPtr);
      break;
    case LRUAction::OVERFLOW_TO_DISK:
      result = new LRUOverFlowToDiskAction(regionPtr, entriesMapPtr);
      break;
    case LRUAction::DESTROY:
      // result = new LRUDestroyAction( regionPtr );
      result = new LRULocalDestroyAction(regionPtr, entriesMapPtr);
      break;
    default:
      /** @TODO throw IllegalArgumentException; */
      break;
  }
  return result;
}

bool LRUOverFlowToDiskAction::evict(const MapEntryImplPtr& mePtr) {
  if (m_regionPtr->isDestroyed()) {
    LOGERROR(
        "[internal error] :: OverflowAction: region is being destroyed, so not "
        "evicting entries");
    return false;
  }
  CacheableKeyPtr keyPtr;
  CacheablePtr valuePtr;
  mePtr->getKeyI(keyPtr);
  mePtr->getValueI(valuePtr);
  if (valuePtr == NULLPTR) {
    LOGERROR(
        "[internal error]:: OverflowAction: destroyed entry added to "
        "LRU list");
    throw FatalInternalException(
        "OverflowAction: destroyed entry added to "
        "LRU list");
  }
  LRUEntryProperties& lruProps = mePtr->getLRUProperties();
  void* persistenceInfo = lruProps.getPersistenceInfo();
  bool setInfo = false;
  if (persistenceInfo == NULL) {
    setInfo = true;
  }
  PersistenceManagerPtr pmPtr = m_regionPtr->getPersistenceManager();
  try {
    pmPtr->write(keyPtr, valuePtr, persistenceInfo);
  } catch (DiskFailureException& ex) {
    LOGERROR("DiskFailureException - %s", ex.getMessage());
    return false;
  } catch (Exception& ex) {
    LOGERROR("write to persistence layer failed - %s", ex.getMessage());
    return false;
  }
  if (setInfo == true) {
    lruProps.setPersistenceInfo(persistenceInfo);
  }
  (m_regionPtr->getRegionStats())->incOverflows();
  (m_regionPtr->getCacheImpl())->m_cacheStats->incOverflows();
  // set value after write on disk to indicate that it is on disk.
  mePtr->setValueI(CacheableToken::overflowed());

  if (m_entriesMapPtr != NULL) {
    int64_t newSize =
        CacheableToken::overflowed()->objectSize() - valuePtr->objectSize();
    m_entriesMapPtr->updateMapSize(newSize);
  }
  return true;
}

bool LRULocalInvalidateAction::evict(const MapEntryImplPtr& mePtr) {
  VersionTagPtr versionTag;
  CacheableKeyPtr keyPtr;
  mePtr->getKeyI(keyPtr);
  //  we should invoke the invalidateNoThrow with appropriate
  // flags to correctly invoke listeners
  LOGDEBUG("LRULocalInvalidate: evicting entry with key [%s]",
           Utils::getCacheableKeyString(keyPtr)->asChar());
  GfErrType err = GF_NOERR;
  if (!m_regionPtr->isDestroyed()) {
    err = m_regionPtr->invalidateNoThrow(
        keyPtr, NULLPTR, -1, CacheEventFlags::EVICTION | CacheEventFlags::LOCAL,
        versionTag);
  }
  return (err == GF_NOERR);
}
