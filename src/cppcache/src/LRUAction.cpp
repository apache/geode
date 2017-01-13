/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/Cache.hpp>
#include "LRUAction.hpp"
#include "LRULocalDestroyAction.hpp"
#include "LRUEntriesMap.hpp"
#include "CacheImpl.hpp"

using namespace gemfire;

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
