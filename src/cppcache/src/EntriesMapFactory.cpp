/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/Cache.hpp>
#include "EntriesMapFactory.hpp"
#include "LRUEntriesMap.hpp"
#include "ExpMapEntry.hpp"
#include "LRUExpMapEntry.hpp"
#include <gfcpp/DiskPolicyType.hpp>
//#include <gfcpp/ExpirationAction.hpp>
#include <gfcpp/SystemProperties.hpp>

using namespace gemfire;

/**
 * @brief Return a ConcurrentEntriesMap if no LRU, otherwise return a
 * LRUEntriesMap.
 * In the future, a EntriesMap facade can be put over the SharedRegionData to
 * support shared regions directly.
 */
EntriesMap* EntriesMapFactory::createMap(RegionInternal* region,
                                         const RegionAttributesPtr& attrs) {
  EntriesMap* result = NULL;
  uint32_t initialCapacity = attrs->getInitialCapacity();
  uint8_t concurrency = attrs->getConcurrencyLevel();
  /** @TODO will need a statistics entry factory... */
  uint32_t lruLimit = attrs->getLruEntriesLimit();
  uint32_t ttl = attrs->getEntryTimeToLive();
  uint32_t idle = attrs->getEntryIdleTimeout();
  bool concurrencyChecksEnabled = attrs->getConcurrencyChecksEnabled();
  bool heapLRUEnabled = false;

  SystemProperties* prop = DistributedSystem::getSystemProperties();
  if ((lruLimit != 0) ||
      (prop && prop->heapLRULimitEnabled())) {  // create LRU map...
    LRUAction::Action lruEvictionAction;
    DiskPolicyType::PolicyType dpType = attrs->getDiskPolicy();
    if (dpType == DiskPolicyType::OVERFLOWS) {
      lruEvictionAction = LRUAction::OVERFLOW_TO_DISK;
    } else if ((dpType == DiskPolicyType::NONE) ||
               (prop && prop->heapLRULimitEnabled())) {
      lruEvictionAction = LRUAction::LOCAL_DESTROY;
      if (prop && prop->heapLRULimitEnabled()) heapLRUEnabled = true;
    } else {
      return NULL;
    }
    if (ttl != 0 || idle != 0) {
      EntryFactory* entryFactory = LRUExpEntryFactory::singleton;
      entryFactory->setConcurrencyChecksEnabled(concurrencyChecksEnabled);
      result = new LRUEntriesMap(entryFactory, region, lruEvictionAction,
                                 lruLimit, concurrencyChecksEnabled,
                                 concurrency, heapLRUEnabled);
    } else {
      EntryFactory* entryFactory = LRUEntryFactory::singleton;
      entryFactory->setConcurrencyChecksEnabled(concurrencyChecksEnabled);
      result = new LRUEntriesMap(entryFactory, region, lruEvictionAction,
                                 lruLimit, concurrencyChecksEnabled,
                                 concurrency, heapLRUEnabled);
    }
  } else if (ttl != 0 || idle != 0) {
    // create entries with a ExpEntryFactory.
    EntryFactory* entryFactory = ExpEntryFactory::singleton;
    entryFactory->setConcurrencyChecksEnabled(concurrencyChecksEnabled);
    result = new ConcurrentEntriesMap(entryFactory, concurrencyChecksEnabled,
                                      region, concurrency);
  } else {
    // create plain concurrent map.
    EntryFactory* entryFactory = EntryFactory::singleton;
    entryFactory->setConcurrencyChecksEnabled(concurrencyChecksEnabled);
    result = new ConcurrentEntriesMap(entryFactory, concurrencyChecksEnabled,
                                      region, concurrency);
  }
  result->open(initialCapacity);
  return result;
}
