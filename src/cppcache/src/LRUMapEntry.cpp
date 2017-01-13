/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "LRUMapEntry.hpp"
#include "MapEntryT.hpp"

using namespace gemfire;

LRUEntryFactory* LRUEntryFactory::singleton = NULL;

/**
 * @brief called when library is initialized... see CppCacheLibrary.
 */
void LRUEntryFactory::init() { singleton = new LRUEntryFactory(); }

void LRUEntryFactory::newMapEntry(const CacheableKeyPtr& key,
                                  MapEntryImplPtr& result) const {
  if (m_concurrencyChecksEnabled) {
    result = MapEntryT<VersionedLRUMapEntry, 0, 0>::create(key);
  } else {
    result = MapEntryT<LRUMapEntry, 0, 0>::create(key);
  }
}
