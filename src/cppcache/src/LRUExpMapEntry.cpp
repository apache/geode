/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "LRUExpMapEntry.hpp"
#include "MapEntryT.hpp"

using namespace gemfire;

LRUExpEntryFactory* LRUExpEntryFactory::singleton = NULL;

/**
 * @brief called when library is initialized... see CppCacheLibrary.
 */
void LRUExpEntryFactory::init() { singleton = new LRUExpEntryFactory(); }

void LRUExpEntryFactory::newMapEntry(const CacheableKeyPtr& key,
                                     MapEntryImplPtr& result) const {
  if (m_concurrencyChecksEnabled) {
    result = MapEntryT<VersionedLRUExpMapEntry, 0, 0>::create(key);
  } else {
    result = MapEntryT<LRUExpMapEntry, 0, 0>::create(key);
  }
}
