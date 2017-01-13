/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ExpMapEntry.hpp"
#include "MapEntryT.hpp"

using namespace gemfire;

ExpEntryFactory* ExpEntryFactory::singleton = NULL;

/**
 * @brief called when library is initialized... see CppCacheLibrary.
 */
void ExpEntryFactory::init() { singleton = new ExpEntryFactory(); }

void ExpEntryFactory::newMapEntry(const CacheableKeyPtr& key,
                                  MapEntryImplPtr& result) const {
  if (m_concurrencyChecksEnabled) {
    result = MapEntryT<VersionedExpMapEntry, 0, 0>::create(key);
  } else {
    result = MapEntryT<ExpMapEntry, 0, 0>::create(key);
  }
}
