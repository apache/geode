/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "MapEntry.hpp"
#include "MapEntryT.hpp"

using namespace gemfire;

EntryFactory* EntryFactory::singleton = NULL;
MapEntryPtr MapEntry::MapEntry_NullPointer(NULLPTR);

/**
 * @brief called when library is initialized... see CppCacheLibrary.
 */
void EntryFactory::init() { singleton = new EntryFactory(); }

void EntryFactory::newMapEntry(const CacheableKeyPtr& key,
                               MapEntryImplPtr& result) const {
  if (m_concurrencyChecksEnabled) {
    result = MapEntryT<VersionedMapEntryImpl, 0, 0>::create(key);
  } else {
    result = MapEntryT<MapEntryImpl, 0, 0>::create(key);
  }
}
