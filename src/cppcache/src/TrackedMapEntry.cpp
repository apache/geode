/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "TrackedMapEntry.hpp"
#include "MapEntry.hpp"

using namespace gemfire;

void TrackedMapEntry::getKey(CacheableKeyPtr& result) const {
  m_entry->getKeyI(result);
}

void TrackedMapEntry::getValue(CacheablePtr& result) const {
  m_entry->getValueI(result);
}

void TrackedMapEntry::setValue(const CacheablePtr& value) {
  m_entry->setValueI(value);
}

LRUEntryProperties& TrackedMapEntry::getLRUProperties() {
  return m_entry->getLRUProperties();
}

ExpEntryProperties& TrackedMapEntry::getExpProperties() {
  return m_entry->getExpProperties();
}
VersionStamp& TrackedMapEntry::getVersionStamp() {
  throw FatalInternalException(
      "MapEntry::getVersionStamp for TrackedMapEntry is not applicable");
}
void TrackedMapEntry::cleanup(const CacheEventFlags eventFlags) {
  m_entry->cleanup(eventFlags);
}
