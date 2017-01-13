/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/Cache.hpp>
#include <gfcpp/CacheableKey.hpp>
#include <CacheableToken.hpp>

using namespace gemfire;

RegionEntry::RegionEntry(const RegionPtr& region, const CacheableKeyPtr& key,
                         const CacheablePtr& value)
    : m_region(region), m_key(key), m_value(value), m_destroyed(false) {}
RegionEntry::~RegionEntry() {}
CacheableKeyPtr RegionEntry::getKey() { return m_key; }
CacheablePtr RegionEntry::getValue() {
  return CacheableToken::isInvalid(m_value) ? NULLPTR : m_value;
}
void RegionEntry::getRegion(RegionPtr& region) { region = m_region; }
void RegionEntry::getStatistics(CacheStatisticsPtr& csptr) {
  csptr = m_statistics;
}
bool RegionEntry::isDestroyed() const { return m_destroyed; }
