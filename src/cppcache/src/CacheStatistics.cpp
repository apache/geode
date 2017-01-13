/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/CacheStatistics.hpp>
#include <HostAsm.hpp>

using namespace gemfire;

CacheStatistics::CacheStatistics() {
  m_lastModifiedTime = 0;
  m_lastAccessTime = 0;
}

CacheStatistics::~CacheStatistics() {}

void CacheStatistics::setLastModifiedTime(uint32_t lmt) {
  HostAsm::atomicSet(m_lastModifiedTime, lmt);
}

void CacheStatistics::setLastAccessedTime(uint32_t lat) {
  HostAsm::atomicSet(m_lastAccessTime, lat);
}

uint32_t CacheStatistics::getLastModifiedTime() const {
  return m_lastModifiedTime;
}

uint32_t CacheStatistics::getLastAccessedTime() const {
  return m_lastAccessTime;
}
