/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */
// RegionConfig.cpp: implementation of the RegionConfig class.
//
//////////////////////////////////////////////////////////////////////

#include "RegionConfig.hpp"

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

namespace gemfire {

RegionConfig::RegionConfig(const std::string& s, const std::string& c)
    : m_capacity(c) {}

void RegionConfig::setLru(const std::string& str) { m_lruEntriesLimit = str; }

void RegionConfig::setConcurrency(const std::string& str) {
  m_concurrency = str;
}

void RegionConfig::setCaching(const std::string& str) { m_caching = str; }
unsigned long RegionConfig::entries() { return atol(m_capacity.c_str()); }
unsigned long RegionConfig::getLruEntriesLimit() {
  return atol(m_lruEntriesLimit.c_str());
}

uint8_t RegionConfig::getConcurrency() {
  uint8_t cl = static_cast<uint8_t>(atoi(m_concurrency.c_str()));
  if (cl == 0) return 16;
  return cl;
}

bool RegionConfig::getCaching() {
  if (strcmp("true", m_caching.c_str()) == 0) {
    return true;
  } else if (strcmp("false", m_caching.c_str()) == 0) {
    return false;
  } else {
    return true;
  }
}
}  // namespace gemfire
