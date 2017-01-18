/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache
