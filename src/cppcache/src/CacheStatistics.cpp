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

#include <gfcpp/CacheStatistics.hpp>
#include <HostAsm.hpp>

using namespace apache::geode::client;

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
