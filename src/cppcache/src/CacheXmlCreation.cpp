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

#include <gfcpp/Cache.hpp>
#include "CacheXmlCreation.hpp"
#include "CacheImpl.hpp"
#include "PoolAttributes.hpp"

using namespace gemfire;

void CacheXmlCreation::addRootRegion(RegionXmlCreation* root) {
  rootRegions.push_back(root);
}

void CacheXmlCreation::addPool(PoolXmlCreation* pool) { pools.push_back(pool); }

void CacheXmlCreation::create(Cache* cache) {
  m_cache = cache;
  m_cache->m_cacheImpl->setPdxIgnoreUnreadFields(m_pdxIgnoreUnreadFields);
  m_cache->m_cacheImpl->setPdxReadSerialized(m_readPdxSerialized);
  // Create any pools before creating any regions.

  std::vector<PoolXmlCreation*>::iterator pool = pools.begin();
  while (pool != pools.end()) {
    (*pool)->create();
    ++pool;
  }

  std::vector<RegionXmlCreation*>::iterator start = rootRegions.begin();
  while (start != rootRegions.end()) {
    (*start)->createRoot(cache);
    ++start;
  }
}

void CacheXmlCreation::setPdxIgnoreUnreadField(bool ignore) {
  // m_cache->m_cacheImpl->setPdxIgnoreUnreadFields(ignore);
  m_pdxIgnoreUnreadFields = ignore;
}

void CacheXmlCreation::setPdxReadSerialized(bool val) {
  // m_cache->m_cacheImpl->setPdxIgnoreUnreadFields(ignore);
  m_readPdxSerialized = val;
}

CacheXmlCreation::CacheXmlCreation()
    /* adongre
     * CID 28926: Uninitialized pointer field (UNINIT_CTOR)
     */
    : m_cache((Cache*)0) {
  m_pdxIgnoreUnreadFields = false;
  m_readPdxSerialized = false;
}

CacheXmlCreation::~CacheXmlCreation() {
  std::vector<RegionXmlCreation*>::iterator start = rootRegions.begin();
  while (start != rootRegions.end()) {
    delete *start;
    *start = NULL;
    ++start;
  }
  std::vector<PoolXmlCreation*>::iterator pool = pools.begin();
  while (pool != pools.end()) {
    delete *pool;
    *pool = NULL;
    ++pool;
  }
}
