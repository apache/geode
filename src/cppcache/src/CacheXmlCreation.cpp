/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
