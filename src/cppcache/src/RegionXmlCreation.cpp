/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/Cache.hpp>
#include <CacheRegionHelper.hpp>
#include "RegionXmlCreation.hpp"
#include "CacheImpl.hpp"
using namespace gemfire;
extern bool Cache_CreatedFromCacheFactory;

void RegionXmlCreation::addSubregion(RegionXmlCreation* regionPtr) {
  subRegions.push_back(regionPtr);
}

void RegionXmlCreation::setAttributes(RegionAttributesPtr attrsPtr) {
  regAttrs = attrsPtr;
}

RegionAttributesPtr RegionXmlCreation::getAttributes() { return regAttrs; }

void RegionXmlCreation::fillIn(RegionPtr regionPtr) {
  std::vector<RegionXmlCreation*>::iterator start = subRegions.begin();
  while (start != subRegions.end()) {
    RegionXmlCreation* regXmlCreation = *start;
    regXmlCreation->create(regionPtr);
    start++;
  }
}

void RegionXmlCreation::createRoot(Cache* cache) {
  GF_D_ASSERT(this->isRoot);
  RegionPtr rootRegPtr = NULLPTR;

  if (Cache_CreatedFromCacheFactory) {
    //  if(cache->m_cacheImpl->getDefaultPool() == NULLPTR)
    {
      // we may need to initialize default pool
      if (regAttrs->getEndpoints() == NULL) {
        if (regAttrs->getPoolName() == NULL) {
          PoolPtr pool = CacheFactory::createOrGetDefaultPool();

          if (pool == NULLPTR) {
            throw IllegalStateException("Pool is not defined create region.");
          }
          regAttrs->setPoolName(pool->getName());
        }
      }
    }
  }

  CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cache);
  cacheImpl->createRegion(regionName.c_str(), regAttrs, rootRegPtr);
  fillIn(rootRegPtr);
}

void RegionXmlCreation::create(RegionPtr parent) {
  GF_D_ASSERT(!(this->isRoot));
  RegionPtr subRegPtr = NULLPTR;

  subRegPtr = parent->createSubregion(regionName.c_str(), regAttrs);
  fillIn(subRegPtr);
}

RegionXmlCreation::RegionXmlCreation(char* name, bool isRootRegion)
    : regAttrs(NULLPTR) {
  std::string tempName(name);
  regionName = tempName;
  isRoot = isRootRegion;
  attrId = "";
}

RegionXmlCreation::~RegionXmlCreation() {
  std::vector<RegionXmlCreation*>::iterator start = subRegions.begin();
  while (start != subRegions.end()) {
    delete *start;
    *start = NULL;
    ++start;
  }
}

std::string RegionXmlCreation::getAttrId() const { return attrId; }

void RegionXmlCreation::setAttrId(const std::string& pattrId) {
  this->attrId = pattrId;
}
