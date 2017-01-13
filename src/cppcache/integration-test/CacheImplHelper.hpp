/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef TEST_CACHEIMPLHELPER_HPP
#define TEST_CACHEIMPLHELPER_HPP

#include <gfcpp/GemfireCppCache.hpp>
#include <stdlib.h>
#include <gfcpp/SystemProperties.hpp>
#include <ace/OS.h>
#include "testUtils.hpp"

#ifndef ROOT_NAME
ROOT_NAME++ +
    DEFINE ROOT_NAME before including CacheHelper.hpp
#endif

#ifndef ROOT_SCOPE
#define ROOT_SCOPE LOCAL
#endif

    using namespace gemfire;
using namespace unitTests;

class CacheImplHelper : public CacheHelper {
 public:
  CacheImplHelper(const char* member_id,
                  const PropertiesPtr& configPtr = NULLPTR)
      : CacheHelper(member_id, configPtr) {}

  CacheImplHelper(const PropertiesPtr& configPtr = NULLPTR)
      : CacheHelper(configPtr) {}

  virtual void createRegion(const char* regionName, RegionPtr& regionPtr,
                            uint32_t size, bool ack = false,
                            bool cacheServerClient = false,
                            bool cacheEnabled = true) {
    RegionAttributesPtr regAttrs;
    AttributesFactory attrFactory;
    // set lru attributes...
    attrFactory.setLruEntriesLimit(0);     // no limit.
    attrFactory.setInitialCapacity(size);  // no limit.
    // then...
    attrFactory.setCachingEnabled(cacheEnabled);
    regAttrs = attrFactory.createRegionAttributes();
    showRegionAttributes(*regAttrs);
    CacheImpl* cimpl = TestUtils::getCacheImpl(cachePtr);
    ASSERT(cimpl != NULL, "failed to get cacheImpl *.");
    cimpl->createRegion(regionName, regAttrs, regionPtr);
    ASSERT(regionPtr != NULLPTR, "failed to create region.");
  }
};
#endif  // TEST_CACHEHELPER_HPP
