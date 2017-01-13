/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testAttributesMutator"

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <CacheRegionHelper.hpp>

// this is a test.

using namespace gemfire;

class TestData {
 public:
  RegionPtr m_region;
  CachePtr m_cache;

} Test;

#define A s1p1

/* setup recipient */
DUNIT_TASK(A, Init)
  {
    CacheFactoryPtr cacheFactoryPtr = CacheFactory::createCacheFactory();
    Test.m_cache = cacheFactoryPtr->create();

    AttributesFactory af;
    af.setEntryTimeToLive(ExpirationAction::LOCAL_INVALIDATE, 5);
    RegionAttributesPtr attrs = af.createRegionAttributes();

    CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(Test.m_cache.ptr());
    cacheImpl->createRegion("Local_ETTL_LI", attrs, Test.m_region);
  }
ENDTASK

DUNIT_TASK(A, CreateAndVerifyExpiry)
  {
    CacheableInt32Ptr value = CacheableInt32::create(1);
    LOGDEBUG("### About to put of :one:1: ###");
    Test.m_region->put("one", value);
    LOGDEBUG("### Finished put of :one:1: ###");

    // countdown begins... it is ttl so access should not play into it..
    SLEEP(3000);  // sleep for a second, expect value to still be there.
    CacheableInt32Ptr res =
        dynCast<CacheableInt32Ptr>(Test.m_region->get("one"));
    ASSERT(res->value() == 1, "Expected to find value 1.");
    fflush(stdout);
    SLEEP(5000);  // sleep for 5 more seconds, expect value to be invalid.
    fflush(stdout);
    res = NULLPTR;
    ASSERT(Test.m_region->containsValueForKey("one") == false,
           "should not contain value.");
  }
ENDTASK

/* Test value sizes up to 1meg */
DUNIT_TASK(A, Close)
  {
    Test.m_region->destroyRegion();
    Test.m_cache->close();
    Test.m_cache = NULLPTR;
    Test.m_region = NULLPTR;
  }
ENDTASK
