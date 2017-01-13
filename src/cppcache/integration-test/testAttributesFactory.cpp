/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testAttributesFactory"

#include "fw_helper.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <CacheRegionHelper.hpp>

using namespace gemfire;

BEGIN_TEST(ATTRIBUTE_FACTORY)
  {
    AttributesFactory af;
    RegionAttributesPtr ra;
    RegionPtr region;

    CacheFactoryPtr cacheFactoryPtr = CacheFactory::createCacheFactory();
    CachePtr cache = cacheFactoryPtr->create();
    ra = af.createRegionAttributes();

    CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cache.ptr());
    cacheImpl->createRegion("region1", ra, region);
    LOG("local region created with HA cache specification.");
    cache->close();
  }
END_TEST(ATTRIBUTE_FACTORY)

/* testing attributes with invalid value */
/* testing with negative values */          /*see bug no #865 */
/* testing with exceed boundry condition */ /*see bug no #865 */
BEGIN_TEST(REGION_FACTORY)
  {
    CacheFactoryPtr cf = CacheFactory::createCacheFactory();
    CachePtr m_cache = cf->create();

    RegionFactoryPtr rf = m_cache->createRegionFactory(LOCAL);
    /*see bug no #865 */
    try {
      rf->setInitialCapacity(-1);
      FAIL("Should have got expected IllegalArgumentException");
    } catch (IllegalArgumentException) {
      LOG("Got expected IllegalArgumentException");
    }

    RegionPtr m_region = rf->create("Local_ETTL_LI");
    LOGINFO("m_region->getAttributes()->getInitialCapacity() = %d ",
            m_region->getAttributes()->getInitialCapacity());
    ASSERT(m_region->getAttributes()->getInitialCapacity() == 10000,
           "Incorrect InitialCapacity");

    m_region->put(1, 1);
    CacheableInt32Ptr res = dynCast<CacheableInt32Ptr>(m_region->get(1));
    ASSERT(res->value() == 1, "Expected to find value 1.");

    m_region->destroyRegion();
    m_cache->close();
    m_cache = NULLPTR;
    m_region = NULLPTR;

    CacheFactoryPtr cf1 = CacheFactory::createCacheFactory();
    CachePtr m_cache1 = cf1->create();

    RegionFactoryPtr rf1 = m_cache1->createRegionFactory(LOCAL);
    /*see bug no #865 */
    try {
      rf1->setInitialCapacity(2147483648U);
      FAIL("Should have got expected IllegalArgumentException");
    } catch (IllegalArgumentException) {
      LOG("Got expected IllegalArgumentException");
    }
    RegionPtr m_region1 = rf1->create("Local_ETTL_LI");
    LOGINFO("m_region1->getAttributes()->getInitialCapacity() = %d ",
            m_region1->getAttributes()->getInitialCapacity());
    ASSERT(m_region1->getAttributes()->getInitialCapacity() == 10000,
           "Incorrect InitialCapacity");

    m_region1->put(1, 1);
    CacheableInt32Ptr res1 = dynCast<CacheableInt32Ptr>(m_region1->get(1));
    ASSERT(res1->value() == 1, "Expected to find value 1.");

    m_region1->destroyRegion();
    m_cache1->close();
    m_cache1 = NULLPTR;
    m_region1 = NULLPTR;
  }
END_TEST(REGION_FACTORY)
