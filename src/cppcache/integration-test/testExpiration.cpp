/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testExpiration"

#include "fw_helper.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <CacheRegionHelper.hpp>

using namespace gemfire;

ExpirationAction::Action action = ExpirationAction::DESTROY;

// This test is for serially running the tests.

int getNumOfEntries(RegionPtr& R1) {
  VectorOfCacheableKey v;
  R1->keys(v);
  LOGFINE("Number of keys in region %s is %d", R1->getFullPath(), v.size());
  return v.size();
}

void startDSandCreateCache(CachePtr& cache) {
  PropertiesPtr pp = Properties::create();
  CacheFactoryPtr cacheFactoryPtr = CacheFactory::createCacheFactory(pp);
  cache = cacheFactoryPtr->create();
  ASSERT(cache != NULLPTR, "cache not equal to null expected");
}

void doNPuts(RegionPtr& rptr, int n) {
  CacheableStringPtr value;
  char buf[16];
  memset(buf, 'A', 15);
  buf[15] = '\0';
  memcpy(buf, "Value - ", 8);
  value = CacheableString::create(buf);
  ASSERT(value != NULLPTR, "Failed to create value.");

  for (int i = 0; i < n; i++) {
    sprintf(buf, "KeyA - %d", i + 1);
    CacheableKeyPtr key = CacheableKey::create(buf);
    LOGINFO("Putting key %s value %s in region %s", buf, value->toString(),
            rptr->getFullPath());
    rptr->put(key, value);
  }
}

CacheableKeyPtr do1Put(RegionPtr& rptr) {
  CacheableStringPtr value;
  char buf[16];
  memset(buf, 'A', 15);
  buf[15] = '\0';
  memcpy(buf, "Value - ", 8);
  value = CacheableString::create(buf);
  ASSERT(value != NULLPTR, "Failed to create value.");

  sprintf(buf, "KeyA - %d", 0 + 1);
  CacheableKeyPtr key = CacheableKey::create(buf);
  LOGINFO("Putting key %s value %s in region %s", buf, value->toString(),
          rptr->getFullPath());
  rptr->put(key, value);
  return key;
}

void setExpTimes(RegionAttributesPtr& attrs, int ettl = 0, int eit = 0,
                 int rttl = 0, int rit = 0) {
  AttributesFactory afact;

  afact.setEntryIdleTimeout(action, eit);
  afact.setEntryTimeToLive(action, ettl);
  afact.setRegionIdleTimeout(action, rit);
  afact.setRegionTimeToLive(action, rttl);

  attrs = afact.createRegionAttributes();
}

BEGIN_TEST(TEST_EXPIRATION)
  {
    CachePtr cache;

    startDSandCreateCache(cache);

    ASSERT(cache != NULLPTR, "Expected cache to be NON-NULL");

    RegionAttributesPtr attrs_1;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_1);

    RegionPtr R1;
    CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cache.ptr());
    cacheImpl->createRegion("R1", attrs_1, R1);
    ASSERT(R1 != NULLPTR, "Expected R1 to be NON-NULL");

    doNPuts(R1, 100);

    ACE_OS::sleep(10);

    int n = getNumOfEntries(R1);
    ASSERT(n == 100, "Expected 100 entries");

    ASSERT(R1->isDestroyed() == false, "Expected R1 to be alive");

    RegionAttributesPtr attrs_2;

    setExpTimes(attrs_2, 20, 2, 0, 0);

    RegionPtr R2;
    cacheImpl->createRegion("R2", attrs_2, R2);
    ASSERT(R2 != NULLPTR, "Expected R2 to be NON-NULL");
    LOG("Region R2 created");
    doNPuts(R2, 1);

    ACE_OS::sleep(5);

    n = getNumOfEntries(R2);
    ASSERT(n == 1, "Expected 1 entry");

    ASSERT(R2->isDestroyed() == false, "Expected R2 to be alive");

    RegionAttributesPtr attrs_3;
    // rttl = 20, reit = 2
    setExpTimes(attrs_3, 0, 0, 20, 2);

    RegionPtr R3;
    cacheImpl->createRegion("R3", attrs_3, R3);
    ASSERT(R3 != NULLPTR, "Expected R3 to be NON-NULL");

    ACE_OS::sleep(5);

    ASSERT(R3->isDestroyed() == false, "Expected R3 to be alive");

    RegionAttributesPtr attrs_4;

    setExpTimes(attrs_4, 5, 0, 0, 0);

    RegionPtr R4;
    cacheImpl->createRegion("R4", attrs_4, R4);
    ASSERT(R4 != NULLPTR, "Expected R4 to be NON-NULL");

    doNPuts(R4, 1);
    // This will be same as updating the object

    ACE_OS::sleep(10);

    n = getNumOfEntries(R4);
    ASSERT(n == 0, "Expected 0 entry");

    ASSERT(R4->isDestroyed() == false, "Expected R4 to be alive");

    RegionAttributesPtr attrs_5;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_5, 0, 5, 0, 0);

    RegionPtr R5;
    cacheImpl->createRegion("R5", attrs_5, R5);
    ASSERT(R5 != NULLPTR, "Expected R5 to be NON-NULL");

    CacheableKeyPtr key_0 = do1Put(R5);

    ACE_OS::sleep(2);

    R5->get(key_0);
    ACE_OS::sleep(3);

    n = getNumOfEntries(R5);

    printf("n ==  %d\n", n);
    ASSERT(n == 1, "Expected 1 entry");

    // ACE_OS::sleep(3);
    ACE_OS::sleep(6);
    n = getNumOfEntries(R5);

    ASSERT(n == 0, "Expected 0 entry");
    ASSERT(R5->isDestroyed() == false, "Expected R5 to be alive");

    RegionAttributesPtr attrs_6;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_6, 0, 0, 5, 0);

    RegionPtr R6;
    cacheImpl->createRegion("R6", attrs_6, R6);
    ASSERT(R6 != NULLPTR, "Expected R6 to be NON-NULL");

    doNPuts(R6, 1);

    ACE_OS::sleep(2);

    doNPuts(R6, 1);

    ACE_OS::sleep(7);

    ASSERT(R6->isDestroyed() == true, "Expected R6 to be dead");

    RegionAttributesPtr attrs_7;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_7, 0, 0, 0, 5);

    RegionPtr R7;
    cacheImpl->createRegion("R7", attrs_7, R7);
    ASSERT(R7 != NULLPTR, "Expected R7 to be NON-NULL");

    doNPuts(R7, 1);

    ACE_OS::sleep(2);

    doNPuts(R7, 1);

    ACE_OS::sleep(10);

    ASSERT(R7->isDestroyed() == true, "Expected R7 to be dead");

    RegionAttributesPtr attrs_8;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_8, 10, 0, 0, 0);

    RegionPtr R8;
    cacheImpl->createRegion("R8", attrs_8, R8);
    ASSERT(R8 != NULLPTR, "Expected R8 to be NON-NULL");

    CacheableKeyPtr key = do1Put(R8);

    ACE_OS::sleep(5);

    R8->get(key);

    ACE_OS::sleep(6);

    n = getNumOfEntries(R8);
    ASSERT(n == 0, "Expected 1 entries");

    RegionAttributesPtr attrs_9;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_9, 0, 0, 0, 8);

    RegionPtr R9;
    cacheImpl->createRegion("R9", attrs_9, R9);
    ASSERT(R9 != NULLPTR, "Expected R9 to be NON-NULL");

    CacheableKeyPtr key_1 = do1Put(R9);

    ACE_OS::sleep(5);

    R9->get(key_1);

    ACE_OS::sleep(5);

    n = getNumOfEntries(R9);
    ASSERT(n == 1, "Expected 1 entries");

    ASSERT(R9->isDestroyed() == false, "Expected R9 to be alive");

    RegionAttributesPtr attrs_10;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_10, 6, 0, 0, 12);

    RegionPtr R10;
    cacheImpl->createRegion("R10", attrs_10, R10);
    ASSERT(R10 != NULLPTR, "Expected R10 to be NON-NULL");

    doNPuts(R10, 1);

    ACE_OS::sleep(10);

    n = getNumOfEntries(R10);
    ASSERT(n == 0, "Expected 0 entries");

    ACE_OS::sleep(11);

    ASSERT(R10->isDestroyed() == true, "Expected R10 to be dead");

    RegionAttributesPtr attrs_11;

    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_11, 0, 4, 0, 7);

    RegionPtr R11;
    cacheImpl->createRegion("R11", attrs_11, R11);
    ASSERT(R11 != NULLPTR, "Expected R11 to be NON-NULL");

    CacheableKeyPtr k11 = do1Put(R11);

    ACE_OS::sleep(3);

    n = getNumOfEntries(R11);
    ASSERT(n == 1, "Expected 1 entries");

    R11->get(k11);

    ACE_OS::sleep(5);

    ASSERT(R11->isDestroyed() == false,
           "Expected R11 to be alive as the get has changed the access time");

    ACE_OS::sleep(5);

    ASSERT(R11->isDestroyed() == true, "Expected R11 to be dead");

    RegionAttributesPtr attrs_12;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_12, 5, 0, 0, 0);

    RegionPtr R12;
    cacheImpl->createRegion("R12", attrs_12, R12);
    ASSERT(R12 != NULLPTR, "Expected R12 to be NON-NULL");

    CacheableKeyPtr key_3 = do1Put(R12);

    ACE_OS::sleep(6);

    n = getNumOfEntries(R12);
    ASSERT(n == 0, "Expected 0 entries");

    ASSERT(R12->isDestroyed() == false, "Expected R12 to be alive");
    /////////

    RegionAttributesPtr attrs_14;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_14, 0, 0, 10, 0);

    RegionPtr R14;
    cacheImpl->createRegion("R14", attrs_14, R14);
    ASSERT(R14 != NULLPTR, "Expected R14 to be NON-NULL");

    doNPuts(R14, 1);

    ACE_OS::sleep(12);

    ASSERT(R14->isDestroyed() == true, "Expected R14 to be dead");

    RegionAttributesPtr attrs_15;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_15, 0, 5, 0, 0);

    RegionPtr R15;
    cacheImpl->createRegion("R15", attrs_15, R15);
    ASSERT(R15 != NULLPTR, "Expected R15 to be NON-NULL");

    CacheableKeyPtr key_4 = do1Put(R15);

    ACE_OS::sleep(2);

    R15->destroy(key_4);

    ACE_OS::sleep(5);

    ASSERT(R15->isDestroyed() == false, "Expected R15 to be alive");

    //////////////
    RegionAttributesPtr attrs_18;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_18, 6, 3, 0, 0);

    RegionPtr R18;
    cacheImpl->createRegion("R18", attrs_18, R18);
    ASSERT(R18 != NULLPTR, "Expected R18 to be NON-NULL");

    doNPuts(R18, 1);

    ACE_OS::sleep(4);

    n = getNumOfEntries(R18);
    ASSERT(n == 1, "entry idle should be useless as ttl is > 0");

    ACE_OS::sleep(4);
    n = getNumOfEntries(R18);
    ASSERT(n == 0, "ttl is over so it should be 0");

    RegionAttributesPtr attrs_19;
    // ettl = 0, eit = 0, rttl = 0, reit = 0
    setExpTimes(attrs_19, 0, 0, 6, 3);

    RegionPtr R19;
    cacheImpl->createRegion("R19x", attrs_19, R19);
    ASSERT(R19 != NULLPTR, "Expected R19 to be NON-NULL");

    ACE_OS::sleep(4);

    doNPuts(R19, 1);

    ACE_OS::sleep(4);

    ASSERT(R19->isDestroyed() == false,
           "Expected R19 to be alive as an entry was put");

    ACE_OS::sleep(4);

    ASSERT(R19->isDestroyed() == true, "Expected R19 to be dead");
    cache->close();
  }
END_TEST(TEST_EXPIRATION)
