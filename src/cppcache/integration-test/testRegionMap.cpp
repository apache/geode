/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/GemfireCppCache.hpp>

#include "fw_helper.hpp"

using test::cout;
using test::endl;

using namespace gemfire;

#define ROOT_NAME "testRegionMap"

#include "CacheHelper.hpp"

/**
 * @brief Test putting and getting entries without LRU enabled.
 */
BEGIN_TEST(TestRegionLRULastTen)
#if 1
  CacheHelper& cacheHelper = CacheHelper::getHelper();
  RegionPtr regionPtr;
  cacheHelper.createLRURegion(fwtest_Name, regionPtr);
  cout << regionPtr->getFullPath() << endl;
  // put more than 10 items... verify limit is held.
  uint32_t i;
  for (i = 0; i < 10; i++) {
    char buf[100];
    sprintf(buf, "%d", i);
    CacheableKeyPtr key = CacheableKey::create(buf);
    sprintf(buf, "value of %d", i);
    CacheableStringPtr valuePtr = cacheHelper.createCacheable(buf);
    regionPtr->put(key, valuePtr);
    VectorOfCacheableKey vecKeys;
    regionPtr->keys(vecKeys);
    ASSERT(vecKeys.size() == (i + 1), "expected more entries");
  }
  for (i = 10; i < 20; i++) {
    char buf[100];
    sprintf(buf, "%d", i);
    CacheableKeyPtr key = CacheableKey::create(buf);
    sprintf(buf, "value of %d", i);
    CacheableStringPtr valuePtr = cacheHelper.createCacheable(buf);
    regionPtr->put(key, valuePtr);
    VectorOfCacheableKey vecKeys;
    regionPtr->keys(vecKeys);
    cacheHelper.showKeys(vecKeys);
    ASSERT(vecKeys.size() == (10), "expected 10 entries");
  }
  VectorOfCacheableKey vecKeys;
  regionPtr->keys(vecKeys);
  ASSERT(vecKeys.size() == 10, "expected 10 entries");
  // verify it is the last 10 keys..
  int expected = 0;
  int total = 0;
  for (int k = 10; k < 20; k++) {
    expected += k;
    CacheableStringPtr key = dynCast<CacheableStringPtr>(vecKeys.back());
    vecKeys.pop_back();
    total += atoi(key->asChar());
  }
  ASSERT(vecKeys.empty(), "expected no more than 10 keys.");
  ASSERT(expected == total, "checksum mismatch.");
#endif
END_TEST(TestRegionLRULastTen)

BEGIN_TEST(TestRegionNoLRU)
#if 1
  CacheHelper& cacheHelper = CacheHelper::getHelper();
  RegionPtr regionPtr;
  cacheHelper.createPlainRegion(fwtest_Name, regionPtr);
  // put more than 10 items... verify limit is held.
  uint32_t i;
  for (i = 0; i < 20; i++) {
    char buf[100];
    sprintf(buf, "%d", i);
    CacheableKeyPtr key = CacheableKey::create(buf);
    sprintf(buf, "value of %d", i);
    CacheableStringPtr valuePtr = cacheHelper.createCacheable(buf);
    regionPtr->put(key, valuePtr);
    VectorOfCacheableKey vecKeys;
    regionPtr->keys(vecKeys);
    cacheHelper.showKeys(vecKeys);
    ASSERT(vecKeys.size() == (i + 1), "unexpected entries count");
  }

#endif

END_TEST(TestRegionNoLRU)

BEGIN_TEST(TestRegionLRULocal)
#if 1
  CacheHelper& cacheHelper = CacheHelper::getHelper();
  RegionPtr regionPtr;
  cacheHelper.createLRURegion(fwtest_Name, regionPtr);
  cout << regionPtr->getFullPath() << endl;
  // put more than 10 items... verify limit is held.
  uint32_t i;
  /** @TODO make this local scope and re-increase the iterations... would also
   * like to time it. */
  for (i = 0; i < 1000; i++) {
    char buf[100];
    sprintf(buf, "%d", i);
    CacheableKeyPtr key = CacheableKey::create(buf);
    sprintf(buf, "value of %d", i);
    CacheableStringPtr valuePtr = cacheHelper.createCacheable(buf);
    regionPtr->put(key, valuePtr);
    VectorOfCacheableKey vecKeys;
    regionPtr->keys(vecKeys);
    ASSERT(vecKeys.size() == (i < 10 ? i + 1 : 10), "expected more entries");
  }

#endif
END_TEST(TestRegionLRULocal)

BEGIN_TEST(TestRecentlyUsedBit)
  // Put twenty items in region. LRU is set to 10.
  // So 10 through 19 should be in region (started at 0)
  // get 15 so it is marked recently used.
  // put 9 more...  check that 15 was skipped for eviction.
  // put 1 more...  15 should then have been evicted.
  CacheHelper& cacheHelper = CacheHelper::getHelper();
  RegionPtr regionPtr;
  cacheHelper.createLRURegion(fwtest_Name, regionPtr);
  cout << regionPtr->getFullPath() << endl;
  // put more than 10 items... verify limit is held.
  uint32_t i;
  char buf[100];
  for (i = 0; i < 20; i++) {
    sprintf(buf, "%d", i);
    CacheableKeyPtr key = CacheableKey::create(buf);
    sprintf(buf, "value of %d", i);
    CacheableStringPtr valuePtr = cacheHelper.createCacheable(buf);
    regionPtr->put(key, valuePtr);
  }
  sprintf(buf, "%d", 15);
  CacheableStringPtr value2Ptr;
  CacheableKeyPtr key2 = CacheableKey::create(buf);
  value2Ptr = dynCast<CacheableStringPtr>(regionPtr->get(key2));
  ASSERT(value2Ptr != NULLPTR, "expected to find key 15 in cache.");
  for (i = 20; i < 35; i++) {
    sprintf(buf, "%d", i);
    CacheableKeyPtr key = CacheableKey::create(buf);
    CacheableStringPtr valuePtr = cacheHelper.createCacheable(buf);
    regionPtr->put(key, valuePtr);
    VectorOfCacheableKey vecKeys;
    regionPtr->keys(vecKeys);
    cacheHelper.showKeys(vecKeys);
    ASSERT(vecKeys.size() == 10, "expected more entries");
  }
  ASSERT(regionPtr->containsKey(key2), "expected to find key 15 in cache.");
  {
    sprintf(buf, "%d", 35);
    CacheableKeyPtr key = CacheableKey::create(buf);
    CacheableStringPtr valuePtr = cacheHelper.createCacheable(buf);
    regionPtr->put(key, valuePtr);
    VectorOfCacheableKey vecKeys;
    regionPtr->keys(vecKeys);
    cacheHelper.showKeys(vecKeys);
    ASSERT(vecKeys.size() == 10, "expected more entries");
  }
  ASSERT(regionPtr->containsKey(key2) == false, "15 should have been evicted.");

END_TEST(TestRecentlyUsedBit)

BEGIN_TEST(TestEmptiedMap)
  CacheHelper& cacheHelper = CacheHelper::getHelper();
  RegionPtr regionPtr;
  cacheHelper.createLRURegion(fwtest_Name, regionPtr);
  cout << regionPtr->getFullPath() << endl;
  // put more than 10 items... verify limit is held.
  uint32_t i;
  for (i = 0; i < 10; i++) {
    char buf[100];
    sprintf(buf, "%d", i);
    CacheableKeyPtr key = CacheableKey::create(buf);
    sprintf(buf, "value of %d", i);
    CacheableStringPtr valuePtr = cacheHelper.createCacheable(buf);
    regionPtr->put(key, valuePtr);
    VectorOfCacheableKey vecKeys;
    regionPtr->keys(vecKeys);
    ASSERT(vecKeys.size() == (i + 1), "expected more entries");
  }
  for (i = 0; i < 10; i++) {
    char buf[100];
    sprintf(buf, "%d", i);
    CacheableKeyPtr key = CacheableKey::create(buf);
    regionPtr->destroy(key);
    cout << "removed key " << dynCast<CacheableStringPtr>(key)->asChar()
         << endl;
  }
  VectorOfCacheableKey vecKeys;
  regionPtr->keys(vecKeys);
  ASSERT(vecKeys.size() == 0, "expected more entries");
  for (i = 20; i < 40; i++) {
    char buf[100];
    sprintf(buf, "%d", i);
    CacheableKeyPtr key = CacheableKey::create(buf);
    sprintf(buf, "value of %d", i);
    CacheableStringPtr valuePtr = cacheHelper.createCacheable(buf);
    regionPtr->put(key, valuePtr);
  }
  vecKeys.clear();
  regionPtr->keys(vecKeys);
  ASSERT(vecKeys.size() == 10, "expected more entries");

END_TEST(TestEmptiedMap)
