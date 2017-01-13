/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __UNIT_TEST_TEST_UTILS__
#define __UNIT_TEST_TEST_UTILS__
#include <gfcpp/GemfireCppCache.hpp>

/* use CacheHelper to gain the impl pointer from cache or region object
 */

#include <CacheRegionHelper.hpp>

#ifdef _WIN32
// ???
#pragma warning(disable : 4290)
// truncated debugging symbol to 255 characters
#pragma warning(disable : 4786)
// template instantiation must have dllinterface
#pragma warning(disable : 4251)
#endif

#include <RegionInternal.hpp>
#include <LocalRegion.hpp>
// #include <DistributedRegion.hpp>
#include <DistributedSystemImpl.hpp>
#include <CacheImpl.hpp>

using namespace gemfire;

namespace unitTests {

class TestUtils {
 public:
  static RegionInternal* getRegionInternal(RegionPtr& rptr) {
    return dynamic_cast<RegionInternal*>(rptr.ptr());
  }

  static CacheImpl* getCacheImpl(const CachePtr& cptr) {
    return CacheRegionHelper::getCacheImpl(cptr.ptr());
  }

  static int testGetNumberOfPdxIds() {
    return PdxTypeRegistry::testGetNumberOfPdxIds();
  }

  static int testNumberOfPreservedData() {
    return PdxTypeRegistry::testNumberOfPreservedData();
  }

  static DistributedSystemImpl* getDistributedSystemImpl() {
    return CacheRegionHelper::getDistributedSystemImpl();
  }

  static bool waitForKey(CacheableKeyPtr& keyPtr, RegionPtr& rptr, int maxTry,
                         uint32_t msleepTime) {
    int tries = 0;
    bool found = false;
    while ((tries < maxTry) && (!(found = rptr->containsKey(keyPtr)))) {
      SLEEP(msleepTime);
      tries++;
    }
    return found;
  }
  static bool waitForValueForKey(CacheableKeyPtr& keyPtr, RegionPtr& rptr,
                                 int maxTry, uint32_t msleepTime) {
    int tries = 0;
    bool found = false;
    while ((tries < maxTry) && (!(found = rptr->containsValueForKey(keyPtr)))) {
      SLEEP(msleepTime);
      tries++;
    }
    return found;
  }
  static bool waitForValueForKeyGoAway(CacheableKeyPtr& keyPtr, RegionPtr& rptr,
                                       int maxTry, uint32_t msleepTime) {
    int tries = 0;
    bool found = true;
    while ((tries < maxTry) && (found = rptr->containsValueForKey(keyPtr))) {
      SLEEP(msleepTime);
      tries++;
    }
    return found;
  }
  static bool waitForValueNotNULL(CacheableStringPtr& valPtr, int maxTry,
                                  uint32_t msleepTime) {
    int tries = 0;
    bool found = false;
    // @TODO: ? How will valPtr every point to something else in this loop?
    while ((found = (valPtr == NULLPTR)) && (tries < maxTry)) {
      SLEEP(msleepTime);
      tries++;
    }
    return !found;
  }

  static int waitForValue(CacheableKeyPtr& keyPtr, int expected,
                          CacheableStringPtr& valPtr, RegionPtr& rptr,
                          int maxTry, uint32_t msleepTime) {
    int tries = 0;
    int val = 0;
    do {
      valPtr = dynCast<CacheableStringPtr>(rptr->get(keyPtr));
      ASSERT(valPtr != NULLPTR, "value should not be null.");
      val = atoi(valPtr->asChar());
      SLEEP(msleepTime);
      tries++;
    } while ((val != expected) && (tries < maxTry));
    return val;
  }
  static void showKeys(RegionPtr& rptr) {
    char buf[2048];
    if (rptr == NULLPTR) {
      sprintf(buf, "this region does not exist!\n");
      LOG(buf);
      return;
    }
    VectorOfCacheableKey v;
    rptr->keys(v);
    uint32_t len = v.size();
    sprintf(buf, "Total keys in region %s : %u\n", rptr->getName(), len);
    LOG(buf);
    for (uint32_t i = 0; i < len; i++) {
      char keyText[100];
      v[i]->logString(keyText, 100);
      sprintf(buf, "key[%u] = '%s'\n", i,
              (v[i] == NULLPTR) ? "NULL KEY" : keyText);
      LOG(buf);
    }
  }
  static void showKeyValues(RegionPtr& rptr) {
    char buf[2048];
    if (rptr == NULLPTR) {
      sprintf(buf, "this region does not exist!\n");
      LOG(buf);
      return;
    }
    VectorOfCacheableKey v;
    rptr->keys(v);
    uint32_t len = v.size();
    sprintf(buf, "Total keys in region %s : %u\n", rptr->getName(), len);
    LOG(buf);
    for (uint32_t i = 0; i < len; i++) {
      CacheableKeyPtr keyPtr = v[i];
      char keyText[100];
      keyPtr->logString(keyText, 100);
      CacheableStringPtr valPtr =
          dynCast<CacheableStringPtr>(rptr->get(keyPtr));
      sprintf(buf, "key[%u] = '%s', value[%u]='%s'\n", i,
              (keyPtr == NULLPTR) ? "NULL KEY" : keyText, i,
              (valPtr == NULLPTR) ? "NULL_VALUE" : valPtr->asChar());
      LOG(buf);
    }
  }
  static void showValues(RegionPtr& rptr) {
    char buf[2048];
    if (rptr == NULLPTR) {
      sprintf(buf, "this region does not exist!\n");
      LOG(buf);
      return;
    }
    VectorOfCacheable v;
    rptr->values(v);
    uint32_t len = v.size();
    sprintf(buf, "Total values in region %s : %u\n", rptr->getName(), len);
    LOG(buf);
    for (uint32_t i = 0; i < len; i++) {
      CacheableStringPtr value = dynCast<CacheableStringPtr>(v[i]);
      sprintf(buf, "value[%u] = '%s'\n", i,
              (value == NULLPTR) ? "NULL VALUE" : value->asChar());
      LOG(buf);
    }
  }
};
};
#endif  // define __UNIT_TEST_TEST_UTILS__
