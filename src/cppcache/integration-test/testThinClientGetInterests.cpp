/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

#include "locator_globals.hpp"

using namespace gemfire;
using namespace test;

const char* durableIds[] = {"DurableId1", "DurableId2"};

DUNIT_TASK(SERVER1, StartServer)
  {
    if (isLocalServer) {
      CacheHelper::initLocator(1);
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
    }
    LOG("SERVER started");
  }
END_TASK(StartServer)

DUNIT_TASK(CLIENT1, SetupClient1)
  {
    PropertiesPtr pp = Properties::create();
    pp->insert("durable-client-id", durableIds[0]);
    pp->insert("durable-timeout", 300);
    pp->insert("notify-ack-interval", 1);

    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1", pp, 0,
                       true);
    getHelper()->createPooledRegion(regionNames[0], false, locatorsG,
                                    "__TEST_POOL1__", true, true);
    CacheableKeyPtr keyPtr0 = CacheableString::create(keys[0]);
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    VectorOfCacheableKey keys0;
    keys0.push_back(keyPtr0);
    regPtr0->registerKeys(keys0, NULLPTR);
    CacheableKeyPtr keyPtr1 = CacheableString::create(keys[1]);
    VectorOfCacheableKey keys1;
    keys1.push_back(keyPtr1);
    regPtr0->registerKeys(keys1, NULLPTR);
    regPtr0->registerRegex(testregex[0]);
    regPtr0->registerRegex(testregex[1]);
    CacheableKeyPtr keyPtr2 = CacheableString::create(keys[2]);
    VectorOfCacheableKey keys2;
    keys2.push_back(keyPtr2);
    keyPtr2 = CacheableString::create(keys[3]);
    keys2.push_back(keyPtr2);
    // durable
    regPtr0->registerKeys(keys2, NULLPTR, true);
    regPtr0->registerRegex(testregex[2], true);

    VectorOfCacheableKey vkey;
    VectorOfCacheableString vreg;
    regPtr0->getInterestList(vkey);
    regPtr0->getInterestListRegex(vreg);
    for (int32_t i = 0; i < vkey.length(); i++) {
      char buf[1024];
      const char* key = dynCast<CacheableStringPtr>(vkey[i])->asChar();
      sprintf(buf, "key[%d]=%s", i, key);
      LOG(buf);
      bool found = false;
      for (int32_t j = 0; j < vkey.length(); j++) {
        if (!strcmp(key, keys[j])) {
          found = true;
          break;
        }
      }
      sprintf(buf, "key[%d]=%s not found!", i, key);
      ASSERT(found, buf);
    }
    for (int32_t i = 0; i < vreg.length(); i++) {
      char buf[1024];
      CacheableStringPtr ptr = vreg[i];
      const char* reg = ptr->asChar();
      sprintf(buf, "regex[%d]=%s", i, reg);
      LOG(buf);
      bool found = false;
      for (int32_t j = 0; j < vreg.length(); j++) {
        if (!strcmp(reg, testregex[j])) {
          found = true;
          break;
        }
      }
      sprintf(buf, "regex[%d]=%s not found!", i, reg);
      ASSERT(found, buf);
    }
    regPtr0->registerAllKeys(true);
    VectorOfCacheableString vreg1;
    regPtr0->getInterestListRegex(vreg1);
    for (int32_t i = 0; i < vreg1.length(); i++) {
      char buf[1024];
      CacheableStringPtr ptr = vreg1[i];
      sprintf(buf, "regex[%d]=%s", i, ptr->asChar());
      LOG(buf);
    }
  }
END_TASK(SetupClient1)

DUNIT_TASK(SERVER1, StopServer)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      CacheHelper::closeLocator(1);
    }
    LOG("SERVER stopped");
  }
END_TASK(StopServer)
DUNIT_TASK(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK(CloseCache1)
