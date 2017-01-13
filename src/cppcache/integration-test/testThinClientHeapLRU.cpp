/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include "BuiltinCacheableWrappers.hpp"

#include <string>

#define ROOT_NAME "TestThinClientHeapLRU"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

#include "locator_globals.hpp"
#include "LocatorHelper.hpp"

const char* _regionNames[] = {"DistRegionAck"};

void createOnekEntries() {
  CacheableHelper::registerBuiltins();
  RegionPtr dataReg = getHelper()->getRegion(_regionNames[0]);
  for (int i = 0; i < 2048; i++) {
    CacheableWrapper* tmpkey =
        CacheableWrapperFactory::createInstance(GemfireTypeIds::CacheableInt32);
    CacheableWrapper* tmpval =
        CacheableWrapperFactory::createInstance(GemfireTypeIds::CacheableBytes);
    tmpkey->initKey(i, 32);
    tmpval->initRandomValue(1024);
    ASSERT(tmpkey->getCacheable() != NULLPTR, "tmpkey->getCacheable() is NULL");
    ASSERT(tmpval->getCacheable() != NULLPTR, "tmpval->getCacheable() is NULL");
    dataReg->put(dynCast<CacheableKeyPtr>(tmpkey->getCacheable()),
                 tmpval->getCacheable());
    // delete tmpkey;
    //  delete tmpval;
  }
  dunit::sleep(10000);
  VectorOfRegionEntry me;
  dataReg->entries(me, false);
  LOG("Verifying size outside loop");
  char buf[1024];
  sprintf(buf, "region size is %d", me.size());
  LOG(buf);

  ASSERT(me.size() <= 1024, "Should have evicted anything over 1024 entries");
}


DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
  {
    PropertiesPtr pp = Properties::create();
    pp->insert("heap-lru-limit", 1);
    pp->insert("heap-lru-delta", 10);
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1", pp, 0,
                       true);
    getHelper()->createPooledRegion(_regionNames[0], USE_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);
    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo)
  {
    PropertiesPtr pp = Properties::create();
    pp->insert("heap-lru-limit", 1);
    pp->insert("heap-lru-delta", 10);
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1", pp, 0,
                       true);
    getHelper()->createPooledRegion(_regionNames[0], USE_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);
    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    // verfy that eviction works
    createOnekEntries();
    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1)
    CALL_TASK(CreateServer1_With_Locator);
    CALL_TASK(StepOne);
    CALL_TASK(StepTwo);
    CALL_TASK(StepThree);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
