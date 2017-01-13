/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef THINCLIENTHEAPLRU_HPP_
#define THINCLIENTHEAPLRU_HPP_

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include "BuiltinCacheableWrappers.hpp"

#include <string>

#define ROOT_NAME "ThinClientHeapLRU"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

CacheHelper* cacheHelper = NULL;
static bool isLocator = false;
static bool isLocalServer = false;
static int numberOfLocators = 0;
const char* poolName = "__TESTPOOL1_";
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);


const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};
const char* vals[] = {"Value-1", "Value-2", "Value-3", "Value-4"};
const char* nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                       "New Value-4"};

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

const bool USE_ACK = true;
const bool NO_ACK = false;

#include "LocatorHelper.hpp"

void initThinClientWithClientTypeAsCLIENT(const bool isthinClient) {
  if (cacheHelper == NULL) {
    PropertiesPtr pp = Properties::create();
    pp->insert("heap-lru-limit", 1);
    pp->insert("heap-lru-delta", 10);

    cacheHelper = new CacheHelper(isthinClient, pp);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

// void initClient( const bool isthinClient )
//{
//  if ( cacheHelper == NULL ) {
//    cacheHelper = new CacheHelper(isthinClient);
//  }
//  ASSERT( cacheHelper, "Failed to create a CacheHelper client instance." );
//}
void cleanProc() {
  if (cacheHelper != NULL) {
    delete cacheHelper;
    cacheHelper = NULL;
  }
}

CacheHelper* getHelper() {
  ASSERT(cacheHelper != NULL, "No cacheHelper initialized.");
  return cacheHelper;
}

void createRegion(const char* name, bool ackMode, const char* endpoints,
                  bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr = getHelper()->createRegion(
      name, ackMode, true, NULLPTR, endpoints, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}

void createPooledRegion(const char* name, bool ackMode, 
                        const char* locators, const char* poolname,
                        bool clientNotificationEnabled = false,
                        bool cachingEnable = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr = getHelper()->createPooledRegion(
      name, ackMode, locators, poolname, cachingEnable,
      clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Pooled Region created.");
}

void createOnekEntries() {
  CacheableHelper::registerBuiltins();
  RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);
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

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) CacheHelper::initServer(1);
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
  {
    initThinClientWithClientTypeAsCLIENT(true);
    createRegion(regionNames[0], USE_ACK, endPoints);
    createRegion(regionNames[1], NO_ACK, endPoints);
    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
  {
    initThinClientWithClientTypeAsCLIENT(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG, poolName);
    createPooledRegion(regionNames[1], NO_ACK,  locatorsG, poolName);
    LOG("StepOne_Pooled_Locators complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_EndPoint)
  {
    initThinClientWithClientTypeAsCLIENT(true);
    createPooledRegion(regionNames[0], USE_ACK, endPoints, NULL, poolName);
    createPooledRegion(regionNames[1], NO_ACK, endPoints, NULL, poolName);
    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo)
  {
    initThinClientWithClientTypeAsCLIENT(true);
    createRegion(regionNames[0], USE_ACK, endPoints);
    createRegion(regionNames[1], NO_ACK, endPoints);
    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_Locator)
  {
    initThinClientWithClientTypeAsCLIENT(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG, poolName);
    createPooledRegion(regionNames[1], NO_ACK,  locatorsG, poolName);
    LOG("StepTwo_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_EndPoint)
  {
    initThinClientWithClientTypeAsCLIENT(true);
    createPooledRegion(regionNames[0], USE_ACK, endPoints, NULL, poolName);
    createPooledRegion(regionNames[1], NO_ACK, endPoints, NULL, poolName);
    LOG("StepTwo_Pooled_EndPoint complete.");
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

void runHeapLRU(bool poolConfig = true, bool isLocator = true) {
  if (poolConfig && isLocator) {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator)
  } else {
    CALL_TASK(CreateServer1);
  }
  if (!poolConfig) {
    CALL_TASK(StepOne);
    CALL_TASK(StepTwo);
  } else if (isLocator) {
    CALL_TASK(StepOne_Pooled_Locator);
    CALL_TASK(StepTwo_Pooled_EndPoint);
  } else {
    CALL_TASK(StepOne_Pooled_EndPoint);
    CALL_TASK(StepTwo_Pooled_EndPoint);
  }
  CALL_TASK(StepThree);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);
  if (poolConfig && isLocator) {
    CALL_TASK(CloseLocator1);
  }
}
#endif /* THINCLIENTHEAPLRU_HPP_ */
