/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testThinClientPutWithDelta"

#include "DeltaEx.hpp"
#include "fw_dunit.hpp"
#include <string>
using namespace gemfire;
using namespace test;

CacheHelper* cacheHelper = NULL;
bool isLocalServer = false;

static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#include "LocatorHelper.hpp"
int DeltaEx::toDeltaCount = 0;
int DeltaEx::toDataCount = 0;
int DeltaEx::fromDeltaCount = 0;
int DeltaEx::fromDataCount = 0;
int DeltaEx::cloneCount = 0;
void initClient(const bool isthinClient) {
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void initClientNoPools() {
  cacheHelper = new CacheHelper(0);
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

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

void createRegion(const char* name, bool ackMode, const char* endpoints,
                  bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(
      name, ackMode, true, NULLPTR, endpoints, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}

const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

const bool USE_ACK = true;
const bool NO_ACK ATTR_UNUSED = false;

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1_UsePools)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG, "__TESTPOOL1_",
                       true);
    LOG("CreateRegions1_PoolLocators complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
  {
    LOG("Step one entered");
    try {
      Serializable::registerType(DeltaEx::create);
    } catch (IllegalStateException&) {
      //  ignore exception caused by type reregistration.
    }
    DeltaEx::toDeltaCount = 0;
    DeltaEx::toDataCount = 0;

    CacheableKeyPtr keyPtr = createKey(keys[0]);
    DeltaEx* ptr = new DeltaEx();
    CacheablePtr valPtr(ptr);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->put(keyPtr, valPtr);
    ptr->setDelta(true);
    regPtr->put(keyPtr, valPtr);

    // Test create with delta - toData() should be invoked instead of toDelta()
    regPtr->destroy(keyPtr);
    ptr->setDelta(true);
    regPtr->create(keyPtr, valPtr);

    DeltaEx* ptr1 = new DeltaEx(0);
    CacheablePtr valPtr1(ptr1);
    regPtr->put(keyPtr, valPtr1);
    ptr1->setDelta(true);
    regPtr->put(keyPtr, valPtr1);
    ASSERT(DeltaEx::toDeltaCount == 2, " Delta count should have been 2 ");
    ASSERT(DeltaEx::toDataCount == 4, " Data count should have been 3 ");
    LOG("Step one exited");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_DisableDelta)
  {
    LOG("Step one (disable delta on server) entered");
    DeltaEx::toDeltaCount = 0;
    DeltaEx::toDataCount = 0;
    try {
      Serializable::registerType(DeltaEx::create);
    } catch (IllegalStateException&) {
      //  Ignore the exception caused by re-registration of DeltaEx.
    }
    CacheableKeyPtr keyPtr = createKey(keys[0]);
    DeltaEx* ptr = new DeltaEx();
    CacheablePtr valPtr(ptr);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->put(keyPtr, valPtr);
    ptr->setDelta(true);
    regPtr->put(keyPtr, valPtr);

    ASSERT(DeltaEx::toDeltaCount == 0, " Delta count should have been 0 ");
    ASSERT(DeltaEx::toDataCount == 2, " Data count should have been 2 ");
    LOG("Step one exited");
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

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_ForDelta)
  {
    // starting servers
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_with_delta.xml", locatorsG);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_DisableDelta)
  {
    // starting servers
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_with_delta.xml", locatorsG, NULL,
                              false, false);
    }
  }
END_TASK_DEFINITION

void doDeltaPut(bool usePools = true) {
  CALL_TASK(CreateServer1_ForDelta)

  CALL_TASK(CreateClient1_UsePools);

  CALL_TASK(StepOne);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseServer1);

  CALL_TASK(CreateServer1_DisableDelta)

  CALL_TASK(CreateClient1_UsePools);

  CALL_TASK(StepOne_DisableDelta);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseServer1);
}

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);

    doDeltaPut();
    doDeltaPut(false);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
