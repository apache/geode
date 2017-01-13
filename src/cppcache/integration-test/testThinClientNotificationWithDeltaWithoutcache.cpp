/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * testThinClientDeltaWithNotification.cpp
 *
 *  Created on: May 18, 2009
 *      Author: pkumar
 */

#define ROOT_NAME "testThinClientNotificationWithDeltaWithoutcache"

#include "DeltaEx.hpp"
#include "fw_dunit.hpp"
#include <string>
using namespace gemfire;
using namespace test;

CacheHelper* cacheHelper = NULL;

#include "locator_globals.hpp"

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

void createPooledRegion(const char* name, bool ackMode, const char* locators,
                        const char* poolname,
                        bool clientNotificationEnabled = false,
                        bool cachingEnable = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr =
      getHelper()->createPooledRegion(name, ackMode, locators, poolname,
                                      cachingEnable, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Pooled Region created.");
}

void createRegionCachingDisabled(const char* name, bool ackMode,
                                 bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, false, NULLPTR,
                                               clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}

const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};

const char* regionNames[] = {"DistRegionAck", "DistRegionAck1"};

const bool USE_ACK = true;
const bool NO_ACK ATTR_UNUSED = false;

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_", true,
                       false);  // without LRU
    try {
      Serializable::registerType(DeltaEx::create);
    } catch (IllegalStateException&) {
      //  ignore exception caused by type reregistration.
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1_NoPools)
  {
    initClientNoPools();
    createRegionCachingDisabled(regionNames[0], USE_ACK, true);  // without LRU
    try {
      Serializable::registerType(DeltaEx::create);
    } catch (IllegalStateException&) {
      //  ignore exception caused by type reregistration.
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_", true,
                       false);
    try {
      Serializable::registerType(DeltaEx::create);
    } catch (IllegalStateException&) {
      //  ignore exception caused by type reregistration.
    }
    DeltaEx::fromDataCount = 0;
    DeltaEx::fromDeltaCount = 0;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2_NoPools)
  {
    initClientNoPools();
    createRegionCachingDisabled(regionNames[0], USE_ACK, true);  // without LRU
    try {
      Serializable::registerType(DeltaEx::create);
    } catch (IllegalStateException&) {
      //  ignore exception caused by type reregistration.
    }
    DeltaEx::fromDataCount = 0;
    DeltaEx::fromDeltaCount = 0;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Client2_RegisterInterest)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    VectorOfCacheableKey vec;
    CacheableKeyPtr keyPtr = createKey(keys[0]);
    // CacheableKeyPtr keyPtr1 = createKey( keys[1] );
    vec.push_back(keyPtr);
    // vec.push_back( keyPt1 );
    regPtr->registerKeys(vec);
    // regPtr->registerAllKeys();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Client1_Put)
  {
    CacheableKeyPtr keyPtr = createKey(keys[0]);
    DeltaEx* ptr = new DeltaEx();
    CacheablePtr valPtr(ptr);

    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->put(keyPtr, valPtr);
    ptr->setDelta(true);
    regPtr->put(keyPtr, valPtr);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Client2_VerifyDelta)
  {
    // Wait for notification
    SLEEP(1000);
    char buff[100];
    sprintf(buff, "From delta count %d  From data count %d",
            DeltaEx::fromDeltaCount, DeltaEx::fromDataCount);
    LOG(buff);
    // In case of Cacheless client only full object would arrive on notification
    // channel.
    ASSERT(DeltaEx::fromDataCount == 2,
           "DeltaEx::fromDataCount should have been 2");
    ASSERT(DeltaEx::fromDeltaCount == 0,
           "DeltaEx::fromDeltaCount should have been 0");
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

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);

    CALL_TASK(CreateServer1_ForDelta)

    CALL_TASK(CreateClient1);
    CALL_TASK(CreateClient2);

    CALL_TASK(Client2_RegisterInterest);

    CALL_TASK(Client1_Put);
    CALL_TASK(Client2_VerifyDelta);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);

    CALL_TASK(CloseServer1);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
