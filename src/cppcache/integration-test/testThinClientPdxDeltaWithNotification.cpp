/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * testThinClientPdxDeltaWithNotification.cpp
 *
 *  Created on: May 8, 2009
 *      Author: abhaware
 */

#define ROOT_NAME "testThinClientPdxDeltaWithNotification"

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

int PdxDeltaEx::m_toDeltaCount = 0;
int PdxDeltaEx::m_toDataCount = 0;
int PdxDeltaEx::m_fromDeltaCount = 0;
int PdxDeltaEx::m_fromDataCount = 0;
int PdxDeltaEx::m_cloneCount = 0;

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

void createPooledExpirationRegion(const char* name, const char* poolname) {
  LOG("createPooledExpirationRegion() entered.");
  // Entry time-to-live = 1 second.
  RegionPtr regPtr = getHelper()->createPooledRegionDiscOverFlow(
      name, true, locatorsG, poolname, true, true, 1, 0, 0, 0, 0, NULL,
      ExpirationAction::LOCAL_INVALIDATE);
}

void createPooledLRURegion(const char* name, bool ackMode, const char* locators,
                           const char* poolname,
                           bool clientNotificationEnabled = false,
                           bool cachingEnable = true) {
  LOG(" createPooledLRURegion entered");
  RegionPtr regPtr = getHelper()->createPooledRegionDiscOverFlow(
      name, ackMode, locators, poolname, cachingEnable,
      clientNotificationEnabled, 0, 0, 0, 0, 3 /*LruLimit = 3*/);
  LOG(" createPooledLRURegion exited");
}

void createRegion(const char* name, bool ackMode,
                  bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, true, NULLPTR,
                                               clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}

void createLRURegion(const char* name, bool clientNotificationEnabled = false,
                     bool cachingEnable = true) {
  LOG(" createPooledLRURegion entered");
  RegionPtr regPtr = getHelper()->createRegionDiscOverFlow(
      name, cachingEnable, clientNotificationEnabled, 0, 0, 0, 0,
      3 /*LruLimit = 3*/);
  LOG(" createPooledLRURegion exited");
}

void createExpirationRegion(const char* name,
                            bool clientNotificationEnabled = false,
                            bool cachingEnable = true) {
  LOG(" createPooledLRURegion entered");
  RegionPtr regPtr = getHelper()->createRegionDiscOverFlow(
      name, cachingEnable, clientNotificationEnabled, 1, 0, 0, 0, 0,
      ExpirationAction::LOCAL_INVALIDATE);
  LOG(" createPooledLRURegion exited");
}

const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};

const char* regionNames[] = {"DistRegionAck", "DistRegionAck1",
                             "DistRegionAck2"};

const bool USE_ACK = true;

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG, "__TESTPOOL1_",
                       true);  // without LRU
    createPooledLRURegion(regionNames[1], USE_ACK, NULL, locatorsG,
                          "__TESTPOOL1_", true);  // with LRU
    createPooledExpirationRegion(regionNames[2], "__TESTPOOL1_");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1_NoPools)
  {
    initClientNoPools();
    createRegion(regionNames[0], USE_ACK, true);  // without LRU
    createLRURegion(regionNames[1], true, true);  // with LRU
    createExpirationRegion(regionNames[2], true, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG, "__TESTPOOL1_",
                       true);
    createPooledLRURegion(regionNames[1], USE_ACK, NULL, locatorsG,
                          "__TESTPOOL1_", true);  // with LRU
    createPooledExpirationRegion(regionNames[2], "__TESTPOOL1_");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2_NoPools)
  {
    initClientNoPools();
    createRegion(regionNames[0], USE_ACK, true);  // without LRU
    createLRURegion(regionNames[1], true, true);  // with LRU
    createExpirationRegion(regionNames[2], true, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Client1_PdxInit)
  {
    try {
      Serializable::registerPdxType(PdxDeltaEx::createDeserializable);
    } catch (IllegalStateException&) {
      //  ignore type reregistration exception.
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Client2_PdxInit)
  {
    try {
      Serializable::registerPdxType(PdxDeltaEx::createDeserializable);
    } catch (IllegalStateException&) {
      //  ignore type reregistration exception.
    }
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
    RegionPtr regPtr2 = getHelper()->getRegion(regionNames[2]);
    regPtr->registerRegex(".*");
    regPtr1->registerRegex(".*");
    regPtr2->registerRegex(".*");

    // Reset counters
    PdxDeltaEx::m_toDeltaCount = 0;
    PdxDeltaEx::m_toDataCount = 0;
    PdxDeltaEx::m_fromDeltaCount = 0;
    PdxDeltaEx::m_fromDataCount = 0;
    PdxDeltaEx::m_cloneCount = 0;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Client1_PdxPut)
  {
    CacheableKeyPtr keyPtr = createKey(keys[0]);
    PdxDeltaEx* ptr = new PdxDeltaEx();
    // CacheablePtr pdxobj(new PdxDeltaEx());
    CacheablePtr valPtr(ptr);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->put(keyPtr, valPtr);
    // Client 2: fromDataCount = 1, fromDeltaCount = 0;
    ptr->setDelta(true);
    regPtr->put(keyPtr, valPtr);
    // Client 2: fromDataCount = 1, fromDeltaCount = 1;

    PdxDeltaEx* ptr1 = new PdxDeltaEx();
    CacheablePtr valPtr1(ptr1);
    CacheableKeyPtr keyPtr1 = createKey(keys[1]);
    regPtr->put(keyPtr1, valPtr1);
    // Client 2: fromDataCount = 2, fromDeltaCount = 1;
    ptr1->setDelta(true);
    regPtr->put(keyPtr1, valPtr1);
    // Client 2: fromDataCount = 3, fromDeltaCount = 2;
    // The put() operation causes an event with delta to be delivered to client
    // 2.
    // PdxDeltaEx::fromDelta() determines that this is the second delta event,
    // and
    // throws InvalidDeltaException,
    // which causes a full object to be fetched from the server. Hence both
    // fromDataCount and fromDeltaCount
    // are incremented.

    // For LRU with notification and disc overflow.
    LOG("LRU with notification");
    CacheableKeyPtr keyPtr2 = createKey(keys[2]);
    CacheableKeyPtr keyPtr3 = createKey(keys[3]);
    CacheableKeyPtr keyPtr4 = createKey("LRUKEY4");
    CacheableKeyPtr keyPtr5 = createKey("LRUKEY5");
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    regPtr1->put(keyPtr2, valPtr1);
    // Client 2: fromDataCount = 4, fromDeltaCount = 2;
    regPtr1->put(keyPtr, valPtr1);
    // Client 2: fromDataCount = 5, fromDeltaCount = 2;
    regPtr1->put(keyPtr1, valPtr1);
    // Client 2: fromDataCount = 6, fromDeltaCount = 2;
    regPtr1->put(keyPtr3, valPtr1);
    // Client 2: fromDataCount = 7, fromDeltaCount = 2;
    regPtr1->put(keyPtr4, valPtr1);
    // Client 2: fromDataCount = 8, fromDeltaCount = 2;
    regPtr1->put(keyPtr5, valPtr1);
    // Client 2: fromDataCount = 9, fromDeltaCount = 2;
    regPtr1->put(keyPtr2, valPtr);
    // Client 2: fromDataCount = 11, fromDeltaCount = 3;
    // The put() operation causes an event with delta to be delivered to client
    // 2.
    // However, the value for keyPtr2 must be fetched from disk, which results
    // in
    // a call to fromData().
    // Delta is applied on the old value to get the new value. Also, to create
    // oldValue for listeners, fromData()
    // is invoked, hence fromDataCount increases by 2.

    RegionPtr regPtr2 = getHelper()->getRegion(regionNames[2]);
    PdxDeltaEx* ptr2 = new PdxDeltaEx();
    CacheablePtr valPtr2(ptr2);
    regPtr2->put(1, valPtr2);
    // Client 2: fromDataCount = 12, fromDeltaCount = 3;
    // Sleep for 5 seconds to allow expiration at client 2.
    SLEEP(5000);
    ptr2->setDelta(true);
    regPtr2->put(1, valPtr2);
    // Client 2: fromDataCount = 13, fromDeltaCount = 3;
    // The put() operation causes an event with delta to be delivered to client
    // 2.
    // Delta cannot be applied to the value because entry has been invalidated.
    // Hence full object is fetched from server, causing fromDataCount to
    // increase
    // by 1,
    // with fromDeltaCount remaining constant.
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Client2_PdxVerifyDelta)
  {
    // Wait for notification
    SLEEP(5000);
    char buff[100];
    sprintf(buff, "From Pdxdelta count %d  From data count %d",
            PdxDeltaEx::m_fromDeltaCount, PdxDeltaEx::m_fromDataCount);
    LOG(buff);
    ASSERT(PdxDeltaEx::m_fromDataCount == 13,
           "Client2_PdxVerifyDelta PdxDeltaEx::m_fromDataCount should have "
           "been 13");
    // 1 for the first case when delta is false, 2 for the second case when
    // delta
    // is false and 3 for the second case when
    // delta could not be applied and full object is sought. For LRU case there
    // are 6 put which is create and one put is update
    // and that update is done on overflown value. For update on overflown value
    // fromData will be called twice and and fromDelta once.
    // Because in order to apply delta we have to read value from disc which
    // will
    // cause one fromData to be called and second time when
    // After put to return old value fromDelta is called..However, this can be
    // optimized away. Will see it later.
    // for expiry region, two calls to fromData().
    ASSERT(PdxDeltaEx::m_fromDeltaCount == 3,
           "Client2_PdxVerifyDelta PdxDeltaEx::m_fromDeltaCount should have "
           "been 3");

    // 1 for the first case 2 for the second case, 3 for LRU case.

    ASSERT(
        PdxDeltaEx::m_cloneCount == 1,
        " Client2_PdxVerifyDelta: Clone should be called once for LRU region ");
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

void doPdxDeltaWithNotification() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_ForDelta)

  CALL_TASK(CreateClient1);
  CALL_TASK(CreateClient2);

  CALL_TASK(Client1_PdxInit);
  CALL_TASK(Client2_PdxInit);

  CALL_TASK(Client1_PdxPut);
  CALL_TASK(Client2_PdxVerifyDelta);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);

  CALL_TASK(CloseServer1);
  CALL_TASK(CloseLocator1);
}

DUNIT_MAIN
  {
    doPdxDeltaWithNotification();  // Test with Pdx Serialization
  }
END_MAIN
