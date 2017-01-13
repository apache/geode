/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

/* This is to test
1) Client can have diffrent Redundancy levels across Pools.
2) Diffrent Pools can connect to diff server groups,
3) Server Groups may have overlapping servers.
*/

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define LOCATOR s2p1
#define SERVERS s2p2

bool isLocalServer = false;
bool isLocator = false;
const char* endPoints1 = CacheHelper::getTcrEndpoints(isLocalServer, 1);
const char* endPoints2 = CacheHelper::getTcrEndpoints(isLocalServer, 2);
const char* endPoints3 = CacheHelper::getTcrEndpoints(isLocalServer, 3);
const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
const char* poolRegNames[] = {"PoolRegion1", "PoolRegion2", "PoolRegion3"};
const char* poolNames[] = {"Pool1", "Pool2", "Pool3"};
const char* sGNames[] = {"ServerGroup1", "ServerGroup2", "ServerGroup3"};

void feedEntries(int keyIndex, bool newValue = false, bool update = false) {
  if (!update) {
    createEntry(poolRegNames[0], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
    createEntry(poolRegNames[1], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
    createEntry(poolRegNames[2], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
  } else {
    updateEntry(poolRegNames[0], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
    updateEntry(poolRegNames[1], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
    updateEntry(poolRegNames[2], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
  }
}

void verifyEntries(int keyIndex, bool netSearch = false,
                   bool newValue = false) {
  if (!netSearch) {
    verifyEntry(poolRegNames[0], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
    verifyEntry(poolRegNames[1], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
    verifyEntry(poolRegNames[2], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
  } else {
    doNetsearch(poolRegNames[0], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
    doNetsearch(poolRegNames[1], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
    doNetsearch(poolRegNames[2], keys[keyIndex],
                newValue ? nvals[keyIndex] : vals[keyIndex]);
  }
}

DUNIT_TASK_DEFINITION(LOCATOR, StartLocator)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVERS, StartServers)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "CacheServPoolRedun1.xml", locHostPort);
      LOG("SERVER1 started");
      CacheHelper::initServer(2, "CacheServPoolRedun2.xml", locHostPort);
      LOG("SERVER2 started");
      CacheHelper::initServer(3, "CacheServPoolRedun3.xml", locHostPort);
      LOG("SERVER3 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1_1)
  {
    initClient(true);

    // create three regions with three pools ( each having diff redun )
    createPool(poolNames[0], locHostPort, sGNames[0], 2, true);
    createRegionAndAttachPool(poolRegNames[0], USE_ACK, poolNames[0], true);

    createPool(poolNames[1], locHostPort, sGNames[0], 1, true);
    createRegionAndAttachPool(poolRegNames[1], USE_ACK, poolNames[1], true);

    createPool(poolNames[2], locHostPort, sGNames[0], 0, true);
    createRegionAndAttachPool(poolRegNames[2], USE_ACK, poolNames[2], true);

    feedEntries(0);

    RegionPtr regPtr0 = getHelper()->getRegion(poolRegNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(poolRegNames[1]);
    RegionPtr regPtr2 = getHelper()->getRegion(poolRegNames[2]);

    regPtr0->registerAllKeys(false, NULLPTR, true);
    regPtr1->registerAllKeys(false, NULLPTR, true);
    regPtr2->registerAllKeys(false, NULLPTR, true);

    LOG("CreateClient1 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2_1)
  {
    initClient(true);

    // create three regions with three pools ( each having diff redun )
    createPool(poolNames[0], locHostPort, NULL, 2, true);
    createRegionAndAttachPool(poolRegNames[0], USE_ACK, poolNames[0], true);

    createPool(poolNames[1], locHostPort, NULL, 1, true);
    createRegionAndAttachPool(poolRegNames[1], USE_ACK, poolNames[1], true);

    createPool(poolNames[2], locHostPort, NULL, 0, true);
    createRegionAndAttachPool(poolRegNames[2], USE_ACK, poolNames[2], true);

    feedEntries(1);

    RegionPtr regPtr0 = getHelper()->getRegion(poolRegNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(poolRegNames[1]);
    RegionPtr regPtr2 = getHelper()->getRegion(poolRegNames[2]);

    regPtr0->registerAllKeys(false, NULLPTR, true);
    regPtr1->registerAllKeys(false, NULLPTR, true);
    regPtr2->registerAllKeys(false, NULLPTR, true);

    LOG("CreateClient2 verify starts.");
    verifyEntries(0);

    LOG("CreateClient2 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1_2)
  {
    initClient(true);

    // create three regions with three pools ( each having diff redun )
    createPool(poolNames[0], locHostPort, sGNames[0], 2, true);
    createRegionAndAttachPool(poolRegNames[0], USE_ACK, poolNames[0], true);

    createPool(poolNames[1], locHostPort, sGNames[1], 1, true);
    createRegionAndAttachPool(poolRegNames[1], USE_ACK, poolNames[1], true);

    createPool(poolNames[2], locHostPort, sGNames[2], 0, true);
    createRegionAndAttachPool(poolRegNames[2], USE_ACK, poolNames[2], true);

    feedEntries(0);

    RegionPtr regPtr0 = getHelper()->getRegion(poolRegNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(poolRegNames[1]);
    RegionPtr regPtr2 = getHelper()->getRegion(poolRegNames[2]);

    regPtr0->registerAllKeys(false, NULLPTR, true);
    regPtr1->registerAllKeys(false, NULLPTR, true);
    regPtr2->registerAllKeys(false, NULLPTR, true);

    LOG("CreateClient1 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2_2)
  {
    initClient(true);

    // create three regions with three pools ( each having diff redun )
    createPool(poolNames[0], locHostPort, NULL, 2, true);
    createRegionAndAttachPool(poolRegNames[0], USE_ACK, poolNames[0], true);

    createPool(poolNames[1], locHostPort, NULL, 1, true);
    createRegionAndAttachPool(poolRegNames[1], USE_ACK, poolNames[1], true);

    createPool(poolNames[2], locHostPort, NULL, 0, true);
    createRegionAndAttachPool(poolRegNames[2], USE_ACK, poolNames[2], true);

    feedEntries(1);

    RegionPtr regPtr0 = getHelper()->getRegion(poolRegNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(poolRegNames[1]);
    RegionPtr regPtr2 = getHelper()->getRegion(poolRegNames[2]);

    regPtr0->registerAllKeys(false, NULLPTR, true);
    regPtr1->registerAllKeys(false, NULLPTR, true);
    regPtr2->registerAllKeys(false, NULLPTR, true);

    verifyEntries(0);

    LOG("CreateClient2 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyK1C1)
  {
    verifyEntries(1);
    LOG("Verify1Client1 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseServer1)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER1 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseServer2)
  {
    CacheHelper::closeServer(2);
    LOG("SERVER2 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, FeedC1)
  {
    feedEntries(0, true, true);
    SLEEP(1000);
    LOG("FeedC1 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyK0C2New)
  {
    verifyEntries(0, false, true);
    LOG("StepFive complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, FeedC2)
  {
    LOG("wait after step seven");
    SLEEP(10000);
    LOG("StepSeven complete.");
    feedEntries(1, true, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyK1C1New)
  {
    verifyEntries(1, false, true);
    LOG("Verify1Client1 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyK1C1New2)
  {
    RegionPtr regPtr = getHelper()->getRegion(poolRegNames[2]);

    CacheableKeyPtr keyPtr = createKey(keys[1]);

    CacheableStringPtr checkPtr =
        dynCast<CacheableStringPtr>(regPtr->get(keyPtr));

    ASSERT(checkPtr != NULLPTR, "Value Ptr should not be null.");

    char buf[1024];
    sprintf(buf, "get returned %s for key %s", checkPtr->asChar(), keys[1]);
    LOG(buf);

    if (strcmp(checkPtr->asChar(), nvals[1]) != 0) {
      LOG("ServerGroup2 is not available. So poolRegion2 returned old value..");
      return;
    }

    LOG("Verify1Client1 complete.");

    ASSERT(false, "Something is wrong with ServerGroup2");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVERS, CloseServers)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER2 stopped");
      CacheHelper::closeServer(2);
      LOG("SERVER3 stopped");
      CacheHelper::closeServer(3);
      LOG("SERVER4 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATOR, CloseLocator)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    for (int runIndex = 0; runIndex < 2; ++runIndex) {
      CALL_TASK(StartLocator)
      CALL_TASK(StartServers)
      if (runIndex == 0) {
        CALL_TASK(CreateClient1_1)
        CALL_TASK(CreateClient2_1)
      } else {
        CALL_TASK(CreateClient1_2)
        CALL_TASK(CreateClient2_2)
      }
      CALL_TASK(VerifyK1C1)

      // Failover

      if (runIndex == 0) {
        CALL_TASK(CloseServer1)
        CALL_TASK(CloseServer2)
      }

      CALL_TASK(FeedC1)
      CALL_TASK(VerifyK0C2New)
      if (runIndex == 1) {
        CALL_TASK(CloseServer1)
      }
      CALL_TASK(FeedC2)

      if (runIndex == 0) {
        CALL_TASK(VerifyK1C1New)
      } else {
        CALL_TASK(VerifyK1C1New2)
      }
      CALL_TASK(CloseCache1)
      CALL_TASK(CloseCache2)
      CALL_TASK(CloseServers)
      CALL_TASK(CloseLocator)
    }
  }
END_MAIN
