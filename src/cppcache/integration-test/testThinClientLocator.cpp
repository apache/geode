/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

bool isLocalServer = false;
bool isLocator = false;

const char* locHostPort =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define LOCATOR1 s2p1
#define SERVER12 s2p2
DUNIT_TASK(LOCATOR1, CreateLocator1)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK(CreateLocator1)
DUNIT_TASK(SERVER12, CreateServer12)
  {
    // starting servers
    if (isLocalServer) CacheHelper::initServer(1, NULL, locHostPort);
    if (isLocalServer) CacheHelper::initServer(2, NULL, locHostPort);
    LOG("Server12 started");
  }
END_TASK(CreateServer12)
DUNIT_TASK(CLIENT1, StepOne)
  {
    // starting client 1
    initClientWithPool(true, "__TEST_POOL1__", locHostPort, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], USE_ACK, locHostPort,
                                    "__TEST_POOL1__", true, true);
    getHelper()->createPooledRegion(regionNames[1], NO_ACK, locHostPort,
                                    "__TEST_POOL1__", true, true);
    LOG("StepOne complete.");
  }
END_TASK(StepOne)
DUNIT_TASK(CLIENT2, StepTwo)
  {
    initClientWithPool(true, "__TEST_POOL1__", locHostPort, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], USE_ACK, locHostPort,
                                    "__TEST_POOL1__", true, true);
    getHelper()->createPooledRegion(regionNames[1], NO_ACK, locHostPort,
                                    "__TEST_POOL1__", true, true);
    LOG("StepTwo complete.");
  }
END_TASK(StepTwo)

DUNIT_TASK(CLIENT1, StepThree)
  {
    createEntry(regionNames[0], keys[0], vals[0]);
    createEntry(regionNames[1], keys[2], vals[2]);
    LOG("StepThree complete.");
  }
END_TASK(StepThree)
DUNIT_TASK(CLIENT2, StepFour)
  {
    doNetsearch(regionNames[0], keys[0], vals[0]);
    doNetsearch(regionNames[1], keys[2], vals[2]);
    createEntry(regionNames[0], keys[1], vals[1]);
    createEntry(regionNames[1], keys[3], vals[3]);
    LOG("StepFour complete.");
  }
END_TASK(StepFour)
DUNIT_TASK(CLIENT1, StepFive)
  {
    doNetsearch(regionNames[0], keys[1], vals[1]);
    doNetsearch(regionNames[1], keys[3], vals[3]);
    updateEntry(regionNames[0], keys[0], nvals[0]);
    updateEntry(regionNames[1], keys[2], nvals[2]);
    LOG("StepFive complete.");
  }
END_TASK(StepFive)
DUNIT_TASK(CLIENT2, StepSix)
  {
    doNetsearch(regionNames[0], keys[0], vals[0], false);
    doNetsearch(regionNames[1], keys[2], vals[2], false);
    updateEntry(regionNames[0], keys[1], nvals[1]);
    updateEntry(regionNames[1], keys[3], nvals[3]);
    LOG("StepSix complete.");
  }
END_TASK(StepSix)
DUNIT_TASK(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK(CloseCache1)

DUNIT_TASK(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK(CloseCache2)
DUNIT_TASK(SERVER12, CloseServers)
  {
    // stop servers
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK(CloseServers)

DUNIT_TASK(LOCATOR1, CloseLocator1)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK(CloseLocator1)
