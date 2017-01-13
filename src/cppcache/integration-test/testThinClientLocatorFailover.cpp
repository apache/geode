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

const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 3);
#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define LOCATORS s2p1
#define SERVERS s2p2
DUNIT_TASK(LOCATORS, CreateLocator_All)
  {
    if (isLocalServer) {
      CacheHelper::initLocator(1);
      CacheHelper::initLocator(2);
      CacheHelper::initLocator(3);
    }
  }
END_TASK(CreateLocator_All)

DUNIT_TASK(SERVERS, CreateServer1_All)
  {
    // starting servers
    if (isLocalServer) CacheHelper::initServer(1, NULL, locatorsG);
    LOG("Server One started");
  }
END_TASK(CreateServer1_All)

DUNIT_TASK(CLIENT1, SetupClient1_All)
  {
    // starting client 1
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG, "Pool1");
    LOG("SetupClient1 complete.");
  }
END_TASK(SetupClient1_All)

DUNIT_TASK(CLIENT2, SetupClient2_All)
  {
    // starting client 2
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG, "Pool2");
    LOG("SetupClient2 complete.");
  }
END_TASK(SetupClient2_All)

DUNIT_TASK(CLIENT1, ConnectC1_All)
  {
    createEntry(regionNames[0], keys[0], vals[0]);
    LOG("ConnectC1 complete.");
  }
END_TASK(ConnectC1_All)

DUNIT_TASK(CLIENT2, ConnectC2_All)
  {
    doNetsearch(regionNames[0], keys[0], vals[0]);
    createEntry(regionNames[0], keys[1], vals[1]);
    LOG("ConnectC2 complete.");
  }
END_TASK(ConnectC2_All)

DUNIT_TASK(SERVERS, CreateServer2_All)
  {
    if (isLocalServer) {
      CacheHelper::initServer(2, NULL, locatorsG);
      LOG("Server 2 started");
    }
  }
END_TASK(CreateServer2_All)

DUNIT_TASK(LOCATORS, CloseLocators)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1);
      CacheHelper::closeLocator(2);
    }
  }
END_TASK(CloseLocators)

DUNIT_TASK(SERVERS, CloseServer1_All)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK(CloseServer1_All)

DUNIT_TASK(CLIENT1, FailoverC1_All)
  {
    doNetsearch(regionNames[0], keys[1], vals[1]);
    updateEntry(regionNames[0], keys[0], nvals[0]);
    LOG("FailoverC1_All complete.");
  }
END_TASK(FailoverC1_All)

DUNIT_TASK(CLIENT2, FailoverC2_All)
  {
    invalidateEntry(regionNames[0], keys[0]);
    doNetsearch(regionNames[0], keys[0], nvals[0], false);
    updateEntry(regionNames[0], keys[1], nvals[1]);
    LOG("FailoverC2_All complete.");
  }
END_TASK(FailoverC2_All)

DUNIT_TASK(LOCATORS, SwapLocators)
  {
    if (isLocator) {
      CacheHelper::initLocator(2);
      CacheHelper::closeLocator(3);
    }
  }
END_TASK(SwapLocators)

DUNIT_TASK(SERVERS, Re_CreateServer1_All)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, NULL, locatorsG);
      LOG("Server 1 started");
      SLEEP(30000);
    }
  }
END_TASK(Re_CreateServer1_All)

DUNIT_TASK(SERVERS, CloseServer2_All)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
      SLEEP(30000);
    }
  }
END_TASK(CloseServer2_All)

DUNIT_TASK(CLIENT1, AgainFailoverC1_All)
  {
    SLEEP(30000);
    invalidateEntry(regionNames[0], keys[1]);
    doNetsearch(regionNames[0], keys[1], nvals[1], false);
    updateEntry(regionNames[0], keys[0], vals[0]);
  }
END_TASK(AgainFailoverC1_All)

DUNIT_TASK(LOCATORS, SwapLocators2)
  {
    if (isLocator) {
      CacheHelper::initLocator(3);
      CacheHelper::closeLocator(2);
    }
  }
END_TASK(SwapLocators2)

DUNIT_TASK(CLIENT2, AgainFailoverC2_All)
  {
    invalidateEntry(regionNames[0], keys[0]);
    doNetsearch(regionNames[0], keys[0], vals[0], false);
    updateEntry(regionNames[0], keys[1], vals[1]);
  }
END_TASK(AgainFailoverC2_All)

DUNIT_TASK(LOCATORS, CloseLocators)
  {
    if (isLocator) {
      CacheHelper::closeLocator(3);
    }
  }
END_TASK(CloseLocators)

DUNIT_TASK(SERVERS, Re_Close1_All_All)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("Server 1 Stopped");
      SLEEP(30000);
    }
  }
END_TASK(Re_Close1_All_All)

DUNIT_TASK(SERVERS, StartServer2_All)
  {
    if (isLocalServer) {
      CacheHelper::initServer(2, NULL, NULL);
      LOG("SERVER2 Started");
      SLEEP(30000);
    }
  }
END_TASK(StartServer2_All)

DUNIT_TASK(CLIENT1, AgainAgainFailoverC1_All)
  {
    try {
      invalidateEntry(regionNames[0], keys[0]);
      doNetsearch(regionNames[0], keys[0], vals[0], false);
      updateEntry(regionNames[0], keys[1], vals[1]);
      FAIL(
          "No locator exception should "
          "have been raised");
    } catch (const NotConnectedException& ex) {
      try {
        ExceptionPtr exCause =
            dynCast<SharedPtr<NoAvailableLocatorsException> >(ex.getCause());
        LOG("Expected "
            "NoAvailableLocatorsExcepti"
            "on");
      } catch (ClassCastException&) {
        FAIL(
            "NotConnectedException "
            "with cause "
            "NoAvailableLocatorsExcepti"
            "on should have been "
            "raised");
      }
    }
  }
END_TASK(AgainAgainFailoverC1_All)

DUNIT_TASK(CLIENT1, CloseCache1_All)
  { cleanProc(); }
END_TASK(CloseCache1_All)

DUNIT_TASK(CLIENT2, CloseCache2_All)
  { cleanProc(); }
END_TASK(CloseCache2_All)

DUNIT_TASK(SERVERS, CloseServer2_All_AgaIN)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK(CloseServer2_All_AgaIN)
