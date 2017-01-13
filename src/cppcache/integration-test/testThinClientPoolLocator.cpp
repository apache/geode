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

const char* locHostPort1 = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
const char* locHostPort2 = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 2);
#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define LOCATORS s2p1
#define SERVERS s2p2

DUNIT_TASK(CLIENT1, SetupClient1_NoLocators_At_Init)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locHostPort1, "Pool1");

    try {
      createEntry(regionNames[0], keys[0], vals[0]);
    } catch (NotConnectedException& ex) {
      try {
        ExceptionPtr exCause =
            dynCast<SharedPtr<NoAvailableLocatorsException> >(ex.getCause());
      } catch (ClassCastException&) {
        FAIL(
            "NotconnectedException with cause NoAvailableLocatorsException was "
            "not thrown.");
      }
    }
    LOG("SetupClient1 complete.");
  }
END_TASK(SetupClient1)

DUNIT_TASK(LOCATORS, CreateLocators)
  {
    if (isLocator) CacheHelper::initLocator(1);

    LOG("Locator1 started");

    if (isLocator) CacheHelper::initLocator(2);
  }
END_TASK(CreateLocators)

DUNIT_TASK(SERVERS, CreateServer1)
  {
    // starting servers
    if (isLocalServer) CacheHelper::initServer(1, NULL, locHostPort1);
    LOG("Server 1 started");
  }
END_TASK(CreateServer1)

DUNIT_TASK(CLIENT2, SetupClient2)
  {
    // starting client 1
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locHostPort1, "Pool2");

    LOG("SetupClient2 complete.");
  }
END_TASK(SetupClient2)

DUNIT_TASK(CLIENT1, ConnectC1)
  {
    createEntry(regionNames[0], keys[0], vals[0]);
    LOG("ConnectC1 complete.");
  }
END_TASK(ConnectC1)

DUNIT_TASK(CLIENT2, ConnectC2)
  {
    doNetsearch(regionNames[0], keys[0], vals[0]);
    createEntry(regionNames[0], keys[1], vals[1]);
    LOG("ConnectC2 complete.");
  }
END_TASK(ConnectC2)

DUNIT_TASK(SERVERS, CreateServer2)
  {
    // starting servers
    if (isLocalServer) CacheHelper::initServer(2, NULL, locHostPort2);
    LOG("Server 2 started");
  }
END_TASK(CreateServer2)

DUNIT_TASK(SERVERS, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK(CloseServer1)

DUNIT_TASK(CLIENT1, FailoverC1)
  {
    doNetsearch(regionNames[0], keys[1], vals[1]);
    updateEntry(regionNames[0], keys[0], nvals[0]);
    LOG("FailoverC1 complete.");
  }
END_TASK(FailoverC1)

DUNIT_TASK(CLIENT2, FailoverC2)
  {
    invalidateEntry(regionNames[0], keys[0]);
    doNetsearch(regionNames[0], keys[0], nvals[0], false);
    updateEntry(regionNames[0], keys[1], nvals[1]);
    LOG("FailoverC2 complete.");
  }
END_TASK(FailoverC2)

DUNIT_TASK(LOCATORS, CloseLocator1)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK(CloseLocator1)

DUNIT_TASK(SERVERS, ReStartS1)
  {
    // Re- starting servers
    if (isLocalServer) CacheHelper::initServer(1, NULL, locHostPort1);
    LOG("Server 1 started");
  }
END_TASK(ReStartS1)

DUNIT_TASK(SERVERS, CloseServer2)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK(CloseServer2)

DUNIT_TASK(CLIENT1, AgainFailoverC1)
  {
    try {
      invalidateEntry(regionNames[0], keys[1]);
      doNetsearch(regionNames[0], keys[1], nvals[1], false);
      updateEntry(regionNames[0], keys[0], vals[0]);
      FAIL("Client Failover Should Fail");
    } catch (const NotConnectedException&) {
      LOG("Expected exception NotConnectedException "
          "got");
    } catch (const Exception& excp) {
      LOG(excp.getName());
      LOG(excp.getMessage());
      FAIL(
          "Unexpected expection - only "
          "NotConnectedException expected");
    } catch (...) {
      FAIL(
          "Unknown expection - only "
          "NotConnectedException expected");
    }
    LOG("AgainFailoverC1 complete.");
  }
END_TASK(AgainFailoverC1)

DUNIT_TASK(LOCATORS, CloseLocator2)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(2);
      LOG("Locator2 stopped");
    }
  }
END_TASK(CloseLocator2)

DUNIT_TASK(CLIENT2, AgainFailoverC2)
  {
    try {
      invalidateEntry(regionNames[0], keys[0]);
      doNetsearch(regionNames[0], keys[0], vals[0], false);
      updateEntry(regionNames[0], keys[1], vals[1]);
      FAIL("Client Failover Should Fail");
    } catch (const NotConnectedException& ex) {
      try {
        ExceptionPtr exCause =
            dynCast<SharedPtr<NoAvailableLocatorsException> >(ex.getCause());
        LOG("Expected exception "
            "NoAvailableLocatorsException got");
      } catch (ClassCastException&) {
        LOG(ex.getName());
        LOG(ex.getMessage());
        FAIL(
            "Unexpected expection - only "
            "NoAvailableLocatorsException "
            "expected");
      }
    } catch (const Exception& excp) {
      LOG(excp.getName());
      LOG(excp.getMessage());
      FAIL(
          "Unexpected expection - only "
          "NoAvailableLocatorsException "
          "expected");
    } catch (...) {
      FAIL(
          "Unexpected expection - only "
          "NotConnectedException with cause "
          "NoAvailableLocatorsException "
          "expected");
    }
    LOG("AgainFailoverC2 complete.");
  }
END_TASK(AgainFailoverC2)
DUNIT_TASK(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK(CloseCache1)

DUNIT_TASK(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK(CloseCache2)

DUNIT_TASK(SERVERS, AgainCloseServer1)
  {
    // stop servers
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK(AgainCloseServer1)
