/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testThinClientDisconnectionListener"

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include <string>
#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#include <gfcpp/CacheListener.hpp>
// CacheHelper* cacheHelper = NULL;
static bool isLocator = false;
static bool isLocalServer = true;
static int numberOfLocators = 1;
static int isDisconnected = false;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
using namespace gemfire;
using namespace test;
class DisconnectCacheListioner : public CacheListener {
  void afterRegionDisconnected(const RegionPtr& region) {
    LOG("After Region Disconnected event received");
    isDisconnected = true;
  }
};
CacheListenerPtr cptr(new DisconnectCacheListioner());
#include "LocatorHelper.hpp"
DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pool_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[0], false /*ack mode*/,
                       locatorsG, "__TEST_POOL1__",
                       true /*client notification*/, cptr);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Verify)
  {
    SLEEP(10000);
    ASSERT(isDisconnected, "Client should have gotten disconenct event");
    isDisconnected = false;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, populateServer)
  {
    SLEEP(10000);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyPtr = createKey("PXR");
    CacheableStringPtr valPtr = CacheableString::create("PXR1");
    regPtr->create(keyPtr, valPtr);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer)
  {
    if (isLocalServer) CacheHelper::closeServer(1);
    LOG("SERVER stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StopClient1)
  {
    cleanProc();
    LOG("CLIENT1 stopped");
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(SetupClient1_Pool_Locator);
    CALL_TASK(Verify);
    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(populateServer);
    CALL_TASK(StopServer);
    CALL_TASK(Verify);
    CALL_TASK(StopClient1);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
