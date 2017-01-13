/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

using namespace gemfire;
using namespace test;

bool isLocalServer = true;
static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
#include "LocatorHelper.hpp"

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pool_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[0], false /*ack mode*/, locatorsG,
                       "__TEST_POOL1__", true /*client notification*/);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, setupClient2_Pool_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[0], false /*ack mode*/, locatorsG,
                       "__TEST_POOL1__", true /*client notification*/);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, populateServer)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    for (int i = 0; i < 5; i++) {
      CacheableKeyPtr keyPtr = CacheableKey::create(keys[i]);
      regPtr->create(keyPtr, vals[i]);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, verify)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    for (int i = 0; i < 5; i++) {
      CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[i]);
      char buf[1024];
      sprintf(buf, "key[%s] should not have been found", keys[i]);
      ASSERT(!regPtr->containsKey(keyPtr1), buf);
      CacheablePtr checkPtr = regPtr->get(keyPtr1);
      verifyEntry(regionNames[0], keys[i], vals[i]);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StopClient1)
  {
    cleanProc();
    LOG("CLIENT1 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StopClient2)
  {
    cleanProc();
    LOG("CLIENT2 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer)
  {
    if (isLocalServer) CacheHelper::closeServer(1);
    LOG("SERVER stopped");
  }
END_TASK_DEFINITION
