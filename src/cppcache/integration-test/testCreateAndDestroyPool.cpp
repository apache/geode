/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <ace/Task.h>
#include <string>

#define ROOT_NAME "testCreateAndDestroyPool"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define LOCATOR s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

#include "locator_globals.hpp"
#include "LocatorHelper.hpp"

const char* poolNames[] = {"Pool1"};

void stepOne() {
  initClient(true);
  createPoolAndDestroy(poolNames[0], locatorsG, NULL, 0, false, -1, 1);
  LOG("StepOne complete.");
}
DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
  {
    LOG("Starting Step One");
    stepOne();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
    LOG("cleanProc 1...");
    cleanProc();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    LOG("closing Server...");
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1)
    CALL_TASK(CreateServer1_With_Locator)
    CALL_TASK(StepOne)
    CALL_TASK(CloseCache1)
    CALL_TASK(CloseServer1)
    CALL_TASK(CloseLocator1)
  }
END_MAIN
