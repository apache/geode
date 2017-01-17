/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
