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
#include "ThinClientHelper.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

#include "locator_globals.hpp"
#include "LocatorHelper.hpp"

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithNBSTrue)
  {
    // starting server with notify_subscription true
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locatorsG,
                                    "__TEST_POOL1__", true, true);
    LOG("Client1 started");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, SetupClient2)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locatorsG,
                                    "__TEST_POOL1__", true, true);
    LOG("Client2 started");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, doPutOperations)
  {
    createEntry(regionNames[0], keys[0], vals[0]);
    LOG("do put operation from client 1 completed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, doGetOperations)
  {
    doNetsearch(regionNames[0], keys[0], vals[0]);
    LOG("do get operation from client 2 completed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, doLocalDestroyAndDestroy)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->localDestroy(keys[0]);
    regPtr->destroy(keys[0]);
    verifyDestroyed(regionNames[0], keys[0]);
    LOG("do local destroy operation from client 2 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, verifyKeyDestroyedOnServer)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheablePtr value = regPtr->get(keys[0]);
    if (value != NULLPTR) {
      FAIL("Entry not destroyed on server.");
    }
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, verifyKeyDestroyedfromClient1)
  {
    try {
      RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
      regPtr->get(keys[0]);
    } catch (const EntryNotFoundException&) {
      // expected
    }
    LOG("verifyKeyDestroyedOnServer from client 1 completed");
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

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);

    CALL_TASK(CreateServerWithNBSTrue);
    CALL_TASK(SetupClient1);
    CALL_TASK(SetupClient2);
    CALL_TASK(doPutOperations);
    CALL_TASK(doGetOperations);
    CALL_TASK(doLocalDestroyAndDestroy);
    CALL_TASK(verifyKeyDestroyedOnServer);
    CALL_TASK(verifyKeyDestroyedfromClient1);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
