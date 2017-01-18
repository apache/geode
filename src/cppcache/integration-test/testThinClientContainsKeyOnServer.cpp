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

using namespace apache::geode::client;
using namespace test;

#include "locator_globals.hpp"

DUNIT_TASK(SERVER1, StartServer)
  {
    if (isLocalServer) {
      CacheHelper::initLocator(1);
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
    }
    LOG("SERVER started");
  }
END_TASK(StartServer)

DUNIT_TASK(CLIENT1, SetupClient1)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locatorsG,
                                    "__TEST_POOL1__", true, true);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr key = CacheableKey::create((const char*)"key01");
    ASSERT(!regPtr->containsKeyOnServer(key), "key should not be there");
  }
END_TASK(SetupClient1)

DUNIT_TASK(CLIENT2, SetupClient2)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locatorsG,
                                    "__TEST_POOL1__", true, true);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr key = CacheableKey::create((const char*)"key01");
    ASSERT(!regPtr->containsKeyOnServer(key), "key should not be there");
  }
END_TASK(SetupClient2)

DUNIT_TASK(CLIENT1, puts)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyPtr = CacheableKey::create((const char*)"key01");
    CacheableBytesPtr valPtr =
        CacheableBytes::create(reinterpret_cast<const uint8_t*>("value01"), 7);
    regPtr->put(keyPtr, valPtr);
    ASSERT(regPtr->containsKeyOnServer(keyPtr), "key should be there");
  }
END_TASK(puts)

DUNIT_TASK(CLIENT2, VerifyPuts)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyPtr = CacheableKey::create((const char*)"key01");
    ASSERT(regPtr->containsKeyOnServer(keyPtr), "key should be there");
  }
END_TASK(VerifyPuts)

DUNIT_TASK(SERVER1, StopServer)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      CacheHelper::closeLocator(1);
    }
    LOG("SERVER stopped");
  }
END_TASK(StopServer)
DUNIT_TASK(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK(CloseCache1)
DUNIT_TASK(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK(CloseCache2)
