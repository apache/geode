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
1- Notification Connection is stablized with server in the given server group.
*/

#define CLIENT1 s1p1
#define LOCATOR1 s2p1
#define SERVER s2p2

bool isLocalServer = false;
bool isLocator = false;

const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
const char* poolRegNames[] = {"PoolRegion1", "PoolRegion2"};
const char* poolName = "__TEST_POOL1__";

const char* serverGroup = "ServerGroup1";

DUNIT_TASK(LOCATOR1, StartLocator1)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK(StartLocator1)

DUNIT_TASK(SERVER, StartS12)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver1_pool.xml", locHostPort);
    }
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver3_pool.xml", locHostPort);
    }
  }
END_TASK(StartS12)

DUNIT_TASK(CLIENT1, StartC1)
  {
    initClient(true);

    createPool(poolName, locHostPort, serverGroup, 0, true);
    createRegionAndAttachPool(poolRegNames[0], USE_ACK, poolName);

    RegionPtr regPtr0 = getHelper()->getRegion(poolRegNames[0]);
    regPtr0->registerAllKeys();

    LOG("Clnt1Init complete.");
  }
END_TASK(StartC1)

DUNIT_TASK(CLIENT1, Client1OpTest)
  {
    // TODO : Write code to Check, that client notification connection is
    // with S1(not with S2)
  }
END_TASK(Client1OpTest)

DUNIT_TASK(CLIENT1, StopC1)
  {
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = True");
  }
END_TASK(StopC1)

DUNIT_TASK(SERVER, CloseServers)
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
