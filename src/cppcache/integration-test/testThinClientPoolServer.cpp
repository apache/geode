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
1) Client should connect to server that in the specified server group,even if
the same region exists in other servers.
2) Region operation should pass if there is atleast one server in the
server-group having the region.
3) 2nd case should be true if no server-group is specified .
*/

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define LOCATOR1 s2p1
#define SERVER s2p2

bool isLocalServer = false;
bool isLocator = false;

const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
const char* poolRegNames[] = {"PoolRegion1", "PoolRegion2", "PoolRegion3"};
const char* poolName = "__TEST_POOL1__";

const char* serverGroup = "ServerGroup1";

DUNIT_TASK_DEFINITION(LOCATOR1, StartLocator1)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER, StartServers)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver1_pool.xml", locHostPort);
    }
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver2_pool.xml", locHostPort);
    }
    if (isLocalServer) {
      CacheHelper::initServer(3, "cacheserver3_pool.xml", locHostPort);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StartClient1)
  {
    initClient(true);
    // CLIENT1 create three regions
    getHelper()->createPool(poolName, locHostPort, serverGroup);

    getHelper()->createRegionAndAttachPool(poolRegNames[0], USE_ACK,
                                           "__TEST_POOL1__");
    getHelper()->createRegionAndAttachPool(poolRegNames[1], USE_ACK,
                                           "__TEST_POOL1__");
    getHelper()->createRegionAndAttachPool(poolRegNames[2], USE_ACK,
                                           "__TEST_POOL1__");

    LOG("Clnt1Init complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StartClient2)
  {
    initClient(true);
    // Create Pool with no server group
    getHelper()->createPool(poolName, locHostPort, NULL);

    getHelper()->createRegionAndAttachPool(poolRegNames[0], USE_ACK,
                                           "__TEST_POOL1__");
    getHelper()->createRegionAndAttachPool(poolRegNames[1], USE_ACK,
                                           "__TEST_POOL1__");
    getHelper()->createRegionAndAttachPool(poolRegNames[2], USE_ACK,
                                           "__TEST_POOL1__");

    LOG("Clnt2 Init complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1Entries)
  {
    try {
      createEntry(poolRegNames[0], keys[0], vals[0]);
    } catch (...) {
      LOG("Operations on Region 1 should Pass.");
      throw;
    }

    try {
      createEntry(poolRegNames[1], keys[0], vals[0]);
      FAIL("Operations on R2 should throw RegionNotFoundException");
    } catch (const gemfire::CacheServerException&) {
      LOG("Expected exception.");
    } catch (...) {
      LOG("Operation on Region 2 got unexpected exception.");
      throw;
    }
    LOG("CreateClient1Entries complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2Entries)
  {
    // operations on R1/ R2 should pass and R3 should Fail.
    try {
      createEntry(poolRegNames[0], keys[0], vals[0]);
    } catch (...) {
      LOG("Operations on Region 1 should Pass.");
      throw;
    }

    try {
      createEntry(poolRegNames[2], keys[0], vals[0]);
      FAIL("Operations on R3 should throw RegionNotFoundException");
    } catch (const gemfire::CacheServerException&) {
      LOG("Expected exception.");
    } catch (...) {
      LOG("Operation on Region 2 got unexpected exception.");
      throw;
    }
    LOG("CreateClient2Entries complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StopClient1)
  {
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = True");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StopClient2)
  {
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = True");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER, CloseServers)
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
    if (isLocalServer) {
      CacheHelper::closeServer(3);
      LOG("SERVER3 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATOR1, CloseLocator1)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(StartLocator1);
    CALL_TASK(StartServers);
    ;
    CALL_TASK(StartClient1);
    CALL_TASK(StartClient2);
    CALL_TASK(CreateClient1Entries);
    CALL_TASK(CreateClient2Entries);
    CALL_TASK(StopClient1);
    CALL_TASK(StopClient2);
    CALL_TASK(CloseServers);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
