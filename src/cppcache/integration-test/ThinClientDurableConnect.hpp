/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientDurableConnect.hpp
 *
 *  Created on: Nov 3, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTDURABLECONNECT_HPP_
#define THINCLIENTDURABLECONNECT_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "CacheImplHelper.hpp"
#include "testUtils.hpp"

/* Test case covered:
With Four Servers
1-  ( R = 1 ) All Server Up, Check if client reconnect to either of server
having queue.
2-  (  R = 1 ) One Redundant Server down, Check if client reconnect to another
server having queue.
3-  (  R = 1 ) Both Redundant Server down, Check if client reconnect to both non
redundant server and all events are lost.
4-  (  R = 1 ) Both Non-Redundant Server down, Check if client reconnect to both
redundant server.
5-  ( R = 1 ) All Server Up, Check if client reconnect to either of server after
timeout period and all the events are lost.
*/

#define CLIENT s1p1
#define SERVER_SET1 s1p2
#define SERVER_SET2 s2p1
#define SERVER_SET3 s2p2
#define SERVER1 s1p2

bool isLocalServerList = false;
const char* endPointsList = CacheHelper::getTcrEndpoints(isLocalServerList, 4);
const char* durableId = "DurableId";

#include "ThinClientDurableInit.hpp"
#include "ThinClientTasks_C2S2.hpp"

const char* g_Locators = locatorsG;

std::string getServerEndPoint(int instance) {
  char instanceStr[16];
  int port;
  if (instance == 1) {
    port = CacheHelper::staticHostPort1;
  } else if (instance == 2) {
    port = CacheHelper::staticHostPort2;
  } else if (instance == 3) {
    port = CacheHelper::staticHostPort3;
  } else {
    port = CacheHelper::staticHostPort4;
  }

  std::string retVal(ACE_OS::itoa(port, instanceStr, 10));
  return retVal;

  std::string allEndPts(endPointsList);
  std::string::size_type start_idx = 0;
  std::string::size_type end_idx = 0;

  for (int i = 0; i < instance - 1; i++)
    start_idx = allEndPts.find(',', start_idx) + 1;

  end_idx = allEndPts.find(',', start_idx);
  if (end_idx == std::string::npos) /* asking for last endpoint */
    end_idx = allEndPts.size();

  return (std::string(allEndPts, start_idx, end_idx - start_idx));
}

DUNIT_TASK_DEFINITION(SERVER_SET1, S1Up)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              g_Locators);
    }
    LOG("SERVER 1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER_SET1, S2Up)
  {
    if (isLocalServer)
      CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml",
                              g_Locators);
    LOG("SERVER 2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER_SET2, S3Up)
  {
    if (isLocalServer)
      CacheHelper::initServer(3, "cacheserver_notify_subscription3.xml",
                              g_Locators);
    LOG("SERVER 3 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER_SET2, S4Up)
  {
    if (isLocalServer)
      CacheHelper::initServer(4, "cacheserver_notify_subscription4.xml",
                              g_Locators);
    LOG("SERVER 4 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER_SET1, S1Down)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER 1 closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER_SET1, S2Down)
  {
    CacheHelper::closeServer(2);
    LOG("SERVER 2 closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER_SET2, S3Down)
  {
    CacheHelper::closeServer(3);
    LOG("SERVER 3 closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER_SET2, S4Down)
  {
    CacheHelper::closeServer(4);
    LOG("SERVER 4 closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT, ClntUp)
  {
    initClientAndRegion(1, 0);
    getHelper()->cachePtr->readyForEvents();

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    regPtr0->registerAllKeys(true);

    LOG("Clnt1Init complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT, ClntUpNonHA)
  {
    initClientAndRegion(0, 0);
    getHelper()->cachePtr->readyForEvents();

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    regPtr0->registerAllKeys(true);

    LOG("Clnt1Init complete.");
  }
END_TASK_DEFINITION

/* Close Client 1 with option keep alive = true*/
DUNIT_TASK_DEFINITION(CLIENT, ClntDown)
  {
    getHelper()->disconnect(true);
    cleanProc();
    LOG("Client Down complete: Keepalive = True");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT, ClntSleep)
  {
    LOG(" Sleeping for 100 seconds");
    SLEEP(100000); /* 100 seconds for timeout*/
    LOG(" Finished sleep of 100 seconds");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT, VerifyNonHA)
  {
    /*Test case 4 , 1 */
    LOG("Verify 1: Verify that Client has queue on S1");

    ASSERT(TestUtils::getCacheImpl(getHelper()->cachePtr)
               ->getEndpointStatus(getServerEndPoint(1)),
           "Server 1 should have queue");
    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(2)),
           "Server 2 should not have queue");
    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(3)),
           "Server 3 should not have queue/connection");
    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(4)),
           "Server 4 should not have queue/connection");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT, Verify1)
  {
    /*Test case 4 , 1 */
    LOG("Verify 1: Verify that Client has queues on S1 and S2");

    ASSERT(TestUtils::getCacheImpl(getHelper()->cachePtr)
               ->getEndpointStatus(getServerEndPoint(1)),
           "Server 1 should have queue");
    ASSERT(TestUtils::getCacheImpl(getHelper()->cachePtr)
               ->getEndpointStatus(getServerEndPoint(2)),
           "Server 2 should have queue");
    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(3)),
           "Server 3 should not have queue/connection");
    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(4)),
           "Server 4 should not have queue/connection");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT, Verify2)
  {
    /*Test case 2*/
    LOG("Verify 2: Verify that Client has queues on S2 and either S3 or S4 and "
        "not both");

    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(1)),
           "Server 1 should not have queue");
    ASSERT(TestUtils::getCacheImpl(getHelper()->cachePtr)
               ->getEndpointStatus(getServerEndPoint(2)),
           "Server 2 should have queue");
    bool server3_hasQueue = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                ->getEndpointStatus(getServerEndPoint(3));
    bool server4_hasQueue = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                ->getEndpointStatus(getServerEndPoint(4));
    ASSERT(((server3_hasQueue && !server4_hasQueue) ||
            (!server3_hasQueue && server4_hasQueue)),
           "Either Server 3 or Server 4 should have queue");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT, Verify3)
  {
    /*Test case 3*/
    LOG("Verify 3: Verify that Client has queues on S3 and S4");

    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(1)),
           "Server 1 should not have queue/connection");
    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(2)),
           "Server 2 should not have queue/connection");
    ASSERT(TestUtils::getCacheImpl(getHelper()->cachePtr)
               ->getEndpointStatus(getServerEndPoint(3)),
           "Server 3 should have queue/connection");
    ASSERT(TestUtils::getCacheImpl(getHelper()->cachePtr)
               ->getEndpointStatus(getServerEndPoint(4)),
           "Server 4 should have queue/connection");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT, Verify4)
  {
    /*Test case 5*/
    LOG("Verify 4: Verify that no servers have queues");

    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(1)),
           "Server 1 should not have queue/connection");
    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(2)),
           "Server 2 should not have queue/connection");
    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(3)),
           "Server 3 should not have queue/connection");
    ASSERT(!TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getEndpointStatus(getServerEndPoint(4)),
           "Server 4 should not have queue/connection");
  }
END_TASK_DEFINITION

void doThinClientDurableConnect(bool poolConfig = true,
                                bool poolLocators = true) {
  CALL_TASK(StartLocator);

  CALL_TASK(S1Up);
  CALL_TASK(ClntUpNonHA);
  CALL_TASK(S2Up);
  CALL_TASK(ClntDown);
  CALL_TASK(ClntUp);
  CALL_TASK(VerifyNonHA);
  CALL_TASK(ClntDown);
  CALL_TASK(S1Down);
  /* Presently for 4 servers only */
  /* Test case 4 */
  CALL_TASK(S1Up);
  CALL_TASK(ClntUp);
  /* Test case 1 */
  CALL_TASK(S3Up);
  CALL_TASK(S4Up);
  CALL_TASK(ClntDown);
  CALL_TASK(ClntUp);
  CALL_TASK(Verify1);
  /* Test case 2 */
  CALL_TASK(ClntDown);
  CALL_TASK(S1Down);
  CALL_TASK(ClntUp);
  CALL_TASK(ClntDown);
  CALL_TASK(ClntUp);
  CALL_TASK(Verify2);
  /* Test case 3 */
  CALL_TASK(ClntDown);
  CALL_TASK(S2Down);
  CALL_TASK(ClntUp);
  CALL_TASK(ClntDown);
  CALL_TASK(ClntUp);
  CALL_TASK(Verify3);
  CALL_TASK(ClntDown);
  CALL_TASK(ClntSleep);
  CALL_TASK(ClntUp);
  CALL_TASK(Verify4);
  /* Close Everything */
  CALL_TASK(ClntDown);
  CALL_TASK(S3Down);
  CALL_TASK(S4Down);

  CALL_TASK(CloseLocator);
}

#endif /* THINCLIENTDURABLECONNECT_HPP_ */
