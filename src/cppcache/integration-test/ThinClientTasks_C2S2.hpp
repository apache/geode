/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientTasks_C2S2.hpp
 *
 *  Created on: Nov 3, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTTASKS_C2S2_HPP_
#define THINCLIENTTASKS_C2S2_HPP_

// define our own names for the 4 test processes
#define PROCESS1 s1p1
#define PROCESS2 s1p2
#define PROCESS3 s2p1
#define PROCESS4 s2p2

DUNIT_TASK_DEFINITION(SERVER1, StartLocator)
  {
    CacheHelper::initLocator(1);
    LOG("Locator started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseLocator)
  {
    CacheHelper::closeLocator(1);
    LOG("Locator stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StartServers)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
      CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml");
    }
    LOG("SERVERs started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, startServer)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
    }
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StartServersWithLocator)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
      CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml",
                              locatorsG);
    }
    LOG("SERVERs started with locator");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, startServerWithLocator)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
    }
    LOG("SERVER started with locator");
  }
END_TASK_DEFINITION

void startServers() { CALL_TASK(StartServersWithLocator); }

void startServer() { CALL_TASK(startServerWithLocator); }

void closeLocator() { CALL_TASK(CloseLocator); }

#endif /* THINCLIENTTASKS_C2S2_HPP_ */
