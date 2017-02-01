#pragma once

#ifndef GEODE_INTEGRATION_TEST_THINCLIENTTASKS_C2S2_H_
#define GEODE_INTEGRATION_TEST_THINCLIENTTASKS_C2S2_H_

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
/*
 * ThinClientTasks_C2S2.hpp
 *
 *  Created on: Nov 3, 2008
 *      Author: abhaware
 */


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


#endif // GEODE_INTEGRATION_TEST_THINCLIENTTASKS_C2S2_H_
