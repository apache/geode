/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef SERVER2
#define SERVER2 s2p2
#endif
DUNIT_TASK_DEFINITION(SERVER1, CreateLocator1)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateLocator1_With_SSL)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1, true);
    LOG("Locator1 started with SSL");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator)
  {
    // starting servers
    if (isLocalServer) CacheHelper::initServer(1, NULL, locatorsG);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator_And_SSL)
  {
    // starting servers
    if (isLocalServer) CacheHelper::initServer(1, NULL, locatorsG, NULL, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer2_With_Locator)
  {
    // starting servers
    if (isLocalServer) CacheHelper::initServer(2, NULL, locatorsG);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator_XML)
  {
    // starting servers
    if (isLocalServer)
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator_XML_Bug849)
  {
    // starting servers
    if (isLocalServer)
      CacheHelper::initServer(1, "cacheserver_notify_subscriptionBug849.xml",
                              locatorsG);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator_XML2)
  {
    // starting servers
    if (isLocalServer)
      CacheHelper::initServer(
          1, "cacheserver_notify_subscription_PutAllTimeout.xml", locatorsG);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator_XML5)
  {
    // starting servers
    if (isLocalServer)
      CacheHelper::initServer(1, "cacheserver_notify_subscription5.xml",
                              locatorsG);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2_With_Locator_XML)
  {
    // starting servers
    if (isLocalServer)
      CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml",
                              locatorsG);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator_OQL)
  {
    // starting servers
    if (isLocalServer)
      CacheHelper::initServer(1, "cacheserver_remoteoql.xml", locatorsG);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2_With_Locator_OQL)
  {
    // starting servers
    if (isLocalServer)
      CacheHelper::initServer(2, "cacheserver_remoteoql2.xml", locatorsG);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseLocator1_With_SSL)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(1, true);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseLocator1)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION
