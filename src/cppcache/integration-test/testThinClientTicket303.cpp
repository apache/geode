/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testThinClientTicket303"

#include "ThinClientSecurityHelper.hpp"

#define SERVER s1p1
#define CLIENT1 s1p2

// Test for Rollback mechanism for put. Ticket #303

void createAuthzRegion() {
  initCredentialGenerator();
  initClientAuth('A');
  RegionPtr regPtr = createOverflowRegion(regionNamesAuth[0], false, 1);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}
DUNIT_TASK_DEFINITION(SERVER, StartServer1)
  {
    initCredentialGenerator();
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator:dummy", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      CacheHelper::initServer(
          1, "cacheserver_notify_subscription.xml", locHostPort,
          const_cast<char*>(cmdServerAuthenticator.c_str()));
      LOG("Server1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER, StartLocator)
  {
    if (isLocator) {
      CacheHelper::initLocator(1);
      LOG("Locator1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StartClient1)
  {
    createAuthzRegion();
    LOG("CLIENT1 started");
  }
END_TASK_DEFINITION
// check that tracking work correctly with put.
DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerification)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    rptr->put("key-1", "client1-value1");
    rptr->put("key-2", "client1-value2");
    rptr->put("key-3", "client1-value3");
    try {
      rptr->put("invalidkey-1", "client1-Invalidvalue1");
      LOG(" Put Operation Successful");
      FAIL("Should have got NotAuthorizedException during put");
    }
    HANDLE_NOT_AUTHORIZED_EXCEPTION
    ASSERT(rptr->containsKey("invalidkey-1") == false,
           "Key should not be found in region.");
    ASSERT(rptr->containsKey("key-1") == true,
           "Key key-1 should be found in region.");
    ASSERT(rptr->containsKey("key-2") == true,
           "Key key-2 should be found in region.");
    ASSERT(rptr->containsKey("key-3") == true,
           "Key key-3 should be found in region.");
    LOG("PutAndVerification completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER, CloseServer1)
  {
    SLEEP(2000);
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER, CloseLocator)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseClient1)
  { cleanProc(); }
END_TASK_DEFINITION
DUNIT_MAIN
  {
    CALL_TASK(StartLocator);
    CALL_TASK(StartServer1);
    CALL_TASK(StartClient1);
    CALL_TASK(PutAndVerification);
    CALL_TASK(CloseClient1);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseLocator);
  }
END_MAIN
