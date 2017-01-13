/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testThinClientTracking"

#include "ThinClientSecurityHelper.hpp"

#define SERVER s1p1
#define CLIENT1 s1p2
#define CLIENT2 s2p1

// This is the test for tracking work.

putThread *thread1 = NULL;
putThread *thread2 = NULL;

void createAuthzRegion() {
  initCredentialGenerator();
  initClientAuth('A');
  RegionPtr rptr;
  createRegion(regionNamesAuth[0], false, true);
  rptr = getHelper()->getRegion(regionNamesAuth[0]);
  rptr->registerAllKeys();
}
void verifyEntry(const char *value) {
  RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
  RegionEntryPtr entry = rptr->getEntry("key-1");
  ASSERT(entry != NULLPTR, "Key should have been found in region.");
  CacheableStringPtr valuePtr = dynCast<CacheableStringPtr>(entry->getValue());
  char buf1[1024];

  if (valuePtr == NULLPTR) {
    FAIL("Value was null.");
  }
  sprintf(buf1, "value for key-1 is %s", valuePtr->asChar());
  LOG(buf1);
  ASSERT(strcmp(valuePtr->asChar(), value) == 0,
         "Updated value not found in region.");
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
          const_cast<char *>(cmdServerAuthenticator.c_str()));
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
DUNIT_TASK_DEFINITION(CLIENT2, StartClient2)
  {
    createAuthzRegion();
    LOG("CLIENT2 started");
  }
END_TASK_DEFINITION
// check that tracking work correctly with put.
DUNIT_TASK_DEFINITION(CLIENT1, PutOnClient1)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    thread1 = new putThread(rptr);
    thread1->setParams(0, 1, 1, true);
    thread1->start();
    LOG("PutOnClient1 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, PutOnClient2)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    thread2 = new putThread(rptr);
    thread2->setParams(0, 1, 1);
    thread2->start();
    thread2->stop();
    delete thread2;
    LOG("PutOnClient2 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, VerifyOnClient1)
  {
    verifyEntry("client2-value1");
    LOG("VerifyOnClient1 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, VerifyOnClient2)
  {
    SLEEP(5000);
    verifyEntry("client2-value1");
    LOG("VerifyOnClient2 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, VerifyOnClient12)
  {
    thread1->stop();
    delete thread1;
    verifyEntry("client2-value1");

    LOG("VerifyOnClient12 completed");
  }
END_TASK_DEFINITION
// check that tracking work correctly with destroy.
DUNIT_TASK_DEFINITION(CLIENT1, DestroyOnClient1)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    thread1 = new putThread(rptr);
    thread1->setParams(0, 1, 1, true);
    thread1->start();
    LOG("DestroyOnClient1 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, DestroyOnClient2)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    thread2 = new putThread(rptr);
    thread2->setParams(2, 1, 1);
    thread2->start();
    thread2->stop();
    delete thread2;
    LOG("DestroyOnClient2 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, VerifyDestroyOnClient1)
  {
    verifyDestroyed(regionNamesAuth[0], "key-1");
    LOG("VerifyDestroyOnClient1 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, VerifyDestroyOnClient2)
  {
    SLEEP(5000);
    verifyDestroyed(regionNamesAuth[0], "key-1");
    LOG("VerifyDestroyOnClient2 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, VerifyDestroyOnClient12)
  {
    thread1->stop();
    delete thread1;
    verifyDestroyed(regionNamesAuth[0], "key-1");
    LOG("VerifyDestroyOnClient12 completed");
  }
END_TASK_DEFINITION
// check that Conversion from Tracked Map Entry and back work correctly
DUNIT_TASK_DEFINITION(CLIENT1, PutTrackedMapOnClient1)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    thread1 = new putThread(rptr);
    thread1->setParams(0, 1, 1, true);
    thread1->start();
    LOG("PutTrackedMapOnClient1 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, PutTrackedMapOnClient2)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    thread2 = new putThread(rptr);
    thread2->setParams(0, 10, 1, false, true);
    thread2->start();
    thread2->stop();
    delete thread2;
    verifyEntry("client2-value10");
    LOG("PutTrackedMapOnClient2 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, VerifyTrackedMapOnClient1)
  {
    verifyEntry("client2-value10");
    LOG("VerifyTrackedMapOnClient1 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, VerifyTrackedMapOnClient2)
  {
    verifyEntry("client2-value10");
    LOG("VerifyTrackedMapOnClient2 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, VerifyTrackedMapOnClient12)
  {
    thread1->stop();
    delete thread1;
    verifyEntry("client2-value10");
    LOG("VerifyTrackedMapOnClient12 completed");
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
DUNIT_TASK_DEFINITION(CLIENT2, CloseClient2)
  { cleanProc(); }
END_TASK_DEFINITION
DUNIT_MAIN
  {
    CALL_TASK(StartLocator);
    CALL_TASK(StartServer1);
    CALL_TASK(StartClient1);
    CALL_TASK(StartClient2);
    CALL_TASK(PutOnClient1);
    CALL_TASK(PutOnClient2);
    CALL_TASK(VerifyOnClient1);
    CALL_TASK(VerifyOnClient2);
    CALL_TASK(VerifyOnClient12);
    CALL_TASK(DestroyOnClient1);
    CALL_TASK(DestroyOnClient2);
    CALL_TASK(VerifyDestroyOnClient1);
    CALL_TASK(VerifyDestroyOnClient2);
    CALL_TASK(VerifyDestroyOnClient12);
    CALL_TASK(PutTrackedMapOnClient1);
    CALL_TASK(PutTrackedMapOnClient2);
    CALL_TASK(VerifyTrackedMapOnClient1);
    CALL_TASK(VerifyTrackedMapOnClient2);
    CALL_TASK(VerifyTrackedMapOnClient12);
    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseLocator);
  }
END_MAIN
