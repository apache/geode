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

#define ROOT_NAME "testThinClientTicket304"

#include "ThinClientSecurityHelper.hpp"

#define SERVER s1p1
#define CLIENT1 s1p2
#define CLIENT2 s2p1

// This is the test for tracking work. bug#304

putThread *thread1 = NULL;

void createAuthzRegion() {
  initCredentialGenerator();
  initClientAuth('A');
  createRegion(regionNamesAuth[0], false, true);
}
DUNIT_TASK_DEFINITION(SERVER, StartServer1)
  {
    initCredentialGenerator();
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      cmdServerAuthenticator += std::string(
          " --J=-Dgemfire.security-client-accessor-pp=javaobject."
          "DummyAuthorization.create");
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
// check that tracking works correctly with put.
DUNIT_TASK_DEFINITION(CLIENT1, PutOnClient1)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    rptr->put("key-1", "client1-value1");
    rptr->put("key-2", "client1-value2");
    rptr->put("key-3", "client1-value3");
    LOG("PutOnClient1 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, RegisterInterestAllOnClient2)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    thread1 = new putThread(rptr, true);
    thread1->start();
    LOG("RegisterInterest started on client 2");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, DestroyEntryOnClient1)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    rptr->destroy("key-3");
    LOG("DestroyEntryOnClient1 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, VerifyOnClient1)
  {
    verifyEntry(regionNamesAuth[0], "key-1", "client1-value1");
    verifyEntry(regionNamesAuth[0], "key-2", "client1-value2");
    verifyDestroyed(regionNamesAuth[0], "key-3");
    LOG("VerifyOnClient1 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, VerifyOnClient2)
  {
    thread1->stop();
    delete thread1;
    SLEEP(5000);
    verifyEntry(regionNamesAuth[0], "key-1", "client1-value1");
    verifyEntry(regionNamesAuth[0], "key-2", "client1-value2");
    verifyDestroyed(regionNamesAuth[0], "key-3");
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    rptr->localDestroyRegion();
    LOG("VerifyOnClient2 completed");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, RegisterInterestKeysOnClient2)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    thread1 = new putThread(rptr);
    thread1->setParams(5, 3, 1);
    thread1->start();
    LOG("RegisterInterestKeys started on client 2");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, RegisterRegexClient2)
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    thread1 = new putThread(rptr);
    thread1->setParams(6, 3, 1);
    thread1->start();
    LOG("RegisterRegex started on client 2");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, CreateRegionOnClient1)
  ;
  {
    RegionPtr rptr = getHelper()->getRegion(regionNamesAuth[0]);
    rptr->localDestroyRegion();
    SLEEP(10000);
    createRegion(regionNamesAuth[0], false, true);
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, CreateRegionOnClient2)
  ;
  {
    SLEEP(10000);
    createRegion(regionNamesAuth[0], false, true);
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
    CALL_TASK(RegisterInterestAllOnClient2);
    CALL_TASK(DestroyEntryOnClient1);
    CALL_TASK(VerifyOnClient1);
    CALL_TASK(VerifyOnClient2);
    CALL_TASK(CreateRegionOnClient1);
    CALL_TASK(CreateRegionOnClient2);
    CALL_TASK(PutOnClient1);
    CALL_TASK(RegisterInterestKeysOnClient2);
    CALL_TASK(DestroyEntryOnClient1);
    CALL_TASK(VerifyOnClient1);
    CALL_TASK(VerifyOnClient2);
    CALL_TASK(CreateRegionOnClient1);
    CALL_TASK(CreateRegionOnClient2);
    CALL_TASK(PutOnClient1);
    CALL_TASK(RegisterRegexClient2);
    CALL_TASK(DestroyEntryOnClient1);
    CALL_TASK(VerifyOnClient1);
    CALL_TASK(VerifyOnClient2);
    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseLocator);
  }
END_MAIN
