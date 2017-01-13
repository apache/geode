/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include <string>

#define ROOT_NAME "testThinClientMultiDS"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientSecurityHelper.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

#ifdef __disabled_test__
static int clientWithNothing = 0;
void initClient() {
  if (cacheHelper == NULL) {
    PropertiesPtr config = Properties::create();
    if (clientWithNothing > 1) config->insert("grid-client", "true");
    clientWithNothing += 1;
    cacheHelper = new CacheHelper(true, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void initializeClient() {
  initCredentialGenerator();
  initClientAuth('A');
  LOG("client initialized.");
}
void startServer(int instance, const char* xmlfile, const char* lochostport) {
  credentialGeneratorHandler = CredentialGenerator::create("DUMMY");
  std::string cmdServerAuthenticator;
  cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
      "authenticator:dummy", getXmlPath());
  printf("string %s", cmdServerAuthenticator.c_str());
  CacheHelper::initServer(instance, xmlfile, lochostport,
                          const_cast<char*>(cmdServerAuthenticator.c_str()),
                          false, true, true);
  LOG("Server started");
}

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    CacheHelper::initServer(1, "cacheserver_notify_subscription.xml", NULL,
                            NULL, false, true, true);
    CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml", NULL,
                            NULL, false, true, true);
    LOG("SERVER1 and server2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
  {
    CacheHelper::initServer(3, "cacheserver_notify_subscription3.xml", NULL,
                            NULL, false, true, true);
    CacheHelper::initServer(4, "cacheserver_notify_subscription4.xml", NULL,
                            NULL, false, true, true);
    LOG("SERVER2 started");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, InitClient1_Pool)
  {
    try {
      initClient();
    } catch (Exception& excp) {
      LOG(excp.getMessage());
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, InitClient2_Pool)
  {
    try {
      initClient();
    } catch (Exception& excp) {
      LOG(excp.getMessage());
    }
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pool_EndPoint)
  {
    try {
      // getHelper()->createPoolWithEPs("__TESTPOOL1_",
      // "localhost:24680,localhost:24681");
      // getHelper()->createPoolWithEPs("__TESTPOOL2_",
      // "localhost:24682,localhost:24683");
      char tmp[128];
      sprintf(tmp, "localhost:%d,localhost:%d", CacheHelper::staticHostPort1,
              CacheHelper::staticHostPort2);
      getHelper()->createPoolWithEPs("__TESTPOOL1_", tmp);
      char tmp2[128];
      sprintf(tmp2, "localhost:%d,localhost:%d", CacheHelper::staticHostPort3,
              CacheHelper::staticHostPort4);
      getHelper()->createPoolWithEPs("__TESTPOOL2_", tmp2);

      getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                             "__TESTPOOL1_", false);
      getHelper()->createRegionAndAttachPool(regionNames[1], NO_ACK,
                                             "__TESTPOOL2_", false);
      RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
      RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
      for (int i = 0; i < 100; i++) {
        regPtr0->put(keys[0], vals[0]);
        regPtr1->put(keys[2], vals[2]);
      }
    } catch (Exception& excp) {
      LOG(excp.getMessage());
      ASSERT(false, "Got unexpected exception");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pool_EndPoint)
  {
    try {
      // getHelper()->createPoolWithEPs("__TESTPOOL1_",
      // "localhost:24680,localhost:24681",false);
      // getHelper()->createPoolWithEPs("__TESTPOOL2_",
      // "localhost:24682,localhost:24683",false);
      char tmp[128];
      sprintf(tmp, "localhost:%d,localhost:%d", CacheHelper::staticHostPort1,
              CacheHelper::staticHostPort2);
      getHelper()->createPoolWithEPs("__TESTPOOL1_", tmp);
      char tmp2[128];
      sprintf(tmp2, "localhost:%d,localhost:%d", CacheHelper::staticHostPort3,
              CacheHelper::staticHostPort4);
      getHelper()->createPoolWithEPs("__TESTPOOL2_", tmp2);

      getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                             "__TESTPOOL1_", false);
      getHelper()->createRegionAndAttachPool(regionNames[1], NO_ACK,
                                             "__TESTPOOL2_", false);
      RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
      RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
      for (int i = 0; i < 100; i++) {
        regPtr0->put(keys[1], vals[1]);
        regPtr1->put(keys[3], vals[3]);
      }
    } catch (Exception& excp) {
      LOG(excp.getMessage());
      ASSERT(false, "Got unexpected exception");
    }

    LOG("Steptwo complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepOne_1)
  {
    try {
      RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
      RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
      CacheableStringPtr checkPtr =
          dynCast<CacheableStringPtr>(regPtr0->get(keys[2]));
      ASSERT(checkPtr == NULLPTR, "checkPtr should be null");
      CacheableStringPtr checkPtr1 =
          dynCast<CacheableStringPtr>(regPtr0->get(keys[0]));
      ASSERT(checkPtr1 != NULLPTR, "checkPtr1 should not be null");
    } catch (Exception& excp) {
      LOG(excp.getMessage());
      ASSERT(false, "Got unexpected exception");
    }
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER1 stopped");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepOne_2)
  {
    try {
      RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
      RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
      for (int i = 0; i < 100; i++) {
        regPtr0->put(keys[0], regPtr1->get(keys[2]));
        regPtr1->put(keys[2], regPtr0->get(keys[0]));
      }

    } catch (Exception& excp) {
      LOG(excp.getMessage());
      ASSERT(false, "Got unexpected exception");
    }
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER2, CloseServer1_1)
  {
    CacheHelper::closeServer(2);
    LOG("SERVER2 stopped");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER2, CloseServer2)
  {
    CacheHelper::closeServer(3);
    CacheHelper::closeServer(4);
    LOG("SERVER2 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, SecureStartServer1)
  {
    char tmp[128];
    sprintf(tmp, "localhost:%d", CacheHelper::staticLocatorHostPort1);
    startServer(1, "cacheserver_notify_subscription.xml", tmp);
    startServer(2, "cacheserver_notify_subscription2.xml", tmp);
    LOG("SecureServer1 started");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER2, SecureStartServer2)
  {
    char tmp[128];
    sprintf(tmp, "localhost:%d", CacheHelper::staticLocatorHostPort2);
    startServer(3, "cacheserver_notify_subscription3.xml", tmp);
    startServer(4, "cacheserver_notify_subscription4.xml", tmp);
    LOG("SecureServer2 started");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER1, StartLocator1)
  {
    CacheHelper::initLocator(1, false, true);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER2, StartLocator2)
  {
    CacheHelper::initLocator(2, false, true);
    LOG("Locator2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StartSecureClient1)
  {
    initializeClient();
    LOG("StartSecureClient1 started");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, StartSecureClient2)
  {
    initializeClient();
    LOG("StartSecureClient2 started");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pool_SecureEndPoint)
  {
    try {
      //  getHelper()->createPoolWithEPs("__TESTPOOL1_",
      //  "localhost:24680,localhost:24681");
      // getHelper()->createPoolWithEPs("__TESTPOOL2_",
      // "localhost:24682,localhost:24683");
      char tmp[128];
      sprintf(tmp, "localhost:%d,localhost:%d", CacheHelper::staticHostPort1,
              CacheHelper::staticHostPort2);
      getHelper()->createPoolWithEPs("__TESTPOOL1_", tmp);
      char tmp2[128];
      sprintf(tmp2, "localhost:%d,localhost:%d", CacheHelper::staticHostPort3,
              CacheHelper::staticHostPort4);
      getHelper()->createPoolWithEPs("__TESTPOOL2_", tmp2);

      getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                             "__TESTPOOL1_", false);
      getHelper()->createRegionAndAttachPool(regionNames[1], NO_ACK,
                                             "__TESTPOOL2_", false);
      RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
      RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
      for (int i = 0; i < 100; i++) {
        regPtr0->put(keys[0], vals[0]);
        regPtr1->put(keys[2], vals[2]);
      }
    } catch (Exception& excp) {
      LOG(excp.getMessage());
      ASSERT(false, "Got unexpected exception");
    }
    LOG("StepOne_Pool_SecureEndPoint complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pool_SecureEndPoint)
  {
    try {
      // getHelper()->createPoolWithEPs("__TESTPOOL1_",
      // "localhost:24680,localhost:24681");
      // getHelper()->createPoolWithEPs("__TESTPOOL2_",
      // "localhost:24682,localhost:24683");
      char tmp[128];
      sprintf(tmp, "localhost:%d,localhost:%d", CacheHelper::staticHostPort1,
              CacheHelper::staticHostPort2);
      getHelper()->createPoolWithEPs("__TESTPOOL1_", tmp);
      char tmp2[128];
      sprintf(tmp2, "localhost:%d,localhost:%d", CacheHelper::staticHostPort3,
              CacheHelper::staticHostPort4);
      getHelper()->createPoolWithEPs("__TESTPOOL2_", tmp2);

      getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                             "__TESTPOOL1_", false);
      getHelper()->createRegionAndAttachPool(regionNames[1], NO_ACK,
                                             "__TESTPOOL2_", false);
      RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
      RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
      for (int i = 0; i < 100; i++) {
        regPtr0->put(keys[1], vals[1]);
        regPtr1->put(keys[3], vals[3]);
      }
    } catch (Exception& excp) {
      LOG(excp.getMessage());
      ASSERT(false, "Got unexpected exception");
    }

    LOG("StepTwo_Pool_SecureEndPoint complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepOne_secureclient1)
  {
    try {
      RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
      RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
      CacheableStringPtr checkPtr =
          dynCast<CacheableStringPtr>(regPtr0->get(keys[2]));
      ASSERT(checkPtr == NULLPTR, "checkPtr should be null");
      CacheableStringPtr checkPtr1 =
          dynCast<CacheableStringPtr>(regPtr0->get(keys[0]));
      ASSERT(checkPtr1 != NULLPTR, "checkPtr1 should not be null");
    } catch (Exception& excp) {
      LOG(excp.getMessage());
      ASSERT(false, "Got unexpected exception");
    }
    LOG("StepOne_secureclient1 complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepOne_secureclient2)
  {
    try {
      RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
      RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
      for (int i = 0; i < 100; i++) {
        regPtr0->put(keys[0], regPtr1->get(keys[2]));
        regPtr1->put(keys[2], regPtr0->get(keys[0]));
      }

    } catch (Exception& excp) {
      LOG(excp.getMessage());
      ASSERT(false, "Got unexpected exception");
    }
    LOG("StepOne_secureclient2 complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER1, CloseLocator1)
  {
    SLEEP(2000);
    CacheHelper::closeServer(2);
    CacheHelper::closeLocator(1);
    LOG("SERVER1 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CloseLocator2)
  {
    CacheHelper::closeServer(3);
    CacheHelper::closeServer(4);
    CacheHelper::closeLocator(2);
    LOG("Locator1 stopped");
  }
END_TASK_DEFINITION
DUNIT_MAIN
  {
    CALL_TASK(CreateServer1);
    CALL_TASK(CreateServer2);
    CALL_TASK(InitClient1_Pool);
    CALL_TASK(InitClient2_Pool);
    CALL_TASK(StepOne_Pool_EndPoint);
    CALL_TASK(StepTwo_Pool_EndPoint);
    CALL_TASK(StepOne_1);
    CALL_TASK(CloseServer1);
    CALL_TASK(StepOne_2);
    CALL_TASK(CloseServer1_1);
    CALL_TASK(CloseServer2);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    // Test MultiDS with security config
    CALL_TASK(StartLocator1);
    CALL_TASK(StartLocator2);
    CALL_TASK(SecureStartServer1);
    CALL_TASK(SecureStartServer2);
    CALL_TASK(StartSecureClient1);
    CALL_TASK(StartSecureClient2);
    CALL_TASK(StepOne_Pool_SecureEndPoint);
    CALL_TASK(StepTwo_Pool_SecureEndPoint);
    CALL_TASK(StepOne_secureclient1);
    CALL_TASK(CloseServer1);
    CALL_TASK(StepOne_secureclient2);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseLocator1);
    CALL_TASK(CloseLocator2);
  }
END_MAIN
#endif
