/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "TallyListener.hpp"
#include "TallyWriter.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

using namespace gemfire;
using namespace test;

bool isLocalServer = true;
static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
#include "LocatorHelper.hpp"
TallyListenerPtr reg1Listener1;
TallyWriterPtr reg1Writer1;
int numCreates = 0;
int numUpdates = 0;
int numInvalidates = 0;
int numDestroys = 0;

void setCacheListener(const char* regName, TallyListenerPtr regListener) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(regListener);
}

void setCacheWriter(const char* regName, TallyWriterPtr regWriter) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheWriter(regWriter);
}

void validateEventCount(int line) {
  LOGINFO("ValidateEvents called from line (%d).", line);
  int num = reg1Listener1->getCreates();
  char buf[1024];
  sprintf(buf, "Got wrong number of creation events. expected[%d], real[%d]",
          numCreates, num);
  ASSERT(num == numCreates, buf);
  num = reg1Listener1->getUpdates();
  sprintf(buf, "Got wrong number of update events. expected[%d], real[%d]",
          numUpdates, num);
  ASSERT(num == numUpdates, buf);
  num = reg1Writer1->getCreates();
  sprintf(buf, "Got wrong number of writer events. expected[%d], real[%d]",
          numCreates, num);
  ASSERT(num == numCreates, buf);
  num = reg1Listener1->getInvalidates();
  sprintf(buf, "Got wrong number of invalidate events. expected[%d], real[%d]",
          numInvalidates, num);
  ASSERT(num == numInvalidates, buf);
  num = reg1Listener1->getDestroys();
  sprintf(buf, "Got wrong number of destroys events. expected[%d], real[%d]",
          numDestroys, num);
  ASSERT(num == numDestroys, buf);
}

DUNIT_TASK_DEFINITION(SERVER1, StartServer)
  {
    if (isLocalServer)
      CacheHelper::initServer(
          1, "cacheserver_notify_subscription_PutAllTimeout.xml");
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pool_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[0], false /*ack mode*/, locatorsG,
                       "__TEST_POOL1__", true /*client notification*/);
  }
END_TASK_DEFINITION

void putAllWithOneEntryTimeout(int timeout, int waitTimeOnServer) {
  LOG("Do large PutAll");
  HashMapOfCacheable map0;
  map0.clear();

  for (int i = 0; i < 100000; i++) {
    char key0[50] = {0};
    char val0[2500] = {0};
    sprintf(key0, "key-%d", i);
    sprintf(val0, "%1000d", i);
    map0.insert(CacheableKey::create(key0), CacheableString::create(val0));
  }

  char val[16];
  sprintf(val, "%d", waitTimeOnServer);
  map0.insert(CacheableKey::create("timeout-this-entry"),
              CacheableString::create(val));

  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);

  regPtr0->putAll(map0, timeout);
}

void putAllWithOneEntryTimeoutWithCallBackArg(int timeout,
                                              int waitTimeOnServer) {
  LOG("Do large PutAll putAllWithOneEntryTimeoutWithCallBackArg");
  HashMapOfCacheable map0;
  map0.clear();

  for (int i = 0; i < 100000; i++) {
    char key0[50] = {0};
    char val0[2500] = {0};
    sprintf(key0, "key-%d", i);
    sprintf(val0, "%1000d", i);
    map0.insert(CacheableKey::create(key0), CacheableString::create(val0));
  }

  char val[16];
  sprintf(val, "%d", waitTimeOnServer);
  map0.insert(CacheableKey::create("timeout-this-entry"),
              CacheableString::create(val));

  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);

  regPtr0->putAll(map0, timeout, CacheableInt32::create(1000));
  LOG("Do large PutAll putAllWithOneEntryTimeoutWithCallBackArg complete. ");
}

DUNIT_TASK_DEFINITION(CLIENT1, testTimeoutException)
  {
    printf("start task testTimeoutException\n");
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);

    regPtr->registerAllKeys();

    try {
      putAllWithOneEntryTimeout(20, 30000);
      FAIL("Didnt get expected timeout exception for putAll");
    } catch (const TimeoutException& excp) {
      std::string logmsg = "";
      logmsg += "PutAll expected timeout exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    }
    dunit::sleep(30000);

    try {
      putAllWithOneEntryTimeoutWithCallBackArg(20, 30000);
      FAIL("Didnt get expected timeout exception for putAllwithCallBackArg");
    } catch (const TimeoutException& excp) {
      std::string logmsg = "";
      logmsg += "PutAll with CallBackArg expected timeout exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    }

    dunit::sleep(30000);
    LOG("testTimeoutException completed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testWithoutTimeoutException)
  {
    printf("start task testWithoutTimeoutException\n");
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);

    // regPtr->registerAllKeys();

    try {
      putAllWithOneEntryTimeout(40, 20000);
      LOG("testWithoutTimeoutException completed");
      return;
    } catch (const TimeoutException& excp) {
      std::string logmsg = "";
      logmsg += "Not expected timeout exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    } catch (const Exception& ex) {
      printf("Exception while putALL :: %s : %s\n", ex.getName(),
             ex.getMessage());
    }
    FAIL("Something is wrong while putAll");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testWithoutTimeoutWithCallBackArgException)
  {
    try {
      putAllWithOneEntryTimeoutWithCallBackArg(40, 20000);
      LOG("testWithoutTimeoutException completed");
      return;
    } catch (const TimeoutException& excp) {
      std::string logmsg = "";
      logmsg += "Not expected timeout exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    } catch (const Exception& ex) {
      printf(
          "Exception while putAllWithOneEntryTimeoutWithCallBackArg :: %s : "
          "%s\n",
          ex.getName(), ex.getMessage());
    }
    FAIL("Something is wrong while putAllWithOneEntryTimeoutWithCallBackArg");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StopClient1)
  {
    cleanProc();
    LOG("CLIENT1 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer)
  {
    if (isLocalServer) CacheHelper::closeServer(1);
    LOG("SERVER stopped");
  }
END_TASK_DEFINITION
