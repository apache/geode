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
const char *endPoint = CacheHelper::getTcrEndpoints(isLocalServer, 1);
static bool isLocator = false;
const char *locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
#include "LocatorHelper.hpp"
TallyListenerPtr reg1Listener1;
TallyWriterPtr reg1Writer1;
int numCreates = 0;
int numUpdates = 0;
int numInvalidates = 0;
int numDestroys = 0;

void setCacheListener(const char *regName, TallyListenerPtr regListener) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(regListener);
}

void setCacheWriter(const char *regName, TallyWriterPtr regWriter) {
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
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pool_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[0], false /*ack mode*/,
                       locatorsG, "__TEST_POOL1__",
                       true /*client notification*/);
    reg1Listener1 = new TallyListener();
    reg1Writer1 = new TallyWriter();
    setCacheListener(regionNames[0], reg1Listener1);
    setCacheWriter(regionNames[0], reg1Writer1);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testCreatesAndUpdates)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[1]);
    CacheableKeyPtr keyPtr2 = CacheableKey::create(keys[2]);
    VectorOfCacheableKey keys;
    keys.push_back(keyPtr1);
    keys.push_back(keyPtr2);
    regPtr->registerKeys(keys, NULLPTR);

    // Do a create followed by a create on the same key
    /*NIL: Changed the asserion due to the change in invalidate.
      Now we create new entery for every invalidate event received or
      localInvalidate call
      so expect  containsKey to returns true insted of false earlier.
      and used put call to update value for that key, instead of create call
      earlier or will get
      Region::create: Entry already exists in the region  */
    ASSERT(regPtr->containsKey(keyPtr1), "Key should be found in region.");
    regPtr->put(keyPtr1, vals[1]);

    numCreates++;
    validateEventCount(__LINE__);

    // Trigger an update event from a put
    regPtr->put(keyPtr1, nvals[1]);
    numUpdates++;
    validateEventCount(__LINE__);

    // This put creates a new value, verify update event is not received
    regPtr->put(keyPtr2, vals[2]);
    numCreates++;
    validateEventCount(__LINE__);

    // Do a get on a preexisting value to confirm no events are triggered
    regPtr->get(keyPtr2);
    validateEventCount(__LINE__);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testInvalidateAndDestroy)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[1]);
    CacheableKeyPtr keyPtr2 = CacheableKey::create(keys[2]);
    regPtr->invalidate(keyPtr1);
    numInvalidates++;
    validateEventCount(__LINE__);

    regPtr->destroy(keyPtr2);
    numDestroys++;
    validateEventCount(__LINE__);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StopClient1)
  {
    cleanProc();
    LOG("CLIENT1 stopped");
    // Reset numValues
    numCreates = 0;
    numUpdates = 0;
    numInvalidates = 0;
    numDestroys = 0;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer)
  {
    if (isLocalServer) CacheHelper::closeServer(1);
    LOG("SERVER stopped");
  }
END_TASK_DEFINITION
