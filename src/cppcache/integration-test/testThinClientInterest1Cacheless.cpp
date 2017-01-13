/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include <gfcpp/GemfireCppCache.hpp>

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

#include "locator_globals.hpp"

using namespace gemfire;
using namespace test;

class MyListener;

typedef gemfire::SharedPtr<MyListener> MyListenerPtr;

class MyListener : public CacheListener {
  uint8_t m_gotit[5];

 public:
  MyListener() : CacheListener() {
    for (int i = 0; i < 5; i++) m_gotit[i] = 0;
  }
  inline void checkEntry(const EntryEvent& event) {
    CacheableStringPtr keyPtr = dynCast<CacheableStringPtr>(event.getKey());
    for (int i = 0; i < 5; i++) {
      if (!ACE_OS::strcmp(keys[i], keyPtr->asChar())) {
        CacheableStringPtr valPtr =
            dynCast<CacheableStringPtr>(event.getNewValue());
        if (!ACE_OS::strcmp(vals[i], valPtr->asChar())) m_gotit[i] = 1;
        break;
      }
    }
  }
  virtual void afterCreate(const EntryEvent& event) { checkEntry(event); }
  virtual void afterUpdate(const EntryEvent& event) { checkEntry(event); }
  inline bool gotAll() {
    for (int i = 0; i < 5; i++) {
      if (m_gotit[i] == 0) return false;
    }
    return true;
  }
};

MyListenerPtr mylistner = NULLPTR;

void setCacheListener(const char* regName, MyListenerPtr regListener) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(regListener);
}

DUNIT_TASK(SERVER1, StartServer)
  {
    if (isLocalServer) {
      CacheHelper::initLocator(1);
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
    }
    LOG("SERVER started");
  }
END_TASK(StartServer)

DUNIT_TASK(CLIENT1, SetupClient1)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locatorsG,
                                    "__TEST_POOL1__", true, true);
  }
END_TASK(SetupClient1)

DUNIT_TASK(CLIENT2, setupClient2)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locatorsG,
                                    "__TEST_POOL1__", true, true);
    mylistner = new MyListener();
    setCacheListener(regionNames[0], mylistner);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->registerAllKeys(false, NULLPTR, true);
  }
END_TASK(setupClient2)

DUNIT_TASK(CLIENT1, populateServer)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    for (int i = 0; i < 5; i++) {
      CacheableKeyPtr keyPtr = CacheableKey::create(keys[i]);
      regPtr->create(keyPtr, vals[i]);
    }
    SLEEP(1000);
  }
END_TASK(populateServer)

DUNIT_TASK(CLIENT2, verify)
  { ASSERT(mylistner->gotAll(), "Did not get all"); }
END_TASK(verify)

DUNIT_TASK(CLIENT1, StopClient1)
  {
    cleanProc();
    LOG("CLIENT1 stopped");
  }
END_TASK(StopClient1)

DUNIT_TASK(CLIENT2, StopClient2)
  {
    cleanProc();
    LOG("CLIENT2 stopped");
  }
END_TASK(StopClient2)

DUNIT_TASK(SERVER1, StopServer)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      CacheHelper::closeLocator(1);
    }
    LOG("SERVER stopped");
  }
END_TASK(StopServer)
