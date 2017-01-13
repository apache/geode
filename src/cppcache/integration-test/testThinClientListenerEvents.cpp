/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

#define CLIENT1 s1p1
#define SERVER1 s2p1

using namespace gemfire;
using namespace test;

#include "locator_globals.hpp"
#include "LocatorHelper.hpp"

using namespace gemfire;
class SimpleCacheListener;

typedef gemfire::SharedPtr<SimpleCacheListener> SimpleCacheListenerPtr;

// Use the "gemfire" namespace.
using namespace gemfire;

// The SimpleCacheListener class.
class SimpleCacheListener : public CacheListener {
 public:
  // The Cache Listener callbacks.
  SimpleCacheListener() { m_count = 0; }
  virtual void afterCreate(const EntryEvent& event) {
    m_count++;
    LOGINFO("SimpleCacheListener: Got an afterCreate event.");
  }
  virtual void afterUpdate(const EntryEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterUpdate event.");
  }
  virtual void afterInvalidate(const EntryEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterInvalidate event.");
  }
  virtual void afterDestroy(const EntryEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterDestroy event.");
  }
  virtual void afterRegionInvalidate(const RegionEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterRegionInvalidate event.");
  }
  virtual void afterRegionDestroy(const RegionEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterRegionDestroy event.");
  }
  virtual void close(const RegionPtr& region) {
    LOGINFO("SimpleCacheListener: Got an close event.");
  }

  int getCount() { return m_count; }

 private:
  int m_count;
};
//---------------------------------------------------------------------------------

DUNIT_TASK_DEFINITION(SERVER1, StartServer)
  {
    if (isLocalServer) CacheHelper::initServer(1, "cacheserver.xml");
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locatorsG,
                                    "__TEST_POOL1__", true, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, doRemoteGet)
  {
    RegionPtr regionPtr = getHelper()->getRegion(regionNames[0]);

    AttributesMutatorPtr attrMutatorPtr = regionPtr->getAttributesMutator();
    SimpleCacheListenerPtr regListener1(new SimpleCacheListener());
    attrMutatorPtr->setCacheListener(regListener1);

    // Put 3 Entries into the Region.
    regionPtr->put("Key1", "Value1");
    regionPtr->put("Key2", "Value2");
    regionPtr->put("Key3", "Value3");

    // Update Key3.
    regionPtr->put("Key3", "Value3-updated");

    // Destroy Key3.
    regionPtr->localDestroy("Key3");

    // Perform remote get (Locally destroyed).
    regionPtr->get("Key3");
    int toalFunCall = regListener1->getCount();
    ASSERT(4 == toalFunCall,
           "afterCreate() did not call expected number of times");
    // printf("[NIL_DEBUG_DUnitTest:149] Total Function Call =
    // %d.............\n",
    // toalFunCall);
    // printf("\n[NIL_DEBUG_DUnitTest:150:Remote get ended.
    // ..................\n");
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

void runThinClientListenerEventsTest() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator);
  CALL_TASK(SetupClient1);

  CALL_TASK(doRemoteGet);

  CALL_TASK(StopClient1);
  CALL_TASK(StopServer);
  CALL_TASK(CloseLocator1);
}

DUNIT_MAIN
  { runThinClientListenerEventsTest(); }
END_MAIN
