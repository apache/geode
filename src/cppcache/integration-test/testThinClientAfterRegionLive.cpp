/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testThinClientAfterRegionLive"

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include <string>
#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#include <gfcpp/CacheListener.hpp>
// CacheHelper* cacheHelper = NULL;
static bool isLocator = false;
static bool isLocalServer = true;
static int numberOfLocators = 1;
static bool isRegionLive[4] = {false, false, false, false};
static bool isRegionDead[4] = {false, false, false, false};
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
using namespace gemfire;
using namespace test;
class DisconnectCacheListioner : public CacheListener {
  int m_index;

 public:
  explicit DisconnectCacheListioner(int index) { m_index = index; };

  void afterRegionDisconnected(const RegionPtr& region) {
    isRegionDead[m_index] = true;
    LOG("After Region Disconnected event received");
  }
  void afterRegionLive(const RegionEvent& event) {
    isRegionLive[m_index] = true;
    LOG("After region live received ");
  }
};

CacheListenerPtr cptr1(new DisconnectCacheListioner(0));
CacheListenerPtr cptr2(new DisconnectCacheListioner(1));
CacheListenerPtr cptr3(new DisconnectCacheListioner(2));
CacheListenerPtr cptr4(new DisconnectCacheListioner(3));

#include "LocatorHelper.hpp"

void createPooledRegionMine(bool callReadyForEventsAPI = false) {
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();
  poolFacPtr->setSubscriptionEnabled(true);
  getHelper()->addServerLocatorEPs(locatorsG, poolFacPtr);
  if ((PoolManager::find("__TEST_POOL1__")) ==
      NULLPTR) {  // Pool does not exist with the same name.
    PoolPtr pptr = poolFacPtr->create("__TEST_POOL1__");
  }
  SLEEP(10000);
  AttributesFactory af;
  af.setCachingEnabled(true);
  af.setLruEntriesLimit(0);
  af.setEntryIdleTimeout(ExpirationAction::DESTROY, 0);
  af.setEntryTimeToLive(ExpirationAction::DESTROY, 0);
  af.setRegionIdleTimeout(ExpirationAction::DESTROY, 0);
  af.setRegionTimeToLive(ExpirationAction::DESTROY, 0);
  af.setPoolName("__TEST_POOL1__");
  LOG("poolName = ");
  LOG("__TEST_POOL1__");
  af.setCacheListener(cptr1);
  RegionAttributesPtr rattrsPtr1 = af.createRegionAttributes();
  af.setCacheListener(cptr2);
  RegionAttributesPtr rattrsPtr2 = af.createRegionAttributes();
  af.setCacheListener(cptr3);
  RegionAttributesPtr rattrsPtr3 = af.createRegionAttributes();
  af.setCacheListener(cptr4);
  RegionAttributesPtr rattrsPtr4 = af.createRegionAttributes();
  CacheImpl* cacheImpl =
      CacheRegionHelper::getCacheImpl(getHelper()->cachePtr.ptr());
  RegionPtr region1;
  cacheImpl->createRegion(regionNames[0], rattrsPtr1, region1);
  RegionPtr region2;
  cacheImpl->createRegion(regionNames[1], rattrsPtr2, region2);
  RegionPtr subregion1 = region1->createSubregion(regionNames[0], rattrsPtr3);
  RegionPtr subregion2 = region2->createSubregion(regionNames[1], rattrsPtr4);
  if (callReadyForEventsAPI) {
    getHelper()->cachePtr->readyForEvents();
  }
}
DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pool_Locator)
  {
    initClient(true);
    createPooledRegionMine(false);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, NotAutoReady_CallReadyForEvents)
  {
    PropertiesPtr props(Properties::create());
    props->insert("auto-ready-for-events", "false");
    initClient(true, props);
    createPooledRegionMine(true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, NotAutoReady_DontCallReadyForEvents)
  {
    PropertiesPtr props(Properties::create());
    props->insert("auto-ready-for-events", "false");
    initClient(true, props);
    createPooledRegionMine(false);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Verify)
  {
    SLEEP(10000);
    ASSERT(isRegionLive[0], "Client should have gotten region1 live event");
    isRegionLive[0] = false;
    ASSERT(isRegionLive[1], "Client should have gotten region2 live event");
    isRegionLive[1] = false;
    ASSERT(isRegionLive[2], "Client should have gotten subregion1 live event");
    isRegionLive[2] = false;
    ASSERT(isRegionLive[3], "Client should have gotten subregion2 live event");
    isRegionLive[3] = false;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyNotLive)
  {
    SLEEP(10000);
    ASSERT(!isRegionLive[0],
           "Client should not have gotten region1 live event");
    ASSERT(!isRegionLive[1],
           "Client should not have gotten region2 live event");
    ASSERT(!isRegionLive[2],
           "Client should not have gotten subregion1 live event");
    ASSERT(!isRegionLive[3],
           "Client should not have gotten subregion2 live event");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyDead)
  {
    SLEEP(10000);
    ASSERT(isRegionDead[0],
           "Client should have gotten region1 disconnected event");
    isRegionDead[0] = false;
    ASSERT(isRegionDead[1],
           "Client should have gotten region2 disconnected event");
    isRegionDead[1] = false;
    ASSERT(isRegionDead[2],
           "Client should have gotten subregion1 disconnected event");
    isRegionDead[2] = false;
    ASSERT(isRegionDead[3],
           "Client should have gotten subregion2 disconnected event");
    isRegionDead[3] = false;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyNotDead)
  {
    SLEEP(10000);
    ASSERT(!isRegionDead[0],
           "Client should not have gotten region1 disconnected event");
    ASSERT(!isRegionDead[1],
           "Client should not have gotten region2 disconnected event");
    ASSERT(!isRegionDead[2],
           "Client should not have gotten subregion1 disconnected event");
    ASSERT(!isRegionDead[3],
           "Client should not have gotten subregion2 disconnected event");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, populateServer)
  {
    SLEEP(10000);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyPtr = createKey("PXR");
    CacheableStringPtr valPtr = CacheableString::create("PXR1");
    regPtr->create(keyPtr, valPtr);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer)
  {
    if (isLocalServer) CacheHelper::closeServer(1);
    LOG("SERVER stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StopClient1)
  {
    cleanProc();
    LOG("CLIENT1 stopped");
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(SetupClient1_Pool_Locator);
    CALL_TASK(populateServer);
    CALL_TASK(StopServer);
    CALL_TASK(VerifyDead);
    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(Verify);
    CALL_TASK(StopClient1);
    CALL_TASK(NotAutoReady_CallReadyForEvents);
    CALL_TASK(Verify);
    CALL_TASK(populateServer);
    CALL_TASK(StopServer);
    CALL_TASK(VerifyDead);
    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(Verify);
    CALL_TASK(StopClient1);
    CALL_TASK(NotAutoReady_DontCallReadyForEvents);
    CALL_TASK(VerifyNotLive);
    CALL_TASK(populateServer);
    CALL_TASK(StopServer);
    CALL_TASK(VerifyNotDead);
    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(VerifyNotLive);
    CALL_TASK(StopClient1);
    CALL_TASK(StopServer);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
