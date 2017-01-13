/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * testThinClientCqDelta.cpp
 *
 *  Created on: Sept 17, 2009
 *      Author: abhaware
 */

#include "testobject/DeltaTestImpl.hpp"
#include "fw_dunit.hpp"
#include <string>
#include "CacheHelper.hpp"
#include <gfcpp/CqAttributesFactory.hpp>
#include <gfcpp/CqAttributes.hpp>
#include <gfcpp/CqListener.hpp>
#include <gfcpp/CqQuery.hpp>
#include <gfcpp/CqServiceStatistics.hpp>

using namespace gemfire;
using namespace test;
using namespace testobject;

CacheHelper* cacheHelper = NULL;

#include "locator_globals.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#include "LocatorHelper.hpp"

class CqDeltaListener : public CqListener {
 public:
  CqDeltaListener() : m_deltaCount(0), m_valueCount(0) {}

  virtual void onEvent(const CqEvent& aCqEvent) {
    CacheableBytesPtr deltaValue = aCqEvent.getDeltaValue();
    DeltaTestImpl newValue;
    DataInput input(deltaValue->value(), deltaValue->length());
    newValue.fromDelta(input);
    if (newValue.getIntVar() == 5) {
      m_deltaCount++;
    }
    DeltaTestImplPtr dptr =
        staticCast<DeltaTestImplPtr>(aCqEvent.getNewValue());
    if (dptr->getIntVar() == 5) {
      m_valueCount++;
    }
  }

  int getDeltaCount() { return m_deltaCount; }
  int getValueCount() { return m_valueCount; }

 private:
  int m_deltaCount;
  int m_valueCount;
};
typedef SharedPtr<CqDeltaListener> CqDeltaListenerPtr;
CqDeltaListenerPtr g_CqListener;

void initClient(const bool isthinClient) {
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void initClientNoPools() {
  cacheHelper = new CacheHelper(0);
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void cleanProc() {
  if (cacheHelper != NULL) {
    delete cacheHelper;
    cacheHelper = NULL;
  }
}

CacheHelper* getHelper() {
  ASSERT(cacheHelper != NULL, "No cacheHelper initialized.");
  return cacheHelper;
}

void createPooledRegion(const char* name, bool ackMode, const char* locators,
                        const char* poolname,
                        bool clientNotificationEnabled = false,
                        bool cachingEnable = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr =
      getHelper()->createPooledRegion(name, ackMode, locators, poolname,
                                      cachingEnable, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Pooled Region created.");
}

void createPooledLRURegion(const char* name, bool ackMode, const char* locators,
                           const char* poolname,
                           bool clientNotificationEnabled = false,
                           bool cachingEnable = true) {
  LOG(" createPooledLRURegion entered");
  RegionPtr regPtr = getHelper()->createPooledRegionDiscOverFlow(
      name, ackMode, locators, poolname, cachingEnable,
      clientNotificationEnabled, 0, 0, 0, 0, 3 /*LruLimit = 3*/);
  LOG(" createPooledLRURegion exited");
}

void createRegion(const char* name, bool ackMode,
                  bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, true, NULLPTR,
                                               clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}
const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};

const char* regionNames[] = {"DistRegionAck", "DistRegionAck1"};

const bool USE_ACK = true;
const bool NO_ACK ATTR_UNUSED = false;

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_",
                       true);
    try {
      Serializable::registerType(DeltaTestImpl::create);
    } catch (IllegalStateException&) {
      //  ignore exception caused by type reregistration.
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_",
                       true);
    try {
      Serializable::registerType(DeltaTestImpl::create);
    } catch (IllegalStateException&) {
      //  ignore exception caused by type reregistration.
    }
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);

    PoolPtr pool = PoolManager::find("__TESTPOOL1_");
    QueryServicePtr qs;
    qs = pool->getQueryService();
    CqAttributesFactory cqFac;
    g_CqListener = new CqDeltaListener();
    CqListenerPtr cqListener = g_CqListener;
    cqFac.addCqListener(cqListener);
    CqAttributesPtr cqAttr = cqFac.create();
    CqQueryPtr qry =
        qs->newCq("Cq_with_delta",
                  "select * from /DistRegionAck d where d.intVar > 4", cqAttr);
    qs->executeCqs();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1_NoPools)
  {
    initClientNoPools();
    createRegion(regionNames[0], USE_ACK, true);
    try {
      Serializable::registerType(DeltaTestImpl::create);
    } catch (IllegalStateException&) {
      //  ignore exception caused by type reregistration.
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2_NoPools)
  {
    initClientNoPools();
    createRegion(regionNames[0], USE_ACK, true);
    try {
      Serializable::registerType(DeltaTestImpl::create);
    } catch (IllegalStateException&) {
      //  ignore exception caused by type reregistration.
    }
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);

    QueryServicePtr qs;
    qs = getHelper()->getQueryService();
    CqAttributesFactory cqFac;
    g_CqListener = new CqDeltaListener();
    CqListenerPtr cqListener = g_CqListener;
    cqFac.addCqListener(cqListener);
    CqAttributesPtr cqAttr = cqFac.create();
    CqQueryPtr qry =
        qs->newCq("Cq_with_delta",
                  "select * from /DistRegionAck d where d.intVar > 4", cqAttr);
    qs->executeCqs();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Client1_Put)
  {
    CacheableKeyPtr keyPtr = createKey(keys[0]);
    DeltaTestImplPtr dptr(new DeltaTestImpl());
    CacheablePtr valPtr(dptr);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->put(keyPtr, valPtr);
    dptr->setIntVar(5);
    dptr->setDelta(true);
    regPtr->put(keyPtr, valPtr);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Client2_VerifyDelta)
  {
    // Wait for notification
    SLEEP(5000);
    ASSERT(g_CqListener->getDeltaCount() == 1,
           "Delta from CQ event does not have expected value");
    ASSERT(g_CqListener->getValueCount() == 1,
           "Value from CQ event is incorrect");
  }

END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_ForCqDelta)
  {
    // starting servers
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_with_delta_test_impl.xml",
                              locatorsG);
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);

    CALL_TASK(CreateServer1_ForCqDelta)

    CALL_TASK(CreateClient1);
    CALL_TASK(CreateClient2);

    CALL_TASK(Client1_Put);
    CALL_TASK(Client2_VerifyDelta);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);

    CALL_TASK(CloseServer1);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
