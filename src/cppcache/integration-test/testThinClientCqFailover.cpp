/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/CqAttributesFactory.hpp>
#include <gfcpp/CqAttributes.hpp>
#include <gfcpp/CqListener.hpp>
#include <gfcpp/CqQuery.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <ace/Task.h>
#include <string>

#define ROOT_NAME "TestThinClientCqFailover"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

#include "QueryStrings.hpp"
#include "QueryHelper.hpp"

#include <gfcpp/Query.hpp>
#include <gfcpp/QueryService.hpp>

#include "ThinClientCQ.hpp"

using namespace gemfire;
using namespace test;
using namespace testobject;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

const char* cqName = "MyCq";

class MyCqListener : public CqListener {
  bool m_failedOver;
  uint32_t m_cnt_before;
  uint32_t m_cnt_after;

 public:
  MyCqListener() : m_failedOver(false), m_cnt_before(0), m_cnt_after(0) {}

  void setFailedOver() { m_failedOver = true; }
  uint32_t getCountBefore() { return m_cnt_before; }
  uint32_t getCountAfter() { return m_cnt_after; }

  void onEvent(const CqEvent& cqe) {
    if (m_failedOver) {
      // LOG("after:MyCqListener::OnEvent called");
      m_cnt_after++;
    } else {
      // LOG("before:MyCqListener::OnEvent called");
      m_cnt_before++;
    }
  }
  void onError(const CqEvent& cqe) {
    if (m_failedOver) {
      // LOG("after: MyCqListener::OnError called");
      m_cnt_after++;
    } else {
      // LOG("before: MyCqListener::OnError called");
      m_cnt_before++;
    }
  }
  void close() { LOG("MyCqListener::close called"); }
};

class KillServerThread : public ACE_Task_Base {
 public:
  bool m_running;
  MyCqListener* m_listener;
  explicit KillServerThread(MyCqListener* listener)
      : m_running(false), m_listener(listener) {}
  int svc(void) {
    while (m_running == true) {
      CacheHelper::closeServer(1);
      LOG("THREAD CLOSED SERVER 1");
      // m_listener->setFailedOver();
      m_running = false;
    }
    return 0;
  }
  void start() {
    m_running = true;
    activate();
  }
  void stop() {
    m_running = false;
    wait();
  }
};

void initClientCq(const bool isthinClient) {
  try {
    Serializable::registerType(Position::createDeserializable);
    Serializable::registerType(Portfolio::createDeserializable);
  } catch (const IllegalStateException&) {
    // ignore exception
  }

  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

const char* regionNamesCq[] = {"Portfolios", "Positions"};

KillServerThread* kst = NULL;

DUNIT_TASK_DEFINITION(SERVER1, CreateLocator)
  {
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

void createServer() {
  LOG("Starting SERVER1...");
  if (isLocalServer) {
    CacheHelper::initServer(1, "remotequery.xml", locatorsG);
  }
  LOG("SERVER1 started");
}

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_Locator)
  { createServer(); }
END_TASK_DEFINITION

void stepOne() {
  initClientCq(true);

  createRegionForCQ(regionNamesCq[0], USE_ACK, true);

  RegionPtr regptr = getHelper()->getRegion(regionNamesCq[0]);
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  RegionPtr subregPtr = regptr->createSubregion(regionNamesCq[1], lattribPtr);

  QueryHelper* qh = &QueryHelper::getHelper();

  qh->populatePortfolioData(regptr, 100, 20, 100);
  qh->populatePositionData(subregPtr, 100, 20);

  LOG("StepOne complete.");
}

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_PoolLocator)
  { stepOne(); }
END_TASK_DEFINITION

void stepOne2() {
  initClientCq(true);
  createRegionForCQ(regionNamesCq[0], USE_ACK, true);
  RegionPtr regptr = getHelper()->getRegion(regionNamesCq[0]);
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  RegionPtr subregPtr = regptr->createSubregion(regionNamesCq[1], lattribPtr);

  LOG("StepOne2 complete.");
}

DUNIT_TASK_DEFINITION(CLIENT2, StepOne2_PoolLocator)
  { stepOne2(); }
END_TASK_DEFINITION

void stepTwo() {
  LOG("Starting SERVER2...");
  if (isLocalServer) {
    CacheHelper::initServer(2, "cqqueryfailover.xml", locatorsG);
  }
  LOG("SERVER2 started");

  LOG("StepTwo complete.");
}

DUNIT_TASK_DEFINITION(SERVER2, StepTwo_Locator)
  { stepTwo(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    try {
      PoolPtr pool = PoolManager::find(regionNamesCq[0]);
      QueryServicePtr qs;
      if (pool != NULLPTR) {
        // Using region name as pool name as in ThinClientCq.hpp
        qs = pool->getQueryService();
      } else {
        qs = getHelper()->cachePtr->getQueryService();
      }
      CqAttributesFactory cqFac;
      CqListenerPtr cqLstner(new MyCqListener());
      cqFac.addCqListener(cqLstner);
      CqAttributesPtr cqAttr = cqFac.create();

      char* qryStr = (char*)"select * from /Portfolios p where p.ID != 2";
      CqQueryPtr qry = qs->newCq(cqName, qryStr, cqAttr);
      qry->execute();

      SLEEP(15000);
    } catch (IllegalStateException& ise) {
      char isemsg[500] = {0};
      ACE_OS::snprintf(isemsg, 499, "IllegalStateException: %s",
                       ise.getMessage());
      LOG(isemsg);
      FAIL(isemsg);
    } catch (Exception& excp) {
      char excpmsg[500] = {0};
      ACE_OS::snprintf(excpmsg, 499, "Exception: %s", excp.getMessage());
      LOG(excpmsg);
      FAIL(excpmsg);
    } catch (...) {
      LOG("Got an exception!");
      FAIL("Got an exception!");
    }

    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepThree2)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);

    QueryHelper* qh = &QueryHelper::getHelper();

    qh->populatePortfolioData(regPtr0, 150, 40, 150);
    qh->populatePositionData(subregPtr0, 150, 40);
    for (int i = 1; i < 150; i++) {
      CacheablePtr port(new Portfolio(i, 150));

      CacheableKeyPtr keyport = CacheableKey::create((char*)"port1-1");
      regPtr0->put(keyport, port);
      SLEEP(100);  // sleep a while to allow server query to complete
    }

    LOG("StepThree2 complete");
    SLEEP(15000);  // sleep 0.25 min to allow server query to complete
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree3)
  {
    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      // Using region name as pool name as in ThinClientCq.hpp
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    CqQueryPtr qry = qs->getCq(cqName);
    ASSERT(qry != NULLPTR, "failed to get CqQuery");
    CqAttributesPtr cqAttr = qry->getCqAttributes();
    ASSERT(cqAttr != NULLPTR, "failed to get CqAttributes");
    CqListenerPtr cqLstner = NULLPTR;
    try {
      VectorOfCqListener vl;
      cqAttr->getCqListeners(vl);
      cqLstner = vl[0];
    } catch (Exception& excp) {
      char excpmsg[500] = {0};
      ACE_OS::snprintf(excpmsg, 499, "Exception: %s", excp.getMessage());
      LOG(excpmsg);
      ASSERT(false, "get listener failed");
    }
    ASSERT(cqLstner != NULLPTR, "listener is NULL");
    MyCqListener* myListener = dynamic_cast<MyCqListener*>(cqLstner.ptr());
    ASSERT(myListener != NULL, "my listener is NULL<cast failed>");
    kst = new KillServerThread(myListener);
    char buf[1024];
    sprintf(buf, "before kill server 1, before=%d, after=%d",
            myListener->getCountBefore(), myListener->getCountAfter());
    LOG(buf);
    ASSERT(myListener->getCountAfter() == 0,
           "cq after failover should be zero");
    ASSERT(myListener->getCountBefore() == 6109,
           "check cq event count before failover");
    kst->start();
    SLEEP(1500);  // to allow the kill performed
    kst->stop();
    myListener->setFailedOver();
    /*
    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);
    for(int i=1; i < 1500; i++)
    {
        CacheablePtr port(new Portfolio(i, 15));

        CacheableKeyPtr keyport = CacheableKey::create("port1-1");
        try {
          regPtr0->put(keyport, port);
        } catch (...)
        {
          LOG("Failover in progress sleep for 100 ms");
           SLEEP(100); // waiting for failover to complete
           continue;
        }
        LOG("Failover completed");
        myListener->setFailedOver();
        break;
    }
    */
    SLEEP(1500);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepThree4)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);

    QueryHelper* qh = &QueryHelper::getHelper();

    qh->populatePortfolioData(regPtr0, 10, 40, 10);
    qh->populatePositionData(subregPtr0, 10, 4);
    for (int i = 1; i < 150; i++) {
      CacheablePtr port(new Portfolio(i, 10));

      CacheableKeyPtr keyport = CacheableKey::create("port1-1");
      regPtr0->put(keyport, port);
      SLEEP(100);  // sleep a while to allow server query to complete
    }

    LOG("StepTwo2 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      // Using region name as pool name as in ThinClientCq.hpp
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    CqQueryPtr qry = qs->getCq(cqName);
    ASSERT(qry != NULLPTR, "failed to get CqQuery");
    CqAttributesPtr cqAttr = qry->getCqAttributes();
    ASSERT(cqAttr != NULLPTR, "failed to get CqAttributes");
    CqListenerPtr cqLstner = NULLPTR;
    try {
      VectorOfCqListener vl;
      cqAttr->getCqListeners(vl);
      cqLstner = vl[0];
    } catch (Exception& excp) {
      char excpmsg[500] = {0};
      ACE_OS::snprintf(excpmsg, 499, "Exception: %s", excp.getMessage());
      LOG(excpmsg);
      ASSERT(false, "get listener failed");
    }
    ASSERT(cqLstner != NULLPTR, "listener is NULL");
    MyCqListener* myListener = dynamic_cast<MyCqListener*>(cqLstner.ptr());
    ASSERT(myListener != NULL, "my listener is NULL<cast failed>");
    char buf[1024];
    sprintf(buf, "after failed over: before=%d, after=%d",
            myListener->getCountBefore(), myListener->getCountAfter());
    LOG(buf);
    ASSERT(myListener->getCountBefore() == 6109,
           "check cq event count before failover");
    ASSERT(myListener->getCountAfter() == 509,
           "check cq event count after failover");
    qry->close();

    LOG("cleanProc 1...");
    cleanProc();
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  {
    LOG("cleanProc 2...");
    cleanProc();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CloseServer2)
  {
    LOG("closing Server2...");
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseLocator)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

/*
DUNIT_TASK(SERVER1,CloseServer1)
{
  LOG("closing Server1...");
  if ( isLocalServer ) {
    CacheHelper::closeServer( 1 );
    LOG("SERVER1 stopped");
  }
}
END_TASK(CloseServer1)
*/

void doThinClientCqFailover() {
  CALL_TASK(CreateLocator);
  CALL_TASK(CreateServer1_Locator);

  CALL_TASK(StepOne_PoolLocator);
  CALL_TASK(StepOne2_PoolLocator);
  CALL_TASK(StepTwo_Locator);

  CALL_TASK(StepThree);
  CALL_TASK(StepThree2);
  CALL_TASK(StepThree3);
  CALL_TASK(StepThree4);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer2);

  CALL_TASK(CloseLocator);
}

DUNIT_MAIN
  { doThinClientCqFailover(); }
END_MAIN
