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
#include <gfcpp/CqStatusListener.hpp>
#include <gfcpp/CqQuery.hpp>
#include <gfcpp/CqServiceStatistics.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <string>

#define ROOT_NAME "TestThinClientCq"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

#include "QueryStrings.hpp"
#include "QueryHelper.hpp"

#include <gfcpp/Query.hpp>
#include <gfcpp/QueryService.hpp>

#include "ThinClientCQ.hpp"

using namespace gemfire;
using namespace test;
using namespace testData;

#define CLIENT1 s1p1
#define SERVER1 s2p1
#define CLIENT2 s1p2
#define LOCATORSERVER s2p2

#define MAX_LISTNER 8

// CacheHelper* cacheHelper = NULL;

static bool m_isPdx = false;

const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
const char* cqNames[MAX_LISTNER] = {"MyCq_0", "MyCq_1", "MyCq_2", "MyCq_3",
                                    "MyCq_4", "MyCq_5", "MyCq_6", "MyCq_7"};

const char* regionName = "DistRegionAck";
const char* regionName1 = "DistRegionAck1";
const char* cqName = "testCQAllServersLeave";
const char* cqName1 = "testCQAllServersLeave1";
const char* cqQueryStatusString = "select * from /DistRegionAck";
const char* cqQueryStatusString1 = "select * from /DistRegionAck1";

const char* queryStrings[MAX_LISTNER] = {
    "select * from /Portfolios p where p.ID < 4",
    "select * from /Portfolios p where p.ID < 2",
    "select * from /Portfolios p where p.ID != 2",
    "select * from /Portfolios p where p.ID != 3",
    "select * from /Portfolios p where p.ID != 4",
    "select * from /Portfolios p where p.ID != 5",
    "select * from /Portfolios p where p.ID != 6",
    "select * from /Portfolios p where p.ID != 7"};

void initClientCq(const bool isthinClient) {
  try {
    Serializable::registerType(Position::createDeserializable);
    Serializable::registerType(Portfolio::createDeserializable);

    Serializable::registerPdxType(PositionPdx::createDeserializable);
    Serializable::registerPdxType(PortfolioPdx::createDeserializable);

  } catch (const IllegalStateException&) {
    // ignore exception
  }

  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

const char* regionNamesCq[] = {"Portfolios", "Positions", "Portfolios2",
                               "Portfolios3"};

class MyCqListener1026 : public CqListener {
 public:
  void close() { LOGINFO("MyCqListener::close called"); }
  void onError(const CqEvent& cqe) { LOGINFO("MyCqListener::OnError called"); }

  void onEvent(const CqEvent& cqe) { LOGINFO("MyCqListener::onEvent called"); }
};

class MyCqListener : public CqListener {
  uint8_t m_id;
  uint32_t m_numInserts;
  uint32_t m_numUpdates;
  uint32_t m_numDeletes;
  uint32_t m_numEvents;

 public:
  uint8_t getId() { return m_id; }
  uint32_t getNumInserts() { return m_numInserts; }
  uint32_t getNumUpdates() { return m_numUpdates; }
  uint32_t getNumDeletes() { return m_numDeletes; }
  uint32_t getNumEvents() { return m_numEvents; }
  explicit MyCqListener(uint8_t id)
      : m_id(id),
        m_numInserts(0),
        m_numUpdates(0),
        m_numDeletes(0),
        m_numEvents(0) {}
  inline void updateCount(const CqEvent& cqEvent) {
    m_numEvents++;
    switch (cqEvent.getQueryOperation()) {
      case CqOperation::OP_TYPE_CREATE:
        m_numInserts++;
        break;
      case CqOperation::OP_TYPE_UPDATE:
        m_numUpdates++;
        break;
      case CqOperation::OP_TYPE_DESTROY:
        m_numDeletes++;
        break;
      default:
        break;
    }
  }

  void onEvent(const CqEvent& cqe) {
    //  LOG("MyCqListener::OnEvent called");
    updateCount(cqe);
  }
  void onError(const CqEvent& cqe) {
    updateCount(cqe);
    //   LOG("MyCqListener::OnError called");
  }
  void close() {
    //   LOG("MyCqListener::close called");
  }
};

class MyCqStatusListener : public CqStatusListener {
  uint8_t m_id;
  uint32_t m_numInserts;
  uint32_t m_numUpdates;
  uint32_t m_numDeletes;
  uint32_t m_numEvents;
  uint32_t m_cqsConnectedCount;
  uint32_t m_cqsDisconnectedCount;

 public:
  uint8_t getId() { return m_id; }
  uint32_t getNumInserts() { return m_numInserts; }
  uint32_t getNumUpdates() { return m_numUpdates; }
  uint32_t getNumDeletes() { return m_numDeletes; }
  uint32_t getNumEvents() { return m_numEvents; }
  uint32_t getCqsConnectedCount() { return m_cqsConnectedCount; }
  uint32_t getCqsDisConnectedCount() { return m_cqsDisconnectedCount; }
  explicit MyCqStatusListener(uint8_t id)
      : m_id(id),
        m_numInserts(0),
        m_numUpdates(0),
        m_numDeletes(0),
        m_numEvents(0),
        m_cqsConnectedCount(0),
        m_cqsDisconnectedCount(0) {}
  inline void updateCount(const CqEvent& cqEvent) {
    m_numEvents++;
    switch (cqEvent.getQueryOperation()) {
      case CqOperation::OP_TYPE_CREATE: {
        m_numInserts++;
        LOG("MyCqStatusListener::OnEvent OP_TYPE_CREATE");
        break;
      }
      case CqOperation::OP_TYPE_UPDATE: {
        LOG("MyCqStatusListener::OnEvent OP_TYPE_UPDATE");
        m_numUpdates++;
        break;
      }
      case CqOperation::OP_TYPE_DESTROY: {
        LOG("MyCqStatusListener::OnEvent OP_TYPE_DESTROY");
        m_numDeletes++;
        break;
      }
      default:
        break;
    }
  }

  void onEvent(const CqEvent& cqe) {
    LOGINFO("MyCqStatusListener::OnEvent %d called", m_id);
    updateCount(cqe);
  }
  void onError(const CqEvent& cqe) {
    updateCount(cqe);
    LOGINFO("MyCqStatusListener::OnError %d called", m_id);
  }
  void close() { LOGINFO("MyCqStatusListener::close %d called", m_id); }
  void onCqDisconnected() {
    LOGINFO("MyCqStatusListener %d got onCqDisconnected", m_id);
    m_cqsDisconnectedCount++;
  }
  void onCqConnected() {
    LOGINFO("MyCqStatusListener %d got onCqConnected", m_id);
    m_cqsConnectedCount++;
  }
  void clear() {
    m_numInserts = 0;
    m_numUpdates = 0;
    m_numDeletes = 0;
    m_numEvents = 0;
    m_cqsDisconnectedCount = 0;
    m_cqsConnectedCount = 0;
  }
};
typedef SharedPtr<MyCqStatusListener> MyCqStatusListenerPtr;

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateLocator)
  {
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

void createServer(bool locator = false) {
  LOG("Starting SERVER1...");
  if (isLocalServer) {
    CacheHelper::initServer(1, "remotequery.xml", locator ? locHostPort : NULL);
  }
  LOG("SERVER1 started");
}

void createServer2(bool locator = false) {
  LOG("Starting SERVER2...");
  if (isLocalServer) {
    CacheHelper::initServer(2, "remotequery2.xml",
                            locator ? locHostPort : NULL);
  }
  LOG("SERVER2 started");
}

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  { createServer(false); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateServer2)
  { createServer2(false); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateServer2_Locator)
  { createServer2(true); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_Locator)
  { createServer(true); }
END_TASK_DEFINITION

void createServer_group(bool locator, const char* XML) {
  LOG("Starting SERVER1...");
  if (isLocalServer) {
    CacheHelper::initServer(1, XML, locator ? locHostPort : NULL);
  }
  LOG("SERVER1 started");
}

void createServer_group2(bool locator, const char* XML) {
  LOG("Starting SERVER2...");
  if (isLocalServer) {
    CacheHelper::initServer(2, XML, locator ? locHostPort : NULL);
  }
  LOG("SERVER2 started");
}

DUNIT_TASK_DEFINITION(SERVER1, CreateServer_servergrop)
  { createServer_group(true, "cacheserver_servergroup.xml"); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateServer_servergrop2)
  { createServer_group2(true, "cacheserver_servergroup2.xml"); }
END_TASK_DEFINITION

void stepOne() {
  initClientCq(true);
  createRegionForCQ(regionNamesCq[0], USE_ACK, true);
  RegionPtr regptr = getHelper()->getRegion(regionNamesCq[0]);
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  RegionPtr subregPtr = regptr->createSubregion(regionNamesCq[1], lattribPtr);

  LOG("StepOne complete.");
}

void initCqStatusClient() {
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(true);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");

  createRegionForCQ(regionName, USE_ACK, true);
  createRegionForCQ(regionName1, USE_ACK, true);
  LOG("initCqStatusClient complete.");
}

DUNIT_TASK_DEFINITION(CLIENT1, initCqStatusClientLoc)
  { initCqStatusClient(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
  {
    initClient(true);

    getHelper()->createPoolWithLocators("__TEST_POOL1__", locHostPort, true, -1,
                                        -1, -1, false, "group1");
    getHelper()->createRegionAndAttachPool(regionName, USE_ACK,
                                           "__TEST_POOL1__", true);

    getHelper()->createPoolWithLocators("__TEST_POOL2__", locHostPort, true, -1,
                                        -1, -1, false, "group2");
    getHelper()->createRegionAndAttachPool(regionName1, USE_ACK,
                                           "__TEST_POOL2__", true);

    LOG("StepOne_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

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

DUNIT_TASK_DEFINITION(CLIENT1, StepTwo)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);

    QueryHelper* qh = &QueryHelper::getHelper();

    if (!m_isPdx) {
      qh->populatePortfolioData(regPtr0, 2, 1, 1);
      qh->populatePositionData(subregPtr0, 2, 1);
    } else {
      qh->populatePortfolioPdxData(regPtr0, 2, 1, 1);
      qh->populatePositionPdxData(subregPtr0, 2, 1);
    }

    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    uint8_t i = 0;
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      // Using region name as pool name as in ThinClientCq.hpp
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    CqAttributesFactory cqFac;
    for (i = 0; i < MAX_LISTNER; i++) {
      CqListenerPtr cqLstner(new MyCqListener(i));
      cqFac.addCqListener(cqLstner);
      CqAttributesPtr cqAttr = cqFac.create();
      CqQueryPtr qry = qs->newCq(cqNames[i], queryStrings[i], cqAttr);
    }

    try {
      LOG("EXECUTE 1 START");

      qs->executeCqs();

      LOG("EXECUTE 1 STOP");
    } catch (const Exception& excp) {
      std::string logmsg = "";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
      excp.printStackTrace();
    }
    /* Test for #1026 */
    {
      try {
        LOG("Testing bug #1026");
        RegionPtr regionPtr = getHelper()->getRegion(regionNamesCq[0]);
        regionPtr->put("1026_Key1", "asdas");
        regionPtr->put("1026_Key2",
                       "Schüler");  // string with extended charater set
        regionPtr->put("1026_Key3",
                       "Gebäude");  // string with extended charater set
        regionPtr->put("1026_Key4",
                       "Königin");  // string with extended charater set

        const char* qryStr = "select * from /Portfolios p where p='Schüler'";

        CqListenerPtr cqLstner(new MyCqListener1026);
        cqFac.addCqListener(cqLstner);
        CqAttributesPtr cqAttr = cqFac.create();
        CqQueryPtr qry = qs->newCq((char*)"1026_MyCq", qryStr, cqAttr);

        // execute Cq Query with initial Results
        CqResultsPtr resultsPtr = qry->executeWithInitialResults();

        LOGINFO("ResultSet Query returned %d rows", resultsPtr->size());
        LOG("Testing bug #1026 Complete");
        // Iterate through the rows of the query result.
      } catch (const Exception& gemfireExcp) {
        LOGERROR("CqQuery GemFire Exception: %s", gemfireExcp.getMessage());
        FAIL(gemfireExcp.getMessage());
      }
    }

    LOG("StepThree complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, StepTwo2)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);

    QueryHelper* qh = &QueryHelper::getHelper();

    if (!m_isPdx) {
      qh->populatePortfolioData(regPtr0, 3, 2, 1);
      qh->populatePositionData(subregPtr0, 3, 2);
    } else {
      qh->populatePortfolioPdxData(regPtr0, 3, 2, 1);
      qh->populatePositionPdxData(subregPtr0, 3, 2);
    }

    CacheablePtr port = NULLPTR;
    for (int i = 1; i < 3; i++) {
      if (!m_isPdx) {
        port = CacheablePtr(new Portfolio(i, 2));
      } else {
        port = CacheablePtr(new PortfolioPdx(i, 2));
      }

      CacheableKeyPtr keyport = CacheableKey::create("port1-1");
      regPtr0->put(keyport, port);
      SLEEP(10);  // sleep a while to allow server query to complete
    }

    LOG("StepTwo2 complete. Sleeping .25 min for server query to complete...");
    SLEEP(15000);  // sleep .25 min to allow server query to complete
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFour)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      // Using region name as pool name as in ThinClientCq.hpp
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }

    char buf[1024];

    uint8_t i = 0;
    int j = 0;
    uint32_t inserts[MAX_LISTNER];
    uint32_t updates[MAX_LISTNER];
    uint32_t deletes[MAX_LISTNER];
    uint32_t events[MAX_LISTNER];
    for (i = 0; i < MAX_LISTNER; i++) {
      inserts[i] = 0;
      updates[i] = 0;
      deletes[i] = 0;
      events[i] = 0;
    }

    CqAttributesFactory cqFac;
    for (i = 0; i < MAX_LISTNER; i++) {
      sprintf(buf, "get info for cq[%s]:", cqNames[i]);
      LOG(buf);
      CqQueryPtr cqy = qs->getCq(cqNames[i]);
      CqStatisticsPtr cqStats = cqy->getStatistics();
      sprintf(buf,
              "Cq[%s]From CqStatistics: numInserts[%d], numDeletes[%d], "
              "numUpdates[%d], numEvents[%d]",
              cqNames[i], cqStats->numInserts(), cqStats->numDeletes(),
              cqStats->numUpdates(), cqStats->numEvents());
      LOG(buf);
      for (j = 0; j <= i; j++) {
        inserts[j] += cqStats->numInserts();
        updates[j] += cqStats->numUpdates();
        deletes[j] += cqStats->numDeletes();
        events[j] += cqStats->numEvents();
      }
      CqAttributesPtr cqAttr = cqy->getCqAttributes();
      VectorOfCqListener vl;
      cqAttr->getCqListeners(vl);
      sprintf(buf, "number of listeners for cq[%s] is %d", cqNames[i],
              vl.size());
      LOG(buf);
      ASSERT(vl.size() == i + 1, "incorrect number of listeners");
      if (i == (MAX_LISTNER - 1)) {
        MyCqListener* myLl[MAX_LISTNER];
        for (int k = 0; k < MAX_LISTNER; k++) {
          MyCqListener* ml = dynamic_cast<MyCqListener*>(vl[k].ptr());
          myLl[ml->getId()] = ml;
        }
        for (j = 0; j < MAX_LISTNER; j++) {
          MyCqListener* ml = myLl[j];
          sprintf(buf,
                  "MyCount for Listener[%d]: numInserts[%d], numDeletes[%d], "
                  "numUpdates[%d], numEvents[%d]",
                  j, ml->getNumInserts(), ml->getNumDeletes(),
                  ml->getNumUpdates(), ml->getNumEvents());
          LOG(buf);
          sprintf(buf,
                  "sum of stats for Listener[%d]: numInserts[%d], "
                  "numDeletes[%d], numUpdates[%d], numEvents[%d]",
                  j, inserts[j], deletes[j], updates[j], events[j]);
          LOG(buf);
          ASSERT(ml->getNumInserts() == inserts[j],
                 "accumulative insert count incorrect");
          ASSERT(ml->getNumUpdates() == updates[j],
                 "accumulative updates count incorrect");
          ASSERT(ml->getNumDeletes() == deletes[j],
                 "accumulative deletes count incorrect");
          ASSERT(ml->getNumEvents() == events[j],
                 "accumulative events count incorrect");
        }
        LOG("removing listener");
        CqAttributesMutatorPtr cqAttrMtor = cqy->getCqAttributesMutator();
        CqListenerPtr ptr = vl[0];
        cqAttrMtor->removeCqListener(ptr);
        cqAttr->getCqListeners(vl);
        sprintf(buf, "number of listeners for cq[%s] is %d", cqNames[i],
                vl.size());
        LOG(buf);
        ASSERT(vl.size() == i, "incorrect number of listeners");
      }
    }
    try {
      CqQueryPtr cqy = qs->getCq(cqNames[1]);
      cqy->stop();

      cqy = qs->getCq(cqNames[6]);

      sprintf(buf, "cq[%s] should have been running!", cqNames[6]);
      ASSERT(cqy->isRunning() == true, buf);
      bool got_exception = false;
      try {
        cqy->execute();
      } catch (IllegalStateException& excp) {
        std::string failmsg = "";
        failmsg += excp.getName();
        failmsg += ": ";
        failmsg += excp.getMessage();
        LOG(failmsg.c_str());
        got_exception = true;
      }
      sprintf(buf, "cq[%s] should gotten exception!", cqNames[6]);
      ASSERT(got_exception == true, buf);

      cqy->stop();

      sprintf(buf, "cq[%s] should have been stopped!", cqNames[6]);
      ASSERT(cqy->isStopped() == true, buf);

      cqy = qs->getCq(cqNames[2]);
      cqy->close();

      sprintf(buf, "cq[%s] should have been closed!", cqNames[2]);
      ASSERT(cqy->isClosed() == true, buf);

      cqy = qs->getCq(cqNames[2]);
      sprintf(buf, "cq[%s] should have been removed after close!", cqNames[2]);
      ASSERT(cqy == NULLPTR, buf);
    } catch (Exception& excp) {
      std::string failmsg = "";
      failmsg += excp.getName();
      failmsg += ": ";
      failmsg += excp.getMessage();
      LOG(failmsg.c_str());
      FAIL(failmsg.c_str());
      excp.printStackTrace();
    }
    CqServiceStatisticsPtr serviceStats = qs->getCqServiceStatistics();
    ASSERT(serviceStats != NULLPTR, "serviceStats is NULL");
    sprintf(buf,
            "numCqsActive=%d, numCqsCreated=%d, "
            "numCqsClosed=%d,numCqsStopped=%d, numCqsOnClient=%d",
            serviceStats->numCqsActive(), serviceStats->numCqsCreated(),
            serviceStats->numCqsClosed(), serviceStats->numCqsStopped(),
            serviceStats->numCqsOnClient());
    LOG(buf);
    /*
    for(i=0; i < MAX_LISTNER; i++)
    {
     CqQueryPtr cqy = qs->getCq(cqNames[i]);
     CqState::StateType state = cqy->getState();
     CqState cqState;
     cqState.setState(state);
     sprintf(buf, "cq[%s] is in state[%s]", cqNames[i], cqState.toString());
     LOG(buf);
    }
    */

    ASSERT(serviceStats->numCqsActive() == 6, "active count incorrect!");
    ASSERT(serviceStats->numCqsCreated() == 9, "created count incorrect!");
    ASSERT(serviceStats->numCqsClosed() == 1, "closed count incorrect!");
    ASSERT(serviceStats->numCqsStopped() == 2, "stopped count incorrect!");
    ASSERT(serviceStats->numCqsOnClient() == 8, "cq count incorrect!");

    try {
      qs->stopCqs();
    } catch (Exception& excp) {
      std::string failmsg = "";
      failmsg += excp.getName();
      failmsg += ": ";
      failmsg += excp.getMessage();
      LOG(failmsg.c_str());
      FAIL(failmsg.c_str());
      excp.printStackTrace();
    }
    sprintf(buf,
            "numCqsActive=%d, numCqsCreated=%d, "
            "numCqsClosed=%d,numCqsStopped=%d, numCqsOnClient=%d",
            serviceStats->numCqsActive(), serviceStats->numCqsCreated(),
            serviceStats->numCqsClosed(), serviceStats->numCqsStopped(),
            serviceStats->numCqsOnClient());
    LOG(buf);
    ASSERT(serviceStats->numCqsActive() == 0, "active count incorrect!");
    ASSERT(serviceStats->numCqsCreated() == 9, "created count incorrect!");
    ASSERT(serviceStats->numCqsClosed() == 1, "closed count incorrect!");
    ASSERT(serviceStats->numCqsStopped() == 8, "stopped count incorrect!");
    ASSERT(serviceStats->numCqsOnClient() == 8, "cq count incorrect!");
    try {
      qs->closeCqs();
    } catch (Exception& excp) {
      std::string failmsg = "";
      failmsg += excp.getName();
      failmsg += ": ";
      failmsg += excp.getMessage();
      LOG(failmsg.c_str());
      FAIL(failmsg.c_str());
      excp.printStackTrace();
    }
    sprintf(buf,
            "numCqsActive=%d, numCqsCreated=%d, "
            "numCqsClosed=%d,numCqsStopped=%d, numCqsOnClient=%d",
            serviceStats->numCqsActive(), serviceStats->numCqsCreated(),
            serviceStats->numCqsClosed(), serviceStats->numCqsStopped(),
            serviceStats->numCqsOnClient());
    LOG(buf);
    ASSERT(serviceStats->numCqsActive() == 0, "active count incorrect!");
    ASSERT(serviceStats->numCqsCreated() == 9, "created count incorrect!");
    ASSERT(serviceStats->numCqsClosed() == 9, "closed count incorrect!");
    ASSERT(serviceStats->numCqsStopped() == 0, "stopped count incorrect!");
    ASSERT(serviceStats->numCqsOnClient() == 0, "cq count incorrect!");

    i = 0;
    CqListenerPtr cqLstner(new MyCqListener(i));
    cqFac.addCqListener(cqLstner);
    CqAttributesPtr cqAttr = cqFac.create();
    try {
      CqQueryPtr qry = qs->newCq(cqNames[i], queryStrings[i], cqAttr);
      qry->execute();
      qry->stop();
      qry->close();
    } catch (Exception& excp) {
      std::string failmsg = "";
      failmsg += excp.getName();
      failmsg += ": ";
      failmsg += excp.getMessage();
      LOG(failmsg.c_str());
      FAIL(failmsg.c_str());
      excp.printStackTrace();
    }

    LOG("StepFour complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
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

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    LOG("closing Server1...");
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CloseServer2)
  {
    LOG("closing Server2...");
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CloseLocator)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetPortfolioTypeToPdxC1)
  { m_isPdx = true; }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, UnsetPortfolioTypeToPdxC1)
  { m_isPdx = false; }
END_TASK_DEFINITION
//
DUNIT_TASK_DEFINITION(CLIENT2, SetPortfolioTypeToPdxC2)
  { m_isPdx = true; }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, UnsetPortfolioTypeToPdxC2)
  { m_isPdx = false; }
END_TASK_DEFINITION
//
void doThinClientCq(bool poolConfig = false, bool poolLocators = false) {
  CALL_TASK(CreateLocator);
  CALL_TASK(CreateServer1_Locator);

  CALL_TASK(StepOne_PoolLocator);
  CALL_TASK(StepOne2_PoolLocator);

  CALL_TASK(StepTwo);
  CALL_TASK(StepThree);
  CALL_TASK(StepTwo2);
  CALL_TASK(StepFour);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator);
}

DUNIT_TASK_DEFINITION(CLIENT1, createCQ)
  {
    SLEEP(10000);
    // Create CqAttributes and Install Listener
    PoolPtr pool = PoolManager::find(regionName);
    QueryServicePtr qs = pool->getQueryService();
    CqAttributesFactory cqFac;
    CqStatusListenerPtr cqLstner(new MyCqStatusListener(100));
    cqFac.addCqListener(cqLstner);
    CqAttributesPtr cqAttr = cqFac.create();
    CqQueryPtr cq =
        qs->newCq(const_cast<char*>(cqName), cqQueryStatusString, cqAttr);

    cq->execute();
    SLEEP(20000);

    cqAttr = cq->getCqAttributes();
    VectorOfCqListener vl;
    cqAttr->getCqListeners(vl);
    MyCqStatusListener* myStatusCq =
        dynamic_cast<MyCqStatusListener*>(vl[0].ptr());
    LOGINFO("checkCQStatusOnConnect = %d ", myStatusCq->getCqsConnectedCount());
    ASSERT(myStatusCq->getCqsConnectedCount() == 1,
           "incorrect number of CqStatus Connected count.");
    LOG("createCQ complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, createCQ_Pool)
  {
    PoolPtr pool = PoolManager::find("__TEST_POOL1__");
    QueryServicePtr qs = pool->getQueryService();
    CqAttributesFactory cqFac;
    CqStatusListenerPtr cqLstner(new MyCqStatusListener(100));
    cqFac.addCqListener(cqLstner);
    CqAttributesPtr cqAttr = cqFac.create();

    CqQueryPtr cq =
        qs->newCq(const_cast<char*>(cqName), cqQueryStatusString, cqAttr);
    cq->execute();
    SLEEP(20000);

    cqAttr = cq->getCqAttributes();
    VectorOfCqListener vl;
    cqAttr->getCqListeners(vl);
    MyCqStatusListener* myStatusCq =
        dynamic_cast<MyCqStatusListener*>(vl[0].ptr());
    LOGINFO("checkCQStatusOnConnect = %d ", myStatusCq->getCqsConnectedCount());
    ASSERT(myStatusCq->getCqsConnectedCount() == 1,
           "incorrect number of CqStatus Connected count.");

    PoolPtr pool2 = PoolManager::find("__TEST_POOL2__");
    QueryServicePtr qs2 = pool2->getQueryService();
    CqAttributesFactory cqFac1;
    CqStatusListenerPtr cqLstner1(new MyCqStatusListener(101));
    cqFac1.addCqListener(cqLstner1);
    CqAttributesPtr cqAttr1 = cqFac1.create();
    CqQueryPtr cq2 =
        qs2->newCq(const_cast<char*>(cqName1), cqQueryStatusString1, cqAttr1);
    cq2->execute();
    SLEEP(20000);

    cqAttr1 = cq2->getCqAttributes();
    VectorOfCqListener vl2;
    cqAttr1->getCqListeners(vl2);
    MyCqStatusListener* myStatusCq2 =
        dynamic_cast<MyCqStatusListener*>(vl2[0].ptr());
    LOGINFO("checkCQStatusOnConnect = %d ",
            myStatusCq2->getCqsConnectedCount());
    ASSERT(myStatusCq2->getCqsConnectedCount() == 1,
           "incorrect number of CqStatus Connected count.");

    RegionPtr regPtr0 = getHelper()->getRegion(regionName);
    RegionPtr regPtr1 = getHelper()->getRegion(regionName1);
    CacheablePtr val = NULLPTR;
    char KeyStr[256] = {0};
    char valStr[256] = {0};
    for (int i = 1; i <= 5; i++) {
      ACE_OS::snprintf(KeyStr, 256, "Key-%d ", i);
      ACE_OS::snprintf(valStr, 256, "val-%d ", i);
      CacheableKeyPtr keyport = CacheableKey::create(KeyStr);
      CacheablePtr valport = CacheableString::create(valStr);
      regPtr0->put(keyport, valport);
      regPtr1->put(keyport, valport);
      SLEEP(10 * 1000);  // sleep a while to allow server query to complete
    }
    LOGINFO("putEntries complete");

    LOGINFO("checkCQStatusOnPutEvent = %d ", myStatusCq->getNumInserts());
    ASSERT(myStatusCq->getNumInserts() == 5,
           "incorrect number of CqStatus Updates count.");

    LOGINFO("checkCQStatusOnPutEvent = %d ", myStatusCq2->getNumInserts());
    ASSERT(myStatusCq2->getNumInserts() == 5,
           "incorrect number of CqStatus Updates count.");

    LOG("createCQ_Pool complete.");
  }
END_TASK_DEFINITION

void executeCq(const char* poolName, const char* name) {
  PoolPtr pool = PoolManager::find(poolName);
  QueryServicePtr qs;
  if (pool != NULLPTR) {
    qs = pool->getQueryService();
  }
  CqQueryPtr cq = qs->getCq(const_cast<char*>(name));
  cq->execute();
  SLEEP(20000);
  LOG("executeCq complete");
}

DUNIT_TASK_DEFINITION(CLIENT1, executeCQ)
  {
    executeCq(regionName, cqName);
    LOG("executeCQ complete.");
  }
END_TASK_DEFINITION

void checkCQStatusOnConnect(const char* poolName, const char* name,
                            int connect) {
  PoolPtr pool = PoolManager::find(poolName);
  QueryServicePtr qs;
  if (pool != NULLPTR) {
    qs = pool->getQueryService();
  }
  CqQueryPtr cq = qs->getCq(const_cast<char*>(name));
  CqAttributesPtr cqAttr = cq->getCqAttributes();
  VectorOfCqListener vl;
  cqAttr->getCqListeners(vl);
  MyCqStatusListener* myStatusCq =
      dynamic_cast<MyCqStatusListener*>(vl[0].ptr());
  LOGINFO("checkCQStatusOnConnect = %d ", myStatusCq->getCqsConnectedCount());
  ASSERT(myStatusCq->getCqsConnectedCount() == connect,
         "incorrect number of CqStatus Connected count.");
}

DUNIT_TASK_DEFINITION(CLIENT1, checkCQStatusOnConnect)
  {
    SLEEP(20000);
    checkCQStatusOnConnect(regionName, const_cast<char*>(cqName), 1);
    LOG("checkCQStatusOnConnect complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, checkCQStatusOnConnect2)
  {
    SLEEP(20000);
    checkCQStatusOnConnect(regionName, const_cast<char*>(cqName), 2);
    LOG("checkCQStatusOnConnect2 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, checkCQStatusOnConnect_Pool)
  {
    SLEEP(20000);
    checkCQStatusOnConnect("__TEST_POOL1__", const_cast<char*>(cqName), 1);
    checkCQStatusOnConnect("__TEST_POOL2__", const_cast<char*>(cqName1), 1);

    LOG("checkCQStatusOnConnect_Pool complete.");
  }
END_TASK_DEFINITION

void checkCQStatusOnDisConnect(const char* poolName, const char* cqName,
                               int disconnect) {
  PoolPtr pool = PoolManager::find(poolName);
  QueryServicePtr qs;
  if (pool != NULLPTR) {
    qs = pool->getQueryService();
  }
  CqQueryPtr cq = qs->getCq(const_cast<char*>(cqName));
  CqAttributesPtr cqAttr = cq->getCqAttributes();
  VectorOfCqListener vl;
  cqAttr->getCqListeners(vl);
  MyCqStatusListener* myStatusCq =
      dynamic_cast<MyCqStatusListener*>(vl[0].ptr());
  LOGINFO("checkCQStatusOnDisConnect = %d ",
          myStatusCq->getCqsDisConnectedCount());
  ASSERT(myStatusCq->getCqsDisConnectedCount() == disconnect,
         "incorrect number of CqStatus Disconnected count.");
}

DUNIT_TASK_DEFINITION(CLIENT1, checkCQStatusOnDisConnect0)
  {
    SLEEP(20000);
    checkCQStatusOnDisConnect(regionName, const_cast<char*>(cqName), 0);
    LOG("checkCQStatusOnDisConnect0 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, checkCQStatusOnDisConnect_Pool)
  {
    SLEEP(20000);
    checkCQStatusOnDisConnect("__TEST_POOL1__", const_cast<char*>(cqName), 1);
    checkCQStatusOnDisConnect("__TEST_POOL2__", const_cast<char*>(cqName1), 1);
    LOG("checkCQStatusOnDisConnect_Pool complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, checkCQStatusOnDisConnect1)
  {
    SLEEP(20000);
    checkCQStatusOnDisConnect(regionName, const_cast<char*>(cqName), 1);
    LOG("checkCQStatusOnDisConnect1 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, checkCQStatusOnDisConnect2)
  {
    SLEEP(20000);
    checkCQStatusOnDisConnect(regionName, const_cast<char*>(cqName), 2);
    LOG("checkCQStatusOnDisConnect2 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putEntries)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionName);
    RegionPtr regPtr1 = getHelper()->getRegion(regionName1);
    CacheablePtr val = NULLPTR;
    char KeyStr[256] = {0};
    char valStr[256] = {0};
    for (int i = 1; i <= 5; i++) {
      ACE_OS::snprintf(KeyStr, 256, "Key-%d ", i);
      ACE_OS::snprintf(valStr, 256, "val-%d ", i);
      CacheableKeyPtr keyport = CacheableKey::create(KeyStr);
      CacheablePtr valport = CacheableString::create(valStr);
      regPtr0->put(keyport, valport);
      regPtr1->put(keyport, valport);
      SLEEP(10 * 1000);  // sleep a while to allow server query to complete
    }
    LOGINFO("putEntries complete");
  }
END_TASK_DEFINITION

void checkCQStatusOnPutEvent(const char* poolName, const char* cqName,
                             int count) {
  PoolPtr pool = PoolManager::find(poolName);
  QueryServicePtr qs;
  if (pool != NULLPTR) {
    qs = pool->getQueryService();
  }
  CqQueryPtr cq = qs->getCq(const_cast<char*>(cqName));
  CqAttributesPtr cqAttr = cq->getCqAttributes();
  VectorOfCqListener vl;
  cqAttr->getCqListeners(vl);
  MyCqStatusListener* myStatusCq =
      dynamic_cast<MyCqStatusListener*>(vl[0].ptr());
  LOGINFO("checkCQStatusOnPutEvent = %d ", myStatusCq->getNumInserts());
  ASSERT(myStatusCq->getNumInserts() == count,
         "incorrect number of CqStatus Updates count.");
}

DUNIT_TASK_DEFINITION(CLIENT1, checkCQStatusOnPutEvent)
  {
    checkCQStatusOnPutEvent(regionName, cqName, 5);
    LOG("checkCQStatusOnPutEvent complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, checkCQStatusOnPutEvent_Pool)
  {
    checkCQStatusOnPutEvent("__TEST_POOL1__", cqName, 5);
    checkCQStatusOnPutEvent("__TEST_POOL2__", cqName1, 5);
    LOG("checkCQStatusOnPutEvent_Pool complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ProcessCQ)
  {
    SLEEP(10000);
    // Create CqAttributes and Install Listener
    PoolPtr pool = PoolManager::find(regionName);
    QueryServicePtr qs = pool->getQueryService();
    CqAttributesFactory cqFac;
    CqListenerPtr cqLstner(new MyCqListener(1));
    CqStatusListenerPtr cqStatusLstner(new MyCqStatusListener(100));
    cqFac.addCqListener(cqLstner);
    cqFac.addCqListener(cqStatusLstner);
    CqAttributesPtr cqAttr = cqFac.create();

    CqQueryPtr cq =
        qs->newCq(const_cast<char*>(cqName), cqQueryStatusString, cqAttr);
    cq->execute();
    SLEEP(20000);
    LOG("ProcessCQ Query executed.");

    RegionPtr regPtr0 = getHelper()->getRegion(regionName);
    CacheablePtr val = NULLPTR;
    char KeyStr[256] = {0};
    char valStr[256] = {0};
    for (int i = 1; i <= 5; i++) {
      ACE_OS::snprintf(KeyStr, 256, "Key-%d ", i);
      ACE_OS::snprintf(valStr, 256, "val-%d ", i);
      CacheableKeyPtr keyport = CacheableKey::create(KeyStr);
      CacheablePtr valport = CacheableString::create(valStr);
      regPtr0->put(keyport, valport);
      SLEEP(10 * 1000);  // sleep a while to allow server query to complete
    }
    LOGINFO("putEntries complete");

    cqAttr = cq->getCqAttributes();
    VectorOfCqListener vl;
    cqAttr->getCqListeners(vl);
    ASSERT(vl.size() == 2, "incorrect number of CqListeners count.");
    MyCqStatusListener* myStatusCq =
        dynamic_cast<MyCqStatusListener*>(vl[1].ptr());
    LOGINFO("No of insert events = %d ", myStatusCq->getNumInserts());
    LOGINFO("No of OnCqConnected events = %d ",
            myStatusCq->getCqsConnectedCount());
    ASSERT(myStatusCq->getNumInserts() == 5,
           "incorrect number of CqStatus Updates count.");
    ASSERT(myStatusCq->getCqsConnectedCount() == 1,
           "incorrect number of CqStatus Connected count.");

    MyCqListener* myCq = dynamic_cast<MyCqListener*>(vl[0].ptr());
    LOGINFO("No of insert events = %d ", myCq->getNumInserts());
    ASSERT(myCq->getNumInserts() == 5,
           "incorrect number of CqStatus Updates count.");

    CqAttributesMutatorPtr cqAttrMtor = cq->getCqAttributesMutator();
    CqListenerPtr ptr = vl[0];
    cqAttrMtor->removeCqListener(ptr);
    cqAttr->getCqListeners(vl);
    LOGINFO("number of listeners = %d", vl.size());

    ASSERT(vl.size() == 1, "incorrect number of listeners");

    cqAttrMtor->removeCqListener(vl[0]);
    LOGINFO("removeCqListener again");
    cqAttr->getCqListeners(vl);
    LOGINFO("number of listeners = %d", vl.size());

    ASSERT(vl.size() == 0, "incorrect number of listeners");

    VectorOfCqListener v2;
    v2.push_back(cqStatusLstner);
    v2.push_back(cqLstner);
    cqAttrMtor->setCqListeners(v2);
    LOG("ProcessCQ setCqListeneres done.");

    cqAttr = cq->getCqAttributes();
    VectorOfCqListener vl3;
    cqAttr->getCqListeners(vl3);
    ASSERT(vl3.size() == 2, "incorrect number of CqListeners count.");

    MyCqStatusListener* myStatusCq2 =
        dynamic_cast<MyCqStatusListener*>(vl3[0].ptr());
    myStatusCq2->clear();

    for (int i = 1; i <= 5; i++) {
      ACE_OS::snprintf(KeyStr, 256, "Key-%d ", i);
      ACE_OS::snprintf(valStr, 256, "val-%d ", i);
      CacheableKeyPtr keyport = CacheableKey::create(KeyStr);
      CacheablePtr valport = CacheableString::create(valStr);
      regPtr0->put(keyport, valport);
      SLEEP(10 * 1000);  // sleep a while to allow server query to complete
    }
    LOGINFO("putEntries complete again");

    VectorOfCqListener vl21;
    vl21.push_back(cqStatusLstner);
    vl21.push_back(cqLstner);
    cqFac.initCqListeners(vl21);
    LOGINFO("initCqListeners complete.");

    cqAttr = cq->getCqAttributes();
    VectorOfCqListener vl2;
    cqAttr->getCqListeners(vl2);
    ASSERT(vl2.size() == 2, "incorrect number of CqListeners count.");
    myStatusCq2 = dynamic_cast<MyCqStatusListener*>(vl2[0].ptr());
    LOGINFO("No of insert events = %d ", myStatusCq2->getNumUpdates());
    LOGINFO("No of OnCqConnected events = %d ",
            myStatusCq2->getCqsConnectedCount());
    ASSERT(myStatusCq2->getNumUpdates() == 5,
           "incorrect number of CqStatus Updates count.");
    ASSERT(myStatusCq2->getCqsConnectedCount() == 0,
           "incorrect number of CqStatus Connected count.");

    MyCqListener* myCq2 = dynamic_cast<MyCqListener*>(vl2[1].ptr());
    LOGINFO("No of insert events = %d ", myCq2->getNumInserts());
    ASSERT(myCq2->getNumUpdates() == 5,
           "incorrect number of CqStatus Updates count.");

    LOG("ProcessCQ complete.");
  }
END_TASK_DEFINITION

void doThinClientCqStatus() {
  CALL_TASK(CreateLocator);
  CALL_TASK(CreateServer1_Locator);

  CALL_TASK(initCqStatusClientLoc);

  CALL_TASK(createCQ);
  // CALL_TASK(executeCQ);
  // CALL_TASK(checkCQStatusOnConnect);
  CALL_TASK(putEntries);
  CALL_TASK(checkCQStatusOnPutEvent);

  CALL_TASK(CreateServer2_Locator);

  CALL_TASK(CloseServer1);
  CALL_TASK(checkCQStatusOnDisConnect0);
  CALL_TASK(CloseServer2);
  CALL_TASK(checkCQStatusOnDisConnect1);

  CALL_TASK(CreateServer1_Locator);

  CALL_TASK(checkCQStatusOnConnect2);
  CALL_TASK(CloseServer1);

  CALL_TASK(checkCQStatusOnDisConnect2);
  CALL_TASK(CloseCache1);

  CALL_TASK(CloseLocator);
}

void doThinClientCqStatus2() {
  CALL_TASK(CreateLocator);
  CALL_TASK(CreateServer_servergrop);
  CALL_TASK(CreateServer_servergrop2);
  CALL_TASK(StepOne_Pooled_Locator);
  CALL_TASK(createCQ_Pool);
  CALL_TASK(CloseServer1);
  CALL_TASK(CloseServer2);
  CALL_TASK(checkCQStatusOnDisConnect_Pool);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseLocator);
}

void doThinClientCqStatus3() {
  CALL_TASK(CreateLocator);
  CALL_TASK(CreateServer1_Locator);

  CALL_TASK(initCqStatusClientLoc);

  CALL_TASK(ProcessCQ);
  CALL_TASK(CloseServer1);
  CALL_TASK(checkCQStatusOnDisConnect1);
  CALL_TASK(CloseCache1);

  CALL_TASK(CloseLocator);
}

void setPortfolioPdxTypeC1() { CALL_TASK(SetPortfolioTypeToPdxC1) }

void UnsetPortfolioTypeC1() { CALL_TASK(UnsetPortfolioTypeToPdxC1) }
//
void setPortfolioPdxTypeC2() { CALL_TASK(SetPortfolioTypeToPdxC2) }

void UnsetPortfolioTypeC2() { CALL_TASK(UnsetPortfolioTypeToPdxC2) }

DUNIT_MAIN
  {
    UnsetPortfolioTypeC1();
    UnsetPortfolioTypeC2();
    for (int runIdx = 1; runIdx <= 2; ++runIdx) {
      doThinClientCq();

      setPortfolioPdxTypeC1();
      setPortfolioPdxTypeC2();
    }

    { doThinClientCqStatus3(); }

    { doThinClientCqStatus(); }

    { doThinClientCqStatus2(); }
  }
END_MAIN
