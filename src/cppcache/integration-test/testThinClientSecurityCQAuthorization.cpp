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
#include <gfcpp/CqServiceStatistics.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <string>

#define ROOT_NAME "TestThinClientCqAuthorization"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

#include "QueryStrings.hpp"
#include "QueryHelper.hpp"

#include <gfcpp/Query.hpp>
#include <gfcpp/QueryService.hpp>

#include "ThinClientCQ.hpp"

using namespace test;
using namespace testData;

#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"
#include "ThinClientHelper.hpp"
#include "ace/Process.h"

//#include "ThinClientSecurity.hpp"

using namespace gemfire::testframework::security;
using namespace gemfire;

const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
CredentialGeneratorPtr credentialGeneratorHandler;
#define CLIENT1 s1p1
#define SERVER1 s2p1
#define CLIENT2 s1p2
#define LOCATORSERVER s2p2

#define MAX_LISTNER 8

const char* cqNames[MAX_LISTNER] = {"MyCq_0", "MyCq_1", "MyCq_2", "MyCq_3",
                                    "MyCq_4", "MyCq_5", "MyCq_6", "MyCq_7"};

const char* queryStrings[MAX_LISTNER] = {
    "select * from /Portfolios p where p.ID < 4",
    "select * from /Portfolios p where p.ID < 2",
    "select * from /Portfolios p where p.ID != 2",
    "select * from /Portfolios p where p.ID != 3",
    "select * from /Portfolios p where p.ID != 4",
    "select * from /Portfolios p where p.ID != 5",
    "select * from /Portfolios p where p.ID != 6",
    "select * from /Portfolios p where p.ID != 7"};

const char* regionNamesCq[] = {"Portfolios", "Positions", "Portfolios2",
                               "Portfolios3"};

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

std::string getXmlPath() {
  char xmlPath[1000] = {'\0'};
  const char* path = ACE_OS::getenv("TESTSRC");
  ASSERT(path != NULL,
         "Environment variable TESTSRC for test source directory is not set.");
  strncpy(xmlPath, path, strlen(path) - strlen("cppcache"));
  strcat(xmlPath, "xml/Security/");
  return std::string(xmlPath);
}

void initCredentialGenerator() {
  credentialGeneratorHandler = CredentialGenerator::create("DUMMY3");

  if (credentialGeneratorHandler == NULLPTR) {
    FAIL("credentialGeneratorHandler is NULL");
  }
}

PropertiesPtr userCreds;
void initClientCq(const bool isthinClient) {
  try {
    Serializable::registerType(Position::createDeserializable);
    Serializable::registerType(Portfolio::createDeserializable);
  } catch (const IllegalStateException&) {
    // ignore exception
  }

  userCreds = Properties::create();
  PropertiesPtr config = Properties::create();
  credentialGeneratorHandler->getAuthInit(config);
  credentialGeneratorHandler->getValidCredentials(config);

  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

DUNIT_TASK_DEFINITION(CLIENT1, CreateServer1_Locator)
  {
    initCredentialGenerator();
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator:authorizer:authorizerPP", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      CacheHelper::initServer(
          1, "remotequery.xml", locatorsG,
          const_cast<char*>(cmdServerAuthenticator.c_str()));
      LOG("Server1 started");
    }
  }
END_TASK_DEFINITION

void stepOne() {
  initClientCq(true);
  createRegionForCQ(regionNamesCq[0], USE_ACK, true);
  RegionPtr regptr = getHelper()->getRegion(regionNamesCq[0]);
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  RegionPtr subregPtr = regptr->createSubregion(regionNamesCq[1], lattribPtr);

  LOG("StepOne complete.");
}

void stepOne2(bool pool = false, bool locator = false) {
  LOG("StepOne2 complete. 1");
  initClientCq(true);
  LOG("StepOne2 complete. 2");
  createRegionForCQ(regionNamesCq[0], USE_ACK, true);
  LOG("StepOne2 complete. 3");
  RegionPtr regptr = getHelper()->getRegion(regionNamesCq[0]);
  LOG("StepOne2 complete. 4");
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  LOG("StepOne2 complete. 5");
  RegionPtr subregPtr = regptr->createSubregion(regionNamesCq[1], lattribPtr);

  LOG("StepOne2 complete.");
}

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_PoolLocator)
  { stepOne(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepOne2_PoolLocator)
  {
    initCredentialGenerator();
    stepOne2();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepTwo)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);

    QueryHelper* qh = &QueryHelper::getHelper();

    qh->populatePortfolioData(regPtr0, 2, 1, 1);
    qh->populatePositionData(subregPtr0, 2, 1);

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

    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo2)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);

    QueryHelper* qh = &QueryHelper::getHelper();

    qh->populatePortfolioData(regPtr0, 3, 2, 1);
    qh->populatePositionData(subregPtr0, 3, 2);
    for (int i = 1; i < 3; i++) {
      CacheablePtr port(new Portfolio(i, 2));

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
    ASSERT(serviceStats->numCqsActive() == 5, "active count incorrect!");
    ASSERT(serviceStats->numCqsCreated() == 8, "created count incorrect!");
    ASSERT(serviceStats->numCqsClosed() == 1, "closed count incorrect!");
    ASSERT(serviceStats->numCqsStopped() == 2, "stopped count incorrect!");
    ASSERT(serviceStats->numCqsOnClient() == 7, "cq count incorrect!");
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
    ASSERT(serviceStats->numCqsCreated() == 8, "created count incorrect!");
    ASSERT(serviceStats->numCqsClosed() == 1, "closed count incorrect!");
    ASSERT(serviceStats->numCqsStopped() == 7, "stopped count incorrect!");
    ASSERT(serviceStats->numCqsOnClient() == 7, "cq count incorrect!");
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
    ASSERT(serviceStats->numCqsCreated() == 8, "created count incorrect!");
    ASSERT(serviceStats->numCqsClosed() == 8, "closed count incorrect!");
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

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateLocator)
  {
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CloseLocator)
  {
    CacheHelper::closeLocator(1);
    LOG("Locator1 stopped");
  }
END_TASK_DEFINITION

void doThinClientCq() {
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

DUNIT_MAIN
  { doThinClientCq(); }
END_MAIN
