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

#define ROOT_NAME "testThinClientSecurityCQAuthorizationMU"
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
    "select * from /Portfolios p where p.ID < 1",
    "select * from /Portfolios p where p.ID < 2",
    "select * from /Portfolios p where p.ID = 2",
    "select * from /Portfolios p where p.ID >= 3",
    "select * from /Portfolios p where p.ID = 4",
    "select * from /Portfolios p where p.ID = 5",
    "select * from /Portfolios p where p.ID = 6",
    "select * from /Portfolios p where p.ID = 7"};

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
    printf(" in cqEvent.getQueryOperation() %d id = %d\n",
           cqEvent.getQueryOperation(), m_id);
    printf(" in update key = %s \n",
           (dynamic_cast<CacheableString*>(cqEvent.getKey().ptr()))->asChar());
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
    printf(" in create = %d, update = %d , delete = %d ", m_numInserts,
           m_numUpdates, m_numDeletes);
  }

  void onEvent(const CqEvent& cqe) {
    LOG("MyCqListener::OnEvent called");
    updateCount(cqe);
  }
  void onError(const CqEvent& cqe) {
    updateCount(cqe);
    LOG("MyCqListener::OnError called");
  }
  void close() { LOG("MyCqListener::close called"); }
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
  // credentialGeneratorHandler->getAuthInit(config);
  credentialGeneratorHandler->getValidCredentials(userCreds);

  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

DUNIT_TASK_DEFINITION(CLIENT1, CreateServer1)
  {
    initCredentialGenerator();
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator:authorizer:authorizerPP", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      CacheHelper::initServer(
          1, "remotequery.xml", NULL,
          const_cast<char*>(cmdServerAuthenticator.c_str()));
      LOG("Server1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateServer2)
  {
    std::string cmdServerAuthenticator;

    if (isLocalServer) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams(
          "authenticator:authorizer:authorizerPP", getXmlPath());
      printf("string %s", cmdServerAuthenticator.c_str());
      CacheHelper::initServer(
          2, "remotequery2.xml", NULL,
          const_cast<char*>(cmdServerAuthenticator.c_str()));
      LOG("Server2 started");
    }
    SLEEP(20000);
  }
END_TASK_DEFINITION

void stepOne(bool pool = false, bool locator = false) {
  LOG("StepOne1 complete. 1");
  initClientCq(true);
  LOG("StepOne1 complete. 2");
  createRegionForCQMU(regionNamesCq[0], USE_ACK, true, 0, NULLPTR, false, true);
  LOG("StepOne1 complete. 3");
  RegionPtr regptr = getHelper()->getRegion(regionNamesCq[0]);
  LOG("StepOne1 complete. 4");
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  LOG("StepOne1 complete. 5");
  RegionPtr subregPtr = regptr->createSubregion(regionNamesCq[1], lattribPtr);

  LOG("StepOne complete.");
}

void stepOne2(bool pool = false, bool locator = false) {
  LOG("StepOne2 complete. 1");
  initClientCq(true);
  LOG("StepOne2 complete. 2");
  createRegionForCQMU(regionNamesCq[0], USE_ACK, true, 0, NULLPTR, false, true);
  LOG("StepOne2 complete. 3");
  RegionPtr regptr = getHelper()->getRegion(regionNamesCq[0]);
  LOG("StepOne2 complete. 4");
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  LOG("StepOne2 complete. 5");
  RegionPtr subregPtr = regptr->createSubregion(regionNamesCq[1], lattribPtr);

  LOG("StepOne2 complete.");
}

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_PoolEP)
  { stepOne(true, false); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepOne2_PoolEP)
  {
    initCredentialGenerator();
    stepOne2(true, false);
  }
END_TASK_DEFINITION

PoolPtr getPool(const char* name) { return PoolManager::find(name); }

RegionServicePtr getVirtualCache(PropertiesPtr creds, const char* name) {
  // PoolPtr pool = getPool(name);
  // return pool->createSecureUserCache(creds);
  return getHelper()->getCache()->createAuthenticatedView(creds, name);
}

static RegionServicePtr userCache;
static QueryServicePtr userQueryService;

DUNIT_TASK_DEFINITION(CLIENT1, StepTwo)
  {
    PropertiesPtr creds = Properties::create();
    char tmp[25] = {'\0'};
    sprintf(tmp, "user%d", 4);

    creds->insert("security-username", tmp);
    creds->insert("security-password", tmp);

    userCache = getVirtualCache(creds, regionNamesCq[0]);
    RegionPtr regPtr0 = userCache->getRegion(regionNamesCq[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);

    // QueryHelper * qh = &QueryHelper::getHelper();

    // qh->populatePortfolioData(regPtr0  , 2, 1, 1);
    // qh->populatePositionData(subregPtr0, 2, 1);

    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    uint8_t i = 0;
    // QueryHelper * qh = &QueryHelper::getHelper();
    // userCache = getVirtualCache(userCreds, regionNamesCq[0]);

    userQueryService = userCache->getQueryService();
    QueryServicePtr qs;

    qs = userQueryService;

    try {
      for (i = 0; i < MAX_LISTNER; i++) {
        CqListenerPtr cqLstner(new MyCqListener(i));
        CqAttributesFactory cqFac;
        cqFac.addCqListener(cqLstner);
        CqAttributesPtr cqAttr = cqFac.create();

        CqQueryPtr qry = qs->newCq(cqNames[i], queryStrings[i], cqAttr);
        qry->execute();
      }

      LOG("EXECUTE 1 START");

      // qs->executeCqs();

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
    PropertiesPtr creds = Properties::create();
    char tmp[25] = {'\0'};
    sprintf(tmp, "user%d", 3);

    creds->insert("security-username", tmp);
    creds->insert("security-password", tmp);

    userCache = getVirtualCache(creds, regionNamesCq[0]);
    RegionPtr regPtr0 = userCache->getRegion(regionNamesCq[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);

    QueryHelper* qh = &QueryHelper::getHelper();

    qh->populatePortfolioData(regPtr0, 3, 2, 1);
    qh->populatePositionData(subregPtr0, 3, 2);
    for (int i = 1; i <= 4; i++) {
      CacheablePtr port(new Portfolio(i, 2));

      char tmp[25] = {'\0'};
      sprintf(tmp, "port1-%d", i);
      CacheableKeyPtr keyport = CacheableKey::create(tmp);
      regPtr0->put(keyport, port);
      SLEEP(10);  // sleep a while to allow server query to complete
    }

    LOG("StepTwo2 complete. Sleeping .25 min for server query to complete...");
    SLEEP(15000);  // sleep .25 min to allow server query to complete
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFour)
  {
    // CachePtr userCache = getVirtualCache(userCreds, regionNamesCq[0]);
    QueryServicePtr qs;

    qs = userQueryService;

    char buf[1024];

    uint8_t i = 0;
    int j ATTR_UNUSED = 0;
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
      // CqStatisticsPtr cqStats = cqy->getStatistics();
    }

    // if key port1-4 then only query 3 and 4 will satisfied
    CqQueryPtr cqy = qs->getCq(cqNames[3]);
    CqAttributesPtr cqAttr = cqy->getCqAttributes();
    VectorOfCqListener vl;
    cqAttr->getCqListeners(vl);

    MyCqListener* cqListener_3 = static_cast<MyCqListener*>(vl[0].ptr());
    printf(" cqListener_3 should have one create event = %d \n",
           cqListener_3->getNumInserts());
    ASSERT(cqListener_3->getNumInserts() == 1,
           "incorrect number of events got listener 3");

    vl.clear();

    cqy = qs->getCq(cqNames[4]);
    cqAttr = cqy->getCqAttributes();
    cqAttr->getCqListeners(vl);

    MyCqListener* cqListener_4 = static_cast<MyCqListener*>(vl[0].ptr());
    printf(" cqListener_4 should have one create event = %d \n",
           cqListener_4->getNumInserts());
    ASSERT(cqListener_4->getNumInserts() == 1,
           "incorrect number of events got listener 4");

    /*for(i=0; i < MAX_LISTNER; i++)
    {
      sprintf(buf, "get info for cq[%s]:", cqNames[i]);
      LOG(buf);
      CqQueryPtr cqy = qs->getCq(cqNames[i]);
      cqy->stop();
      cqy->close();
     // CqStatisticsPtr cqStats = cqy->getStatistics();
    }*/

    // userCache->close();
    LOG("StepFour complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFour2)
  {
    // CachePtr userCache = getVirtualCache(userCreds, regionNamesCq[0]);
    QueryServicePtr qs;

    qs = userQueryService;

    char buf[1024];

    uint8_t i = 0;
    int j ATTR_UNUSED = 0;
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
      // CqStatisticsPtr cqStats = cqy->getStatistics();
    }

    // if key port1-4 then only query 3 and 4 will satisfied
    CqQueryPtr cqy = qs->getCq(cqNames[3]);
    CqAttributesPtr cqAttr = cqy->getCqAttributes();
    VectorOfCqListener vl;
    cqAttr->getCqListeners(vl);

    MyCqListener* cqListener_3 = static_cast<MyCqListener*>(vl[0].ptr());
    printf(" cqListener_3 should have one update event = %d \n",
           cqListener_3->getNumUpdates());
    ASSERT(cqListener_3->getNumUpdates() == 1,
           "incorrect number of events got listener 3");

    vl.clear();

    cqy = qs->getCq(cqNames[4]);
    cqAttr = cqy->getCqAttributes();
    cqAttr->getCqListeners(vl);

    MyCqListener* cqListener_4 = static_cast<MyCqListener*>(vl[0].ptr());
    printf(" cqListener_4 should have one update event = %d \n",
           cqListener_4->getNumUpdates());
    ASSERT(cqListener_4->getNumUpdates() == 1,
           "incorrect number of events got listener 4");

    /*for(i=0; i < MAX_LISTNER; i++)
    {
      sprintf(buf, "get info for cq[%s]:", cqNames[i]);
      LOG(buf);
      CqQueryPtr cqy = qs->getCq(cqNames[i]);
      cqy->stop();
      cqy->close();
     // CqStatisticsPtr cqStats = cqy->getStatistics();
    }*/

    userCache->close();
    LOG("StepFour2 complete.");
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
    SLEEP(5000);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer2)
  {
    LOG("closing Server2...");
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK_DEFINITION

void doThinClientCq() {

//  CALL_TASK(CreateLocator);
//  CALL_TASK(CreateServer1_Locator);
//
//  CALL_TASK(StepOne_PoolLocator);
//  CALL_TASK(StepOne2_PoolLocator);

  CALL_TASK(StepTwo);
  CALL_TASK(StepThree);
  CALL_TASK(StepTwo2);
  CALL_TASK(StepFour);  // validates listener events
  CALL_TASK(CreateServer2);
  CALL_TASK(CloseServer1);
  CALL_TASK(StepTwo2);  // again put data
  CALL_TASK(StepFour2);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer2);

//  CALL_TASK(CloseLocator);
}

DUNIT_MAIN
  {
    throw "not supported, needs locators";
    //doThinClientCq();
  }
END_MAIN
