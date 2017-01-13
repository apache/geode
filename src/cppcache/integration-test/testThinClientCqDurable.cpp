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
#include <gfcpp/CqAttributesFactory.hpp>
#include <gfcpp/CqAttributes.hpp>
#include <gfcpp/CqListener.hpp>
#include <gfcpp/CqQuery.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <string>

#define ROOT_SCOPE DISTRIBUTED_ACK

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

const char* durableIds[] = {"DurableId1", "DurableId2"};

const char* cqName = "MyCq";
const char* durableCQNamesClient1[] = {
    "durableCQ1Client1", "durableCQ2Client1", "durableCQ3Client1",
    "durableCQ4Client1", "durableCQ5Client1", "durableCQ6Client1",
    "durableCQ7Client1", "durableCQ8Client1"};

const char* durableCQNamesClient2[] = {
    "durableCQ1Client2", "durableCQ2Client2", "durableCQ3Client2",
    "durableCQ4Client2", "durableCQ5Client2", "durableCQ6Client2",
    "durableCQ7Client2", "durableCQ8Client2"};

static bool m_isPdx = false;

void initClientWithId(int ClientIdx, bool typeRegistered = false) {
  if (typeRegistered == false) {
    try {
      Serializable::registerType(Position::createDeserializable);
      Serializable::registerType(Portfolio::createDeserializable);

      Serializable::registerPdxType(PositionPdx::createDeserializable);
      Serializable::registerPdxType(PortfolioPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
  }
  PropertiesPtr pp = Properties::create();
  pp->insert("durable-client-id", durableIds[ClientIdx]);
  pp->insert("durable-timeout", 60);
  pp->insert("notify-ack-interval", 1);

  initClient(true, pp);
}

class MyCqListener1 : public CqListener {
 public:
  static int m_cntEvents;
  void onEvent(const CqEvent& cqe) {
    m_cntEvents++;
    char* opStr = (char*)"Default";
    CacheableInt32Ptr value(dynCast<CacheableInt32Ptr>(cqe.getNewValue()));
    CacheableInt32Ptr key(dynCast<CacheableInt32Ptr>(cqe.getKey()));
    switch (cqe.getQueryOperation()) {
      case CqOperation::OP_TYPE_CREATE: {
        opStr = (char*)"CREATE";
        break;
      }
      case CqOperation::OP_TYPE_UPDATE: {
        opStr = (char*)"UPDATE";
        break;
      }
      case CqOperation::OP_TYPE_DESTROY: {
        opStr = (char*)"UPDATE";
        break;
      }
      default:
        break;
    }
    LOGINFO("MyCqListener1::OnEvent called with %s, key[%s], value=(%s)", opStr,
            key->toString()->asChar(), value->toString()->asChar());
  }

  void onError(const CqEvent& cqe) { LOGINFO("MyCqListener1::OnError called"); }

  void close() { LOGINFO("MyCqListener1::close called"); }
};
int MyCqListener1::m_cntEvents = 0;

const char* regionNamesCq[] = {"Portfolios", "Positions", "Portfolios2",
                               "Portfolios3"};

int onEventCount = 0;
int onErrorCount = 0;
int onEventCountBefore = 0;
class MyCqListener : public CqListener {
  void onEvent(const CqEvent& cqe) {
    //    LOG("MyCqListener::OnEvent called");
    onEventCount++;
  }
  void onError(const CqEvent& cqe) {
    //   LOG("MyCqListener::OnError called");
    onErrorCount++;
  }
  void close() { LOG("MyCqListener::close called"); }
};

DUNIT_TASK_DEFINITION(SERVER1, CreateLocator)
  {
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

void createServer(bool locator = false) {
  LOG("Starting SERVER1...");
  if (isLocalServer) {
    CacheHelper::initServer(1, "remotequery.xml", locator ? locatorsG : NULL);
  }
  LOG("SERVER1 started");
}

void createServer_XML() {
  LOG("Starting SERVER...");
  if (isLocalServer) {
    CacheHelper::initServer(1, "serverDurableClient.xml", NULL);
  }
  LOG("SERVER started");
}

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  { createServer(false); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer)
  { createServer_XML(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_Locator)
  { createServer(true); }
END_TASK_DEFINITION

void stepOne() {
  initClientWithId(0);
  createRegionForCQ(regionNamesCq[0], USE_ACK, true);
  RegionPtr regptr = getHelper()->getRegion(regionNamesCq[0]);
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  RegionPtr subregPtr = regptr->createSubregion(regionNamesCq[1], lattribPtr);

  LOG("StepOne complete.");
}

void RunDurableCqClient() {
  // Create durable client's properties using api.
  PropertiesPtr pp = Properties::create();
  pp->insert("durable-client-id", "DurableClientId");
  pp->insert("durable-timeout", 3600);

  // Create a GemFire Cache Programmatically.
  CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory(pp);
  CachePtr cachePtr = cacheFactory->setSubscriptionEnabled(true)
                          ->setSubscriptionAckInterval(5000)
                          ->setSubscriptionMessageTrackingTimeout(50000)
                          ->create();

  LOGINFO("Created the GemFire Cache Programmatically");

  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);

  // Create the Region Programmatically.
  RegionPtr regionPtr = regionFactory->create("DistRegionAck");

  LOGINFO("Created the Region Programmatically");

  // Get the QueryService from the Cache.
  QueryServicePtr qrySvcPtr = cachePtr->getQueryService();

  // Create CqAttributes and Install Listener
  CqAttributesFactory cqFac;
  CqListenerPtr cqLstner(new MyCqListener1());
  cqFac.addCqListener(cqLstner);
  CqAttributesPtr cqAttr = cqFac.create();

  LOGINFO("Attached CqListener");

  // create a new Cq Query
  const char* qryStr = "select * from /DistRegionAck ";
  CqQueryPtr qry = qrySvcPtr->newCq((char*)"MyCq", qryStr, cqAttr, true);

  LOGINFO("Created new CqQuery");

  // execute Cq Query
  qry->execute();
  gemfire::millisleep(10000);

  LOGINFO("Executed new CqQuery");

  // Send ready for Event message to Server( only for Durable Clients ).
  // Server will send queued events to client after recieving this.
  cachePtr->readyForEvents();

  LOGINFO("Sent ReadyForEvents message to server");

  // wait for some time to recieve events
  gemfire::millisleep(10000);

  // Close the GemFire Cache with keepalive = true.  Server will queue events
  // for
  // durable registered keys and will deliver all events when client will
  // reconnect
  // within timeout period and send "readyForEvents()"
  cachePtr->close(true);

  LOGINFO("Closed the GemFire Cache with keepalive as true");
}

void RunFeederClient() {
  CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();
  LOGINFO("Feeder connected to the GemFire Distributed System");

  CachePtr cachePtr = cacheFactory->create();

  LOGINFO("Created the GemFire Cache");

  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(PROXY);

  LOGINFO("Created the RegionFactory");

  // Create the Region Programmatically.
  RegionPtr regionPtr = regionFactory->create("DistRegionAck");

  LOGINFO("Created the Region Programmatically.");

  for (int i = 0; i < 10; i++) {
    CacheableKeyPtr keyPtr =
        dynCast<CacheableKeyPtr>(CacheableInt32::create(i));
    CacheableInt32Ptr valPtr = CacheableInt32::create(i);

    regionPtr->put(keyPtr, valPtr);
  }
  gemfire::millisleep(10000);
  LOGINFO("put on 0-10 keys done.");

  // Close the GemFire Cache
  cachePtr->close();

  LOGINFO("Closed the GemFire Cache");
}

void RunFeederClient1() {
  CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();
  LOGINFO("Feeder connected to the GemFire Distributed System");

  CachePtr cachePtr = cacheFactory->create();

  LOGINFO("Created the GemFire Cache");

  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(PROXY);

  LOGINFO("Created the RegionFactory");

  // Create the Region Programmatically.
  RegionPtr regionPtr = regionFactory->create("DistRegionAck");

  LOGINFO("Created the Region Programmatically.");

  for (int i = 10; i < 20; i++) {
    CacheableKeyPtr keyPtr =
        dynCast<CacheableKeyPtr>(CacheableInt32::create(i));
    CacheableInt32Ptr valPtr = CacheableInt32::create(i);

    regionPtr->put(keyPtr, valPtr);
  }
  gemfire::millisleep(10000);
  LOGINFO("put on 0-10 keys done.");

  // Close the GemFire Cache
  cachePtr->close();

  LOGINFO("Closed the GemFire Cache");
}

DUNIT_TASK_DEFINITION(CLIENT1, RunDurableClient)
  { RunDurableCqClient(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, RunFeeder)
  { RunFeederClient(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, RunFeeder1)
  { RunFeederClient1(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_PoolLocator)
  { stepOne(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyEvents)
  {
    LOGINFO("MyCqListener1::m_cntEvents = %d ", MyCqListener1::m_cntEvents);
    ASSERT(MyCqListener1::m_cntEvents == 20, "Incorrect events, expected 20");
  }
END_TASK_DEFINITION

void stepOne2() {
  initClientWithId(1);
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
      qh->populatePortfolioData(regPtr0, 130, 20, 20);
      qh->populatePositionData(subregPtr0, 130, 20);
    } else {
      qh->populatePortfolioPdxData(regPtr0, 130, 20, 20);
      qh->populatePositionPdxData(subregPtr0, 130, 20);
    }

    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    CqAttributesFactory cqFac;
    CqListenerPtr cqLstner(new MyCqListener());
    cqFac.addCqListener(cqLstner);
    CqAttributesPtr cqAttr = cqFac.create();

    const char* qryStr = "select * from /Portfolios p where p.ID < 3";
    CqQueryPtr qry = qs->newCq(cqName, qryStr, cqAttr);

    SelectResultsPtr results;
    try {
      LOG("EXECUTE 1 START");

      results = qry->executeWithInitialResults();

      LOG("EXECUTE 1 STOP");
      SelectResultsIterator iter = results->getIterator();
      char buf[100];
      int count = results->size();
      sprintf(buf, "results size=%d", count);
      LOG(buf);
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

    qh->populatePortfolioData(regPtr0, 140, 30, 20);
    qh->populatePositionData(subregPtr0, 140, 30);
    CacheablePtr port = NULLPTR;
    for (int i = 1; i < 140; i++) {
      if (!m_isPdx) {
        port = CacheablePtr(new Portfolio(i, 20));
      } else {
        port = CacheablePtr(new PortfolioPdx(i, 20));
      }
      CacheableKeyPtr keyport = CacheableKey::create("port1-1");
      regPtr0->put(keyport, port);
      SLEEP(10);  // sleep a while to allow server query to complete
    }

    LOG("StepTwo2 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Client1Down)
  {
    getHelper()->disconnect(true);
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = True");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Client2Down)
  {
    getHelper()->disconnect(true);
    cleanProc();
    LOG("Clnt2Down complete: Keepalive = True");
  }
END_TASK_DEFINITION

void client1Up() {
  // No RegisterIntrest again
  initClientWithId(0, true);
  createRegionForCQ(regionNamesCq[0], USE_ACK, true);

  LOG("Client1Up complete.");
  QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

  QueryServicePtr qs;

  qs = PoolManager::find(regionNamesCq[0])->getQueryService();
  CqAttributesFactory cqFac;
  CqListenerPtr cqLstner(new MyCqListener());
  cqFac.addCqListener(cqLstner);
  CqAttributesPtr cqAttr = cqFac.create();

  const char* qryStr = "select * from /Portfolios p where p.ID < 3";
  CqQueryPtr qry = qs->newCq(cqName, qryStr, cqAttr);

  try {
    LOG("EXECUTE 1 START");

    qry->execute();

    LOG("EXECUTE 1 STOP");
  } catch (const Exception& excp) {
    std::string logmsg = "";
    logmsg += excp.getName();
    logmsg += ": ";
    logmsg += excp.getMessage();
    LOG(logmsg.c_str());
    excp.printStackTrace();
  }
  try {
    getHelper()->cachePtr->readyForEvents();
  } catch (...) {
    LOG("Exception occured while sending readyForEvents");
  }
}

void client1UpDurableCQList() {
  // No RegisterIntrest again
  initClientWithId(0, true);
  createRegionForCQ(regionNamesCq[0], USE_ACK, true);

  LOG("Client1Up complete.");
  QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();
}

void client2UpDurableCQList() {
  // No RegisterIntrest again
  initClientWithId(1, true);
  createRegionForCQ(regionNamesCq[0], USE_ACK, true);
  LOG("Client2Up complete.");
  QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();
}

DUNIT_TASK_DEFINITION(CLIENT1, Client1Up_Pool)
  { client1Up(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Client1UpDurableCQList_Pool)
  { client1UpDurableCQList(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Client2UpDurableCQList_Pool)
  { client2UpDurableCQList(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFour)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    char buf[1024];
    try {
      // TEST_COVERAGE
      LOGINFO(
          "CLIENT-1 StepFour: verifying getCq() behaviour for the invalid CQ "
          "Name");
      CqQueryPtr invalidCqptr = qs->getCq("InValidCQ");
      ASSERT(invalidCqptr == NULLPTR,
             "Cqptr must be NULL, as it getCq() is invoked on invalid CQ name");
      /*if(invalidCqptr == NULLPTR){
        LOGINFO("Testing getCq(InvalidName) :: invalidCqptr is NULLPTR");
      }else{
         LOGINFO("Testing getCq(InvalidName) :: invalidCqptr is NOT NULL");
      }*/

      CqQueryPtr cqy = qs->getCq(cqName);
      cqy->stop();
      SLEEP(1500);  // sleep 0.025 min to allow server stop query to complete
      CqStatisticsPtr cqStats = cqy->getStatistics();
      sprintf(buf,
              "numInserts[%d], numDeletes[%d], numUpdates[%d], numEvents[%d]",
              cqStats->numInserts(), cqStats->numDeletes(),
              cqStats->numUpdates(), cqStats->numEvents());
      LOG(buf);
      sprintf(buf, "MyCount:onEventCount=%d, onErrorCount=%d", onEventCount,
              onErrorCount);
      LOG(buf);
      ASSERT(cqStats->numEvents() > 0, "stats incorrect!");
      cqy->close();
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

DUNIT_TASK_DEFINITION(SERVER1, CloseLocator)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetPortfolioTypeToPdx)
  { m_isPdx = true; }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, UnsetPortfolioTypeToPdx)
  { m_isPdx = false; }
END_TASK_DEFINITION

bool isDurableCQName(const char* cqName, int clientID, bool isRecycled) {
  bool bRetVal = false;
  int i = 0;
  if (clientID == 1) {
    if (!isRecycled) {
      for (i = 0; i < 4; i++) {
        if (strcmp(cqName, durableCQNamesClient1[i]) == 0) break;
      }
      if (i < 4) bRetVal = true;
    } else {
      for (i = 0; i < 8; i++) {
        if (strcmp(cqName, durableCQNamesClient1[i]) == 0) break;
      }
      if (i < 8) bRetVal = true;
    }
  } else if (clientID == 2) {
    if (!isRecycled) {
      for (i = 0; i < 4; i++) {
        if (strcmp(cqName, durableCQNamesClient2[i]) == 0) break;
      }
      if (i < 4) bRetVal = true;
    } else {
      for (i = 0; i < 8; i++) {
        if (strcmp(cqName, durableCQNamesClient2[i]) == 0) break;
      }
      if (i < 8) bRetVal = true;
    }
  }
  return bRetVal;
}

void doThinClientCqDurable() {
  CALL_TASK(CreateLocator);
  CALL_TASK(CreateServer1_Locator);

  CALL_TASK(StepOne_PoolLocator);
  CALL_TASK(StepOne2_PoolLocator);

  CALL_TASK(StepTwo);
  CALL_TASK(StepThree);
  CALL_TASK(StepTwo2);
  CALL_TASK(Client1Down);

  CALL_TASK(Client1Up_Pool);

  CALL_TASK(StepFour);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator);
}

DUNIT_TASK_DEFINITION(CLIENT1, RegisterCqs1)
  {
    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    CqAttributesFactory cqFac;
    CqAttributesPtr cqAttr = cqFac.create();

    qs->newCq(durableCQNamesClient1[0],
              "select * from /Portfolios p where p.ID < 3", cqAttr, true)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient1[1],
              "select * from /Portfolios p where p.ID > 5", cqAttr, true)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient1[2],
              "select * from /Portfolios p where p.ID > 10", cqAttr, false)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient1[3],
              "select * from /Portfolios p where p.ID = 0", cqAttr, false)
        ->executeWithInitialResults();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, RegisterCqsAfterClientup1)
  {
    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    CqAttributesFactory cqFac;
    CqAttributesPtr cqAttr = cqFac.create();

    qs->newCq(durableCQNamesClient1[4],
              "select * from /Portfolios p where p.ID < 3", cqAttr, true)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient1[5],
              "select * from /Portfolios p where p.ID > 5", cqAttr, true)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient1[6],
              "select * from /Portfolios p where p.ID > 10", cqAttr, false)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient1[7],
              "select * from /Portfolios p where p.ID = 0", cqAttr, false)
        ->executeWithInitialResults();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, RegisterCqs2)
  {
    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    CqAttributesFactory cqFac;
    CqAttributesPtr cqAttr = cqFac.create();

    qs->newCq(durableCQNamesClient2[0],
              "select * from /Portfolios p where p.ID < 3", cqAttr, true)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient2[1],
              "select * from /Portfolios p where p.ID > 5", cqAttr, true)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient2[2],
              "select * from /Portfolios p where p.ID > 10", cqAttr, true)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient2[3],
              "select * from /Portfolios p where p.ID = 5", cqAttr, true)
        ->executeWithInitialResults();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, RegisterCqsAfterClientup2)
  {
    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    CqAttributesFactory cqFac;
    CqAttributesPtr cqAttr = cqFac.create();

    qs->newCq(durableCQNamesClient2[4],
              "select * from /Portfolios p where p.ID < 3", cqAttr, true)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient2[5],
              "select * from /Portfolios p where p.ID > 5", cqAttr, true)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient2[6],
              "select * from /Portfolios p where p.ID > 10", cqAttr, true)
        ->executeWithInitialResults();
    qs->newCq(durableCQNamesClient2[7],
              "select * from /Portfolios p where p.ID = 5", cqAttr, true)
        ->executeWithInitialResults();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyCqs1)
  {
    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }

    CacheableArrayListPtr durableCqListPtr = qs->getAllDurableCqsFromServer();

    ASSERT(durableCqListPtr != NULLPTR, "Durable CQ List should not be null");
    ASSERT(durableCqListPtr->length() == 2,
           "Durable CQ List lenght should be 2");
    ASSERT(isDurableCQName(durableCqListPtr->at(0)->toString()->asChar(), 1,
                           false),
           "Durable CQ name should be in the durable cq list");
    ASSERT(isDurableCQName(durableCqListPtr->at(1)->toString()->asChar(), 1,
                           false),
           "Durable CQ name should be in the durable cq list");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyCqsAfterClientup1)
  {
    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }

    CacheableArrayListPtr durableCqListPtr = qs->getAllDurableCqsFromServer();
    ASSERT(durableCqListPtr != NULLPTR, "Durable CQ List should not be null");
    ASSERT(durableCqListPtr->length() == 4,
           "Durable CQ List length should be 4");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(0)->toString()->asChar(), 1, true),
        "Durable CQ name should be in the durable cq list");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(1)->toString()->asChar(), 1, true),
        "Durable CQ name should be in the durable cq list");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(2)->toString()->asChar(), 1, true),
        "Durable CQ name should be in the durable cq list");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(3)->toString()->asChar(), 1, true),
        "Durable CQ name should be in the durable cq list");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyCqs2)
  {
    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    CacheableArrayListPtr durableCqListPtr = qs->getAllDurableCqsFromServer();
    ASSERT(durableCqListPtr != NULLPTR, "Durable CQ List should not be null");
    ASSERT(durableCqListPtr->length() == 4,
           "Durable CQ List lenght should be 4");
    ASSERT(isDurableCQName(durableCqListPtr->at(0)->toString()->asChar(), 2,
                           false),
           "Durable CQ name should be in the durable cq list");
    ASSERT(isDurableCQName(durableCqListPtr->at(1)->toString()->asChar(), 2,
                           false),
           "Durable CQ name should be in the durable cq list");
    ASSERT(isDurableCQName(durableCqListPtr->at(2)->toString()->asChar(), 2,
                           false),
           "Durable CQ name should be in the durable cq list");
    ASSERT(isDurableCQName(durableCqListPtr->at(3)->toString()->asChar(), 2,
                           false),
           "Durable CQ name should be in the durable cq list");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyCqsAfterClientup2)
  {
    PoolPtr pool = PoolManager::find(regionNamesCq[0]);
    QueryServicePtr qs;
    if (pool != NULLPTR) {
      qs = pool->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    CacheableArrayListPtr durableCqListPtr = qs->getAllDurableCqsFromServer();
    ASSERT(durableCqListPtr != NULLPTR, "Durable CQ List should not be null");
    ASSERT(durableCqListPtr->length() == 8,
           "Durable CQ List lenght should be 8");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(0)->toString()->asChar(), 2, true),
        "Durable CQ name should be in the durable cq list");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(1)->toString()->asChar(), 2, true),
        "Durable CQ name should be in the durable cq list");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(2)->toString()->asChar(), 2, true),
        "Durable CQ name should be in the durable cq list");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(3)->toString()->asChar(), 2, true),
        "Durable CQ name should be in the durable cq list");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(4)->toString()->asChar(), 2, true),
        "Durable CQ name should be in the durable cq list");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(5)->toString()->asChar(), 2, true),
        "Durable CQ name should be in the durable cq list");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(6)->toString()->asChar(), 2, true),
        "Durable CQ name should be in the durable cq list");
    ASSERT(
        isDurableCQName(durableCqListPtr->at(7)->toString()->asChar(), 2, true),
        "Durable CQ name should be in the durable cq list");
  }
END_TASK_DEFINITION

void verifyEmptyDurableCQList() {
  PoolPtr pool = PoolManager::find(regionNamesCq[0]);
  QueryServicePtr qs;
  if (pool != NULLPTR) {
    qs = pool->getQueryService();
  } else {
    qs = getHelper()->cachePtr->getQueryService();
  }

  CacheableArrayListPtr durableCqListPtr = qs->getAllDurableCqsFromServer();
  ASSERT(durableCqListPtr == NULLPTR, "Durable CQ List should be null");
}

DUNIT_TASK_DEFINITION(CLIENT1, VerifyEmptyDurableCQList1)
  { verifyEmptyDurableCQList(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyEmptyDurableCQList2)
  { verifyEmptyDurableCQList(); }
END_TASK_DEFINITION

void getDurableCQsFromServerEmptyList() {
  CALL_TASK(CreateLocator);
  CALL_TASK(CreateServer1_Locator);

  CALL_TASK(StepOne_PoolLocator);
  CALL_TASK(StepOne2_PoolLocator);

  CALL_TASK(VerifyEmptyDurableCQList1);
  CALL_TASK(VerifyEmptyDurableCQList2);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator);
}

void getDurableCQsFromServer() {
  CALL_TASK(CreateLocator);
  CALL_TASK(CreateServer1_Locator);

  CALL_TASK(StepOne_PoolLocator);
  CALL_TASK(StepOne2_PoolLocator);

  CALL_TASK(RegisterCqs1);
  CALL_TASK(RegisterCqs2);
  CALL_TASK(VerifyCqs1);
  CALL_TASK(VerifyCqs2);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator);
}

void getDurableCQsFromServerWithCyclicClients() {
  CALL_TASK(CreateLocator);
  CALL_TASK(CreateServer1_Locator);

  CALL_TASK(StepOne_PoolLocator);
  CALL_TASK(StepOne2_PoolLocator);

  CALL_TASK(RegisterCqs1);
  CALL_TASK(RegisterCqs2);
  CALL_TASK(VerifyCqs1);
  CALL_TASK(VerifyCqs2);

  CALL_TASK(Client1Down);
  CALL_TASK(Client2Down);

  CALL_TASK(Client1UpDurableCQList_Pool);
  CALL_TASK(Client2UpDurableCQList_Pool);

  CALL_TASK(RegisterCqsAfterClientup1);
  CALL_TASK(RegisterCqsAfterClientup2);
  CALL_TASK(VerifyCqsAfterClientup1);
  CALL_TASK(VerifyCqsAfterClientup2);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator);
}

void setPortfolioPdxType() { CALL_TASK(SetPortfolioTypeToPdx) }

void UnsetPortfolioType() { CALL_TASK(UnsetPortfolioTypeToPdx) }

void doThinClientCqDurable1() {
  CALL_TASK(CreateServer);
  // First Run of Durable Client
  CALL_TASK(RunDurableClient);
  // Intermediate Feeder, feeding events
  CALL_TASK(RunFeeder);
  // Reconnect Durable Client
  CALL_TASK(RunDurableClient);
  // Intermediate Feeder, feeding events again
  CALL_TASK(RunFeeder1);
  // Reconnect Durable Client again
  CALL_TASK(RunDurableClient);
  // Verify we get 20 events
  CALL_TASK(VerifyEvents);
  CALL_TASK(CloseServer1);
}

DUNIT_MAIN
  {
    UnsetPortfolioType();
    for (int runIdx = 1; runIdx <= 2; ++runIdx) {
      doThinClientCqDurable();

      setPortfolioPdxType();

      getDurableCQsFromServerEmptyList();

      getDurableCQsFromServer();

      getDurableCQsFromServerWithCyclicClients();
    }

    doThinClientCqDurable1();
  }
END_MAIN
