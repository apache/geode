/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <string>

#define ROOT_NAME "testThinClientRemoteQueryTimeout"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientHelper.hpp"

#include "QueryStrings.hpp"
#include "QueryHelper.hpp"

#include <gfcpp/Query.hpp>
#include <gfcpp/QueryService.hpp>

using namespace gemfire;
using namespace test;
using namespace testData;

#define CLIENT1 s1p1
#define LOCATOR s1p2
#define SERVER1 s2p1

bool isLocalServer = false;
bool isLocator = false;
const char* poolNames[] = {"Pool1", "Pool2", "Pool3"};
const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);

const char* qRegionNames[] = {"Portfolios", "Positions", "Portfolios2",
                              "Portfolios3"};

bool isPoolConfig = false;  // To track if pool case is running
static bool m_isPdx = false;
void stepOne() {
  try {
    Serializable::registerType(Position::createDeserializable);
    Serializable::registerType(Portfolio::createDeserializable);

    Serializable::registerPdxType(PositionPdx::createDeserializable);
    Serializable::registerPdxType(PortfolioPdx::createDeserializable);
  } catch (const IllegalStateException&) {
    // ignore exception
  }
  initClient(true);
  isPoolConfig = true;
  createPool(poolNames[0], locHostPort, NULL, 0, true);
  createRegionAndAttachPool(qRegionNames[0], USE_ACK, poolNames[0]);

  RegionPtr regptr = getHelper()->getRegion(qRegionNames[0]);
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  RegionPtr subregPtr = regptr->createSubregion(qRegionNames[1], lattribPtr);

  LOG("StepOne complete.");
}

DUNIT_TASK_DEFINITION(LOCATOR, StartLocator)
  {
    // starting locator 1 2
    if (isLocator) {
      CacheHelper::initLocator(1);
    }
    LOG("Locator started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    LOG("Starting SERVER1...");

    if (isLocalServer) CacheHelper::initServer(1, "remotequery.xml");

    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator)
  {
    LOG("Starting SERVER1...");

    if (isLocalServer) {
      CacheHelper::initServer(1, "remotequery.xml", locHostPort);
    }

    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc)
  {
    LOG("Starting Step One with Pool + Locator lists");
    stepOne();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepTwo)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(qRegionNames[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(qRegionNames[1]);

    QueryHelper* qh = &QueryHelper::getHelper();

    if (!m_isPdx) {
      qh->populatePortfolioData(regPtr0, 100, 20, 100);
      qh->populatePositionData(subregPtr0, 100, 20);
    } else {
      qh->populatePortfolioPdxData(regPtr0, 100, 20, 100);
      qh->populatePositionPdxData(subregPtr0, 100, 20);
    }

    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    QueryPtr qry =
        qs->newQuery(const_cast<char*>(resultsetQueries[34].query()));

    SelectResultsPtr results;

    try {
      LOG("EXECUTE 1 START");

      results = qry->execute(3);

      LOG("EXECUTE 1 STOP");

      char logmsg[50] = {0};
      ACE_OS::sprintf(logmsg, "Result size is %d", results->size());
      LOG(logmsg);

      LOG("Didnt get expected timeout exception for first execute");
      FAIL("Didnt get expected timeout exception for first execute");
    } catch (const TimeoutException& excp) {
      std::string logmsg = "";
      logmsg += "First execute expected exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    }

    SLEEP(150000);  // sleep 2.5 min to allow server query to complete

    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFour)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    QueryPtr qry =
        qs->newQuery(const_cast<char*>(resultsetQueries[34].query()));

    SelectResultsPtr results;

    try {
      LOG("EXECUTE 2 START");

      results = qry->execute(850);

      LOG("EXECUTE 2 STOP");

      char logmsg[50] = {0};
      ACE_OS::sprintf(logmsg, "Result size is %d", results->size());
      LOG(logmsg);
    } catch (Exception excp) {
      std::string failmsg = "";
      failmsg += "Second execute unwanted exception ";
      failmsg += excp.getName();
      failmsg += ": ";
      failmsg += excp.getMessage();
      LOG(failmsg.c_str());
      FAIL(failmsg.c_str());
    }

    LOG("StepFour complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    QueryPtr qry =
        qs->newQuery(const_cast<char*>(structsetQueries[17].query()));

    SelectResultsPtr results;

    try {
      LOG("EXECUTE 3 START");

      results = qry->execute(2);

      LOG("EXECUTE 3 STOP");

      char logmsg[50] = {0};
      ACE_OS::sprintf(logmsg, "Result size is %d", results->size());
      LOG(logmsg);

      LOG("Didnt get expected timeout exception for third execute");
      FAIL("Didnt get expected timeout exception for third execute");
    } catch (const TimeoutException& excp) {
      std::string logmsg = "";
      logmsg += "Third execute expected exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    }

    SLEEP(40000);  // sleep to allow server query to complete

    LOG("StepFive complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSix)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    QueryPtr qry =
        qs->newQuery(const_cast<char*>(structsetQueries[17].query()));

    SelectResultsPtr results;

    try {
      LOG("EXECUTE 4 START");

      results = qry->execute(850);

      LOG("EXECUTE 4 STOP");

      char logmsg[50] = {0};
      ACE_OS::sprintf(logmsg, "Result size is %d", results->size());
      LOG(logmsg);
    } catch (Exception excp) {
      std::string failmsg = "";
      failmsg += "Fourth execute unwanted exception ";
      failmsg += excp.getName();
      failmsg += ": ";
      failmsg += excp.getMessage();
      LOG(failmsg.c_str());
      FAIL(failmsg.c_str());
    }

    LOG("StepSix complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSeven)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    QueryPtr qry =
        qs->newQuery(const_cast<char*>(structsetParamQueries[5].query()));

    SelectResultsPtr results;

    try {
      LOG("EXECUTE 5 START");

      CacheableVectorPtr paramList = CacheableVector::create();

      for (int j = 0; j < numSSQueryParam[5]; j++) {
        if (atoi(queryparamSetSS[5][j]) != 0) {
          paramList->push_back(Cacheable::create(atoi(queryparamSetSS[5][j])));
        } else {
          paramList->push_back(Cacheable::create(queryparamSetSS[5][j]));
        }
      }
      results = qry->execute(paramList, 1);

      LOG("EXECUTE Five STOP");

      char logmsg[50] = {0};
      ACE_OS::sprintf(logmsg, "Result size is %d", results->size());
      LOG(logmsg);

      LOG("Didnt get expected timeout exception for fifth execute");
      FAIL("Didnt get expected timeout exception for fifth execute");
    } catch (const TimeoutException& excp) {
      std::string logmsg = "";
      logmsg += "Fifth execute expected exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    }

    SLEEP(40000);  // sleep to allow server query to complete

    LOG("StepSeven complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepEight)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    QueryPtr qry =
        qs->newQuery(const_cast<char*>(structsetParamQueries[5].query()));

    SelectResultsPtr results;

    try {
      LOG("EXECUTE 6 START");

      CacheableVectorPtr paramList = CacheableVector::create();

      for (int j = 0; j < numSSQueryParam[5]; j++) {
        if (atoi(queryparamSetSS[5][j]) != 0) {
          paramList->push_back(Cacheable::create(atoi(queryparamSetSS[5][j])));
        } else {
          paramList->push_back(Cacheable::create(queryparamSetSS[5][j]));
        }
      }

      results = qry->execute(paramList, 850);

      LOG("EXECUTE 6 STOP");

      char logmsg[50] = {0};
      ACE_OS::sprintf(logmsg, "Result size is %d", results->size());
      LOG(logmsg);
    } catch (Exception excp) {
      std::string failmsg = "";
      failmsg += "Sixth execute unwanted exception ";
      failmsg += excp.getName();
      failmsg += ": ";
      failmsg += excp.getMessage();
      LOG(failmsg.c_str());
      FAIL(failmsg.c_str());
    }

    LOG("StepEight complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, verifyNegativeValueTimeout)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    QueryPtr qry =
        qs->newQuery(const_cast<char*>(resultsetQueries[34].query()));

    SelectResultsPtr results;

    try {
      LOG("Task::verifyNegativeValueTimeout - EXECUTE 1 START");

      results = qry->execute(-3);

      LOG("Task::verifyNegativeValueTimeout - EXECUTE 1 STOP");

      char logmsg[50] = {0};
      ACE_OS::sprintf(logmsg, "Result size is %d", results->size());
      LOG(logmsg);

      LOG("Didnt get expected timeout exception for first execute");
      FAIL("Didnt get expected timeout exception for first execute");
    }

    catch (const IllegalArgumentException& excp) {
      std::string logmsg = "";
      logmsg += "execute expected exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    }

    SLEEP(150000);  // sleep 2.5 min to allow server query to complete

    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, verifyLargeValueTimeout)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    QueryPtr qry =
        qs->newQuery(const_cast<char*>(resultsetQueries[34].query()));

    SelectResultsPtr results;

    try {
      LOG("Task:: verifyLargeValueTimeout - EXECUTE 1 START");

      results = qry->execute(2147500);

      LOG("Task:: verifyLargeValueTimeout - EXECUTE 1 STOP");

      char logmsg[50] = {0};
      ACE_OS::sprintf(logmsg, "Result size is %d", results->size());
      LOG(logmsg);

      LOG("Didnt get expected timeout exception for first execute");
      FAIL("Didnt get expected timeout exception for first execute");
    }

    catch (const IllegalArgumentException& excp) {
      std::string logmsg = "";
      logmsg += "execute expected exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    }

    SLEEP(150000);  // sleep 2.5 min to allow server query to complete

    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
    isPoolConfig = false;
    LOG("cleanProc 1...");
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

DUNIT_TASK_DEFINITION(LOCATOR, CloseLocator)
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

void runRemoteQueryTimeoutTest() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator)
  CALL_TASK(StepOnePoolLoc)

  CALL_TASK(StepTwo)
  CALL_TASK(StepThree)
  CALL_TASK(StepFour)
  CALL_TASK(StepFive)
  CALL_TASK(StepSix)
  CALL_TASK(StepSeven)
  CALL_TASK(StepEight)
  CALL_TASK(verifyNegativeValueTimeout);
  CALL_TASK(verifyLargeValueTimeout);
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseServer1)

  CALL_TASK(CloseLocator)
}

void setPortfolioPdxType() { CALL_TASK(SetPortfolioTypeToPdx) }

void UnsetPortfolioType() { CALL_TASK(UnsetPortfolioTypeToPdx) }

DUNIT_MAIN
  {
    // Basic Old Test
    runRemoteQueryTimeoutTest();

    UnsetPortfolioType();
    for (int runIdx = 1; runIdx <= 2; ++runIdx) {
      // New Test with Pool + EP
      runRemoteQueryTimeoutTest();
      setPortfolioPdxType();
    }
  }
END_MAIN
