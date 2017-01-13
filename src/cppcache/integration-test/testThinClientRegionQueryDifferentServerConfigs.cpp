/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * @file testThinClientRegionQueryDifferentServerConfigs.cpp
 *
 * @brief This tests that Region::query makes use of only region-level
 *        endpoints, and not all endpoints.
 *
 *
 */

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <string>
#include "QueryStrings.hpp"
#include "QueryHelper.hpp"
#include "ThinClientHelper.hpp"

using namespace gemfire;
using namespace test;
using namespace testobject;

#define CLIENT1 s1p1
#define LOCATOR s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

bool isLocalServer = false;
bool isLocator = false;
const char* poolNames[] = {"Pool1", "Pool2", "Pool3"};
const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);

const char* qRegionNames[] = {"Portfolios", "Positions"};
const char* sGNames[] = {"ServerGroup1", "ServerGroup2"};

void initClient() {
  try {
    Serializable::registerType(Position::createDeserializable);
    Serializable::registerType(Portfolio::createDeserializable);
  } catch (const IllegalStateException&) {
    // ignore exception
  }
  initClient(true);
  ASSERT(getHelper() != NULL, "null CacheHelper");
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

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1WithLocator)
  {
    LOG("Starting SERVER1...");

    if (isLocalServer) {
      CacheHelper::initServer(1, "regionquery_diffconfig_SG.xml", locHostPort);
    }

    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InitClientCreateRegionAndRunQueries)
  {
    LOG("Starting Step One with Pool + Locator lists");
    initClient();
    PoolPtr pool1 = NULLPTR;
    pool1 = createPool(poolNames[0], locHostPort, sGNames[0], 0, true);
    createRegionAndAttachPool(qRegionNames[0], USE_ACK, poolNames[0]);

    // populate the region
    RegionPtr reg = getHelper()->getRegion(qRegionNames[0]);
    QueryHelper& qh = QueryHelper::getHelper();
    qh.populatePortfolioData(reg, qh.getPortfolioSetSize(),
                             qh.getPortfolioNumSets());

    std::string qry1Str = (std::string) "select * from /" + qRegionNames[0];
    std::string qry2Str = (std::string) "select * from /" + qRegionNames[1];

    QueryServicePtr qs = NULLPTR;
    qs = pool1->getQueryService();

    SelectResultsPtr results;
    QueryPtr qry = qs->newQuery(qry1Str.c_str());
    results = qry->execute();
    ASSERT(
        results->size() == qh.getPortfolioSetSize() * qh.getPortfolioNumSets(),
        "unexpected number of results");
    try {
      qry = qs->newQuery(qry2Str.c_str());
      results = qry->execute();
      FAIL("Expected a QueryException");
    } catch (const QueryException& ex) {
      printf("Good expected exception: %s\n", ex.getMessage());
    }

    // now region queries
    results = reg->query(qry1Str.c_str());
    ASSERT(
        results->size() == qh.getPortfolioSetSize() * qh.getPortfolioNumSets(),
        "unexpected number of results");
    try {
      results = reg->query(qry2Str.c_str());
      FAIL("Expected a QueryException");
    } catch (const QueryException& ex) {
      printf("Good expected exception: %s\n", ex.getMessage());
    }

    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2WithLocator)
  {
    LOG("Starting SERVER2...");

    if (isLocalServer) {
      CacheHelper::initServer(2, "regionquery_diffconfig2_SG.xml", locHostPort);
    }
    LOG("SERVER2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateRegionAndRunQueries)
  {
    LOG("Starting Step Two with Pool + Locator list");
    // Create pool2
    PoolPtr pool2 = NULLPTR;

    pool2 = createPool(poolNames[1], locHostPort, sGNames[1], 0, true);
    createRegionAndAttachPool(qRegionNames[1], USE_ACK, poolNames[1]);

    // populate the region
    RegionPtr reg = getHelper()->getRegion(qRegionNames[1]);
    QueryHelper& qh = QueryHelper::getHelper();
    qh.populatePositionData(reg, qh.getPositionSetSize(),
                            qh.getPositionNumSets());

    std::string qry1Str = (std::string) "select * from /" + qRegionNames[0];
    std::string qry2Str = (std::string) "select * from /" + qRegionNames[1];

    QueryServicePtr qs = NULLPTR;
    qs = pool2->getQueryService();
    SelectResultsPtr results;
    QueryPtr qry;

    // now region queries
    try {
      results = reg->query(qry1Str.c_str());
      FAIL("Expected a QueryException");
    } catch (const QueryException& ex) {
      printf("Good expected exception: %s\n", ex.getMessage());
    }
    results = reg->query(qry2Str.c_str());
    ASSERT(results->size() == qh.getPositionSetSize() * qh.getPositionNumSets(),
           "unexpected number of results");

    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
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

DUNIT_TASK_DEFINITION(SERVER2, CloseServer2)
  {
    LOG("closing Server2...");
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
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

DUNIT_MAIN
  {
    CALL_TASK(StartLocator);
    CALL_TASK(CreateServer1WithLocator);
    CALL_TASK(InitClientCreateRegionAndRunQueries);
    CALL_TASK(CreateServer2WithLocator);
    CALL_TASK(CreateRegionAndRunQueries);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseServer2);
    CALL_TASK(CloseLocator);
  }
END_MAIN
