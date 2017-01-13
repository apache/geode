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
#include <ace/Task.h>
#include <string>

#define ROOT_NAME "testThinClientRegionQueryExclusiveness"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientHelper.hpp"

#include "testobject/Portfolio.hpp"

#include <gfcpp/Query.hpp>
#include <gfcpp/QueryService.hpp>
#include <gfcpp/ResultSet.hpp>
#include <gfcpp/StructSet.hpp>
#include <gfcpp/SelectResultsIterator.hpp>

using namespace gemfire;
using namespace test;
using namespace testobject;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER s2p1
#define LOCATOR s2p2

bool isLocator = false;
bool isLocalServer = false;
const char* poolNames[] = {"Pool1", "Pool2", "Pool3"};
const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);

const char* qRegionNames[] = {"Portfolios", "Positions"};

void clientOperations() {
  try {
    Serializable::registerType(Position::createDeserializable);
    Serializable::registerType(Portfolio::createDeserializable);
  } catch (const IllegalStateException&) {
    // ignore exception
  }
  initClient(true);

  try {
    QueryServicePtr qs = NULLPTR;  // getHelper()->cachePtr->getQueryService();

    qs = createPool2("_TESTFAILPOOL_", NULL, NULL)->getQueryService();

    SelectResultsPtr results;
    QueryPtr qry = qs->newQuery("select distinct * from /Portfolios");
    results = qry->execute();
    FAIL("Since no region has been created yet, so exception expected");
  } catch (IllegalStateException& ex) {
    const char* err_msg = ex.getMessage();
    LOG("Good expected exception");
    LOG(err_msg);
  }

  PoolPtr pool1 = NULLPTR;
  pool1 = createPool(poolNames[0], locHostPort, NULL, 0, true);
  createRegionAndAttachPool(qRegionNames[0], USE_ACK, poolNames[0]);

  RegionPtr rptr = getHelper()->cachePtr->getRegion(qRegionNames[0]);
  PortfolioPtr p1(new Portfolio(1, 100));
  PortfolioPtr p2(new Portfolio(2, 100));
  PortfolioPtr p3(new Portfolio(3, 100));
  PortfolioPtr p4(new Portfolio(4, 100));

  rptr->put("1", p1);
  rptr->put("2", p2);
  rptr->put("3", p3);
  rptr->put("4", p4);

  QueryServicePtr qs = NULLPTR;
  qs = pool1->getQueryService();

  QueryPtr qry1 = qs->newQuery("select distinct * from /Portfolios");
  SelectResultsPtr results1 = qry1->execute();
  ASSERT(results1->size() == 4,
         "Expected 4 as number of portfolio objects put were 4");

  // Bring down the region
  rptr->localDestroyRegion();
  LOG("StepOne complete.");

  try {
    LOG("Going to execute the query");
    QueryPtr qry2 = qs->newQuery("select distinct * from /Portfolios");
    SelectResultsPtr results2 = qry2->execute();
    ASSERT(results2->size() == 4, "Failed verification");
  } catch (...) {
    FAIL("Got an exception!");
  }
  LOG("StepTwo complete.");
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

DUNIT_TASK_DEFINITION(SERVER, CreateServer)
  {
    LOG("Starting SERVER...");
    if (isLocalServer) CacheHelper::initServer(1, "cacheserver_remoteoql.xml");
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER, CreateServerWithLocator)
  {
    LOG("Starting SERVER1...");

    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_remoteoql.xml", locHostPort);
    }

    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ClientOpPoolLocator)
  { clientOperations(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
    LOG("cleanProc 1...");
    cleanProc();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER, CloseServer)
  {
    LOG("closing Server1...");
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER stopped");
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
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator)
    CALL_TASK(ClientOpPoolLocator)
    CALL_TASK(CloseCache1)
    CALL_TASK(CloseServer)
    CALL_TASK(CloseLocator)
  }
END_MAIN
