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

#define ROOT_NAME "testThinClientRemoteQueryFailover"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientHelper.hpp"

#include <gfcpp/Query.hpp>
#include <gfcpp/QueryService.hpp>
#include <gfcpp/ResultSet.hpp>
#include <gfcpp/StructSet.hpp>
#include <gfcpp/SelectResultsIterator.hpp>

#include "testobject/Portfolio.hpp"
#include "testobject/PortfolioPdx.hpp"

using namespace gemfire;
using namespace test;
using namespace testobject;

#define CLIENT1 s1p1
#define LOCATOR s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

class KillServerThread : public ACE_Task_Base {
 public:
  bool m_running;
  KillServerThread() : m_running(false) {}
  int svc(void) {
    while (m_running == true) {
      CacheHelper::closeServer(1);
      LOG("THREAD CLOSED SERVER 1");
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

bool isLocator = false;
bool isLocalServer = false;

const char* qRegionNames[] = {"Portfolios", "Positions"};
KillServerThread* kst = NULL;
const char* poolNames[] = {"Pool1", "Pool2", "Pool3"};
const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
bool isPoolConfig = false;  // To track if pool case is running

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
      CacheHelper::initServer(1, "cacheserver_remoteoql.xml", locHostPort);
    }
    LOG("SERVER1 started with Locator");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2WithLocator)
  {
    LOG("Starting SERVER2...");
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver_remoteoql2.xml", locHostPort);
    }
    LOG("SERVER2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, RegisterTypesAndCreatePoolAndRegion)
  {
    LOG("Starting Step One with Pool + Locator lists");
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

    RegionPtr rptr = getHelper()->cachePtr->getRegion(qRegionNames[0]);

    CacheablePtr port1(new Portfolio(1, 100));
    CacheablePtr port2(new Portfolio(2, 200));
    CacheablePtr port3(new Portfolio(3, 300));
    CacheablePtr port4(new Portfolio(4, 400));

    rptr->put("1", port1);
    rptr->put("2", port2);
    rptr->put("3", port3);
    rptr->put("4", port4);

    LOG("RegisterTypesAndCreatePoolAndRegion complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ValidateQueryExecutionAcrossServerFailure)
  {
    try {
      kst = new KillServerThread();

      QueryServicePtr qs = NULLPTR;
      if (isPoolConfig) {
        PoolPtr pool1 = findPool(poolNames[0]);
        qs = pool1->getQueryService();
      } else {
        qs = getHelper()->cachePtr->getQueryService();
      }

      for (int i = 0; i < 10000; i++) {
        QueryPtr qry = qs->newQuery("select distinct * from /Portfolios");

        SelectResultsPtr results;
        results = qry->execute();

        if (i == 10) {
          kst->start();
        }

        int resultsize = results->size();

        if (i % 100 == 0) {
          printf("Iteration upto %d done, result size is %d\n", i, resultsize);
        }

        if (resultsize != 4)  // the XMLs for server 1 and server 2 have 1 and 2
                              // entries respectively
        {
          LOG("Result size is not 4!");
          FAIL("Result size is not 4!");
        }
      }

      kst->stop();
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

    LOG("ValidateQueryExecutionAcrossServerFailure complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
    LOG("cleanProc 1...");
    isPoolConfig = false;
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

DUNIT_TASK_DEFINITION(LOCATOR, CloseLocator)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

void runRemoteQueryFailoverTest() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServer1WithLocator)
  CALL_TASK(RegisterTypesAndCreatePoolAndRegion)
  CALL_TASK(CreateServer2WithLocator)
  CALL_TASK(ValidateQueryExecutionAcrossServerFailure)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseServer2)
  CALL_TASK(CloseLocator)
}

DUNIT_MAIN
  { runRemoteQueryFailoverTest(); }
END_MAIN
