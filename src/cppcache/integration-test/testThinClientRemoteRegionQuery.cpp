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

#define ROOT_NAME "testThinClientRemoteRegionQuery"
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
static bool m_isPdx = false;
const char* qRegionNames[] = {"Portfolios", "Positions", "Portfolios2",
                              "Portfolios3"};

DUNIT_TASK_DEFINITION(LOCATOR, StartLocator)
  {
    // starting locator 1 2
    if (isLocator) {
      CacheHelper::initLocator(1);
    }
    LOG("Locator started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer)
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

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLocator)
  {
    try {
      Serializable::registerType(Position::createDeserializable);
      Serializable::registerType(Portfolio::createDeserializable);

      Serializable::registerPdxType(PositionPdx::createDeserializable);
      Serializable::registerPdxType(PortfolioPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    initClient(true);
    createPool(poolNames[0], locHostPort, NULL, 0, true);
    createRegionAndAttachPool(qRegionNames[0], USE_ACK, poolNames[0]);
    createRegionAndAttachPool(qRegionNames[1], USE_ACK, poolNames[0]);

    createRegionAndAttachPool(qRegionNames[2], USE_ACK, poolNames[0]);

    createPool(poolNames[1], locHostPort, NULL, 0, true);
    createRegionAndAttachPool(qRegionNames[3], USE_ACK, poolNames[1]);

    RegionPtr regptr = getHelper()->getRegion(qRegionNames[0]);
    RegionAttributesPtr lattribPtr = regptr->getAttributes();
    RegionPtr subregPtr = regptr->createSubregion(qRegionNames[1], lattribPtr);

    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepTwo)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(qRegionNames[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion(qRegionNames[1]);
    RegionPtr regPtr1 = getHelper()->getRegion(qRegionNames[1]);

    RegionPtr regPtr2 = getHelper()->getRegion(qRegionNames[2]);
    RegionPtr regPtr3 = getHelper()->getRegion(qRegionNames[3]);

    QueryHelper* qh = &QueryHelper::getHelper();

    char buf[100];
    sprintf(buf, "SetSize %d, NumSets %d", qh->getPortfolioSetSize(),
            qh->getPortfolioNumSets());
    LOG(buf);

    if (!m_isPdx) {
      qh->populatePortfolioData(regPtr0, qh->getPortfolioSetSize(),
                                qh->getPortfolioNumSets());
      qh->populatePositionData(subregPtr0, qh->getPositionSetSize(),
                               qh->getPositionNumSets());
      qh->populatePositionData(regPtr1, qh->getPositionSetSize(),
                               qh->getPositionNumSets());

      qh->populatePortfolioData(regPtr2, qh->getPortfolioSetSize(),
                                qh->getPortfolioNumSets());
      qh->populatePortfolioData(regPtr3, qh->getPortfolioSetSize(),
                                qh->getPortfolioNumSets());
    } else {
      qh->populatePortfolioPdxData(regPtr0, qh->getPortfolioSetSize(),
                                   qh->getPortfolioNumSets());
      qh->populatePositionPdxData(subregPtr0, qh->getPositionSetSize(),
                                  qh->getPositionNumSets());
      qh->populatePositionPdxData(regPtr1, qh->getPositionSetSize(),
                                  qh->getPositionNumSets());

      qh->populatePortfolioPdxData(regPtr2, qh->getPortfolioSetSize(),
                                   qh->getPortfolioNumSets());
      qh->populatePortfolioPdxData(regPtr3, qh->getPortfolioSetSize(),
                                   qh->getPortfolioNumSets());
    }
    LOG("StepTwo complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    bool doAnyErrorOccured = false;

    RegionPtr region = getHelper()->getRegion(qRegionNames[0]);

    for (int i = 0; i < QueryStrings::RQsize(); i++) {
      if (m_isPdx) {
        if (i == 18) {
          LOGINFO(
              "Skipping query index %d because it is unsupported for pdx type.",
              i);
          continue;
        }
      }

      if (regionQueries[i].category == unsupported) {
        continue;
      }

      SelectResultsPtr results =
          region->query(const_cast<char*>(regionQueries[i].query()));

      if (results->size() != regionQueryRowCounts[i]) {
        char failmsg[100] = {0};
        ACE_OS::sprintf(
            failmsg,
            "FAIL: Query # %d expected result size is %d, actual is %d", i,
            regionQueryRowCounts[i], results->size());
        doAnyErrorOccured = true;
        LOG(failmsg);
        continue;
      }
    }

    try {
      SelectResultsPtr results = region->query("");
      FAIL("Expected IllegalArgumentException exception for empty predicate");
    } catch (gemfire::IllegalArgumentException ex) {
      LOG("got expected IllegalArgumentException exception for empty "
          "predicate:");
      LOG(ex.getMessage());
    }

    try {
      SelectResultsPtr results =
          region->query(const_cast<char*>(regionQueries[0].query()), 2200000);
      FAIL("Expected IllegalArgumentException exception for invalid timeout");
    } catch (gemfire::IllegalArgumentException ex) {
      LOG("got expected IllegalArgumentException exception for invalid "
          "timeout:");
      LOG(ex.getMessage());
    }

    try {
      SelectResultsPtr results =
          region->query(const_cast<char*>(regionQueries[0].query()), -1);
      FAIL("Expected IllegalArgumentException exception for invalid timeout");
    } catch (gemfire::IllegalArgumentException ex) {
      LOG("got expected IllegalArgumentException exception for invalid "
          "timeout:");
      LOG(ex.getMessage());
    }
    try {
      SelectResultsPtr results = region->query("bad predicate");
      FAIL("Expected QueryException exception for wrong predicate");
    } catch (QueryException ex) {
      LOG("got expected QueryException exception for wrong predicate:");
      LOG(ex.getMessage());
    }

    if (!doAnyErrorOccured) {
      LOG("ALL QUERIES PASSED");
    } else {
      LOG("QUERY ERROR(S) OCCURED");
      FAIL("QUERY ERROR(S) OCCURED");
    }

    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFour)
  {
    bool doAnyErrorOccured = false;

    RegionPtr region = getHelper()->getRegion(qRegionNames[0]);

    for (int i = 0; i < QueryStrings::RQsize(); i++) {
      if (regionQueries[i].category == unsupported) {
        continue;
      }

      bool existsValue =
          region->existsValue(const_cast<char*>(regionQueries[i].query()));

      bool expectedResult = regionQueryRowCounts[i] > 0 ? true : false;

      if (existsValue != expectedResult) {
        char failmsg[100] = {0};
        ACE_OS::sprintf(
            failmsg,
            "FAIL: Query # %d existsValue expected is %s, actual is %s", i,
            expectedResult ? "true" : "false", existsValue ? "true" : "false");
        doAnyErrorOccured = true;
        ASSERT(false, failmsg);
        continue;
      }
    }

    try {
      bool existsValue ATTR_UNUSED = region->existsValue("");
      FAIL("Expected IllegalArgumentException exception for empty predicate");
    } catch (gemfire::IllegalArgumentException ex) {
      LOG("got expected IllegalArgumentException exception for empty "
          "predicate:");
      LOG(ex.getMessage());
    }

    try {
      bool existsValue ATTR_UNUSED = region->existsValue(
          const_cast<char*>(regionQueries[0].query()), 2200000);
      FAIL("Expected IllegalArgumentException exception for invalid timeout");
    } catch (gemfire::IllegalArgumentException ex) {
      LOG("got expected IllegalArgumentException exception for invalid "
          "timeout:");
      LOG(ex.getMessage());
    }

    try {
      bool existsValue ATTR_UNUSED =
          region->existsValue(const_cast<char*>(regionQueries[0].query()), -1);
      FAIL("Expected IllegalArgumentException exception for invalid timeout");
    } catch (gemfire::IllegalArgumentException ex) {
      LOG("got expected IllegalArgumentException exception for invalid "
          "timeout:");
      LOG(ex.getMessage());
    }
    try {
      bool existsValue ATTR_UNUSED = region->existsValue("bad predicate");
      FAIL("Expected QueryException exception for wrong predicate");
    } catch (QueryException ex) {
      LOG("got expected QueryException exception for wrong predicate:");
      LOG(ex.getMessage());
    }

    if (!doAnyErrorOccured) {
      LOG("ALL QUERIES PASSED");
    } else {
      LOG("QUERY ERROR(S) OCCURED");
      FAIL("QUERY ERROR(S) OCCURED");
    }

    LOG("StepFour complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
  {
    bool doAnyErrorOccured = false;

    RegionPtr region = getHelper()->getRegion(qRegionNames[0]);

    for (int i = 0; i < QueryStrings::RQsize(); i++) {
      if (regionQueries[i].category == unsupported) {
        continue;
      }

      try {
        SerializablePtr result =
            region->selectValue(const_cast<char*>(regionQueries[i].query()));

        /*
              if (result == NULLPTR)
              {
                char logmsg[100] = {0};
                ACE_OS::sprintf(logmsg, "Query # %d query selectValue result is
           NULL", i);
                LOG(logmsg);
              }
              else
              {
                char logmsg[100] = {0};
                ACE_OS::sprintf(logmsg, "Query # %d query selectValue result
           size
           is not NULL", i);
                LOG(logmsg);
              }
        */
        if (!(regionQueryRowCounts[i] == 0 || regionQueryRowCounts[i] == 1)) {
          char logmsg[100] = {0};
          ACE_OS::sprintf(
              logmsg, "FAIL: Query # %d expected query exception did not occur",
              i);
          LOG(logmsg);
          doAnyErrorOccured = true;
        }
      } catch (const QueryException&) {
        if (regionQueryRowCounts[i] == 0 || regionQueryRowCounts[i] == 1) {
          char logmsg[100] = {0};
          ACE_OS::sprintf(
              logmsg, "FAIL: Query # %d unexpected query exception occured", i);
          LOG(logmsg);
          doAnyErrorOccured = true;
        }
      } catch (...) {
        char logmsg[100] = {0};
        ACE_OS::sprintf(logmsg,
                        "FAIL: Query # %d unexpected exception occurred", i);
        LOG(logmsg);
        FAIL(logmsg);
      }
    }

    try {
      SelectResultsPtr results = region->selectValue("");
      FAIL("Expected IllegalArgumentException exception for empty predicate");
    } catch (gemfire::IllegalArgumentException ex) {
      LOG("got expected IllegalArgumentException exception for empty "
          "predicate:");
      LOG(ex.getMessage());
    }

    try {
      SelectResultsPtr results = region->selectValue(
          const_cast<char*>(regionQueries[0].query()), 2200000);
      FAIL("Expected IllegalArgumentException exception for invalid timeout");
    } catch (gemfire::IllegalArgumentException ex) {
      LOG("got expected IllegalArgumentException exception for invalid "
          "timeout:");
      LOG(ex.getMessage());
    }

    try {
      SelectResultsPtr results =
          region->selectValue(const_cast<char*>(regionQueries[0].query()), -1);
      FAIL("Expected IllegalArgumentException exception for invalid timeout");
    } catch (gemfire::IllegalArgumentException ex) {
      LOG("got expected IllegalArgumentException exception for invalid "
          "timeout:");
      LOG(ex.getMessage());
    }
    try {
      SelectResultsPtr results = region->selectValue("bad predicate");
      FAIL("Expected IllegalArgumentException exception for wrong predicate");
    } catch (QueryException ex) {
      LOG("got expected QueryException for wrong predicate:");
      LOG(ex.getMessage());
    }
    if (!doAnyErrorOccured) {
      LOG("ALL QUERIES PASSED");
    } else {
      LOG("QUERY ERROR(S) OCCURED");
      FAIL("QUERY ERROR(S) OCCURED");
    }

    LOG("StepFive complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, QueryError)
  {
    RegionPtr region = getHelper()->getRegion(qRegionNames[0]);

    for (int i = 0; i < QueryStrings::RQsize(); i++) {
      if ((regionQueries[i].category != unsupported) ||
          (i == 3))  // UNDEFINED case
      {
        continue;
      }

      try {
        SelectResultsPtr results =
            region->query(const_cast<char*>(regionQueries[i].query()));

        char failmsg[100] = {0};
        ACE_OS::sprintf(failmsg, "Query exception didnt occur for index %d", i);
        LOG(failmsg);
        FAIL(failmsg);
      } catch (gemfire::QueryException ex) {
        // ok, expecting an exception, do nothing
      } catch (...) {
        LOG("Got unexpected exception");
        FAIL("Got unexpected exception");
      }
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
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

void runRemoteRegionQueryTest() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator)
  CALL_TASK(StepOnePoolLocator)

  CALL_TASK(StepTwo)
  CALL_TASK(StepThree)
  CALL_TASK(StepFour)
  CALL_TASK(StepFive)
  CALL_TASK(QueryError)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseServer1)

  CALL_TASK(CloseLocator)
}

void setPortfolioPdxType() { CALL_TASK(SetPortfolioTypeToPdx) }

void UnsetPortfolioType() { CALL_TASK(UnsetPortfolioTypeToPdx) }

DUNIT_MAIN
  {
    // Basic Old Test
    // runRemoteRegionQueryTest();

    UnsetPortfolioType();
    for (int runIdx = 1; runIdx <= 2; ++runIdx) {
      runRemoteRegionQueryTest();
      setPortfolioPdxType();
    }
  }
END_MAIN
