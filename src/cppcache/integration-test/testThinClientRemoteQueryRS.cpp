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

#define ROOT_NAME "testThinClientRemoteQueryRS"
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

bool isLocator = false;
bool isLocalServer = false;

const char* poolNames[] = {"Pool1", "Pool2", "Pool3"};
const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
bool isPoolConfig = false;  // To track if pool case is running
const char* qRegionNames[] = {"Portfolios", "Positions", "Portfolios2",
                              "Portfolios3"};
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
  // Create just one pool and attach all regions to that.
  initClient(true);
  isPoolConfig = true;
  createPool(poolNames[0], locHostPort, NULL, 0, true);
  createRegionAndAttachPool(qRegionNames[0], USE_ACK, poolNames[0]);
  createRegionAndAttachPool(qRegionNames[1], USE_ACK, poolNames[0]);
  createRegionAndAttachPool(qRegionNames[2], USE_ACK, poolNames[0]);
  createRegionAndAttachPool(qRegionNames[3], USE_ACK, poolNames[0]);

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

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc)
  {
    LOG("Starting Step One with Pool + Locator lists");
    stepOne();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetPortfolioTypeToPdx)
  { m_isPdx = true; }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, UnsetPortfolioTypeToPdx)
  { m_isPdx = false; }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(qRegionNames[0]);
    RegionPtr subregPtr0 = regPtr0->getSubregion("Positions");
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
      LOG("StepThree complete:Done populating Portfolio/Position object\n");
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
    LOG("StepThree complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFour)
  {
    SLEEP(500);
    bool doAnyErrorOccured = false;
    QueryHelper* qh = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    for (int i = 0; i < QueryStrings::RSOPLsize(); i++) {
      QueryPtr qry =
          qs->newQuery(const_cast<char*>(resultsetQueriesOPL[i].query()));
      SelectResultsPtr results = qry->execute();
      if (!qh->verifyRS(results, resultsetRowCountsOPL[i])) {
        char failmsg[100] = {0};
        ACE_OS::sprintf(failmsg, "Query verify failed for query index %d", i);
        doAnyErrorOccured = true;
        ASSERT(false, failmsg);
        continue;
      }

      ResultSetPtr rsptr = dynCast<ResultSetPtr>(results);
      SelectResultsIterator iter = rsptr->getIterator();
      for (int32_t rows = 0; rows < rsptr->size(); rows++) {
        if (rows > (int32_t)QueryHelper::getHelper().getPortfolioSetSize()) {
          continue;
        }

        if (!m_isPdx) {
          SerializablePtr ser = (*rsptr)[rows];
          if (instanceOf<PortfolioPtr>(ser)) {
            PortfolioPtr portfolio = staticCast<PortfolioPtr>(ser);
            printf(
                "   query idx %d pulled portfolio object ID %d, pkid  :: %s\n",
                i, portfolio->getID(), portfolio->getPkid()->asChar());
          } else if (instanceOf<PositionPtr>(ser)) {
            PositionPtr position = staticCast<PositionPtr>(ser);
            printf(
                "   query idx %d pulled position object secId %s, shares  :: "
                "%d\n",
                i, position->getSecId()->asChar(),
                position->getSharesOutstanding());
          } else {
            if (ser != NULLPTR) {
              printf(" query idx %d pulled object %s \n", i,
                     ser->toString()->asChar());
            } else {
              printf("   query idx %d pulled bad object \n", i);
              FAIL("Unexpected object received in query");
            }
          }
        } else {
          SerializablePtr pdxser = (*rsptr)[rows];
          if (instanceOf<PortfolioPdxPtr>(pdxser)) {
            PortfolioPdxPtr portfoliopdx = staticCast<PortfolioPdxPtr>(pdxser);
            printf(
                "   query idx %d pulled portfolioPdx object ID %d, pkid %s  :: "
                "\n",
                i, portfoliopdx->getID(), portfoliopdx->getPkid());
          } else if (instanceOf<PositionPdxPtr>(pdxser)) {
            PositionPdxPtr positionpdx = staticCast<PositionPdxPtr>(pdxser);
            printf(
                "   query idx %d pulled positionPdx object secId %s, shares %d "
                " "
                ":: \n",
                i, positionpdx->getSecId(),
                positionpdx->getSharesOutstanding());
          } else {
            if (pdxser != NULLPTR) {
              if (pdxser->toString()->isWideString()) {
                printf(" query idx %d pulled object %S  :: \n", i,
                       pdxser->toString()->asWChar());
              } else {
                printf(" query idx %d pulled object %s  :: \n", i,
                       pdxser->toString()->asChar());
              }
            } else {
              printf("   query idx %d pulled bad object  :: \n", i);
              FAIL("Unexpected object received in query");
            }
          }
        }
      }
    }

    if (!doAnyErrorOccured) {
      printf("HURRAY !! StepFour PASSED \n\n");
    } else {
      FAIL("Failed in StepFour verification");
    }

    LOG("StepFour complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
  {
    SLEEP(500);
    bool doAnyErrorOccured = false;
    QueryHelper* qh = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }
    for (int i = 0; i < QueryStrings::RSsize(); i++) {
      if (m_isPdx == true) {
        if (i == 2 || i == 3 || i == 4) {
          LOGINFO(
              "Skipping query index %d for Pdx because it is function type.",
              i);
          continue;
        }
      }

      if (resultsetQueries[i].category != unsupported) {
        QueryPtr qry =
            qs->newQuery(const_cast<char*>(resultsetQueries[i].query()));
        SelectResultsPtr results = qry->execute();
        if (!qh->verifyRS(results, (qh->isExpectedRowsConstantRS(i)
                                        ? resultsetRowCounts[i]
                                        : resultsetRowCounts[i] *
                                              qh->getPortfolioNumSets()))) {
          char failmsg[100] = {0};
          ACE_OS::sprintf(failmsg, "Query verify failed for query index %d", i);
          doAnyErrorOccured = true;
          ASSERT(false, failmsg);
          continue;
        }

        ResultSetPtr rsptr = dynCast<ResultSetPtr>(results);
        SelectResultsIterator iter = rsptr->getIterator();
        for (int32_t rows = 0; rows < rsptr->size(); rows++) {
          if (rows > (int32_t)QueryHelper::getHelper().getPortfolioSetSize()) {
            continue;
          }

          if (!m_isPdx) {
            SerializablePtr ser = (*rsptr)[rows];
            if (instanceOf<PortfolioPtr>(ser)) {
              PortfolioPtr portfolio = staticCast<PortfolioPtr>(ser);
              printf(
                  "   query idx %d pulled portfolio object ID %d, pkid  :: "
                  "%s\n",
                  i, portfolio->getID(), portfolio->getPkid()->asChar());
            } else if (instanceOf<PositionPtr>(ser)) {
              PositionPtr position = staticCast<PositionPtr>(ser);
              printf(
                  "   query idx %d pulled position object secId %s, shares  :: "
                  "%d\n",
                  i, position->getSecId()->asChar(),
                  position->getSharesOutstanding());
            } else {
              if (ser != NULLPTR) {
                printf(" query idx %d pulled object %s \n", i,
                       ser->toString()->asChar());
              } else {
                printf("   query idx %d pulled bad object \n", i);
                FAIL("Unexpected object received in query");
              }
            }
          } else {
            SerializablePtr pdxser = (*rsptr)[rows];
            if (instanceOf<PortfolioPdxPtr>(pdxser)) {
              PortfolioPdxPtr portfoliopdx =
                  staticCast<PortfolioPdxPtr>(pdxser);
              printf(
                  "   query idx %d pulled portfolioPdx object ID %d, pkid %s  "
                  ":: "
                  "\n",
                  i, portfoliopdx->getID(), portfoliopdx->getPkid());
            } else if (instanceOf<PositionPdxPtr>(pdxser)) {
              PositionPdxPtr positionpdx = staticCast<PositionPdxPtr>(pdxser);
              printf(
                  "   query idx %d pulled positionPdx object secId %s, shares "
                  "%d "
                  " :: \n",
                  i, positionpdx->getSecId(),
                  positionpdx->getSharesOutstanding());
            } else {
              if (pdxser != NULLPTR) {
                if (pdxser->toString()->isWideString()) {
                  printf(" query idx %d pulled object %S  :: \n", i,
                         pdxser->toString()->asWChar());
                } else {
                  printf(" query idx %d pulled object %s  :: \n", i,
                         pdxser->toString()->asChar());
                }
              } else {
                printf("   query idx %d pulled bad object  :: \n", i);
                FAIL("Unexpected object received in query");
              }
            }
          }
        }
      }
    }

    if (!doAnyErrorOccured) {
      printf("HURRAY !! We PASSED \n\n");
    } else {
      FAIL("Failed in StepFive verification");
    }

    LOG("StepFive complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSix)
  {
    SLEEP(500);
    bool doAnyErrorOccured = false;
    QueryHelper* qh = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }

    for (int i = 0; i < QueryStrings::RSPsize(); i++) {
      if (resultsetparamQueries[i].category != unsupported) {
        QueryPtr qry =
            qs->newQuery(const_cast<char*>(resultsetparamQueries[i].query()));
        // LOGINFO("NIL::229:Retrieved QueryString = %s",
        // qry->getQueryString());

        CacheableVectorPtr paramList = CacheableVector::create();
        for (int j = 0; j < noofQueryParam[i]; j++) {
          // LOGINFO("NIL::260: queryparamSet[%d][%d] = %s", i, j,
          // queryparamSet[i][j]);
          if (atoi(queryparamSet[i][j]) != 0) {
            paramList->push_back(Cacheable::create(atoi(queryparamSet[i][j])));
          } else {
            paramList->push_back(Cacheable::create(queryparamSet[i][j]));
          }
        }

        SelectResultsPtr results = qry->execute(paramList);
        if (!qh->verifyRS(results, (qh->isExpectedRowsConstantPQRS(i)
                                        ? resultsetRowCountsPQ[i]
                                        : resultsetRowCountsPQ[i] *
                                              qh->getPortfolioNumSets()))) {
          char failmsg[100] = {0};
          ACE_OS::sprintf(failmsg, "Query verify failed for query index %d", i);
          doAnyErrorOccured = true;
          ASSERT(false, failmsg);
          continue;
        }

        ResultSetPtr rsptr = dynCast<ResultSetPtr>(results);
        SelectResultsIterator iter = rsptr->getIterator();
        for (int32_t rows = 0; rows < rsptr->size(); rows++) {
          if (rows > (int32_t)QueryHelper::getHelper().getPortfolioSetSize()) {
            continue;
          }

          if (!m_isPdx) {
            SerializablePtr ser = (*rsptr)[rows];
            if (instanceOf<PortfolioPtr>(ser)) {
              PortfolioPtr portfolio = staticCast<PortfolioPtr>(ser);
              printf(
                  "   query idx %d pulled portfolio object ID %d, pkid %s : \n",
                  i, portfolio->getID(), portfolio->getPkid()->asChar());
            } else if (instanceOf<PositionPtr>(ser)) {
              PositionPtr position = staticCast<PositionPtr>(ser);
              printf(
                  "   query idx %d pulled position object secId %s, shares %d  "
                  ": "
                  "\n",
                  i, position->getSecId()->asChar(),
                  position->getSharesOutstanding());
            } else {
              if (ser != NULLPTR) {
                printf(" query idx %d pulled object %s  : \n", i,
                       ser->toString()->asChar());
              } else {
                printf("   query idx %d pulled bad object  \n", i);
                FAIL("Unexpected object received in query");
              }
            }
          } else {
            SerializablePtr ser = (*rsptr)[rows];
            if (instanceOf<PortfolioPdxPtr>(ser)) {
              PortfolioPdxPtr portfoliopdx = staticCast<PortfolioPdxPtr>(ser);
              printf(
                  "   query idx %d pulled portfolioPdx object ID %d, pkid %s  "
                  ": "
                  "\n",
                  i, portfoliopdx->getID(), portfoliopdx->getPkid());
            } else if (instanceOf<PositionPdxPtr>(ser)) {
              PositionPdxPtr positionpdx = staticCast<PositionPdxPtr>(ser);
              printf(
                  "   query idx %d pulled positionPdx object secId %s, shares "
                  "%d "
                  " : \n",
                  i, positionpdx->getSecId(),
                  positionpdx->getSharesOutstanding());
            } else {
              if (ser != NULLPTR) {
                printf(" query idx %d pulled object %s : \n", i,
                       ser->toString()->asChar());
              } else {
                printf("   query idx %d pulled bad object\n", i);
                FAIL("Unexpected object received in query");
              }
            }
          }
        }
      }
    }

    if (!doAnyErrorOccured) {
      printf("HURRAY !! We PASSED \n\n");
    } else {
      FAIL("Failed in StepSix verification");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, DoQueryRSError)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }

    for (int i = 0; i < QueryStrings::RSsize(); i++) {
      if (resultsetQueries[i].category == unsupported) {
        QueryPtr qry =
            qs->newQuery(const_cast<char*>(resultsetQueries[i].query()));

        try {
          SelectResultsPtr results = qry->execute();

          char failmsg[100] = {0};
          ACE_OS::sprintf(failmsg, "Query exception didnt occur for index %d",
                          i);
          LOG(failmsg);
          FAIL(failmsg);
        } catch (gemfire::QueryException&) {
          // ok, expecting an exception, do nothing
        } catch (...) {
          ASSERT(false, "Got unexpected exception");
        }
      }
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
    LOG("cleanProc 1...");
    isPoolConfig = false;
    cleanProc();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer)
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

void runRemoteQueryRSTest() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator)
  CALL_TASK(StepOnePoolLoc)

  CALL_TASK(StepThree)
  CALL_TASK(StepFour)
  CALL_TASK(StepFive)
  CALL_TASK(StepSix)
  CALL_TASK(DoQueryRSError)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void setPortfolioPdxType() { CALL_TASK(SetPortfolioTypeToPdx) }

void UnsetPortfolioType() { CALL_TASK(UnsetPortfolioTypeToPdx) }

DUNIT_MAIN
  {
    for (int i = 0; i < 2; i++) {
      runRemoteQueryRSTest();
      setPortfolioPdxType();
    }
  }
END_MAIN
