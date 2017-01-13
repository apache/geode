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

#define ROOT_NAME "testThinClientRemoteQuerySS"
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
const char* checkNullString(const char* str) {
  return ((str == NULL) ? "(null)" : str);
}

const wchar_t* checkNullString(const wchar_t* str) {
  return ((str == NULL) ? L"(null)" : str);
}

void _printFields(CacheablePtr field, Struct* ssptr, int32_t& fields) {
  PortfolioPtr portfolio = NULLPTR;
  PositionPtr position = NULLPTR;
  PortfolioPdxPtr portfolioPdx = NULLPTR;
  PositionPdxPtr positionPdx = NULLPTR;
  CacheableStringPtr str = NULLPTR;
  CacheableBooleanPtr boolptr = NULLPTR;

  if ((portfolio = dynamic_cast<Portfolio*>(field.ptr())) != NULLPTR) {
    printf("   pulled %s :- ID %d, pkid %s\n",
           checkNullString(ssptr->getFieldName(fields)), portfolio->getID(),
           checkNullString(portfolio->getPkid()->asChar()));
  } else if ((position = dynamic_cast<Position*>(field.ptr())) != NULLPTR) {
    printf("   pulled %s :- secId %s, shares %d\n",
           checkNullString(ssptr->getFieldName(fields)),
           checkNullString(position->getSecId()->asChar()),
           position->getSharesOutstanding());
  } else if ((portfolioPdx = dynamic_cast<PortfolioPdx*>(field.ptr())) !=
             NULLPTR) {
    printf("   pulled %s :- ID %d, pkid %s\n",
           checkNullString(ssptr->getFieldName(fields)), portfolioPdx->getID(),
           checkNullString(portfolioPdx->getPkid()));
  } else if ((positionPdx = dynamic_cast<PositionPdx*>(field.ptr())) !=
             NULLPTR) {
    printf("   pulled %s :- secId %s, shares %d\n",
           checkNullString(ssptr->getFieldName(fields)),
           checkNullString(positionPdx->getSecId()),
           positionPdx->getSharesOutstanding());
  } else {
    if ((str = dynamic_cast<CacheableString*>(field.ptr())) != NULLPTR) {
      if (str->isWideString()) {
        printf("   pulled %s :- %S\n",
               checkNullString(ssptr->getFieldName(fields)),
               checkNullString(str->asWChar()));
      } else {
        printf("   pulled %s :- %s\n",
               checkNullString(ssptr->getFieldName(fields)),
               checkNullString(str->asChar()));
      }
    } else if ((boolptr = dynamic_cast<CacheableBoolean*>(field.ptr())) !=
               NULLPTR) {
      printf("   pulled %s :- %s\n",
             checkNullString(ssptr->getFieldName(fields)),
             boolptr->toString()->asChar());
    } else {
      CacheableKeyPtr ptr = NULLPTR;
      CacheableStringArrayPtr strArr = NULLPTR;
      CacheableHashMapPtr map = NULLPTR;
      StructPtr structimpl = NULLPTR;

      if ((ptr = dynamic_cast<CacheableKey*>(field.ptr())) != NULLPTR) {
        char buff[1024] = {'\0'};
        ptr->logString(&buff[0], 1024);
        printf("   pulled %s :- %s \n",
               checkNullString(ssptr->getFieldName(fields)), buff);
      } else if ((strArr = dynamic_cast<CacheableStringArray*>(field.ptr())) !=
                 NULLPTR) {
        printf(" string array object printing \n\n");
        for (int stri = 0; stri < strArr->length(); stri++) {
          if (strArr->operator[](stri)->isWideString()) {
            printf("   pulled %s(%d) - %S \n",
                   checkNullString(ssptr->getFieldName(fields)), stri,
                   checkNullString(strArr->operator[](stri)->asWChar()));
          } else {
            printf("   pulled %s(%d) - %s \n",
                   checkNullString(ssptr->getFieldName(fields)), stri,
                   checkNullString(strArr->operator[](stri)->asChar()));
          }
        }
      } else if ((map = dynamic_cast<CacheableHashMap*>(field.ptr())) !=
                 NULLPTR) {
        int index = 0;
        for (CacheableHashMap::Iterator iter = map->begin(); iter != map->end();
             iter++) {
          printf("   hashMap %d of %d ... \n", ++index, map->size());
          _printFields(iter.first(), ssptr, fields);
          _printFields(iter.second(), ssptr, fields);
        }
        printf("   end of map \n");
      } else if ((structimpl = dynamic_cast<Struct*>(field.ptr())) != NULLPTR) {
        printf("   structImpl %s {\n",
               checkNullString(ssptr->getFieldName(fields)));
        for (int32_t inner_fields = 0; inner_fields < structimpl->length();
             inner_fields++) {
          SerializablePtr field = (*structimpl)[inner_fields];
          if (field == NULLPTR) {
            printf("we got null fields here, probably we have NULL data\n");
            continue;
          }

          _printFields(field, structimpl.ptr(), inner_fields);

        }  // end of field iterations
        printf("   } //end of %s\n",
               checkNullString(ssptr->getFieldName(fields)));
      } else {
        printf(
            "unknown field data.. couldn't even convert it to Cacheable "
            "variants\n");
      }
    }

  }  // end of else
}

void _verifyStructSet(StructSetPtr& ssptr, int i) {
  printf("query idx %d \n", i);
  for (int32_t rows = 0; rows < ssptr->size(); rows++) {
    if (rows > (int32_t)QueryHelper::getHelper().getPortfolioSetSize()) {
      continue;
    }

    Struct* siptr = dynamic_cast<Struct*>(((*ssptr)[rows]).ptr());
    if (siptr == NULL) {
      printf("siptr is NULL \n\n");
      continue;
    }

    printf("   Row : %d \n", rows);
    for (int32_t fields = 0; fields < siptr->length(); fields++) {
      SerializablePtr field = (*siptr)[fields];
      if (field == NULLPTR) {
        printf("we got null fields here, probably we have NULL data\n");
        continue;
      }

      _printFields(field, siptr, fields);

    }  // end of field iterations
  }    // end of row iterations
}

void compareMaps(HashMapOfCacheable& map, HashMapOfCacheable& expectedMap) {
  ASSERT(expectedMap.size() == map.size(),
         "Unexpected number of entries in map");
  LOGINFO("Got expected number of %d entries in map", map.size());
  for (HashMapOfCacheable::Iterator iter = map.begin(); iter != map.end();
       ++iter) {
    const CacheableKeyPtr& key = iter.first();
    const CacheablePtr& val = iter.second();
    HashMapOfCacheable::Iterator expectedIter = expectedMap.find(key);
    if (expectedIter == expectedMap.end()) {
      FAIL("Could not find expected key in map");
    }
    const CacheablePtr& expectedVal = expectedIter.second();

    if (instanceOf<PositionPdxPtr>(expectedVal)) {
      const PositionPdxPtr& posVal = dynCast<PositionPdxPtr>(val);
      const PositionPdxPtr& expectedPosVal =
          staticCast<PositionPdxPtr>(expectedVal);
      ASSERT(*expectedPosVal->getSecId() == *posVal->getSecId(),
             "Expected the secIDs to be equal in PositionPdx");
      ASSERT(expectedPosVal->getSharesOutstanding() ==
                 posVal->getSharesOutstanding(),
             "Expected the sharesOutstanding to be equal in PositionPdx");
    } else {
      const PortfolioPdxPtr& portVal = dynCast<PortfolioPdxPtr>(val);
      const PortfolioPdxPtr& expectedPortVal =
          dynCast<PortfolioPdxPtr>(expectedVal);
      ASSERT(expectedPortVal->getID() == portVal->getID(),
             "Expected the IDs to be equal in PortfolioPdx");
      ASSERT(expectedPortVal->getNewValSize() == portVal->getNewValSize(),
             "Expected the sizes to be equal in PortfolioPdx");
    }
  }
}

void stepOne() {
  try {
    Serializable::registerType(Position::createDeserializable);
    Serializable::registerType(Portfolio::createDeserializable);

    Serializable::registerPdxType(PositionPdx::createDeserializable);
    Serializable::registerPdxType(PortfolioPdx::createDeserializable);
  } catch (const IllegalStateException&) {
    // ignore exception
  }

  initGridClient(true);

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

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(qRegionNames[0]);
    RegionPtr regPtr1 = regPtr0->getSubregion("Positions");
    RegionPtr regPtr2 = getHelper()->getRegion(qRegionNames[1]);

    RegionPtr regPtr3 = getHelper()->getRegion(qRegionNames[2]);
    RegionPtr regPtr4 = getHelper()->getRegion(qRegionNames[3]);

    QueryHelper* qh = &QueryHelper::getHelper();

    qh->populatePortfolioPdxData(regPtr0, qh->getPortfolioSetSize(),
                                 qh->getPortfolioNumSets());
    qh->populatePositionPdxData(regPtr1, qh->getPositionSetSize(),
                                qh->getPositionNumSets());
    qh->populatePositionPdxData(regPtr2, qh->getPositionSetSize(),
                                qh->getPositionNumSets());

    qh->populatePortfolioPdxData(regPtr3, qh->getPortfolioSetSize(),
                                 qh->getPortfolioNumSets());
    qh->populatePortfolioPdxData(regPtr4, qh->getPortfolioSetSize(),
                                 qh->getPortfolioNumSets());

    char buf[100];
    sprintf(buf, "SetSize %d, NumSets %d", qh->getPortfolioSetSize(),
            qh->getPortfolioNumSets());
    LOG(buf);

    LOG("StepThree complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFour)
  {
    SLEEP(100);
    bool doAnyErrorOccured = false;
    QueryHelper* qh = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }

    for (int i = 0; i < QueryStrings::SSOPLsize(); i++) {
      QueryPtr qry =
          qs->newQuery(const_cast<char*>(structsetQueriesOPL[i].query()));
      SelectResultsPtr results = qry->execute();
      if (!qh->verifySS(results, structsetRowCountsOPL[i],
                        structsetFieldCountsOPL[i])) {
        char failmsg[100] = {0};
        ACE_OS::sprintf(failmsg, "Query verify failed for query index %d", i);
        ASSERT(false, failmsg);
        continue;
      }

      StructSetPtr ssptr =
          StructSetPtr(dynamic_cast<StructSet*>(results.ptr()));
      if ((ssptr) == NULLPTR) {
        LOG("Zero records were expected and found. Moving onto next. ");
        continue;
      }

      _verifyStructSet(ssptr, i);
    }

    if (!doAnyErrorOccured) printf("HURRAY !! StepFour PASSED \n\n");

    LOG("StepFour complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
  {
    SLEEP(100);
    bool doAnyErrorOccured = false;
    QueryHelper* qh = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }

    for (int i = 0; i < QueryStrings::SSsize(); i++) {
      if (i == 12 || i == 4 || i == 7 || i == 22 || i == 30 || i == 34) {
        LOGDEBUG("Skipping query index %d for pdx because it has function.", i);
        continue;
      }

      if (structsetQueries[i].category != unsupported) {
        QueryPtr qry =
            qs->newQuery(const_cast<char*>(structsetQueries[i].query()));
        SelectResultsPtr results = qry->execute();
        if (!qh->verifySS(results, (qh->isExpectedRowsConstantSS(i)
                                        ? structsetRowCounts[i]
                                        : structsetRowCounts[i] *
                                              qh->getPortfolioNumSets()),
                          structsetFieldCounts[i])) {
          char failmsg[100] = {0};
          ACE_OS::sprintf(failmsg, "Query verify failed for query index %d", i);
          ASSERT(false, failmsg);
          continue;
        }

        StructSetPtr ssptr =
            StructSetPtr(dynamic_cast<StructSet*>(results.ptr()));
        if ((ssptr) == NULLPTR) {
          LOG("Zero records were expected and found. Moving onto next. ");
          continue;
        }

        _verifyStructSet(ssptr, i);
      }
    }

    if (!doAnyErrorOccured) printf("HURRAY !! We PASSED \n\n");

    LOG("StepFive complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSix)
  {
    SLEEP(100);
    bool doAnyErrorOccured = false;
    QueryHelper* qh = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }

    for (int i = 0; i < QueryStrings::SSPsize(); i++) {
      if (i == 16) {
        LOGDEBUG("Skipping query index %d for pdx because it has function.", i);
        continue;
      }

      if (structsetParamQueries[i].category != unsupported) {
        QueryPtr qry =
            qs->newQuery(const_cast<char*>(structsetParamQueries[i].query()));
        CacheableVectorPtr paramList = CacheableVector::create();

        for (int j = 0; j < numSSQueryParam[i]; j++) {
          // LOGINFO("NIL::SSPQ::328: queryparamSetSS[%d][%d] = %s", i, j,
          // queryparamSetSS[i][j]);
          if (atoi(queryparamSetSS[i][j]) != 0) {
            paramList->push_back(
                Cacheable::create(atoi(queryparamSetSS[i][j])));
          } else {
            paramList->push_back(Cacheable::create(queryparamSetSS[i][j]));
          }
        }

        SelectResultsPtr results = qry->execute(paramList);
        if (!qh->verifySS(results, (qh->isExpectedRowsConstantSSPQ(i)
                                        ? structsetRowCountsPQ[i]
                                        : structsetRowCountsPQ[i] *
                                              qh->getPortfolioNumSets()),
                          structsetFieldCountsPQ[i])) {
          char failmsg[100] = {0};
          ACE_OS::sprintf(failmsg, "Query verify failed for query index %d", i);
          ASSERT(false, failmsg);
          continue;
        }

        StructSetPtr ssptr =
            StructSetPtr(dynamic_cast<StructSet*>(results.ptr()));
        if ((ssptr) == NULLPTR) {
          LOG("Zero records were expected and found. Moving onto next. ");
          continue;
        }
        _verifyStructSet(ssptr, i);
      }
    }
    if (!doAnyErrorOccured) printf("HURRAY !! We PASSED \n\n");
    LOG("StepSix complete.");
  }
END_TASK_DEFINITION

// test for getAll with complex objects after they have been deserialized
// on the server
DUNIT_TASK_DEFINITION(CLIENT1, GetAll)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(qRegionNames[0]);
    RegionPtr regPtr1 = regPtr0->getSubregion("Positions");
    RegionPtr regPtr2 = getHelper()->getRegion(qRegionNames[1]);
    RegionPtr regPtr3 = getHelper()->getRegion(qRegionNames[2]);
    RegionPtr regPtr4 = getHelper()->getRegion(qRegionNames[3]);

    // reset the counter for uniform population of position objects
    PositionPdx::resetCounter();

    int numSecIds = sizeof(secIds) / sizeof(char*);

    VectorOfCacheableKey posKeys;
    VectorOfCacheableKey portKeys;
    HashMapOfCacheable expectedPosMap;
    HashMapOfCacheable expectedPortMap;

    QueryHelper& qh = QueryHelper::getHelper();
    int setSize = qh.getPositionSetSize();
    int numSets = qh.getPositionNumSets();

    for (int set = 1; set <= numSets; ++set) {
      for (int current = 1; current <= setSize; ++current) {
        char posname[100] = {0};
        ACE_OS::sprintf(posname, "pos%d-%d", set, current);

        CacheableKeyPtr posKey(CacheableKey::create(posname));
        CacheablePtr pos = NULLPTR;
        pos = CacheablePtr(
            new PositionPdx(secIds[current % numSecIds], current * 100));

        posKeys.push_back(posKey);
        expectedPosMap.insert(posKey, pos);
      }
    }

    // reset the counter for uniform population of position objects
    PositionPdx::resetCounter();

    setSize = qh.getPortfolioSetSize();
    numSets = qh.getPortfolioNumSets();
    for (int set = 1; set <= numSets; ++set) {
      for (int current = 1; current <= setSize; ++current) {
        char portname[100] = {0};
        ACE_OS::sprintf(portname, "port%d-%d", set, current);

        CacheableKeyPtr portKey(CacheableKey::create(portname));
        CacheablePtr port = NULLPTR;
        port = CacheablePtr(new PortfolioPdx(current, 1));

        portKeys.push_back(portKey);
        expectedPortMap.insert(portKey, port);
      }
    }

    HashMapOfCacheablePtr resMap(new HashMapOfCacheable());
    HashMapOfExceptionPtr exMap(new HashMapOfException());

    // execute getAll for different regions and verify results
    regPtr0->getAll(portKeys, resMap, exMap);
    compareMaps(*resMap, expectedPortMap);
    ASSERT(exMap->size() == 0, "Expected no exceptions");
    resMap->clear();

    regPtr1->getAll(posKeys, resMap, exMap);
    compareMaps(*resMap, expectedPosMap);
    ASSERT(exMap->size() == 0, "Expected no exceptions");
    resMap->clear();

    regPtr2->getAll(posKeys, resMap, exMap);
    compareMaps(*resMap, expectedPosMap);
    ASSERT(exMap->size() == 0, "Expected no exceptions");
    resMap->clear();

    regPtr3->getAll(portKeys, resMap, exMap);
    compareMaps(*resMap, expectedPortMap);
    ASSERT(exMap->size() == 0, "Expected no exceptions");
    resMap->clear();

    regPtr4->getAll(portKeys, resMap, exMap);
    compareMaps(*resMap, expectedPortMap);
    ASSERT(exMap->size() == 0, "Expected no exceptions");
    resMap->clear();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, DoQuerySSError)
  {
    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();

    QueryServicePtr qs = NULLPTR;
    if (isPoolConfig) {
      PoolPtr pool1 = findPool(poolNames[0]);
      qs = pool1->getQueryService();
    } else {
      qs = getHelper()->cachePtr->getQueryService();
    }

    for (int i = 0; i < QueryStrings::SSsize(); i++) {
      if (structsetQueries[i].category == unsupported) {
        QueryPtr qry =
            qs->newQuery(const_cast<char*>(structsetQueries[i].query()));

        try {
          SelectResultsPtr results = qry->execute();

          char failmsg[100] = {0};
          ACE_OS::sprintf(failmsg, "Query exception didnt occur for index %d",
                          i);
          LOG(failmsg);
          FAIL(failmsg);
        } catch (gemfire::QueryException& ex) {
          // ok, expecting an exception, do nothing
          fprintf(stdout, "Got expected exception: %s", ex.getMessage());
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

DUNIT_MAIN
  {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepThree)
    CALL_TASK(StepFour)
    CALL_TASK(StepFive)
    CALL_TASK(StepSix)
    CALL_TASK(GetAll)
    CALL_TASK(DoQuerySSError)
    CALL_TASK(CloseCache1)
    CALL_TASK(CloseServer1)
    CALL_TASK(CloseLocator)
  }
END_MAIN
