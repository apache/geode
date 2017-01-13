/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* testThinClientPdxEnum.cpp
*
*/

#ifndef TEST_THIN_CLIENT_PDX_ENUM_HPP_
#define TEST_THIN_CLIENT_PDX_ENUM_HPP_

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include "testobject/NestedPdxObject.hpp"
#include "ThinClientHelper.hpp"
#include "QueryStrings.hpp"
#include "QueryHelper.hpp"
#include <gfcpp/Query.hpp>
#include <gfcpp/QueryService.hpp>

using namespace gemfire;
using namespace test;
using namespace testobject;

bool isLocalServer = false;


#define CLIENT1 s1p1
#define SERVER1 s2p1
static bool isLocator = false;

const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);

DUNIT_TASK_DEFINITION(CLIENT1, SetupClientPoolLoc)
  {
    LOG("Starting Step One with Pool + Locator lists");
    initClient(true);

    createPool("__TEST_POOL1__", locatorsG, NULL, 0, true);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, "__TEST_POOL1__");

    LOG("SetupClient complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateLocator1)
  {
    // starting locator
    CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserverPdxSerializer.xml");
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserverPdxSerializer.xml", locatorsG);
    }
    LOG("SERVER1 with locator started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putPdxWithEnum)
  {
    LOG("putPdxWithEnum started ");

    // Creating objects of type PdxEnumTestClass
    PdxEnumTestClassPtr pdxobj1(new PdxEnumTestClass(0));
    PdxEnumTestClassPtr pdxobj2(new PdxEnumTestClass(1));
    PdxEnumTestClassPtr pdxobj3(new PdxEnumTestClass(2));

    RegionPtr rptr = getHelper()->getRegion("DistRegionAck");

    // PUT Operations
    rptr->put(CacheableInt32::create(0), pdxobj1);
    LOG("pdxPut 1 completed ");

    rptr->put(CacheableInt32::create(1), pdxobj2);
    LOG("pdxPut 2 completed ");

    rptr->put(CacheableInt32::create(2), pdxobj3);
    LOG("pdxPut 3 completed ");

    LOG("putPdxWithEnum complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, pdxEnumQuery)
  {
    LOG("pdxEnumQuery started ");

    try {
      Serializable::registerPdxType(PdxEnumTestClass::createDeserializable);
      LOG("PdxEnumTestClass Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("PdxEnumTestClass IllegalStateException");
    }

    RegionPtr rptr = getHelper()->getRegion("DistRegionAck");
    SelectResultsPtr results = rptr->query("m_enumid.name = 'id2'");
    ASSERT(results->size() == 1, "query result should have one item");
    ResultSetPtr rsptr = dynCast<ResultSetPtr>(results);
    SelectResultsIterator iter = rsptr->getIterator();
    while (iter.moveNext()) {
      PdxEnumTestClassPtr re = dynCast<PdxEnumTestClassPtr>(iter.current());
      ASSERT(re->getID() == 1, "query should have return id 1");
    }

    QueryHelper* qh ATTR_UNUSED = &QueryHelper::getHelper();
    QueryServicePtr qs = NULLPTR;
    PoolPtr pool1 = findPool("__TEST_POOL1__");
    qs = pool1->getQueryService();
    QueryPtr qry = qs->newQuery(
        "select distinct * from /DistRegionAck this where m_enumid.name = "
        "'id3'");
    results = qry->execute();
    rsptr = dynCast<ResultSetPtr>(results);
    SelectResultsIterator iter1 = rsptr->getIterator();
    while (iter1.moveNext()) {
      PdxEnumTestClassPtr re = dynCast<PdxEnumTestClassPtr>(iter1.current());
      ASSERT(re->getID() == 2, "query should have return id 0");
    }

    LOG("pdxEnumQuery complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER1 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseLocator1)
  {
    // stop locator
    CacheHelper::closeLocator(1);
    LOG("Locator1 stopped");
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1)
    CALL_TASK(CreateServer1_With_Locator)

    CALL_TASK(SetupClientPoolLoc)
    CALL_TASK(putPdxWithEnum)
    CALL_TASK(pdxEnumQuery)

    CALL_TASK(CloseCache1)
    CALL_TASK(CloseServer1)

    CALL_TASK(CloseLocator1)
  }
END_MAIN

#endif /* TEST_THIN_CLIENT_PDX_ENUM_HPP_ */
