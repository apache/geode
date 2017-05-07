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
 * @author sumedh
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
const char * poolNames[] = { "Pool1","Pool2","Pool3" };
const char * locHostPort = CacheHelper::getLocatorHostPort( isLocator, 1 );
const char* endPoints = CacheHelper::getTcrEndpoints(isLocalServer, 2);
char* endPoint1 = NULL;
const char* endPoint2 = NULL;
const char* qRegionNames[] = { "Portfolios", "Positions" };
const char * sGNames[] = { "ServerGroup1","ServerGroup2"};

void initClient()
{
  try {
    Serializable::registerType(Position::createDeserializable);
    Serializable::registerType(Portfolio::createDeserializable);
  }
  catch (const IllegalStateException& ) {
    // ignore exception
  }
  initClient(true);
  ASSERT(getHelper() != NULL, "null CacheHelper");
  endPoint2 = strstr(endPoints, ",");
  endPoint1 = ACE_OS::strdup(endPoints);
  endPoint1[endPoint2 - endPoints] = '\0';
  ++endPoint2;
  printf("Endpoint 1: %s, Endpoint 2: %s\n", endPoint1, endPoint2);
}

void stepOne(bool pool = false, bool locator = false )
{
  initClient();
  PoolPtr pool1 = NULLPTR;
  if(!pool){
    createRegion(qRegionNames[0], USE_ACK, endPoint1, true);
  }else if(locator){ //connecting to SG1 ( just S1 )
    pool1 = createPool(poolNames[0], locHostPort, sGNames[0],NULL, 0, true );
    createRegionAndAttachPool( qRegionNames[0], USE_ACK, poolNames[0]);
  }else {
    pool1 = createPool(poolNames[0], NULL, NULL,endPoint1, 0, true );
    createRegionAndAttachPool( qRegionNames[0], USE_ACK, poolNames[0]);
  }

  // populate the region
  RegionPtr reg = getHelper()->getRegion(qRegionNames[0]);
  QueryHelper& qh = QueryHelper::getHelper();
  qh.populatePortfolioData(reg, qh.getPortfolioSetSize(),
      qh.getPortfolioNumSets());

  std::string qry1Str = (std::string)"select * from /" + qRegionNames[0];
  std::string qry2Str = (std::string)"select * from /" + qRegionNames[1];

  QueryServicePtr qs = NULLPTR;
  if(pool){
    qs = pool1->getQueryService();
  }else{
    qs = getHelper()->cachePtr->getQueryService();
  }

  SelectResultsPtr results;
  QueryPtr qry = qs->newQuery(qry1Str.c_str());
  results = qry->execute();
  ASSERT(results->size() == qh.getPortfolioSetSize() * qh.getPortfolioNumSets(),
      "unexpected number of results");
  try {
    qry = qs->newQuery(qry2Str.c_str());
    results = qry->execute();
    FAIL("Expected a QueryException");
  }
  catch (const QueryException& ex) {
    printf("Good expected exception: %s\n", ex.getMessage());
  }

  // now region queries
  results = reg->query(qry1Str.c_str());
  ASSERT(results->size() == qh.getPortfolioSetSize() * qh.getPortfolioNumSets(),
      "unexpected number of results");
  try {
    results = reg->query(qry2Str.c_str());
    FAIL("Expected a QueryException");
  }
  catch (const QueryException& ex) {
    printf("Good expected exception: %s\n", ex.getMessage());
  }

  LOG( "StepOne complete." );
}

void stepTwo(bool pool = false, bool locator = false)
{
  //Create pool2
  PoolPtr pool2 = NULLPTR;

  if(!pool){
    createRegion(qRegionNames[1], USE_ACK, endPoint2, true);
  }else if(locator){ //connecting to SG2 ( just S2 )
    pool2 = createPool(poolNames[1], locHostPort, sGNames[1],NULL, 0, true );
    createRegionAndAttachPool( qRegionNames[1], USE_ACK, poolNames[1]);
  }else {
    pool2 = createPool(poolNames[1], NULL, NULL,endPoint2, 0, true );
    createRegionAndAttachPool( qRegionNames[1], USE_ACK, poolNames[1]);
  }

  // populate the region
  RegionPtr reg = getHelper()->getRegion(qRegionNames[1]);
  QueryHelper& qh = QueryHelper::getHelper();
  qh.populatePositionData(reg, qh.getPositionSetSize(),
      qh.getPositionNumSets());

  std::string qry1Str = (std::string)"select * from /" + qRegionNames[0];
  std::string qry2Str = (std::string)"select * from /" + qRegionNames[1];

  QueryServicePtr qs = NULLPTR;
  if(pool){
    qs = pool2->getQueryService();
  }else{
    qs = getHelper()->cachePtr->getQueryService();
  }
  SelectResultsPtr results;
  QueryPtr qry;

  // VJR: If Pool enabled, we do not support cache level query service  
  if (!pool) {
    qry = qs->newQuery(qry1Str.c_str());
    results = qry->execute();
    ASSERT(results->size() == qh.getPortfolioSetSize() * qh.getPortfolioNumSets(),
      "unexpected number of results");
      
    try {
      qry = qs->newQuery(qry2Str.c_str());
      results = qry->execute();
      FAIL("Expected a QueryException");
    }
    catch (const QueryException& ex) {
      printf("Good expected exception: %s\n", ex.getMessage());
    }
  }
  // now region queries
  try {
    results = reg->query(qry1Str.c_str());
    FAIL("Expected a QueryException");
  }
  catch (const QueryException& ex) {
    printf("Good expected exception: %s\n", ex.getMessage());
  }
  results = reg->query(qry2Str.c_str());
  ASSERT(results->size() == qh.getPositionSetSize() * qh.getPositionNumSets(),
      "unexpected number of results");

  LOG( "StepTwo complete." );
}

DUNIT_TASK_DEFINITION(LOCATOR, StartLocator)
{
  //starting locator 1 2
  if ( isLocator ) {
    CacheHelper::initLocator( 1 );
  }
  LOG("Locator started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
{
  // start server1 having regions "Portfolios"
  LOG("Starting SERVER1...");
  if (isLocalServer) {
    CacheHelper::initServer(1, "regionquery_diffconfig.xml");
  }
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1WithLocator)
{
  LOG("Starting SERVER1...");

  if ( isLocalServer ) CacheHelper::initServer( 1, "regionquery_diffconfig_SG.xml",locHostPort );

  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
{
  LOG("Starting Step One");
  stepOne();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolEP)
{
  LOG("Starting Step One with Pool + Explicit server list");
  stepOne(true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc)
{
  LOG("Starting Step One with Pool + Locator lists");
  stepOne(true, true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
{
  // start server2 having regions "Positions"
  LOG("Starting SERVER2...");
  if (isLocalServer) {
    CacheHelper::initServer(2, "regionquery_diffconfig2.xml");
  }
  LOG("SERVER2 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2WithLocator)
{
  LOG("Starting SERVER2...");

  if ( isLocalServer ){
    CacheHelper::initServer( 2, "regionquery_diffconfig2_SG.xml",locHostPort );
  }
  LOG("SERVER2 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepTwo)
{
  LOG("Starting Step Two in old way");
  stepTwo();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepTwoPoolEP)
{
  LOG("Starting Step Two with Pool + Explicit server list");
  stepTwo(true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepTwoPoolLoc)
{
  LOG("Starting Step Two with Pool + Locator list");
  stepTwo(true, true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
{
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

DUNIT_TASK_DEFINITION(SERVER2, CloseServer2)
{
  LOG("closing Server2...");
  if (isLocalServer) {
    CacheHelper::closeServer(2);
    LOG("SERVER2 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATOR,CloseLocator)
{
  if ( isLocator ) {
    CacheHelper::closeLocator( 1 );
    LOG("Locator1 stopped");
  }
}
END_TASK_DEFINITION

void runRegionQueryDiffServTest(bool poolConfig = false, bool withLocators = false)
{
  if(!poolConfig){
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
    CALL_TASK(CreateServer2)
    CALL_TASK(StepTwo)
  }else if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServer1WithLocator)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(CreateServer2WithLocator)
    CALL_TASK(StepTwoPoolLoc)
  }else{
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(CreateServer2)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseServer1)
  CALL_TASK(CloseServer2)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}

DUNIT_MAIN
{
  //Basic Old Test
  runRegionQueryDiffServTest();

  //New Test with Pool + EP
  runRegionQueryDiffServTest(true);

  //New Test with Pool + Locators
  runRegionQueryDiffServTest(true,true);
}
END_MAIN
