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
#include <gfcpp/CqAttributesFactory.hpp>
#include <gfcpp/CqAttributes.hpp>
#include <gfcpp/CqListener.hpp>
#include <gfcpp/CqQuery.hpp>
#include <gfcpp/Struct.hpp>
#include <gfcpp/CqResults.hpp>
#define ROOT_NAME "TestThinClientCqWithIR"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

#include "QueryStrings.hpp"
#include "QueryHelper.hpp"

#include "Query.hpp"
#include "QueryService.hpp"

#include "ThinClientCQ.hpp"

using namespace gemfire;
using namespace test;
using namespace testData;

#define CLIENT1 s1p1
#define SERVER1 s2p1
#define CLIENT2 s1p2

bool isLocalServer = false;
static bool m_isPdx = false;
const char * endPoints = CacheHelper::getTcrEndpoints(isLocalServer, 1);
const char* cqName = "MyCq";

void initClientCq( const bool isthinClient )
{
  try {
    Serializable::registerType(Position::createDeserializable);
    Serializable::registerType(Portfolio::createDeserializable);

    Serializable::registerPdxType(PositionPdx::createDeserializable);
    Serializable::registerPdxType(PortfolioPdx::createDeserializable);
  }
  catch (const IllegalStateException& ) {
    // ignore exception
  }

  if ( cacheHelper == NULL ) {
    cacheHelper = new CacheHelper(isthinClient);
  }
  ASSERT( cacheHelper, "Failed to create a CacheHelper client instance." );
}
const char * regionNamesCq[] = { "Portfolios", "Positions", "Portfolios2", "Portfolios3" };


DUNIT_TASK_DEFINITION(SERVER1, CreateLocator)
{
  if ( isLocator )
    CacheHelper::initLocator( 1 );
    LOG("Locator1 started");
}
END_TASK_DEFINITION

void createServer(bool locator = false)
{
  LOG("Starting SERVER1...");
  if ( isLocalServer ) CacheHelper::initServer( 1, "remotequery.xml", locator?locatorsG:NULL );
  LOG("SERVER1 started");
}

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
{
  createServer(false);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_Locator)
{
  createServer(true);
}
END_TASK_DEFINITION

void stepOne(bool pool = false, bool locator = false)
{
  initClientCq(true);
  createRegionForCQ( pool, locator, regionNamesCq[0], USE_ACK, endPoints, true);
  createRegionForCQ( pool, locator, regionNamesCq[2], USE_ACK, endPoints, true);
  RegionPtr regptr = getHelper()->getRegion(regionNamesCq[0]);
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  RegionPtr subregPtr = regptr->createSubregion( regionNamesCq[1], lattribPtr );

  LOG( "StepOne complete." );
}

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
{
  stepOne();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_PoolEP)
{
  stepOne(true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_PoolLocator)
{
  stepOne(true, true);
}
END_TASK_DEFINITION

void stepOne2(bool pool = false, bool locator = false)
{
  initClientCq(true);
  createRegionForCQ( pool, locator, regionNamesCq[0], USE_ACK, endPoints, true);
  RegionPtr regptr = getHelper()->getRegion(regionNamesCq[0]);
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  RegionPtr subregPtr = regptr->createSubregion( regionNamesCq[1], lattribPtr );

  LOG( "StepOne2 complete." );
}

DUNIT_TASK_DEFINITION(CLIENT2, StepOne2)
{
  stepOne2();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepOne2_PoolEP)
{
  stepOne2(true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepOne2_PoolLocator)
{
  stepOne2(true, true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepTwo)
{
  RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
  RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);
  RegionPtr regPtr1 = getHelper()->getRegion(regionNamesCq[2]);

  QueryHelper * qh = &QueryHelper::getHelper();

  if(!m_isPdx)
  {
    qh->populatePortfolioData(regPtr0  , 30, 20, 20);
    qh->populatePortfolioData(regPtr1  , 30, 20, 20);
    qh->populatePositionData(subregPtr0, 30, 20);
  }else{
    qh->populatePortfolioPdxData(regPtr0  , 30, 20, 20);
    qh->populatePortfolioPdxData(regPtr1  , 30, 20, 20);
    qh->populatePositionPdxData(subregPtr0, 30, 20);
  }

  LOG( "StepTwo complete." );
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, StepThree2)
{
  RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
  RegionPtr subregPtr0 = regPtr0->getSubregion(regionNamesCq[1]);

  QueryHelper * qh = &QueryHelper::getHelper();
  LOGINFO("C2.StepThree2 m_isPdx = %d ", m_isPdx);
  if(!m_isPdx)
  {
    qh->populatePortfolioData(regPtr0  , 150, 40, 150);
    qh->populatePositionData(subregPtr0, 150, 40);
  }else{
    qh->populatePortfolioPdxData(regPtr0  , 150, 40, 150);
    qh->populatePositionPdxData(subregPtr0, 150, 40);
  }
  CacheablePtr port = NULLPTR;
  for(int i=1; i < 150; i++)
  {
    if(!m_isPdx)
    {
      port = CacheablePtr(new Portfolio(i, 150));
    }else{
        port = CacheablePtr(new PortfolioPdx(i, 150));
    }

      CacheableKeyPtr keyport = CacheableKey::create((char*)"port1-1");
      regPtr0->put(keyport, port);
      SLEEP(100); // sleep a while to allow server query to complete
  }

  LOG( "StepTwo2 complete." );
  SLEEP(15000); // sleep 0.25 min to allow server query to complete
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
{
  QueryHelper * qh = &QueryHelper::getHelper();

  // using region name as pool name
  PoolPtr pool = PoolManager::find(regionNamesCq[0]);
  QueryServicePtr qs;
  if (pool != NULLPTR) {
    qs = pool->getQueryService();
  } else {
    qs = getHelper()->cachePtr->getQueryService();
  }

  CqAttributesFactory cqFac;
  //CqListenerPtr cqLstner(new MyCqListener());
  //cqFac.addCqListener(cqLstner);
  CqAttributesPtr cqAttr = cqFac.create();

  //char* qryStr = (char*)"select * from /Portfolios p where p.ID != 2";
  //qry->execute();

  const char* qryStr  = "select * from /Portfolios where ID != 2";
  //const char* qryStr = "select * from /Portfolios p where p.ID < 3";
  //this will cause exception since distinct is not supported:
  //const char* qryStr  = "select distinct * from /Portfolios where ID != 2";
  CqQueryPtr qry = qs->newCq(cqName, qryStr, cqAttr);
  //QueryPtr qry = qs->newQuery(qryStr);

  CqResultsPtr results;
  try
  {
    LOG("before executing executeWithInitialResults.");
    results = qry->executeWithInitialResults();
    LOG("before executing executeWithInitialResults done.");
    //results = qry->execute();

    SelectResultsIterator iter = results->getIterator();
    char buf[100];
    int count = results->size();
    sprintf(buf, "results size=%d", count);
    LOG(buf);
    while( iter.hasNext())
    {
        count--;
        SerializablePtr ser = iter.next();
        /*PortfolioPtr portfolio( dynamic_cast<Portfolio*> (ser.ptr() ));
        PositionPtr  position(dynamic_cast<Position*>  (ser.ptr() ));

        if (portfolio != NULLPTR) {
          printf("   query pulled portfolio object ID %d, pkid %s\n",
              portfolio->getID(), portfolio->getPkid()->asChar());
        }

        else if (position != NULLPTR) {
          printf("   query  pulled position object secId %s, shares %d\n",
              position->getSecId()->asChar(), position->getSharesOutstanding());
        }
*/
        if (ser != NULLPTR) {
          printf (" query pulled object %s\n", ser->toString()->asChar());
          
          StructPtr stPtr(dynamic_cast<Struct*>  (ser.ptr() ));
          
          ASSERT( stPtr != NULLPTR, "Failed to get struct in CQ result." );
          
          if (stPtr != NULLPTR)
          {
            LOG(" got struct ptr ");
            SerializablePtr serKey = (*(stPtr.ptr()))["key"];
            ASSERT( serKey != NULLPTR, "Failed to get KEY in CQ result." );
            if (serKey != NULLPTR)
            {
              LOG("got struct key ");
              printf ("  got struct key %s\n", serKey->toString()->asChar());
            }
              
            SerializablePtr serVal = (*(stPtr.ptr()))["value"];
            
            ASSERT( serVal != NULLPTR, "Failed to get VALUE in CQ result." );
            
            if (serVal != NULLPTR)
            {
              LOG("got struct value ");
              printf ("  got struct value %s\n", serVal->toString()->asChar());
            }
          }
        }
        else {
          printf("   query pulled bad object\n");
        }

    }
    sprintf(buf, "results last count=%d", count);
    LOG(buf);
    
    qry = qs->newCq("MyCq2", "select * from /Portfolios2", cqAttr);
    
    LOG("before executing executeWithInitialResults2.");
    results = qry->executeWithInitialResults();
    LOG("before executing executeWithInitialResults2 done.");

    SelectResultsIterator iter2 = results->getIterator();

    count = results->size();
    sprintf(buf, "results2 size=%d", count);
    LOG(buf);
    while( iter2.hasNext())
    {
        count--;
        SerializablePtr ser = iter2.next();

        if (ser != NULLPTR) {
          printf (" query pulled object %s\n", ser->toString()->asChar());
          
          StructPtr stPtr(dynamic_cast<Struct*>  (ser.ptr() ));
          
          ASSERT( stPtr != NULLPTR, "Failed to get struct in CQ result." );
          
          if (stPtr != NULLPTR)
          {
            LOG(" got struct ptr ");
            SerializablePtr serKey = (*(stPtr.ptr()))["key"];
            ASSERT( serKey != NULLPTR, "Failed to get KEY in CQ result." );
            if (serKey != NULLPTR)
            {
              LOG("got struct key ");
              printf ("  got struct key %s\n", serKey->toString()->asChar());
            }
              
            SerializablePtr serVal = (*(stPtr.ptr()))["value"];
            
            ASSERT( serVal != NULLPTR, "Failed to get VALUE in CQ result." );
            
            if (serVal != NULLPTR)
            {
              LOG("got struct value ");
              printf ("  got struct value %s\n", serVal->toString()->asChar());
            }
          }
        }
        else {
          printf("   query pulled bad object\n");
        }

    }
    sprintf(buf, "results last count=%d", count);
    LOG(buf);

    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
    regPtr0->destroyRegion();
    SLEEP(20000);
    qry = qs->getCq(cqName);
    sprintf(buf, "cq[%s] should have been removed after close!", cqName);
    ASSERT(qry==NULLPTR, buf);
  }
  catch(const Exception& excp)
  {
    std::string logmsg = "";
    logmsg += excp.getName();
    logmsg += ": ";
    logmsg += excp.getMessage();
    LOG(logmsg.c_str());
    excp.printStackTrace();
  }

  LOG( "StepThree complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,CheckRegionDestroy)
{
  LOG("check region destory");
   try {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNamesCq[0]);
     if(regPtr0==NULLPTR)
        LOG("regPtr0==NULLPTR");
     else
     {
        LOG("regPtr0!=NULLPTR");
        ASSERT(regPtr0->isDestroyed(), "should have been distroyed");
     }
   } catch (...)
  {
     LOG("exception in getting region");
  }
  LOG("region has been destoryed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,CloseCache1)
{
  LOG("cleanProc 1...");
  cleanProc();
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2,CloseCache2)
{
  LOG("cleanProc 2...");
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1,CloseServer1)
{
  LOG("closing Server1...");
  if ( isLocalServer ) {
    CacheHelper::closeServer( 1 );
    LOG("SERVER1 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1,CloseLocator)
{
  if ( isLocator ) {
    CacheHelper::closeLocator( 1 );
    LOG("Locator1 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetPortfolioTypeToPdxC1)
{
    m_isPdx = true;
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, UnsetPortfolioTypeToPdxC1)
{
    m_isPdx = false;
}
END_TASK_DEFINITION
//
DUNIT_TASK_DEFINITION(CLIENT2, SetPortfolioTypeToPdxC2)
{
    m_isPdx = true;
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, UnsetPortfolioTypeToPdxC2)
{
    m_isPdx = false;
}
END_TASK_DEFINITION
//
void doThinClientCqIR( bool poolConfig = false, bool poolLocators = false )
{
  if (poolConfig && poolLocators) {
    CALL_TASK(CreateLocator);
    CALL_TASK(CreateServer1_Locator);
  } else {
    CALL_TASK(CreateServer1);
  }
  if (poolConfig) {
    if (poolLocators) {
      CALL_TASK(StepOne_PoolLocator);
      CALL_TASK(StepOne2_PoolLocator);
    } else {
      CALL_TASK(StepOne_PoolEP);
      CALL_TASK(StepOne2_PoolEP);
    }
  } else {
    CALL_TASK(StepOne);
    CALL_TASK(StepOne2);
  }
  CALL_TASK(StepTwo);
  CALL_TASK(StepThree2);
  CALL_TASK(StepThree);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);
  if (poolConfig && poolLocators) {
    CALL_TASK(CloseLocator);
  }
}

void setPortfolioPdxTypeC1(){
  CALL_TASK(SetPortfolioTypeToPdxC1)
}

void UnsetPortfolioTypeC1(){
  CALL_TASK(UnsetPortfolioTypeToPdxC1)
}
//
void setPortfolioPdxTypeC2(){
  CALL_TASK(SetPortfolioTypeToPdxC2)
}

void UnsetPortfolioTypeC2(){
  CALL_TASK(UnsetPortfolioTypeToPdxC2)
}

DUNIT_MAIN
{
  //doThinClientCqIR(); // normal case: pool == false, locators == false

  UnsetPortfolioTypeC1();
  UnsetPortfolioTypeC2();
  for(int runIdx = 1; runIdx <=2; ++runIdx)
  {
    doThinClientCqIR(true); // pool-with-endpoints case: pool == true, locators == false
    doThinClientCqIR(true, true); // pool-with-locator case: pool == true, locators == true

    setPortfolioPdxTypeC1();
    setPortfolioPdxTypeC2();
  }
}
END_MAIN

