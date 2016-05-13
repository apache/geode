/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    QueryTest.cpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#include "QueryTest.hpp"
#include "fwklib/QueryHelper.hpp"

#include <ace/Time_Value.h>
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/PaceMeter.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/RegionHelper.hpp"
#include "fwklib/PoolHelper.hpp"
#include "testobject/PortfolioPdx.hpp"
#include "fwklib/FwkExport.hpp"
#include <vector>
#define QUERY_RESPONSE_TIMEOUT 600

namespace FwkQuery {
  std::string REGIONSBB( "Regions" );
  std::string CLIENTSBB( "ClientsBb" );
  std::string READYCLIENTS( "ReadyClients" );
}

using namespace FwkQuery;
using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::query;
using namespace testobject;
using namespace testData;


static CacheableHashMapPtr RegionSnapShot = NULLPTR;
static CacheableHashSetPtr destroyedKey = NULLPTR;
static bool isSerialExecution = false;
static QueryTest * g_test = NULL;

// ----------------------------------------------------------------------------

TESTTASK initialize( const char * initArgs ) {
  int32_t result = FWK_SUCCESS;
  if ( g_test == NULL ) {
    FWKINFO( "Initializing QueryTest library." );
    try {
      g_test = new QueryTest( initArgs );
    } catch( const FwkException &ex ) {
      FWKSEVERE( "initialize: caught exception: " << ex.getMessage() );
      result = FWK_SEVERE;
    }
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Finalizing QueryTest library." );
  if ( g_test != NULL ) {
    g_test->cacheFinalize();
    delete g_test;
    g_test = NULL;
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doCloseCache() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Closing cache, disconnecting from distributed system." );
  if ( g_test != NULL ) {
    g_test->cacheFinalize();
  }
  return result;
}

// ----------------------------------------------------------------------------


void QueryTest::checkTest( const char * taskId ) {
  SpinLockGuard guard( m_lck );
  setTask( taskId );
  if (m_cache == NULLPTR) {
    PropertiesPtr pp = Properties::create();
    bool isDC = getBoolValue("isDurable");
    if(isDC) {
      int32_t timeout = getIntValue("durableTimeout");
      bool isFeeder = getBoolValue( "isFeeder");
      std::string durableId;

      if(isFeeder) {
        durableId =  std::string("Feeder");
      }
      else {
        char name[32] = {'\0'};
        ACE_OS::sprintf(name,"ClientName_%d",getClientId());
        durableId =  std::string(name);
      }

      FWKINFO( "checktest durableID = " << durableId);
      pp->insert( "durable-client-id", durableId.c_str() );
      if(timeout > 0)
      pp->insert( "durable-timeout", timeout );
   }

   //CacheAttributesPtr cAttrs = NULLPTR;
   setCacheLevelEp();
   cacheInitialize( pp );
   bool isTimeOutInMillis = m_cache->getDistributedSystem()->getSystemProperties()->readTimeoutUnitInMillis();
   if(isTimeOutInMillis){
     #define QUERY_RESPONSE_TIMEOUT 600*1000
   }
   // QueryTest specific initialization
  // none
}
}
// ----------------------------------------------------------------------------
TESTTASK doCreateUserDefineRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateUserDefineRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->createUserDefineRegion();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateUserDefineRegion caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doRegisterCq( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterCq called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->registerCQ();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterCq caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doRegisterCqForConc( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterCq called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->registerCQForConc();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterCq caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doValidateCq( const char * taskId ){
   int32_t result = FWK_SUCCESS;
   FWKINFO( "doValidateCq called for task: " << taskId );
   try {
	 g_test->checkTest( taskId );
	 result = g_test->validateCq();
   } catch ( FwkException ex ) {
	 result = FWK_SEVERE;
      FWKSEVERE( "doValidateCq caught exception: " << ex.getMessage() );
   }
   return result;
}

TESTTASK doValidateEvents( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doValidateEvents called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->validateEvents();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doValidateEvents caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doVerifyCQListenerInvoked( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyCQListenerInvoked called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->verifyCQListenerInvoked();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyCQListenerInvoked caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doVerifyCqDestroyed( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyCqDestroyed called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->verifyCqDestroyed();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyCqDestroyed caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doCqState( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCqState called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->cqState();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCqState caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doCQOperation( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCQOperation called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->cqOperations();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCQOperation caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doPopulateUserObject( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopulateUserObject called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->populateUserObject();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopulateUserObject caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doPopulatePortfolioObject( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopulatePortfolioObject called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->populatePortfolioObject();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopulatePortfolioObject caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doPopulateRangePositions( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopulateRangePortfolioObject called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->populateRangePositionObjects();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopulateRangePortfolioObject caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doGetAndComparePositionObjects( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGetAndComparePortfolioObject called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->getAndComparePositionObjects();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGetAndComparePortfolioObject caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doUpdateRangePositions( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doUpdateRangePositions called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->updateRangePositions();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doUpdateRangePositions caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doVerifyAllPositionObjects( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyAllPositionObjects called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->verifyAllPositionObjects();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyAllPositionObjects caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doDestroyUserObject( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doDestroyUserObject called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->destroyUserObject();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doDestroyUserObject caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doInvalidateUserObject( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doInvalidateUserObject called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->invalidateUserObject();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doInvalidateUserObject caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doGetObject( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGetObject called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->getObject();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGetObject caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doRunQuery( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRunQuery called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->runQuery();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRunQuery caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doRunQueryWithPayloadAndEntries( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRunQueryWithPayloadAndEntries called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doRunQueryWithPayloadAndEntries();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRunQueryWithPayloadAndEntries caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doAddRootAndSubRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doAddRootAndSubRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->addRootAndSubRegion();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doAddRootAndSubRegion caught exception: " << ex.getMessage() );
  }
  return  result;
}

TESTTASK doCloseCacheAndReInitialize(const char * taskId) {
  int32_t result = FWK_SUCCESS;

  FWKINFO( "doCloseCacheAndReInitialize called for task: " << taskId );

  try {
    g_test->setTask( taskId );
    result = g_test->closeNormalAndRestart(taskId);
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCloseCacheAndReInitialize caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doRegisterAllKeys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterAllKeys called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->registerAllKeys();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterAllKeys caught exception: " << ex.getMessage() );
  }
  return  result;
}

// ----------------------------------------------------------------------------

int32_t QueryTest::createUserDefineRegion()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::createUserDefineRegion()" );
  try {
    createPool();
    RegionHelper help( g_test );
    if(!m_isObjectRegistered ){
      Serializable::registerType( Position::createDeserializable);
      Serializable::registerType( Portfolio::createDeserializable);
      Serializable::registerPdxType(testobject::PositionPdx::createDeserializable);
      Serializable::registerPdxType(testobject::PortfolioPdx::createDeserializable);
      Serializable::registerPdxType(AutoPdxTests::PositionPdx::createDeserializable);
      Serializable::registerPdxType(AutoPdxTests::PortfolioPdx::createDeserializable);
      m_isObjectRegistered = true;
    }

    isSerialExecution=getBoolValue("serialExecution");
    if (isSerialExecution)
      RegionSnapShot = CacheableHashMap::create();
    RegionPtr region;
    std::string regionName = getStringValue( "regionName" );
    if(regionName.c_str() == NULL) {
      region = help.createRootRegion( m_cache);
    } else {
      region = help.createRootRegion( m_cache, regionName.c_str());
    }
    std::string key( region->getName() );
    bbIncrement( REGIONSBB, key );
    FWKINFO( "QueryTest::createRegion Created region " << region->getName() << std::endl);
    
    result = FWK_SUCCESS;

  } catch ( Exception e ) {
    FWKEXCEPTION( "QueryTest::createRegion FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "QueryTest::createRegion FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::createRegion FAILED -- caught unknown exception." );
  }

  return result;
}
int32_t QueryTest::registerCQ()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::registerCQ()" );

  try {
    static int cqNum = 0;
    std::string qryStr = getStringValue( "query" ); // set the query string in xml
    char name[32] = {'\0'};
    sprintf(name,"%d",g_test->getClientId());
    std::string key = std::string( "CQLISTENER_") + std::string(name);
    while(!qryStr.empty()) {
      QueryServicePtr qs=checkQueryService();
      CqAttributesFactory cqFac;
      CqListenerPtr cqLstner(new MyCqListener());
      cqFac.addCqListener(cqLstner);
      CqAttributesPtr cqAttr = cqFac.create();
      char cqName[100];
      sprintf(cqName,"cq-%d",cqNum++);
      FWKINFO("Reading query from xml : " << qryStr << " for cq " << cqName);
      CqQueryPtr qry = qs->newCq(cqName, qryStr.c_str(), cqAttr);
      SelectResultsPtr results = qry->executeWithInitialResults(QUERY_RESPONSE_TIMEOUT);
      FWKINFO("Result size is "<<results->size());
      if(results == NULLPTR)
        FWKEXCEPTION("For cq with name "<<qry->getName()<<" executeWithInitialResults returned "<<results->size());
      qryStr = getStringValue( "query" ); // set the query string in xml
      bbIncrement( "CQListenerBB", key );
      result = FWK_SUCCESS;
   }
  } catch ( Exception e ) {
    FWKEXCEPTION( "QueryTest::registerCQ FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "QueryTest::registerCQ FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::registerCQ FAILED -- caught unknown exception." );
  }
  return result;
}
int32_t QueryTest::registerCQForConc()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::registerCQForConc()" );
  try {
    std::string qryStr = getStringValue( "query" ); // set the query string in xml
    char name[32] = {'\0'};
    char n[32] = {'\0'};
    sprintf(name,"Client_%d",g_test->getClientId());
    std::string key = std::string( "CQLISTENER_") + std::string(name);
    resetValue("isDurableC");
    bool isdurable = getBoolValue("isDurableC");
    while(!qryStr.empty()){
       QueryServicePtr qs=checkQueryService();
       CqAttributesFactory cqFac;
       CqListenerPtr cqLstner(new MyCqListener());
       cqFac.addCqListener(cqLstner);
       CqAttributesPtr cqAttr = cqFac.create();
       FWKINFO("Reading query from xml : " << qryStr);
       CqQueryPtr qry = NULLPTR;
       FWKINFO("Client Id is "<< name);
       qry = qs->newCq(qryStr.c_str(), cqAttr,isdurable);
       bool regAndExeCq = getBoolValue("registerAndExecuteCQs");
       bool isExecuteIR = getBoolValue("executeWithIR");
       if(regAndExeCq)
         qry->execute();
       if(isExecuteIR)
         qry->executeWithInitialResults(QUERY_RESPONSE_TIMEOUT);
       qryStr = getStringValue( "query" ); // set the query string in xml
       bbIncrement( "CQListenerBB", key );
       result = FWK_SUCCESS;
    }
  } catch ( Exception e ) {
     FWKEXCEPTION( "QueryTest::registerCQForConc FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
     FWKEXCEPTION( "QueryTest::registerCQForConc FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::registerCQForConc FAILED -- caught unknown exception." );
  }
  return result;
}

int32_t QueryTest::validateCq()
{
   int32_t result = FWK_SEVERE;
   FWKINFO( "In QueryTest::validateCq()" );
   try {
	 QueryServicePtr qs=checkQueryService();
	 uint8_t i=0;
         char buf[1024];
         resetValue("isDurableC");
         bool isdurable = getBoolValue("isDurableC");
	 VectorOfCqQuery vec;
	 qs->getCqs(vec);
         FWKINFO("Total number of Cqs registered on this client is "<< vec.size());
	 CqServiceStatisticsPtr serviceStats = qs->getCqServiceStatistics();
         sprintf(buf, "numCqsActive=%d, numCqsCreated=%d, numCqsClosed=%d,numCqsStopped=%d, numCqsOnClient=%d", serviceStats->numCqsActive(), serviceStats->numCqsCreated(),
            serviceStats->numCqsClosed(), serviceStats->numCqsStopped(),
            serviceStats->numCqsOnClient());
         FWKINFO(buf);
	 CacheableArrayListPtr durableCqListPtr = qs->getAllDurableCqsFromServer();
         int actualCqs=durableCqListPtr->length();
         int expectedCqs=serviceStats->numCqsCreated() - serviceStats->numCqsClosed();
         
	 if(isdurable)
	 {
           if(expectedCqs == actualCqs)
	   {
             FWKINFO(" No of durableCqs on DC client is "<< actualCqs<< " which is equal to the number of expected Cqs "<<expectedCqs);
           }
           else
             FWKEXCEPTION("No of durableCqs on DC client is "<< actualCqs << " is not equal to the number of expected Cqs "<<expectedCqs);
           result=FWK_SUCCESS;
	 }
	 else
         {
           if(durableCqListPtr->length()!= 0)
	   {
             FWKEXCEPTION("Client is not durable hence durableCqs for this client should have, had been 0,but it is"<< durableCqListPtr->length());
           }
           else
             FWKINFO("durableCqs for NDC client is "<< durableCqListPtr->length());
           result=FWK_SUCCESS;
         }
         result=FWK_SUCCESS;
   }catch ( Exception e ) {
	    FWKEXCEPTION( "QueryTest::validateCq() FAILED -- caught exception: " << e.getMessage() );
	  } catch ( FwkException& e ) {
	    FWKEXCEPTION( "QueryTest::validateCq() FAILED -- caught test exception: " << e.getMessage() );
	  } catch ( ... ) {
	    FWKEXCEPTION( "QueryTest::validateCq() FAILED -- caught unknown exception." );
	  }
	  return result;
}

int32_t QueryTest::verifyCQListenerInvoked()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::verifyCQListenerInvoked()" );

  try {
    QueryServicePtr qs=checkQueryService();
    char buf[1024];
    
    RegionPtr region = getRegionPtr();
    FWKINFO("Region Size is"<< region->size());
    uint8_t i=0;
    VectorOfCqQuery vec;
    qs->getCqs(vec);
    FWKINFO("number of cqs for verification "<< vec.size());
    resetValue( "distinctKeys" );
    int32_t numOfKeys = getIntValue( "distinctKeys");
    for(i=0; i < vec.size(); i++)
    {
      CqQueryPtr cqy = vec.at(i);
      CqStatisticsPtr cqStats = cqy->getStatistics();
      CqAttributesPtr cqAttr = cqy->getCqAttributes();
      VectorOfCqListener vl;
      cqAttr->getCqListeners(vl);
      sprintf(buf, "number of listeners for cq[%s] is %d", cqy->getName(), vl.size());
      FWKINFO(buf);
      MyCqListener*  ml =  dynamic_cast<MyCqListener*>(vl[0].ptr());
      sprintf(buf, "MyCount for cq[%s] Listener: numInserts[%d], numDeletes[%d], numUpdates[%d], numInvalidates[%d], numEvents[%d]",
             cqy->getName(), ml->getNumInserts(), ml->getNumDeletes(), ml->getNumUpdates(),ml->getNumInvalidates(), ml->getNumEvents());
      FWKINFO(buf);
      sprintf(buf, "cq[%s] From CqStatistics : numInserts[%d], numDeletes[%d], numUpdates[%d], numEvents[%d]",
             cqy->getName(), cqStats->numInserts(), cqStats->numDeletes(), cqStats->numUpdates(), cqStats->numEvents());
      std::string cqName=cqy->getName();
      bbSet("CQLISTNERBB","CQ",ml->getNumEvents());
      resetValue("checkEvents");
      bool isCheckEvent=getBoolValue("checkEvents");
      if(isCheckEvent)
      {
        int32_t createCnt=bbGet("OpsBB","CREATE");
        int32_t updateCnt=bbGet("OpsBB","UPDATE");
        int32_t destCnt=bbGet("OpsBB","DESTROY");
        int32_t invalCnt=bbGet("OpsBB","INVALIDATE");
        int32_t totalEvent=createCnt+updateCnt+destCnt+invalCnt;
        
        resetValue("invalidate");
        bool isInvalidate=getBoolValue("invalidate");
        resetValue("stopped");
        bool isStoped=getBoolValue("stopped");
        resetValue("execute");
        bool isExecute=getBoolValue("execute");
        
        FWKINFO("BB cnt are create = "<< createCnt << " update ="<<updateCnt<<" destroy = "<<destCnt<<" invalidate = "<<invalCnt << " TotalEvent = "<<totalEvent);
        if(!cqy->isStopped() && isExecute )
        {
          if(isInvalidate)
          {
            if(ml->getNumEvents()==totalEvent)
              result = FWK_SUCCESS;
          else
            FWKEXCEPTION("Total event count incorrect");
          }  
          else{
           if(ml->getNumInserts()==createCnt && ml->getNumUpdates()== updateCnt && ml->getNumDeletes()== destCnt && ml->getNumInvalidates()==invalCnt && ml->getNumEvents()==totalEvent )
             result = FWK_SUCCESS;
           else
             FWKEXCEPTION(" accumulative event count incorrect");
          }
        }  
        else if(isStoped)
        {
          if(cqStats->numEvents()== 0)
            result = FWK_SUCCESS;
          else
            FWKEXCEPTION("Cq is stopped before entry operation,hence events should not have been received.");
        }
      }
      else
      {
        //if(ml->getNumEvents()>0)
        {
          if(ml->getNumInserts()==cqStats->numInserts() && ml->getNumUpdates()== cqStats->numUpdates() && ml->getNumDeletes()== cqStats->numDeletes() && ml->getNumEvents()==cqStats->numEvents() )
            result = FWK_SUCCESS;
          else
           FWKEXCEPTION(" accumulative event count incorrect");
        }
        //else
          //FWKEXCEPTION("The listener should have processed some events:");
      }
    }
  } catch ( Exception e ) {
    FWKEXCEPTION( "QueryTest::verifyCQListenerInvoked() FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "QueryTest::verifyCQListenerInvoked() FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::verifyCQListenerInvoked() FAILED -- caught unknown exception." );
  }
  return result;
}


int32_t QueryTest::validateEvents()
{
  int32_t result=FWK_SEVERE;
  FWKINFO("Inside validateEvents()");
  try{
    result=verifyCQListenerInvoked();
    resetValue("distinctKeys");
    resetValue("NumNewKeys" );
    int32_t numKeys=getIntValue("distinctKeys");
    int32_t numNewKeys=getIntValue("NumNewKeys");
    FWKINFO("NUMKEYS = "<<numKeys);
    int32_t clntCnt=getIntValue("clientCount");
    int32_t totalEvents = 0;
    int32_t listnEvent = 0;
    int32_t numDestroyed = (int32_t)bbGet("ImageBB", "Last_Destroy") - (int32_t)bbGet("ImageBB", "First_Destroy") + 1;
    int32_t numInvalide = (int32_t)bbGet("ImageBB", "Last_Invalidate") - (int32_t)bbGet("ImageBB", "First_Invalidate") + 1;
    int32_t updateCount = (int32_t)bbGet("ImageBB", "Last_UpdateExistingKey") - (int32_t)bbGet("ImageBB", "First_UpdateExistingKey") + 1;    
    FWKINFO("ImageBB values are numDestroyed = "<<numDestroyed<<" numInvalide = "<<numInvalide<<" updateCount= "<<updateCount);

     //As CqListener is not invoked when events like "get,localDEstroy and localInvalidate" happen,hence not adding those  events count for correct validation.
     // totalEvents = (numKeys - numDestroyed + numNewKeys) - (numInvalide) - (localDestroy) - (localInvalidate);
      totalEvents = numNewKeys + numDestroyed + numInvalide + updateCount;
      FWKINFO("TOTALEVENTS = "<<totalEvents);
      listnEvent = bbGet("CQLISTNERBB","CQ"); 
    if(listnEvent == totalEvents)
    { 
      FWKINFO("ListenerEvents and RegionEvents are equal i.e ListenerEvents = "<< listnEvent << " RegionEvents = "<<totalEvents  );
      result=FWK_SUCCESS;
    }
    else
      FWKEXCEPTION("Events mismatch Listner event = "<< listnEvent << " and entry event = "<< totalEvents);
  }
  catch(Exception e)
  {FWKEXCEPTION("QueryTest::validateEvents() FAILED caught exception " << e.getMessage()); }
  return result;
}

int32_t QueryTest::verifyCqDestroyed()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::verifyCqDestroyed()" );

  try {
     QueryServicePtr qs = checkQueryService();
     VectorOfCqQuery vec;
     qs->getCqs(vec);
     if(vec.size() != 0){
       FWKEXCEPTION( "cqs should have been removed after region destroyed! " );
     } else {
       result = FWK_SUCCESS;
     }
   } catch ( Exception &e ) {
     FWKEXCEPTION( "QueryTest::verifyCqDestroyed() FAILED -- caught exception: " << e.getMessage() );
   } catch ( FwkException& e ) {
     FWKEXCEPTION( "QueryTest::verifyCqDestroyed() FAILED -- caught test exception: " << e.getMessage() );
   } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::verifyCqDestroyed() FAILED -- caught unknown exception." );
  }
  return result;

}
// ----------------------------------------------------------------------------
int32_t QueryTest::getObject()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::getObject" );

  try {
    RegionPtr region = getRegionPtr();
    resetValue( "distinctKeys" );
    int32_t numOfKeys = getIntValue( "distinctKeys");

    std::string objectType = getStringValue( "objectType" );
    QueryHelper *qh = &QueryHelper::getHelper();
    int setSize = qh->getPortfolioSetSize();
    if(numOfKeys < setSize){
      setSize = numOfKeys;
    }
    int numSets = numOfKeys/setSize;

    CacheablePtr valuePtr;
    CacheableKeyPtr keypos;
  for(int set=1; set<=numSets; set++)
  {
    for(int current=1; current<=setSize; current++)
    {
      char posname[100] = {0};
      if( objectType == "Portfolio" || objectType == "PortfolioPdx")
        ACE_OS::sprintf(posname, "port%d-%d", set,current);
      else if(objectType == "Position" || objectType == "PositionPdx")
        ACE_OS::sprintf(posname, "pos%d-%d", set,current);
      keypos = CacheableKey::create(posname);
      valuePtr = region->get(keypos);
      if (valuePtr != NULLPTR) {
        result = FWK_SUCCESS;
      }
    }
   }
  } catch ( Exception e ) {
    FWKEXCEPTION( "QueryTest::getObject Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "QueryTest::getObject Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::getObject Caught unknown exception." );
  }
  FWKINFO( "QueryTest::getObject complete." );
  return result;
}
// ----------------------------------------------------------------------------
SelectResultsPtr QueryTest::remoteQuery(const QueryServicePtr qs,const char * querystr)
{
  ACE_Time_Value startTime, endTime;
  QueryPtr qry = qs->newQuery(querystr);
  startTime = ACE_OS::gettimeofday();
  SelectResultsPtr results = qry->execute(QUERY_RESPONSE_TIMEOUT);
  endTime = ACE_OS::gettimeofday() - startTime;
  FWKINFO(" Time Taken to execute the query : "<< querystr << ": is " <<  endTime.sec() << "." << endTime.usec() << " sec");
  return results;
}

SelectResultsPtr QueryTest::continuousQuery(const QueryServicePtr qs,const char * queryStr,int cqNum)
{
  ACE_Time_Value startTime, endTime;
  CqAttributesFactory cqFac;
  CqListenerPtr cqLstner(new MyCqListener());
  cqFac.addCqListener(cqLstner);
  CqAttributesPtr cqAttr = cqFac.create();
  char cqName[100];
  sprintf(cqName,"cq-%d",cqNum);
  CqQueryPtr qry = qs->newCq(cqName, queryStr, cqAttr);
  startTime = ACE_OS::gettimeofday();
  SelectResultsPtr results = qry->executeWithInitialResults(QUERY_RESPONSE_TIMEOUT);
  endTime = ACE_OS::gettimeofday() - startTime;
  FWKINFO(" Time Taken to execute the cq : "<< queryStr << ": is " <<  endTime.sec() << "." << endTime.usec() << " sec");
  return results;
}
bool QueryTest::verifyResultSet(int distinctKeys)
{
  bool result = false;
  int numOfKeys;
  if(distinctKeys > 0) {
    FWKINFO("QueryTest::verifyResultSet distinctKeys > 0");
    numOfKeys = distinctKeys ;
  }else {
    resetValue( "distinctKeys" );
    numOfKeys = getIntValue( "distinctKeys" );
  }
  QueryHelper *qh = &QueryHelper::getHelper();
  int setSize = qh->getPortfolioSetSize();
  FWKINFO("QueryTest::verifyResultSet : numOfKeys = " <<numOfKeys << " setSize = " << setSize);
  if(numOfKeys < setSize){
    setSize = numOfKeys;
  }
  int numSet = numOfKeys/setSize;
  QueryServicePtr qs=checkQueryService();
  resetValue( "categoryType" );
  int32_t category = getIntValue( "categoryType" );
  bool islargeSetQuery = g_test->getBoolValue( "largeSetQuery" );
  resetValue( "cq" );
  bool isCq = getBoolValue( "cq" );
  resetValue( "paramquery" );
  bool isParamquery = getBoolValue( "paramquery" );
  int RSsize = 0;
  if(isCq)
    RSsize = QueryStrings::CQRSsize();
  else if(isParamquery)
    RSsize = QueryStrings::RSPsize();
  else
    RSsize = QueryStrings::RSsize();
  while(category > 0){
    for ( int i = 0; i < RSsize; i++)
    {
      try {
        if(isCq){
          if ((cqResultsetQueries[i].category == category) && (cqResultsetQueries[i].category != unsupported)
                  && (!cqResultsetQueries[i].haveLargeResultset != islargeSetQuery))
          {
            FWKINFO(" Query Category : " << category << ": Query :" << " cqResultsetQueries[" << i << "] :" << cqResultsetQueries[i].query() << ": numSet is :" << numSet);
            perf::sleepMillis( 50 % 2 + 3);
            SelectResultsPtr results = continuousQuery(qs,cqResultsetQueries[i].query(),i);
            if ((cqResultsetQueries[i].category != unsupported) &&
                    (!qh->verifySS(results, (qh->isExpectedRowsConstantCQRS(i) ? cqResultsetRowCounts[i] : cqResultsetRowCounts[i] * numSet), 2)))
            {
              char failmsg[100] = {0};
              ACE_OS::sprintf(failmsg, "Query verify failed for query index %d", i);
              FWKSEVERE( "QueryTest::verifyResultSet " << failmsg );
              return false;
             } else {
               result = true;
             }
           }
          }
        else if (isParamquery) {
          if ((resultsetparamQueries[i].category == category)
              && (resultsetparamQueries[i].category != unsupported)
              && (!resultsetparamQueries[i].haveLargeResultset
                  != islargeSetQuery)) {
            FWKINFO(" Query Category : " << category << ": Query :" << " resultsetparamQueries[" << i << "] :" << resultsetparamQueries[i].query() << ": numSet is :" << numSet);
            CacheableVectorPtr paramList = CacheableVector::create();
            for (int j = 0; j < noofQueryParam[i]; j++) {
              if (atoi(queryparamSet[i][j]) != 0) {
                paramList->push_back(Cacheable::create(atoi(queryparamSet[i][j])));
              } else
                paramList->push_back(Cacheable::create(queryparamSet[i][j]));
            }
            perf::sleepMillis(50 % 2 + 3);
            ACE_Time_Value startTime, endTime;

            QueryPtr qry = qs->newQuery(resultsetparamQueries[i].query());
            startTime = ACE_OS::gettimeofday();
            SelectResultsPtr results = qry->execute(paramList,QUERY_RESPONSE_TIMEOUT);
            endTime = ACE_OS::gettimeofday() - startTime;
            FWKINFO(" Time Taken to execute param query : "<< resultsetparamQueries[i].query() << ": is " <<  endTime.sec() << "." << endTime.usec() << " sec");
            FWKINFO("resultSet size = " << results->size() << " numSet = " << numSet << " resultsetRowCountsPQ[" << i << "] = "
                << resultsetRowCounts[i] << " resultsetRowCountsPQ[" << i << "] * numSet = " << resultsetRowCountsPQ[i]*numSet);
            if ((resultsetparamQueries[i].category != unsupported)
                && (!qh->verifyRS(
                    results,
                    (qh->isExpectedRowsConstantPQRS(i) ? resultsetRowCountsPQ[i]
                        : resultsetRowCountsPQ[i] * numSet)))) {
              char failmsg[100] = { 0 };
              ACE_OS::sprintf(failmsg,"Query verify failed for param query index %d", i);
              FWKSEVERE( "QueryTest::verifyResultSet " << failmsg );
              return false;
            } else {
              result = true;
            }
          }
        } else
          {
            if ((resultsetQueries[i].category == category) && (resultsetQueries[i].category != unsupported)
                      && (!resultsetQueries[i].haveLargeResultset != islargeSetQuery))
            {
              FWKINFO(" Query Category : " << category << ": Query :" << " resultsetQueries[" << i << "] :" << resultsetQueries[i].query() << ": numSet is :" << numSet);
              perf::sleepMillis( 50 % 2 + 3);
               bool isPdx=getBoolValue("isPdx");
              if(isPdx)
              {
                if (i == 2 || i == 3 || i == 4)
            	  {
            	    FWKINFO("Skipping query index " << i <<" for Pdx because it is function type.");
            	    continue;
            	  }
              }
              SelectResultsPtr results = remoteQuery(qs,resultsetQueries[i].query());
              FWKINFO("resultSet size = " << results->size() << " numSet = " << numSet << " resultsetRowCounts[" << i << "] = "
                        << resultsetRowCounts[i] << " resultsetRowCounts[" << i << "] * numSet = " << resultsetRowCounts[i]*numSet);
              if((resultsetQueries[i].category != unsupported)
                   && (!qh->verifyRS(results, (qh->isExpectedRowsConstantRS(i) ? resultsetRowCounts[i] : resultsetRowCounts[i] * numSet))))
              {
                char failmsg[100] = {0};
                ACE_OS::sprintf(failmsg, "Query verify failed for query index %d", i);
                FWKSEVERE( "QueryTest::verifyResultSet " << failmsg );
                return false;
              }else {
                result = true;
              }
            }
          }
        } catch(...) {
            if((resultsetQueries[i].category != unsupported ) ||( resultsetparamQueries[i].category != unsupported )
                || ( cqResultsetQueries[i].category != unsupported))
            throw;

        }
      }
      category = getIntValue( "categoryType" );
    }
    return result;
}
//------------------------------------------------------------------------------------

bool QueryTest::verifyStructSet(int distinctKeys)
{
  bool result = false;
  int numOfKeys;
  if(distinctKeys > 0) {
    numOfKeys = distinctKeys;
  }else {
    resetValue( "distinctKeys" );
    numOfKeys = getIntValue( "distinctKeys" );
  }
  QueryHelper *qh = &QueryHelper::getHelper();
  int setSize = qh->getPortfolioSetSize();
  FWKINFO("QueryTest::verifyStructSet : numOfKeys = " <<numOfKeys << " setSize = " << setSize);
  if(numOfKeys < setSize){
    setSize = numOfKeys;
  }
  int numSet = numOfKeys/setSize;
  QueryServicePtr qs=checkQueryService();
  ACE_Time_Value startTime, endTime;
  resetValue( "categoryType" );
  int32_t category = getIntValue( "categoryType" );
  bool islargeSetQuery = g_test->getBoolValue( "largeSetQuery" );
  resetValue( "paramquery" );
  bool isParamquery = getBoolValue( "paramquery" );
  int SSsize = 0;
   if(isParamquery)
      SSsize = QueryStrings::SSPsize();
    else
      SSsize = QueryStrings::SSsize();
  FWKINFO("QueryTest::verifyStructSet category = " << category);
  try {
    while(category > 0){
      for ( int i = 0; i < SSsize; i++)
      {
        if(isParamquery) {
          if ((structsetParamQueries[i].category == category) && (!structsetParamQueries[i].haveLargeResultset != islargeSetQuery) && (structsetParamQueries[i].category != unsupported))
          {
            FWKINFO(" Query Category : " << category << ": Query :" << " structsetParamQueries[" << i << "] :" << structsetParamQueries[i].query() << ": numSet is :" << numSet);
            perf::sleepMillis( 50 % 2 + 3);
            try {
              CacheableVectorPtr paramList = CacheableVector::create();

              for (int j = 0; j < numSSQueryParam[i]; j++) {
                if (atoi(queryparamSetSS[i][j]) != 0) {
                  paramList->push_back(Cacheable::create(atoi(
                      queryparamSetSS[i][j])));
                } else
                  paramList->push_back(Cacheable::create(queryparamSetSS[i][j]));
              }

              QueryPtr qry = qs->newQuery(structsetParamQueries[i].query());
              startTime = ACE_OS::gettimeofday();
              SelectResultsPtr results = qry->execute(paramList,QUERY_RESPONSE_TIMEOUT);
              endTime = ACE_OS::gettimeofday() - startTime;
              FWKINFO(" Time Taken to execute the query : "<< structsetParamQueries[i].query() << ": is " << endTime.sec() << "." << endTime.usec() <<" sec");
              if((structsetParamQueries[i].category != unsupported)
                  && (! qh->verifySS(results, (qh->isExpectedRowsConstantSSPQ(i) ? structsetRowCountsPQ[i] : structsetRowCountsPQ[i] * numSet), structsetFieldCountsPQ[i])) )
              {
                char failmsg[100] = {0};
                ACE_OS::sprintf(failmsg, "Query verify failed for param query index %d", i);
                FWKSEVERE( "QueryTest::verifyStructSet " << failmsg );
                return false;
              } else {
                result = true;
              }
            } catch(...) {
              if(structsetParamQueries[i].category != unsupported)
              throw;
            }
          }
        }
        else {
        if ((structsetQueries[i].category == category) && (!structsetQueries[i].haveLargeResultset != islargeSetQuery) && (structsetQueries[i].category != unsupported))
        {
          bool isPdx=getBoolValue("isPdx");
          int32_t skipQry []={4,6,7,9,12,14,15,16};
          std::vector<int>skipVec;
          for(int32_t j=0;j<10;j++)
          {
        	  skipVec.push_back(skipQry[j]);
          }
          bool isPresent=(std::find(skipVec.begin(),skipVec.end(),i)!= skipVec.end());
          if(isPdx && isPresent)
          {
        	  FWKINFO("Skiping Query for pdx object" <<structsetQueries[i].query());
          }
          else
          {
            FWKINFO(" Query Category : " << category << ": Query :" << " structsetQueries[" << i << "] :" << structsetQueries[i].query() << ": numSet is :" << numSet);
            perf::sleepMillis( 50 % 2 + 3);
            try {
              QueryPtr qry = qs->newQuery(structsetQueries[i].query());
              startTime = ACE_OS::gettimeofday();
              SelectResultsPtr results = qry->execute(QUERY_RESPONSE_TIMEOUT);
              endTime = ACE_OS::gettimeofday() - startTime;
              FWKINFO(" Time Taken to execute the query : "<< structsetQueries[i].query() << ": is " << endTime.sec() << "." << endTime.usec() <<" sec");
              if((structsetQueries[i].category != unsupported)
                 && (! qh->verifySS(results, (qh->isExpectedRowsConstantSS(i) ? structsetRowCounts[i] : structsetRowCounts[i] * numSet), structsetFieldCounts[i])) )
              {
                char failmsg[100] = {0};
                ACE_OS::sprintf(failmsg, "Query verify failed for query index %d", i);
                FWKSEVERE( "QueryTest::verifyStructSet " << failmsg );
                return false;
              }else {
                result = true;
              }
             } catch(...) {
               if(structsetQueries[i].category != unsupported)
               throw;
              }
         }
        }
      }
     }
     category = getIntValue( "categoryType" );
    }
   } catch ( Exception e ) {
     FWKSEVERE( "QueryTest::verifyStructSet Caught Exception: " << e.getMessage() );
     result = false;
   } catch ( FwkException& e ) {
     FWKSEVERE( "QueryTest::verifyStructSet Caught FwkException: " << e.getMessage() );
     result = false;
   } catch ( ... ) {
     FWKSEVERE( "QueryTest::verifyStructSet Caught unknown exception." );
     result = false;
   }
   return result;
}

bool QueryTest::readQueryStringfromXml(std::string &queryString){
   bool result=false;
   bool isCq = getBoolValue( "cq");
   //int cqNum = 0;
   char cqName[300];
   do {
     int resultSize = getIntValue( "resultSize" );  // set the query result size in xml
     FWKINFO("Qyery String is " << queryString << ": resultSize :" << resultSize);
     if(resultSize < 0)
     {
       result = false;
       FWKEXCEPTION("resultSize data type is not define in xml");
     }
     QueryServicePtr qs=checkQueryService();
     ACE_Time_Value startTime,endTime;
     SelectResultsPtr sptr;
     if(isCq) {
       ///VectorOfCqQuery vec;
       sprintf(cqName,"_default%s",queryString.c_str());
       CqQueryPtr qry = qs->getCq(cqName);
       //CqQueryPtr qry = vec.at(cqNum++);
       startTime = ACE_OS::gettimeofday();
       sptr = qry->executeWithInitialResults(QUERY_RESPONSE_TIMEOUT);
       endTime = ACE_OS::gettimeofday() - startTime;
       FWKINFO(" Time Taken to execute the cq query : " << qry->getQueryString()  << " for cq " << qry->getName() << " : is " << endTime.sec() << "." << endTime.usec() << " sec");
     }
     else
     {
       startTime = ACE_OS::gettimeofday();
       QueryPtr q = qs->newQuery(queryString.c_str());
       sptr = q->execute(QUERY_RESPONSE_TIMEOUT);
       endTime = ACE_OS::gettimeofday() - startTime;
       FWKINFO(" Time Taken to execute the query : "<< queryString << ": is " << endTime.sec() << "." << endTime.usec() << " sec");
     }
     if(resultSize == (int)sptr->size())
       result = true;
     else {
       FWKSEVERE(" result size found is "<< sptr->size() << " and expected result size is " << resultSize);
       return result;
     }
     FWKINFO(" result size found is "<< sptr->size() << " and expected result size is " << resultSize);
     queryString = getStringValue( "query" );
     FWKINFO("Reading query from xml : " << queryString);
   } while(!queryString.empty());

   return result;
}
//----------------------------------------------------------------------------------------
int32_t QueryTest::runQuery()
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "In QueryTest::runQuery" );

  try {
    std::string queryType = getStringValue( "queryResultType" );
    std::string queryStr = getStringValue( "query" ); // set the query string in xml
    if( queryType == "resultSet") {
      FWKINFO("calling verifyResultSet ");
      if(!verifyResultSet())
        result = FWK_SEVERE;
    } else if( queryType == "structSet"){
      FWKINFO("calling verifyStructSet ");
      if(!verifyStructSet())
        result = FWK_SEVERE;
    } else if(queryStr != "") {
      FWKINFO("Reading query from xml : "<< queryStr);
      if(!readQueryStringfromXml(queryStr))
        result = FWK_SEVERE;
    } else {
      result = FWK_SEVERE;
      FWKEXCEPTION( "Query type is not supported :" <<  queryType);
    }
  } catch ( Exception &e ) {
    FWKEXCEPTION( "QueryTest::runQuery Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "QueryTest::runQuery Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::runQuery Caught unknown exception." );
  }
  FWKINFO( "QueryTest::runQuery complete." );
  return result;
}

// ----------------------------------------------------------------------------

int32_t QueryTest::populateRangePositionObjects( ) {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::populateUserObject()" );

  try {
    RegionPtr region = getRegionPtr();
    int rangeStart = getIntValue( "range-start" );
    int rangeEnd = getIntValue( "range-end" );
    QueryHelper *qh = &QueryHelper::getHelper();
    qh->populateRangePositionData(region, rangeStart, rangeEnd);
    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "QueryTest::populateRangePosition() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "QueryTest::populateRangePosition Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "QueryTest::populateRangePosition Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::populateRangePosition Caught unknown exception." );
  }
  FWKINFO( "QueryTest::populateRangePosition complete." );
  return result;
}

int32_t QueryTest::getAndComparePositionObjects( ) {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::populateUserObject()" );

  try {
    RegionPtr region = getRegionPtr();
    int rangeStart = getIntValue( "get-range-start" );
    int rangeEnd = getIntValue( "get-range-end" );

    QueryHelper *qh = &QueryHelper::getHelper();
    for(int i = rangeStart; i<=rangeEnd; i++)
    {
      SerializablePtr cachedpos = qh->getCachedPositionObject(region, i);
      SerializablePtr generatedpos = qh->getExactPositionObject(i);
      if (!qh->compareTwoPositionObjects(cachedpos, generatedpos))
      {
        FWKSEVERE( "QueryTest::getAndComparePositionObjects objects not same for index " << i );
      }
    }
    qh->populateRangePositionData(region, rangeStart, rangeEnd);
    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "QueryTest::populateRangePosition() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "QueryTest::populateRangePosition Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "QueryTest::populateRangePosition Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::populateRangePosition Caught unknown exception." );
  }
  FWKINFO( "QueryTest::populateRangePosition complete." );
  return result;
}

int32_t QueryTest::updateRangePositions( ) {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::updateRangePositions()" );

  int32_t maxrange = getIntValue("range-max");

  int32_t secondsToRun = getTimeValue("workTime");
  secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;

  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  ACE_Time_Value now;

  RegionPtr region = getRegionPtr();
  GsRandom * gsrand = GsRandom::getInstance();
  QueryHelper *qh = &QueryHelper::getHelper();

  try {
    while(now < end)
    {
      int32_t randval = gsrand->nextInt(maxrange);
      qh->putExactPositionObject(region, randval);
      now = ACE_OS::gettimeofday();
    }
    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "QueryTest::updateRangePositions() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "QueryTest::updateRangePositions Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "QueryTest::updateRangePositions Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::updateRangePositions Caught unknown exception." );
  }
  FWKINFO( "QueryTest::updateRangePositions complete." );
  return result;
}

int32_t QueryTest::verifyAllPositionObjects( ) {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::verifyAllPositionObjects()" );

  RegionPtr region = getRegionPtr();
  int32_t maxrange = getIntValue("range-max");

  QueryHelper *qh = &QueryHelper::getHelper();

  try {
    for(int i =1; i<=maxrange; i++)
    {
      SerializablePtr cachedpos = qh->getCachedPositionObject(region, i);
      SerializablePtr generatedpos = qh->getExactPositionObject(i);
      if (!qh->compareTwoPositionObjects(cachedpos, generatedpos))
      {
        FWKSEVERE( "QueryTest::verifyAllPositionObjects objects not same for index " << i );
      }
    }
    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "QueryTest::verifyAllPositionObjects Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "QueryTest::verifyAllPositionObjects Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "QueryTest::verifyAllPositionObjects Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::verifyAllPositionObjects Caught unknown exception." );
  }
  FWKINFO( "QueryTest::verifyAllPositionObjects complete." );
  return result;
}
int32_t QueryTest::populatePortfolioObject()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::populatePortfolioObject()" );

  try {
    RegionPtr region = getRegionPtr();
    int32_t numClients = getIntValue( "clientCount" );
    std::string label = RegionHelper::regionTag( region->getAttributes() );
    resetValue("distinctKeys");
    int numOfKeys = getIntValue( "distinctKeys" ); // number of key should be multiple of 20
    resetValue("valueSizes");
    int objSize = getIntValue( "valueSizes" );
    QueryHelper *qh = &QueryHelper::getHelper();
    const std::string objectType = getStringValue( "objectType" );
    while ( objSize > 0 ) { // valueSizes loop
      if( objectType == "Portfolio") {
         qh->populatePortfolio(region,numOfKeys,objSize);
      }
      FWKINFO( "Populated Portfolio objects" << objectType << "for value size " << objSize );

      bool checked = checkReady(numClients);
      if (checked) {
        bbDecrement(CLIENTSBB, READYCLIENTS);
      }
      objSize = getIntValue( "valueSizes" );
      if ( objSize > 0 ) {
        perf::sleepSeconds( 2 ); // Put a marker of inactivity in the stats
      }
    } // valueSizes loop
     perf::sleepSeconds(10);

    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "QueryTest::populatePortfolioObject() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "QueryTest::populatePortfolioObject Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "QueryTest::populatePortfolioObject Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::populatePortfolioObject Caught unknown exception." );
  }
  perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
  FWKINFO( "QueryTest::populatePortfolioObject complete." );
  return result;
}

int32_t QueryTest::populateUserObject()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::populateUserObject()" );

  try {
    RegionPtr region = getRegionPtr();
    int32_t numClients = getIntValue( "clientCount" );
    std::string label = RegionHelper::regionTag( region->getAttributes() );
    resetValue("distinctKeys");
    int numOfKeys = getIntValue( "distinctKeys" ); // number of key should be multiple of 20
    resetValue("valueSizes");
    int objSize = getIntValue( "valueSizes" );
    QueryHelper *qh = &QueryHelper::getHelper();
    int numSet=0;
    int setSize=0;
    RegionPtr rgnPtr;
    CacheableKeyPtr idxCreateKey;
    CacheableKeyPtr idxRemoveKey;
    CacheablePtr port;
    const std::string objectType = getStringValue( "objectType" );
    while ( objSize > 0 ) { // valueSizes loop
      if( objectType == "Portfolio") {
        setSize = qh->getPortfolioSetSize();
        if(numOfKeys < setSize){
          FWKEXCEPTION( "QueryTest::populateUserObject : Number of keys should be multiple of 20");
        }
        numSet = numOfKeys/setSize;
        qh->populatePortfolioData(region,setSize,numSet,objSize);
      } else if(objectType == "Position") { 
        setSize = qh->getPositionSetSize();
        if(numOfKeys < setSize){
          FWKEXCEPTION( "QueryTest::populateUserObject : Number of keys should be multiple of 20");
        }
        numSet = numOfKeys/setSize;
        qh->populatePositionData(region, setSize ,numSet);
      }
     else if (objectType == "PortfolioPdx")
       {
    	 setSize = qh->getPortfolioSetSize();
    	 if(numOfKeys < setSize){
    	   FWKEXCEPTION( "QueryTest::populateUserObject : Number of keys should be multiple of 20");
    	 }
    	 numSet = numOfKeys/setSize;
    	 qh->populatePortfolioPdxData(region,setSize,numSet,objSize);
      }
      else if(objectType == "PositionPdx")
      {
        setSize = qh->getPositionSetSize();
    	if(numOfKeys < setSize){
    	  FWKEXCEPTION( "QueryTest::populateUserObject : Number of keys should be multiple of 20");
    	}
    	numSet = numOfKeys/setSize;
    	qh->populatePositionPdxData(region, setSize ,numSet);
      }
      else if (objectType == "AutoPortfolioPdx")
      {
      	setSize = qh->getPortfolioSetSize();
       	  if(numOfKeys < setSize){
       	    FWKEXCEPTION( "QueryTest::populateUserObject : Number of keys should be multiple of 20");
       	  }
       	  numSet = numOfKeys/setSize;
       	  qh->populateAutoPortfolioPdxData(region,setSize,numSet,objSize);
        }
        else if(objectType == "AutoPositionPdx")
        {
          setSize = qh->getPositionSetSize();
          if(numOfKeys < setSize){
            FWKEXCEPTION( "QueryTest::populateUserObject : Number of keys should be multiple of 20");
          }
          numSet = numOfKeys/setSize;
          qh->populateAutoPositionPdxData(region, setSize ,numSet);
       }
      FWKINFO( "Populated User objects" << objectType << "for value size " << objSize );

      bool checked = checkReady(numClients);
      if (checked) {
        bbDecrement(CLIENTSBB, READYCLIENTS);
      }
      objSize = getIntValue( "valueSizes" );
      if ( objSize > 0 ) {
        perf::sleepSeconds( 2 ); // Put a marker of inactivity in the stats
      }
    } // valueSizes loop
    perf::sleepSeconds(10);
    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "QueryTest::populateUserObject() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "QueryTest::populateUserObject Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "QueryTest::populateUserObject Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::populateUserObject Caught unknown exception." );
  }
  perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
  FWKINFO( "QueryTest::populateUserObject complete." );
  return result;
}
//----------------------------------------------------------------------------
int32_t QueryTest::destroyUserObject()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::destroyUserObject()" );

  try {
    RegionPtr region = getRegionPtr();
    std::string label = RegionHelper::regionTag( region->getAttributes() );
    resetValue( "distinctKeys" );
    int numOfKeys = getIntValue( "distinctKeys" ); // number of key should be multiple of 20
    QueryHelper *qh = &QueryHelper::getHelper();

    const std::string objectType = getStringValue( "objectType" );
    int setSize = qh->getPortfolioSetSize();
    int destroyKeys = getIntValue( "destroyKeys");
    if( destroyKeys <= 0) {
      if(numOfKeys < setSize){
        FWKEXCEPTION( "QueryTest::destroyUserObject : Number of keys should be multiple of 20");
      }
    } else {
      numOfKeys = destroyKeys;
    }
    int numSet = numOfKeys/setSize;
    FWKINFO( "Destroying "<< numOfKeys << " keys,  distinctKeys " << numOfKeys <<" destroyKeys " << destroyKeys << " setSize " << setSize << " numSet " << numSet);
    qh->destroyPortfolioOrPositionData(region,setSize,numSet,objectType.c_str());
    FWKINFO( "Destroyed User objects" );

    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "QueryTest::destroyUserObject() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "QueryTest::destroyUserObject Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "QueryTest::destroyUserObject Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::destroyUserObject Caught unknown exception." );
  }
  FWKINFO( "QueryTest::destroyUserObject complete." );
  return result;
}

//----------------------------------------------------------------------------
int32_t QueryTest::invalidateUserObject()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::invalidateUserObject()" );

  try {
    RegionPtr region = getRegionPtr();
    std::string label = RegionHelper::regionTag( region->getAttributes() );
    resetValue( "distinctKeys" );
    int numOfKeys = getIntValue( "distinctKeys" ); // number of key should be multiple of 20
    QueryHelper *qh = &QueryHelper::getHelper();

    const std::string objectType = getStringValue( "objectType" );
    int setSize = qh->getPortfolioSetSize();
    int invalidateKeys = getIntValue( "invalidateKeys");
    if( invalidateKeys <= 0) {
      if(numOfKeys < setSize){
        FWKEXCEPTION( "QueryTest::invalidateUserObject : Number of keys should be multiple of 20");
      }
    } else {
      numOfKeys = invalidateKeys;
    }
    int numSet = numOfKeys/setSize;
    FWKINFO( "Invalidating "<< numOfKeys << " keys,  distinctKeys " << numOfKeys <<" invalidateKeys " << invalidateKeys << " setSize " << setSize << " numSet " << numSet);
    qh->invalidatePortfolioOrPositionData(region,setSize,numSet,objectType.c_str());
    FWKINFO( "Invalidated User objects" );

    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "QueryTest::invalidateUserObject() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "QueryTest::invalidateUserObject Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "QueryTest::invalidateUserObject Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::invalidateUserObject Caught unknown exception." );
  }
  FWKINFO( "QueryTest::invalidateUserObject complete." );
  return result;
}


// ----------------------------------------------------------------------------

RegionPtr QueryTest::getRegionPtr( const char * reg )
{
  RegionPtr region;
  std::string name;
  if ( reg == NULL ) {
    name = getStringValue( "regionName" );
    if ( name.empty() ) {
      try {
        RegionHelper help( g_test );
        name = help.regionName();
        if ( name.empty() ) {
          name = help.specName();
        }
      } catch( ... ) {}
    }
  }
  else {
    name = reg;
  }

  try {
      FWKINFO( "Getting region: " << name );
      if (m_cache == NULLPTR) {
        FWKEXCEPTION( "Failed to get region: " << name << "  cache ptr is null." );
      }
      FWKINFO( "Getting a " << name << " region." );
      region = m_cache->getRegion( name.c_str() );
      if (region == NULLPTR) {
        FWKEXCEPTION( "Failed to get region: " << name );
      }
  } catch( CacheClosedException e ) {
    FWKEXCEPTION( "In QueryTest::getRegionPtr()  CacheFactory::getInstance encountered "
      "CacheClosedException: " << e.getMessage() );
  } catch( EntryNotFoundException e ) {
    FWKEXCEPTION( "In QueryTest::getRegionPtr()  CacheFactory::getInstance encountered "
      "EntryNotFoundException: " << e.getMessage() );
  } catch( IllegalArgumentException e ) {
    FWKEXCEPTION( "In QueryTest::getRegionPtr()  CacheFactory::getInstance encountered "
      "IllegalArgumentException: " << e.getMessage() );
  }
  return region;
}

bool QueryTest::checkReady(int32_t numClients)
{
  if (numClients > 0) {
    FWKINFO( "Check whether all clients are ready to run" );
    bbIncrement( CLIENTSBB, READYCLIENTS );
    int64_t readyClients = 0;
    while (readyClients < numClients) {
      readyClients = bbGet( CLIENTSBB, READYCLIENTS );
      perf::sleepMillis(3);
    }
    FWKINFO( "All Clients are ready to go !!" );
    return true;
  }
  FWKINFO( "All Clients are ready to go !!" );
  return false;
}

// ----------------------------------------------------------------------------
std::string QueryTest::getNextRegionName(RegionPtr& regionPtr)
{
  std::string regionName;
  int count = 0;
  std::string path;
  do {
    path = getStringValue( "regionPaths" );
    if ( path.empty() ) {
      return path;
    }
    FWKINFO("QueryTest::getNextRegionName regionPath is " << path);
    do {
      size_t length = path.length();
      try {
        regionPtr = m_cache->getRegion( path.c_str() );
      } catch( ... ) {}
      if (regionPtr == NULLPTR) {
        size_t pos = path.rfind( '/' );
        regionName = path.substr( pos + 1, length - pos );
        path = path.substr( 0, pos );
      }
    } while ((regionPtr == NULLPTR) && !path.empty());
  } while ( ( ++count < 5 ) && regionName.empty() );
  return regionName;
}
//--------------------------------------------------------------------------------
int32_t QueryTest::addRootAndSubRegion()
{
  int32_t fwkResult = FWK_SEVERE;
  resetValue("subRegion");
  bool createSubReg=getBoolValue("subRegion");
 try
 {
  Serializable::registerType( Position::createDeserializable);
  Serializable::registerType( Portfolio::createDeserializable);
  {
    Serializable::registerPdxType(testobject::PositionPdx::createDeserializable);
    FWKINFO("registered PositionPdx object");
    Serializable::registerPdxType(AutoPdxTests::PositionPdx::createDeserializable);
    FWKINFO("registered AutoPositionPdx object");
  }
  {
    Serializable::registerPdxType(testobject::PortfolioPdx::createDeserializable);
    FWKINFO("registered PortfolioPdx object");
    Serializable::registerPdxType(AutoPdxTests::PortfolioPdx::createDeserializable);
    FWKINFO("registered AutoPortfolioPdx object");
  }
 }
 catch(const IllegalStateException& )
{} 	
  RegionPtr parentRegionPtr = NULLPTR;
  //RegionPtr region;
  resetValue("regionPaths");
  std::string sRegionName = getNextRegionName(parentRegionPtr);
  while (!sRegionName.empty()){
  try {
    createPool();
    RegionPtr region;
    FWKINFO( "In addRootAndSubregionRegion, enter create region " << sRegionName );
    if (parentRegionPtr == NULLPTR) {
      FWKINFO( "In addRootAndSubregionRegion, creating root region");
      RegionHelper regionHelper( g_test );
      region = regionHelper.createRootRegion( m_cache, sRegionName ) ;
      fwkResult = FWK_SUCCESS;
    } else {
      FWKINFO( "In addRootAndSubregionRegion, creating sub region");
      std::string fullName = parentRegionPtr->getFullPath();
      RegionAttributesPtr atts;
      atts = parentRegionPtr->getAttributes();
      AttributesFactory fact(atts);
      atts = fact.createRegionAttributes();
      region = parentRegionPtr->createSubregion( sRegionName.c_str(), atts );
      std::string bb( "CppQueryRegions" );
      bbSet( bb, sRegionName, fullName );
      fwkResult = FWK_SUCCESS;
    }
  } catch (Exception e) {
    FWKEXCEPTION("addRootAndSubregionRegion caught Exception: " <<
      e.getMessage());
    break;
  } catch (std::exception e) {
    FWKEXCEPTION("addRootAndSubregionRegion caught std::exception: " <<
      e.what());
    break;
  }
  sRegionName = getNextRegionName(parentRegionPtr);
  FWKINFO( "In addRegion, exit create region " << sRegionName );
 }
 return fwkResult;
}
//-------------------------------------------------------------------------

int32_t QueryTest::doRunQueryWithPayloadAndEntries(){
  int32_t fwkResult = FWK_SUCCESS;

  try {
    int32_t numClients = getIntValue( "clientCount" );
    //std::string label = RegionHelper::regionTag( region->getAttributes() );
    QueryHelper *qh = &QueryHelper::getHelper();
    int numSet=0;
    int setSize=0;
    //populating data
    resetValue("distinctKeys");
    int numOfKeys = getIntValue( "distinctKeys" ); // number of key should be multiple of 20
    while (numOfKeys > 0) { // distinctKeys loop
      resetValue("valueSizes");
      int objSize = getIntValue( "valueSizes" );
      while ( objSize > 0 ) { // valueSizes loop
        FWKINFO("QueryTest::doRunQueryWithPayloadAndEntries Populating " << numOfKeys << " entries with " << objSize << " payload size")
        RegionPtr region = getRegionPtr();
        while (region != NULLPTR) {
          std::string rgnName(region->getName());
          if( (rgnName == "Portfolios") || (rgnName == "Portfolios2") || (rgnName == "Portfolios3")) {
            FWKINFO("QueryTest::doRunQueryWithPayloadAndEntries Populating Portfolio object");
            setSize = qh->getPortfolioSetSize();
            if(numOfKeys < setSize){
              FWKEXCEPTION( "doRunQueryWithPayloadAndEntries : Number of keys should be multiple of 20");
            }
            numSet = numOfKeys/setSize;
            qh->populatePortfolioData(region,setSize,numSet,objSize);
          } else if((rgnName == "Positions") || (rgnName == "/Portfolios/Positions")) {
            FWKINFO("QueryTest::doRunQueryWithPayloadAndEntries Populating Position object");
            setSize = qh->getPositionSetSize();
            if(numOfKeys < setSize){
              FWKEXCEPTION( "doRunQueryWithPayloadAndEntries : Number of keys should be multiple of 20");
            }
            numSet = numOfKeys/setSize;
            qh->populatePositionData(region, setSize ,numSet);
          }
          region = getRegionPtr();
        }
        FWKINFO( "Populated User objects" );
        bool checked = checkReady(numClients);
        if (checked) {
          bbDecrement(CLIENTSBB, READYCLIENTS);
        }
        perf::sleepSeconds(10);
        // running queries
        FWKINFO("calling verifyResultSet ");
        if(!verifyResultSet(numOfKeys)) {
          fwkResult = FWK_SEVERE;
        }
        FWKINFO("calling verifyStructSet ");
        if(!verifyStructSet(numOfKeys)) {
          fwkResult = FWK_SEVERE;
        }
        perf::sleepSeconds(2);
        objSize = getIntValue( "valueSizes" );
        if ( objSize > 0 ) {
          perf::sleepSeconds( 1 ); // Put a marker of inactivity in the stats
        }
      } // valueSizes loop
      numOfKeys = getIntValue( "distinctKeys" );
      if (numOfKeys> 0 ) {
        perf::sleepSeconds(2);
      }
    }// distinctKeys loop
  } catch ( Exception & e ) {
    FWKEXCEPTION( "doRunQueryWithPayloadAndEntries() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "doRunQueryWithPayloadAndEntries Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "doRunQueryWithPayloadAndEntries Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "doRunQueryWithPayloadAndEntries Caught unknown exception." );
  }
  perf::sleepSeconds( 4 ); // Put a marker of inactivity in the stats
  FWKINFO( "doRunQueryWithPayloadAndEntries complete." );
  return fwkResult;
}
int32_t QueryTest::cqState()
{
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO( "cqState Called");
  std::string opcode;
  try {
    opcode = getStringValue( "cqState" );
    QueryServicePtr qs=checkQueryService();
    VectorOfCqQuery vec;
    qs->getCqs(vec);
    FWKINFO("number of cqs for verification "<< vec.size());
    for(uint8_t i=0; i< vec.size(); i++)
    {
      CqQueryPtr cq = vec.at(i);
      if ( opcode == "stopped")
         cq->stop();
      else if(opcode == "closed")
         cq->close();
      else if(opcode == "execute")
         cq->execute();
      else
        FWKEXCEPTION( "Invalid operation specified: " << opcode );
    }
  } catch ( Exception &e ) {
    fwkResult = FWK_SEVERE;
    FWKEXCEPTION( "Caught unexpected exception during cq " << opcode
      << " operation: " << e.getMessage() << " exiting task." );
  }
  return fwkResult;
}

int32_t QueryTest::cqOperations()
{
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO( "cqOperations called." );

  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;

  int32_t opsSec = getIntValue( "opsSecond" );
  opsSec = ( opsSec < 1 ) ? 0 : opsSec;

  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  ACE_Time_Value now;

  std::string opcode;
  std::string objectType;

  int32_t getCQAttributes = 0, getCQName = 0, getQuery = 0, getQueryString = 0, getStatistics=0, stopCq=0,closeCq=0,executeCq=0,executeCqWithIR=0;

  FWKINFO( "cqOperations will work for " << secondsToRun );

  PaceMeter meter( opsSec );
  QueryServicePtr qs=checkQueryService();
  while ( now < end ) {
    try {
      opcode = getStringValue( "cqOps" );
      VectorOfCqQuery vec;
      qs->getCqs(vec);
      int idx = GsRandom::random( vec.size() );
      CqQueryPtr cq = vec.at(idx);
      FWKINFO("Performing " << opcode << " on cq named " << cq->getName());
      if ( opcode.empty() ) opcode = "no-op";

        if ( opcode == "getCQAttributes" ) {
          cq->getCqAttributes();
          getCQAttributes++;
        }
        else if ( opcode == "getCQName" ) {
          cq->getName();
          getCQName++;
        }
        else if ( opcode == "getQuery" ) {
          cq->getQuery();
          getQuery++;
        }
        else if ( opcode == "getQueryString" ) {
          cq->getQueryString();
          getQueryString++;
        }
        else if ( opcode == "getStatistics" ) {
          cq->getStatistics();
          getStatistics++;
        }
        else if ( opcode == "stopCq") {
          stopCQ(cq);
          stopCq++;
        }
        else if ( opcode == "closeCQ") {
          closeCQ(cq);
          closeCq++;
        }
        else if ( opcode == "executeCQ") {
          executeCQ(cq);
          executeCq++;
        }
        else if ( opcode == "executeCQWithIR") {
          executeCQWithIR(cq);
          executeCqWithIR++;
        }
        else {
          FWKEXCEPTION( "Invalid operation specified: " << opcode );
        }
    } catch ( Exception &e ) {
      end = 0;
      fwkResult = FWK_SEVERE;
      FWKEXCEPTION( "Caught unexpected exception during cq " << opcode
        << " operation: " << e.getMessage() << " exiting task." );
    }
    meter.checkPace();
    now = ACE_OS::gettimeofday();
  }

  FWKINFO( "cqOperations did " << getCQAttributes << " getCQAttributes, " << getCQName << " getCQName, " <<  getQuery << " getQuery, " << getQueryString << " getQueryString, " << getStatistics << " getStatistics " << stopCq << " stopCQ " << closeCq << " closeCQ " << executeCq << " executeCQ " << executeCqWithIR << " executeCQWithIR ." );
  return fwkResult;
}

void QueryTest::stopCQ(const CqQueryPtr cq)
{
   try {
      if (cq->isRunning()) {
        cq->stop();
      }
      else if (cq->isStopped()) {
        try {
          cq->stop();
          FWKEXCEPTION("QueryTest::stopCQ : should have thrown IllegalStateException. executed stop() successfully on STOPPED CQ");
        }
        catch (IllegalStateException &) {
          // expected
        }
      }

      if (cq->isClosed()) {
        try {
          cq->stop();
          FWKEXCEPTION("QueryTest::stopCQ : should have thrown CQClosedException. executed stop() successfully on CLOSED CQ");
        }
        catch (CqClosedException &) {
          // expected
          reRegisterCQ(cq);
        }
      }
    }
    catch (Exception &e) {
      FWKEXCEPTION( "QueryTest::stopCQ : Caught unexpected exception during cq stop operation :" << e.getMessage());
    }

}
void QueryTest::closeCQ(const CqQueryPtr cq)
{
  try {
      if (cq->isRunning() || cq->isStopped()) {
        cq->close();
      }

      if (cq->isClosed()) {
        try {
          cq->close();
          FWKINFO( "CQ:- " <<  cq->getName()
                  << " is closed hence registering it before execution");
          reRegisterCQ(cq);
        }
        catch (CqClosedException &) {
          FWKEXCEPTION( "QueryTest::closeCQ : Should not have thrown CQClosedException. close() on CLOSED query is not successful");
        }
      }
    }
    catch (Exception &e) {
      FWKEXCEPTION( "QueryTest::closeCQ : Caught unexpected exception during cq close operation: " << e.getMessage());
    }
}
void QueryTest::executeCQ(const CqQueryPtr cq)
{
   try {
      if (cq->isStopped()) {
        cq->execute();
        FWKINFO("executed query:- " << cq->getName());
      }
      else if (cq->isRunning()) {
        try {
          cq->execute();
          FWKEXCEPTION("QueryTest::executeCQ : Should have thrown IllegalStateException. Execute on RUNNING query is successful");
        }
        catch (IllegalStateException &) {
          // expected
        }
      }
      if (cq->isClosed()) {
        try {
          cq->execute();
          FWKEXCEPTION( "QueryTest::executeCQ : Should have thrown CQClosedException. execute() on CLOSED query is successful");
        }
        catch (CqClosedException &) {
          FWKINFO("CQ:- " << cq->getName() << " is closed hence registering it before execution");
          reRegisterCQ(cq);
        }
      }
    }
    catch (Exception &e) {
      FWKEXCEPTION( "QueryTest::executeCQ : Caught unexpected exception during cq execute operation: "<< e.getMessage());
    }
}
void QueryTest::executeCQWithIR(const CqQueryPtr cq)
{
  try {
      FWKINFO("Inside executeCQWithIR");
      if (cq->isStopped()) {
        SelectResultsPtr results = cq->executeWithInitialResults();
        FWKINFO( "executed query:- " << cq->getName() << " with initial results");
      }
      else if (cq->isRunning()) {
        try {
          cq->execute();
          FWKEXCEPTION( "QueryTest::executeCQWithIR : Should have thrown IllegalStateException. executeWithInitialResults on RUNNING query is successful");
        }
        catch (IllegalStateException &) {
          // expected
        }
      }
      if (cq->isClosed()) {
        try {
          cq->executeWithInitialResults();
          FWKEXCEPTION( "QueryTest::executeCQWithIR: Should have thrown CQClosedException. executeWithInitialResults() on CLOSED query is succussful");
        }
        catch (CqClosedException &) {
          FWKINFO( "CQ:- " << cq->getName() << " is closed hence registering it before executeWithInitialResults");
          reRegisterCQ(cq);
        }
      }
    }
    catch (Exception &e) {
      FWKEXCEPTION("QueryTest::executeCQWithIR : Caught unexpected exception during cq executeWithIR operation: " << e.getMessage());
    }
}
void QueryTest::reRegisterCQ(const CqQueryPtr cq)
{
  try {
    FWKINFO("re-registering CQ:- " << cq->getName());
    std::string query = cq->getQueryString();
    CqAttributesFactory cqFac;
    CqListenerPtr cqLstner(new MyCqListener());
    cqFac.addCqListener(cqLstner);
    CqAttributesPtr attr = cqFac.create();
    QueryServicePtr qs=checkQueryService();
    qs->newCq(query.c_str(), attr);
    FWKINFO("CQ re-registered:- " << cq->getName());
   }
   catch (Exception &e) {
   FWKEXCEPTION( "Caught unexpected exception during reRegisterCQ :" << e.getMessage());
   }
}
int32_t QueryTest::closeNormalAndRestart( const char * taskId ) {

  int32_t result = FWK_SUCCESS;

  try {
    if (m_cache != NULLPTR) {
      destroyAllRegions();
      bool keepalive = getBoolValue("keepAlive");
      bool isDC = getBoolValue("isDurable");

      if(isDC) {
        FWKINFO("keepalive is " << keepalive);
        m_cache->close(keepalive);
      }
      else {
        m_cache->close();
      }
      m_cache = NULLPTR;
      FWKINFO( "Cache closed." );
    }

    FrameworkTest::cacheFinalize();
    result = restartClientAndRegInt(taskId);
    bool isCq = getBoolValue( "cq" );
    if( result == FWK_SUCCESS && isCq ){
      result = doRegisterCqForConc(taskId);
    }

  } catch( CacheClosedException ignore ) {
  } catch( Exception & e ) {
     FWKEXCEPTION( "Caught an unexpected Exception during cache close: " << e.getMessage() );
  } catch( ... ) {
    FWKEXCEPTION( "Caught an unexpected unknown exception during cache close." );
  }
  return result;
}

// ----------------------------------------------------------------------------
int32_t QueryTest::restartClientAndRegInt( const char * taskId ) {

  int32_t  result = FWK_SUCCESS;

  // Do sleep  based on restartTime ,
  int32_t sleepSec = getIntValue("restartTime");
  if(sleepSec > 0 ) {
    perf::sleepSeconds(sleepSec);
  }

  result = doCreateUserDefineRegion(taskId);

  if(result == FWK_SUCCESS) {
     FWKINFO("Client Created Successfully.");
     RegionPtr region = getRegionPtr();
     bool isDurable = g_test->getBoolValue("isDurableReg");
     region->registerAllKeys(isDurable);
     const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
     if(strlen(durableClientId) > 0) {
       m_cache->readyForEvents();
     }
     result = FWK_SUCCESS;
  } else {
    FWKEXCEPTION( "Create Client Failed");
    return result;
  }
  // Sleep for updateReceiveTime to recieve entries,
  int32_t waitTime = getIntValue("updateReceiveTime");
  if(waitTime > 0 ) {
    perf::sleepSeconds(waitTime);
  }

  return result;
}

int32_t QueryTest::registerAllKeys()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In QueryTest::registerAllKeys()" );

  try {
  RegionPtr region = getRegionPtr();
  FWKINFO("QueryTest::registerAllKeys region name is " << region->getName());
  bool isDurable = g_test->getBoolValue("isDurableReg");
  region->registerAllKeys(isDurable);
  const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
  if(strlen(durableClientId) > 0) {
    m_cache->readyForEvents();
  }
  result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "QueryTest::registerAllKeys() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "QueryTest::registerAllKeys() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "QueryTest::registerAllKeys() Caught unknown exception." );
  }
  FWKINFO( "QueryTest::registerAllKeys() complete." );
  return result;
}
