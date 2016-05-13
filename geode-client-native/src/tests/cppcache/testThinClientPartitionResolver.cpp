/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include "BuiltinCacheableWrappers.hpp"
#include "impl/Utils.hpp"
#include "PartitionResolver.hpp"
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include <string>

#define ROOT_NAME "DistOps"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

// Include these 2 headers for access to CacheImpl for test hooks.
#include "CacheImplHelper.hpp"
#include "testUtils.hpp"

#include "ThinClientHelper.hpp"

using namespace gemfire;
class CustomPartitionResolver : public PartitionResolver
{
public:
  CustomPartitionResolver() { }
  ~CustomPartitionResolver() { }
  const char* getName()
  {
    LOG("CustomPartitionResolver::getName()");
    return "CustomPartitionResolver";
  }

 CacheableKeyPtr getRoutingObject(const EntryEvent& opDetails)
  {
     LOG("CustomPartitionResolver::getRoutingObject()");
     int32_t key = atoi(opDetails.getKey()->toString()->asChar());
     int32_t newKey = key + 5;
     return CacheableKey::create(newKey);
  }
};
PartitionResolverPtr cptr( new CustomPartitionResolver());

#define CLIENT1 s1p1
#define SERVER1 s2p1
#define SERVER2 s1p2
#define SERVER3 s2p2

bool isLocalServer = false;
const char * endPoints = CacheHelper::getTcrEndpoints(isLocalServer, 3);

static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, 1);

std::vector<char *> storeEndPoints( const char * points )
{
 std::vector<char *> endpointNames;
 if (points != NULL)
 {
   char * ep = strdup(points);
   char *token = strtok( ep, ",");
   while(token)
   {
     endpointNames.push_back(token);
     token = strtok(NULL, ",");
   }
 }
 ASSERT( endpointNames.size() == 3, "There should be 3 end points" );
 return endpointNames;
}

std::vector<char *> endpointNames = storeEndPoints(endPoints);

DUNIT_TASK_DEFINITION(CLIENT1, InitClient1_Pool_XML)
{
  initClient("client_pool_pr.xml");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
{
  if (isLocalServer) CacheHelper::initServer(1,"cacheserver1_pr.xml");
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
{
  if (isLocalServer) CacheHelper::initServer(2,"cacheserver2_pr.xml");
  LOG("SERVER2 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER3, CreateServer3)
{
  if (isLocalServer) CacheHelper::initServer(3,"cacheserver3_pr.xml");
  LOG("SERVER3 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
{
  initClient(true);
  
  getHelper()->createPoolWithLocators("__TEST_POOL1__", locatorsG);
  getHelper()->createRegionAndAttachPool2(regionNames[0], USE_ACK, "__TEST_POOL1__",cptr);
  getHelper()->createRegionAndAttachPool2(regionNames[1], NO_ACK, "__TEST_POOL1__",cptr);
                              
  LOG( "StepOne_Pooled_Locator complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Endpoint)
{
  initClient(true);
  
  char endpoints[1024] = {0};
  sprintf( endpoints, "%s,%s,%s", endpointNames.at(0),endpointNames.at(1),endpointNames.at(2));
  
  getHelper()->createPoolWithEPs("__TEST_POOL1__", endpoints);
  getHelper()->createRegionAndAttachPool2(regionNames[0], USE_ACK, "__TEST_POOL1__",cptr);
  getHelper()->createRegionAndAttachPool2(regionNames[1], NO_ACK, "__TEST_POOL1__",cptr);
                 
  LOG( "StepOne_Pooled_EndPoint complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, WarmUpTask)
{
  LOG("WarmUpTask started.");
  int failureCount = 0;
  int nonSingleHopCount = 0, metadatarefreshCount = 0;
  RegionPtr dataReg = getHelper()->getRegion(regionNames[1]);

  //This is to get MetaDataService going.
  for(int i = 0; i < 1000; i++)
  {
    CacheableKeyPtr keyPtr = dynCast<CacheableKeyPtr> (CacheableInt32::create(i));
    try {
      LOGINFO("CPPTEST: put item %d", i);   
      dataReg->put(keyPtr,(int32_t)keyPtr->hashcode());
      bool networkhop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetNetworkHopFlag();
      if (networkhop){
        failureCount++;
      } 
      StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
      StatisticsType* type = factory->findType("RegionStatistics");
      if(type ) {
        Statistics* rStats = factory->findFirstStatisticsByType(type);
       if (rStats) {
         nonSingleHopCount = rStats->getInt((char*)"nonSingleHopCount");
         metadatarefreshCount = rStats->getInt((char*)"metaDataRefreshCount");
       }
      }
      LOGINFO("CPPTEST: put success ");
    }
    catch(CacheServerException&) {
      // This is actually a success situation!
      //bool singlehop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetSingleHopFlag();
      //if (!singlehop) {
        LOGERROR("CPPTEST: Put caused networkhop");
        FAIL( "Put caused networkhop" );
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      //}
      //LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while putting key %s with hashcode %d", logmsg, (int32_t)keyPtr->hashcode());
    }
    catch(CacheWriterException&) {
      // This is actually a success situation! Once bug #521 is fixed.
      //bool singlehop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetSingleHopFlag();
      //if (!singlehop) {
        LOGERROR("CPPTEST: Put caused networkhop");
        FAIL( "Put caused networkhop" );
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      //}
      //LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while putting key %s with hashcode %d", logmsg, (int32_t)keyPtr->hashcode());
    }
    catch(Exception& ex) {
      LOGERROR("CPPTEST: Unexpected %s: %s", ex.getName(),ex.getMessage());
      FAIL(ex.getMessage());
    }
    catch(...) {    
     LOGERROR("CPPTEST: Put caused random exception in WarmUpTask");
     cleanProc();
     FAIL( "Put caused unexpected exception" );
     throw IllegalStateException("TEST FAIL");
    }
  }
  LOGINFO("nonSingleHopCount = %d metadatarefreshCount = %d ", nonSingleHopCount, metadatarefreshCount);
  ASSERT( failureCount > 0, "Count should be greater than 0" );
  //This is for the initial fetch when region is created we fetch metadata for it.
  ASSERT( nonSingleHopCount > 0, "nonSingleHopCount should be greater than 0" );
  ASSERT( metadatarefreshCount > 0, "metadatarefreshCount should be greater than 0" );  
  SLEEP(2000);

  LOG("WarmUpTask completed.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CheckPrSingleHopForIntKeysTask)
{
  LOG("CheckPrSingleHopForIntKeysTask started.");
  
  RegionPtr dataReg = getHelper()->getRegion(regionNames[1]);

  for(int i = 1000; i < 2000; i++)
  {
    CacheableKeyPtr keyPtr = dynCast<CacheableKeyPtr> (CacheableInt32::create(i));
    
    try {      
      LOGINFO("CPPTEST: Putting key %d with hashcode %d", i, (int32_t)keyPtr->hashcode());      
      dataReg->put(keyPtr,(int32_t)keyPtr->hashcode());
      bool networkhop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetNetworkHopFlag();
      ASSERT( !networkhop, "It is networkhop operation" );
    }
   catch(CacheServerException&) {
      // This is actually a success situation!
      //bool singlehop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetSingleHopFlag();
      //if (!singlehop) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL( "Put caused extra hop." );
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      //}
      LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while putting key %d with hashcode %d", i, (int32_t)keyPtr->hashcode());
    }
    catch(CacheWriterException&) {
      // This is actually a success situation! Once bug #521 is fixed.
      //bool singlehop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetSingleHopFlag();
      //if (!singlehop) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL( "Put caused extra hop." );
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      //}
      LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while putting key %d with hashcode %d", i, (int32_t)keyPtr->hashcode());
    }
    catch(Exception& ex) {
      LOGERROR("CPPTEST: Put caused unexpected %s: %s", ex.getName(),ex.getMessage());
      cleanProc();
      FAIL( "Put caused unexpected exception" );
      throw IllegalStateException("TEST FAIL");
    }
    catch(...) {
      LOGERROR("CPPTEST: Put caused random exception");
      cleanProc();
      FAIL( "Put caused unexpected exception" );
      throw IllegalStateException("TEST FAIL");
    }
    
    try {
      LOGINFO("CPPTEST: Destroying key %i with hashcode %d", i, (int32_t)keyPtr->hashcode());      
      dataReg->destroy(keyPtr);
      bool networkhop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetNetworkHopFlag();
      ASSERT( !networkhop , "It is networkhop operation" );      
    }
    catch(CacheServerException&) {
      // This is actually a success situation!
      //bool singlehop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetSingleHopFlag();
      //if (!singlehop) {
        LOGERROR("CPPTEST: Destroy caused extra hop.");
        FAIL( "Destroy caused extra hop." );
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      //}
      LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while destroying key %d with hashcode %d", i, (int32_t)keyPtr->hashcode());
    }
    catch(CacheWriterException&) {
      // This is actually a success situation! Once bug #521 is fixed.
      //bool singlehop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetSingleHopFlag();
      //if (!singlehop) {
        LOGERROR("CPPTEST: Destroy caused extra hop.");
        FAIL( "Destroy caused extra hop." );
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      //}
      LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while destroying key %d with hashcode %d", i, (int32_t)keyPtr->hashcode());
    }
    catch(Exception& ex) {
      LOGERROR("CPPTEST: Destroy caused unexpected %s: %s", ex.getName(),ex.getMessage());
      cleanProc();
      FAIL( "Destroy caused unexpected exception" );
      throw IllegalStateException("TEST FAIL");
    }
    catch(...) {
      LOGERROR("CPPTEST: Put caused random exception");
      cleanProc();
      FAIL( "Put caused unexpected exception" );
      throw IllegalStateException("TEST FAIL");
    }
  }  
  //SLEEP(20000);
  LOG("CPPTEST: CheckPrSingleHopForIntKeysTask:: For Region1.");
  RegionPtr dataReg1 = getHelper()->getRegion(regionNames[0]);
  
  for(int i = 1000; i < 2000; i++)
  {
    CacheableKeyPtr keyPtr = dynCast<CacheableKeyPtr> (CacheableInt32::create(i));
    
    try {      
      LOGINFO("CPPTEST: Putting key %d with hashcode %d", i, (int32_t)keyPtr->hashcode());      
      dataReg1->put(keyPtr,(int32_t)keyPtr->hashcode());
      bool networkhop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetNetworkHopFlag();
      ASSERT( !networkhop, "It is networkhop operation" );
    }
   catch(CacheServerException&) {
      // This is actually a success situation!
      //bool singlehop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetSingleHopFlag();
      //if (!singlehop) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL( "Put caused extra hop." );
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      //}
      LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while putting key %d with hashcode %d", i, (int32_t)keyPtr->hashcode());
    }
    catch(CacheWriterException&) {
      // This is actually a success situation! Once bug #521 is fixed.
      //bool singlehop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetSingleHopFlag();
      //if (!singlehop) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL( "Put caused extra hop." );
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      //}
      LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while putting key %d with hashcode %d", i, (int32_t)keyPtr->hashcode());
    }
    catch(Exception& ex) {
      LOGERROR("CPPTEST: Put caused unexpected %s: %s", ex.getName(),ex.getMessage());
      cleanProc();
      FAIL( "Put caused unexpected exception" );
      throw IllegalStateException("TEST FAIL");
    }
    catch(...) {
      LOGERROR("CPPTEST: Put caused random exception");
      cleanProc();
      FAIL( "Put caused unexpected exception" );
      throw IllegalStateException("TEST FAIL");
    }
    
    try {
      LOGINFO("CPPTEST: Destroying key %i with hashcode %d", i, (int32_t)keyPtr->hashcode());      
      dataReg1->destroy(keyPtr);
      bool networkhop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetNetworkHopFlag();
      ASSERT( !networkhop , "It is networkhop operation" );      
    }
    catch(CacheServerException&) {
      // This is actually a success situation!
      //bool singlehop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetSingleHopFlag();
      //if (!singlehop) {
        LOGERROR("CPPTEST: Destroy caused extra hop.");
        FAIL( "Destroy caused extra hop." );
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      //}
      LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while destroying key %d with hashcode %d", i, (int32_t)keyPtr->hashcode());
    }
    catch(CacheWriterException&) {
      // This is actually a success situation! Once bug #521 is fixed.
      //bool singlehop = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getAndResetSingleHopFlag();
      //if (!singlehop) {
        LOGERROR("CPPTEST: Destroy caused extra hop.");
        FAIL( "Destroy caused extra hop." );
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      //}
      LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while destroying key %d with hashcode %d", i, (int32_t)keyPtr->hashcode());
    }
    catch(Exception& ex) {
      LOGERROR("CPPTEST: Destroy caused unexpected %s: %s", ex.getName(),ex.getMessage());
      cleanProc();
      FAIL( "Destroy caused unexpected exception" );
      throw IllegalStateException("TEST FAIL");
    }
    catch(...) {
      LOGERROR("CPPTEST: Put caused random exception");
      cleanProc();
      FAIL( "Put caused unexpected exception" );
      throw IllegalStateException("TEST FAIL");
    }
  } 
  LOG("CheckPrSingleHopForIntKeysTask completed.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,CloseCache1)
{
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1,CloseServer1)
{
  if (isLocalServer) {
    CacheHelper::closeServer(1);
    LOG("SERVER1 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2,CloseServer2)
{
  if (isLocalServer) {
    CacheHelper::closeServer(2);
    LOG("SERVER2 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER3,CloseServer3)
{
  if (isLocalServer) {
    CacheHelper::closeServer(3);
    LOG("SERVER3 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateLocator1)
{
  //starting locator
  if ( isLocator )
    CacheHelper::initLocator( 1 );
    LOG("Locator1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1,CloseLocator1)
{
  //stop locator
  if ( isLocator ) {
    CacheHelper::closeLocator( 1 );
    LOG("Locator1 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator_PR)
{
  //starting servers
  if ( isLocalServer )
    CacheHelper::initServer( 1, "cacheserver1_pr.xml", locatorsG );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2_With_Locator_PR)
{
  //starting servers
  if ( isLocalServer )
    CacheHelper::initServer( 2, "cacheserver2_pr.xml", locatorsG );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER3, CreateServer3_With_Locator_PR)
{
  //starting servers
  if ( isLocalServer )
    CacheHelper::initServer( 3, "cacheserver3_pr.xml", locatorsG );
}
END_TASK_DEFINITION

DUNIT_MAIN
{
  CacheableHelper::registerBuiltins(true);
  
  // First pool with endpoints
  
  CALL_TASK(CreateServer1);
  CALL_TASK(CreateServer2);  
  CALL_TASK(CreateServer3);
  
  CALL_TASK(StepOne_Pooled_Endpoint);
  
  CALL_TASK(WarmUpTask);
  CALL_TASK(CheckPrSingleHopForIntKeysTask);
  
  CALL_TASK(CloseCache1);
  
  CALL_TASK(CloseServer1);
  CALL_TASK(CloseServer2);
  CALL_TASK(CloseServer3);
  
  // Then pool with locator
  
  CALL_TASK(CreateLocator1);
  
  CALL_TASK(CreateServer1_With_Locator_PR);
  CALL_TASK(CreateServer2_With_Locator_PR);  
  CALL_TASK(CreateServer3_With_Locator_PR);
  
  CALL_TASK(StepOne_Pooled_Locator);
  
  CALL_TASK(WarmUpTask);  
  CALL_TASK(CheckPrSingleHopForIntKeysTask);    
  
  CALL_TASK(CloseCache1);
  
  CALL_TASK(CloseServer1);
  CALL_TASK(CloseServer2);
  CALL_TASK(CloseServer3);
  
  CALL_TASK(CloseLocator1);
}
END_MAIN
