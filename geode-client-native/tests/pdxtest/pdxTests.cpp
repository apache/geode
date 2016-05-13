/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * pdxTest.cpp
 */
// ----------------------------------------------------------------------------
#include "pdxTests.hpp"
#include <ace/Time_Value.h>
#include <time.h>
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/RegionHelper.hpp"
#include "fwklib/FwkExport.hpp"
#include "fwklib/PoolHelper.hpp"
#include "security/CredentialGenerator.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "fwklib/PaceMeter.hpp"
#include "QueryHelper.hpp"
#include "Query.hpp"
#include "QueryService.hpp"

#include <vector>
#include <map>

namespace fwkpdxtests {
  std::string REGIONSBB("Regions");
}
using namespace fwkpdxtests;
using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::pdxtests;
using namespace gemfire::testframework::security;

Pdxtests * g_test = NULL;
static CacheableHashMapPtr RegionSnapShot = NULLPTR;

static CacheableHashSetPtr destroyedKey = NULLPTR;
static bool isSerialExecution = false;
static bool isEmptyClient=false;
static bool isThinClient=false;
static ACE_Recursive_Thread_Mutex * m_lock;


template <typename T1, typename T2>
bool genericValCompare(T1 value1, T2 value2) /*const*/
{
  if (value1 != value2)
    return false;
  return true;    
}
 
template <typename T1, typename T2>
bool genericCompare(T1* value1, T2* value2, int length) /*const*/
{
  int i = 0;
  while (i < length)
  {
    if (value1[i] != value2[i])
      return false;
    else
      i++;
  }
  return true;
}


// ----------------------------------------------------------------------------

TESTTASK initialize(const char * initArgs) {
  int32_t result = FWK_SUCCESS;
  if (g_test == NULL) {
    FWKINFO( "Initializing Pdxtests library." );
    m_lock = new ACE_Recursive_Thread_Mutex();
    try {
      g_test = new Pdxtests(initArgs);
    } catch (const FwkException &ex) {
      FWKSEVERE( "initialize: caught exception: " << ex.getMessage() );
      result = FWK_SEVERE;
    }
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Finalizing Pdxtest library." );
  if (g_test != NULL) {
    g_test->cacheFinalize();
    delete g_test;
    g_test = NULL;
  }
  if(m_lock != NULL)
   {delete m_lock;
    m_lock = NULL;
}
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doCloseCache() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Closing cache, disconnecting from distributed system." );
  if (g_test != NULL) {
    g_test->cacheFinalize();
  }
  return result;
}

//----------------------------------------------------------------------------

void Pdxtests::checkTest( const char * taskId,bool ispool) {
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
   setCacheLevelEp(ispool);
   cacheInitialize( pp );
   // QueryTest specific initialization
  // none
  }
}

// ----------------------------------------------------------------------------

TESTTASK doCreateRegion(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateRegion called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->createRegion();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateRegion caught exception: " << ex.getMessage() );
  }
  return result;
}
         
TESTTASK dumpDataOnBB(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doDumpToBB called for task: " << taskId );
   try {
     g_test->checkTest(taskId);
     result = g_test->DumpToBB();
   } catch (FwkException ex) {
     result = FWK_SEVERE;
     FWKSEVERE( "doDumpToBB caught exception: " << ex.getMessage() );
   }
   return result;
}

TESTTASK doPuts(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPuts called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->puts();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPuts caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doGets(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGets called for task: " << taskId );
   try {
     g_test->checkTest(taskId);
     result = g_test->gets();
   } catch (FwkException ex) {
     result = FWK_SEVERE;
     FWKSEVERE( "doGets caught exception: " << ex.getMessage() );
   }
   return result;
}

TESTTASK doPopulateRegion(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopulateRegion called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->populateRegion();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopulateRegion caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doRandomEntryOperation(const char * taskId){
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRandomEntryOperation called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->randomEntryOperation();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRandomEntryOperation caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doCreatePool(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO("doCreatePool called for task: " << taskId );
  try {
    g_test->checkTest(taskId,true);
    result = g_test->createPools();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreatePool caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK verifyFromSnapshot(const char * taskId){
  int32_t result = FWK_SUCCESS;
  FWKINFO( "verifyFromSnapshot called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->verifyFromSnapshotOnly();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "verifyFromSnapshot caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doRegisterAllKeys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterAllKeys called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->registerAllKeys();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterAllKeys caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doVerifyAndModifyPdxInstance(const char * taskId){
  int32_t result = FWK_SUCCESS;
  FWKINFO("doVerifyAndModifyPdxInstance called for task: " << taskId );
  try{
    g_test->checkTest(taskId);
    result = g_test->doVerifyModifyPdxInstance();
  }
  catch(FwkException ex)
  {
	result = FWK_SEVERE;
    FWKSEVERE( "doVerifyAndModifyPdxInstance caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doVerifyAndModifyAutoPdxInstance(const char * taskId){
  int32_t result = FWK_SUCCESS;
  FWKINFO("doVerifyAndModifyPdxInstance called for task: " << taskId );
  try{
    g_test->checkTest(taskId);
    result = g_test->doVerifyModifyAutoPdxInstance();
  }
  catch(FwkException ex)
  {
	result = FWK_SEVERE;
    FWKSEVERE( "doVerifyModifyAutoPdxInstance caught exception: " << ex.getMessage() );
  }
  return result;
}

// -----------------------------------------------------------------------------
int32_t Pdxtests::createRegion() {
  int32_t result = FWK_SEVERE;
  try {
    FWKINFO("Inside createRegion()");
    RegionHelper help(g_test);
    RegionPtr region = help.createRootRegion(m_cache);
    RegionAttributesPtr atts = region->getAttributes();
    resetValue("useTransaction");
    //bool m_istransaction = getIntValue("useTransactions");
    isEmptyClient =!(atts->getCachingEnabled());  //!(region.Attributes.CachingEnabled);
    isThinClient = atts->getCachingEnabled(); //region.Attributes.CachingEnabled;
    std::string objectType = getStringValue("objectType");
    int32_t versionnum = getIntValue("versionNum");
    if(objectType.c_str() != NULL){
		if(objectType == "PdxVersioned" && versionnum == 1)
		{
		  Serializable::registerPdxType(PdxTests::PdxVersioned1::createDeserializable);
		  FWKINFO("Registering Pdx Type with version no 1");
		}
		if(objectType == "PdxVersioned" && versionnum == 2)
		{
		  Serializable::registerPdxType(PdxTests::PdxVersioned2::createDeserializable);
		  FWKINFO("Registering Pdx Type with version no 2");
		}
		if(objectType == "AutoPdxVersioned" && versionnum == 1)
		{
			FWKINFO("Registering before AutoPdxVersioned Type with version no 1");
		  Serializable::registerPdxType(AutoPdxTests::AutoPdxVersioned1::createDeserializable);
		  FWKINFO("Registering AutoPdxVersioned Type with version no 1");
		}
		if(objectType == "AutoPdxVersioned" && versionnum == 2)
		{
			FWKINFO("Registering before AutoPdxVersioned Type with version no 2");
		  Serializable::registerPdxType(AutoPdxTests::AutoPdxVersioned2::createDeserializable);
		  FWKINFO("Registering AutoPdxVersioned Type with version no 2");
		}
		if(objectType == "PdxType")
		{
		   FWKINFO("Registering Pdx Type");
		   Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
		   Serializable::registerPdxType(PdxTests::Address::createDeserializable);
		}
		if(objectType == "AutoPdxType")
		{
			FWKINFO("Registering AutoPdxType Type");
			Serializable::registerPdxType(PdxTestsAuto::PdxType::createDeserializable);
			Serializable::registerPdxType(PdxTestsAuto::Address::createDeserializable);
		}
		if(objectType == "Nested")
		{
			FWKINFO("Registering Pdx with Nested Type");
			Serializable::registerPdxType(PdxTests::NestedPdx::createDeserializable);
			Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
			Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
			Serializable::registerPdxType(PdxTests::PdxTypes3::createDeserializable);
			Serializable::registerPdxType(PdxTests::PdxTypes4::createDeserializable);
			Serializable::registerPdxType(PdxTests::PdxTypes5::createDeserializable);
			Serializable::registerPdxType(PdxTests::PdxTypes6::createDeserializable);
			Serializable::registerPdxType(PdxTests::PdxTypes7::createDeserializable);
			Serializable::registerPdxType(PdxTests::PdxTypes8::createDeserializable);
		}
		if(objectType == "AutoNested")
		{
			FWKINFO("Registering AutoPdx with Nested Type");
			Serializable::registerPdxType(AutoPdxTests::PdxTypes1::createDeserializable);
			Serializable::registerPdxType(AutoPdxTests::PdxTypes2::createDeserializable);
			Serializable::registerPdxType(AutoPdxTests::NestedPdx::createDeserializable);

		}
    }
    isSerialExecution=getBoolValue("serialExecution");
    if (isSerialExecution)
    {
      RegionSnapShot = CacheableHashMap::create();
      destroyedKey = CacheableHashSet::create();
    }
      if (region == NULLPTR)
      {
        FWKEXCEPTION("DoCreateRegion()  could not create region.");
      }

    std::string key(region->getName());
    bbIncrement(REGIONSBB, key);
    FWKINFO( "PdxTest::createRegion Created region " << region->getName() << std::endl);
    result = FWK_SUCCESS;

  } catch (Exception e) {
    FWKEXCEPTION( "Pdxtest::createRegion FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Pdxtest::createRegion FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Pdxtest::createRegion FAILED -- caught unknown exception." );
  }

  return result;
}

// ----------------------------------------------------------------------------

#ifndef WIN32
#include <unistd.h>
#endif

//-----------------------------------------------------------------------------

RegionPtr Pdxtests::getRegionPtr( const char * reg )
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
  try {
    if ( name.empty() ) { // just get a random root region
      VectorOfRegion rootRegionVector;
      m_cache->rootRegions( rootRegionVector );
      int32_t size = rootRegionVector.size();

      if ( size == 0 ) {
        FWKEXCEPTION( "In Pdxtests::getRegionPtr()  No regions exist." );
      }

      FWKINFO( "Getting a random root region." );
      region = rootRegionVector.at( GsRandom::random( size ) );
    }
    else {
      FWKINFO( "Getting region: " << name );
      if (m_cache == NULLPTR) {
        FWKEXCEPTION( "Failed to get region: " << name << "  cache ptr is null." );
      }
      region = m_cache->getRegion( name.c_str() );
      if (region == NULLPTR) {
        FWKEXCEPTION( "Failed to get region: " << name );
      }
    }
  } catch( CacheClosedException e ) {
    FWKEXCEPTION( "In PerfTest::getRegionPtr()  CacheFactory::getInstance encountered "
      "CacheClosedException: " << e.getMessage() );
  } catch( EntryNotFoundException e ) {
    FWKEXCEPTION( "In PerfTest::getRegionPtr()  CacheFactory::getInstance encountered "
      "EntryNotFoundException: " << e.getMessage() );
  } catch( IllegalArgumentException e ) {
    FWKEXCEPTION( "In PerfTest::getRegionPtr()  CacheFactory::getInstance encountered "
      "IllegalArgumentException: " << e.getMessage() );
  }
  return region;
}

//-----------------------------------------------------------------------------
int32_t Pdxtests::createPools() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Pdxtest::createPool()" );
  try {
    PoolHelper help(g_test);
    PoolPtr pool = help.createPool();
    FWKINFO( "Pdxtest::createPool Created Pool " << pool->getName() << std::endl);
    result = FWK_SUCCESS;
  } catch (Exception e) {
    FWKEXCEPTION( "Pdxtest::createPool FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Pdxtest::createPool FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Pdxtest::createPool FAILED -- caught unknown exception. " );
  }
  return result;
}
// --------------------------------------------------------------------------
int32_t Pdxtests::DumpToBB(){
  int32_t result=FWK_SEVERE;
  FWKINFO("In Pdxtests::DumpToBB");
  try{
		int versionNo=getIntValue("versionNum");
		std::string objectType = getStringValue("objectType");
		RegionPtr region=getRegionPtr();
		CacheableHashMapPtr RegionServerMap = CacheableHashMap::create();
		VectorOfCacheableKey serverKey;
		region->serverKeys(serverKey);
		for(int i=0;i<serverKey.size();i++){
		  CacheableKeyPtr key=dynCast<CacheableKeyPtr>(serverKey.at(i));
		  PdxSerializablePtr pdxVal=dynCast<PdxSerializablePtr>(region->get(key));
		 // std::string keyStr=key->toString()->asChar();
		  RegionServerMap->update(key,pdxVal);
		}
		setBBStrMap(RegionServerMap,objectType,versionNo);
		result=FWK_SUCCESS;
  }
  catch (Exception e) {
     FWKEXCEPTION( "Pdxtest::DumpToBB FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
      FWKEXCEPTION( "Pdxtest::DumpToBB FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
      FWKEXCEPTION( "Pdxtest::DumpToBB FAILED -- caught unknown exception. " );
  }
  return result;
}

//----------------------------------------------------------------------------

int32_t Pdxtests::randomEntryOperation(){
  int32_t result=FWK_SEVERE;
  FWKINFO("In Pdxtests::randomEntryOperation");
  try{
    RegionPtr region=getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();
    int versionNo=getIntValue("versionNum");
    std::string objectType = getStringValue("objectType");
    int timedInterval = getIntValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    int32_t opsSec = getIntValue( "opsSecond" );
    opsSec = ( opsSec < 1 ) ? 0 : opsSec;
    // Loop over key set sizes
    resetValue("distinctKeys");
    int numKeys = getIntValue("distinctKeys");
    int clientNum=g_test->getClientId();


    int numClients = getIntValue("clientCount");
    int numServers = getIntValue("serverCount");
    std::string clntid = "";
    char name[32] = {'\0'};
    int32_t Cid=(g_test->getClientId() - numServers);
    ACE_OS::sprintf(name,"ClientName_%d",Cid);
    int roundPosition = 0;
    bool isdone = 0;
    if (isSerialExecution)
    {
      bbSet("RoundPositionBB", "roundPosition", 1);
      int roundPosition = (int)bbGet("RoundPositionBB", "roundPosition");
      char buf[128];
      sprintf(buf, "ClientName_%d", roundPosition);
      clntid = buf;
    }
    else
    {
      clntid = name;
    }
    bbSet("RoundPositionBB", "done", false);
    resetValue("numThreads");
    int numThreads = getIntValue("numThreads");
    while(true){
      FWKINFO("roundPosition = " << roundPosition <<" and numClients = " << numClients << " clientnum= " << clientNum << " clientid = " << clntid << " and name = " << name);
      if (roundPosition > numClients)
        break;
      try{
    	 //---
    	 if(clntid == name)
    	 {
    	  	 try {

    		  PdxEntryTask * entrytask = new PdxEntryTask (region,numKeys,RegionSnapShot,destroyedKey,isSerialExecution,objectType,versionNo,g_test);
			  if (isSerialExecution)
			  {
				if (!clnt->timeInterval(entrytask, timedInterval, numThreads, 10 * timedInterval)){
				  FWKEXCEPTION( "In doEntryOperation()  Timed run timed out." );
				}
			     waitForSilenceListenerComplete(30,2000);
			     /*std::string RegionSnapShotStr1;
			     const char* stringValue = "";
			     for (HashMapOfCacheable::Iterator MapItr = RegionSnapShot->begin(); MapItr != RegionSnapShot->end(); MapItr++) {
			      //for(MapItr=tempMap.begin();MapItr != tempMap.end();MapItr++){
			    	std::string key=MapItr.first()->toString()->asChar();
			         NestedPdxPtr value = dynCast<NestedPdxPtr>(MapItr.second());
			          if(value != NULLPTR)
			            stringValue = value->getString();
			          std::string val = std::string(stringValue);
			           RegionSnapShotStr1 += key+"="+val+",";
			     }
                             std::string DestroyKeyStr1;
			     for (CacheableHashSet::Iterator it = destroyedKey->begin(); it != destroyedKey->end(); it++){
			      //for(uint32_t i=0;i<tempVec.size();i++)
			      //{
			    	CacheableKeyPtr key = *it;
			        std::string key1=key->toString()->asChar();
			        DestroyKeyStr1 += key1+",";
			      }
			      FWKINFO("rjk: doEntryOperation Concatenated DestroyKey length is "<<DestroyKeyStr1.length()<<" string is = "<< DestroyKeyStr1);
			      //to be deleted
			      VectorOfCacheableKey keyVec;
			      region->serverKeys(keyVec);
			      int beforeSize=keyVec.size();
			      VectorOfCacheableKey Exkeys;
			      region->keys(Exkeys);
			      int beforeSize1=Exkeys.size();
			      FWKINFO("rjk: doEntryOperation server entry size is " << beforeSize << " local cache size is " <<beforeSize1);
*/
			     setBBStrMap(RegionSnapShot,objectType,versionNo);
			     setBBStrVec(destroyedKey);
			     roundPosition = (int32_t)bbIncrement("RoundPositionBB", "roundPosition");
			     bbSet("RoundPositionBB", "roundPosition", roundPosition);
			     //clntid = String.Format("Client.{0}", roundPosition);
			     bbSet("RoundPositionBB", "done", true);
			     bbSet("RoundPositionBB", "VerifyCnt", 1);
			  }
			  else
			  {
			    if (!clnt->timeInterval(entrytask, timedInterval, numThreads, 10 * timedInterval)){
				  FWKEXCEPTION( "In doEntryOperation()  Timed run timed out." );
			    }
			    break;
			  }
			}
			catch (TimeoutException ex){
			  FWKEXCEPTION("In DoPuts()  Timed run timed out:" << ex.getMessage());
			}
			catch (Exception ex){
			  FWKEXCEPTION("In DoPuts()  Exception caught:"<< ex.getMessage());
			}
    	 }
    	 //---
   	else{
          for(;;){
            isdone = bbGet("RoundPositionBB", "done");
            if(isdone)
              break;
          }
            if (isdone)
            {
              waitForSilenceListenerComplete(30,2000);
              bbSet("RoundPositionBB", "done", false);
              bbIncrement("RoundPositionBB", "VerifyCnt");
              verifyFromSnapshotOnly();
             /* FWKINFO("rjk: verifyFromSnapshotOnly after " << retvalue);
              if(retvalue == FWK_SUCCESS){
                bbSet("RoundPositionBB", "done", false);
                bbIncrement("RoundPositionBB", "VerifyCnt");
                FWKINFO("rjk: verifyFromSnapshotOnly success");
              }
              else {
            	bbIncrement("RoundPositionBB", "VerifyCnt");
                bbSet("RoundPositionBB", "done", true);
                bbSet("RoundPositionBB", "roundPosition", numClients + 1);
                FWKINFO("rjk: verifyFromSnapshotOnly fail "<< (int)bbGet("RoundPositionBB", "VerifyCnt"));
                break;
              }*/
            }
            perf::sleepSeconds( 1 );
       }

		if (isSerialExecution)
		{
			int verifyCnt = (int)bbGet("RoundPositionBB", "VerifyCnt");
			while (verifyCnt < numClients - 1)
			{
			   verifyCnt = (int)bbGet("RoundPositionBB", "VerifyCnt");
			   perf::sleepSeconds( 1 );
			}
			//Util.BBSet("RoundPositionBB", "VerifyCnt", 0);
			roundPosition = (int)bbGet("RoundPositionBB", "roundPosition");
			char buf[128];
			sprintf(buf, "ClientName_%d", roundPosition);
			clntid = buf;

			char name[32] = {'\0'};
			int32_t Cid=(g_test->getClientId() - numServers);
			ACE_OS::sprintf(name,"ClientName_%d",Cid);
		}
       perf::sleepSeconds( 3 );

      }
      catch(TimeoutException e){
    	  bbSet("RoundPositionBB", "done", true);
    	  bbSet("RoundPositionBB", "roundPosition", numClients + 1);
        FWKEXCEPTION("In DoRandomEntryOperation()  Timed run timed out.");
      }
      catch(Exception e){
    	  bbSet("RoundPositionBB", "done", true);
    	  bbSet("RoundPositionBB", "roundPosition", numClients + 1);
        FWKEXCEPTION("randomEntryOperation() Caught Exception:" << e.getMessage());
      }
      perf::sleepSeconds( 3 );
    }
    result = FWK_SUCCESS;
  }
  catch(Exception e){
    FWKEXCEPTION("randomEntryOperation() Caught Exception:" << e.getMessage());
  }
  FWKINFO("randomEntryOperation() complete");
  return result;
}

//----------------------------------------------------------------------------

int32_t Pdxtests::puts() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Pdxtest::puts()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }

    int versionNo=getIntValue("versionNum");
    std::string objectType = getStringValue("objectType");
    resetValue("distinctKeys");

    int32_t numKeys = getIntValue("distinctKeys");
    while (numKeys > 0) { // keys loop
      resetValue( "valueSizes" );
      int32_t valSize = getIntValue("valueSizes");
      while(valSize > 0) {//ValSize Loop
        resetValue("numThreads");
        int32_t numThreads = getIntValue("numThreads");
        while (numThreads > 0) { // thread loop
          PdxEntryTask * puts = new PdxEntryTask(region,numKeys,RegionSnapShot,destroyedKey,isSerialExecution,objectType,versionNo,g_test);
          FWKINFO( "Running warmup task." );

          if ( !clnt->runIterations( puts, numKeys, 1, 0 ) ) {
            FWKEXCEPTION( "In doPuts()  Warmup timed out." );
          }
          perf::sleepSeconds(3);
          FWKINFO( "Running timed task." );
          if ( !clnt->timeInterval( puts, timedInterval, numThreads, 10 * timedInterval ) ) {
            FWKEXCEPTION( "In doPuts()  Timed run timed out." );
          }
          if(clnt->getTaskStatus() == FWK_SEVERE)
            FWKEXCEPTION( "Exception during put task");

          numThreads = getIntValue("numThreads");
          if (numThreads > 0) {
            perf::sleepSeconds(3); // Put a marker of inactivity in the stats
          }
          FWKINFO( "Updated " << puts->getIters() << " entries." );
          delete puts;
          if(isSerialExecution){
            setBBStrMap(RegionSnapShot,objectType,versionNo);
            setBBStrVec(destroyedKey);
          }
        }
        valSize = getIntValue("valueSizes");
        if ( valSize > 0 ) {
          perf::sleepSeconds(3); // Put a marker of inactivity in the stats// thread loop
        }
      }
      numKeys = getIntValue("distinctKeys");
      if (numKeys > 0) {
        perf::sleepSeconds(3); // Put a marker of inactivity in the stats
      }
      FWKINFO("Updates Finish");
    }
    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Pdxtest::puts() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Pdxtest::puts() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Pdxtest::puts() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Pdxtest::puts() Caught unknown exception." );
  }
  perf::sleepSeconds(3); // Put a marker of inactivity in the stats
  FWKINFO( "Pdxtest::puts() complete." );
  return result;
}

//----------------------------------------------------------------------------------------------

int32_t Pdxtests::gets(){
  int32_t result = FWK_SEVERE;
  try{
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();
    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    int maxTime = 10 * timedInterval;
    resetValue("distinctKeys");
    int32_t numKeys = getIntValue("distinctKeys");
    resetValue("versionNum");
    int32_t versionNo = getIntValue("versionNum");
    std::string objectType = getStringValue("objectType");
    resetValue("numThreads");
    int32_t numThreads = getIntValue("numThreads");
    while(numThreads > 0){
      PdxEntryTask * gets = new PdxEntryTask(region,numKeys,RegionSnapShot,destroyedKey,isSerialExecution,objectType,versionNo,g_test);
      FWKINFO( "Running warmup task." );
      if ( !clnt->runIterations( gets, numKeys, 1, 0 ) ) {
         FWKEXCEPTION( "In TestTask_gets()  Warmup timed out." );
      }
      region->localInvalidateRegion();
      perf::sleepSeconds( 3 );
      FWKINFO( "Running timed task." );
      if ( !clnt->timeInterval( gets, timedInterval, numThreads, maxTime ) ) {
        FWKEXCEPTION( "In TestTask_gets()  Timed run timed out." );
      }
      if(clnt->getTaskStatus() == FWK_SEVERE)
          FWKEXCEPTION( "Exception during get task");

      numThreads = getIntValue( "numThreads" );
      if (numThreads > 0) {
        perf::sleepSeconds(3); // Put a marker of inactivity in the stats
      }
      delete gets;
      //setBBStrMap(RegionSnapShot,objectType,versionNo);
      //setBBStrVec(destroyedKey);
    }
    result = FWK_SUCCESS;
  }
  catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::gets Caught Exception: " << e.getMessage() );
  }catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::gets Caught FwkException: " << e.getMessage() );
  }catch ( ... ) {
    FWKEXCEPTION( "PerfTest::gets Caught unknown exception." );
  } 
  return result;
}

//----------------------------------------------------------------------------------------------

int32_t Pdxtests::populateRegion() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In ::populateRegion()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();
    resetValue("distinctKeys");
    int32_t numKeys = getIntValue("distinctKeys");
    resetValue("versionNum");
    int32_t versionNo = getIntValue("versionNum");
    std::string objectType = getStringValue("objectType");
    PdxEntryTask addTask(region,numKeys,RegionSnapShot,destroyedKey,isSerialExecution,objectType,versionNo,g_test);
    FWKINFO( "Populating region." );
    if (!clnt->runIterations(&addTask, numKeys,1, 0)) {
      FWKEXCEPTION( "In populateRegion()  Population timed out." );
    }
    if(isSerialExecution){
      setBBStrMap(RegionSnapShot,objectType,versionNo);
      setBBStrVec(destroyedKey);
    }

    result= FWK_SUCCESS;
  } catch (std::exception e) {
    FWKEXCEPTION( "Pdxtest::populateRegion() Caught std::exception: " << e.what() );
  } catch (Exception e) {
    FWKEXCEPTION( "Pdxtest::populateRegion() Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Pdxtest::populateRegion() Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Pdxtest::populateRegion() Caught unknown exception." );
  }
  FWKINFO( "Pdxtest::populateRegion() complete." );
  return result;
}

// ----------------------------------------------------------------------------------------------
void Pdxtests::setBBStrMap(CacheableHashMapPtr tempMap,std::string objecttype,int32_t versionno){
  std::string RegionSnapShotStr;
  const char* stringValue = "";
  for (HashMapOfCacheable::Iterator MapItr = tempMap->begin(); MapItr != tempMap->end(); MapItr++) {
  //for(MapItr=tempMap.begin();MapItr != tempMap.end();MapItr++){
	std::string key=MapItr.first()->toString()->asChar();
    if(objecttype == "PdxVersioned" && versionno == 1){
      PdxVersioned1Ptr value = dynCast<PdxVersioned1Ptr>(MapItr.second());
      if(value != NULLPTR)
        stringValue = value->getString();
      }else if(objecttype == "PdxVersioned" && versionno == 2){
    	PdxVersioned2Ptr value = dynCast<PdxVersioned2Ptr>(MapItr.second());
      if(value != NULLPTR)
        stringValue = value->getString();
      }else if(objecttype == "Nested"){
    	  PdxTests::NestedPdxPtr value = dynCast<PdxTests::NestedPdxPtr>(MapItr.second());
       if(value != NULLPTR)
        stringValue = value->getString();
       }
      else if(objecttype == "AutoPdxType"){
         PdxTestsAuto::PdxTypePtr value = dynCast<PdxTestsAuto::PdxTypePtr>(MapItr.second());
        if(value != NULLPTR)
          stringValue = value->getString();
      }else if(objecttype == "AutoPdxVersioned" && versionno == 1){
    	AutoPdxTests::AutoPdxVersioned1Ptr value = dynCast<AutoPdxTests::AutoPdxVersioned1Ptr>(MapItr.second());
        if(value != NULLPTR)
          stringValue = value->getString();
      }else if(objecttype == "AutoPdxVersioned" && versionno == 2){
         AutoPdxTests::AutoPdxVersioned2Ptr value = dynCast<AutoPdxTests::AutoPdxVersioned2Ptr>(MapItr.second());
         if(value != NULLPTR)
          stringValue = value->getString();
      }
      else if(objecttype == "AutoNested"){
    	 AutoPdxTests::NestedPdxPtr value = dynCast<AutoPdxTests::NestedPdxPtr>(MapItr.second());
         if(value != NULLPTR)
          stringValue = value->getString();
     }
    std::string val = std::string(stringValue);
    RegionSnapShotStr += key+"="+val+",";
  }
  FWKINFO("Concatenated string is = "<< RegionSnapShotStr);
  bbSet("RegionSnapShot","regionsnapshot",RegionSnapShotStr);
}

// ----------------------------------------------------------------------------------------------

void Pdxtests::setBBStrVec(CacheableHashSetPtr tempVec){
	FWKINFO("Inside setBBStrVec ");
  FWKINFO("Inside setBBStrVec and DestroyVector Size is "<< tempVec->size());
  std::string DestroyKeyStr;
  for (CacheableHashSet::Iterator it = tempVec->begin(); it != tempVec->end(); it++){
  //for(uint32_t i=0;i<tempVec.size();i++)
  //{
	CacheableKeyPtr key = *it;
    std::string key1=key->toString()->asChar();
    DestroyKeyStr += key1+",";
  }
  FWKINFO("Concatenated DestroyKey length is "<<DestroyKeyStr.length()<<" string is = "<< DestroyKeyStr);
  bbSet("DestroyedKeys", "destroyedKeys",DestroyKeyStr);
}

// ----------------------------------------------------------------------------------------------

//Method to retrieve the key/value pair from BB and store it back in a map.
std::map<std::string,std::string> Pdxtests::getBBStrMap(std::string BBString){
  FWKINFO("Inside getBBStrMap");
  std::map<std::string,std::string> KeyValMap;
  std::string s = BBString;
  char c=',';
  std::vector<std::string> v;
  size_t i = 0;
  size_t j = s.find(c);
  while (j != std::string::npos) {
    v.push_back(s.substr(i, j-i));
    i = ++j;
    j = s.find(c, j);
    if (j == std::string::npos)
      v.push_back(s.substr(i, s.length( )));
  }
  for (uint32_t i = 0; i < v.size(); ++i) {
    std::string VecStr=v.at(i);
    size_t position=VecStr.find('=');
    if (position != std::string::npos) {
      std::string KEY = VecStr.substr(0, position);
      std::string VALUE = VecStr.substr(position + 1);
      KeyValMap[KEY]=VALUE;
    }
  }
  return KeyValMap;
}

//---------------------------------------------------------------------------------------------

std::vector<std::string>Pdxtests::getBBStrVec(std::string BBString){
  FWKINFO("Inside getBBStrVec");
  std::vector<std::string> DestKeyVec;
  std::string s = BBString;
  char c=',';
  size_t i = 0;
  size_t j = s.find(c);
  while (j != std::string::npos) {
    DestKeyVec.push_back(s.substr(i, j-i));
    i = ++j;
    j = s.find(c, j);
    if (j == std::string::npos)
      DestKeyVec.push_back(s.substr(i, s.length( )));
    
   }
   return DestKeyVec;
}

// ----------------------------------------------------------------------------------------------

void Pdxtests::GetPdxVersionedVal(CacheableKeyPtr key,SerializablePtr myPdxVal,int32_t versionNo,bool expectedVal,const char* pdxTostring){
  FWKINFO("Inside GetPdxVersionedVal Version No is :" << versionNo);
  RegionPtr region=getRegionPtr();
  if(instanceOf<PdxTests::PdxVersioned1Ptr>(myPdxVal)){
	  PdxTests::PdxVersioned1Ptr  pdx1Val=dynCast<PdxTests::PdxVersioned1Ptr>(myPdxVal);
    if(pdx1Val != NULLPTR)
      expectedVal=true;
    if(expectedVal)
    {
      try{
        //PdxType1V1Ptr pdx1RegVal=dynCast<PdxType1V1Ptr>(region->get(key));
        if(strcmp(pdx1Val->getString(),pdxTostring)!=0)
        {
          FWKEXCEPTION("verifyFromSnapshotOnly actual value "<<  pdx1Val->getString() << " is not same as expected val " << pdxTostring <<" for key " <<key->toString()->asChar());
        }
      }
      catch(KeyNotFoundException){}
      catch(Exception e){FWKEXCEPTION("Pdx with version 1 got this "<< e.getMessage());}
    }
  }
  else if(instanceOf<PdxTests::PdxVersioned2Ptr>(myPdxVal)){
	  PdxTests::PdxVersioned2Ptr pdx2Val=dynCast<PdxTests::PdxVersioned2Ptr>(myPdxVal);
    if(pdx2Val != NULLPTR)
      expectedVal=true;
    if(expectedVal)
    {
      try{
        //PdxTypes1V2Ptr pdx2RegVal=dynCast<PdxTypes1V2Ptr>(region->get(key));
        if(strcmp(pdx2Val->getString(),pdxTostring)!= 0)
        {
          FWKEXCEPTION("verifyFromSnapshotOnly actual value "<<  pdx2Val->getString() << " is not same as expected val " << pdxTostring <<" for key " <<key->toString()->asChar());
        }
      }
      catch(KeyNotFoundException){}
      catch(Exception e){FWKEXCEPTION("Pdx with version 2 got this "<< e.getMessage());}
    }
  }
  else if (instanceOf<PdxTests::NestedPdxPtr>(myPdxVal))
  {
	  PdxTests::NestedPdxPtr nstPdxVal=dynCast<PdxTests::NestedPdxPtr>(myPdxVal);
    if(nstPdxVal != NULLPTR)
      expectedVal=true;
    if(expectedVal)
    {
      try{
      	//NestedPdxPtr nstPdxRegVal=dynCast<NestedPdxPtr>(region->get(key));
       	if(strcmp(nstPdxVal->getString(),pdxTostring)!= 0)
        {
          FWKEXCEPTION("verifyFromSnapshotOnly actual value "<<  nstPdxVal->getString() << " is not same as expected val " << pdxTostring <<" for key " <<key->toString()->asChar());
        }
      }
      catch(KeyNotFoundException){}
      catch(Exception e){FWKEXCEPTION("Pdx with version 2 got this "<< e.getMessage());}
    }
  }
  else if(instanceOf<AutoPdxTests::AutoPdxVersioned1Ptr>(myPdxVal)){
	  AutoPdxTests::AutoPdxVersioned1Ptr  pdx1Val=dynCast<AutoPdxTests::AutoPdxVersioned1Ptr>(myPdxVal);
      if(pdx1Val != NULLPTR)
        expectedVal=true;
      if(expectedVal)
      {
        try{
          //PdxType1V1Ptr pdx1RegVal=dynCast<PdxType1V1Ptr>(region->get(key));
          if(strcmp(pdx1Val->getString(),pdxTostring)!=0)
          {
            FWKEXCEPTION("verifyFromSnapshotOnly actual value "<<  pdx1Val->getString() << " is not same as expected val " << pdxTostring <<" for key " <<key->toString()->asChar());
          }
        }
        catch(KeyNotFoundException){}
        catch(Exception e){FWKEXCEPTION("Pdx with version 1 got this "<< e.getMessage());}
      }
    }
    else if(instanceOf<AutoPdxTests::AutoPdxVersioned2Ptr>(myPdxVal)){
    	AutoPdxTests::AutoPdxVersioned2Ptr pdx2Val=dynCast<AutoPdxTests::AutoPdxVersioned2Ptr>(myPdxVal);
      if(pdx2Val != NULLPTR)
        expectedVal=true;
      if(expectedVal)
      {
        try{
          //PdxTypes1V2Ptr pdx2RegVal=dynCast<PdxTypes1V2Ptr>(region->get(key));
          if(strcmp(pdx2Val->getString(),pdxTostring)!= 0)
          {
            FWKEXCEPTION("verifyFromSnapshotOnly actual value "<<  pdx2Val->getString() << " is not same as expected val " << pdxTostring <<" for key " <<key->toString()->asChar());
          }
        }
        catch(KeyNotFoundException){}
        catch(Exception e){FWKEXCEPTION("Pdx with version 2 got this "<< e.getMessage());}
      }
    }
    else if (instanceOf<AutoPdxTests::NestedPdxPtr>(myPdxVal))
    {
    	AutoPdxTests::NestedPdxPtr nstPdxVal=dynCast<AutoPdxTests::NestedPdxPtr>(myPdxVal);
      if(nstPdxVal != NULLPTR)
        expectedVal=true;
      if(expectedVal)
      {
        try{
        	//NestedPdxPtr nstPdxRegVal=dynCast<NestedPdxPtr>(region->get(key));
         	if(strcmp(nstPdxVal->getString(),pdxTostring)!= 0)
          {
            FWKEXCEPTION("verifyFromSnapshotOnly actual value "<<  nstPdxVal->getString() << " is not same as expected val " << pdxTostring <<" for key " <<key->toString()->asChar());
          }
        }
        catch(KeyNotFoundException){}
        catch(Exception e){FWKEXCEPTION("Pdx with version 2 got this "<< e.getMessage());}
      }
    }
} 
// ----------------------------------------------------------------------------------------------
int32_t Pdxtests::verifyFromSnapshotOnly(){
  int32_t result = FWK_SEVERE;
  int32_t versionNo = getIntValue("versionNum");
  bool expectedVal=false;
  RegionPtr region = getRegionPtr();
  std::map<std::string,std::string> ValidateMap;
  std::string RegionSnapShotStr=bbGetString("RegionSnapShot","regionsnapshot");
  ValidateMap=getBBStrMap(RegionSnapShotStr);
  std::vector<std::string> ValidateVec;
  if(isEmptyClient)
  {
    verifyServerKeysFromSnapshot();
    return 0;
  }
  int snapShotSize=0;
  int regionSize=0;
  snapShotSize=(int)ValidateMap.size();
  if(isEmptyClient){
    VectorOfCacheableKey Skeys;
    region->serverKeys(Skeys);
    regionSize=Skeys.size();
  }
  else
  {
    regionSize=region->size();
  }
  FWKINFO("Verifying from snapShot containing " << snapShotSize << "entries");
  if(snapShotSize != regionSize)
  {
	  FWKEXCEPTION("Expected snapShotSize to be of size = " << regionSize << " but it is " << snapShotSize);
  }
  std::map<std::string,std::string>::iterator SSIter;
  for(SSIter=ValidateMap.begin();SSIter != ValidateMap.end();SSIter++)
  {     
    CacheableKeyPtr SSKey=CacheableKey::create(atoi(SSIter->first.c_str()));
    SerializablePtr myPdxVal=region->get(SSKey);
    const char* pdxValueString = SSIter->second.c_str();
     //PdxSerializablePtr myPdxVal=dynCast<PdxSerializablePtr>(SSIter->second.c_str());
   /* if(myPdxVal != NULLPTR){
      std::string TypeName = myPdxVal->getClassName();
    }
    else
      FWKINFO("Pdx Val is null");*/
    GetPdxVersionedVal(SSKey,myPdxVal,versionNo,expectedVal,pdxValueString);
    try{
      verifyContainsKey(region,SSKey,true);
    }
    catch(Exception e){
      FWKEXCEPTION("verifyContainsKey method got this exception. " << e.getMessage());
    }
    bool containsValueForKey=region->containsValueForKey(SSKey);
    FWKINFO("verifyFromSnapshotOnly: containsValueForKey= "<<containsValueForKey);
    try{
      verifyContainsValueForKey(region,SSKey,(myPdxVal != NULLPTR));
    }
    catch(Exception e){
      FWKEXCEPTION("verifyContainsValueForKey got this exception " << e.getMessage());
    }

  }
  if(isSerialExecution)
       {
         std::string DestroyKeyStr=bbGetString("DestroyedKeys", "destroyedKeys");
         std::vector<std::string> DestroyKeyVec=getBBStrVec(DestroyKeyStr);
         if(DestroyKeyVec.size() != 0){
           for(uint32_t i=0;i<DestroyKeyVec.size();i++){
             std::string dKeyStr=DestroyKeyVec.at(i);
             CacheableKeyPtr DestKey=CacheableKey::create(atoi(dKeyStr.c_str()));
             try {
               verifyContainsKey(region, DestKey, false);
             }catch(Exception e){
                 FWKEXCEPTION(e.getMessage());
             }

           }
               FWKINFO("No entries destroyed as Destroy Vector size is 0");
         }
       }
  try
  {
     verifyServerKeysFromSnapshot();
  }
  catch(Exception e)
  {}
  result = FWK_SUCCESS;
  FWKINFO("Done verifying from snapshot containing " << snapShotSize << "entries");
  return result;
}

int32_t Pdxtests::doVerifyModifyPdxInstance(){
  int32_t result = FWK_SEVERE;
  result = doAccessPdxInstanceAndVerify();
  result = doModifyPdxInstance();
  return result;
}
int32_t Pdxtests::doVerifyModifyAutoPdxInstance(){
  int32_t result = FWK_SEVERE;
  result = doAccessAutoPdxInstanceAndVerify();
  result = doModifyPdxInstance();
  return result;
}
int32_t Pdxtests::doAccessPdxInstanceAndVerify(){
  int32_t result = FWK_SUCCESS;
  FWKINFO("Inside doAccessPdxInstanceAndVerify ");
  //Serializable::registerPdxType(PdxType::createDeserializable);
  RegionPtr region=getRegionPtr();
  CacheableKeyPtr key;
  VectorOfCacheableKey vecKeys;
  PdxTests::PdxTypePtr pdxObj(new PdxTests::PdxType());

  resetValue("distinctKeys");
  int numKeys=getIntValue("distinctKeys");
  region->serverKeys(vecKeys);
  int size=vecKeys.size();
  FWKINFO("doAccessPdxInstanceAndVerify Key size = "<<size << " pdxSerializer is "<<region->getCache()->getPdxReadSerialized());
  try{
    if(size != numKeys)
      FWKEXCEPTION("doAccessPdxInstanceAndVerify() number of entries "<<size<<" on server is not same as expected "<<numKeys);
    for(int i=0;i<numKeys;i++){
      key=vecKeys.at(i);

      PdxInstancePtr pIPtr = dynCast<PdxInstancePtr>(region->get(key));

      char* retStr = NULL;
      pIPtr->getField("m_string",&retStr);
      genericValCompare(pdxObj->getString(),retStr);

      bool bl=true;
      pIPtr->getField("m_bool",bl);
      genericValCompare(pdxObj->getBool(),bl);

      wchar_t charVal = ' ';
      pIPtr->getField("m_char",charVal);
      genericValCompare(charVal,pdxObj->getChar());

      signed char byteVal = 0;
      pIPtr->getField("m_byte",byteVal);
      genericValCompare(byteVal,pdxObj->getByte());

      int val = 0;
      pIPtr->getField("m_int32",val);
      genericValCompare(val,pdxObj->getInt());

      float fval=0.0f;
      pIPtr->getField("m_float",fval);
      genericValCompare(fval,pdxObj->getFloat());

      double dVal = 0.0;
      pIPtr->getField("m_double",dVal);
      genericValCompare(dVal,pdxObj->getDouble());

      int64_t lval=0;
      pIPtr->getField("m_long",lval);
      genericValCompare(lval,pdxObj->getLong());

      int16_t shortVal = 0;
      pIPtr->getField("m_int16",shortVal);
      genericValCompare(shortVal,pdxObj->getShort());

      char** strArrVal = NULL;
      int32_t strArrLen = 0;
      pIPtr->getField("m_stringArray", &strArrVal, strArrLen);
      genericValCompare(pdxObj->getStringArrayLength(), strArrLen);
      char** strArray = pdxObj->getStringArray();
      for(int i=0; i<strArrLen; i++) {
        if(!(strcmp(strArray[i], strArrVal[i]) == 0))
          FWKEXCEPTION("All stringVal lenght should be equal");
        }  
     
      signed char* byteArr = NULL;
      int32_t byteArrLength = 0; 
      pIPtr->getField("m_byteArray", &byteArr, byteArrLength);
      genericValCompare(pdxObj->getByteArrayLength(), byteArrLength);
      genericCompare(pdxObj->getByteArray(), byteArr, byteArrLength);

      pIPtr->getField("m_sbyteArray", &byteArr, byteArrLength);
      genericValCompare(pdxObj->getByteArrayLength(), byteArrLength);
      genericCompare(pdxObj->getSByteArray(), byteArr, byteArrLength);

      bool* boolArr = NULL;
      int32_t boolArrLength = 0; 
      pIPtr->getField("m_boolArray", &boolArr, boolArrLength);
      genericValCompare(pdxObj->getBoolArrayLength(), boolArrLength);
      genericCompare(pdxObj->getBoolArray(), boolArr, boolArrLength);

      int16_t* shortArr = NULL;
      int32_t shortArrLength = 0; 
      pIPtr->getField("m_int16Array", &shortArr, shortArrLength);
      genericValCompare(pdxObj->getShortArrayLength(), shortArrLength);
      genericCompare(pdxObj->getShortArray(), shortArr, shortArrLength);

      pIPtr->getField("m_uint16Array", &shortArr, shortArrLength);
      genericValCompare(pdxObj->getShortArrayLength(), shortArrLength);
      genericCompare(pdxObj->getUInt16Array(), shortArr, shortArrLength);

      int32_t* intArr = NULL;
      int32_t intArrLength = 0; 
      pIPtr->getField("m_int32Array", &intArr, intArrLength);
      genericValCompare(pdxObj->getIntArrayLength(), intArrLength);
      genericCompare(pdxObj->getIntArray(), intArr, intArrLength);

      pIPtr->getField("m_uint32Array", &intArr, intArrLength);
      genericValCompare(pdxObj->getIntArrayLength(), intArrLength);
      genericCompare(pdxObj->getUIntArray(), intArr, intArrLength);

      int64_t* longArr = NULL;
      int32_t longArrLength = 0; 
      pIPtr->getField("m_longArray", &longArr, longArrLength);
      genericValCompare(pdxObj->getLongArrayLength(), longArrLength);
      genericCompare(pdxObj->getLongArray(), longArr, longArrLength);

      pIPtr->getField("m_ulongArray", &longArr, longArrLength);
      genericValCompare(pdxObj->getLongArrayLength(), longArrLength);
      genericCompare(pdxObj->getULongArray(), longArr, longArrLength);

      double* doubleArr = NULL;
      int32_t doubleArrLength = 0; 
      pIPtr->getField("m_doubleArray", &doubleArr, doubleArrLength);
      genericValCompare(pdxObj->getDoubleArrayLength(), doubleArrLength);
      genericCompare(pdxObj->getDoubleArray(), doubleArr, doubleArrLength);

      float* floatArr = NULL;
      int32_t floatArrLength = 0; 
      pIPtr->getField("m_floatArray", &floatArr, floatArrLength);
      genericValCompare(pdxObj->getFloatArrayLength(), floatArrLength);
      genericCompare(pdxObj->getFloatArray(), floatArr, floatArrLength);

      CacheablePtr object = NULLPTR;    
      pIPtr->getField("m_pdxEnum", object);
      if(object == NULLPTR)
  	FWKEXCEPTION("enumObject should not be NULL");
      CacheableEnumPtr enumObject = dynCast<CacheableEnumPtr>(object);
      /*if(enumObject->getEnumOrdinal() == pdxObj->getEnum()->getEnumOrdinal())
      { FWKINFO("enumObject ordinal are equal");}
      else
        FWKEXCEPTION("enumObject ordinal should be equal");  
       if(!strcmp(enumObject->getEnumClassName() , pdxObj->getEnum()->getEnumClassName()) == 0)
         FWKEXCEPTION("enumObject classname should be equal");
       if(!strcmp(enumObject->getEnumName() , pdxObj->getEnum()->getEnumName()) == 0)
       	  FWKEXCEPTION("enumObject enumname should be equal");  */
       CacheablePtr object2 = NULLPTR;
       pIPtr->getField("m_map",object2);
       CacheableHashMapPtr tmpMap = dynCast<CacheableHashMapPtr>(object2);
       /*if(tmpMap->size()!= pdxObj->getHashMap()->size())
       {
         throw new IllegalStateException("Not got expected value for type ");
         result=FWK_SEVERE;
       }

       pIPtr->getField("m_vector",object2);
       CacheableVectorPtr tmpVec = dynCast<CacheableVectorPtr>(object2);
       if(tmpVec->size()!= pdxObj->getVector()->size()){
         throw new IllegalStateException("Not got expected value for type ");
        result=FWK_SEVERE;
        }

        pIPtr->getField("m_chs",object2);
        CacheableHashSetPtr tmpHash = dynCast<CacheableHashSetPtr>(object2);
        if(tmpHash->size()!= pdxObj->getHashSet()->size()){
       	  throw new IllegalStateException("Not got expected value for type ");
       	  result=FWK_SEVERE;
        }
        pIPtr->getField("m_clhs",object2);
        CacheableLinkedHashSetPtr tmpLinkHS = dynCast<CacheableLinkedHashSetPtr>(object2);
        if(tmpLinkHS->size()!= pdxObj->getLinkedHashSet()->size()){
       	  throw new IllegalStateException("Not got expected value for type ");
      	  result=FWK_SEVERE;
        }*/
      }
    }
    catch(Exception e){FWKEXCEPTION("doAccessPdxInstanceAndVerify() Caught Exception:"<<e.getMessage());result=FWK_SEVERE;}
    return result;
}
int32_t Pdxtests::doAccessAutoPdxInstanceAndVerify(){
  int32_t result = FWK_SUCCESS;
  FWKINFO("Inside doAccessAutoPdxInstanceAndVerify ");
  RegionPtr region=getRegionPtr();
  CacheableKeyPtr key;
  VectorOfCacheableKey vecKeys;
  PdxTestsAuto::PdxTypePtr pdxObj(new PdxTestsAuto::PdxType());

  resetValue("distinctKeys");
  int numKeys=getIntValue("distinctKeys");
  region->serverKeys(vecKeys);
  int size=vecKeys.size();
  FWKINFO("doAccessAutoPdxInstanceAndVerify Key size = "<<size << " pdxSerializer is "<<region->getCache()->getPdxReadSerialized());
  try{
    if(size != numKeys)
      FWKEXCEPTION("doAccessAutoPdxInstanceAndVerify() number of entries "<<size<<" on server is not same as expected "<<numKeys);
    for(int i=0;i<numKeys;i++){
      key=vecKeys.at(i);

      PdxInstancePtr pIPtr = dynCast<PdxInstancePtr>(region->get(key));

      char* retStr = NULL;
      pIPtr->getField("m_string",&retStr);
      genericValCompare(pdxObj->getString(),retStr);

      bool bl=true;
      pIPtr->getField("m_bool",bl);
      genericValCompare(pdxObj->getBool(),bl);

      wchar_t charVal = ' ';
      pIPtr->getField("m_char",charVal);
      genericValCompare(charVal,pdxObj->getChar());

      signed char byteVal = 0;
      pIPtr->getField("m_byte",byteVal);
      genericValCompare(byteVal,pdxObj->getByte());

      int val = 0;
      pIPtr->getField("m_int32",val);
      genericValCompare(val,pdxObj->getInt());

      float fval=0.0f;
      pIPtr->getField("m_float",fval);
      genericValCompare(fval,pdxObj->getFloat());

      double dVal = 0.0;
      pIPtr->getField("m_double",dVal);
      genericValCompare(dVal,pdxObj->getDouble());

      int64_t lval=0;
      pIPtr->getField("m_long",lval);
      genericValCompare(lval,pdxObj->getLong());

      int16_t shortVal = 0;
      pIPtr->getField("m_int16",shortVal);
      genericValCompare(shortVal,pdxObj->getShort());

      char** strArrVal = NULL;
      int32_t strArrLen = 0;
      pIPtr->getField("m_stringArray", &strArrVal, strArrLen);
      genericValCompare(pdxObj->getStringArrayLength(), strArrLen);
      char** strArray = pdxObj->getStringArray();
      for(int i=0; i<strArrLen; i++) {
        if(!(strcmp(strArray[i], strArrVal[i]) == 0))
          FWKEXCEPTION("All stringVal lenght should be equal");
        }

      signed char* byteArr = NULL;
      int32_t byteArrLength = 0;
      pIPtr->getField("m_byteArray", &byteArr, byteArrLength);
      genericValCompare(pdxObj->getByteArrayLength(), byteArrLength);
      genericCompare(pdxObj->getByteArray(), byteArr, byteArrLength);

      pIPtr->getField("m_sbyteArray", &byteArr, byteArrLength);
      genericValCompare(pdxObj->getByteArrayLength(), byteArrLength);
      genericCompare(pdxObj->getSByteArray(), byteArr, byteArrLength);

      bool* boolArr = NULL;
      int32_t boolArrLength = 0;
      pIPtr->getField("m_boolArray", &boolArr, boolArrLength);
      genericValCompare(pdxObj->getBoolArrayLength(), boolArrLength);
      genericCompare(pdxObj->getBoolArray(), boolArr, boolArrLength);

      int16_t* shortArr = NULL;
      int32_t shortArrLength = 0;
      pIPtr->getField("m_int16Array", &shortArr, shortArrLength);
      genericValCompare(pdxObj->getShortArrayLength(), shortArrLength);
      genericCompare(pdxObj->getShortArray(), shortArr, shortArrLength);

      pIPtr->getField("m_uint16Array", &shortArr, shortArrLength);
      genericValCompare(pdxObj->getShortArrayLength(), shortArrLength);
      genericCompare(pdxObj->getUInt16Array(), shortArr, shortArrLength);

      int32_t* intArr = NULL;
      int32_t intArrLength = 0;
      pIPtr->getField("m_int32Array", &intArr, intArrLength);
      genericValCompare(pdxObj->getIntArrayLength(), intArrLength);
      genericCompare(pdxObj->getIntArray(), intArr, intArrLength);

      pIPtr->getField("m_uint32Array", &intArr, intArrLength);
      genericValCompare(pdxObj->getIntArrayLength(), intArrLength);
      genericCompare(pdxObj->getUIntArray(), intArr, intArrLength);

      int64_t* longArr = NULL;
      int32_t longArrLength = 0;
      pIPtr->getField("m_longArray", &longArr, longArrLength);
      genericValCompare(pdxObj->getLongArrayLength(), longArrLength);
      genericCompare(pdxObj->getLongArray(), longArr, longArrLength);

      pIPtr->getField("m_ulongArray", &longArr, longArrLength);
      genericValCompare(pdxObj->getLongArrayLength(), longArrLength);
      genericCompare(pdxObj->getULongArray(), longArr, longArrLength);

      double* doubleArr = NULL;
      int32_t doubleArrLength = 0;
      pIPtr->getField("m_doubleArray", &doubleArr, doubleArrLength);
      genericValCompare(pdxObj->getDoubleArrayLength(), doubleArrLength);
      genericCompare(pdxObj->getDoubleArray(), doubleArr, doubleArrLength);

      float* floatArr = NULL;
      int32_t floatArrLength = 0;
      pIPtr->getField("m_floatArray", &floatArr, floatArrLength);
      genericValCompare(pdxObj->getFloatArrayLength(), floatArrLength);
      genericCompare(pdxObj->getFloatArray(), floatArr, floatArrLength);

      CacheablePtr object = NULLPTR;
      pIPtr->getField("m_pdxEnum", object);
      if(object == NULLPTR)
  	    FWKEXCEPTION("enumObject should not be NULL");
      CacheableEnumPtr enumObject = dynCast<CacheableEnumPtr>(object);
      CacheablePtr object2 = NULLPTR;
      pIPtr->getField("m_map",object2);
      CacheableHashMapPtr tmpMap = dynCast<CacheableHashMapPtr>(object2);

      }
    }
    catch(Exception e){FWKEXCEPTION("doAccessAutoPdxInstanceAndVerify() Caught Exception:"<<e.getMessage());result=FWK_SEVERE;}
    return result;
}
int32_t Pdxtests::doModifyPdxInstance(){
  int32_t result = FWK_SUCCESS;
  RegionPtr region=getRegionPtr();
  FWKINFO("Inside doModifyPdxInstance pdxSerializer is "<<region->getCache()->getPdxReadSerialized());
  VectorOfCacheableKey vecKey;
  VectorOfCacheableKey EmpvecKey;
  CacheableKeyPtr key;
  int size;
  if(isEmptyClient){
    region->serverKeys(EmpvecKey);
    size=EmpvecKey.size();
  }
  else{
    region->keys(vecKey);
    size=region->size();
  }
  resetValue("distinctKeys");
  int numKeys=getIntValue("distinctKeys");
  try{
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(*m_lock);
    if(size != numKeys)
      FWKEXCEPTION("doModifyPdxInstance:No of entries on server = "<<size<<" is not equal to what was expected = "<< numKeys);
    for(int i=0;i<numKeys;i++){
      if(isEmptyClient)
        key=EmpvecKey.at(i);
      else
        key=vecKey.at(i);
      PdxInstancePtr newPI;
      PdxInstancePtr pdxI=dynCast<PdxInstancePtr>(region->get(key));
      WritablePdxInstancePtr wPdxI(pdxI->createWriter());
      FWKINFO("Modifying value for key = "<<key->toString()->asChar());
      
      int32_t oldVal= 0;
      pdxI->getField("m_int32",oldVal);
      FWKINFO("OldValu= "<< oldVal);
      int tempIntVal=oldVal +1;
      wPdxI->setField("m_int32",tempIntVal);
      {
        region->put(key,wPdxI);
      }
      perf::sleepMillis(100);
      newPI=dynCast<PdxInstancePtr>(region->get(key));
      int32_t newVal=0;
      newPI->getField("m_int32",newVal);
      FWKINFO("modified oldValue = "<<tempIntVal<< " newValue = "<<newVal);
      if(tempIntVal!= newVal)
      {
        result=FWK_SEVERE;
        FWKEXCEPTION("field m_int32 didnot get modified oldValue = "<<tempIntVal<< " newValue = "<<newVal);
      }
     
      const char* str1 = "change the string";
      wPdxI->setField("m_string", str1);
      region->put(key, wPdxI);  
      perf::sleepMillis(100);
      newPI = dynCast<PdxInstancePtr>(region->get(key));
      char* getstringVal = NULL;
      newPI->getField("m_string", &getstringVal);
      FWKINFO("str1= "<<str1<<"and getstringVal= "<<getstringVal);
      if(!(strcmp(getstringVal, str1)==0))
      {
        result=FWK_SEVERE;
        FWKEXCEPTION("field m_string did not get modified oldValue = "<<str1<< " newValue = "<<getstringVal);
      }
      
      const wchar_t* chgStr=L"NewString";
      wPdxI->setField("m_string",chgStr);
      region->put(key,wPdxI);
      perf::sleepMillis(100);
      wchar_t* newStr=NULL;
      newPI = dynCast<PdxInstancePtr>(region->get(key));
      newPI->getField("m_string",&newStr);
      LOGINFO("stringVal= %ls and newValue= %ls",chgStr,newStr);
      if(!(wcscmp(chgStr,newStr)==0))
      {
        result=FWK_SEVERE;
        FWKEXCEPTION("field m_string did not get modified oldValue = "<<chgStr<< " newValue = "<<newStr);
      }

      bool NewBool=true;
      wPdxI->setField("m_bool",false);
      region->put(key,wPdxI);
      perf::sleepMillis(100);
      newPI=dynCast<PdxInstancePtr>(region->get(key));
      newPI->getField("m_bool",NewBool);
      FWKINFO("NewBool val is "<< NewBool);
      if(NewBool == true)
      {
        result=FWK_SEVERE;
        FWKEXCEPTION("field m_bool is not equal ,it is "<<NewBool);
      }
 
      signed char getByteVal = 0;
      signed char setByteVal = 0x75;
      wPdxI->setField("m_byte",setByteVal);
      region->put(key,wPdxI);
      perf::sleepMillis(100);
      newPI = dynCast<PdxInstancePtr>(region->get(key));
      newPI->getField("m_byte", getByteVal);
      FWKINFO("setByteVal = "<<setByteVal<<" getByteVal value is "<<getByteVal);
      if(getByteVal != 0x75){
        result=FWK_SEVERE;
        FWKEXCEPTION("field m_byte is not equal,it is "<< getByteVal);
      }

      wchar_t charVal = ' ';
      wchar_t setVal = 'S';
      wPdxI->setField("m_char", setVal);
      region->put(key, wPdxI);
      perf::sleepMillis(100);
      newPI = dynCast<PdxInstancePtr>(region->get(key));
      newPI->getField("m_char", charVal);
      LOGINFO("NewCharVal= %c and charVal= %c",charVal,setVal);
      if(setVal != charVal){
        result=FWK_SEVERE;
    	FWKEXCEPTION("field m_char is not equal,it is "<<charVal);
      }
    }
  }
  catch(Exception e){FWKEXCEPTION("doModifyPdxInstance:Got this exception " << e.getMessage());result=FWK_SEVERE;}
  return result;
}

void Pdxtests::verifyServerKeysFromSnapshot(){
  FWKINFO("Inside verifyServerKeysFromSnapshot");
  RegionPtr region=getRegionPtr();
  int32_t versionNo = getIntValue("versionNum");
  std::map<std::string,std::string> VerifySKMap;
  std::string RegionSnapShotStr=bbGetString("RegionSnapShot","regionsnapshot");
  VerifySKMap=getBBStrMap(RegionSnapShotStr);
  std::vector<CacheableKeyPtr> serverKey;
  std::vector<std::string> DestroyKeyVec;
    
  VectorOfCacheableKey sKeys;
  region->serverKeys(sKeys);
  CacheableKeyPtr key;
  bool expectedVal=false;
  for(int32_t i = 0; i < (int32_t) sKeys.size(); i++)
  {
    key=sKeys.at(i);
    serverKey.push_back(key);
  }
  int snapShotSize=(int)VerifySKMap.size();
  int numServerKey=sKeys.size();
    FWKINFO("Verifying server keys from snapshot containing " << snapShotSize << " entries and numServerKey= "<<numServerKey<<" and serverKey Vector sixe = "<<serverKey.size());
  if(snapShotSize != numServerKey)
  {
    FWKINFO("Expected number of keys on server to be " << snapShotSize << " but it is " << numServerKey);
  }
  std::map<std::string,std::string>::iterator skIter;
  for(skIter=VerifySKMap.begin();skIter != VerifySKMap.end();skIter++)
  {
    CacheableKeyPtr SKKey=CacheableKey::create(atoi(skIter->first.c_str()));
    SerializablePtr expectedValue=region->get(SKKey);
    const char* kstr=SKKey->toString()->asChar();
    uint32_t i=0;
    while(i<serverKey.size())
    {
      const char * skey=serverKey.at(i)->toString()->asChar();
      if(strcmp(kstr,skey))
      {
    	if((!isThinClient && !isEmptyClient) || (isThinClient && region->containsKey(SKKey)))
    	{
     	  try{
     		 const char* pdxVal = skIter->second.c_str();
    	    GetPdxVersionedVal(SKKey,expectedValue,versionNo,expectedVal,pdxVal);
          }
    	  catch(Exception e){FWKINFO("other Exception in verifyServerKeysFromSnapshot with message " << e.getMessage());}
    	}
           
      }
      i++;
    }
  }
  if(isSerialExecution)
  {
    std::string DestroyKeyStr=bbGetString("DestroyedKeys", "destroyedKeys");
    DestroyKeyVec=getBBStrVec(DestroyKeyStr);
    if(DestroyKeyVec.size() != 0){
      for(uint32_t i=0;i<DestroyKeyVec.size();i++){
        std::string dKeyStr=DestroyKeyVec.at(i);
        CacheableKeyPtr DestKey=CacheableKey::create(dKeyStr.c_str());
        bool isPresent=(std::find(serverKey.begin(),serverKey.end(),DestKey)!= serverKey.end());
        if(isPresent)
        {
          FWKEXCEPTION("Destroyed keys present in serverkey set");
        }
      }
      std::map<std::string,std::string>::iterator SKIter;
      for(SKIter=VerifySKMap.begin();SKIter!=VerifySKMap.end();SKIter++)
      {
        serverKey.erase(serverKey.begin());
      }
      if(serverKey.size()!= 0)
      {
        FWKEXCEPTION("Destroyed keys still exists on server");
      }
    }
    else
      FWKINFO("No entries destroyed as Destroy Vector size is 0");
  }
  FWKINFO("Done verifying server keys from snapshot containing "<<snapShotSize <<" entries");

}

void Pdxtests::verifyContainsValueForKey(RegionPtr region,CacheableKeyPtr key,bool expected){
  FWKINFO("Inside verifyContainsValueForKey expected bool val is "<<expected);
  bool containsValue=false;
  containsValue=region->containsValueForKey(key);
  std::string containsVal = "false";
  std::string expectedVal = "false";
  if(containsValue)
    containsVal = "true";
  if(expected)
    expectedVal = "true"; 
  if(containsValue != expected){
    FWKEXCEPTION("Expected ContainsValue() for key "<< key->toString()->asChar() << " to be " << expectedVal << " but it is " << containsVal);
  }
  FWKINFO("verifyContainsValueForKey completed successfully");
}

void Pdxtests::verifyContainsKey(RegionPtr region,CacheableKeyPtr key,bool expected){
  FWKINFO("Inside verifyContainsKey");
  bool containsKey=false;
  if(isEmptyClient)
  {
    containsKey=region->containsKeyOnServer(key);
  }
  else
  {
    containsKey=region->containsKey(key);
  }
  std::string containsKeyVal = "false";
  std::string expectedVal = "false";
  if(containsKey)
    containsKeyVal = "true";
  if(expected)
    expectedVal = "true"; 
  if(containsKey != expected)
  {
    FWKEXCEPTION("Expected ContainsKey() for key "<< key->toString()->asChar()  << " to be " << expectedVal << " but it is " << containsKeyVal );
  }
}
 void Pdxtests::waitForSilenceListenerComplete(int32_t desiredSilenceSec, int32_t sleepMS) {
   FWKINFO("Waiting for a period of silence for " << desiredSilenceSec << " seconds...");
   int64_t desiredSilenceMS = desiredSilenceSec * 1000;
   ACE_Time_Value startTime = ACE_OS::gettimeofday();
   int64_t silenceStartTime = startTime.msec();
   int64_t currentTime = startTime.msec();
   int64_t lastEventTime = bbGet("ListenerBB","lastEventTime");

   while (currentTime - silenceStartTime < desiredSilenceMS) {
      try {
    	  perf::sleepMillis(sleepMS);
      } catch (...) {
    	 FWKEXCEPTION( "PerfTest::waitForSilence() Caught unknown exception." );
       }
      lastEventTime = bbGet("ListenerBB","lastEventTime");
      if (lastEventTime > silenceStartTime) {
         // restart the wait
         silenceStartTime = lastEventTime;
      }
      startTime = ACE_OS::gettimeofday();
      currentTime = startTime.msec();
   }
   int64_t duration = currentTime - silenceStartTime;
   FWKINFO("Done waiting, clients have been silent for " << duration << " ms");
}

 //-----------------------------------------------------------------------------
 int32_t Pdxtests::registerAllKeys()
 {
   int32_t result = FWK_SEVERE;
   FWKINFO( "In Pdxtests::registerAllKeys()" );

   try {
     RegionPtr region = getRegionPtr();
     resetValue( "getInitialValues" );
     bool isGetInitialValues = getBoolValue( "getInitialValues" );
     FWKINFO("PerfTest::registerAllKeys region name is " << region->getName()
         << "; getInitialValues is " << isGetInitialValues);
     bool isReceiveValues = true;
     bool checkReceiveVal = getBoolValue("checkReceiveVal");
     if (checkReceiveVal) {
       resetValue("receiveValue");
       isReceiveValues = getBoolValue("receiveValue");
     }
     region->registerAllKeys(false, NULLPTR, isGetInitialValues,isReceiveValues);
     const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
     if(strlen(durableClientId) > 0) {
       m_cache->readyForEvents();
     }
     result = FWK_SUCCESS;

   } catch ( Exception& e ) {
     FWKEXCEPTION( "Pdxtests::registerAllKeys() Caught Exception: " << e.getMessage() );
   } catch ( FwkException& e ) {
     FWKEXCEPTION( "Pdxtests::registerAllKeys() Caught FwkException: " << e.getMessage() );
   } catch ( ... ) {
     FWKEXCEPTION( "Pdxtests::registerAllKeys() Caught unknown exception." );
   }
   FWKINFO( "Pdxtests::registerAllKeys() complete." );
   return result;

 }
