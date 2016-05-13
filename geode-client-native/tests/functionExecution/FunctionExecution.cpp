/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    FunctionExecution.cpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#include "FunctionExecution.hpp"

#include <ace/Time_Value.h>
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/RegionHelper.hpp"
#include "fwklib/FwkExport.hpp"
#include "fwklib/PoolHelper.hpp"
#include <gfcpp/SystemProperties.hpp>

#define FE_TIMEOUT 15

using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::functionexe;

FunctionExecution * g_test = NULL;


TESTTASK initialize( const char * initArgs ) {
  int32_t result = FWK_SUCCESS;
  if ( g_test == NULL ) {
    FWKINFO( "Initializing FunctionExecution library." );
    try {
      g_test = new FunctionExecution( initArgs );
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
  FWKINFO( "Finalizing FunctionExecution library." );
  if ( g_test != NULL ) {
    g_test->cacheFinalize();
    delete g_test;
    g_test = NULL;
  }
  return result;
}

// ---------------------------------------------------------------------------

TESTTASK doCloseCache() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Closing cache, disconnecting from distributed system." );
  if ( g_test != NULL ) {
    g_test->cacheFinalize();
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doCreateRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->createRegion();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateRegion caught exception: " << ex.getMessage() );
  }

  return result;
}
// ----------------------------------------------------------------------------
TESTTASK doCreatePool( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreatePool called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->createPools();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreatePool caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doLoadRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doLoadRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->loadRegion();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doLoadRegion caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doAddDestroyNewKeysFunction( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doAddDestroyNewKeysFunction called for task: " << taskId );  
  try {
    g_test->checkTest( taskId);
    result = g_test->addNewKeyFunction();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doAddDestroyNewKeysFunction caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doExecuteFunctions( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doExecuteFunctions called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->doExecuteFunctions();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doExecuteFunctions caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doExecuteFunctionsHA( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doExecuteFunctions called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->doExecuteFunctionsHA();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doExecuteFunctions caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doExecuteExceptionHandling( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doExecuteExceptionHandling called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->doExecuteExceptionHandling();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doExecuteExceptionHandling caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doClearRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doClearRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->ClearRegion();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doClearRegion caught exception: " << ex.getMessage() );
  }
 return result;
}
TESTTASK doGetServerKeys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGetServerKeys called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->GetServerKeys();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGetServerKeys caught exception: " << ex.getMessage() );
  } catch (Exception ex)
  {
	 result = FWK_SEVERE;
	 FWKSEVERE( "doGetServerKeys caught exception: " << ex.getMessage() );
  }

 return result;
}
TESTTASK doUpdateServerKeys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doUpdateServerKeys called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->UpdateServerKeys();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doUpdateServerKeys caught exception: " << ex.getMessage() );
  }
 return result;
}
TESTTASK doOps( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doOps called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->doOps();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doOps caught exception: " << ex.getMessage() );
  }
 return result;
}

// ----------------------------------------------------------------------------
int32_t FunctionExecution::createRegion()
{
  FWKINFO( "In FunctionExecution::createRegion()" );

  int32_t result = FWK_SEVERE;
  try {
    createPool();
    RegionHelper help( g_test );
    RegionPtr region = help.createRootRegion( m_cache );
    std::string key( region->getName() );
    bbIncrement( REGIONSBB, key );
    FWKINFO( "FunctionExecution::createRegion Created region " << region->getName() << std::endl);
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKEXCEPTION( "FunctionExecution::createRegion Caught unexpected " << e.getName() << " : " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "FunctionExecution::createRegion FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "FunctionExecution::createRegion FAILED -- caught unknown exception." );
  }
  return result;
}

int32_t FunctionExecution::createPools()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In FunctionExecution::createPool()" );

  try{
    PoolHelper help( g_test );
    PoolPtr pool = help.createPool();
    FWKINFO( "FunctionExecution::createPool Created Pool " << pool->getName() << std::endl);
    result = FWK_SUCCESS;
  } catch( Exception e ) {
    FWKEXCEPTION( "FunctionExecution::createPool Caught unexpected " << e.getName() << " : " << e.getMessage() );
  } catch( FwkException& e ){
    FWKEXCEPTION( "FunctionExecution::createPool FAILED -- caught test exception: " << e.getMessage() );
  } catch( ... ) {
    FWKEXCEPTION( "FunctionExecution::createPool FAILED -- caught unknown exception. " );
  }
  return result;
}

void FunctionExecution::checkTest( const char * taskId  ) {
  SpinLockGuard guard( m_lck );
  setTask( taskId );
  if (m_cache == NULLPTR) {
    PropertiesPtr pp = Properties::create();

    int32_t heapLruLimit = getIntValue( "heapLruLimit" );
    if( heapLruLimit > 0 )
      pp->insert("heap-lru-limit",heapLruLimit);

    bool conflate = getBoolValue( "conflate" );
    if(conflate) {
      std::string conflateEvents = getStringValue( "conflateEvents" );
      pp->insert("conflate-events",conflateEvents.c_str());
    }
   //CacheAttributesPtr cAttrs = NULLPTR;
    setCacheLevelEp();
    cacheInitialize(pp);
    bool isTimeOutInMillis = m_cache->getDistributedSystem()->getSystemProperties()->readTimeoutUnitInMillis();
    if(isTimeOutInMillis){
      #define FE_TIMEOUT 15*1000
   }
  }
}

RegionPtr FunctionExecution::getRegionPtr( const char * reg )
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
    name = std::string(reg);
  }
  try {
    if ( name.empty() ) { // just get a random root region
      VectorOfRegion rootRegionVector;
      m_cache->rootRegions( rootRegionVector );
      int32_t size = rootRegionVector.size();

      if ( size == 0 ) {
        FWKEXCEPTION( "In FunctionExecution::getRegionPtr()  No regions exist." );
      }
      region = rootRegionVector.at( GsRandom::random( size ) );
    }
    else {
      //FWKINFO( "Getting region: " << name );
      if (m_cache == NULLPTR) {
        FWKEXCEPTION( "Failed to get region: " << name << "  cache ptr is null." );
      }
      region = m_cache->getRegion( name.c_str() );
      if (region == NULLPTR) {
        FWKEXCEPTION( "Failed to get region: " << name );
      }
    }
  } catch( CacheClosedException e ) {
    FWKEXCEPTION( "In FunctionExecution::getRegionPtr()  CacheFactory::getInstance encountered "
      "CacheClosedException: " << e.getMessage() );
  } catch( EntryNotFoundException e ) {
    FWKEXCEPTION( "In FunctionExecution::getRegionPtr()  CacheFactory::getInstance encountered "
      "EntryNotFoundException: " << e.getMessage() );
  } catch( IllegalArgumentException e ) {
    FWKEXCEPTION( "In FunctionExecution::getRegionPtr()  CacheFactory::getInstance encountered "
      "IllegalArgumentException: " << e.getMessage() );
  }
  return region;
}

int32_t FunctionExecution::loadRegion()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In FunctionExecution::loadRegion()" );

  try {

    std::string name = getStringValue( "regionName" );
    RegionPtr region = getRegionPtr(name.c_str());
    char keys[1024];
    char values[1024];
    int32_t numKeys = getIntValue( "distinctKeys" );  // check distinct keys first
    bool isUpdate = getBoolValue("update");
    for(int32_t num = 1; num < numKeys; num++)
    {
      sprintf(keys, "key-%d",num);
      if(isUpdate)
        sprintf(values, "valueUpdate-%d",num);
      else
        sprintf(values, "valueCreate-%d",num);
      region->put(keys,values);
    }
    FWKINFO( "Added " << numKeys << " entries." );
    result = FWK_SUCCESS;
  } catch ( std::exception e ) {
    FWKEXCEPTION( "FunctionExecution::loadRegion() Caught std::exception: " << e.what() );
  } catch ( Exception e ) {
      FWKEXCEPTION( "FunctionExecution::loadRegion() Caught unexpected " << e.getName() << " : " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "FunctionExecution::loadRegion() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "FunctionExecution::loadRegion() Caught unknown exception." );
  }
  FWKINFO( "FunctionExecution::loadRegion() complete." );
  return result;
}

int32_t FunctionExecution::addNewKeyFunction()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In FunctionExecution::addNewKeyFunction()" );
  try {
    CacheableVectorPtr filterObj = CacheableVector::create();
    resetValue( "distinctKeys" );
    int32_t numKeys = getIntValue( "distinctKeys" );
    int clntId = g_test->getClientId();
    char buf[128];
    for(int32_t i=0;i<numKeys;i++){
      sprintf(buf, "KEY--%d--%d", clntId,i);
      CacheableKeyPtr key = CacheableKey::create(buf);
      filterObj->push_back(key);
    }
    std::string opcode = getStringValue( "entryOps" );
    if(opcode == "destroy")
      executeFunction(filterObj,"destroy");
    else
      executeFunction(filterObj,"addKey");
    result = FWK_SUCCESS;
  } catch ( std::exception e ) {
    FWKEXCEPTION( "FunctionExecution::addNewKeyFunction() Caught std::exception: " << e.what() );
  } catch ( Exception e ) {
      FWKEXCEPTION( "FunctionExecution::addNewKeyFunction() Caught unexpected " << e.getName() << " : " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "FunctionExecution::addNewKeyFunction() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "FunctionExecution::addNewKeyFunction() Caught unknown exception." );
  }
  FWKINFO( "FunctionExecution::addNewKeyFunction() complete." );
  return result;
}

void FunctionExecution::verifyAddNewResult(CacheableVectorPtr exefuncResult, CacheableVectorPtr filterObj)
{
  try {
    perf::sleepMillis(30000);
    RegionPtr region = getRegionPtr();
    CacheableStringPtr value = NULLPTR;
    if(filterObj != NULLPTR )
    {
      for(int32_t i = 0; i < filterObj->size()-1;i++) {
        CacheableKeyPtr key = dynCast<CacheableKeyPtr>(filterObj->operator[](i));
        for( int cnt = 0; cnt < 100; cnt++){
          value = dynCast<CacheableStringPtr>(region->get(key));
        }
        CacheableStringPtr expectedVal = dynCast<CacheableStringPtr>(filterObj->operator[](i));
        if(value != NULLPTR) {
          if(strcmp(expectedVal->asChar(), value->asChar())==0){
          }else {
            FWKEXCEPTION("verifyAddNewResult Failed: expected value is" << expectedVal->asChar() << " and found " << value->asChar() << " for key " << key->toString()->asChar());
          }
        }
      }
    }
  } catch ( std::exception e ) {
    FWKEXCEPTION( "FunctionExecution::verifyAddNewResult() Caught std::exception: " << e.what() );
  } catch ( Exception e ) {
      FWKEXCEPTION( "FunctionExecution::verifyAddNewResult() Caught unexpected " << e.getName() << " : " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "FunctionExecution::verifyAddNewResult() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "FunctionExecution::verifyAddNewResult() Caught unknown exception." );
  }
}


void FunctionExecution::executeFunction(CacheableVectorPtr filterObj,const char* ops)
{
  try {
    resetValue( "getResult" );
    bool getresult = getBoolValue("getResult");
    resetValue( "replicated" );
    bool isReplicate = getBoolValue("replicated");
    resetValue( "distinctKeys" );
    int32_t numKeys = getIntValue( "distinctKeys" );
    if(filterObj == NULLPTR){
      int clntId = g_test->getClientId();
      filterObj = CacheableVector::create();
      char buf[128];
      sprintf(buf, "KEY--%d--%d", clntId,GsRandom::random(numKeys));
      CacheableKeyPtr key = CacheableKey::create(buf);
      filterObj->push_back(key);
    }

    ExecutionPtr exc = NULLPTR;
    PoolPtr pptr = NULLPTR;
    CacheablePtr args = CacheableString::create(ops);
    RegionPtr region = getRegionPtr();
    std::string executionMode = getStringValue( "executionMode" );
    resetValue( "poolName" );
    std::string poolname = getStringValue( "poolName" );
    char* funcName = NULL;
    if(executionMode == "onServers" || executionMode  == "onServer"){
      pptr = PoolManager::find(poolname.c_str());
      if(getresult)
        funcName = (char*)"ServerOperationsFunction";
      else
        funcName = (char*)"ServerOperationsWithOutResultFunction";
    }
    if ( executionMode == "onServers") {
      exc = FunctionService::onServers(pptr);
    } if ( executionMode == "onServer"){
      exc = FunctionService::onServer(pptr);
    }else if( executionMode == "onRegion"){
      exc = FunctionService::onRegion(region);
      if(getresult)
        funcName = (char*)"RegionOperationsFunction";
      else
        funcName = (char*)"RegionOperationsWithOutResultFunction";
    }
    CacheableVectorPtr executeFunctionResult = NULLPTR;
    if(!isReplicate){
      if(getresult){
        if(executionMode == "onRegion"){
          executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(funcName,getresult, FE_TIMEOUT, true, true)->getResult();
        }else{
          filterObj->push_back(args);
          args = filterObj;
          executeFunctionResult = exc->withArgs(args)->execute(funcName,getresult, FE_TIMEOUT, true, true)->getResult();
        }
      }else {
        if(executionMode == "onRegion"){
          exc->withFilter(filterObj)->withArgs(args)->execute(funcName,getresult, FE_TIMEOUT, false, true);
        } else {
          filterObj->push_back(args);
          args = filterObj;
          exc->withArgs(args)->execute(funcName,getresult, FE_TIMEOUT, false, true);
        }
      }
    } else {
      filterObj->push_back(args);
      args = filterObj;
      if (getresult)
        executeFunctionResult = exc->withArgs(args)->execute(funcName,getresult, FE_TIMEOUT, true, true)->getResult();
    else
        executeFunctionResult = exc->withArgs(args)->execute(funcName,getresult, FE_TIMEOUT, false, true)->getResult();

    }
    perf::sleepMillis(100);
    if(strcmp(ops,"addKey") ==0)
      verifyAddNewResult(executeFunctionResult,filterObj);
    else
      verifyResult(executeFunctionResult,filterObj,ops);
  } catch ( std::exception e ) {
    FWKEXCEPTION( "FunctionExecution::executeFunction() Caught std::exception: for operation " << ops << " is " << e.what() );
  } catch ( Exception e ) {
      FWKEXCEPTION( "Caught unexpected " << e.getName() << " for " << ops
           << " operation: " << e.getMessage() << ": exiting task." );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "FunctionExecution::executeFunction() Caught FwkException: for operation " << ops <<" is " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "FunctionExecution::executeFunction() Caught unknown exception for operation " << ops );
  }
}
void FunctionExecution::verifyResult(CacheableVectorPtr exefuncResult, CacheableVectorPtr filterObj, const char* ops)
{
    RegionPtr region = getRegionPtr();
    char buf[128];
    if(exefuncResult != NULLPTR)
    {
      CacheableBooleanPtr lastResult = dynCast<CacheableBooleanPtr>(exefuncResult->at(0));
      if(lastResult->value() != true)
        FWKEXCEPTION("FunctionExecution::verifyResult failed, last result is not true");
    }
    CacheableStringPtr value = NULLPTR;
    if(filterObj != NULLPTR )
    {
        CacheableKeyPtr key = dynCast<CacheableKeyPtr>(filterObj->operator[](0));
        for (int i = 0; i<20;i++){
          value = dynCast<CacheableStringPtr>(region->get(key));
        }
        if(strcmp(ops,"update")==0 || strcmp(ops,"get")==0){
          if (value != NULLPTR) {
            if(strncmp(value->asChar(),"update_",7) == 0)
              sprintf(buf, "update_%s", key->toString()->asChar());
            else
              sprintf(buf, "%s", key->toString()->asChar());

            if(strcmp(buf,value->asChar())!=0)
              FWKEXCEPTION("verifyResult Failed: expected value is" << buf << " and found " << value->asChar() <<
                 " for key " << key->toString()->asChar() << "for operation " << ops);
          }
        } else if(strcmp(ops,"destroy")==0){
            if(value == NULLPTR){
              executeFunction(filterObj,"addKey");
            }else{
              FWKEXCEPTION("FunctionExecution::verifyResult failed to destroy key " << key->toString()->asChar());
            }
        } else if(strcmp(ops,"invalidate")==0){
          if(value == NULLPTR)
          {
            executeFunction(filterObj,"update");
          }else {
            FWKEXCEPTION("FunctionExecution::verifyResult Failed for invalidate key " << key->toString()->asChar());
          }
        }
    }
}
int32_t FunctionExecution::doExecuteFunctions()
{
  int32_t fwkResult = FWK_SUCCESS;
  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;
  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  ACE_Time_Value now;
  std::string opcode;
  while ( now < end ) {
    try {
      opcode = getStringValue( "entryOps" );
      executeFunction(NULLPTR,opcode.c_str());
    } catch ( Exception &e ) {
      end = 0;
      fwkResult = FWK_SEVERE;
      FWKEXCEPTION( "Caught unexpected exception during entry " << opcode
           << " operation: " << e.getMessage() << ": exiting task." );
    }
    now = ACE_OS::gettimeofday();
  }
  return fwkResult;
}
int32_t FunctionExecution::doExecuteExceptionHandling()
{
  int32_t fwkResult = FWK_SUCCESS;
  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;
  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  ACE_Time_Value now;
  std::string opcode;
  while ( now < end ) {
    try {
      opcode = getStringValue( "entryOps" );
      if(opcode == "ParitionedRegionFunctionExecution")
      {
        doParitionedRegionFunctionExecution();
      }
      else if (opcode == "ReplicatedRegionFunctionExecution")
      {
        doReplicatedRegionFunctionExecution();
      }
      else if (opcode == "FireAndForgetFunctionExecution")
      {
        doFireAndForgetFunctionExecution();
      }
      else if (opcode == "OnServersFunctionExcecution")
      {
        doOnServersFunctionExcecution();
      }
    } catch ( Exception &e ) {
      end = 0;
      fwkResult = FWK_SEVERE;
      FWKEXCEPTION( "Caught unexpected " << e.getName() << " during exception handling for " << opcode
           << " operation: " << e.getMessage() << ": exiting task." );
    }
    now = ACE_OS::gettimeofday();
  }
  return fwkResult;
}

void FunctionExecution::doParitionedRegionFunctionExecution()
{
  ExecutionPtr exc = NULLPTR;
  RegionPtr region = getRegionPtr("partitionedRegion");
  try {
    exc = FunctionService::onRegion(region);
    MyResultCollector *myRC = new MyResultCollector();
    if(GsRandom::getInstance()->nextBoolean())
    {
      //Execution on partitionedRegion with no filter
      exc = exc->withCollector(ResultCollectorPtr(myRC));
    }
    else
    {
      //Execution on partitionedRegion with filter
      VectorOfCacheableKey keyVector;
      region->keys(keyVector);
      CacheableVectorPtr filterObj = CacheableVector::create();
      for(int32_t i =0 ; i < keyVector.size(); i++)
      {
        CacheableKeyPtr key = keyVector.at(i);;
        filterObj->push_back(key);
      }
      exc = exc->withFilter(filterObj)->withCollector(ResultCollectorPtr(myRC));
    }
    // execute function
    CacheableVectorPtr executeFunctionResult = exc->execute("ExceptionHandlingFunction", true,30, true, true)->getResult();
  }catch ( FunctionExecutionException &/*ignore*/ ) {
  } catch ( gemfire::TimeoutException ){
  }
}

void FunctionExecution::doFireAndForgetFunctionExecution()
{
  char* name = NULL;
  if(GsRandom::getInstance()->nextBoolean())
  {
    //Execution Fire and forget on partitioned region
    name = "partitionedRegion";
  }
  else
  {
    //Execution Fire and forget on replicated region
    name = "replicatedRegion";
  }
  RegionPtr region = getRegionPtr(name);
  try {
    MyResultCollector *myRC = new MyResultCollector();
    ExecutionPtr exc = FunctionService::onRegion(region)->withCollector(ResultCollectorPtr(myRC));
    // execute function
    ResultCollectorPtr rc = exc->execute("FireNForget", false,30, false, false);
  }catch ( FunctionExecutionException &/*ignore*/ ) {
  } catch ( gemfire::TimeoutException ){
  }
}
void FunctionExecution::doReplicatedRegionFunctionExecution()
{
  ExecutionPtr exc = NULLPTR;
  try {
    RegionPtr region = getRegionPtr("replicatedRegion");
    exc = FunctionService::onRegion(region);
    MyResultCollector *myRC = new MyResultCollector();
    exc = exc->withCollector(ResultCollectorPtr(myRC));
    // execute function
    CacheableVectorPtr executeFunctionResult = exc->execute("ExceptionHandlingFunction", true,30, true, true)->getResult();
  }catch ( FunctionExecutionException &/*ignore*/ ) {
  } catch ( gemfire::TimeoutException ){
  }

}

void FunctionExecution::doOnServersFunctionExcecution()
{
  resetValue( "poolName" );
  std::string poolname = getStringValue( "poolName" );
  try {
  PoolPtr pptr = PoolManager::find(poolname.c_str());
  ExecutionPtr exc = FunctionService::onServers(pptr);
  MyResultCollector *myRC = new MyResultCollector();
  exc = exc->withCollector(ResultCollectorPtr(myRC));
    // execute function
    CacheableVectorPtr executeFunctionResult = exc->execute("ExceptionHandlingFunction", true,30, true, true)->getResult();
  }catch ( FunctionExecutionException &/*ignore*/ ) {
  } catch ( gemfire::TimeoutException ){
  }
}
int32_t FunctionExecution::doExecuteFunctionsHA()
{
  int32_t result = FWK_SUCCESS;
  resetValue( "getResult" );
  bool getresult = getBoolValue("getResult");
  resetValue( "distinctKeys" );
  int32_t numKeys = getIntValue( "distinctKeys" );
  PoolPtr pptr = NULLPTR;
  RegionPtr region = getRegionPtr();
  std::string executionMode = getStringValue("executionMode");
  resetValue("poolName");
  std::string poolname = getStringValue("poolName");
  CacheableVectorPtr executeFunctionResult = NULLPTR;
  ExecutionPtr exc = FunctionService::onRegion(region);
  MyResultCollectorHA *myRC = new MyResultCollectorHA();
  exc = exc->withCollector(ResultCollectorPtr(myRC));
  executeFunctionResult = exc->execute((char*)"GetFunctionExeHA",getresult,120,true,true)->getResult();
  char buf[128];
  if (executeFunctionResult != NULLPTR) {
    CacheableVectorPtr resultList = myRC->getResult(60);
    if (resultList != NULLPTR) {
      if (numKeys == resultList->size()) {
        for (int i = 1; i < numKeys; i++) {
          int count = 0;
          sprintf(buf, "key-%d", i);
          for(int j = 0; j < resultList->size(); j++) {
                CacheablePtr key = dynCast<CacheablePtr>(resultList->operator[](j));
            if(strcmp(key->toString()->asChar(), buf) == 0){
              count++;
              if(count > 1){
                FWKEXCEPTION("FunctionExecution::doExecuteFunctionsHA failed duplicate entry found for key " << buf);
              }
            }
          }
          if(count == 0 ) {
               FWKEXCEPTION("FunctionExecution::doExecuteFunctionsHA failed key is missing in result list " << buf);
            }
          }
        }
      }
      else {
        FWKEXCEPTION("FunctionExecution::doExecuteFunctionsHA failed: result size " << resultList->size()
           << " doesn't match with number of keys " << numKeys);
        result = FWK_SEVERE;
      }
    }
  else {
   FWKEXCEPTION("FunctionExecution::doExecuteFunctionsHA executeFunctionResult is null");
  }
    return result;
  }
//---------------DoFETask ---
TEST_EXPORT CacheListener * createSilenceListener() {
  return new SilenceListener(g_test);
}
void DoFETask::checkContainsValueForKey(CacheablePtr key, bool expected, std::string logStr) {
   //RegionPtr regionPtr = getRegion();
   bool containsValue = regionPtr->containsValueForKey(key);
   std::string sString = !expected? "false":"true";
   std::string ckey = !containsValue? "false":"true";
   if (containsValue != expected)
      FWKEXCEPTION("DoOpsTask::checkContainsValueForKey: Expected containsValueForKey(" << key->toString()->asChar() << ") to be " << sString <<
                ", but it was " << ckey << ": " << logStr);
}

void DoFETask::verifyFEResult(CacheableVectorPtr exefuncResult, std::string funcName)
{
	if (exefuncResult != NULLPTR)
	{
	  for(int i = 0; i < exefuncResult->size(); i++)
	  {
		  CacheableBooleanPtr lastResult = dynCast<CacheableBooleanPtr>(exefuncResult->at(i));
		  //ASSERT(lastResult->value() == true,"DoFETask:: failed, last result is not true");
		  if(lastResult->value() != true){
			  char buf[500];
			  sprintf(buf, "DoFETask:: %s failed, last result is not true",funcName.c_str());
			  FWKEXCEPTION(buf);
		  }
	  }
	}
}

bool DoFETask::addNewKeyFunction()
{
	int32_t numNewKeysCreated = (int32_t)(m_test->bbGet("ImageBB", "NUM_NEW_KEYS_CREATED"));
	m_test->bbIncrement("ImageBB", "NUM_NEW_KEYS_CREATED");
	int32_t numNewKeys = m_test->getIntValue("NumNewKeys");
	if (numNewKeysCreated > numNewKeys)
	{
		FWKINFO("All new keys created; returning from addNewKey");
		return true;
	}
	int32_t entryCount = m_test->getIntValue("entryCount");
	entryCount = (entryCount < 1) ? 10000 : entryCount;
	CacheableInt32Ptr key = CacheableInt32::create(entryCount + numNewKeysCreated);
	checkContainsValueForKey(key, false, "before addNewKey");

	//char* funcName = (char*)"RegionOperationsFunctionPdx";
	CacheableVectorPtr filterObj = CacheableVector::create();
	filterObj->push_back(key);
	bool pdxobject = m_test->getBoolValue("isPdxObject");
	CacheableVectorPtr args = CacheableVector::create();
	args->push_back(CacheableString::create("addKey"));
	if (pdxobject)
		args->push_back(CacheableBoolean::create(pdxobject));
	args->push_back(filterObj->at(0));

	ExecutionPtr exc = FunctionService::onRegion(regionPtr);
	//FWKINFO("Going to do addKey execute");
	CacheableVectorPtr executeFunctionResult = NULLPTR;
	bool isReplicate = m_test->getBoolValue("replicated");
	if (!isReplicate){
		executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}else
	{
		executeFunctionResult = exc->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}
	verifyFEResult(executeFunctionResult, "addNewKeyFunction");
	executeFunctionResult->clear();
	return (numNewKeysCreated >= numNewKeys);
}
bool DoFETask::putAllNewKeyFunction()
{
	int32_t numNewKeysCreated = (int32_t)(m_test->bbGet("ImageBB", "NUM_NEW_KEYS_CREATED"));
	m_test->bbIncrement("ImageBB", "NUM_NEW_KEYS_CREATED");
	int32_t numNewKeys = m_test->getIntValue("NumNewKeys");
	if (numNewKeysCreated > numNewKeys)
	{
		FWKINFO("All new keys created; returning from addNewKey");
		return true;
	}
	int32_t entryCount = m_test->getIntValue("entryCount");
	entryCount = (entryCount < 1) ? 10000 : entryCount;
	CacheableInt32Ptr key = CacheableInt32::create(entryCount + numNewKeysCreated);
        RegionAttributesPtr atts = regionPtr->getAttributes();
	if(atts->getCachingEnabled() != false)
	  checkContainsValueForKey(key, false, "before addNewKey");
	//char* funcName = (char*)"RegionOperationsFunctionPdx";
	CacheableVectorPtr filterObj = CacheableVector::create();
	filterObj->push_back(key);
	bool pdxobject = m_test->getBoolValue("isPdxObject");
	CacheableVectorPtr args = CacheableVector::create();
	args->push_back(CacheableString::create("putAll"));
	if (pdxobject)
		args->push_back(CacheableBoolean::create(pdxobject));
	args->push_back(filterObj->at(0));

	ExecutionPtr exc = FunctionService::onRegion(regionPtr);
	//FWKINFO("Going to do addKey execute");
	CacheableVectorPtr executeFunctionResult = NULLPTR;
	bool isReplicate = m_test->getBoolValue("replicated");
	if (!isReplicate){
		executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}else
	{
		executeFunctionResult = exc->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}
	verifyFEResult(executeFunctionResult, "putAllNewKeyFunction");
	executeFunctionResult->clear();
	return (numNewKeysCreated >= numNewKeys);
}
bool DoFETask::invalidateFunction()
{
	int32_t nextKey = (int32_t)(m_test->bbGet("ImageBB", "LASTKEY_INVALIDATE"));
	m_test->bbIncrement("ImageBB", "LASTKEY_INVALIDATE");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_Invalidate");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_Invalidate");
	if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
	{
		FWKINFO("All existing keys invalidated; returning from invalidate");
		return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create(nextKey);
	//char* funcName = (char*)"RegionOperationsFunctionPdx";

	CacheableVectorPtr filterObj = CacheableVector::create();
	filterObj->push_back(key);
	bool pdxobject = m_test->getBoolValue("isPdxObject");
	CacheableVectorPtr args = CacheableVector::create();
	args->push_back(CacheableString::create("invalidate"));
	if (pdxobject)
		args->push_back(CacheableBoolean::create(pdxobject));
	args->push_back(filterObj->at(0));

	ExecutionPtr exc = FunctionService::onRegion(regionPtr);
	//FWKINFO("Going to do addKey execute");
	CacheableVectorPtr executeFunctionResult = NULLPTR;
	bool isReplicate = m_test->getBoolValue("replicated");
	if (!isReplicate){
		executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}else
	{
		executeFunctionResult = exc->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}
	verifyFEResult(executeFunctionResult, "invalidateFunction");
	executeFunctionResult->clear();
	return (nextKey >= lastKey);
}
bool DoFETask::localInvalidateFunction()
{
	int32_t nextKey = (int32_t)(m_test->bbGet("ImageBB", "LASTKEY_LOCAL_INVALIDATE"));
	m_test->bbIncrement("ImageBB", "LASTKEY_LOCAL_INVALIDATE");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_LocalInvalidate");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_LocalInvalidate");
	if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
	{
		FWKINFO("All local invalidates completed; returning from localInvalidate");
		return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create(nextKey);
	//char* funcName = (char*)"RegionOperationsFunctionPdx";

	CacheableVectorPtr filterObj = CacheableVector::create();
	filterObj->push_back(key);
	bool pdxobject = m_test->getBoolValue("isPdxObject");
	CacheableVectorPtr args = CacheableVector::create();
	args->push_back(CacheableString::create("localinvalidate"));
	if (pdxobject)
	 args->push_back(CacheableBoolean::create(pdxobject));
	args->push_back(filterObj->at(0));

	ExecutionPtr exc = FunctionService::onRegion(regionPtr);
	//FWKINFO("Going to do addKey execute");
	CacheableVectorPtr executeFunctionResult = NULLPTR;
	bool isReplicate = m_test->getBoolValue("replicated");
	if (!isReplicate){
		executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}else
	{
		executeFunctionResult = exc->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}
	verifyFEResult(executeFunctionResult, "localInvalidateFunction");
	executeFunctionResult->clear();
	return (nextKey >= lastKey);
}
bool DoFETask::destroyFunction()
{
	int32_t nextKey = (int32_t)(m_test->bbGet("ImageBB", "LASTKEY_DESTROY"));
	m_test->bbIncrement("ImageBB", "LASTKEY_DESTROY");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_Destroy");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_Destroy");
	if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
	{
		FWKINFO("All destroys completed; returning from destroy");
		return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create(nextKey);
	//char* funcName = (char*)"RegionOperationsFunctionPdx";

	CacheableVectorPtr filterObj = CacheableVector::create();
	filterObj->push_back(key);
	bool pdxobject = m_test->getBoolValue("isPdxObject");
	CacheableVectorPtr args = CacheableVector::create();
	args->push_back(CacheableString::create("destroy"));
	if (pdxobject)
	 args->push_back(CacheableBoolean::create(pdxobject));
	args->push_back(filterObj->at(0));

	ExecutionPtr exc = FunctionService::onRegion(regionPtr);
	//FWKINFO("Going to do addKey execute");
	CacheableVectorPtr executeFunctionResult = NULLPTR;
	bool isReplicate = m_test->getBoolValue("replicated");
	if (!isReplicate){
		executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}else
	{
		executeFunctionResult = exc->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}
	verifyFEResult(executeFunctionResult, "destroyFunction");
	executeFunctionResult->clear();
	return (nextKey >= lastKey);
}
bool DoFETask::localDestroyFunction()
{
	int32_t nextKey = (int32_t)(m_test->bbGet("ImageBB", "LASTKEY_LOCAL_DESTROY"));
	m_test->bbIncrement("ImageBB", "LASTKEY_LOCAL_DESTROY");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_LocalDestroy");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_LocalDestroy");
	if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
	{
		FWKINFO("All local destroys completed; returning from localDestroy");
		return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create(nextKey);
	//char* funcName = (char*)"RegionOperationsFunctionPdx";

	CacheableVectorPtr filterObj = CacheableVector::create();
	filterObj->push_back(key);
	bool pdxobject = m_test->getBoolValue("isPdxObject");
	CacheableVectorPtr args = CacheableVector::create();
	args->push_back(CacheableString::create("localdestroy"));
	if (pdxobject)
	 args->push_back(CacheableBoolean::create(pdxobject));
	args->push_back(filterObj->at(0));

	ExecutionPtr exc = FunctionService::onRegion(regionPtr);
	//FWKINFO("Going to do addKey execute");
	CacheableVectorPtr executeFunctionResult = NULLPTR;
	bool isReplicate = m_test->getBoolValue("replicated");
	if (!isReplicate){
		executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}else
	{
		executeFunctionResult = exc->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}
	verifyFEResult(executeFunctionResult, "localDestroyFunction");
	executeFunctionResult->clear();

	return (nextKey >= lastKey);
}
bool DoFETask::updateExistingKeyFunction()
{
	int32_t nextKey = (int32_t)(m_test->bbGet("ImageBB", "LASTKEY_UPDATE_EXISTING_KEY"));
	m_test->bbIncrement("ImageBB", "LASTKEY_UPDATE_EXISTING_KEY");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_UpdateExistingKey");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_UpdateExistingKey");
	if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
	{
		FWKINFO("All existing keys updated; returning from updateExistingKey");
		return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create(nextKey);

	//char* funcName = (char*)"RegionOperationsFunctionPdx";

	CacheableVectorPtr filterObj = CacheableVector::create();
	filterObj->push_back(key);
	bool pdxobject = m_test->getBoolValue("isPdxObject");
	CacheableVectorPtr args = CacheableVector::create();
	args->push_back(CacheableString::create("update"));
	if (pdxobject)
	 args->push_back(CacheableBoolean::create(pdxobject));
	args->push_back(filterObj->at(0));

	ExecutionPtr exc = FunctionService::onRegion(regionPtr);
	//FWKINFO("Going to do addKey execute");
	CacheableVectorPtr executeFunctionResult = NULLPTR;
	bool isReplicate = m_test->getBoolValue("replicated");
	if (!isReplicate){
		executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}else
	{
		executeFunctionResult = exc->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}
	verifyFEResult(executeFunctionResult, "updateExistingKeyFunction");
	executeFunctionResult->clear();
	return (nextKey >= lastKey);
}
bool DoFETask::getFunction()
{
	int32_t nextKey = (int32_t)(m_test->bbGet("ImageBB", "LASTKEY_GET"));
	m_test->bbIncrement("ImageBB", "LASTKEY_GET");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_Get");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_Get");
	if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
	{
		FWKINFO("All gets completed; returning from get");
		return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create(nextKey);
	//char* funcName = (char*)"RegionOperationsFunctionPdx";

	CacheableVectorPtr filterObj = CacheableVector::create();
	filterObj->push_back(key);
	bool pdxobject = m_test->getBoolValue("isPdxObject");
	CacheableVectorPtr args = CacheableVector::create();
	args->push_back(CacheableString::create("get"));
	if (pdxobject)
	 args->push_back(CacheableBoolean::create(pdxobject));
	args->push_back(filterObj->at(0));

	ExecutionPtr exc = FunctionService::onRegion(regionPtr);
	//FWKINFO("Going to do addKey execute");
	CacheableVectorPtr executeFunctionResult = NULLPTR;
	bool isReplicate = m_test->getBoolValue("replicated");
	if (!isReplicate){
		executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}else
	{
		executeFunctionResult = exc->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}
	verifyFEResult(executeFunctionResult, "getFunction");
	executeFunctionResult->clear();
	return (nextKey >= lastKey);
}
bool DoFETask::queryFunction()
{
	int32_t numNewKeysCreated = (int32_t)(m_test->bbGet("ImageBB", "NUM_NEW_KEYS_CREATED"));
	int32_t numThread = m_test->getIntValue("numThreads");
	numNewKeysCreated = numNewKeysCreated - (numThread - 1);
	int numNewKeys = m_test->getIntValue("NumNewKeys");
	if (numNewKeysCreated > numNewKeys)
	{
		FWKINFO("All query executed; returning from addNewKey");
		return true;
	}
	int entryCount = m_test->getIntValue("entryCount");
	entryCount = (entryCount < 1) ? 10000 : entryCount;
	CacheableInt32Ptr key = CacheableInt32::create(entryCount + numNewKeysCreated);
	checkContainsValueForKey(key, false, "before addNewKey");
	//TVal value = GetValue((TVal)(object)(entryCount + numNewKeysCreated));
	//GetValue(value);
	//m_region.Add(key, value);
	//char* funcName = (char*)"RegionOperationsFunctionPdx";

	CacheableVectorPtr filterObj = CacheableVector::create();
	filterObj->push_back(key);
	bool pdxobject = m_test->getBoolValue("isPdxObject");
	CacheableVectorPtr args = CacheableVector::create();
	args->push_back(CacheableString::create("query"));
	if (pdxobject)
	 args->push_back(CacheableBoolean::create(pdxobject));
	args->push_back(filterObj->at(0));

	ExecutionPtr exc = FunctionService::onRegion(regionPtr);
	//FWKINFO("Going to do addKey execute");
	CacheableVectorPtr executeFunctionResult = NULLPTR;
	bool isReplicate = m_test->getBoolValue("replicated");
	if (!isReplicate){
		executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}else
	{
		executeFunctionResult = exc->withArgs(args)->execute(m_funcname.c_str(),true)->getResult();
	}
	verifyFEResult(executeFunctionResult, "queryFunction");
	executeFunctionResult->clear();
	return (numNewKeysCreated >= numNewKeys);
}

int32_t  FunctionExecution::ClearRegion()
{
  int32_t result = FWK_SEVERE;
  FWKINFO("In ClearRegion()");
  try
  {
	  RegionPtr region = getRegionPtr();
	  region->clear();
	  VectorOfCacheableKey Exkeys;
	  region->serverKeys(Exkeys);
	  if(Exkeys.size() != 0)
		  FWKEXCEPTION( "Region is not clear ");
	  result = FWK_SUCCESS;
  }
  catch (Exception &ex)
  {
	FWKEXCEPTION( "Caught unexpected exception during ClearRegion " << ex.getMessage());
  }
  FWKINFO("ClearRegion() complete.");
  return result;
}

int32_t FunctionExecution::GetServerKeys()
{
	int32_t result = FWK_SEVERE;
	try
	{
	  FWKINFO("FunctionExecution:DoGetServerKeys");
	  RegionPtr regionPtr = getRegionPtr();
	  VectorOfCacheableKey keys;
	  regionPtr->serverKeys(keys);
          FWKINFO("FunctionExecution:GetServerKeys - ServerKeys = " << keys.size());
	  bool pdxobject = getBoolValue("isPdxObject");
	  CacheableVectorPtr args = CacheableVector::create();
		args->push_back(CacheableString::create("get"));
		if (pdxobject){
		 args->push_back(CacheableBoolean::create(pdxobject));
		}
		CacheableVectorPtr server_keys = CacheableVector::create();
	    for (int i = 0; i < keys.size(); i++){
		  server_keys->push_back(keys[i]);
	    }
	    for (int i = 0; i < server_keys->size(); i++){
		  args->push_back(server_keys->at(i));
	    }
	    std::string  funcName = getStringValue( "funcName" );
		//char* funcName = (char*)"RegionOperationsFunctionPdx";
		ExecutionPtr exc = FunctionService::onRegion(regionPtr);
		//FWKINFO("Going to do addKey execute");
		CacheableVectorPtr executeFunctionResult = NULLPTR;
		bool isReplicate = getBoolValue("replicated");
		if (!isReplicate){
		  executeFunctionResult = exc->withFilter(server_keys)->withArgs(args)->execute(funcName.c_str(),true)->getResult();
		}else
		{
		  executeFunctionResult = exc->withArgs(args)->execute(funcName.c_str(),true)->getResult();
		}
		if (executeFunctionResult != NULLPTR)
		{
		  for(int i = 0; i < executeFunctionResult->size(); i++)
		  {
			  CacheableBooleanPtr lastResult = dynCast<CacheableBooleanPtr>(executeFunctionResult->at(i));
			  if(lastResult->value() != true){
				char buf[500];
				sprintf(buf, "GetServerKeys:: %s failed, last result is not true for key",funcName.c_str());
				FWKEXCEPTION(buf);
			}
		  }
		}
		result = FWK_SUCCESS;
	  }
	  catch (CacheServerException &ex)
	  {
		  FWKEXCEPTION("DoGetServerKeys() Caught CacheServerException: "<< ex.getMessage());
	  }
	  catch (Exception &ex)
	  {
		  FWKEXCEPTION("DoGetServerKeys() Caught Exception: "<< ex.getMessage());
	  }
	  return result;
	}

int32_t FunctionExecution::UpdateServerKeys()
{
	int32_t result = FWK_SEVERE;
	try
	{
	  FWKINFO("FunctionExecution:UpdateServerKeys");
	  RegionPtr regionPtr = getRegionPtr();
	  VectorOfCacheableKey keys;
	  regionPtr->serverKeys(keys);
          FWKINFO("FunctionExecution:UpdateServerKeys - ServerKeys = " << keys.size());
	  bool pdxobject = getBoolValue("isPdxObject");
	  CacheableVectorPtr args = CacheableVector::create();
	  args->push_back(CacheableString::create("update"));
	  if (pdxobject)
		args->push_back(CacheableBoolean::create(pdxobject));
	  CacheableVectorPtr server_keys = CacheableVector::create();
	  for (int i = 0; i < keys.size(); i++)
		  server_keys->push_back(keys[i]);
	  for (int i = 0; i < server_keys->size(); i++)
		  args->push_back(server_keys->at(i));
	    std::string  funcName = getStringValue( "funcName" );
		//char* funcName = (char*)"RegionOperationsFunctionPdx";
		ExecutionPtr exc = FunctionService::onRegion(regionPtr);
		//FWKINFO("Going to do addKey execute");
		CacheableVectorPtr executeFunctionResult = NULLPTR;
		bool isReplicate = getBoolValue("replicated");
		if (!isReplicate){
		  executeFunctionResult = exc->withFilter(server_keys)->withArgs(args)->execute(funcName.c_str(),true)->getResult();
		}else
		{
		  executeFunctionResult = exc->withArgs(args)->execute(funcName.c_str(),true)->getResult();
		}
		if (executeFunctionResult != NULLPTR)
		{
		  for(int i = 0; i < executeFunctionResult->size(); i++)
		  {
		    CacheableBooleanPtr lastResult = dynCast<CacheableBooleanPtr>(executeFunctionResult->at(i));
			if(lastResult->value() != true){
				char buf[500];
				sprintf(buf, "UpdateServerKeys:: %s failed, last result is not true for key %s",funcName.c_str(),server_keys->at(i)->toString()->asChar());
				FWKEXCEPTION(buf);
			}
		  }
		}
		result = FWK_SUCCESS;
	  }
	  catch (CacheServerException &ex)
	  {
		  FWKEXCEPTION("UpdateServerKeys() Caught CacheServerException: "<< ex.getMessage());
	  }
	  catch (Exception &ex)
	  {
		  FWKEXCEPTION("UpdateServerKeys() Caught Exception: "<< ex.getMessage());
	  }
	  return result;
	}

int32_t FunctionExecution::doOps()
{
	int32_t result = FWK_SEVERE;
	FWKINFO("FunctionExcution:doOps called.");

	int32_t entryCount = getIntValue("entryCount");
	int32_t numNewKeys = getIntValue("NumNewKeys");
	TestClient * clnt = TestClient::getTestClient();
	RegionPtr regionPtr=getRegionPtr();
	if (regionPtr == NULLPTR)
	{
	  FWKEXCEPTION("FunctionExcution:doOps(): No region to perform operations on.");
	}
	int timedInterval = getIntValue("timedInterval");
	if (timedInterval <= 0) {
	  timedInterval = 5;
	}
	bool m_istransaction = getBoolValue("useTransactions");
	try
	{
	  DoFETask dooperation(regionPtr, m_istransaction,g_test);
	  resetValue("numThreads");
	  int32_t numThreads = getIntValue("numThreads");
	  if ( !clnt->runIterations( &dooperation, entryCount+numNewKeys + NUM_EXTRA_KEYS, numThreads, 0 ) ) {
	    FWKEXCEPTION( "In doOps()  doOps timed out." );
	  }
	  if(clnt->getTaskStatus() == FWK_SEVERE)
	     FWKEXCEPTION( "Exception in doOps task");
	  result = FWK_SUCCESS;
	}
	catch (Exception &e)
	{
	  FWKEXCEPTION("Caught exception during FunctionExcution:doOps: " << e.getMessage());
	}
	FWKINFO("Done in FunctionExcution:doOps");
	return result;
}


//---------------DoFETask ---
