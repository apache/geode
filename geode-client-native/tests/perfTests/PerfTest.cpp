/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    PerfTest.cpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#include "PerfTest.hpp"
#include "PerfTasks.hpp"
#include <CacheableToken.hpp>

#include <ace/Time_Value.h>
#include "fwklib/GsRandom.hpp"

#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/RegionHelper.hpp"
#include <statistics/StatisticsFactory.hpp>
//#include <statistics/Statistics.hpp>

#include "fwklib/FwkExport.hpp"
#include "fwklib/PoolHelper.hpp"
#include "fwklib/QueryHelper.hpp"

#include "security/CredentialGenerator.hpp"
#include <gfcpp/SystemProperties.hpp>

#define PUTALL_TIMEOUT 60

namespace FwkPerfTests {
  std::string REGIONSBB( "Regions" );
  std::string CLIENTSBB( "ClientsBb" );
  std::string READYCLIENTS( "ReadyClients" );
}

using namespace FwkPerfTests;
using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::perf;
using namespace gemfire::testframework::security;
using namespace gemfire_statistics;

PerfTest * g_test = NULL;

// ----------------------------------------------------------------------------

TESTTASK initialize( const char * initArgs ) {
  int32_t result = FWK_SUCCESS;
  if ( g_test == NULL ) {
    FWKINFO( "Initializing PerfTest library." );
    try {
      g_test = new PerfTest( initArgs );
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
  FWKINFO( "Finalizing PerfTest library." );
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

void PerfTest::getClientSecurityParams(PropertiesPtr prop, std::string credentials) {

  std::string securityParams = getStringValue("securityParams");

  // no security means security param is not applicable
  if( securityParams.empty() ) {
    FWKINFO("security is DISABLED :");
    return;
  }

  FWKDEBUG("security param is : " << securityParams);

  std::string bb ( "GFE_BB" );
  std::string key( "securityScheme" );
  std::string sc = bbGetString(bb,key);
  if( sc.empty() ) {
    sc = getStringValue( key.c_str() );
    if( !sc.empty() ) {
      bbSet(bb, key, sc);
    }
  }
  FWKINFO("security scheme : " << sc);

  if (prop == NULLPTR) prop = Properties::create();

  CredentialGeneratorPtr cg = CredentialGenerator::create( sc );
  cg->getAuthInit(prop);

  if( securityParams=="valid" || securityParams=="VALID" ) {
    FWKINFO("getting valid credentials ");
    cg->getValidCredentials(prop);
  } else if( securityParams=="invalid" || securityParams=="INVALID" ) {
    FWKDEBUG("getting invalid credentials ");
    cg->getInvalidCredentials(prop);
  } else {
    FWKDEBUG("getting credentials for list of operations ");
    opCodeList opCodes;
#if defined(_SOLARIS) && (__cplusplus < 201103L)
    int n = 0;
    while (!securityParams.empty() && (std::count(opCodes.begin(),
            opCodes.end(), strToOpCode(securityParams), n), n == 0)) {
#else
    while (!securityParams.empty() && std::count(opCodes.begin(),
            opCodes.end(), strToOpCode(securityParams)) == 0) {
#endif

      opCodes.push_back( strToOpCode(securityParams) );

      securityParams = getStringValue("securityParams");
      FWKINFO("next security params " << securityParams);
    }
    cg->getAllowedCredentialsForOps(opCodes,prop);
    FWKDEBUG("securities properties entries " << prop->getSize());
  }
}

// ----------------------------------------------------------------------------

void PerfTest::checkTest( const char * taskId  ) {
  SpinLockGuard guard( m_lck );
  setTask( taskId );
  if (m_cache == NULLPTR) {
    PropertiesPtr pp = Properties::create();
    std::string authInit;
    getClientSecurityParams(pp, getStringValue("credentials"));

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
      // TODO jbarrett - this isn't doing what they think it is doing
      // #define PUTALL_TIMEOUT 60*1000
    }
  // PerfTest specific initialization
  // none
  }
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

TESTTASK doLocalDestroyEntries( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doLocalDestroyEntries called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->localDestroyEntries();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doLocalDestroyEntries caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doDestroyRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doDestroyRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->destroyRegion();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doDestroyRegion caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doLocalDestroyRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doLocalDestroyRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->localDestroyRegion();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doLocalDestroyRegion caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doCheckValues( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCheckValues called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->checkValues();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCheckValues caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doPuts( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPuts called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->puts();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPuts caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doPutAll( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPutAll called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->putAll();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPutAll caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doSerialPuts( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doSerialPuts called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->serialPuts();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doSerialPuts caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doPutBursts( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPutBursts called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->putBursts();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPutBursts caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doPopClient( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopClient called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->popClient();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopClient caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doPopClientMS( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopClientMS called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->popClientMS();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopClientMS caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doPopServers( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopServers called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->popServers();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopServers caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doLatencyPuts( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doLatencyPuts called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->latencyPuts();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doLatencyPuts caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doGets( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGets called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->gets();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGets caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doDestroys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doDestroys called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->destroys();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doDestroys caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doDestroysKeys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doDestroysKeys called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->destroysKeys();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doDestroysKeys caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doNetsearch( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doNetsearch called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->netsearch();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doNetsearch caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doPopulateRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopulateRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->populateRegion();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopulateRegion caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doPutAllRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopulateRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->putAllRegion();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopulateRegion caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doResetListener( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doResetListener called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->resetListener();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doResetListener caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doRegisterInterestList( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterInterestList called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->registerInterestList();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterInterestList caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doRegisterRegexList( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterRegexList called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->registerRegexList();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterRegexList caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doUnRegisterRegexList( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterRegexList called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->unregisterRegexList();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterRegexList caught exception: " << ex.getMessage() );
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
TESTTASK doRegisterAllKeysWithResultKeys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "registerAllKeysWithResultKeys called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->registerAllKeysWithResultKeys();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "registerAllKeysWithResultKeys caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doVerifyInterestList( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyInterestList called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->verifyInterestList();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyInterestList caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doServerKeys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doServerKeys called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->doServerKeys();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doServerKeys caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doQuery( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doQuery called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->queries();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doQuery caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doIterateInt32Keys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doIterateInt32Keys called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doIterateInt32Keys();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doIterateInt32Keys caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doValidateQConflation ( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doValidateQConflation called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
     result = g_test->validateQConflation();

  } catch ( FwkException ex ) {
      result = FWK_SEVERE;
      FWKSEVERE( "doValidateQConflation caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doValidateBankTest( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doValidateBankTest called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
     result = g_test->validateBankTest();

  } catch ( FwkException ex ) {
      result = FWK_SEVERE;
      FWKSEVERE( "doValidateBankTest caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doGetAllAndVerification( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGetAllAndVerification called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->getAllAndVerification();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGetAllAndVerification caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doCreateUpdateDestroy( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopulateRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->createUpdateDestroy();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopulateRegion caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doMemoryMeasurement( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doMemoryMeasurement called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->measureMemory();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doMemoryMeasurement caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doVerifyDB( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyDB called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->verifyDB();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyDB caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doCheckOverFlow( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCheckOverFlow called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->checkOverFlow();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCheckOverFlow caught exception: " << ex.getMessage() );
  }
  return result;
}


// Factory functions for listeners, loaders, etc.
// ----------------------------------------------------------------------------

TEST_EXPORT CacheListener * createPerfTestCacheListener() {
  return new PerfTestCacheListener();
}

// ----------------------------------------------------------------------------

TEST_EXPORT CacheListener * createLatencyListener() {
  return new LatencyListener( g_test );
}

TEST_EXPORT CacheListener * createDupChecker() {
  return new DupChecker();
}
TEST_EXPORT CacheListener * createConflationTestCacheListener() {
  return new ConflationTestCacheListener( g_test );
}
void ConflationTestCacheListener::afterCreate( const EntryEvent& event )
{
  m_numAfterCreate++;
}
void ConflationTestCacheListener::afterUpdate( const EntryEvent& event )
{
   m_numAfterUpdate++;
   std::string keyIsOldValue = std::string( "isOldValue");
   std::string key1 = m_test->bbGetString(m_bb,keyIsOldValue);
   RegionPtr rPtr = event.getRegion();
   std::string regionName = rPtr->getFullPath();
   if (regionName.compare("/notifyBySubscriptionRegion") == 0) {
     if((event.getOldValue() == NULLPTR) && (key1 == "false"))
       m_test->bbSet(m_bb,keyIsOldValue, "true");
   }
}
void ConflationTestCacheListener::afterInvalidate( const EntryEvent& event )
{
  m_numAfterInvalidate++;
}
void ConflationTestCacheListener::afterDestroy( const EntryEvent& event )
{
  m_numAfterDestroy++;
}
void ConflationTestCacheListener::dumpToBB(const RegionPtr& regPtr)
{
  char name[32] = {'\0'};
  sprintf(name,"%d",g_test->getClientId());
  std::string key1 = std::string( "AFTER_CREATE_COUNT_") + std::string(name) + std::string("_") + std::string(regPtr->getName());
  std::string key2 = std::string( "AFTER_UPDATE_COUNT_" ) + std::string(name) + std::string("_") + std::string(regPtr->getName());
  std::string key3 = std::string( "AFTER_INVALIDATE_COUNT_" ) + std::string(name) + std::string("_") + std::string(regPtr->getName());
  std::string key4 = std::string( "AFTER_DESTROY_COUNT_" ) + std::string(name) + std::string("_") + std::string(regPtr->getName());
  m_test->bbSet(m_bb,key1,m_numAfterCreate);
  m_test->bbSet(m_bb,key2,m_numAfterUpdate);
  m_test->bbSet(m_bb,key3,m_numAfterInvalidate);
  m_test->bbSet(m_bb,key4,m_numAfterDestroy);

}
// ========================================================================

int32_t PerfTest::doServerKeys()
{
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO( "doServerKeys called." );

  int32_t expected = getIntValue( "expectedCount" );

  RegionPtr regionPtr = getRegionPtr();
  if (regionPtr == NULLPTR) {
    FWKEXCEPTION( "PerfTest::doServerKeys(): No region to perform operations on." );
  }

  VectorOfCacheableKey keysVec;
  try {
    regionPtr->serverKeys( keysVec );
  } catch( Exception & e ) {
    FWKEXCEPTION( "Exception thrown by serverKeys(): " << e.getMessage() );
  }

  int32_t keys = keysVec.size();

  if ( keys != expected ) {
    FWKEXCEPTION( "doServerKeys expected " << expected << " keys on server, but server reports " << keys << " keys." );
  }

  return fwkResult;
}

// ========================================================================

int32_t PerfTest::doIterateInt32Keys()
{
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO( "PerfTest::doIterateInt32Keys() called." );

  RegionPtr regionPtr = getRegionPtr();
  if (regionPtr == NULLPTR) {
    FWKEXCEPTION( "PerfTest::doIterateInt32Keys(): No region to perform operations on." );
  }

  VectorOfCacheableKey keysVec;
  try {
    regionPtr->serverKeys( keysVec );
  } catch( Exception & e ) {
    FWKEXCEPTION( "Exception thrown by serverKeys(): " << e.getMessage() );
  }

  int32_t keys = keysVec.size();
  FWKINFO( "PerfTest::doIterateInt32Keys() serverKeys() returned " << keys << " keys." );

  for ( int32_t i = 0; i < keys; i++ ) {
    CacheableInt32Ptr value = dynCast<CacheableInt32Ptr>( keysVec.at( i ) );
    FWKINFO( "ServerKeys: " << value->value() );
  }

  return fwkResult;
}

// ========================================================================

void PerfTest::clearKeys() {
  if ( m_KeysA != NULL ) {
    for ( int32_t i = 0; i < m_MaxKeys; i++ ) {
      m_KeysA[i] = NULLPTR;
    }
    delete [] m_KeysA;
    m_KeysA = NULL;
    m_MaxKeys = 0;
  }
}

// ========================================================================

int32_t PerfTest::initKeys(bool useDefault) {
  static char keyType = 'i';
  std::string typ = getStringValue( "keyType" ); // int is only value to use
  char newType = typ.empty() ? 'i' : typ[0];

  int32_t low = getIntValue( "keyIndexBegin" );
  low = (low > 0) ? low : 0;
  int32_t numKeys = getIntValue( "distinctKeys" );  // check distince keys first
  if (numKeys <= 0) {
    if (useDefault) {
      numKeys = 5000;
    } else {
      return  numKeys;
    }
  }
  int32_t high = numKeys + low;
  FWKINFO("numKeys: " << numKeys << " low: " << low);
  if ( ( newType == keyType ) && ( numKeys == m_MaxKeys ) &&  (m_KeyIndexBegin == low)) {
    return numKeys;
  }

  clearKeys();
  m_MaxKeys = numKeys;
  m_KeyIndexBegin = low;
  keyType = newType;
  if ( keyType == 's' ) {
    int32_t keySize = getIntValue( "keySize" );
    keySize = (keySize > 0) ? keySize : 10;
    std::string keyBase(keySize, 'A');
    initStrKeys(low, high, keyBase);
  } else {
    initIntKeys(low, high);
  }

  for(int j=0; j< m_MaxKeys; j++){
    std::swap(m_KeysA[GsRandom::random(numKeys)],m_KeysA[numKeys - 1]);
    numKeys--;
  }

  return m_MaxKeys;
}

// ========================================================================

void PerfTest::initStrKeys(int32_t low, int32_t high, const std::string & keyBase) {
  m_KeysA = new CacheableKeyPtr[m_MaxKeys];
  const char * const base = keyBase.c_str();

  char buf[128];
  for ( int32_t i = low; i < high; i++ ) {
    sprintf( buf, "%s%010d", base, i);
    m_KeysA[i - low] = CacheableKey::create( buf );
  }
}

// ========================================================================

void PerfTest::initIntKeys(int32_t low, int32_t high) {
  m_KeysA = new CacheableKeyPtr[m_MaxKeys];
  FWKINFO("m_MaxKeys: " << m_MaxKeys << " low: " << low << " high: " << high);

  for ( int32_t i = low; i < high; i++ ) {
    m_KeysA[i - low] = CacheableKey::create( i );
//    CacheableInt32Ptr value = dynCast<CacheableInt32Ptr>( m_KeysA[i - low] );
//    FWKINFO( "Created key: " << value->value() );
  }
}

// ========================================================================

int32_t PerfTest::initValues( int32_t numKeys, int32_t siz, bool useDefault) {
  if (siz == 0) {
    siz = getIntValue( "valueSizes" );
  }
  if ( siz <= 0 ) {
    if (useDefault) {
      siz = 55;
    } else {
      return siz;
    }
  }

  if ( numKeys <= 0 ) {
    numKeys = 5000;
  }

  if ( m_CValue != NULL ) {
    for ( int32_t i = 0; i < m_MaxValues; i++ ) {
      m_CValue[i] = NULLPTR;
    }
    delete [] m_CValue;
  }

  m_MaxValues = numKeys;

  m_CValue = new CacheableBytesPtr[m_MaxValues];

  char * buf = new char[siz];
  memset( buf, 'V', siz );
  int32_t rsiz = ( siz <= 20 ) ? siz : 20;
  for ( int32_t i = 0; i < m_MaxValues; i++ ) {
    GsRandom::getAlphanumericString( rsiz, buf );
    m_CValue[i] = CacheableBytes::create( ( const unsigned char * )buf, siz );
  }
  return siz;
}

// ----------------------------------------------------------------------------
int32_t PerfTest::createRegion()
{
  FWKINFO( "In PerfTest::createRegion()" );

  int32_t result = FWK_SEVERE;
  try {
    createPool();
    RegionHelper help( g_test );
    RegionPtr region = help.createRootRegion( m_cache );
    std::string key( region->getName() );
    bbIncrement( REGIONSBB, key );
    FWKINFO( "PerfTest::createRegion Created region " << region->getName() << std::endl);
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::createRegion FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::createRegion FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::createRegion FAILED -- caught unknown exception." );
  }
  return result;
}

// ----------------------------------------------------------------------------
int32_t PerfTest::createPools()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::createPool()" );

  try{
    PoolHelper help( g_test );
    PoolPtr pool = help.createPool();
    FWKINFO( "PerfTest::createPool Created Pool " << pool->getName() << std::endl);
    result = FWK_SUCCESS;
  } catch( Exception e ) {
    FWKEXCEPTION( "PerfTest::createPool FAILED -- caught exception: " << e.getMessage() );
  } catch( FwkException& e ){
    FWKEXCEPTION( "PerfTest::createPool FAILED -- caught test exception: " << e.getMessage() );
  } catch( ... ) {
    FWKEXCEPTION( "PerfTest::createPool FAILED -- caught unknown exception. " );
  }
  return result;
}

// ----------------------------------------------------------------------------


#ifndef WIN32
#include <unistd.h>
#endif

// ----------------------------------------------------------------------------

perf::Semaphore ackSem( 0 );
SpinLock msgLock;

// ----------------------------------------------------------------------------
int32_t PerfTest::putBursts()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::putBursts()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue( "clientCount" );
    if ( numClients < 0 ) {
      std::string key( region->getName() );
      int64_t cnt = bbGet( REGIONSBB, key );
      numClients = ( int32_t )cnt;
    }

    std::string label = RegionHelper::regionTag( region->getAttributes() );

    int32_t timedInterval = getTimeValue( "timedInterval" );
    if ( timedInterval <= 0 ) {
      timedInterval = 5;
    }

    int32_t burstMillis = getTimeValue( "burstMillis" );
    if ( burstMillis <= 0 ) {
      burstMillis = 500;
    }

    int32_t burstPause = getTimeValue( "burstPause" );
    if ( burstPause <= 0 ) {
      burstPause = 1;
    }

    int32_t opsSec = getIntValue( "opsSecond" );
    if ( opsSec <= 0 ) {
      opsSec = 1;
    }

    // Loop over key set sizes
    resetValue( "distinctKeys" );
    int32_t numKeys = initKeys(false);
    while ( numKeys > 0 ) { // keys loop
      initIntKeys( 0, numKeys );

      // Loop over value sizes
      resetValue( "valueSizes" );
      int32_t valSize = initValues(numKeys, 0, false);
      while ( valSize > 0 ) { // value loop
        // Loop over threads
        resetValue( "numThreads" );
        int32_t numThreads = getIntValue( "numThreads" );
        while ( numThreads > 0 ) { // thread loop

          // And we do the real work now
          m_PerfSuite.setName( label.c_str() );
          m_PerfSuite.setAction( "Puts" );
          m_PerfSuite.setNumKeys( m_MaxKeys );
          m_PerfSuite.setNumClients( numClients );
          m_PerfSuite.setValueSize( valSize );
          m_PerfSuite.setNumThreads( numThreads );
          FWKINFO( "Doing " << m_PerfSuite.asString() << " test." );

          MeteredPutsTask * mputs = new MeteredPutsTask( region, m_KeysA, m_MaxKeys, m_CValue, opsSec );
          FWKINFO( "Running warmup task." );
          if ( !clnt->runIterations( mputs, m_MaxKeys, 1, 0 ) ) {
            clearKeys();
            FWKEXCEPTION( "In putBursts()  Warmup timed out." );
          }
          delete mputs;
          perf::sleepSeconds( 10 );

          PutsTask * puts = new PutsTask( region, m_KeysA, m_MaxKeys, m_CValue );          FWKINFO( "Running warmup task." );
          if ( !clnt->runIterations( puts, m_MaxKeys, 1, 0 ) ) {
            FWKEXCEPTION( "In putBursts()  Warmup timed out." );
          }
          perf::sleepSeconds( 3 );

          int32_t loopIters = ( ( timedInterval * 1000 ) / burstMillis ) + 1;
          FWKINFO( "Running timed task." );
          uint32_t totIters = 0;
          uint32_t totMicros = 0;
          for ( int32_t i = loopIters; i > 0; i-- ) {
            if ( !clnt->timeMillisInterval( puts, burstMillis, numThreads, 30 ) ) {
              clearKeys();
              FWKEXCEPTION( "In putBursts()  Timed run timed out." );
            }
            totIters += puts->getIters();
            totMicros += ( uint32_t )clnt->getTotalMicros();
            double psec = ( totIters * 1000000.0 ) / totMicros;
            FWKINFO( "PerfSuit interim: " << ( int32_t )psec << "   " << totIters << "   " << totMicros );
            perf::sleepSeconds( burstPause );
          }
          if(clnt->getTaskStatus() == FWK_SEVERE)
            FWKEXCEPTION( "Exception during putBursts task");
          m_PerfSuite.addRecord( totIters, totMicros );
          // real work complete for this pass thru the loop

          numThreads = getIntValue( "numThreads" );
          if ( numThreads > 0 ) {
            perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
          }
          delete puts;
        } // thread loop

        valSize = initValues(numKeys, 0, false);
        if ( valSize > 0 ) {
          perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
        }
      } // value loop

      numKeys = initKeys(false);
      if ( numKeys > 0 ) {
        perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
      }
    } // keys loop
    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "PerfTest::putBursts() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "PerfTest::putBursts() Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "PerfTest::putBursts() Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::putBursts() Caught unknown exception." );
    //perf::sleepSeconds( 120 );
  }
  clearKeys();
  perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
  FWKINFO( "PerfTest::putBursts() complete." );
  return result;
}

// ----------------------------------------------------------------------------
int32_t PerfTest::popServers()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::popServers()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    resetValue( "distinctKeys" );
    int32_t numKeys = initKeys();

    resetValue( "valueSizes" );
    initValues(numKeys);

    int32_t opsSec = getIntValue( "opsSecond" );

    MeteredPutsTask * puts = new MeteredPutsTask( region, m_KeysA, m_MaxKeys, m_CValue, opsSec );
    if ( !clnt->runIterations( puts, m_MaxKeys, 1, 0 ) ) {
      clearKeys();
      FWKEXCEPTION( "In popServers()  Puts timed out." );
    }
    delete puts;
    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "PerfTest::popServers() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "PerfTest::popServers() Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "PerfTest::popServers() Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::popServers() Caught unknown exception." );
  }
  clearKeys();
  FWKINFO( "PerfTest::popServers() complete." );
  return result;
}

// ----------------------------------------------------------------------------
int32_t PerfTest::latencyPuts()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::latencyPuts()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue( "clientCount" );
    if ( numClients < 0 ) {
      std::string key( region->getName() );
      int64_t cnt = bbGet( REGIONSBB, key );
      numClients = ( int32_t )cnt;
    }

    std::string label = RegionHelper::regionTag( region->getAttributes() );

    int32_t timedInterval = getTimeValue( "timedInterval" );
    if ( timedInterval <= 0 ) {
      timedInterval = 5;
    }

    int32_t opsSec = getIntValue( "opsSecond" );
    if ( opsSec < 0 ) {
      opsSec = 100;
    }

    std::string bb( "LatencyBB" );
    std::string key( "LatencyTag" );

    // Loop over key set sizes
    resetValue( "distinctKeys" );
    int32_t numKeys = initKeys(false);
    while ( numKeys > 0 ) { // keys loop
      // Loop over value sizes
      resetValue( "valueSizes" );
      int32_t valSize = initValues(numKeys, 0, false);
      while ( valSize > 0 ) { // value loop
        // Loop over threads
        resetValue( "numThreads" );
        int32_t numThreads = getIntValue( "numThreads" );
        while ( numThreads > 0 ) { // thread loop

          // And we do the real work now
          m_PerfSuite.setName( label.c_str() );
          m_PerfSuite.setAction( "LatencyPuts" );
          m_PerfSuite.setNumKeys( m_MaxKeys );
          m_PerfSuite.setNumClients( numClients );
          m_PerfSuite.setValueSize( valSize );
          m_PerfSuite.setNumThreads( numThreads );
          FWKINFO( "Doing " << m_PerfSuite.asString() << " test." );
          bbSet( bb, key, m_PerfSuite.asString() );

          LatencyPutsTask * puts = new LatencyPutsTask( region, m_KeysA, 10,
            m_CValue, g_test, 0 );
          FWKINFO( "Running warmup task." );
          if ( !clnt->runIterations( puts, m_MaxKeys, 1, 0 ) ) {
            clearKeys();
            FWKEXCEPTION( "In doPuts()  Warmup timed out." );
          }
          perf::sleepSeconds( 3 );
          delete puts;
          puts = new LatencyPutsTask( region, m_KeysA, m_MaxKeys, m_CValue, g_test, opsSec );
          FWKINFO( "Running timed task." );
          if ( !clnt->timeInterval( puts, timedInterval, numThreads, 10 * timedInterval ) ) {
            clearKeys();
            FWKEXCEPTION( "In latencyPuts()  Timed run timed out." );
          }
          if(clnt->getTaskStatus() == FWK_SEVERE)
            FWKEXCEPTION( "Exception during latencyPuts task");
          m_PerfSuite.addRecord( puts->getIters(), ( uint32_t )clnt->getTotalMicros() );
          // real work complete for this pass thru the loop

          numThreads = getIntValue( "numThreads" );
          if ( numThreads > 0 ) {
            perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
          }
          delete puts;
        } // thread loop

        valSize = initValues(numKeys, 0, false);
        if ( valSize > 0 ) {
          perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
        }
      } // value loop

      numKeys = initKeys(false);
      if ( numKeys > 0 ) {
        perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
      }
    } // keys loop
    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "PerfTest::latencyPuts() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "PerfTest::latencyPuts() Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "PerfTest::latencyPuts() Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::latencyPuts() Caught unknown exception." );
    //perf::sleepSeconds( 120 );
  }
  clearKeys();
  perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
  FWKINFO( "PerfTest::latencyPuts() complete." );
  return result;
}

// ----------------------------------------------------------------------------

int32_t PerfTest::popClient()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::popClient()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    resetValue( "distinctKeys" );
    initKeys();

    GetsTask * gets = new GetsTask( region, m_KeysA, m_MaxKeys );
    if ( !clnt->runIterations( gets, m_MaxKeys, 1, 0 ) ) {
      clearKeys();
      FWKEXCEPTION( "In popClient()  Gets timed out." );
    }

    delete gets;
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKSEVERE( "PerfTest::popClient Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKSEVERE( "PerfTest::popClient Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKSEVERE( "PerfTest::popClient Caught unknown exception." );
  }
  clearKeys();
  FWKINFO( "PerfTest::popClient() complete." );
  return result;
}

int32_t PerfTest::popClientMS()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::popClientMS()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    resetValue( "distinctKeys" );
    int32_t numKeys = getIntValue( "distinctKeys" );
    int32_t clntCount = getIntValue( "clientCount" );
    int32_t interestPercent = getIntValue( "interestPercent" );
    if ( interestPercent <= 0 ) {
      if ( clntCount <= 0 ) {
        interestPercent = 100;
      }
      else {
        interestPercent = ( 100 / clntCount );
      }
    }

    int32_t myNumKeys = ( numKeys * interestPercent ) / 100;
    int32_t myId = getClientId();
    int32_t myStart = ( myId - 1 ) * myNumKeys;
    int32_t myValsize = 10;
    m_MaxKeys = myNumKeys;
    initIntKeys( myStart, myStart + myNumKeys );
    initValues( myNumKeys, myValsize );

    FWKINFO( "Client Id: " << myId << "  Client count: " << clntCount << "  Max keys: " << numKeys );
    PutsTask * puts = new PutsTask( region, m_KeysA, m_MaxKeys, m_CValue );
    if ( !clnt->runIterations( puts, m_MaxKeys, 1, 0 ) ) {
      clearKeys();
      FWKEXCEPTION( "In popClientMS()  Gets timed out." );
    }

    delete puts;
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::popClientMS Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::popClientMS Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::popClientMS Caught unknown exception." );
  }
  clearKeys();
  FWKINFO( "PerfTest::popClientMS() complete." );
  return result;
}

// ----------------------------------------------------------------------------

int32_t PerfTest::verifyInterestList()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::verifyInterestList()" );

  try {
    int32_t countUpdate = 0;
    RegionPtr region = getRegionPtr();
    int32_t numOfRegisterKeys = getIntValue( "registerKeys");
    resetValue( "keyIndexBegin" );
    int32_t low = getIntValue( "keyIndexBegin" );
    low = (low > 0) ? low : 0;
    resetValue("limit");
    int32_t Limit=getIntValue("limit");
    int32_t high = Limit + low;
    int32_t payload = getIntValue( "valueSizes");
    resetValue("checkFirst");
    bool checkFirst=getBoolValue("checkFirst");
    
    VectorOfCacheableKey keys;
    region->keys(keys);
    CacheableBytesPtr valuePtr;
    CacheableKeyPtr keyPtr;
    CacheableKeyPtr regKey;
    VectorOfCacheableString vreg;
    region->getInterestListRegex(vreg);
    uint32_t valueSize;
    FWKINFO("Keys in region is" << keys.size()<<" KeyIndexBegin is i.e low = "<< low << " high = "<<high);
    
    std::vector<CacheableKeyPtr> registerKeyList; 
    //char buf[128];
    for (int j = low; j < high; j++) {
     if (m_KeysA[j - low] != NULLPTR) {
       registerKeyList.push_back(m_KeysA[j - low]);
     } 
     else {
       FWKINFO("PerfTest::registerInterestList key is NULL");
     }
    }
    for(int32_t i = 0; i < (int32_t) keys.size(); i++)
    {
      /* check for keys starting from (300 -399 ) skip these keys 
 *    and checkn the values for remaining keys.
 * */
      keyPtr = keys.at(i);
      bool containsValue = region->containsValueForKey(keyPtr);
      FWKINFO("conatins value is "<<containsValue << "for key "<< keyPtr->toString()->asChar());
      if(checkFirst)
      {
        FWKINFO("getInitial Value is false hence value for every key should be null");
        if(containsValue)
        {
          FWKEXCEPTION("Expected value for key "<<keyPtr->toString()->asChar()<<" to be null");
        }
        else
         FWKINFO("conatins value is "<<containsValue << "for key "<< keyPtr->toString()->asChar());
      }
      else{
        bool isPresent=(std::find(registerKeyList.begin(),registerKeyList.end(),keyPtr)!= registerKeyList.end());
        if(!isPresent && !containsValue)
        { 
           FWKINFO("Skipping the check for key "<<keyPtr->toString()->asChar());
        }
        else
        {
          RegionEntryPtr entry = region->getEntry(keyPtr);
          if (entry == NULLPTR) {
            FWKEXCEPTION("Failed to find entry for key [" <<
          keyPtr->toString()->asChar() << "] in local cache");
          } 
          valuePtr = dynCast<CacheableBytesPtr>(entry->getValue());
          if (valuePtr == NULLPTR) {
            FWKEXCEPTION("Failed to find value for index " << i << " key [" <<
            keyPtr->toString()->asChar() << "] in local cache");
          }
          valueSize = valuePtr->length();
          if( (int32_t)valueSize == payload )
          {
            ++countUpdate;
          }
        }
      }
    }
    if( countUpdate == numOfRegisterKeys){
      FWKINFO( "PerfTest::verifyInterestList update interest list count " << countUpdate <<
                 " is equal to number of register keys " << numOfRegisterKeys);
      result = FWK_SUCCESS;
    }
    else{
      FWKSEVERE( "PerfTest::verifyInterestList update interest list count " << countUpdate << " is not equal to number of register keys " << numOfRegisterKeys);
      return result;
    }

  } catch (const Exception& e) {
    FWKSEVERE("PerfTest::verifyInterestList Caught Exception: " << e.getName()
        << ": " << e.getMessage());
  } catch ( FwkException& e ) {
    FWKSEVERE( "PerfTest::verifyInterestList Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKSEVERE( "PerfTest::verifyInterestList Caught unknown exception." );
  }
  FWKINFO( "PerfTest::verifyInterestList complete." );
  return result;
}
//-----------------------------------------------------------------------------------------
int32_t PerfTest::gets()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::gets()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();
    RegionAttributesPtr atts = region->getAttributes();

    int32_t numClients = getIntValue( "clientCount" );

    std::string label = RegionHelper::regionTag( region->getAttributes() );
    int32_t timedInterval = getTimeValue( "timedInterval" );
    if ( timedInterval <= 0 )
      timedInterval = 5;

    resetValue( "distinctKeys" );
    initKeys();

    int32_t valSize = getIntValue( "valueSizes" );

    // Loop over threads
    resetValue( "numThreads" );
    int32_t numThreads = getIntValue( "numThreads" );
    double vs = 0, rs = 0;
    measureMemory("Memory Before get operation: ", vs, rs);
    while ( numThreads > 0 ) { // thread loop

      // And we do the real work now
      m_PerfSuite.setName( label.c_str() );
      m_PerfSuite.setAction( "Gets" );
      m_PerfSuite.setNumKeys( m_MaxKeys );
      m_PerfSuite.setNumClients( numClients );
      m_PerfSuite.setValueSize( valSize );
      m_PerfSuite.setNumThreads( numThreads );
      FWKINFO( "Doing " << m_PerfSuite.asString() << " test." );

      GetsTask * gets = new GetsTask( region, m_KeysA, m_MaxKeys );
      FWKINFO( "Running warmup task." );

      bool checked = checkReady(numClients);
      if ( !clnt->runIterations( gets, m_MaxKeys, 1, 0 ) ) {
        clearKeys();
        FWKEXCEPTION( "In TestTask_gets()  Warmup timed out." );
      }
    // do localInvalidateRegion so that the gets dont happen on local region,but on the server,but dont do localInvalidateRegion when diskPolicy= overFlow
      if(!atts->getDiskPolicy())
        region->localInvalidateRegion();
      perf::sleepSeconds( 3 );
      FWKINFO( "Running timed task." );
      if ( !clnt->timeInterval( gets, timedInterval, numThreads, 300 + timedInterval ) ) {
        clearKeys();
        FWKEXCEPTION( "In TestTask_gets()  Timed run timed out." );
      }
      if(clnt->getTaskStatus() == FWK_SEVERE)
        FWKEXCEPTION( "Exception during get task");
      m_PerfSuite.addRecord( gets->getIters(), ( uint32_t )clnt->getTotalMicros() );
      // real work complete for this pass thru the loop
      if (checked) {
        bbDecrement( CLIENTSBB, READYCLIENTS );
      }

      numThreads = getIntValue( "numThreads" );
      if ( numThreads > 0 ) {
        perf::sleepSeconds( 3 );
      }
      delete gets;
    } // thread loop
    measureMemory("Memory After get operation: ", vs, rs);
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::gets Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::gets Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::gets Caught unknown exception." );
  }
  clearKeys();
  perf::sleepSeconds( 3 );
  FWKINFO( "PerfTest::gets() complete." );
  return result;
}

// -------------------------------------------------------------------------------------------

int32_t PerfTest::verifyDB()
{
  int32_t result = FWK_SUCCESS;
  FWKINFO("In PerfTest::verifyDB()");
  try{
    resetValue( "exists" );
    //bool ifExists = getBoolValue("exists");
    std::string name = getStringValue( "regionName" );
    //std::string subReg[5] = {"R1","R2","R3","R4","R5"};
    const char* regName=name.c_str();
    char sqliteDirRgn[512];
    //char sqliteSubReg[512];
    char *path = NULL;
    path = getcwd(NULL, 0); // or _getcwd
    resetValue("expectedPass");
    bool isExpectedPass = getBoolValue("expectedPass");
    if ( path != NULL)
    {
      printf("CURRENT = %s\n", path);
      sprintf(sqliteDirRgn, "%s/SqLiteRoot1/%s/%s.db",path,regName,regName);
      printf("Parent db DIR is %s",sqliteDirRgn);
      ACE_stat fileStat;
      if(isExpectedPass){
        if(ACE_OS::stat(sqliteDirRgn, &fileStat) == -1)
        {
          FWKINFO("FILE DOESNOT exist");
          FWKSEVERE("DB File should have been present");
          result=FWK_SEVERE;
        }
        else
          FWKINFO("FILE still exists");
      }
      else
      {
        if(ACE_OS::stat(sqliteDirRgn, &fileStat) == -1)
        {
          FWKINFO("Expected :FILE DOESNOT exist");
        }
        else{
          FWKSEVERE("DB FILE still exists,even after regionDestroy");
          result=FWK_SEVERE;
        }  
      }
    }
  }
  catch ( Exception & e ) {
    FWKEXCEPTION( "PerfTest::verifyDB() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "PerfTest::verifyDB() Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "PerfTest::verifyDB() Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::verifyDB() Caught unknown exception." );
  }
  return result;
}

//---------------------------------------------------------------------------

int32_t PerfTest::checkOverFlow()
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "In PerfTest::checkOverFlow()" );
  try{
    RegionPtr region = getRegionPtr();
    VectorOfCacheableKey v;
    region->keys(v);
    CacheableKeyPtr keyPtr;
    CacheablePtr valuePtr;
    RegionAttributesPtr atts = region->getAttributes();
    int lruLimit = atts->getLruEntriesLimit();
    int overflowCount=0;
    int nonOverflowCount = 0;
    FWKINFO("\nKeys vector size is" << v.size()<<"\n");
    for(int i = 0; i < v.size(); i++)
    {
      keyPtr = v.at(i);
      RegionEntryPtr rPtr = region->getEntry(keyPtr);
      valuePtr = rPtr->getValue( );
      FWKINFO("KEY IS "<<keyPtr->toString()->asChar());
      if(CacheableToken::isOverflowed(valuePtr)==true)
       {
        FWKINFO("OVERFLOW VALUE IS "<<valuePtr->toString()->asChar());
        overflowCount++;
       }
      else if(valuePtr != NULLPTR)
       {
        FWKINFO("NONOVERFLOW VALUE IS "<<valuePtr->toString()->asChar());
        nonOverflowCount++;
       }
      else
        FWKINFO("INVALID VALUE ");
      valuePtr = NULLPTR;
    }
    int sputCount=0, sgetCount=0, soverflowCount =0;
    StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
    StatisticsType* type = factory->findType("RegionStatistics");
    if(type ) {
      Statistics* rStats = factory->findFirstStatisticsByType(type);
      if (rStats) {
        sputCount = rStats->getInt((char*)"puts");
        sgetCount = rStats->getInt((char*)"gets");
        soverflowCount = rStats->getInt((char*)"overflows");
      }
    }
    LOGINFO("overflow count is %d & put count is %d get count is = %d", soverflowCount, sputCount, sgetCount);
    FWKINFO("overflow count is "<<soverflowCount <<" & put count is" <<sputCount <<"get count is = "<< sgetCount);
    FWKINFO("Overflow cnt is " << overflowCount);
    FWKINFO("Non Overflow cnt is " << nonOverflowCount);
    //if(lruLimit != nonOverflowCount)
    if(nonOverflowCount <= lruLimit )
    {
      FWKINFO("Overflow cnt is "<< overflowCount << " is equal to expected cnt ");
    }
    else
    {
      FWKSEVERE("LRU entries limit "<< lruLimit << " is not equal to normal entries count "<<nonOverflowCount);
      result=FWK_SEVERE;
    }
 }
  catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::gets Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::gets Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::gets Caught unknown exception." );
  }
  return result;
}

//----------------------------------------------------------------------------
int32_t PerfTest::puts()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::puts()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue( "clientCount" );
    std::string label = RegionHelper::regionTag( region->getAttributes() );

    int32_t timedInterval = getTimeValue( "timedInterval" );
    if ( timedInterval <= 0 ) {
      timedInterval = 5;
    }

    // Loop over key set sizes
    resetValue( "distinctKeys" );
    int32_t numKeys = initKeys(false);
    double vs = 0, rs = 0;
    measureMemory("Memory Before put operation: ", vs, rs);
    while ( numKeys > 0 ) { // keys loop
       // Loop over value sizes
      resetValue( "valueSizes" );
      int32_t valSize = initValues(numKeys, 0 , false);
      while ( valSize > 0 ) { // value loop
        // Loop over threads
        resetValue( "numThreads" );
        int32_t numThreads = getIntValue( "numThreads" );
        while ( numThreads > 0 ) { // thread loop

          // And we do the real work now
          m_PerfSuite.setName( label.c_str() );
          m_PerfSuite.setAction( "Puts" );
          m_PerfSuite.setNumKeys( m_MaxKeys );
          m_PerfSuite.setNumClients( numClients );
          m_PerfSuite.setValueSize( valSize );
          m_PerfSuite.setNumThreads( numThreads );
          FWKINFO( "Doing " << m_PerfSuite.asString() << " test." );
          PutsTask * puts = new PutsTask( region, m_KeysA, m_MaxKeys, m_CValue );
          FWKINFO( "Running warmup task." );

          bool checked = checkReady(numClients);
          if ( !clnt->runIterations( puts, m_MaxKeys, 1, 0 ) ) {
            clearKeys();
            FWKEXCEPTION( "In doPuts()  Warmup timed out." );
          }
          perf::sleepSeconds( 3 );
          FWKINFO( "Running timed task." );
          if ( !clnt->timeInterval( puts, timedInterval, numThreads, 10 * timedInterval ) ) {
            clearKeys();
            FWKEXCEPTION( "In doPuts()  Timed run timed out." );
          }
          if(clnt->getTaskStatus() == FWK_SEVERE)
            FWKEXCEPTION( "Exception during put task");
          m_PerfSuite.addRecord( puts->getIters(), ( uint32_t )clnt->getTotalMicros() );
          // real work complete for this pass thru the loop

          if (checked) {
            bbDecrement( CLIENTSBB, READYCLIENTS );
          }

          numThreads = getIntValue( "numThreads" );
          if ( numThreads > 0 ) {
            perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
          }
          delete puts;
        } // thread loop

        valSize = initValues(numKeys, 0, false);
        if ( valSize > 0 ) {
          perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
        }
      } // value loop

      numKeys = initKeys(false);
      if ( numKeys > 0 ) {
        perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
      }
    } // keys loop
    measureMemory("Memory After put operation: ", vs, rs);
    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "PerfTest::puts() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "PerfTest::puts() Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "PerfTest::puts() Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::puts() Caught unknown exception." );
    //perf::sleepSeconds( 120 );
  }
  clearKeys();
  perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
  FWKINFO( "PerfTest::puts() complete." );
  return result;
}
//----------------------------------------------------------------------------------------------
int32_t PerfTest::serialPuts()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::serialPuts()" );

  try {
    RegionPtr region = getRegionPtr();

    int keyStart = getIntValue( "keyStart");
    int keyEnd = getIntValue( "keyEnd");

    for (int keys = keyStart; keys <= keyEnd; keys++)
    {
      if (keys % 50 == 1)
      {
        FWKINFO("PerfTest::serialPuts() putting 1000 values for key " << keys);
      }

      CacheableInt32Ptr key = CacheableInt32::create(keys);

      for (int values = 1; values <= 1000; values++)
      {
        CacheableInt32Ptr value = CacheableInt32::create(values);

        region->put(key, value);
      }
    }

    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "PerfTest::serialPuts() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "PerfTest::serialPuts() Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "PerfTest::serialPuts() Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::serialPuts() Caught unknown exception." );
  }
  //clearKeys();
  perf::sleepSeconds( 3 ); // Put a marker of inactivity in the stats
  FWKINFO( "PerfTest::serialPuts() complete." );
  return result;
}

// ----------------------------------------------------------------------------

int32_t PerfTest::populateRegion()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::populateRegion()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();
    resetValue( "distinctKeys" );
    initValues( initKeys());

    double vs = 0, rs = 0;
    measureMemory("Memory Before populate operation: ", vs, rs);
    PutsTask puts( region, m_KeysA, m_MaxKeys, m_CValue );
    measureMemory("Memory Before populate operation: ", vs, rs);
    FWKINFO( "Populating region." );
    if ( !clnt->runIterations( &puts, m_MaxKeys, 1, 0 ) ) {
      FWKEXCEPTION( "In populateRegion()  Population timed out." );
    }
    FWKINFO( "Added " << puts.getIters() << " entries." );
    result = FWK_SUCCESS;
  } catch ( std::exception e ) {
    FWKEXCEPTION( "PerfTest::populateRegion() Caught std::exception: " << e.what() );
  } catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::populateRegion() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::populateRegion() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::populateRegion() Caught unknown exception." );
  }
  FWKINFO( "PerfTest::populateRegion() complete." );
  return result;
}
//----------------------------------------------------------------------------
int32_t PerfTest::putAllRegion()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::putAllRegion()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();
    resetValue( "distinctKeys" );
    initValues( initKeys());

    PutAllTask putAll( region, m_KeysA, m_MaxKeys, m_CValue );
    FWKINFO( "Populating region." );
    if ( !clnt->runIterations( &putAll, m_MaxKeys, 1, 0 ) ) {
      FWKEXCEPTION( "In putAllRegion()  Population timed out." );
    }
    FWKINFO( "Added " << putAll.getIters() << " entries." );
    result = FWK_SUCCESS;
  } catch ( std::exception e ) {
    FWKEXCEPTION( "PerfTest::putAllRegion() Caught std::exception: " << e.what() );
  } catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::putAllRegion() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::putAllRegion() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::putAllRegion() Caught unknown exception." );
  }
  FWKINFO( "PerfTest::putAllRegion() complete." );
  return result;
}
//--------------------------------------------------------
int32_t PerfTest::destroysKeys()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::destroysKeys()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();
    resetValue( "distinctKeys" );
    initValues( initKeys());

    DestroyTask destroys(region, m_KeysA, m_MaxKeys );
    FWKINFO( "Populating region." );
    if ( !clnt->runIterations( &destroys, m_MaxKeys, 1, 0 ) ) {
      FWKEXCEPTION( "In destroysKeys()  Population timed out." );
    }
    FWKINFO( "Added " << destroys.getIters() << " entries." );
    result = FWK_SUCCESS;
  } catch ( std::exception e ) {
    FWKEXCEPTION( "PerfTest::destroysKeys() Caught std::exception: " << e.what() );
  } catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::destroysKeys() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::destroysKeys() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::destroysKeys() Caught unknown exception." );
  }
  FWKINFO( "PerfTest::destroysKeys() complete." );
  return result;
}
//---------------------------------------------
int32_t PerfTest::registerInterestList()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::registerInterestList()" );

  try {
  static char keyType = 'i';
  std::string typ = getStringValue( "keyType" ); // int is only value to use
  char newType = typ.empty() ? 's' : typ[0];

  RegionPtr region = getRegionPtr();
  resetValue( "distinctKeys" );
  resetValue( "keyIndexBegin" );
  resetValue( "registerKeys" );
  int32_t numKeys = getIntValue( "distinctKeys" );  // check distince keys first
  if (numKeys <= 0) {
    //FWKSEVERE( "Failed to initialize keys with numKeys :" <<  numKeys);
    return  result;
  }
  int32_t low = getIntValue( "keyIndexBegin" );
  low = (low > 0) ? low : 0;
  int32_t numOfRegisterKeys = getIntValue( "registerKeys");
  int32_t high = numOfRegisterKeys + low;

  clearKeys();
  m_MaxKeys = numOfRegisterKeys;
  m_KeyIndexBegin = low;
  keyType = newType;
  VectorOfCacheableKey registerKeyList;
  if ( keyType == 'i' ) {
   initIntKeys(low, high);
  } else {
    int32_t keySize = getIntValue( "keySize" );
    keySize = (keySize > 0) ? keySize : 10;
    std::string keyBase(keySize, 'A');
    initStrKeys(low, high, keyBase);
  }

  CacheableKeyPtr keyPtr;
  //uint32_t valueSize;
  CacheableBytesPtr valuePtr;
  for (int j = low; j < high; j++) {
    if (m_KeysA[j - low] != NULLPTR) {
      RegionEntryPtr entry = region->getEntry(m_KeysA[j - low]);
      registerKeyList.push_back(m_KeysA[j - low]);
    }
    else {
      FWKINFO("PerfTest::registerInterestList key is NULL");
    }
  }
  resetValue( "getInitialValues" );
  bool isGetInitialValues = getBoolValue( "getInitialValues" );
  bool isReceiveValues = true;
  bool checkReceiveVal = getBoolValue("checkReceiveVal");
  if (checkReceiveVal) {
    resetValue("receiveValue");
    isReceiveValues = getBoolValue("receiveValue");
  }
  resetValue( "isDurableReg" );
  bool isDurableReg = getBoolValue( "isDurableReg" );
  FWKINFO("PerfTest::registerInterestList region name is " << region->getName()
      << "; getInitialValues is " << isGetInitialValues);
  region->registerKeys(registerKeyList, isDurableReg, isGetInitialValues,isReceiveValues);
  const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
  if(strlen(durableClientId) > 0) {
      m_cache->readyForEvents();
    }
  result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "PerfTest::registerInterestList() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::registerInterestList() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::registerInterestList() Caught unknown exception." );
  }
  FWKINFO( "PerfTest::registerInterestList() complete." );
  return result;

}

// ----------------------------------------------------------------------------

int32_t PerfTest::registerRegexList()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::registerRegexList()" );
  try {
    RegionPtr region = getRegionPtr();
    std::string registerRegex = getStringValue("registerRegex");
    FWKINFO("PerfTest::registerRegexList region name is " << region->getName() << "regex is: " << registerRegex.c_str());
    resetValue( "getInitialValues" );
    bool isGetInitialValues = getBoolValue( "getInitialValues" );
    bool isReceiveValues = true;
    bool checkReceiveVal = getBoolValue("checkReceiveVal");
    if (checkReceiveVal) {
      resetValue("receiveValue");
      isReceiveValues = getBoolValue("receiveValue");
    }
    resetValue( "isDurableReg" );
    bool isDurableReg = getBoolValue( "isDurableReg" );
    region->registerRegex(registerRegex.c_str(), isDurableReg,
        NULLPTR, isGetInitialValues,isReceiveValues);
    const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
    if(strlen(durableClientId) > 0) {
      m_cache->readyForEvents();
    }
    result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "PerfTest::registerRegexList() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::registerRegexList() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::registerRegexList() Caught unknown exception." );
  }
  FWKINFO( "PerfTest::registerRegexList() complete." );
  return result;

}
// ----------------------------------------------------------------------------

int32_t PerfTest::unregisterRegexList()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::unregisterRegexList()" );
  try {
    RegionPtr region = getRegionPtr();
    std::string unregisterRegex = getStringValue("unregisterRegex");
    FWKINFO("PerfTest::unregisterRegexList region name is " << region->getName() << "regex is: " << unregisterRegex.c_str());
    region->unregisterRegex(unregisterRegex.c_str());
    result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "PerfTest::unregisterRegexList() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::unregisterRegexList() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::unregisterRegexList() Caught unknown exception." );
  }
  FWKINFO( "PerfTest::unregisterRegexList() complete." );
  return result;

}

//-----------------------------------------------------------------------------
int32_t PerfTest::registerAllKeys()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::registerAllKeys()" );

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
    FWKEXCEPTION( "PerfTest::registerAllKeys() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::registerAllKeys() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::registerAllKeys() Caught unknown exception." );
  }
  FWKINFO( "PerfTest::registerAllKeys() complete." );
  return result;

}
//-----------------------------------------------------------------------------
int32_t PerfTest::registerAllKeysWithResultKeys()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::registerAllKeys()" );

  try {
    RegionPtr region = getRegionPtr();
    resetValue( "getInitialValues" );
    bool isGetInitialValues = getBoolValue( "getInitialValues" );
    FWKINFO("PerfTest::registerAllKeys region name is " << region->getName()
        << "; getInitialValues is " << isGetInitialValues);
    bool isReceiveValues = true;
    VectorOfCacheableKeyPtr resultKeys(new VectorOfCacheableKey());
    bool checkReceiveVal = getBoolValue("checkReceiveVal");
    if (checkReceiveVal) {
      resetValue("receiveValue");
      isReceiveValues = getBoolValue("receiveValue");
    }
    region->registerAllKeys(false, resultKeys, isGetInitialValues,isReceiveValues);
    const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
    if(strlen(durableClientId) > 0) {
      m_cache->readyForEvents();
    }
    result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "PerfTest::registerAllKeys() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::registerAllKeys() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::registerAllKeys() Caught unknown exception." );
  }
  FWKINFO( "PerfTest::registerAllKeys() complete." );
  return result;

}

//-----------------------------------------------------------------------------
int32_t PerfTest::checkValues()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::checkValues()" );
  try {
    RegionPtr region = getRegionPtr();

    VectorOfCacheable vals;
    region->values( vals );
    int32_t creates = 0;
    int32_t updates = 0;
    int32_t unknowns = 0;
    for ( int32_t i = 0; i < vals.size(); i++ ) {
      CacheableBytesPtr valStr = dynCast<CacheableBytesPtr>( vals.at( i ) );
      if ( strncmp( "Create", ( const char * )valStr->value(), 6 ) == 0 )
        creates++;
      else
      if ( strncmp( "Update", ( const char * )valStr->value(), 6 ) == 0 )
        updates++;
      else
        unknowns++;
    }
    FWKINFO( "PerfTest::checkValues Found " << creates << " values from creates, " << updates \
      << " values from updates, and " << unknowns << " unknown values." );
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::checkValues FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::checkValues FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::checkValues FAILED -- caught unknown exception." );
  }

  return result;
}

// ----------------------------------------------------------------------------

int32_t PerfTest::localDestroyEntries()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::localDestroyEntries()" );
  try {
    RegionPtr region = getRegionPtr();
    VectorOfCacheableKey keys;
    region->keys( keys );
    double vs = 0, rs = 0;
    measureMemory("Memory Before local destroy operation: ", vs, rs);
    for ( int32_t i = 0; i < keys.size(); i++ ) {
      CacheableKeyPtr key = keys.at( i );
      region->localDestroy( key );
    }
    measureMemory("Memory After local destroy operation: ", vs, rs);
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::localDestroyEntries FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::localDestroyEntries FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::localDestroyEntries FAILED -- caught unknown exception." );
  }

  return result;
}

// ----------------------------------------------------------------------------

int32_t PerfTest::localDestroyRegion()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::localDestroyRegion()" );
  try {
    RegionPtr region = getRegionPtr();
    if (region != NULLPTR) {
      region->localDestroyRegion();
    }
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKSEVERE( "PerfTest::localDestroyRegion FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKSEVERE( "PerfTest::localDestroyRegion FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKSEVERE( "PerfTest::localDestroyRegion FAILED -- caught unknown exception." );
  }

  return result;
}

//----------------------------------------------------------------------------------

int32_t PerfTest::destroyRegion()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::destroyRegion()" );
  try {
    RegionPtr region = getRegionPtr();
    if (region != NULLPTR) {
      region->destroyRegion();
    }
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKSEVERE( "PerfTest::destroyRegion FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKSEVERE( "PerfTest::destroyRegion FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKSEVERE( "PerfTest::destroyRegion FAILED -- caught unknown exception." );
  }
  return result;
}
// ----------------------------------------------------------------------------

int32_t PerfTest::destroys()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::destroys()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = 0;
    std::string key( region->getName() );
    int64_t cnt = bbGet( REGIONSBB, key );
    numClients = ( int32_t )cnt;
    FWKINFO( "PerfTest::destroys numclients set to " << numClients );

    std::string label = RegionHelper::regionTag( region->getAttributes() );

    int32_t timedInterval = getTimeValue( "timedInterval" );
  if ( timedInterval <= 0 ){
      timedInterval = 5;
  }
    int32_t numThreads = 1;
  // always use test only one thread.
  int32_t numKeys = initKeys(false);
    // Loop over distinctKeys
    while ( numKeys > 0 ) { // thread loop
      int32_t valueSize = initValues( numKeys);
      // And we do the real work now
      m_PerfSuite.setName( label.c_str() );
      m_PerfSuite.setAction( "destroys" );
      m_PerfSuite.setNumKeys( numKeys );
      m_PerfSuite.setNumClients( numClients );
      m_PerfSuite.setValueSize( valueSize );
      m_PerfSuite.setNumThreads( numThreads );
    FWKINFO( "Doing " << m_PerfSuite.asString() << " test." );

    //populate the region
//	  PutsTask * puts = new PutsTask( region, m_KeysA, m_MaxKeys, m_CValue );
//      FWKINFO( "Populating region." );
//      if ( !clnt->runIterations( puts, m_MaxKeys, 1, 0 ) ) {
//        clearKeys();
//        FWKEXCEPTION( "In populateRegion()  Population timed out." );
//      }
    DestroyTask * destroys = new DestroyTask( region, m_KeysA, m_MaxKeys );
      FWKINFO( "Running timed task." );
      if ( !clnt->timeIterations( destroys, numKeys, numThreads, 10 * timedInterval ) ) {
        clearKeys();
        FWKEXCEPTION( "In TestTask_destroys()  Timed run timed out." );
      }
      m_PerfSuite.addRecord(destroys->getIters(), ( uint32_t )clnt->getTotalMicros() );
      // real work complete for this pass thru the loop

      numKeys = initKeys(false);
      perf::sleepSeconds( 3 );
//      delete puts;
      delete destroys;
    } // distinctKeys loop
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKEXCEPTION( "PerfTest::destroys Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::destroys Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "PerfTest::destroys Caught unknown exception." );
  }
  clearKeys();
  perf::sleepSeconds( 3 );
  FWKINFO( "PerfTest::destroys() complete." );
  return result;
}

// ----------------------------------------------------------------------------

int32_t PerfTest::netsearch()
{
  int32_t result = FWK_SUCCESS;
  // do the whole netsearch test, looping over threads, value sizes, distinct keys, etc.
  FWKINFO( "NEW netsearch invoked. SHOULD FAIL" );
  result = FWK_SEVERE;
  return result;
}

// ----------------------------------------------------------------------------

RegionPtr PerfTest::getRegionPtr( const char * reg )
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
        FWKEXCEPTION( "In PerfTest::getRegionPtr()  No regions exist." );
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

bool PerfTest::checkReady(int32_t numClients)
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

int32_t PerfTest::resetListener()
{
  int32_t result = FWK_SEVERE;
  try {
    RegionPtr region = getRegionPtr();
    int32_t sleepTime = getIntValue( "sleepTime" );
    PerfTestCacheListener * listener = dynamic_cast<PerfTestCacheListener *> (region->getAttributes()->getCacheListener().ptr());
    if (listener) {
      listener->reset(sleepTime);
    }
    result = FWK_SUCCESS;
  } catch ( Exception e ) {
    FWKSEVERE( "PerfTest::resetListener Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKSEVERE( "PerfTest::resetListener Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKSEVERE( "PerfTest::resetListener Caught unknown exception." );
  }
  perf::sleepSeconds( 3 );
  FWKINFO( "PerfTest::resetListener() complete." );
  return result;

}

int32_t PerfTest::validateQConflation()
{
  int32_t result = FWK_SEVERE;
  try {
    RegionPtr region = getRegionPtr();
    int32_t expectedAfterCreateEvent = getIntValue( "expectedAfterCreateCount" );
    int32_t expectedAfterUpdateEvent = getIntValue( "expectedAfterUpdateCount" );
    bool isServerConflateTrue = getBoolValue( "isServerConflateTrue" );

    char *conflateEvent = DistributedSystem::getSystemProperties()->conflateEvents();
    const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
    region->localDestroyRegion(); //to dump the event count to BB

    char name[32] = {'\0'};
    sprintf(name,"%d",g_test->getClientId());
    std::string rgnName = std::string(region->getName());
    std::string afterCreateKey = std::string( "AFTER_CREATE_COUNT_" ) + std::string(name) + std::string("_") + rgnName;
    std::string afterUpdateKey = std::string( "AFTER_UPDATE_COUNT_" ) + std::string(name) + std::string("_") + rgnName;
    std::string bb( "ConflationCacheListener" );
    int32_t eventAfterCreate = ( int32_t )bbGet( bb, afterCreateKey );
    int32_t eventAfterUpdate = ( int32_t )bbGet( bb, afterUpdateKey );
    int32_t totalCount = 3500;
    if(strlen(durableClientId) > 0) {
      if(strcmp(conflateEvent,"true") == 0 && ((eventAfterCreate + eventAfterUpdate) < totalCount + 10)){
        result = FWK_SUCCESS;
      } else if (strcmp(conflateEvent,"false") == 0 && ((eventAfterCreate + eventAfterUpdate) == totalCount + 10)){
        result = FWK_SUCCESS;
      }  else if (strcmp(conflateEvent,"server") == 0 && isServerConflateTrue && ((eventAfterCreate + eventAfterUpdate) < totalCount + 10)) {
         result = FWK_SUCCESS;
      }  else if (strcmp(conflateEvent,"server") == 0 && !isServerConflateTrue && ((eventAfterCreate + eventAfterUpdate) == totalCount + 10)) {
         result = FWK_SUCCESS;
      } else {
        FWKEXCEPTION( "ConflateEvent setting is "<< conflateEvent << " and Expected AfterCreateCount to have " << expectedAfterCreateEvent <<
                     " keys and found " << eventAfterCreate << ". Expected AfterUpdateCount to have " << expectedAfterUpdateEvent <<
                     " keys, found " <<  eventAfterUpdate << " keys " );
      }
    }
    else {
      if(strcmp(conflateEvent,"true") == 0 && ((eventAfterCreate == expectedAfterCreateEvent) &&
          (((eventAfterUpdate >= expectedAfterUpdateEvent)) && eventAfterUpdate < totalCount))) {
        result = FWK_SUCCESS;
      } else if (strcmp(conflateEvent,"false") == 0 && ((eventAfterCreate == expectedAfterCreateEvent) &&
          (eventAfterUpdate == expectedAfterUpdateEvent))){
        result = FWK_SUCCESS;
      }  else if (strcmp(conflateEvent,"server") == 0 && isServerConflateTrue && ((eventAfterCreate == expectedAfterCreateEvent) &&
          (((eventAfterUpdate >= expectedAfterUpdateEvent)) && eventAfterUpdate < totalCount ))) {
         result = FWK_SUCCESS;
      }  else if (strcmp(conflateEvent,"server") == 0 && !isServerConflateTrue && ((eventAfterCreate == expectedAfterCreateEvent) &&
          (eventAfterUpdate == expectedAfterUpdateEvent))) {
         result = FWK_SUCCESS;
      } else {
        FWKEXCEPTION( "ConflateEvent setting is "<< conflateEvent << " and Expected AfterCreateCount to have " << expectedAfterCreateEvent <<
                     " keys and found " << eventAfterCreate << ". Expected AfterUpdateCount to have " << expectedAfterUpdateEvent <<
                     " keys, found " <<  eventAfterUpdate << " keys " );
      }
    }
  } catch ( Exception e ) {
    FWKEXCEPTION("PerfTest::validateQConflation Caught Exception: " << e.getMessage());
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::validateQConflation Caught FwkException: " << e.getMessage() );
  }
  return result;
}

int32_t PerfTest::validateBankTest()
{
  int32_t result = FWK_SEVERE;
  try {
    RegionPtr region = getRegionPtr();
    region->localDestroyRegion(); //to dump the evnt count to BB
    int32_t expectedAfterCreateEvent = getIntValue( "expectedAfterCreateCount" );
    int32_t expectedAfterUpdateEvent = getIntValue( "expectedAfterUpdateCount" );
    int32_t expectedAfterInvalidateEvent = getIntValue( "expectedAfterInvalidateCount" );
    int32_t expectedAfterDestroyEvent = getIntValue( "expectedAfterDestroyCount" );
    bool checkOldValueNull = getBoolValue( "checkOldValueNull" );
    char name[32] = {'\0'};
    sprintf(name,"%d",g_test->getClientId());
    std::string rgnName = std::string(region->getName());
    std::string afterCreateKey = std::string( "AFTER_CREATE_COUNT_" ) + std::string(name) + std::string("_") + rgnName;
    std::string afterUpdateKey = std::string( "AFTER_UPDATE_COUNT_" ) + std::string(name) + std::string("_") + rgnName;
    std::string afterInvalidateKey = std::string( "AFTER_INVALIDATE_COUNT_" ) + std::string(name) + std::string("_") + rgnName;
    std::string afterDestroyKey = std::string( "AFTER_DESTROY_COUNT_" ) + std::string(name) + std::string("_") + rgnName;
    std::string bb( "ConflationCacheListener" );
    int32_t eventAfterCreate = ( int32_t )bbGet( bb, afterCreateKey );
    int32_t eventAfterUpdate = ( int32_t )bbGet( bb, afterUpdateKey );
    int32_t eventAfterInvalidate = ( int32_t )bbGet( bb, afterInvalidateKey );
    int32_t eventAfterDestroy = ( int32_t )bbGet( bb, afterDestroyKey );
    if(expectedAfterCreateEvent == eventAfterCreate && expectedAfterUpdateEvent == eventAfterUpdate
        && expectedAfterInvalidateEvent == eventAfterInvalidate && expectedAfterDestroyEvent == eventAfterDestroy){
      result = FWK_SUCCESS;
    }
    else {
      FWKINFO("Region: " << rgnName << " afterCreateKey = " << eventAfterCreate << ", afterUpdateKey = " << eventAfterUpdate <<
                        ", afterInvalidateKey = " << eventAfterInvalidate << " eventAfterDestroy = " << eventAfterDestroy );
      FWKEXCEPTION("Validation Failed: expected count is not equal to BB value");
    }
    if(checkOldValueNull){
      std::string keyIsOldValue = std::string( "isOldValue");
      std::string key1 = bbGetString(bb,keyIsOldValue);
      if(key1 == "true")
        FWKEXCEPTION("Validation Failed: entry event old value should not be null");
    }


  } catch ( Exception e ) {
    FWKEXCEPTION("PerfTest::validateBankTest Caught Exception: " << e.getMessage());
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "PerfTest::validateBankTest Caught FwkException: " << e.getMessage() );
  }
  return result;
}

int32_t PerfTest::getAllAndVerification()
{
  int32_t result = FWK_SEVERE;
  FWKINFO("In PerfTest::getAllAndVerification()");

  try
  {
    RegionPtr region = getRegionPtr();
    resetValue( "distinctKeys" );
    resetValue( "addToLocalCache" );
    resetValue( "valueSizes" );
    resetValue( "invalidateRegion" );
    int32_t numKeys = initKeys(false);
    int32_t i = 0;
    bool isInvalidateRegion = getBoolValue( "invalidateRegion" );
    if(isInvalidateRegion)
      region->localInvalidateRegion();
    VectorOfCacheableKey keyVec;
    while( i < numKeys)
    {
      keyVec.push_back(m_KeysA[i++]);
    }
    FWKINFO("PerfTest::getAllAndVerification region name is " << region->getName());
    FWKINFO("PerfTest::getAllAndVerification KeyCount = " << keyVec.size());
    bool isAddToLocalCache = getBoolValue( "addToLocalCache" );
    HashMapOfCacheablePtr values(new HashMapOfCacheable());
    ACE_Time_Value startTime = ACE_OS::gettimeofday();
    region->getAll(keyVec, values, NULLPTR, isAddToLocalCache);
    ACE_Time_Value interval = ACE_OS::gettimeofday() - startTime;
    FWKINFO("Time Taken to execute getAll for " << numKeys << " is: " <<
        interval.sec() << "." << interval.usec() << " sec");
    int32_t payload = getIntValue("valueSizes");
    FWKINFO("PerfTest::getAllAndVerification KeyCount = " << keyVec.size() <<
        " ValueCount = " << values->size());
    if(values->size() == keyVec.size())
    {
      for(HashMapOfCacheable::Iterator iter = values->begin(); iter != values->end(); iter++)
      {
        CacheablePtr key = dynCast<CacheablePtr>(iter.first());
        CacheableBytesPtr val = dynCast<CacheableBytesPtr>(iter.second());
        if(val->length() != payload )
        FWKEXCEPTION( "PerfTest::getAllAndVerification() value size " << val->length() << " is not "
            "equal to expected payload size " << payload << " for key : " << key->toString()->asChar());
      }
      result = FWK_SUCCESS;
    }
    if (isAddToLocalCache) {
      if (keyVec.size() != (int32_t)region->size()) {
        FWKEXCEPTION("PerfTest::getAllAndVerification() number of keys in "
            "region do not match expected number");
      }
    }
    else {
      if (region->size() != 0) {
        FWKEXCEPTION("PerfTest::getAllAndVerification() expected zero "
            "keys in region");
      }
    }

   }
  catch ( Exception& e )
  {
    FWKEXCEPTION( "PerfTest::getAllAndVerification() Caught Exception: " << e.getMessage() );
  }
  catch ( FwkException& e )
  {
    FWKEXCEPTION( "PerfTest::getAllAndVerification() Caught FwkException: " << e.getMessage() );
  }
  catch ( ... )
  {
    FWKEXCEPTION( "PerfTest::getAllAndVerification() Caught unknown exception." );
  }
  FWKINFO("PerfTest::getAllAndVerification() complete.");
  return result;

}


int32_t PerfTest::putAll()
{
  int32_t result = FWK_SEVERE;
  FWKINFO("In PerfTest::putAll()");

  try
  {
    RegionPtr region = getRegionPtr();
    resetValue( "distinctKeys" );
    resetValue( "valueSizes" );
    int32_t numKeys = initKeys(false);
    initValues(numKeys,0,false);
    int32_t i = 0;
    HashMapOfCacheable map;
    while( i < numKeys)
    {
      map.insert(m_KeysA[i],m_CValue[i]);
      i++;
    }
    FWKINFO("PerfTest::putAll region name is " << region->getName());
    ACE_Time_Value startTime = ACE_OS::gettimeofday();
    region->putAll(map,PUTALL_TIMEOUT);
    ACE_Time_Value interval = ACE_OS::gettimeofday() - startTime;
    FWKINFO("Time Taken to execute putAll for " << numKeys << " is: " <<
        interval.sec() << "." << interval.usec() << " sec");
    result = FWK_SUCCESS;
  }
  catch ( Exception& e )
  {
    FWKEXCEPTION( "PerfTest::putAll() Caught Exception: " << e.getMessage() );
  }
  catch ( FwkException& e )
  {
    FWKEXCEPTION( "PerfTest::putAll() Caught FwkException: " << e.getMessage() );
  }
  catch ( ... )
  {
    FWKEXCEPTION( "PerfTest::putAll() Caught unknown exception." );
  }
  clearKeys();
  FWKINFO("PerfTest::putAll() complete.");
  return result;
}

int32_t PerfTest::queries() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In PerfTest::queries()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }

    // Loop over key set sizes
    resetValue("queryResultType");
    resetValue("query");
    std::string queryType = getStringValue("queryResultType");
    if(queryType.empty())
      queryType = "resultSet";
    std::string queryStr = getStringValue("query"); // set the query string in xml
    if(queryStr.empty())
      queryStr = "select distinct * from /Portfolios where ID =";
    int32_t resultSize = getIntValue( "resultSize" );
    int32_t numItr = getIntValue( "numIteration" );
    if(numItr < 0)
      numItr = 1;
    if(resultSize < 0)
      resultSize = 1;
    resetValue("numThreads");
    int32_t numThreads = getIntValue("numThreads");
    resetValue("distinctKeys");
    int32_t numKeys = getIntValue( "distinctKeys" );
    ACE_Time_Value startTime,endTime;
    while (numThreads > 0) { // thread loop
      QueryTask * query = new QueryTask(region,numKeys,queryStr,queryType,resultSize,g_test);
      FWKINFO( "Running timed task." );
      startTime = ACE_OS::gettimeofday();
      if (!clnt->runIterations(query, numItr, numThreads, 0)) {
        FWKEXCEPTION( "In Queries()  Timed run timed out." );
      }
      endTime = ACE_OS::gettimeofday() - startTime;
      FWKINFO(" Time Taken to execute the queries with  : "<< numThreads << " threads is: " << endTime.sec() << "." << endTime.usec() << " sec");
      numThreads = getIntValue("numThreads");
      if (numThreads > 0) {
        perf::sleepSeconds(3); // Put a marker of inactivity in the stats
      }
      delete query;
    } // thread loop
    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "PerfTest::queries() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "PerfTest::queries() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "PerfTest::queries() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "PerfTest::queries() Caught unknown exception." );
    //perf::sleepSeconds( 120 );
  }
  perf::sleepSeconds(3); // Put a marker of inactivity in the stats
  FWKINFO( "PerfTest::queries() complete." );
  return result;
}
int32_t PerfTest::createUpdateDestroy()
{
  int32_t result = FWK_SEVERE;
    FWKINFO( "In PerfTest::createUpdateDestroy()" );

    try {
      result = populateRegion();
      if(result == FWK_SUCCESS){
        populateRegion();
        result = destroysKeys();
      }
    } catch (Exception & e) {
      FWKEXCEPTION( "PerfTest::createUpdateDestroy() Caught Exception: " << e.getMessage() );
    } catch (FwkException & e) {
      FWKEXCEPTION( "PerfTest::createUpdateDestroy() Caught FwkException: " << e.getMessage() );
    } catch (std::exception & e) {
      FWKEXCEPTION( "PerfTest::createUpdateDestroy() Caught std::exception: " << e.what() );
    } catch (...) {
      FWKEXCEPTION( "PerfTest::createUpdateDestroy() Caught unknown exception." );
      //perf::sleepSeconds( 120 );
    }
    FWKINFO( "PerfTest::createUpdateDestroy() complete." );
    return result;
}
int32_t PerfTest::measureMemory()
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "In PerfTest::measureMemory()" );
  try {
    double vs = 0, rs = 0;
    measureMemory("Measuring Memory: ", vs, rs);
    int32_t expectedsize = getIntValue("expectedSizeInMB");
    if( vs < expectedsize ) {
      result = FWK_SEVERE;
      FWKSEVERE(" vs size " << vs <<" is not equal or less then expected vm size " << expectedsize);
    }
  } catch (...) {
    FWKEXCEPTION( "PerfTest::measureMemory() Caught unknown exception." );
  }
  return result;
}
void PerfTest::measureMemory(std::string location, double & vs, double & rs)
{
  //sleep 2 seconds to let things settle down
  perf::sleepSeconds( 10 );

//#ifndef WIN32
  uint32_t pid = ACE_OS::getpid();
  char procFileName[128];
  FILE * fil;

  sprintf( procFileName, "/proc/%u/status", pid );
  fil = fopen( procFileName, "rb" ); /* read only */
  if ( fil == ( FILE * )0 ) {
    FWKINFO( "Unable to read status file." );
    vs = -1;
    rs = -1;
    return;
  }

  uint32_t val = 0;
  char rbuff[1024];
  while ( fgets( rbuff, 1023, fil ) != 0 ) {
    if ( ( ACE_OS::strncasecmp( rbuff, "VmSize:", 7 ) == 0 ) &&
      ( sscanf( rbuff, "%*s %u", &val ) == 1 ) )
    {
      vs = ( double )val;
    } else if ( ( ACE_OS::strncasecmp( rbuff, "VmRSS:", 6 ) == 0 ) &&
      ( sscanf( rbuff, "%*s %u", &val ) == 1 ) )
    {
      rs = ( double )val;
    }
  }
  fclose( fil );
  vs /= 1024.0;
  rs /= 1024.0;

  FWKINFO(location << "VmSize: " << vs << " Mb   RSS: " << rs << " Mb" << " for process id: " << pid);
//#else
//  FWKEXCEPTION("Memory measurement is not implemented on WIN32" );
//#endif
}
