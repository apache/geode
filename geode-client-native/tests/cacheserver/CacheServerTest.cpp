/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

/**
* @file    CacheServerTest.cpp
* @since   1.0
* @version 1.0
* @see
*
*/

// ----------------------------------------------------------------------------

#include <GemfireCppCache.hpp>
#include <SystemProperties.hpp>
#include <gfcpp_globals.hpp>

#include "CacheServerTest.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkStrCvt.hpp"
#include "fwklib/PaceMeter.hpp"
#include "fwklib/FwkExport.hpp"
#include "perfTests/PerfTest.hpp"
#include <statistics/StatisticsFactory.hpp>

#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Time_Value.h>
#include <stdlib.h>
#include <map>
#include <vector>
#include <ace/os_include/os_assert.h>
#define QUERY_RESPONSE_TIMEOUT 600
#define PUTALL_TIMEOUT 60

#ifdef _WIN32
  #ifdef llabs
    #undef llabs
  #endif
  #define llabs(x) ( ((x) < 0) ? ((x) * -1LL) : (x) )
#else
  #undef llabs
  #define llabs ::llabs
#endif

using namespace std;
using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire_statistics;
using namespace testData;

CacheServerTest * g_test = NULL;
static int32_t m_createCount = 0;
static int32_t m_createMin = 1000000000;
static int32_t m_createMax = 0;
static int32_t m_createAvg = 0;
static int64_t m_createTotal = 0;

static int32_t m_updateCount = 0;
static int32_t m_updateMin = 1000000000;
static int32_t m_updateMax = 0;
static int32_t m_updateAvg = 0;
static int64_t m_updateTotal = 0;

static int totalOperation = 0;
static int notAuthzCount = 0;
int32_t totalNumKeys;
bool doAddOperation = true;
static std::map< std::string, int > operationsMap;
static std::map< std::string, int > exceptionMap;

ExpectedRegionContents *static_RI_noops_keysValues;
ExpectedRegionContents *static_RI_noops_none;
ExpectedRegionContents *static_RI_ops_keysValues;
ExpectedRegionContents *static_RI_ops_none;
ExpectedRegionContents *static_ops_RI_keysValues;
ExpectedRegionContents *dynamicKeysValues;
ExpectedRegionContents *expectedRgnContents4;
// ----------------------------------------------------------------------------

TESTTASK initialize( const char * initArgs ) {
  int32_t result = FWK_SUCCESS;
  if ( g_test == NULL ) {
    FWKINFO( "Initializing CacheServerTest library." );
    try {
      g_test = new CacheServerTest( initArgs );
    } catch( const FwkException &ex ) {
      FWKSEVERE( "initialize: caught exception: " << ex.getMessage() );
      result = FWK_SEVERE;
    }
  }
  return result;
}

TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Finalizing CacheServerTest library." );
  if ( g_test != NULL ) {
    g_test->cacheFinalize();
    delete g_test;
    g_test = NULL;
  }
  return result;
}

// ----------------------------------------------------------------------------

void CacheServerTest::checkTest( const char * taskId ) {
  SpinLockGuard guard( m_lck );
  setTask( taskId );
  if (m_cache == NULLPTR || m_cache->isClosed()) {
    PropertiesPtr pp = Properties::create();
    // Default to PEER ( used by feeds ), other valid values are SERVER and CLIENT
    std::string typ = getStringValue( "cacheType" );

    cacheInitialize( pp );

    // CacheServerTest specific initialization
    m_cacheType = typ;
    const std::string bbName = getStringValue( "BBName" );
    if ( !bbName.empty() ) {
      m_bb = bbName;
    }
    bool isTimeOutInMillis = m_cache->getDistributedSystem()->getSystemProperties()->readTimeoutUnitInMillis();
    if(isTimeOutInMillis){
      #define QUERY_RESPONSE_TIMEOUT 600*1000
      #define PUTALL_TIMEOUT 60*1000
   }
  }
}

// ----------------------------------------------------------------------------

TESTTASK verifyKeyCount( const char * taskId ) {
  return g_test->verifyKeyCount();
}

// ----------------------------------------------------------------------------

TESTTASK verifyCount( const char * taskId ) {
  return g_test->verifyCount();
}

// ----------------------------------------------------------------------------

TESTTASK doDestroyRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doDestroyRegion called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->destroyRegion();
  } catch ( const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doDestroyRegion caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doInitInstance( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doInitInstance called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->initInstance();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doInitInstance caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doRegisterAllKeys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterAllKeys called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->registerAllKeys();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterAllKeys caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doRegisterSingleKey( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterSingleKey called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->registerInterestSingle();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterSingleKey caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doRegisterInterestList( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterInterest called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->registerInterestList();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterInterest caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doFeed( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doFeed called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->doFeedPuts();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doFeed caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doFeedInt( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doFeedInt called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->doFeedIntPuts();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doFeedInt caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doVerifyRegionContentsBeforeOps( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyRegionContentsBeforeOps called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->verifyRegionContentsBeforeOps();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyRegionContentsBeforeOps caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doVerifyRegionContentsAfterLateOps( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyRegionContentsAfterLateOps called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->verifyRegionContentsAfterLateOps();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyRegionContentsAfterLateOps caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doVerifyRegionContentsDynamic( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyRegionContentsDynamic called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->verifyRegionContentsDynamic();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyRegionContentsDynamic caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doVerifyRegionContentsAfterOpsRI( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyRegionContentsAfterOps called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->verifyRegionContentsAfterOpsRI();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyRegionContentsAfterOps caught exception: " << ex.getMessage() );
  }
  return result;
}
// ----------------------------------------------------------------------------

TESTTASK doTwinkleRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doTwinkleRegion called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->doTwinkleRegion();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doTwinkleRegion caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doEntryOperations( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doEntryOperations called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    result = g_test->doEntryOperations();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doEntryOperations caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doOps( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doOps called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    result = g_test->doOps();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doOps caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doRROps( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRROps called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    result = g_test->doRROps();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRROps caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doEntryOperationsForSecurity( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doEntryOperationsForSecurity called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    result = g_test->doEntryOperationsForSecurity();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doEntryOperationsForSecurity caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doValidateEntryOperationsForSecurity( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doValidateEntryOperationsForSecurity called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    result = g_test->validateEntryOperationsForSecurity();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doValidateEntryOperationsForSecurity caught exception: " << ex.getMessage() );
  }
  return result;
}
// ----------------------------------------------------------------------------

TESTTASK doResetImageBB( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doInitProcess called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->ResetImageBB();
  } catch ( const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doResetImageBB caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doReInitProcess( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doInitProcess called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->reInitCache();
  } catch ( const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doInitProcess caught exception: " << ex.getMessage() );
  }
  return result;
}
// ----------------------------------------------------------------------------

TESTTASK doInitProcess( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doInitProcess called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
  } catch ( const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doInitProcess caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doCreateRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateRegion called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->createRegion( g_test->cacheType() );
  } catch ( const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateRegion caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK createProcess( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "createProcess called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    int32_t sleepTime = g_test->getTimeValue( "sleepTime" );
    if(sleepTime > 0 ) {
      perf::sleepSeconds( sleepTime );
    }
    g_test->createRegion(g_test->cacheType());
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "createProcess caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------

TEST_EXPORT CacheListener * createLatencyListener() {
  return new perf::LatencyListener( g_test );
}

TEST_EXPORT CacheListener * createCountingCacheListener() {
  return new CountingCacheListener( g_test, g_test->cacheType().c_str() );
}
TEST_EXPORT CacheListener * createSilenceListener() {
  return new SilenceListener(g_test);
}
//------------------------------------------------------------------------

TESTTASK createCountingProcess( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "createCountingProcess called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->createRegion(g_test->cacheType(), NULLPTR, "cacheserver",
        "createCountingCacheListener");
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "createCountingProcess caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------

TEST_EXPORT CacheListener * createValidationCacheListener() {
  return new ValidationCacheListener( g_test, g_test->cacheType().c_str() );
}

TESTTASK createValidationProcess( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "createValidationProcess called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->createRegion(g_test->cacheType(), NULLPTR, "cacheserver",
        "createValidationCacheListener");
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "createValidationProcess caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doDepartSystem( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doDepartSystem called for task: " << taskId );
	int32_t doExit = -1;
  try {
    g_test->checkTest( taskId );
		doExit = g_test->getIntValue( "exit" );
    result = g_test->doDepartSystem();
  } catch (const FwkException &ex ) {
		if (doExit < 0) {
			// Do not expect exception
			result = FWK_SEVERE;
			FWKSEVERE( "doDepartSystem caught exception: " << ex.getMessage() );
		}
  }
  return result;
}

TESTTASK doVerifyClientCount( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyClientCount called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    result = g_test->doVerifyClientCount();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyClientCount caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doWaitForSilenceListenerComplete( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doWaitForSilenceListenerComplete called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->waitForSilenceListenerComplete(30,2000);
  } catch ( FwkException ex ) {
	    result = FWK_SEVERE;
	    FWKSEVERE( "doWaitForSilenceListenerComplete caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doVerifyQueryResult( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyQueryResult called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->verifyQueryResult();
  } catch ( FwkException ex ) {
            result = FWK_SEVERE;
            FWKSEVERE( "doVerifyQueryResult caught exception: " << ex.getMessage() );
  }

  return result;
}
// ----------------------------------------------------------------------------

void CacheServerTest::destroyRegion()
{
  bool local = getBoolValue( "localDestroy" );
  const std::string name = getStringValue( "regionName" );
  if ( name.empty() ) {
    FWKEXCEPTION( "Region name not specified in test." );
  }

  try {
    RegionPtr reg = m_cache->getRegion( name.c_str() );
    if (reg != NULLPTR) {
      if ( local ) {
        reg->localDestroyRegion();
      }
      else {
        reg->destroyRegion();
      }
    }
  } catch( Exception& e ) {
    FWKEXCEPTION( "Attempt to destroy region " << name << " encountered exception: " << e.getMessage() );
  }
}

// ----------------------------------------------------------------------------
void CacheServerTest::createRegion(const std::string & cacheType,  CacheListenerPtr cl, const char* cllib, const char* clfunc )
{
  const std::string name = getStringValue( "regionName" );
  if ( name.empty() ) {
    FWKEXCEPTION( "Region name not specified in test." );
  }

  try {
    createPool();
    RegionPtr reg = m_cache->getRegion( name.c_str() );
    if (reg != NULLPTR) {
      FWKINFO( "Destroying region " << name << " before creating it." );
      reg->destroyRegion();
    }
  } catch( Exception& ) {}

  std::string func;
  std::string lib;

  if ((cl == NULLPTR) && (cllib == NULL) && (clfunc == NULL)) {
    func = getStringValue( "listener" );
    lib = getStringValue( "listenerLib" );
    if ( lib.empty() ) {
      lib = "cacheserver";
    }
  }


  AttributesFactory afact;
  const std::string scope = getStringValue( "scope" );
  if ( ( scope.empty() ) || ( scope != "ACK" ) ) {
    afact.setScope( ScopeType::DISTRIBUTED_NO_ACK );
  }else {
    afact.setScope( ScopeType::DISTRIBUTED_ACK );
  }

  bool isCacheEnabled = true;

  if (cacheType == "SERVER") {
    // Region for a server
    bool enableOverflow = getBoolValue( "enableOverflow" );
    if ( enableOverflow ) {
      FWKINFO(" setting overflow attributes for subregion");
      char persistenceDir[512];
      char hname[100];
      ACE_OS::hostname( hname, 100 );
      sprintf( persistenceDir, "BDB-%s/", name.c_str() );
      FWKINFO(" setting overflow attributes for persistence directory for region: "<< persistenceDir);
      char persistenceEnvDir[512];
      sprintf( persistenceEnvDir, "BDBEnv-%s/", name.c_str() );
      FWKINFO(" setting overflow attributes for persistence environment directory for region: "<< persistenceEnvDir);
      afact.setDiskPolicy(DiskPolicyType::OVERFLOWS);
      PropertiesPtr bdbProperties = Properties::create();
      bdbProperties->insert("CacheSizeGb","0");
      bdbProperties->insert("CacheSizeMb","512");
      bdbProperties->insert("PageSize","65536");
      bdbProperties->insert("MaxFileSize","512000000");
      bdbProperties->insert("PersistenceDirectory",persistenceDir);
      bdbProperties->insert("EnvironmentDirectory",persistenceEnvDir);
      afact.setPersistenceManager("BDBImpl","createBDBInstance",bdbProperties);
      afact.setLruEntriesLimit(200);
    }
    afact.setConcurrencyLevel( 16 );
  } else if(cacheType == "CLIENT") {
    // Region for a client
    bool enableCache = getBoolValue( "enableCache" );
    if ( !enableCache ) {
      isCacheEnabled = false;
    }
    afact.setConcurrencyLevel( 16 );
  } else {
    // Region for a peer (feeder)
    isCacheEnabled = false;
  }
  afact.setCachingEnabled( isCacheEnabled );

  if (cl != NULLPTR) {
    afact.setCacheListener( cl );
  }
  else if ( ( cllib != NULL ) && ( clfunc != NULL ) ) {
    afact.setCacheListener( cllib, clfunc );
  }
  else if ( !lib.empty() && !func.empty() ) {
    afact.setCacheListener( lib.c_str(), func.c_str() );
  }

  RegionAttributesPtr atts = afact.createRegionAttributes();
  try {
    RegionPtr regionPtr = m_cache->createRegion( name.c_str(), atts);
    if (regionPtr == NULLPTR) {
      FWKEXCEPTION( "CacheServerTest::createRegion( ): failed to create region." );
    }
  } catch ( const RegionExistsException& ree ) { ree.getMessage();
  } catch ( const Exception& e ) {
    FWKEXCEPTION( "CacheServerTest::createRegion( ): caught Exception: " << e.getMessage() );
  }
}

// ----------------------------------------------------------------------------

void CacheServerTest::reInitCache()
{
  FWKINFO( "reInitCache called." );

  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;

  ACE_Time_Value end = ACE_OS::gettimeofday();
  ACE_Time_Value now;
  end += secondsToRun;

  int32_t id = getClientId();
  perf::sleepSeconds( id * 60 );

  while ( now < end ) {
    cacheFinalize();
    checkTest( getTaskId().c_str() );
    createRegion( m_cacheType );

    int32_t sleepSec = GsRandom::random((int32_t) now.sec() ) % 120;

    perf::sleepSeconds( sleepSec > 0 ? sleepSec : 120 );
    now = ACE_OS::gettimeofday();
  }

  FWKINFO( "reInitCache complete." );
}

int32_t CacheServerTest::doDepartSystem()
{
	int32_t result = FWK_SUCCESS;
  int32_t doExit = getIntValue( "exit" );
	if (doExit > 0) {
		// exit without clean the system
		ACE_OS::exit();
	} else {
		// nice exit
		cacheFinalize();
	}
	return result;
}

int32_t CacheServerTest::doVerifyClientCount()
{
  int32_t result = FWK_SUCCESS;
  if (m_cacheType == "SERVER") {
    int32_t secs = getTimeValue( "sleepTime" );
    secs = ( secs < 1 ) ? 30 : secs;
    perf::sleepSeconds( secs );

    StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
    StatisticsType* statsType = factory->findType("ServerStatistics");
    Statistics* serverStats = factory->findFirstStatisticsByType( statsType );
    int32_t clientsStatId = statsType->nameToId( "clients" );
    int actualClientCount = serverStats->getInt( clientsStatId );
    int32_t expectedCount = getIntValue( "clientCount" );
    FWKINFO("expected " << expectedCount << " clients, actually " << actualClientCount << " clients");
    result = (actualClientCount <= expectedCount) ? result : FWK_SEVERE;
  }
  return result;
}

// ----------------------------------------------------------------------------

RegionPtr CacheServerTest::getRegion( const char * rname ) {
  RegionPtr regionPtr;
  if (m_cache == NULLPTR) {
    return regionPtr;
  }
  std::string name;
  if ( rname != NULL ) {
    name = rname;
  }
  else {
    name = getStringValue( "regionName" );
    if ( name.empty() ) {
      VectorOfRegion regVec;
      m_cache->rootRegions( regVec );
      int32_t siz = regVec.size();
      if ( siz > 0 ) {
        int idx = GsRandom::random( regVec.size() );
        regionPtr = regVec.at( idx );
      }
      return regionPtr;
    }
  }
  regionPtr = m_cache->getRegion( name.c_str() );
  return regionPtr;
}

// ----------------------------------------------------------------------------

void CacheServerTest::doTwinkleRegion() {
  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 10 : secondsToRun;

  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  bool done = false;
  bool regionDestroyed = false;

  while ( !done ) {
    int32_t sleepTime = getTimeValue( "sleepTime" );
    sleepTime = ( ( sleepTime < 1 ) || regionDestroyed ) ? 10 : sleepTime;

    ACE_Time_Value now = ACE_OS::gettimeofday();
    ACE_Time_Value pause( sleepTime );
    if ( ( now > end ) || ( ( now + pause ) > end ) ) {
      done = true;
      continue;
    }

    FWKINFO( "CacheServerTest::doTwinkleRegion() sleeping for " << sleepTime << " seconds." );
    perf::sleepSeconds( sleepTime );

    if ( regionDestroyed ) {
      createRegion("SERVER");
      regionDestroyed = false;
      FWKINFO( "CacheServerTest::doTwinkleRegion() region created." );
    } else {
      RegionPtr regionPtr(getRegion());
      if (regionPtr != NULLPTR) {
        regionPtr->localDestroyRegion();
        regionPtr = NULLPTR;
      }
      regionDestroyed = true;
      FWKINFO( "CacheServerTest::doTwinkleRegion() local region destroy is complete." );
    }
  } // while

  if ( regionDestroyed ) {
    createRegion("SERVER");
    FWKINFO( "CacheServerTest::doTwinkleRegion() region created." );
  }

  FWKINFO( "CacheServerTest::doTwinkleRegion() completed." );
}

// ----------------------------------------------------------------------------

int32_t CacheServerTest::doFeedPuts() {
  FWKINFO( "doFeedPuts called." );
  RegionPtr regionPtr = getRegion();
  int32_t fwkResult = FWK_SUCCESS;

  int32_t opsSec = getIntValue( "opsSecond" );
  opsSec = ( opsSec < 1 ) ? 0 : opsSec;

  int32_t entryCount = getIntValue( "entryCount" );
  entryCount = ( entryCount < 1 ) ? 10000 : entryCount;

  int32_t cnt = 0;
  int32_t valSize = getIntValue( "valueSizes" );
  valSize = ( ( valSize < 0) ? 32 : valSize );
  char * valBuf = new char[valSize + 1];
  memset( valBuf, 'A', valSize );
  valBuf[valSize] = 0;

  PaceMeter meter( opsSec );
  try {
  while ( cnt < entryCount ) {
   add(cnt, valBuf);
  // regionPtr->create( cnt, cnt );
    cnt++;
    meter.checkPace();
  }
  } catch ( Exception &e ) {
      fwkResult = FWK_SEVERE;
      FWKEXCEPTION( "Caught unexpected exception during doFeedPuts "<< e.getMessage() << " exiting task." );
  }
  delete [] valBuf;
  return fwkResult;
}


// ----------------------------------------------------------------------------

CacheableStringPtr CacheServerTest::getKey( int32_t max )
{
  CacheableStringPtr keyPtr;
  char buf[32];
  resetValue("objectType");
  const std::string objectType = getStringValue( "objectType" );
  QueryHelper *qh = &QueryHelper::getHelper();
  int32_t numSet=0;
  int32_t setSize=0;

  if( objectType == "Portfolio" || objectType == "PortfolioPdx") {
    setSize = qh->getPortfolioSetSize();
    numSet = max/setSize;
    sprintf( buf, "port%d-%d",( uint32_t )GsRandom::random(numSet),( uint32_t )GsRandom::random(setSize));
    //FWKINFO("  CacheServerTest::getKey Portfolio numOfKeys " << max << " setSize = " << setSize << " numSets = " << numSet);
  } else if(objectType == "Position" || objectType == "PositionPdx") {
    setSize = qh->getPositionSetSize();
    numSet = max/setSize;
    sprintf( buf, "pos%d-%d",( uint32_t )GsRandom::random(numSet),( uint32_t )GsRandom::random(setSize));
    //FWKINFO("  CacheServerTest::getKey Position numOfKeys " << max <<  " setSize = " << setSize << " numSets = " << numSet);
  } else {
    sprintf( buf, "%u", ( uint32_t )GsRandom::random( max ) );
  }
  keyPtr = CacheableString::create( buf );
  return keyPtr;
}
//----------------------------------------------------------------------------

CacheablePtr CacheServerTest::getUserObject(const std::string & objType)
{
  CacheablePtr usrObj = NULLPTR;
  resetValue("entryCount");
  int numOfKeys = getIntValue( "entryCount" ); // number of key should be multiple of 20
  resetValue("valueSizes");
  int objSize = getIntValue( "valueSizes" );
  QueryHelper *qh = &QueryHelper::getHelper();
  int numSet=0;
  int setSize=0;
  if( objType == "Portfolio") {
    setSize = qh->getPortfolioSetSize();
    numSet = numOfKeys/setSize;
     usrObj = new Portfolio( GsRandom::random(setSize), objSize);
   } else if(objType == "Position") {
     setSize = qh->getPositionSetSize();
     numSet = numOfKeys/setSize;
      int numSecIds = sizeof(secIds)/sizeof(char*);
      usrObj = new Position(secIds[setSize % numSecIds], setSize*100);
    }
   else if( objType == "PortfolioPdx") {
    setSize = qh->getPortfolioSetSize();
    numSet = numOfKeys/setSize;
     usrObj = new testobject::PortfolioPdx( GsRandom::random(setSize), objSize);
   } else if(objType == "PositionPdx") {
     setSize = qh->getPositionSetSize();
     numSet = numOfKeys/setSize;
      int numSecIds = sizeof(secIds)/sizeof(char*);
      usrObj = new testobject::PositionPdx(secIds[setSize % numSecIds], setSize*100);
    }
   else if( objType == "AutoPortfolioPdx") {
     setSize = qh->getPortfolioSetSize();
     numSet = numOfKeys/setSize;
     usrObj = new AutoPdxTests::PortfolioPdx( GsRandom::random(setSize), objSize);
   }else if(objType == "AutoPositionPdx") {
      setSize = qh->getPositionSetSize();
      numSet = numOfKeys/setSize;
      int numSecIds = sizeof(secIds)/sizeof(char*);
      usrObj = new AutoPdxTests::PositionPdx(secIds[setSize % numSecIds], setSize*100);
   }
    return usrObj;
}

// ----------------------------------------------------------------------------

int32_t CacheServerTest::doEntryOperations()
{
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO( "doEntryOperations called." );

  int32_t opsSec = getIntValue( "opsSecond" );
  opsSec = ( opsSec < 1 ) ? 0 : opsSec;

  int32_t entryCount = getIntValue( "entryCount" );
  entryCount = ( entryCount < 1 ) ? 10000 : entryCount;

  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;

  int32_t valSize = getIntValue( "valueSizes" );
  valSize = ( ( valSize < 0) ? 32 : valSize );
  
  bool isCq=getBoolValue("cq");

  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  ACE_Time_Value now;

  CacheableStringPtr keyPtr;
  CacheablePtr valuePtr;
  CacheablePtr tmpValue;
  char * valBuf = new char[valSize + 1];
  memset( valBuf, 'A', valSize );
  valBuf[valSize] = 0;

  std::string opcode;
  std::string objectType;

  int32_t creates = 0, puts = 0, gets = 0, dests = 0, invals = 0, query=0, putAll=0, getAll=0,removeAll=0;
  RegionPtr regionPtr = getRegion();
  if (regionPtr == NULLPTR) {
    fwkResult = FWK_SEVERE;
    FWKSEVERE( "CacheServerTest::doEntryOperations(): No region to perform operations on." );
    now = end; // Do not do the loop
  }

  FWKINFO( "doEntryOperations will work for " << secondsToRun << " using " << valSize << " byte values." );

  PaceMeter meter( opsSec );
  objectType = getStringValue( "objectType" );
  bool multiRegion = getBoolValue( "multiRegion" );
  //int32_t cnt=0;
  while ( now < end ) {
    try {
      opcode = getStringValue( "entryOps" );
      if ( opcode.empty() ) opcode = "no-op";
      FWKINFO("Opcode is " << opcode);
      if(multiRegion) {
        regionPtr = getRegion();
        if( regionPtr == NULLPTR ) {
          FWKEXCEPTION("CacheServerTest::doEntryOperations(): No region to perform operation " << opcode);
        }
      }

      if ( opcode == "add" ) {
        if(isCq)
        {
          keyPtr = getKey( entryCount );
          if(regionPtr->containsKey(keyPtr))
          {
            if(!objectType.empty()) {
              tmpValue = getUserObject(objectType);
            } else {
              int32_t keyVal = atoi(keyPtr->toString());
              tmpValue = CacheableBytes::create( ( const unsigned char * )valBuf, static_cast<int32_t> (strlen( valBuf )) );
              int32_t * val = ( int32_t * )(dynCast<CacheableBytesPtr>(tmpValue)->value());
              *val = (*val == keyVal) ? keyVal + 1 : keyVal;  // alternate the value so that it can be validated later.
              int64_t * adjNow = ( int64_t * )(dynCast<CacheableBytesPtr>(tmpValue)->value() + 4 );
              *adjNow = getAdjustedNowMicros();
            }
            regionPtr->put( keyPtr, tmpValue );
            FWKINFO("Updated Key "<<keyPtr->toString());
            puts++;
          }
          else{
            if(!objectType.empty()) {
              tmpValue = getUserObject(objectType);
            } else
               tmpValue = CacheableBytes::create( ( const unsigned char * )valBuf, static_cast<int32_t> (strlen( valBuf )) );
            try{
              regionPtr->create( keyPtr, tmpValue );
              FWKINFO("Created Key "<<keyPtr->toString());
              creates++;
            }
            catch( EntryExistsException e)
            {
              FWKINFO("GOT this "<< e.getMessage());
            }
         }
       }
       else
       {
         keyPtr = getKey( entryCount );
        if(!objectType.empty()) {
          tmpValue = getUserObject(objectType);
        } else
          tmpValue = CacheableBytes::create( ( const unsigned char * )valBuf, static_cast<int32_t> (strlen( valBuf )) );
          if(tmpValue != NULLPTR)
            {FWKINFO("Inside create,value is not null");}
          else
           FWKINFO("Inside create,value is NULL");
          regionPtr->create( keyPtr, tmpValue );
        creates++;
       }
      }
      else {
        keyPtr = getKey( entryCount );
        if ( opcode == "update" ) {
            if(!objectType.empty()) {
            tmpValue = getUserObject(objectType);
          } else {
            int32_t keyVal = atoi(keyPtr->toString());
            tmpValue = CacheableBytes::create( ( const unsigned char * )valBuf, static_cast<int32_t> (strlen( valBuf )) );
            int32_t * val = ( int32_t * )(dynCast<CacheableBytesPtr>(tmpValue)->value());
            *val = (*val == keyVal) ? keyVal + 1 : keyVal;  // alternate the value so that it can be validated later.
            int64_t * adjNow = ( int64_t * )(dynCast<CacheableBytesPtr>(tmpValue)->value() + 4 );
            *adjNow = getAdjustedNowMicros();
          }
          if(tmpValue != NULLPTR)
            {
              FWKINFO("Inside update,value is not null");
            }
          regionPtr->put( keyPtr, tmpValue );
          //regionPtr->put(cnt,cnt);
          puts++;
        }
        else if ( opcode == "invalidate" ) {
         if(isCq){
           if(regionPtr->containsKey(keyPtr))
           {
             if(regionPtr->containsValueForKey(keyPtr))
             {
               try
               {
                 regionPtr->invalidate( keyPtr );
                 invals++;
               }
               catch(Exception e){FWKINFO("CAUGHT EXCEPTION during Invalidate "<<e.getMessage());}
             }
           }
          }
          else
          {
             regionPtr->invalidate( keyPtr );
             invals++;
          }
        }
        else if ( opcode == "destroy" ) {
           try{
               regionPtr->destroy( keyPtr );
               dests++;
           }
             catch(Exception e){FWKINFO("CAUGHT EXCEPTION during destroy "<<e.getMessage());}
        
        }
        else if ( opcode == "read" ) {
          valuePtr = regionPtr->get( keyPtr );
          gets++;
        }
        else if ( opcode == "read+localdestroy" ) {
          valuePtr = regionPtr->get( keyPtr );
          gets++;
          regionPtr->localDestroy( keyPtr );
          dests++;
        }
        else if ( opcode == "query" ) {
          runQuery(query);
        }
        else if ( opcode == "putAll" ) {
          putAllOps();
          putAll++;
        }
        else if ( opcode == "getAll" ) {
          getAllOps();
          getAll++;
        }
        else if ( opcode == "removeAll" ) {
          removeAllOps();
          removeAll++;
        }
        else if ( opcode == "removeAllCB" ) {
          removeAllOps(true);
          removeAll++;
        }
        else if (opcode == "putAllCB"){
          putAllOps(true);
        }
        else if ( opcode == "getAllCB" ) {
          getAllOps(true);
        }
        else {
          FWKEXCEPTION( "Invalid operation specified: " << opcode );
        }
      }
      //cnt++;
    } catch ( TimeoutException &e ) {
      fwkResult = FWK_SEVERE;
      FWKSEVERE( "Caught unexpected timeout exception during entry " << opcode
        << " operation: " << e.getMessage() << " continuing with test." );
    } catch ( EntryExistsException &ignore ) { ignore.getMessage();
    } catch ( EntryNotFoundException &ignore ) { ignore.getMessage();
    } catch ( EntryDestroyedException &ignore ) { ignore.getMessage();
    } catch ( Exception &e ) {
      end = 0;
      fwkResult = FWK_SEVERE;
      FWKEXCEPTION( "Caught unexpected exception during entry " << opcode
        << " operation: " << e.getMessage() << " exiting task." );
    }
    meter.checkPace();
    now = ACE_OS::gettimeofday();
  }
  keyPtr = NULLPTR;
  valuePtr = NULLPTR;
  delete [] valBuf;

  FWKINFO( "doEntryOperations did " << creates << " creates, " << puts << " puts, " << gets << " gets, " << invals << " invalidates, " << dests << " destroys, " << query << " query, " << putAll << " putAll, " << getAll << " getAll ." << removeAll << "removeAll");
  bbSet("OpsBB","CREATE",creates);
  bbSet("OpsBB","UPDATE",puts);
  bbSet("OpsBB","GETS",gets);
  bbSet("OpsBB","INVALIDATE",invals);
  bbSet("OpsBB","DESTROY",dests);
  return fwkResult;
}

//-----------------------------------------doOps ----------------------------------
int32_t CacheServerTest::registerAllKeys()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In CacheServerTest::registerAllKeys()" );

  try {
    RegionPtr region = getRegion();
    resetValue( "getInitialValues" );
    isGetInitialValues = getBoolValue( "getInitialValues" );
    bool isReceiveValues = true;
    VectorOfCacheableKeyPtr resultKeys(new VectorOfCacheableKey());
    bool checkReceiveVal = getBoolValue("checkReceiveVal");
    if (checkReceiveVal) {
      resetValue("receiveValue");
      isReceiveValues = getBoolValue("receiveValue");
    }
    resetValue("sleepBeforeRegisterInterest");
    int32_t sleepTime = getIntValue( "sleepBeforeRegisterInterest" );
    sleepTime = ( sleepTime < 0 ) ? 0 : sleepTime;
    FWKINFO("Sleeping for " << sleepTime << " millis");
    perf::sleepMillis(sleepTime);
    FWKINFO("CacheServerTest::registerAllKeys region name is " << region->getName()
         << "; getInitialValues is " << isGetInitialValues);
    region->registerAllKeys(false, resultKeys, isGetInitialValues,isReceiveValues);
    const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
    if(strlen(durableClientId) > 0) {
      m_cache->readyForEvents();
    }
    result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "CacheServerTest::registerAllKeys() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "CacheServerTest::registerAllKeys() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "CacheServerTest::registerAllKeys() Caught unknown exception." );
  }
  FWKINFO( "CacheServerTest::registerAllKeys() complete." );
  return result;

}

int32_t CacheServerTest::registerInterestSingle()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In CacheServerTest::registerInterestSingle()" );

  try {

  ExpectedRegionContents *expected = NULL;
  std::string expectedcontentsRI_noops = getStringValue("expectedRegionContents");
  if(expectedcontentsRI_noops == "static_RI_noops_keysValues")
	  expected = static_RI_noops_keysValues;
  else
	  expected = static_ops_RI_keysValues;
  resetValue( "getInitialValues" );
  isGetInitialValues = getBoolValue( "getInitialValues" );
  /*ExpectedRegionContents *expected;
  if (isGetInitialValues)
       expected = static_ops_RI_keysValues;
     else
       expected = static_RI_noops_none;
*/
  bool isReceiveValues = true;
  bool checkReceiveVal = getBoolValue("checkReceiveVal");
  if (checkReceiveVal) {
    resetValue("receiveValue");
    isReceiveValues = getBoolValue("receiveValue");
  }
  resetValue( "isDurableReg" );
  bool isDurableReg = getBoolValue( "isDurableReg" );
  int32_t entryCount = getIntValue( "entryCount" );
  int32_t numNewKeys = getIntValue( "NumNewKeys" );
  RegionPtr regionPtr = getRegion();
  FWKINFO("CacheServerTest::registerInterestSingle region name is " << regionPtr->getName()
      << "; getInitialValues is " << isGetInitialValues);
  VectorOfCacheableKey keys;
  resetValue("sleepBeforeRegisterInterest");
  int32_t sleepTime = getIntValue( "sleepBeforeRegisterInterest" );
  sleepTime = ( sleepTime < 0 ) ? 0 : sleepTime;
  FWKINFO("Sleeping for " << sleepTime << " millis" <<" and keys are = "<<entryCount+numNewKeys + NUM_EXTRA_KEYS );
  perf::sleepMillis(sleepTime);
  for(int32_t i = 0;i<entryCount+numNewKeys + NUM_EXTRA_KEYS;i++){
	  keys.push_back(m_KeysA[i]);
	  regionPtr->registerKeys(keys, isDurableReg, isGetInitialValues,isReceiveValues);
	  if (expected != NULL)
	    verifyEntry(m_KeysA[i], expected);
  }

  const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
  if(strlen(durableClientId) > 0) {
      m_cache->readyForEvents();
    }
  //delete expected;
  //delete static_RI_noops_keysValues;
  //delete static_RI_noops_none;
  result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "CacheServerTest::registerInterestSingle() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "CacheServerTest::registerInterestSingle() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "CacheServerTest::registerInterestSingle() Caught unknown exception." );
  }
  FWKINFO( "CacheServerTest::registerInterestSingle() complete." );
  return result;

}


int32_t CacheServerTest::registerInterestList()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In CacheServerTest::registerInterestList()" );

  try {

  resetValue( "getInitialValues" );
  isGetInitialValues = getBoolValue( "getInitialValues" );
  bool isReceiveValues = true;
  bool checkReceiveVal = getBoolValue("checkReceiveVal");
  if (checkReceiveVal) {
    resetValue("receiveValue");
    isReceiveValues = getBoolValue("receiveValue");
  }
  resetValue( "isDurableReg" );
  bool isDurableReg = getBoolValue( "isDurableReg" );
  int32_t entryCount = getIntValue( "entryCount" );
  int32_t numNewKeys = getIntValue( "NumNewKeys" );
  RegionPtr regionPtr = getRegion();
  FWKINFO("CacheServerTest::registerInterestList region name is " << regionPtr->getName()
      << "; getInitialValues is " << isGetInitialValues);
  VectorOfCacheableKey keys;
  for(int32_t i = 0;i<entryCount+numNewKeys + NUM_EXTRA_KEYS;i++){
	  keys.push_back(m_KeysA[i]);
  }
  resetValue("sleepBeforeRegisterInterest");
  int32_t sleepTime = getIntValue( "sleepBeforeRegisterInterest" );
  sleepTime = ( sleepTime < 0 ) ? 0 : sleepTime;
  FWKINFO("Sleeping for " << sleepTime << " millis");
  perf::sleepMillis(sleepTime);
  regionPtr->registerKeys(keys, isDurableReg, isGetInitialValues,isReceiveValues);
  const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
  if(strlen(durableClientId) > 0) {
      m_cache->readyForEvents();
    }
  result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "CacheServerTest::registerInterestList() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "CacheServerTest::registerInterestList() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "CacheServerTest::registerInterestList() Caught unknown exception." );
  }
  FWKINFO( "CacheServerTest::registerInterestList() complete." );
  return result;

}

int32_t CacheServerTest::initInstance() {
	int32_t fwkResult = FWK_SEVERE;
	try {
		resetValue("entryCount" );
		resetValue("NumNewKeys" );
	int32_t entryCount = getIntValue( "entryCount" );
	entryCount = ( entryCount < 1 ) ? 10000 : entryCount;
	int32_t numNewKeys = getIntValue( "NumNewKeys" );
	m_KeysA = new CacheableKeyPtr[entryCount+numNewKeys + NUM_EXTRA_KEYS];
	SetFisrtAndLastKeyOnBB(entryCount);
	for(int32_t cnt = 0;cnt < entryCount;cnt++){
		m_KeysA[cnt] = CacheableInt32::create( cnt+1 );
	}
	 for (int32_t i = entryCount; i < entryCount+numNewKeys; i++)
		  m_KeysA[i] = CacheableInt32::create(i +1);

	  // add some extra keys to the list that are never created by the test
	 for (int i = entryCount+numNewKeys;i<entryCount+numNewKeys + NUM_EXTRA_KEYS; i++) // do a few more keys than we really need
	    	 m_KeysA[i] = CacheableInt32::create(i+1 );

	 static_RI_noops_keysValues = new ExpectedRegionContents(
	       true,  true,  // none
	       true,  true,  // invalidate
	       true,  true,  // localInvalidate
	       true,  true,  // destroy
	       true,  true,  // localDestroy
	       true,  true,  // update
	       true,  true,  // get
	       false, false, // newKey
	       false,        // get allowed during validate
	       false);       // update has occurred
	 static_RI_noops_keysValues->exactSize(entryCount);
	/* static_RI_noops_keys = new ExpectedRegionContents(
	       true,  false,  // none
	       true,  false,  // invalidate
	       true,  false,  // localInvalidate
	       true,  false,  // destroy
	       true,  false,  // localDestroy
	       true,  false,  // update
	       true,  false,  // get
	       false, false, // newKey
	       false,        // get allowed during validate
	       false);
	 static_RI_noops_none = new ExpectedRegionContents(
	       false, false,  // none
	       false, false,  // invalidate
	       false, false,  // localInvalidate
	       false, false,  // destroy
	       false, false,  // localDestroy
	       false, false,  // update
	       false, false,  // get
	       false, false, // newKey
	       false,        // get allowed during validate
	       false);
      */
	 static_RI_ops_keysValues = new ExpectedRegionContents(
	 	       true,  true,  // none
	 	       true,  false, // invalidate
	 	       true,  true,  // localInvalidate
	 	       false, false, // destroy
	 	       true,  true,  // localDestroy
	 	       true,  true,  // update
	 	       true,  true,  // get
	 	       true,  true,  // newKey
	 	       true,         // get allowed during validate
	 	       true);        // update has occurred
	 int32_t numDestroyed = (int32_t)bbGet("ImageBB", "Last_Destroy") - (int32_t)bbGet("ImageBB", "First_Destroy") + 1;
         int32_t numInvalided = (int32_t)bbGet("ImageBB", "Last_Invalidate") - (int32_t)bbGet("ImageBB", "First_Invalidate") + 1;
	 static_RI_ops_keysValues->exactSize(entryCount- numDestroyed + numNewKeys);
	/* static_RI_ops_keys = new ExpectedRegionContents(
	       true,  false, // none
	       true,  false, // invalidate
	       true,  false, // localInvalidate
	       false, false, // destroy
	       true,  false, // localDestroy
	       true,  true,  // update
	       true,  false, // get
	       true,  true,  // newKey
	       true,         // get allowed during validate
	       true);        // update has occurred
	 static_RI_ops_none = new ExpectedRegionContents(
	       false, false, // none
	       false, false, // invalidate
	       false, false, // localInvalidate
	       false, false, // destroy
	       false, false, // localDestroy
	       true,  true,  // update
	       false, false, // get
	       true,  true,  // newKey
	       true,         // get allowed during validate
	       true);        // update has occurred
	       */
	 static_ops_RI_keysValues = new ExpectedRegionContents(
	       true,  true,  // none
	       true,  false, // invalidate
	       true,  true,  // localInvalidate
	       false, false, // destroy
	       true,  true,  // localDestroy
	       true,  true,  // update
	       true,  true,  // get
	       true,  true,  // newKey
	       true,         // get allowed during validate
	       true);        // update has occurred
	       static_ops_RI_keysValues->exactSize(entryCount- 2*numDestroyed + numNewKeys + numInvalided);

	  dynamicKeysValues = new ExpectedRegionContents(true, true, true);
	          dynamicKeysValues->containsValue_invalidate(false);
	          dynamicKeysValues->containsKey_destroy(false);
	          dynamicKeysValues->containsValue_destroy(false);
	          dynamicKeysValues->valueIsUpdated(true);
	          dynamicKeysValues->exactSize(entryCount- numDestroyed + numNewKeys);

	expectedRgnContents4 = new ExpectedRegionContents( // Thit is for the same same client where we done ADD operation.
		true, true,  // none
		true, true,  // invalidate
		true, true,  // localInvalidate
		true, true,  // destroy
		true, true,  // localDestroy
		true, true,  // update
		true, true,  // get
		false, false, // newKey
		true,        // get allowed during validate
		false);       // update has occurred
	 expectedRgnContents4->exactSize(entryCount);
	   fwkResult = FWK_SUCCESS;
	 } catch ( Exception &e ) {
	     FWKEXCEPTION( "Caught unexpected exception during CacheServerTest::initInstance "<< e.getMessage() << " exiting task." );
     }
	 return fwkResult;
}

int32_t CacheServerTest::doFeedIntPuts() {
  FWKINFO( "doFeedIntPuts called." );
  int32_t fwkResult = FWK_SUCCESS;
  try {
    RegionPtr regionPtr = getRegion();
    int32_t entryCount = getIntValue( "entryCount" );
    entryCount = ( entryCount < 1 ) ? 10000 : entryCount;
    //SetFisrtAndLastKeyOnBB(entryCount);
    for(int32_t i = 0;i < entryCount;i++){
      CacheablePtr value = GetValue(i+1);
      regionPtr->create( m_KeysA[i], value);
    }
    FWKINFO("REGION size after doFeedIntPuts is "<<regionPtr->size());
   } catch ( Exception &e ) {
     fwkResult = FWK_SEVERE;
     FWKEXCEPTION( "Caught unexpected exception during doFeedIntPuts "<< e.getMessage() << " exiting task." );
   }
   return fwkResult;
}

void CacheServerTest::SetFisrtAndLastKeyOnBB(int32_t entrycount  /* similar to numKeyIntervals on hydra*/)
{
        FWKINFO("In CacheServerTest::SetFisrtAndLastKeyOnBB");
	int32_t count; // 7 is num of ops ( create,update,get invalidate, local invalidate, destroy, local destroy )
        bool m_istransaction = getBoolValue( "useTransactions" );
        resetValue("cqtest");
        bool isCqTest = getBoolValue("cqtest");
        if(m_istransaction){
          FWKINFO("Inside only 5 operations");
          count  = entrycount/5; // 5 is num of ops ( create,update,get invalidate,destroy )
	  bbSet("ImageBB", "First_LocalInvalidate",-1);
	  bbSet("ImageBB", "Last_LocalInvalidate",-1);
	  bbSet("ImageBB", "First_LocalDestroy",-1);
	  bbSet("ImageBB", "Last_LocalDestroy",-1);
	  bbSet("ImageBB","LASTKEY_LOCAL_INVALIDATE",1);
	  bbSet("ImageBB","LASTKEY_LOCAL_DESTROY",1);
	}
        else {
          count  = entrycount/7; // 7 is num of ops ( create,update,get invalidate, local invalidate, destroy, local destroy )
          bbSet("ImageBB", "First_LocalInvalidate",(5*count) +1);
	  bbSet("ImageBB", "Last_LocalInvalidate",6*count);
	  bbSet("ImageBB", "First_LocalDestroy",(6*count) +1);
	  bbSet("ImageBB", "Last_LocalDestroy",entrycount);
	  bbSet("ImageBB","LASTKEY_LOCAL_INVALIDATE",(5*count) +1);
	  bbSet("ImageBB","LASTKEY_LOCAL_DESTROY",(6*count) +1);
	}
	bbSet("ImageBB", "First_None",1);
	bbSet("ImageBB", "Last_None",count);
	bbSet("ImageBB", "First_Invalidate",count + 1);
	bbSet("ImageBB", "Last_Invalidate",2*count);
        bbSet("ImageBB", "First_Destroy",(2*count) + 1);
	bbSet("ImageBB", "Last_Destroy",3*count);
	bbSet("ImageBB", "First_UpdateExistingKey",(3*count)+ 1);
	bbSet("ImageBB", "Last_UpdateExistingKey",4*count);
	bbSet("ImageBB", "First_Get",(4*count)+1);
	bbSet("ImageBB", "Last_Get",5*count);
	bbSet("ImageBB","NUM_NEW_KEYS_CREATED",1);
	bbSet("ImageBB","LASTKEY_UPDATE_EXISTING_KEY",(3*count) + 1);
	bbSet("ImageBB","LASTKEY_INVALIDATE",(count) + 1);
	bbSet("ImageBB","LASTKEY_DESTROY",(2*count) + 1);
	bbSet("ImageBB","LASTKEY_GET",(4*count) + 1);

	FWKINFO(printKeyIntervalsBBData());
}
std::string CacheServerTest::printKeyIntervalsBBData( ) {
	int32_t numNewKeys = getIntValue( "NumNewKeys" );
	int32_t numKeyIntervals = getIntValue( "entryCount" );
	int32_t numDestroyed = (int32_t)bbGet("ImageBB", "Last_Destroy") - (int32_t)bbGet("ImageBB", "First_Destroy")  + 1;
	totalNumKeys = numKeyIntervals + numNewKeys - numDestroyed;
    std::string sString;
    sString+="\nkeyIntervals read from blackboard = ";
    sString += "\n none: ";
    sString += "firstKey: " + FwkStrCvt( bbGet("ImageBB", "First_None")).toString()  +
    		", lastKey: " + FwkStrCvt( bbGet("ImageBB", "Last_None")).toString();
    sString += "\n invalidate: ";
    sString += "firstKey: " + FwkStrCvt( bbGet("ImageBB", "First_Invalidate")).toString()  +
      		", lastKey: " + FwkStrCvt( bbGet("ImageBB", "Last_Invalidate")).toString();
    sString += "\n localInvalidate: ";
    sString += "firstKey: " + FwkStrCvt( bbGet("ImageBB", "First_LocalInvalidate")).toString()  +
         		", lastKey: " + FwkStrCvt( bbGet("ImageBB", "Last_LocalInvalidate")).toString();
    sString += "\n destroy: ";
    sString += "firstKey: " + FwkStrCvt( bbGet("ImageBB", "First_Destroy")).toString()  +
   		", lastKey: " + FwkStrCvt( bbGet("ImageBB", "Last_Destroy")).toString();
    sString += "\n localDestroy,: ";
    sString += "firstKey: " + FwkStrCvt( bbGet("ImageBB", "First_LocalDestroy")).toString()  +
   		", lastKey: " + FwkStrCvt( bbGet("ImageBB", "Last_LocalDestroy")).toString();
    sString += "\n updateExistingKey: ";
    sString += "firstKey: " + FwkStrCvt( bbGet("ImageBB", "First_UpdateExistingKey")).toString()  +
      		", lastKey: " + FwkStrCvt( bbGet("ImageBB", "Last_UpdateExistingKey")).toString();
    sString += "\n get: ";
    sString += "firstKey: " + FwkStrCvt( bbGet("ImageBB", "First_Get")).toString()  +
         		", lastKey: " + FwkStrCvt( bbGet("ImageBB", "Last_Get")).toString();
    sString += "\n\n numKeyIntervals is " + FwkStrCvt(numKeyIntervals).toString();
    sString += "\n numNewKeys is " + FwkStrCvt(numNewKeys).toString();
    sString += "\n numDestroyed is " + FwkStrCvt(numDestroyed).toString();
    sString += "\n totalNumKeys is " + FwkStrCvt(totalNumKeys).toString();
    sString += "\n\n";

    return sString;
}
void CacheServerTest::checkContainsKey(CacheablePtr key, bool expected, std::string logStr){
   RegionPtr regionPtr = getRegion();
   bool containsKey = regionPtr->containsKey(key);
   std::string sString = !expected? "false":"true";
   std::string ckey = !containsKey? "false":"true";
   if (containsKey != expected) {
	   FWKEXCEPTION("Expected containsKey(" << key->toString()->asChar() << ") to be " << expected <<
			   ", but it was " << containsKey << ": " << logStr);
   }

}

CacheablePtr DoOpsTask::GetValue(int32_t value,char* update)
 {
	SerializablePtr tempVal;
	std::string m_objectType = m_test->getStringValue( "objectType" );
	int32_t m_version = m_test->getIntValue( "versionNum" );

   char buf[252];
   sprintf(buf,"%d",value);
   if(update != NULL)
   {
	   sprintf(buf,"%s%d",update,value);
   }
   else
	  sprintf(buf,"%d",value);
   if(m_objectType != ""){
	   if(m_objectType == "PdxVersioned" && m_version == 1)
	   {
		 PdxTests::PdxVersioned1Ptr pdxV1(new PdxTests::PdxVersioned1(buf));
		 tempVal = dynCast<PdxSerializablePtr>(pdxV1);
	   }
	   else if(m_objectType == "PdxVersioned" && m_version == 2)
	   {
		   PdxTests::PdxVersioned2Ptr pdxV2(new PdxTests::PdxVersioned2(buf));
		   tempVal = dynCast<PdxSerializablePtr>(pdxV2);
	   }
	   else if(m_objectType == "PdxType")
	   {
		 PdxTests::PdxTypePtr pdxTp(new PdxTests::PdxType());
		 tempVal = dynCast<PdxSerializablePtr>(pdxTp);
	   }
	   else if(m_objectType == "Nested")
	   {
		 PdxTests::NestedPdxPtr nestedPtr(new PdxTests::NestedPdx(buf));
		 tempVal = dynCast<PdxSerializablePtr>(nestedPtr);
	   }
	   else if(m_objectType == "AutoPdxVersioned" && m_version == 1)
	   {
		   AutoPdxTests::AutoPdxVersioned1Ptr pdxV1(new AutoPdxTests::AutoPdxVersioned1(buf));
		   tempVal = dynCast<PdxSerializablePtr>(pdxV1);
	   }
	   else if(m_objectType == "AutoPdxVersioned" && m_version == 2)
	   {
		   AutoPdxTests::AutoPdxVersioned2Ptr pdxV2(new AutoPdxTests::AutoPdxVersioned2(buf));
	     tempVal = dynCast<PdxSerializablePtr>(pdxV2);
	   }
	   else if(m_objectType == "AutoNested")
	   {
		 AutoPdxTests::NestedPdxPtr nestedPtr(new AutoPdxTests::NestedPdx(buf));
	   	 tempVal = dynCast<PdxSerializablePtr>(nestedPtr);
	   }

   }else if(update != NULL && m_objectType == ""){
	   tempVal = CacheableString::create( buf );
   }
   else
   {
	 tempVal = CacheableInt32::create( value );
   }

   return tempVal;
 }
void CacheServerTest::checkContainsValueForKey(CacheablePtr key, bool expected, std::string logStr) {
   RegionPtr regionPtr = getRegion();
   bool containsValue = regionPtr->containsValueForKey(key);
   std::string sString = !expected? "false":"true";
   std::string ckey = !containsValue? "false":"true";
   if (containsValue != expected)
      FWKEXCEPTION("Expected containsValueForKey(" << key->toString()->asChar() << ") to be " << expected <<
                ", but it was " << containsValue << ": " << logStr);
}
void DoOpsTask::checkContainsValueForKey(CacheablePtr key, bool expected, std::string logStr) {
   //RegionPtr regionPtr = getRegion();
   bool containsValue = regionPtr->containsValueForKey(key);
   std::string sString = !expected? "false":"true";
   std::string ckey = !containsValue? "false":"true";
   if (containsValue != expected)
      FWKEXCEPTION("DoOpsTask::checkContainsValueForKey: Expected containsValueForKey(" << key->toString()->asChar() << ") to be " << expected <<
                ", but it was " << containsValue << ": " << logStr);
}

bool DoOpsTask::addNewKey(RegionPtr regionPtr)
{
	int32_t numNewKeysCreated = (int32_t)m_test->bbGet("ImageBB","NUM_NEW_KEYS_CREATED");
	m_test->bbIncrement("ImageBB","NUM_NEW_KEYS_CREATED");
	int32_t numNewKeys = m_test->getIntValue( "NumNewKeys" );
       
	if (numNewKeysCreated > numNewKeys) {
	      FWKINFO("All new keys created; returning from addNewKey");
	      return true;
	}
	int32_t entryCount = m_test->getIntValue( "entryCount" );
	entryCount = ( entryCount < 1 ) ? 10000 : entryCount;
	CacheableInt32Ptr key = CacheableInt32::create( entryCount + numNewKeysCreated);
	checkContainsValueForKey(key, false, "before addNewKey");
	CacheablePtr value = GetValue(entryCount + numNewKeysCreated);
	regionPtr->put(key,value);
	return (numNewKeysCreated >= numNewKeys);
}
bool DoOpsTask::putAllNewKey(RegionPtr regionPtr) {
	int32_t numNewKeysCreated = (int32_t)m_test->bbGet("ImageBB","NUM_NEW_KEYS_CREATED");
	m_test->bbIncrement("ImageBB","NUM_NEW_KEYS_CREATED");
	int32_t numNewKeys = m_test->getIntValue( "NumNewKeys" );
		if (numNewKeysCreated > numNewKeys) {
		      FWKINFO("All new keys created; returning from putAllNewKey");
		      return true;
		}
		int32_t entryCount = m_test->getIntValue( "entryCount" );
		entryCount = ( entryCount < 1 ) ? 10000 : entryCount;
		CacheableInt32Ptr key = CacheableInt32::create( entryCount + numNewKeysCreated);
		checkContainsValueForKey(key, false, "before addNewKey");
		CacheablePtr value = GetValue(entryCount + numNewKeysCreated);
		 HashMapOfCacheable map0;
		  map0.clear();
		  map0.insert(key, value);
		  regionPtr->putAll(map0,PUTALL_TIMEOUT);
          return (numNewKeysCreated >= numNewKeys);
}
bool DoOpsTask::updateExistingKey(RegionPtr regionPtr) {
	int32_t nextKey = (int32_t)m_test->bbGet("ImageBB", "LASTKEY_UPDATE_EXISTING_KEY");
	m_test->bbIncrement("ImageBB","LASTKEY_UPDATE_EXISTING_KEY");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_UpdateExistingKey");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_UpdateExistingKey");
	if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		FWKINFO("All existing keys updated; returning from updateExistingKey");
	   return true;
	}
	int32_t entryCount = m_test->getIntValue( "entryCount" );
	entryCount = ( entryCount < 1 ) ? 10000 : entryCount;
	CacheableInt32Ptr key = CacheableInt32::create( nextKey);
	CacheablePtr existingValue = regionPtr->get(key);
	      if (existingValue == NULLPTR)
	    	  throw new Exception("Get of key %s returned unexpected null",key->toString()->asChar());
	      if (instanceOf<CacheableStringPtr>(existingValue))
	    	  throw new Exception("Trying to update a key which was already updated: %s", existingValue->toString()->asChar());
	CacheablePtr newValue = GetValue(nextKey,"updated_");
	//CacheablePtr newValue = CacheableString::create(createNewKey.c_str());
	  regionPtr->put(key,newValue);
	return (nextKey >= lastKey);
}

bool DoOpsTask::localInvalidate(RegionPtr regionPtr) {
	int32_t nextKey = (int32_t)m_test->bbGet("ImageBB", "LASTKEY_LOCAL_INVALIDATE");
	m_test->bbIncrement("ImageBB","LASTKEY_LOCAL_INVALIDATE");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_LocalInvalidate");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_LocalInvalidate");
	if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		FWKINFO("All local invalidates completed; returning from localInvalidate");
	   return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create( nextKey);
	try {
	regionPtr->localInvalidate(key);
	} catch (EntryNotFoundException e) {
	      throw EntryNotFoundException(e.getMessage());
	}

	return (nextKey >= lastKey);
}

bool DoOpsTask::invalidate(RegionPtr regionPtr) {
	int32_t nextKey = (int32_t)m_test->bbGet("ImageBB", "LASTKEY_INVALIDATE");
    m_test->bbIncrement("ImageBB","LASTKEY_INVALIDATE");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_Invalidate");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_Invalidate");
	if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		FWKINFO("All existing keys invalidated; returning from invalidate");
	   return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create( nextKey);
	try {
  	  regionPtr->invalidate(key);
        }
        catch (EntryNotFoundException e) { 
	  throw  EntryNotFoundException(e.getMessage());
        }
	return (nextKey >= lastKey);
}
bool DoOpsTask::localDestroy(RegionPtr regionPtr) {
	int32_t nextKey = (int32_t)m_test->bbGet("ImageBB", "LASTKEY_LOCAL_DESTROY");
    m_test->bbIncrement("ImageBB","LASTKEY_LOCAL_DESTROY");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_LocalDestroy");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_LocalDestroy");
	if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		FWKINFO("All local destroys completed; returning from localDestroy");
	   return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create( nextKey);
	try {
	  regionPtr->localDestroy(key);
    } catch (EntryNotFoundException e) {
	  throw EntryNotFoundException(e.getMessage());
    }
	return (nextKey >= lastKey);
}
bool DoOpsTask::destroy(RegionPtr regionPtr) {
	int32_t nextKey = (int32_t)m_test->bbGet("ImageBB", "LASTKEY_DESTROY");
	m_test->bbIncrement("ImageBB","LASTKEY_DESTROY");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_Destroy");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_Destroy");
	if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		FWKINFO("All destroys completed; returning from destroy");
	   return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create( nextKey);
	try{
	  regionPtr->destroy(key);
    }
    catch (CacheWriterException e) {
      throw CacheWriterException(e.getMessage());
    } catch (EntryNotFoundException e) {
      throw EntryNotFoundException(e.getMessage());
    }
	return (nextKey >= lastKey);
}
bool DoOpsTask::get(RegionPtr regionPtr) {
	int32_t nextKey = (int32_t)m_test->bbGet("ImageBB", "LASTKEY_GET");
	m_test->bbIncrement("ImageBB","LASTKEY_GET");
	int32_t firstKey = (int32_t)m_test->bbGet("ImageBB", "First_Get");
	int32_t lastKey = (int32_t)m_test->bbGet("ImageBB", "Last_Get");
	if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		FWKINFO("All gets completed; returning from get");
	   return true;
	}
	CacheableInt32Ptr key = CacheableInt32::create( nextKey);
	CacheablePtr existingValue = NULLPTR;
  	  existingValue = regionPtr->get(key);
	if (existingValue == NULLPTR)
		FWKSEVERE("Get of key " << key->toString()->asChar() << " returned unexpected " << existingValue->toString()->asChar());
	return (nextKey >= lastKey);
}


int32_t CacheServerTest::doOps()
{
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO( "doOps called." );

  int32_t opsSec = getIntValue( "opsSecond" );
  opsSec = ( opsSec < 1 ) ? 0 : opsSec;

  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;

  int32_t valSize = getIntValue( "valueSizes" );
  valSize = ( ( valSize < 0) ? 32 : valSize );
  resetValue("entryCount");
  int32_t entryCount = getIntValue( "entryCount" );
  resetValue("NumNewKeys");
  int32_t numNewKeys = getIntValue( "NumNewKeys" );

  TestClient * clnt = TestClient::getTestClient();
  RegionPtr regionPtr = getRegion();
  if (regionPtr == NULLPTR) {
    fwkResult = FWK_SEVERE;
    FWKSEVERE( "CacheServerTest::doOps(): No region to perform operations on." );
    }
  DoOpsTask dooperation( regionPtr, g_test );
  resetValue("numThreads");
  int numThreads = getIntValue("numThreads");
  try {
    if ( !clnt->runIterations( &dooperation, entryCount+numNewKeys + NUM_EXTRA_KEYS, numThreads, 0 ) ) {
	  FWKEXCEPTION( "In doOps()  doOps timed out." );
    }
    if(clnt->getTaskStatus() == FWK_SEVERE)
             FWKEXCEPTION( "Exception in doOps task");
  } catch ( Exception &e ) {
	  fwkResult=FWK_SEVERE;
      FWKEXCEPTION( "Caught unexpected exception during doOps: " << e.getMessage());
  }catch ( FwkException & ex ) {
	  fwkResult = FWK_SEVERE;
      FWKSEVERE( "FwkException caught in CacheServerTest::doOps: " << ex.getMessage() );
  } catch (...) {
	  fwkResult = FWK_SEVERE;
	  FWKSEVERE( "Unknown exception caught in CacheServerTest::doOps" );
  }

  FWKINFO("Done in doOps");
  perf::sleepSeconds( 10 );
return fwkResult;
}

int32_t CacheServerTest::doRROps()
{
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO( "doRROps called." );

  int32_t opsSec = getIntValue( "opsSecond" );
  opsSec = ( opsSec < 1 ) ? 0 : opsSec;

  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;

  int32_t valSize = getIntValue( "valueSizes" );
  valSize = ( ( valSize < 0) ? 32 : valSize );
  resetValue("entryCount");
  int32_t entryCount = getIntValue( "entryCount" );
  resetValue("NumNewKeys");
  int32_t numNewKeys = getIntValue( "NumNewKeys" );

  TestClient * clnt = TestClient::getTestClient();
  RegionPtr regionPtr = getRegion();
  if (regionPtr == NULLPTR) {
    fwkResult = FWK_SEVERE;
    FWKSEVERE( "CacheServerTest::doRROps(): No region to perform operations on." );
    //now = end; // Do not do the loop
  }
  int clientNum=g_test->getClientId();
  int numClients = getIntValue("clientCnt");
  int numServers = getIntValue("serverCount");
  std::string clntid = "";
  char name[32] = {'\0'};
  int32_t Cid=(g_test->getClientId() - numServers);
  ACE_OS::sprintf(name,"ClientName_%d",Cid);
  int roundPosition = 0;
  bool isdone = 0;
  resetValue("serialExecution");
  bool isSerialExecution = getBoolValue("serialExecution");
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
      if(clntid == name)
      {
     	try {    
   		  DoOpsTask dooperation( regionPtr, g_test );
 		  if (isSerialExecution)
 		  {
 			if ( !clnt->runIterations( &dooperation, entryCount+numNewKeys + NUM_EXTRA_KEYS, numThreads, 0 ) ) {
 			 	  FWKEXCEPTION( "In doRROps()  doRROps timed out." );
 			}
  		    waitForSilenceListenerComplete(30,2000);
  		    roundPosition = (int32_t)bbIncrement("RoundPositionBB", "roundPosition");
  		    bbSet("RoundPositionBB", "roundPosition", roundPosition);
  		    bbSet("RoundPositionBB", "done", true);
  		    bbSet("RoundPositionBB", "VerifyCnt", 1);
    	          }
  		  else
  		  {
  			if ( !clnt->runIterations( &dooperation, entryCount+numNewKeys + NUM_EXTRA_KEYS, numThreads, 0 ) ) {
  				  FWKEXCEPTION( "In doRROps()  doRROps timed out." );
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
           verifyQueryResult();
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
  FWKINFO("Done in doRROps");
  perf::sleepSeconds( 10 );
  return fwkResult;
}

CacheableHashSetPtr CacheServerTest::verifyRegionSize(ExpectedRegionContents *expected)
{
	RegionPtr regionPtr = getRegion();
	  int32_t numKeys = regionPtr->size();
	  int32_t entryCount = getIntValue( "entryCount" );
	  int32_t numNewKeys = getIntValue( "NumNewKeys" );
	  FWKINFO("Expecting exact size of region " << expected->exactSize());
	  if (expected->exactSize() != numKeys) {
		  FWKEXCEPTION("Expected " << expected->exactSize() << " keys, but there are " << numKeys);
	  }
	  CacheableHashSetPtr keysToCheck = CacheableHashSet::create();
	  for(int32_t i = 0;i<entryCount+numNewKeys + NUM_EXTRA_KEYS;i++)
		    keysToCheck->insert(m_KeysA[i]);// check all keys in the keyList (including "extra" keys not in server)

	  VectorOfCacheableKey keyVec;
	  regionPtr->keys(keyVec );
	  for(int32_t i = 0;i<keyVec.size();i++)
		  keysToCheck->insert(keyVec[i]);// also check any keys in the region that are not in the keyList
	  /*if (isGetInitialValues)
	       expected = static_RI_ops_keysValues;
	     else
	       expected = static_RI_ops_none;
      */
	  return keysToCheck;

}
int32_t CacheServerTest::verifyRegionContentsAfterOpsRI()
{
  int32_t fwkResult = FWK_SUCCESS;
  FWKINFO( "verifyRegionContentsOpsRI called." );

  CacheableHashSetPtr keysToCheck = verifyRegionSize(static_ops_RI_keysValues);
 // CacheableHashSetPtr::Iterator it = keysToCheck->Iterator;
  for (CacheableHashSet::Iterator it = keysToCheck->begin(); it != keysToCheck->end(); it++){
	  CacheableKeyPtr key = *it;
	  try {
	        verifyEntry(key,static_ops_RI_keysValues);
	   } catch (Exception e) {
		   fwkResult = FWK_SEVERE;
		   FWKEXCEPTION("Caught unexpected exception  during verifyRegionContentsAfterOpsRI "<< e.getMessage());
	   }
  }
  //delete static_ops_RI_keysValues;
  //delete static_RI_noops_none;
  return fwkResult;
}

int32_t CacheServerTest::verifyRegionContentsBeforeOps()
{
  int32_t fwkResult = FWK_SUCCESS;
  FWKINFO( "verifyRegionContentsBeforeOps called." );

  CacheableHashSetPtr keysToCheck = verifyRegionSize(static_RI_noops_keysValues);
 // CacheableHashSetPtr::Iterator it = keysToCheck->Iterator;
  for (CacheableHashSet::Iterator it = keysToCheck->begin(); it != keysToCheck->end(); it++){
	  CacheableKeyPtr key = *it;
	  try {
	        verifyEntry(key,static_RI_noops_keysValues);
	   } catch (Exception e) {
		   fwkResult = FWK_SEVERE;
		   FWKEXCEPTION("Caught unexpected exception  during verifyRegionContentsBeforeOps "<< e.getMessage());
	   }
  }
  //delete static_RI_noops_keysValues;
  //delete static_RI_noops_none;
  return fwkResult;
}

int32_t CacheServerTest::verifyRegionContentsAfterLateOps()
{
  int32_t fwkResult = FWK_SUCCESS;
  FWKINFO( "verifyRegionContentsAfterOps called." );

  CacheableHashSetPtr keysToCheck = verifyRegionSize(static_RI_ops_keysValues);
 // CacheableHashSetPtr::Iterator it = keysToCheck->Iterator;
  for (CacheableHashSet::Iterator it = keysToCheck->begin(); it != keysToCheck->end(); it++){
	  CacheableKeyPtr key = *it;
	  try {
	        verifyEntry(key,static_RI_ops_keysValues);
	   } catch (Exception e) {
		   fwkResult = FWK_SEVERE;
		   FWKEXCEPTION("Caught unexpected exception  during verifyRegionContentsAfterOps "<< e.getMessage());
	   }
  }
  //delete static_RI_ops_keysValues;
  //delete static_RI_ops_none;
  return fwkResult;
}


int32_t CacheServerTest::verifyRegionContentsDynamic(){
	 int32_t fwkResult = FWK_SUCCESS;
	  FWKINFO( "verifyRegionContentsDynamic called." );

	  CacheableHashSetPtr keysToCheck = verifyRegionSize(dynamicKeysValues);
	 // CacheableHashSetPtr::Iterator it = keysToCheck->Iterator;
	  for (CacheableHashSet::Iterator it = keysToCheck->begin(); it != keysToCheck->end(); it++){
		  CacheableKeyPtr key = *it;
		  try {
		        verifyEntry(key,dynamicKeysValues);
		   } catch (Exception e) {
			   fwkResult = FWK_SEVERE;
			   FWKEXCEPTION("Caught unexpected exception  during verifyRegionContentsDynamic "<< e.getMessage());
		   }
	  }
	  //delete dynamicKeysValues;
	  return fwkResult;

}
void CacheServerTest::verifyEntry(CacheableKeyPtr key,ExpectedRegionContents *expected){  
	int32_t i = dynCast<CacheableInt32Ptr>(key)->value();
    int32_t numNewKeys = getIntValue( "NumNewKeys" );

    int32_t entryCount = getIntValue( "entryCount" );

    RegionPtr regionPtr = getRegion();
    if ((i >= bbGet("ImageBB", "First_None")) &&
              (i <=  bbGet("ImageBB", "Last_None"))) {
      checkContainsKey(key, expected->containsKey_none(), "key was untouched");
      checkContainsValueForKey(key, expected->containsValue_none(), "key was untouched");
      if (expected->getAllowed_none()) {
    	  CacheablePtr value = regionPtr->get(key);
          checkValue(key, value);
      }
    } else if ((i >= bbGet("ImageBB", "First_Get")) &&
               (i <= bbGet("ImageBB", "Last_Get"))) {
      // this key was untouched after its creation
      checkContainsKey(key, expected->containsKey_get(), "get key");
      checkContainsValueForKey(key, expected->containsValue_get(), "get key");
      if (expected->getAllowed_get()) {
    	  CacheablePtr value = regionPtr->get(key);
         checkValue(key, value);
      }
   } else if ((i >= bbGet("ImageBB", "First_Invalidate")) &&
              (i <= bbGet("ImageBB", "Last_Invalidate"))) {
      checkContainsKey(key, expected->containsKey_invalidate(), "key was invalidated (Bug 35303)");
      bool expectValue = expected->containsValue_invalidate();
      checkContainsValueForKey(key, expectValue, "key was invalidated (Bug 35303)");
      if (expected->getAllowed_invalidate()) {
    	  CacheablePtr value = regionPtr->get(key);
         if (expectValue) {
            checkValue(key, value);
         } else {
            if (value != NULLPTR) {
            	throw new Exception("Bug 35303, after calling get( %s ), expected invalidated value to be null but it is %s " , key->toString()->asChar(),value->toString()->asChar());
            }
         }
      }
   } else if ((i >= bbGet("ImageBB", "First_LocalInvalidate")) &&
              (i <= bbGet("ImageBB", "Last_LocalInvalidate"))) {
      // this key was locally invalidated
     checkContainsKey(key, expected->containsKey_localInvalidate(), "key was locally invalidated");
      checkContainsValueForKey(key, expected->containsValue_localInvalidate(), "key was locally invalidated");
      if (expected->getAllowed_localInvalidate()) {
    	  CacheablePtr value = regionPtr->get(key);
         checkValue(key, value);
      }
   } else if ((i >= bbGet("ImageBB", "First_Destroy")) &&
              (i <= bbGet("ImageBB", "Last_Destroy"))) {
      // this key was destroyed
      checkContainsKey(key, expected->containsKey_destroy(), "key was destroyed");
      bool expectValue = expected->containsValue_destroy();
      checkContainsValueForKey(key, expectValue, "key was destroyed");
      if (expected->getAllowed_destroy()) {
    	  CacheablePtr value = regionPtr->get(key);
         if (expectValue) {
            checkValue(key, value);
         } else {
            if (value != NULLPTR) {
            	throw new Exception("Expected value for %s to be null",key->toString()->asChar() );
            }
         }
      }
   } else if ((i >= bbGet("ImageBB", "First_LocalDestroy")) &&
              (i <= bbGet("ImageBB", "Last_LocalDestroy"))) {
      // this key was locally destroyed
     checkContainsKey(key, expected->containsKey_localDestroy(), "key was locally destroyed");
      checkContainsValueForKey(key, expected->containsValue_localDestroy(), "key was locally destroyed");
      if (expected->getAllowed_localDestroy()) {
    	  CacheablePtr value = regionPtr->get(key);
         checkValue(key, value);
      }
   } else if ((i >= bbGet("ImageBB", "First_UpdateExistingKey")) &&
              (i <= bbGet("ImageBB", "Last_UpdateExistingKey"))) {
      // this key was updated
      checkContainsKey(key, expected->containsKey_update(), "key was updated");
      checkContainsValueForKey(key, expected->containsValue_update(), "key was updated");
      if (expected->getAllowed_update()) {
    	  CacheablePtr value = regionPtr->get(key);
         if (expected->valueIsUpdated()) {
            checkUpdatedValue(key, value);
         } else {
            checkValue(key, value);
         }
      }
   } else if ((i > entryCount) && (i <= (entryCount + numNewKeys))) {
      // key was newly added
      checkContainsKey(key, expected->containsKey_newKey(), "key was new");
      checkContainsValueForKey(key, expected->containsValue_newKey(), "key was new");
      if (expected->getAllowed_newKey()) {
    	  CacheablePtr value = regionPtr->get(key);
         checkValue(key, value);
      }
   } else { // key is outside of keyIntervals and new keys; it was never loaded
     // key was never loaded
     checkContainsKey(key, false, "key was never used");
     checkContainsValueForKey(key, false, "key was never used");
   }
}
/** Check that the value of the given key is expected for this test.
 *  Throw an error if any problems.
 *
 *  @param key The key to check.
 *  @param value The value for the key.
 */
void CacheServerTest::checkValue(CacheableKeyPtr key, CacheablePtr value) {
	int keyCounter = dynCast<CacheableInt32Ptr>(key)->value();
	if (instanceOf<CacheableInt32Ptr>(value)){
		int longValue = dynCast<CacheableInt32Ptr>(value)->value();
		if (keyCounter != longValue)
            throw new Exception("Inconsistent Value for key %s Expected Value should be %s" ,key->toString()->asChar(), value->toString()->asChar());
	}else if (instanceOf<PdxVersioned1Ptr>(value)){
		string expectedValue = std::string("PdxVersioned ")+  std::string(key->toString()->asChar());
		PdxVersioned1Ptr val = dynCast<PdxVersioned1Ptr>(value);
		if(strcmp(val->getString(),expectedValue.c_str())!=0)
		{
			throw new Exception("Inconsistent Value for key %s Expected Value should be %s" ,key->toString()->asChar(), value->toString()->asChar());
		}
	}else if (instanceOf<PdxVersioned2Ptr>(value)){
		string expectedValue = std::string("PdxVersioned ")+  std::string(key->toString()->asChar());
		PdxVersioned2Ptr val = dynCast<PdxVersioned2Ptr>(value);
		if(strcmp(val->getString(),expectedValue.c_str())!=0)
		{
			throw new Exception("Inconsistent Value for key %s Expected Value should be %s" ,key->toString()->asChar(), value->toString()->asChar());
		}
	}else if (instanceOf<AutoPdxTests::AutoPdxVersioned1Ptr>(value)){
		string expectedValue = std::string("PdxVersioned ")+  std::string(key->toString()->asChar());
		AutoPdxTests::AutoPdxVersioned1Ptr val = dynCast<AutoPdxTests::AutoPdxVersioned1Ptr>(value);
		if(strcmp(val->getString(),expectedValue.c_str())!=0)
		{
			throw new Exception("Inconsistent Value for key %s Expected Value should be %s" ,key->toString()->asChar(), value->toString()->asChar());
		}
	}else if (instanceOf<AutoPdxTests::	AutoPdxVersioned2Ptr>(value)){
		string expectedValue = std::string("PdxVersioned ")+  std::string(key->toString()->asChar());
		AutoPdxTests::AutoPdxVersioned2Ptr val = dynCast<AutoPdxTests::AutoPdxVersioned2Ptr>(value);
		if(strcmp(val->getString(),expectedValue.c_str())!=0)
		{
			throw new Exception("Inconsistent Value for key %s Expected Value should be %s" ,key->toString()->asChar(), value->toString()->asChar());
		}
	}
	else
        throw new Exception("Expected Value and type for key %s not found. " , key->toString()->asChar());
}

/** Check that the value of the given key is expected as an updated value.
 *  Throw an error if any problems.
 *
 *  @param key The key to check.
 *  @param value The value for the key.
 */
void CacheServerTest::checkUpdatedValue(CacheableKeyPtr key, CacheablePtr value) {
	int keyCounter = dynCast<CacheableInt32Ptr>(key)->value();
	if (instanceOf<CacheableStringPtr>(value)) {
		std::string aStr(value->toString()->asChar());
		std::string expectedStr = "updated_" + FwkStrCvt(keyCounter).toString();
		if (aStr.compare(expectedStr) != 0){
			std::string errMsg = "Inconsistent Value for key " + std::string(key->toString()->asChar()) + " Expected Value should be " + expectedStr
			    +  " but is is " + std::string(value->toString()->asChar());
	       throw new Exception(errMsg.c_str());
		}
    }else if (instanceOf<PdxVersioned1Ptr>(value)){
		string expectedValue = std::string("PdxVersioned updated_")+  std::string(key->toString()->asChar());
		PdxVersioned1Ptr val = dynCast<PdxVersioned1Ptr>(value);
		if(strcmp(val->getString(),expectedValue.c_str())!=0)
		{
			std::string errMsg = "Inconsistent Value for key " + std::string(key->toString()->asChar()) + " Expected Value should be " + expectedValue
						    +  " but is is " + std::string(val->toString()->asChar());
			throw new Exception(errMsg.c_str());
		}
	}else if (instanceOf<PdxVersioned2Ptr>(value)){
		string expectedValue = std::string("PdxVersioned updated_") +  std::string(key->toString()->asChar());
		PdxVersioned2Ptr val = dynCast<PdxVersioned2Ptr>(value);
		if(strcmp(val->getString(),expectedValue.c_str())!=0)
		{
			std::string errMsg = "Inconsistent Value for key " + std::string(key->toString()->asChar()) + " Expected Value should be " + expectedValue
									    +  " but is is " + std::string(val->toString()->asChar());
			throw new Exception(errMsg.c_str());
		}
	}
	else if (instanceOf<AutoPdxTests::AutoPdxVersioned1Ptr>(value)){
		string expectedValue = std::string("PdxVersioned updated_")+  std::string(key->toString()->asChar());
		AutoPdxTests::AutoPdxVersioned1Ptr val = dynCast<AutoPdxTests::AutoPdxVersioned1Ptr>(value);
		if(strcmp(val->getString(),expectedValue.c_str())!=0)
		{
			std::string errMsg = "Inconsistent Value for key " + std::string(key->toString()->asChar()) + " Expected Value should be " + expectedValue
						    +  " but is is " + std::string(val->toString()->asChar());
			throw new Exception(errMsg.c_str());
		}
	}else if (instanceOf<AutoPdxTests::AutoPdxVersioned2Ptr>(value)){
		string expectedValue = std::string("PdxVersioned updated_") +  std::string(key->toString()->asChar());
		AutoPdxTests::AutoPdxVersioned2Ptr val = dynCast<AutoPdxTests::AutoPdxVersioned2Ptr>(value);
		if(strcmp(val->getString(),expectedValue.c_str())!=0)
		{
			std::string errMsg = "Inconsistent Value for key " + std::string(key->toString()->asChar()) + " Expected Value should be " + expectedValue
									    +  " but is is " + std::string(val->toString()->asChar());
			throw new Exception(errMsg.c_str());
		}
	}
	else{
		 throw new Exception("Expected Value and type for key %s not found. " , key->toString()->asChar());
    }

}
//---------------------------------------------------------------------------------


int32_t CacheServerTest::doEntryOperationsForSecurity()
{
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO( "doEntryOperationsForSecurity called." );

  int32_t opsSec = getIntValue( "opsSecond" );
  opsSec = ( opsSec < 1 ) ? 0 : opsSec;

  int32_t entryCount = getIntValue( "entryCount" );
  entryCount = ( entryCount < 1 ) ? 10000 : entryCount;

  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;

  int32_t valSize = getIntValue( "valueSizes" );
  valSize = ( ( valSize < 0) ? 32 : valSize );

  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  ACE_Time_Value now;

  CacheableStringPtr keyPtr;
  CacheablePtr valuePtr;
  CacheablePtr tmpValue;
  char * valBuf = new char[valSize + 1];
  memset( valBuf, 'A', valSize );
  valBuf[valSize] = 0;

  std::string opcode;
  std::string objectType;

  RegionPtr regionPtr = getRegion();
  if (regionPtr == NULLPTR) {
    fwkResult = FWK_SEVERE;
    FWKEXCEPTION( "CacheServerTest::doEntryOperationsForSecurity(): No region to perform operations on." );
    now = end; // Do not do the loop
  }

  FWKINFO( "doEntryOperationsForSecurity will work for " << secondsToRun << " using " << valSize << " byte values." );
  char cqName[32] = {'\0'};
  int cnt = 0;

  PaceMeter meter( opsSec );
  objectType = getStringValue( "objectType" );
  while ( now < end ) {
    doAddOperation = true;
    opcode = getStringValue( "entryOps" );
    try {
      updateOperationMap(opcode);
      if ( opcode.empty() ) opcode = "no-op";

      if ( opcode == "create" ) {
        keyPtr = getKey( entryCount );
        if(!objectType.empty()) {
          tmpValue = getUserObject(objectType);
        } else
            tmpValue = CacheableBytes::create( ( const unsigned char * )valBuf, static_cast<int32_t> (strlen( valBuf )) );
            regionPtr->create( keyPtr, tmpValue );
      }
      else {
        keyPtr = getKey( entryCount );
        if ( opcode == "update" ) {
          if(!objectType.empty()) {
            tmpValue = getUserObject(objectType);
          } else {
            int32_t keyVal = atoi(keyPtr->toString());
            tmpValue = CacheableBytes::create( ( const unsigned char * )valBuf, static_cast<int32_t> (strlen( valBuf )) );
            int32_t * val = ( int32_t * )(dynCast<CacheableBytesPtr>(tmpValue)->value());
            *val = (*val == keyVal) ? keyVal + 1 : keyVal;  // alternate the value so that it can be validated later.
            int64_t * adjNow = ( int64_t * )(dynCast<CacheableBytesPtr>(tmpValue)->value() + 4 );
            *adjNow = getAdjustedNowMicros();
          }
          regionPtr->put( keyPtr, tmpValue );
        }
        else if ( opcode == "invalidate" ) {
          regionPtr->invalidate( keyPtr );
        }
        else if ( opcode == "destroy" ) {
          regionPtr->destroy( keyPtr );
        }
        else if ( opcode == "get" ) {
            valuePtr = regionPtr->get( keyPtr );
        }
        else if ( opcode == "getServerKeys" ) {
          VectorOfCacheableKey keysVec;
          regionPtr->serverKeys( keysVec );
        }
        else if ( opcode == "read+localdestroy" ) {
          valuePtr = regionPtr->get( keyPtr );
          regionPtr->localDestroy( keyPtr );
        }
        else if ( opcode == "regNUnregInterest" ) {
           VectorOfCacheableKey registerKeyList;
           registerKeyList.push_back(keyPtr);
            regionPtr->registerKeys(registerKeyList);
             regionPtr->unregisterKeys(registerKeyList);
        }
        else if ( opcode == "query" ) {
          QueryServicePtr qs = checkQueryService();
          QueryPtr qry;
          SelectResultsPtr results;
          qry = qs->newQuery("select distinct * from /Portfolios where FALSE");
          results = qry->execute( QUERY_RESPONSE_TIMEOUT );
        }
        else if ( opcode == "cq" ) {
          ACE_OS::sprintf(cqName,"cq-%d-%d",g_test->getClientId(),cnt++);
          QueryServicePtr qs = checkQueryService();
          CqAttributesFactory cqFac;
          CqListenerPtr cqLstner(new MyCqListener());
          cqFac.addCqListener(cqLstner);
          CqAttributesPtr cqAttr = cqFac.create();
          CqQueryPtr qry = qs->newCq(cqName,"select * from /Portfolios where FALSE", cqAttr);
          qry->execute();
          qry->stop();
          qry->execute();
          qry->close();
        }
        else {
          FWKEXCEPTION( "Invalid operation specified: " << opcode );
        }
      }
    } catch(const gemfire::NotAuthorizedException &){
       FWKINFO(" Got Expected NotAuthorizedException for operation " << opcode);
       updateExceptionMap(opcode);
    }catch ( TimeoutException &e ) {
      fwkResult = FWK_SEVERE;
      FWKSEVERE( "Caught unexpected timeout exception during entry " << opcode
        << " operation: " << e.getMessage() << " continuing with test." );
    }catch ( EntryExistsException &ignore ) {
      ignore.getMessage();
      doAddOperation = false;
      updateOperationMap(opcode);
    } catch ( EntryNotFoundException &ignore ) {
      ignore.getMessage();
      doAddOperation = false;
      updateOperationMap(opcode);
    } catch ( EntryDestroyedException &ignore ) {
      ignore.getMessage();
      doAddOperation = false;
      updateOperationMap(opcode);
    } catch ( Exception &e ) {
      end = 0;
      fwkResult = FWK_SEVERE;
      FWKEXCEPTION( "Caught unexpected exception during entry " << opcode
        << " operation: " << e.getMessage() << " : "<< e.getName() <<" exiting task." );
    }
    meter.checkPace();
    now = ACE_OS::gettimeofday();
  }
  keyPtr = NULLPTR;
  valuePtr = NULLPTR;
  delete [] valBuf;

  return fwkResult;
}
bool CacheServerTest::allowQuery(queryCategory category, bool haveLargeResultset,bool islargeSetQuery, bool isUnsupportedPRQuery)
{
  if(category == unsupported)
    return false;
  else if(haveLargeResultset != islargeSetQuery)
    return false;
  else if(isUnsupportedPRQuery && ((category == multiRegion) || (category == nestedQueries)))
    return false;
  else
    return true;

}
//
void CacheServerTest::remoteQuery(QueryStrings currentQuery,bool islargeSetQuery ,
    bool isUnsupportedPRQuery, int32_t queryIndex, bool isparam, bool isStructSet)
{
  ACE_Time_Value startTime, endTime;
  QueryServicePtr qs = checkQueryService();
  QueryPtr qry;
  SelectResultsPtr results;
  if (allowQuery(currentQuery.category,
      currentQuery.haveLargeResultset, islargeSetQuery,
      isUnsupportedPRQuery)) {
    FWKINFO(" running currentQuery["<< queryIndex << "] query : " <<currentQuery.query());
    qry = qs->newQuery(currentQuery.query());
    CacheableVectorPtr paramList = NULLPTR;
    if(isparam)
    {
      paramList = CacheableVector::create();
      if (isStructSet) {
        for (int j = 0; j < numSSQueryParam[queryIndex]; j++) {
          if (atoi(queryparamSetSS[queryIndex][j]) != 0) {
            paramList->push_back(Cacheable::create(atoi(queryparamSetSS[queryIndex][j])));
          } else
            paramList->push_back(Cacheable::create(queryparamSetSS[queryIndex][j]));
        }
      } else {
        for (int j = 0; j < noofQueryParam[queryIndex]; j++) {
          if (atoi(queryparamSet[queryIndex][j]) != 0) {
            paramList->push_back(Cacheable::create(atoi(queryparamSet[queryIndex][j])));
          } else
            paramList->push_back(Cacheable::create(queryparamSet[queryIndex][j]));
        }
      }
    }
    startTime = ACE_OS::gettimeofday();
    if(isparam){
      results = qry->execute(paramList,QUERY_RESPONSE_TIMEOUT);
      paramList->clear();
    }else {
      results = qry->execute(QUERY_RESPONSE_TIMEOUT);
    }    
    endTime = ACE_OS::gettimeofday() - startTime;
    FWKINFO(" Time Taken to execute the query : "<< currentQuery.query() << ": is " << endTime.sec() << "." << endTime.usec() << " sec");
  }
}
// ----------------------------------------------------------------------------
void CacheServerTest::runQuery(int32_t & queryCnt)
{
  try {
    resetValue( "entryCount" );
    int numOfKeys = getIntValue( "entryCount" );
    QueryHelper *qh = &QueryHelper::getHelper();
    int setSize = qh->getPortfolioSetSize();
    if(numOfKeys < setSize){
      setSize = numOfKeys;
    }
    int32_t i = GsRandom::random((uint32_t)0,( uint32_t)QueryStrings::RSsize());

    resetValue( "largeSetQuery" );
    resetValue( "unsupportedPRQuery" );
    bool islargeSetQuery = g_test->getBoolValue( "largeSetQuery" );
    bool isUnsupportedPRQuery = g_test->getBoolValue( "unsupportedPRQuery" );
    //QueryStrings currentQuery = resultsetQueries[i];
    remoteQuery(resultsetQueries[i], islargeSetQuery, isUnsupportedPRQuery, i, false,false);
    queryCnt++;
    i = GsRandom::random((uint32_t) 0, (uint32_t) QueryStrings::SSsize());
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
    //currentQuery = structsetQueries[i];
      remoteQuery(structsetQueries[i], islargeSetQuery, isUnsupportedPRQuery, i, false,false);
    queryCnt++;
    i = GsRandom::random((uint32_t) 0, (uint32_t) QueryStrings::RSPsize());
    //currentQuery = resultsetparamQueries[i];
    remoteQuery(resultsetparamQueries[i], islargeSetQuery, isUnsupportedPRQuery, i, true,false);
    queryCnt++;
    i = GsRandom::random((uint32_t) 0, (uint32_t) QueryStrings::SSPsize());
    //currentQuery = structsetParamQueries[i];
    remoteQuery(structsetParamQueries[i], islargeSetQuery, isUnsupportedPRQuery, i, true, true);
    queryCnt++;

  } catch ( Exception e ) {
    FWKEXCEPTION( "CacheServerTest::runQuery Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "CacheServerTest::runQuery Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "CacheServerTest::runQuery Caught unknown exception." );
  }

}

void CacheServerTest::add( int32_t count, char * valBuf)
{
  RegionPtr regionPtr = getRegion();
  if (regionPtr == NULLPTR) {
    FWKSEVERE( "CacheServerTest::add(): No region to perform add on." );
    return;
  }

  char buf[33];
  sprintf( buf, "%u", count );
  CacheableKeyPtr key = CacheableKey::create( buf );
  CacheableBytesPtr value = CacheableBytes::create( ( const unsigned char * )valBuf, static_cast<int32_t> (strlen( valBuf )) );
  int32_t * val = ( int32_t * )( value->value() );
  *val = count;
  int64_t * adjNow = ( int64_t * )( value->value() + 4 );
  *adjNow = getAdjustedNowMicros();

  try {
    regionPtr->create( key, value );
   } catch (const NotConnectedException &e ) {
    FWKEXCEPTION( "CacheServerTest::add() caught NotConnectedException " << e.getMessage() );
  } catch (const OutOfMemoryException &e ) {
    FWKEXCEPTION( "CacheServerTest::add() caught OutOfMemoryException " << e.getMessage() );
  } catch (const CacheProxyException &e ) {
    FWKEXCEPTION( "CacheServerTest::add() caught CacheProxyException " << e.getMessage() );
  } catch (const IllegalArgumentException &e ) {
    FWKEXCEPTION( "CacheServerTest::add() caught IllegalArgumentException " << e.getMessage() );
  } catch (const RegionDestroyedException &e ) {
    FWKEXCEPTION( "CacheServerTest::add() caught RegionDestroyedException " << e.getMessage() );
  }
}

int32_t CacheServerTest::verifyKeyCount()
{
  int32_t result = FWK_SUCCESS;
  RegionPtr regionPtr = getRegion();
  if (regionPtr == NULLPTR) {
    result = FWK_SEVERE;
    FWKSEVERE( "CacheServerTest::verifyKeyCount(): No region to count keys in." );
    return result;
  }

  // Get the feed's final count from the BB
  std::string key( "FINAL_FEED_COUNT" );
  int64_t final = bbGet( CSCacheListener::getBB(), key );
  // How many keys in our region?
  VectorOfCacheableKey keys;
  regionPtr->keys( keys );

  int32_t kcnt = keys.size();
  int32_t fcnt = ( int32_t )final;

  if ( kcnt != fcnt ) {
    FWKSEVERE( "verifyKeyCount:: Counts do not match: have " << kcnt << ", expected " << fcnt );
    result = FWK_SEVERE;
  }
  return result;
}

int32_t CacheServerTest::verifyCount()
{
  int32_t result = FWK_SUCCESS;
  RegionPtr regionPtr = getRegion();
  if (regionPtr == NULLPTR) {
    result = FWK_SEVERE;
    FWKSEVERE( "CacheServerTest::verifyCount(): No region to verify." );
    return result;
  }

  std::string key( "AFTER_CREATE_VERIFY_NAME" );
  std::string tag( bbGetString( CSCacheListener::getBB(), key ) );
  int64_t val = bbGet( CSCacheListener::getBB(), tag );
  int32_t expected = ( int32_t )val;
  CSCacheListenerPtr listener = dynCast<CSCacheListenerPtr>( regionPtr->getAttributes()->getCacheListener() );
  val = bbGet( CSCacheListener::getBB(), listener->afterCreateTag() );
  int32_t cnt = ( int32_t )val;
  FWKINFO( "CacheListener -- creates: " << m_createCount << " ( " << cnt
    << " ) ( expected: "
    << expected << " )   latencies ( usec ): min: " << m_createMin
    << " max: " << m_createMax << " average: " << m_createAvg );
  if ( ( m_createCount != expected ) && ( cnt != expected ) ) {
    FWKSEVERE( "verifyCount:: Counts do not match: have " << cnt << ", expected " << expected );
    result = FWK_SEVERE;
  }
  return result;
}

void CacheServerTest::updateOperationMap(std::string operation)
{
  if(doAddOperation) {
    totalOperation = operationsMap[operation];
    if(totalOperation == 0)
      operationsMap[operation]= 1;
    else
      operationsMap[operation] = ++totalOperation;
  } else
      operationsMap[operation]= --totalOperation;


}
void CacheServerTest::updateExceptionMap(std::string operation)
{
  notAuthzCount = exceptionMap[operation];
  if(notAuthzCount == 0)
    exceptionMap[operation] = 1;
  else
    exceptionMap[operation] = ++notAuthzCount;
}
int32_t CacheServerTest::validateEntryOperationsForSecurity()
{
  int32_t result = FWK_SUCCESS;
  bool isExpectedPass = getBoolValue( "isExpectedPass" );
  std::string operation = getStringValue( "entryOps" );
  while (!operation.empty()){
    totalOperation = operationsMap[operation];
    notAuthzCount = exceptionMap[operation];
    if( isExpectedPass ) {
      if( totalOperation != 0 && notAuthzCount == 0){
        FWKINFO("Task passed sucessfully with total operation = " << totalOperation);
      }
      else {
         FWKEXCEPTION(notAuthzCount << " NotAuthorizedException found for operation " << operation << " while expected 0");
         result = FWK_SEVERE;
      }
    } else {
      if( totalOperation == notAuthzCount ) {
       FWKINFO("Task passed sucessfully and got the expected number of not authorize exception: "
                              << notAuthzCount << " with total number of operation " << totalOperation);
      } else {
         FWKEXCEPTION("Expected NotAuthorizedException with total number of " << operation
             << " operation " << totalOperation << " but found " << notAuthzCount );
         result = FWK_SEVERE;
      }
    }
    operation = getStringValue( "entryOps" );
  }
  return result;
}
//----------------------------------------------------------------------------
void CacheServerTest::removeAllOps(bool isWithCallBck)
{

	RegionPtr regPtr0=getRegion();
	VectorOfCacheableKey keys1;
	CacheableKeyPtr callBckArg=CacheableKey::create(10);
	RegionAttributesPtr attr=regPtr0->getAttributes();
	CacheListenerPtr cacheListrn = attr->getCacheListener();
//	perf::PerfTestCacheListener * listener = dynamic_cast<perf::PerfTestCacheListener *> (regPtr0->getAttributes()->getCacheListener().ptr());
	char buf[33];
	for(uint32_t i=0; i<200; i++) {
      sprintf( buf, "%u", i );
      CacheableKeyPtr key = CacheableKey::create( buf );
      keys1.push_back(key);
    }
    try{
      if(cacheListrn!=NULLPTR){
        if(isWithCallBck){
    	  perf::PerfTestCacheListener * listener = dynamic_cast<perf::PerfTestCacheListener *> (regPtr0->getAttributes()->getCacheListener().ptr());
    	  listener->setCallBackArg(callBckArg);
    	  regPtr0->removeAll(keys1,callBckArg);
    	  FWKINFO("removeAll Successful with callBckArg = " << callBckArg->toString()->asChar());
        }
        else{
      	  regPtr0->removeAll(keys1);
    	  FWKINFO("removeAll Successful");
        }
      }
    }
    catch(Exception &e)
    {
	   FWKEXCEPTION("Caught the following exception" << e.getMessage());
    }
    FWKINFO("REMOVEALL SUCCESSFULL");
}

void CacheServerTest::getAllOps(bool isWithCallBck)
{
  RegionPtr regPtr0 = getRegion();
  VectorOfCacheableKey keys1;
  CacheableKeyPtr callBckArg=CacheableKey::create(10);
  RegionAttributesPtr attr=regPtr0->getAttributes();
  CacheListenerPtr cacheListrn = attr->getCacheListener();
  //perf::PerfTestCacheListener * listener = dynamic_cast<perf::PerfTestCacheListener *> (regPtr0->getAttributes()->getCacheListener().ptr());
  char buf[33];
  for (uint32_t i=0; i<200; i++) {
    sprintf( buf, "%u", i );
    CacheableKeyPtr key = CacheableKey::create( buf );
    keys1.push_back(key);
  }
  HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
  valuesMap->clear();
  try{
    if(cacheListrn != NULLPTR){
      if(isWithCallBck){
    	perf::PerfTestCacheListener * listener = dynamic_cast<perf::PerfTestCacheListener *> (regPtr0->getAttributes()->getCacheListener().ptr());
    	listener->setCallBackArg(callBckArg);
    	regPtr0->getAll(keys1, valuesMap, NULLPTR, false,callBckArg);
    	FWKINFO("getAllCB Successful with callBckArg = " << callBckArg->toString()->asChar());
      }
      else{
    	regPtr0->getAll(keys1, valuesMap, NULLPTR, false);
  	    FWKINFO("getAll Successful");
      }
    }
  }
  catch(Exception &e)
  {
	FWKEXCEPTION("Caught the following exception" << e.getMessage());
  }
  }

void CacheServerTest::putAllOps(bool isWithCallBck)
{

  RegionPtr regPtr0 = getRegion();
  HashMapOfCacheable map0;
  map0.clear();
  CacheableKeyPtr key;
  CacheableKeyPtr callBckArg=CacheableKey::create(10);
  int32_t valSize = getIntValue( "valueSizes" );
  valSize = ( ( valSize < 0) ? 32 : valSize );
  char buf[33];
  char * valBuf = NULL;
  resetValue("objectType");
  const std::string objectType = getStringValue( "objectType" );
  RegionAttributesPtr attr=regPtr0->getAttributes();
  CacheListenerPtr cacheListrn = attr->getCacheListener();
  //perf::PerfTestCacheListener * listener = dynamic_cast<perf::PerfTestCacheListener *> (regPtr0->getAttributes()->getCacheListener().ptr());
   if(!objectType.empty()){
     int32_t numSet=0;
     int32_t setSize=0;
     QueryHelper *qh = &QueryHelper::getHelper();
     CacheablePtr port;
     setSize = qh->getPortfolioSetSize();
     numSet = 200/setSize;
     for(int set=1; set<=numSet; set++)
     {
       for(int current=1; current<=setSize; current++)
       {
         if (objectType == "Portfolio")
         {
           port = new Portfolio(current, valSize);
         }
         else if( objectType == "PortfolioPdx")
         {
           port = new testobject::PortfolioPdx(current, valSize);
         }
         else if( objectType == "AutoPortfolioPdx")
         {
           port = new AutoPdxTests::PortfolioPdx(current, valSize);
         }
         sprintf(buf, "port%d-%d", set, current);
         key = CacheableKey::create(buf);
         map0.insert(key, port);

       }
     }
   }
   else
   {
     valBuf = new char[valSize + 1];
     memset( valBuf, 'A', valSize );
     valBuf[valSize] = 0;
     for (uint32_t i=0; i<200; i++) {
       sprintf( buf, "%u", i );
       key = CacheableKey::create( buf );
       CacheableBytesPtr value = CacheableBytes::create( ( const unsigned char * )valBuf, static_cast<int32_t> (strlen( valBuf )) );
       map0.insert(key, value);
     }
   }
   if(cacheListrn != NULLPTR){
     if(isWithCallBck){
      try{
    	  SpinLockGuard guard( m_lck );{
       perf::PerfTestCacheListener * listener = dynamic_cast<perf::PerfTestCacheListener *> (regPtr0->getAttributes()->getCacheListener().ptr());
       listener->setCallBackArg(callBckArg);
    	  }
	   regPtr0->putAll(map0,PUTALL_TIMEOUT,CacheableInt32::create(10));
       FWKINFO("PUTALL Successful for key " << key->toString()->asChar()<<" with callBckArg = " << callBckArg->toString()->asChar());
      }
      catch(Exception &e)
      {
    	  FWKEXCEPTION("Got this exception during PUTALLCB"<< e.getMessage());
      }
     }
   }
   else{
	 regPtr0->putAll(map0,PUTALL_TIMEOUT);
	 FWKINFO("PUTALL Successful for key " << key->toString()->asChar());
   }


  if( valBuf != NULL)
    delete [] valBuf;
}
// ----------------------------------------------------------------------------

std::string CSCacheListener::m_bb( "CacheListenerBB" );

CSCacheListener::CSCacheListener( FrameworkTest * test, const char * pref )
: m_test( test )
{
  int32_t id = test->getClientId();

  char buf[48];
  sprintf( buf, "%s_%s_%d", pref, "AFTER_CREATE_COUNT", id );
  m_acTag = buf;
  if ( strcmp( pref, "PEER" ) == 0 ) {
    std::string key( "AFTER_CREATE_VERIFY_NAME" );
    test->bbSet( m_bb, key, m_acTag );
  }
  sprintf( buf, "%s_%s_%d", pref, "AFTER_UPDATE_COUNT", id );
  m_auTag = buf;
  sprintf( buf, "%s_%s_%d", pref, "AFTER_INVALIDATE_COUNT", id );
  m_aiTag = buf;
  sprintf( buf, "%s_%s_%d", pref, "AFTER_DESTROY_COUNT", id );
  m_adTag = buf;
  sprintf( buf, "%s_%s_%d", pref, "AFTER_REGION_INVALIDATE_COUNT", id );
  m_ariTag = buf;
  sprintf( buf, "%s_%s_%d", pref, "AFTER_REGION_DESTROY_COUNT", id );
  m_ardTag = buf;
  sprintf( buf, "%s_%s_%d", pref, "CLOSE_COUNT", id );
  m_cTag = buf;
}

void ValidationCacheListener::afterCreate( const EntryEvent& event )
{
  validate( event, m_createCount, m_createMin, m_createMax, m_createAvg, m_createTotal, m_acTag );
}

ValidationCacheListener::~ValidationCacheListener()
{
  FWKINFO( "CacheListener -- creates: " << m_createCount << "    latencies ( usec ): min: " << m_createMin
    << " max: " << m_createMax << " average: " << m_createAvg );
    FWKINFO( "CacheListener -- updates: " << m_updateCount << "   latencies ( usec ): min: " << m_updateMin
    << " max: " << m_updateMax << " average: " << m_updateAvg );

}

void ValidationCacheListener::afterUpdate( const EntryEvent& event )
{
  validate( event, m_updateCount, m_updateMin, m_updateMax, m_updateAvg, m_updateTotal, m_auTag );
}

void ValidationCacheListener::validate( const EntryEvent& event, int32_t & eventCount,
                                       int32_t & latencyMin , int32_t & latencyMax , int32_t & latencyAvg,
                                       int64_t & latencyTotal, const std::string &tag )
{
  int64_t now = g_test->getAdjustedNowMicros();
  // Verify values
  CacheableBytesPtr newVal = dynCast<CacheableBytesPtr>( event.getNewValue() );
  int32_t keyVal = atoi(event.getKey()->toString()->asChar());
  int32_t newValue = *( ( int32_t * )newVal->value() );

  if (keyVal != newValue && keyVal + 1 != newValue) {
    FWKSEVERE( "Incorrect Distribution events, for key: " << keyVal
      << " actual value: " << newValue );
  }

  // check latency
  int64_t * sendTime = ( int64_t * )( newVal->value() + 4 );
  int32_t latency = ( int32_t )llabs( now - *sendTime );
  latencyMin = std::min(latency, latencyMin);
  latencyMax = std::max(latency, latencyMax);
  latencyTotal += latency;
  eventCount++;
     // FWKINFO( "Count: " << eventCount << "  latency: " << latency << "  sent: " << *sendTime << "  now: " << now );
  if ( ( eventCount % 1000 ) == 0 ) {
    latencyAvg = ( int32_t )( latencyTotal / eventCount );
    //      FWKINFO( "Average latency: " << m_createAvg << " min: " << m_createMin << " max: " << m_createMax );
  }

 g_test->bbIncrement( CSCacheListener::getBB(), tag );
}

int32_t CacheServerTest::waitForSilenceListenerComplete(int64_t desiredSilenceSec, int64_t sleepMS) {
   int32_t result = FWK_SUCCESS;
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
    	  result = FWK_SEVERE;
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

   return result;
}
int32_t CacheServerTest::ResetImageBB()
{
	FWKINFO("In ResetImageBB");
	int32_t result = FWK_SEVERE;
	try
	{
	  resetValue("entryCount");
	  bbClear("ImageBB");
	  //bbClear("ListenerBB");
	  int entryCount = getIntValue("entryCount");
	  SetFisrtAndLastKeyOnBB(entryCount);
	  //ACE_Time_Value startTime = ACE_OS::gettimeofday();
	  //bbSet("ListenerBB", "lastEventTime",startTime.msec());
	  result = FWK_SUCCESS;
	}catch (Exception &e){
		  FWKEXCEPTION("Caught exception during ResetImageBB: " << e.getMessage());
	}
    FWKINFO("Done in ResetImageBB");
    return result;
}

CacheablePtr CacheServerTest::GetValue(int32_t value,char* update)
 {
	SerializablePtr tempVal;
	std::string m_objectType = getStringValue( "objectType" );
	int32_t m_version = getIntValue( "versionNum" );

   char buf[252];
   sprintf(buf,"%d",value);
   if(update != NULL)
   {
	   sprintf(buf,"%s%d",update,value);
   }
   else
	  sprintf(buf,"%d",value);
   if(m_objectType != ""){
	   if(m_objectType == "PdxVersioned" && m_version == 1)
	   {
		 PdxTests::PdxVersioned1Ptr pdxV1(new PdxTests::PdxVersioned1(buf));
		 tempVal = dynCast<PdxSerializablePtr>(pdxV1);
	   }
	   else if(m_objectType == "PdxVersioned" && m_version == 2)
	   {
		   PdxTests::PdxVersioned2Ptr pdxV2(new PdxTests::PdxVersioned2(buf));
		   tempVal = dynCast<PdxSerializablePtr>(pdxV2);
	   }
	   else if(m_objectType == "PdxType")
	   {
		 PdxTests::PdxTypePtr pdxTp(new PdxTests::PdxType());
		 tempVal = dynCast<PdxSerializablePtr>(pdxTp);
	   }
	   else if(m_objectType == "Nested")
	   {
		 PdxTests::NestedPdxPtr nestedPtr(new PdxTests::NestedPdx(buf));
		 tempVal = dynCast<PdxSerializablePtr>(nestedPtr);
	   }
	   else if(m_objectType == "AutoPdxVersioned" && m_version == 1)
	   {
		   AutoPdxTests::AutoPdxVersioned1Ptr pdxV1(new AutoPdxTests::AutoPdxVersioned1(buf));
		   tempVal = dynCast<PdxSerializablePtr>(pdxV1);
	   }
	   else if(m_objectType == "AutoPdxVersioned" && m_version == 2)
	   {
		   AutoPdxTests::AutoPdxVersioned2Ptr pdxV2(new AutoPdxTests::AutoPdxVersioned2(buf));
	     tempVal = dynCast<PdxSerializablePtr>(pdxV2);
	   }
	   else if(m_objectType == "AutoNested")
	   {
		 AutoPdxTests::NestedPdxPtr nestedPtr(new AutoPdxTests::NestedPdx(buf));
	   	 tempVal = dynCast<PdxSerializablePtr>(nestedPtr);
	   }
   }else if(update != NULL && m_objectType == ""){
	   tempVal = CacheableString::create( buf );
   }
   else
   {
	 tempVal = CacheableInt32::create( value );
   }

   return tempVal;
}

int32_t CacheServerTest::verifyQueryResult()
{
  FWKINFO("Inside verifyQueryResult");
  RegionPtr region = getRegion();
  VectorOfCacheableKey Skeys;
  region->serverKeys(Skeys);
  int32_t result = FWK_SEVERE;
  resetValue("query");
  std::string qryStr = getStringValue( "query" );
  QueryServicePtr qs=checkQueryService();
  ACE_Time_Value startTime,endTime;
  SelectResultsPtr sptr;
  bool isSerial=getBoolValue("serialExecution");
  int32_t localInvalidate=0;
  if(isSerial)
    localInvalidate = (int32_t)bbGet("ImageBB", "Last_LocalInvalidate") - (int32_t)bbGet("ImageBB", "First_LocalInvalidate") + 1;    
  //FWKINFO("localInvalidate = "<<localInvalidate << " region size = "<< region->size());  
  while(!qryStr.empty()){
    char cqName[100];
    sprintf(cqName,"_default%s",qryStr.c_str());
    CqQueryPtr qry = qs->getCq(cqName);
    try{
      if(qry->isRunning())
      {
        qry->stop();
      }
    }
    catch(IllegalStateException ex)
    {
      FWKEXCEPTION("CAUGHT while stopping CQ"<< ex.getMessage());
    }
    startTime = ACE_OS::gettimeofday();
    try
    {
      sptr = qry->executeWithInitialResults(QUERY_RESPONSE_TIMEOUT);
    }
    catch(IllegalStateException ex )
    {
      FWKEXCEPTION("CAUGHT while executing CQ"<< ex.getMessage());
    }
    endTime = ACE_OS::gettimeofday() - startTime;
    //FWKINFO("SP:Skeys.size = "<<Skeys.size()- GetVal);
    if((Skeys.size()- localInvalidate) == sptr->size())
      result=FWK_SUCCESS;
    else
    {
       FWKSEVERE(" result size found is "<< sptr->size() << " and expected result size is " << Skeys.size() - localInvalidate);
       return result;
    }
    FWKINFO(" Time Taken to execute the cq query : " << qry->getQueryString()  << " for cq " << qry->getName() << " : is " << endTime.sec() << "." << endTime.usec() << " sec "<<" result size found is "<< sptr->size());
    qryStr = getStringValue( "query" );
  }
  return result;
}

// ----------------------------------------------------------------------------

