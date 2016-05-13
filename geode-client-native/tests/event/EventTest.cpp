/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

/**
* @file    EventTest.cpp
* @since   1.0
* @version 1.0
* @see
*
*/

// ----------------------------------------------------------------------------

#define DEFAULT_WORK_TIME    60  // default, 60 seconds
#define DEFAULT_MAX_REGION                  10 // default, 100 regions
// ----------------------------------------------------------------------------

#include <GemfireCppCache.hpp>
#include <SystemProperties.hpp>
#include <gfcpp_globals.hpp>


#include "EventTest.hpp"
#include "fwklib/GsRandom.hpp"
#include "fwklib/PaceMeter.hpp"
#include "fwklib/RegionHelper.hpp"

#include "fwklib/FwkExport.hpp"

#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/os_include/os_assert.h>

using namespace gemfire;
using namespace gemfire::testframework;

static EventTest * g_test = NULL;
static ACE_Thread_Mutex * testLock;
static ACE_Thread_Mutex * balanceLock;
static ACE_Thread_Mutex * keyLock;

// ----------------------------------------------------------------------------

void EventTest::checkTest( const char * taskId ) {
  SpinLockGuard guard( m_lck );
  setTask( taskId );
  if (m_cache == NULLPTR) {
    PropertiesPtr pp = Properties::create();

    int32_t heapLruLimit = getIntValue( "heapLruLimit" );
    if( heapLruLimit > 0 )
    {
      pp->insert("heap-lru-limit",heapLruLimit);
    }
    cacheInitialize( pp );

    // EventTest specific initialization
    std::string val = getStringValue( "EventBB" );
    if ( !val.empty() ) {
      m_bb = val;
    }
  }
  clearFailCount();
}

// ----------------------------------------------------------------------------

int32_t checkResult( int32_t result ) {
  int32_t retVal = result;
  if ( ( retVal == FWK_SUCCESS ) && ( g_test->getFailCount() > 0 ) ) {
    retVal = FWK_SEVERE;
  }
  return retVal;
}

// ----------------------------------------------------------------------------

TESTTASK initialize( const char * initArgs ) {
  int32_t result = FWK_SUCCESS;
  if ( g_test == NULL ) {
    FWKINFO( "Initializing EventTest library." );
    testLock = new ACE_Thread_Mutex();
    balanceLock = new ACE_Thread_Mutex();
    keyLock = new ACE_Thread_Mutex();
    try {
      g_test = new EventTest( initArgs );
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
  FWKINFO( "Finalizing EventTest library." );
  if ( g_test != NULL ) {
    g_test->cacheFinalize();
    delete g_test;
    g_test = NULL;
  }
  if ( testLock != NULL ) {
    delete testLock;
    testLock = NULL;
  }
  if ( balanceLock != NULL ) {
    delete balanceLock;
    balanceLock = NULL;
  }
  if ( keyLock != NULL ) {
    delete keyLock;
    keyLock = NULL;
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doCacheClose( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCacheClose called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    g_test->cacheFinalize();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCacheClose caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

// This may not be needed.
TESTTASK doRemoveRootRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRemoveRootRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    RegionPtr region = g_test->getRandomRegion(true);
    g_test->removeRegion(region);
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRemoveRootRegion caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

// should not limited to be root.
TESTTASK doCreateRootRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateRootRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doCreateRootRegion();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateRootRegion caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doTwinkleRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doTwinkleRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    g_test->doTwinkleRegion();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doTwinkleRegion caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doEntryEvents( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doEntryEvents called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doEntryEvents();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doEntryEvents caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doEntryOperations( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doEntryOperations called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doEventOperations();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doEntryOperations caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doIterate( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doIterate called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doIterate();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doIterate caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doRegionOperations( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegionOperations called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doEventOperations();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegionOperations caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doVerifyKeyCount( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyKeyCount called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->verifyKeyCount();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyKeyCount caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doFeedEntries( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doFeedEntries called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->feedEntries();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doFeedEntries caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doAddEntry( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doAddEntry called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->addEntry();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doAddEntry caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doAddOrDestroyEntry( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doAddOrDestroyEntry called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->addOrDestroyEntry();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doAddOrDestroyEntry caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doValidateCacheContent( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doValidateCacheContent for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->validateCacheContent();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doValidateCacheContent caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doValidateRegionContent( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doValidateRegionContent called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->validateRegionContent();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doValidateRegionContent caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doIterateOnEntry( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doIterateOnEntry called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doIterateOnEntry();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doIterateOnEntry caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doGets( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGets called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doGets();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGets caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doMemoryMeasurement( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doMemoryMeasurement called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doMemoryMeasurement();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doMemoryMeasurement caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

TESTTASK doBasicTest( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doBasicTest called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doBasicTest();
  } catch ( FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doBasicTest caught exception: " << ex.getMessage() );
  }
  return checkResult( result );
}

// Factory functions for listeners, loaders, etc.
// ----------------------------------------------------------------------------

TEST_EXPORT CacheListener * createETCacheListener() {
  return new ETCacheListener( g_test );
}

// ----------------------------------------------------------------------------

TEST_EXPORT CacheWriter * createETCacheWriter() {
  return new ETCacheWriter( g_test );
}

// ----------------------------------------------------------------------------

TEST_EXPORT CacheLoader * createETCacheLoader() {
  return new ETCacheLoader( g_test );
}

// ----------------------------------------------------------------------------

void  EventTest::doEntryTest( const char* opcode )
{
  RegionPtr randomRegion = getRandomRegion( true );
  if (randomRegion == NULLPTR) {
    FWKEXCEPTION( "No region to operate on." );
    return;
  }
  try {
    {
      ACE_Guard<ACE_Thread_Mutex> guard( *balanceLock);
      VectorOfCacheableKey keys;
      randomRegion->keys( keys );
      int32_t maxKeys = getIntValue( "distinctKeys" );
      bool balanceEntries = getBoolValue( "balanceEntries" );
      if ( balanceEntries ) {
        if ( keys.size() >= maxKeys ) {
          FWKINFO( "Balancing cache content by destroying excess entries." );
          int32_t dcnt = 0;
          for ( uint32_t i = 100; i > 0; i-- ) {
            try {
              destroyObject( randomRegion, true );
              dcnt++;
            } catch( Exception& ignore ) { ignore.getMessage(); }
          }
          FWKINFO( "Cache balancing complete, did " << dcnt << " destroys." );
        } else if ( keys.size() == 0 ) {
          FWKINFO( "Balancing cache content by creating entries." );
          int32_t ccnt = 0;
          for ( uint32_t i = 100; i > 0; i-- ) {
            try {
              addObject( randomRegion );
              ccnt++;
            } catch( TimeoutException& e ) {
              incFailCount();
              FWKSEVERE( "Caught unexpected timeout exception during entry " << opcode
                << " operation: " << e.getMessage() << " continuing with test." );
            } catch( Exception& ignore ) { ignore.getMessage(); }
          }
          FWKINFO( "Cache balancing complete, did " << ccnt << " creates." );
        }
      }
    }
//    FWKINFO( "Executing op: " << opcode );
    if ( strcmp( "add", opcode ) == 0 ) {
      addObject( randomRegion );
    } else if ( strcmp( "update", opcode ) == 0 ) {
      updateObject( randomRegion );
    } else if ( strcmp( "invalidate", opcode ) == 0 ) {
      invalidateObject( randomRegion, false );
    } else if ( strcmp( "destroy", opcode ) == 0 ) {
      destroyObject( randomRegion, false );
    } else if ( strcmp( "read", opcode ) == 0 ) {
      readObject( randomRegion);
    } else if ( strcmp( "localInvalidate", opcode ) == 0 ) {
      invalidateObject( randomRegion,true );
    } else if ( strcmp( "localDestroy", opcode ) == 0 ) {
      destroyObject( randomRegion,true );
    } else {
      FWKSEVERE( "Invalid operation specified: " << opcode );
    }
//    FWKINFO( "Completed op: " << opcode );

  } catch( TimeoutException& e ) {
    incFailCount();
    FWKSEVERE( "Caught unexpected timeout exception during entry " << opcode
      << " operation: " << e.getMessage() << " continuing with test." );
  } catch( EntryExistsException& e ) {
    // the entry has already been added.
    handleExpectedException( e );
  } catch( EntryNotFoundException &e ) {
    // the entry could be already destroyed.
    handleExpectedException( e );
  } catch( EntryDestroyedException &e ) {
    // the region could be already destroyed.
    handleExpectedException( e );
  } catch( RegionDestroyedException &e ) {
    // the region could be already destroyed.
    handleExpectedException( e );
  } catch( IllegalArgumentException &e ) {
    // the region could be already destroyed.
    handleExpectedException( e );
  } catch( Exception &e ) {
    FWKSEVERE( "Caught unexpected exception during entry " << opcode
      << " operation: " << e.getMessage() << " continuing with test." );
  }
}

void  EventTest::doRegionTest(const char* opcode, int iMaxRegions)
{
  RegionPtr randomRegion;
  try {
    {
      ACE_Guard<ACE_Thread_Mutex> guard( *balanceLock);
      int32_t iRegionCount = getAllRegionCount();
      if ( iRegionCount >= iMaxRegions ) {
        while ( iRegionCount > iMaxRegions / 2 ) {
          try {
            randomRegion = getRandomRegion( true );
            if(randomRegion == NULLPTR) {
              FWKEXCEPTION("expected to get a valid random region, get a null region instead");
            }
            else {
            destroyRegion( randomRegion, false );
            }
            iRegionCount = getAllRegionCount();
          } catch( Exception& ignore ) { ignore.getMessage(); }
        }
      } else if ( iRegionCount <= 0 ) {
        for ( int32_t i = iMaxRegions / 2; i > 0; i-- ) {
          try {
            addRegion();
          } catch( Exception& ignore ) { ignore.getMessage(); }
        }
      }
    }
    randomRegion = getRandomRegion( true );
    if (randomRegion == NULLPTR) {
      //need to create a region
      opcode = "addRegion";
    }
//    FWKINFO( "Do region test: " << opcode );
    if ( strcmp( "addRegion", opcode ) == 0 ) {
      addRegion();
    } else if ( strcmp( "invalidateRegion", opcode ) == 0 ) {
      invalidateRegion( randomRegion, false );
    } else if ( strcmp( "destroyRegion", opcode ) == 0 ) {
      destroyRegion( randomRegion, false );
    } else if ( strcmp( "localInvalidateRegion", opcode ) == 0 ) {
      invalidateRegion( randomRegion, true );
    } else if ( strcmp( "localDestroyRegion", opcode ) == 0 ) {
      destroyRegion( randomRegion, true );
    } else {
      FWKSEVERE( "Invalid operation specified: " << opcode );
    }
  } catch( RegionExistsException e ) {
    handleExpectedException( e );
  } catch( EntryExistsException e ) {
    handleExpectedException( e );
  } catch( RegionDestroyedException e ) {
    handleExpectedException( e );
  } catch( Exception &e ) {
    e.printStackTrace();
    FWKSEVERE( "Caught unexpected exception during region " << opcode
      << " operation: " << e.getMessage() << "continuing with test." );
  }
}

const std::string EventTest::getNextRegionName(RegionPtr& regionPtr)
{
  std::string regionName;
  int count = 0;
  std::string path;
  do {
    path = getStringValue( "regionPaths" );
    if ( path.empty() ) {
      FWKEXCEPTION( "No regionPaths defined in the xml file. Needed for region event test" );
    }
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

void  EventTest::removeRegion(RegionPtr & region)
{
  try {
    std::string nam = region->getFullPath();
    FWKINFO( "In removeRegion, local destroy on " << nam );
    region->localDestroyRegion();
    //    FWKINFO( "In removeRegion, local destroy complete on " << nam );
    bbDecrement( m_bb, nam );
  } catch (RegionDestroyedException e) {
    // the region could be already destroyed.
    handleExpectedException( e );
  } catch ( Exception ex ) {
    FWKEXCEPTION( "Caught unexpected exception during region local destroy: " <<
      ex.getMessage() );
  }
}

// ----------------------------------------------------------------------------

void EventTest::doTwinkleRegion() {
  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 10 : secondsToRun;
//  FWKINFO( "Seconds to run: " << secondsToRun );

  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  bool done = false;
  bool regionDestroyed = false;
  int32_t errCnt = 0;

  while ( !done ) {
    int32_t sleepTime = getIntValue( "sleepTime" );
    sleepTime = ( ( sleepTime < 1 ) || regionDestroyed ) ? 10 : sleepTime;
//    FWKINFO( "sleepTime is " << sleepTime << " seconds." );

    ACE_Time_Value now = ACE_OS::gettimeofday();
    ACE_Time_Value pause( sleepTime );
    if ( ( now > end ) || ( ( now + pause ) > end ) ) {
//      FWKINFO( "Exiting loop, time is up." );
      done = true;
      continue;
    }

    FWKINFO( "EventTest::doTwinkleRegion() sleeping for " << sleepTime << " seconds." );
    perf::sleepSeconds( sleepTime );

    if ( regionDestroyed ) {
      FWKINFO( "EventTest::doTwinkleRegion() will create a region." );
      createRootRegion();
      regionDestroyed = false;
      FWKINFO( "EventTest::doTwinkleRegion() region created." );
      int32_t percentDiff = percentDifferent();
      if ( percentDiff > 10 ) {
        errCnt++;
        FWKSEVERE( "Actual number of keys is not within 10% of expected." );
      }
    } else {
      FWKINFO( "EventTest::doTwinkleRegion() will destroy a region." );
      RegionPtr regionPtr = getRandomRegion( true );
      if (regionPtr != NULLPTR) {
        regionPtr->localDestroyRegion();
        regionPtr = NULLPTR;
      }
      regionDestroyed = true;
      FWKINFO( "EventTest::doTwinkleRegion() local region destroy is complete." );
    }
  } // while

  if ( regionDestroyed ) {
    createRootRegion();
    FWKINFO( "EventTest::doTwinkleRegion() region created." );
  }

  FWKINFO( "EventTest::doTwinkleRegion() completed." );
  if ( errCnt > 0 ) {
    FWKEXCEPTION( "Region key count was out of bounds on " << errCnt << " region creates." );
  }
}

// ----------------------------------------------------------------------------

int32_t EventTest::doMemoryMeasurement() {
  // we set to success if we match counts below, so we start set to error
  int32_t fwkResult = FWK_SEVERE;
  destroyAllRegions();
  int32_t workTime = getTimeValue( "workTime" );
  if ( workTime <= 0 )
    workTime = DEFAULT_WORK_TIME;
  int iMaxRegions = getIntValue( "maxRegions" );
  iMaxRegions = (iMaxRegions > 0) ? iMaxRegions: DEFAULT_MAX_REGION;

  ACE_Time_Value endTime = ACE_OS::gettimeofday() +
    ACE_Time_Value( workTime, 0 );

  std::ostringstream results;

  try {
    ACE_Time_Value now;
    double vs = 0, rs = 0;
    FWKINFO(" Start measuring memory consumption for region creation and destruction");

    //do {
    for (int i = 0; i < 4; ++i) {
       measureMemory("Before region creation: ", vs, rs);
      // Create some root regions
      for (int i = 0; i < iMaxRegions; ++i) {
        createRootRegion();
      }
      measureMemory("After region creation: ", vs, rs);
      destroyAllRegions();
 //     measureMemory("After region destruction: ", vs, rs);

      now = ACE_OS::gettimeofday();
    }
    // }while (now < endTime );
    results << "Region Specification: " <<
      getStringValue("regionSpec") << "\n"
      << "Memory measurement for 10 regions: " << "VmSize: " << vs
      << " Mb   RSS: " << rs << " Mb\n";

    FWKINFO(" End measuring memory consumption for region creation and destruction");

    // Create one region
    createRootRegion();
    VectorOfRegion regions;
    m_cache->rootRegions(regions);
    int iRootSize = regions.size();
    if (iRootSize != 1) {
      FWKSEVERE(" Expected one root region. Found none");
      return FWK_SEVERE;
    }

    RegionPtr region = regions.at(0);
    std::vector<int32_t> keyCounts, sizeCounts;

    int32_t numKeys = getIntValue( "distinctKeys" );
    while (numKeys > 0) {
      keyCounts.push_back(numKeys);
      numKeys = getIntValue( "distinctKeys" );
    }

    int32_t valueSize = getIntValue( "valueSizes");
    while (valueSize > 0) {
      sizeCounts.push_back(valueSize);
      valueSize = getIntValue( "valueSizes");
    }

    FWKINFO(" Start measuring memory consumption for entry creation and destruction");
    results << "Keys,     " << "Size,       " << "VmSize,       " << "RSS\n";

    for (size_t kCount = 0; kCount < keyCounts.size(); ++kCount) {
      for (size_t sCount = 0; sCount < sizeCounts.size(); ++sCount) {
        endTime = ACE_OS::gettimeofday() + ACE_Time_Value( workTime, 0 );
        now = ACE_OS::gettimeofday();
        FWKINFO(keyCounts[kCount] << ", " << sizeCounts[sCount]);
        //do {
        for (int i = 0; i < 4; ++i) {
          measureMemory("Before entry creation: ", vs, rs);
          for (int k = 0; k < keyCounts[kCount]; ++k) {
            std::string sKey = "K_";
            sKey += FwkStrCvt(k ).toString();
            CacheableKeyPtr keyPtr = CacheableKey::create( sKey.c_str() );
            if (keyPtr == NULLPTR){
              FWKSEVERE("EventTest::doMemoryMeasurement null keyPtr generated.");
              return FWK_SEVERE;
            } else {
              const std::string sValue = GsRandom::getAlphanumericString( sizeCounts[sCount] );
              CacheableBytesPtr valuePtr = CacheableBytes::create( ( const unsigned char * )sValue.c_str(), static_cast<int32_t>(sValue.length()) );
              region->create( keyPtr, valuePtr );
            }
          }

          measureMemory("After entry creation: ", vs, rs);

          {
            VectorOfCacheableKey keys;
            region->keys(keys);
            if ((int32_t)keys.size() != keyCounts[kCount]) {
              FWKEXCEPTION("EventTest::doMemoryMeasurement: Number of entries in the region is not as expected!");
            }
            for (int k = 0; k < keyCounts[kCount]; ++k) {
              region->destroy( keys.at(k));
            }
          }
          //           measureMemory("After entry destruction: ", vs, rs);
          now = ACE_OS::gettimeofday();
        }
        //} while (now < endTime );
        results << keyCounts[kCount] << ", " << sizeCounts[sCount] << ", " << vs << ", " << rs << "\n";

      }
    }
    FWKINFO(" End measuring memory consumption for entry creation and destruction");
    FWKINFO (results.str().c_str());
    fwkResult = FWK_SUCCESS;
  }catch (FwkException e) {
    FWKSEVERE("doMemoryMeasurement caught FwkException: " <<
      e.getMessage());
    fwkResult = FWK_SEVERE;
  }catch (Exception e) {
    FWKSEVERE("doMemoryMeasurement caught Exception: " <<
      e.getMessage());
    fwkResult = FWK_SEVERE;
  }catch (std::exception e) {
    FWKSEVERE("doMemoryMeasurement caught std::exception: " <<
      e.what());
    fwkResult = FWK_SEVERE;
  }
  return fwkResult;
}
// ----------------------------------------------------------------------------

int32_t EventTest::doCreateRootRegion()
{
  int32_t fwkResult = FWK_SUCCESS;

  try{
    std::string key( "ROOT_REGION_INDEX" );
    bbIncrement( m_bb, key );
    std::string name = getStringValue( "regionName" );
    createRootRegion( name.c_str() );
  } catch (FwkException e) {
    FWKSEVERE("doCreateRootRegion caught FwkException: " <<
      e.getMessage());
    fwkResult = FWK_SEVERE;
  } catch (Exception e) {
    FWKSEVERE("doCreateRootRegion caught Exception: " <<
      e.getMessage());
    fwkResult = FWK_SEVERE;
  } catch (std::exception e) {
    FWKSEVERE("doCreateRootRegion caught std::exception: " <<
      e.what());
    fwkResult = FWK_SEVERE;
  }

  return fwkResult;
}

// ---------------------------------------------------------------------------
int32_t EventTest::validateRegionContent()
{
  int32_t fwkResult = FWK_SUCCESS;
  FWKINFO("validateRegionContent()");

  try{
    const std::string testRegionName =  getStringValue( "testRegion" ) ;
    const std::string validateRegionName = getStringValue( "validateRegion" );
    const std::string regionName = getStringValue( "regionName" );
    RegionPtr testRegionPtr = m_cache->getRegion( testRegionName.c_str() );
    RegionPtr validateRegionPtr = m_cache->getRegion( validateRegionName.c_str() );

    FWKINFO("localDestroyRegion region name is " << testRegionPtr->getName());
    // destroy the region
    testRegionPtr->localDestroyRegion();
    fwkResult = doCreateRootRegion();
    RegionPtr regionPtr = NULLPTR;
    if(fwkResult == FWK_SUCCESS){
      regionPtr = m_cache->getRegion( regionName.c_str() );
      FWKINFO(" Recreated Region name is " << regionPtr->getName());
    }

    VectorOfCacheableKey keyVector;
    VectorOfCacheableKey keyVectorValidateRegion;

    regionPtr->keys(keyVector);
    validateRegionPtr->keys(keyVectorValidateRegion);
    uint32_t ulKeysInRegion = keyVector.size();
    uint32_t ulKeysInValidateRegion = keyVectorValidateRegion.size();
    if( ulKeysInRegion != ulKeysInValidateRegion)
    {
      FWKINFO("Region Key count is not equal, Region " << regionPtr->getName() << " key count is "<< ulKeysInRegion << " and Region " <<
         validateRegionPtr->getName() << " key count is "<< ulKeysInValidateRegion);
      fwkResult = FWK_SEVERE;
    }

    CacheableStringPtr keyPtr;
    CacheableBytesPtr valuePtr;

    uint32_t entryPassCnt = 0;
    uint32_t entryFailCnt = 0;
    for(uint32_t ulIndex = 0; ulIndex < ulKeysInRegion; ulIndex++){
      keyPtr = dynCast<CacheableStringPtr>( keyVector.at(ulIndex) );
      valuePtr = dynCast<CacheableBytesPtr>( regionPtr->get(keyPtr) );

      if (TestEntryPropagation(validateRegionPtr, keyPtr, valuePtr))
  entryFailCnt++;
      else
  entryPassCnt++;
    }
    FWKINFO("entryFailCnt is " << entryFailCnt << " entryPassCnt is " << entryPassCnt);
    if (entryFailCnt == 0){
      FWKINFO("validateRegionContent() - TEST ENDED, RESULT = SUCCESSFUL ");
    }else{
      FWKSEVERE("validateRegionContent() - TEST ENDED, RESULT = FAILED ");
      fwkResult = FWK_SEVERE;
    }
  }
  catch (Exception& e) {
    FWKEXCEPTION("validateRegionContent encountered exception: " << e.getMessage());
    fwkResult = FWK_SEVERE;
  }
  catch (std::exception& e) {
    FWKSEVERE("validateRegionContent caught std::exception: " <<
    e.what());
    fwkResult = FWK_SEVERE;
  }
  return fwkResult;
}
// ----------------------------------------------------------------------------

bool EventTest::TestNetSearch(RegionPtr& regionPtr, CacheableStringPtr& pszKey,
                              CacheableBytesPtr& pszValue)
{
  bool bSuccess = true;

  try {
    CacheableBytesPtr  valuePtr;
    valuePtr = dynCast<CacheableBytesPtr>( regionPtr->get(pszKey) );
    if (valuePtr == NULLPTR){
      FWKSEVERE("Key: " << pszKey->asChar() << " should found in region " <<
        regionPtr->getFullPath() << ", doing netsearch");
      bSuccess = false;
    }else{
      if ( strcmp( ( const char * )valuePtr->value(), ( const char * )pszValue->value() ) ){ // check the value
        FWKSEVERE("Key: " << pszKey->asChar() << " has invalid value in region " <<
          regionPtr->getFullPath() << ", Value in put: " << pszValue->value() <<
          " Value in get: " << valuePtr->value());
        bSuccess = false;
      }
    }
  } catch (Exception e) {
    FWKEXCEPTION("TestNetSearch encountered exception: " << e.getMessage());
  }

  return bSuccess;
}

// ----------------------------------------------------------------------------

int32_t EventTest::validateCacheContent()
{
  int32_t fwkResult = FWK_SUCCESS;
  FWKINFO("validateCacheContent()");

  try{
    const std::string testRegionName =  getStringValue( "testRegion" ) ;
    const std::string validateRegionName = getStringValue( "validateRegion" );
    RegionPtr testRegionPtr = m_cache->getRegion( testRegionName.c_str() );
    RegionPtr validateRegionPtr = m_cache->getRegion( validateRegionName.c_str() );

    VectorOfCacheableKey keyVector;

    testRegionPtr->keys(keyVector);
    uint32_t ulKeysInRegion = keyVector.size();
    if (ulKeysInRegion == 0) {
      fwkResult = FWK_SEVERE;
    }

    CacheableStringPtr keyPtr;
    CacheableBytesPtr    valuePtr;

    uint32_t entryPassCnt = 0;
    uint32_t entryFailCnt = 0;
    for(uint32_t ulIndex = 0; ulIndex < ulKeysInRegion; ulIndex++){
      keyPtr = dynCast<CacheableStringPtr>( keyVector.at(ulIndex) );
      valuePtr = dynCast<CacheableBytesPtr>( testRegionPtr->get(keyPtr) );

      if (TestEntryPropagation(validateRegionPtr, keyPtr, valuePtr))
  entryFailCnt++;
      else
  entryPassCnt++;
    }
    FWKINFO("entryFailCnt is " << entryFailCnt << " entryPassCnt is " << entryPassCnt);
    if (entryFailCnt == 0){
      FWKINFO("validateCacheContent() - TEST ENDED, RESULT = SUCCESSFUL ");
    }else{
      FWKSEVERE("validateCacheContent() - TEST ENDED, RESULT = FAILED ");
      fwkResult = FWK_SEVERE;
    }
  }
  catch (Exception& e) {
    FWKEXCEPTION("validateCacheContent encountered exception: " << e.getMessage());
    fwkResult = FWK_SEVERE;
  }
  catch (std::exception& e) {
    FWKSEVERE("validateCacheContent caught std::exception: " <<
    e.what());
    fwkResult = FWK_SEVERE;
  }
  return fwkResult;
}

// ------------------------------------------------------------------------------

bool EventTest::TestEntryPropagation(RegionPtr& regionPtr,
                                       CacheableStringPtr& szKey, CacheableBytesPtr& szValue)
{
 /* FWKINFO("TestEntryPropagation() Values for Region " <<
    regionPtr->getFullPath() << " " <<
    RegionHelper::regionTag(regionPtr->getAttributes()));*/


  bool bEntryError = false;
  bool bContainsKey = false;
  bool bContainsValue = false;

    bContainsKey = regionPtr->containsKey( szKey );
    bContainsValue = regionPtr->containsValueForKey( szKey );
      if ( !bContainsKey || !bContainsValue){
        FWKSEVERE("Key: " << szKey->asChar() << " not found in region " <<
          regionPtr->getFullPath());
        bEntryError = true;
      }
      else if(!TestNetSearch(regionPtr,szKey,szValue)){
        bEntryError = true;
      }

 return bEntryError;
}

// ----------------------------------------------------------------------------

int32_t EventTest::doIterate()
{
  int32_t fwkResult = FWK_SUCCESS;
  FWKINFO("doIterate()");

  uint32_t ulKeysInRegion;
  uint32_t ulNoneNullValuesInRegion;
  std::string sError;

  VectorOfRegion rootRegionVector;
  RegionPtr rootRegionPtr;
  RegionAttributesPtr attrPtr;

  try{

    m_cache->rootRegions(rootRegionVector);

    uint32_t ulRegionCount = rootRegionVector.size();

    for(uint32_t ulIndex = 0; ulIndex < ulRegionCount; ulIndex++){
      rootRegionPtr = rootRegionVector.at(ulIndex);

      attrPtr = rootRegionPtr->getAttributes();

      bool bHasInvalidateAction =
        (attrPtr->getEntryIdleTimeoutAction() ==
        ExpirationAction::INVALIDATE) ||
        (attrPtr->getEntryTimeToLiveAction() ==
        ExpirationAction::INVALIDATE);

      iterateRegion(
        rootRegionPtr, true, bHasInvalidateAction,
        ulKeysInRegion, ulNoneNullValuesInRegion, sError );

      if (sError.size() > 0)
        FWKEXCEPTION(sError);
    }
  }
  catch (FwkException e) {
    FWKSEVERE("doIterate caught FwkException: " <<
      e.getMessage());
    fwkResult = FWK_SEVERE;
  }
  catch (Exception e) {
    FWKSEVERE("doIterate caught Exception: " <<
      e.getMessage());
    fwkResult = FWK_SEVERE;
  }
  catch (std::exception e) {
    FWKSEVERE("doIterate caught std::exception: " <<
      e.what());
    fwkResult = FWK_SEVERE;
  }

  return fwkResult;
}

int32_t EventTest::doIterateOnEntry()
{
  int32_t fwkResult = FWK_SUCCESS;
  FWKINFO("doIterateOnEntry()");

  try {
    const std::string testRegionName =  getStringValue( "testRegion" );
    const std::string validateRegionName = getStringValue( "validateRegion" );
    RegionPtr testRegionPtr = m_cache->getRegion( testRegionName.c_str() );
    RegionPtr validateRegionPtr = m_cache->getRegion( validateRegionName.c_str() );

    VectorOfCacheableKey keyVector;
    uint32_t keysInRegion = 1;
    uint32_t lastCount = 0;
    uint32_t tryCount = 30;
    uint32_t tries = 0;

    while ( ( keysInRegion != lastCount ) && ( tries++ < tryCount ) ) {
      perf::sleepSeconds( 10 );
      lastCount = keysInRegion;
      testRegionPtr->keys( keyVector );
      keysInRegion = keyVector.size();
    }

    if ( ( keysInRegion == 0 ) || ( tries >= tryCount ) ) {
      FWKEXCEPTION( "After " << tries << " tries, counted " << keysInRegion << " keys in the region." );
    }

    FWKINFO( "After " << tries << " tries, counted " << keysInRegion << " keys in the region." );

    CacheableKeyPtr keyPtr;
    CacheablePtr valuePtr;

    for ( uint32_t index = 0; index < keysInRegion; index++ ) {
      keyPtr = keyVector.at( index );
      valuePtr = testRegionPtr->get( keyPtr );
      validateRegionPtr->create( keyPtr, valuePtr );
    }
  } catch( FwkException& e ) {
    FWKSEVERE( "doIterateOnEntry caught FwkException: " << e.getMessage() );
    fwkResult = FWK_SEVERE;
  } catch( Exception& e ) {
    FWKSEVERE( "doIterateOnEntry caught Exception: " << e.getMessage() );
    fwkResult = FWK_SEVERE;
  } catch( std::exception& e ) {
    FWKSEVERE( "doIterateOnEntry caught std::exception: " << e.what() );
    fwkResult = FWK_SEVERE;
  }

  return fwkResult;
}

// ----------------------------------------------------------------------------

void  EventTest::entryEvent( const char* opcode )
{
  bool distinctKeys = getBoolValue( "useDistinctKeys" );
  static uint32_t keyCounter = 0;
  char * key = NULL;
  char keyBuffer[32];
  if ( distinctKeys ) {
    sprintf( keyBuffer, "%d_%u", getClientId(), ++keyCounter );
    key = keyBuffer;
  }

  RegionPtr randomRegion = getRandomRegion( true );
  if (randomRegion == NULLPTR) {
    FWKEXCEPTION( "No region to operate on." );
  }
  try {
    if ( strcmp( "add", opcode ) == 0 ) {
      addObject( randomRegion, key );
    } else if ( strcmp( "update", opcode ) == 0 ) {
      updateObject( randomRegion );
    } else if ( strcmp( "invalidate", opcode ) == 0 ) {
      invalidateObject( randomRegion, false );
    } else if ( strcmp( "destroy", opcode ) == 0 ) {
      destroyObject( randomRegion, false );
    } else if ( strcmp( "read", opcode ) == 0 ) {
      readObject( randomRegion);
    } else if ( strcmp( "localInvalidate", opcode ) == 0 ) {
      invalidateObject( randomRegion,true );
    } else if ( strcmp( "localDestroy", opcode ) == 0 ) {
      destroyObject( randomRegion,true );
    } else {
      FWKSEVERE( "Invalid operation specified: " << opcode );
    }
  } catch( TimeoutException& e ) {
    incFailCount();
    FWKSEVERE( "Caught unexpected timeout exception during entry " << opcode
      << " operation: " << e.getMessage() << " continuing with test." );
  } catch( EntryExistsException& e ) {
    // the entry has already been added.
    handleExpectedException( e );
  } catch( EntryNotFoundException &e ) {
    // the entry could be already destroyed.
    handleExpectedException( e );
  } catch( EntryDestroyedException &e ) {
    // the region could be already destroyed.
    handleExpectedException( e );
  } catch( RegionDestroyedException &e ) {
    // the region could be already destroyed.
    handleExpectedException( e );
  } catch( IllegalArgumentException &e ) {
    // the region could be already destroyed.
    handleExpectedException( e );
  } catch( Exception &e ) {
    FWKSEVERE( "Caught unexpected exception during entry " << opcode
      << " operation: " << e.getMessage() << " continuing with test." );
  }
}

// ----------------------------------------------------------------------------

int32_t EventTest::doEntryEvents()
{
  int32_t fwkResult = FWK_SUCCESS;
  FWKINFO("doEntryEvents()");
  uint32_t counter = 0;

  int32_t workTime = getTimeValue( "workTime" );
  FWKINFO( "doEntryEvents will work for " << workTime << " seconds. " );

  const ACE_Time_Value endTime = ACE_OS::gettimeofday() +
    ACE_Time_Value( workTime, 0 );

  int32_t opsSecond = getIntValue( "opsSecond" );
  opsSecond = ( opsSecond > 0 ) ? opsSecond : 0;
  PaceMeter meter( opsSecond );

  int32_t opCount = 0;
  std::string key( "CURRENT_OPS_COUNT" );

  try {
    FWKINFO( "doEntryEvents(): Entering event loop." );
    ACE_Time_Value now;
    do {
      std::string opcode = getStringValue( "entryOps" );
      if ( !opcode.empty() ) {
        entryEvent( opcode.c_str() );
        opCount++;
        meter.checkPace();
        counter++;
        if ( ( counter % 1000 ) == 0 ) {
          FWKINFO( "Performed " << counter << " operations." );
        }

        bbIncrement( m_bb, key );
      }
      else {
        FWKSEVERE( "NULL operation specified." );
      }
      now = ACE_OS::gettimeofday();
    } while ( now < endTime );
  } catch ( FwkException e ) {
    FWKSEVERE( "doEntryEvents caught FwkException: " << e.getMessage() );
    fwkResult = FWK_SEVERE;
  } catch ( Exception e ) {
    FWKSEVERE( "doEntryEvents caught Exception: " << e.getMessage() );
    fwkResult = FWK_SEVERE;
  } catch ( std::exception e ) {
    FWKSEVERE( "doEntryEvents caught std::exception: " << e.what() );
    fwkResult = FWK_SEVERE;
  }

  FWKINFO( "doEntryEvents() performed " << counter << " operations." );
  return fwkResult;
}

// ----------------------------------------------------------------------------

int32_t EventTest::doEventOperations()
{
  int32_t fwkResult = FWK_SUCCESS;
  //  FWKINFO("doEventOperations()");
  uint32_t counter = 0;

  static char * taskID = strdup( "begin" );
  // Clear up everything from previous test.
  // Make sure we have one root region.
  {
//  FWKINFO( "DBG doEventOperations try to acquire lock" );
    ACE_Guard<ACE_Thread_Mutex> guard( *testLock);
//  FWKINFO( "DBG doEventOperations check id" );
    if ( ( taskID != NULL ) && ( getTaskId() != taskID ) ) {
//  FWKINFO( "DBG doEventOperations remove regions" );
      destroyAllRegions();
//  FWKINFO( "DBG doEventOperations create region" );
      fwkResult = doCreateRootRegion();
//  FWKINFO( "DBG doEventOperations free id" );
      if ( taskID != NULL ) {
        free( taskID );
      }
//  FWKINFO( "DBG doEventOperations set id" );
      taskID = strdup( getTaskId().c_str() );
    }
//  FWKINFO( "DBG doEventOperations release lock" );
  }
  int32_t workTime = getTimeValue( "workTime" );
  FWKINFO( "doEventOperations will work for " << workTime << " seconds. " );

  int32_t skipCounter = getIntValue( "skipCount" );
  skipCounter = ( skipCounter > 0 ) ? skipCounter : 100;

  int iMaxRegions = getIntValue( "maxRegions" );
  iMaxRegions = (iMaxRegions > 0) ? iMaxRegions: DEFAULT_MAX_REGION;

  const ACE_Time_Value endTime = ACE_OS::gettimeofday() +
    ACE_Time_Value( workTime, 0 );

  int32_t opsSecond = getIntValue( "opsSecond" );
  opsSecond = ( opsSecond > 0 ) ? opsSecond : 0;
  PaceMeter meter( opsSecond );

  int32_t logSize = getIntValue( "logSize" );

  int32_t opCount = 0;

  try {
    ACE_Time_Value now;
    std::string opcode;
    bool isDone = false;
    FWKINFO( "Entering event loop." );
    do{
      if ( logSize == 1 ) {
        char buf[128];
        int32_t cnt = g_test->getRegionCount();
        sprintf( buf, "%d %s  %d operations.", cnt, ( ( cnt == 1 ) ? "region" : "regions" ), opCount );
        perf::logSize( buf );
      }
      int32_t randomOP = getIntValue("randomOP");
      if ( randomOP == 5 ) {
        opcode = getStringValue( "regionOps" );
      }
      else {
        opcode = getStringValue( "entryOps" );
      }
      if ( !opcode.empty() ) {
        bool skipTest = false;
        if ( opcode == "abort" ) {
          skipTest = true;
          if ( --skipCounter == 0 ) {
            char * segv = NULL;
            strcpy( segv, "Forcing segv" );
          }
        }else if ( opcode == "exit" ) {
          skipTest = true;
          if ( --skipCounter == 0 ) {
            exit( 0 );
          }
        }else if ( opcode == "done" ) {
          skipTest = true;
          if ( --skipCounter == 0 ) {
            isDone = true;
          }
        }

        if (!skipTest) {
//          FWKINFO( "Execution of op: " << opcode << " begins." );
          if (randomOP == 5) {
            doRegionTest( opcode.c_str(), iMaxRegions );
          } else {
            doEntryTest( opcode.c_str() );
          }
//          FWKINFO( "Execution of op: " << opcode << " ends." );
          opCount++;
          meter.checkPace();
        }
        counter++;
        if ( ( counter % 1000 ) == 0 ) {
          FWKINFO( "Performed " << counter << " operations." );
        }

        std::string key( "CURRENT_OPS_COUNT" );
        bbIncrement( m_bb, key );
      }else {
        FWKSEVERE( "NULL operation specified."  << "randomOP: " << randomOP);
      }
      now = ACE_OS::gettimeofday();
    } while ( ( now < endTime ) && !isDone );
    FWKINFO( "Event loop complete." );
  }catch (FwkException e) {
    FWKSEVERE("doEventOperations caught FwkException: " <<
      e.getMessage());
    fwkResult = FWK_SEVERE;
  }catch (Exception e) {
    FWKSEVERE("doEventOperations caught Exception: " <<
      e.getMessage());
    fwkResult = FWK_SEVERE;
  }catch (std::exception e) {
    FWKSEVERE("doEventOperations caught std::exception: " <<
      e.what());
    fwkResult = FWK_SEVERE;
  }

  FWKINFO( "doEventOperations() performed " << counter << " operations." );
  return fwkResult;
}

// ----------------------------------------------------------------------------

void EventTest::addRegion()
{
  RegionPtr parentRegionPtr = NULLPTR;
  const std::string sRegionName = getNextRegionName(parentRegionPtr);
  if (sRegionName.empty()) {
    // nothing to do
    return;
  }

  RegionPtr region;
FWKINFO( "In addRegion, enter create region " << sRegionName );
  if (parentRegionPtr == NULLPTR) {
    RegionHelper regionHelper( g_test );
    region = regionHelper.createRootRegion( m_cache, sRegionName ) ;
  } else {

    std::string fullName = parentRegionPtr->getFullPath();
    RegionAttributesPtr atts;
    atts = parentRegionPtr->getAttributes();
    AttributesFactory fact(atts);
    atts = fact.createRegionAttributes();
    region = parentRegionPtr->createSubregion( sRegionName.c_str(), atts );
    std::string bb( "CppEventRegions" );
    bbSet( bb, sRegionName, fullName );
  }

  int iInitRegionNumObjects = getIntValue( "initRegionNumObjects" );
  // Create objects in the new region
  for(int iIndex = 0; iIndex < iInitRegionNumObjects; iIndex++) {
    std::string skey = FwkStrCvt( iIndex ).toString();
    addObject(region, skey.c_str());
  }
  FWKINFO( "In addRegion, exit create region " << sRegionName );
}

// ----------------------------------------------------------------------------

void EventTest::invalidateRegion(RegionPtr &randomRegion, bool bIsLocalInvalidate)
{
  // invalidate the region
  int64_t iSubRegionCount = getSubRegionCount( randomRegion ) + 1;
  FWKINFO( "In invalidateRegion, enter invalidate region " << randomRegion->getName() );

  const char * pszCounterName = "LOCAL_REGION_INVALIDATE_COUNT";
  if (bIsLocalInvalidate) {
    randomRegion->localInvalidateRegion();
  } else {
    pszCounterName = "REGION_INVALIDATE_COUNT";
    randomRegion->invalidateRegion();
  }
  bbAdd( m_bb, pszCounterName, iSubRegionCount );
  FWKINFO( "In invalidateRegion, exit invalidate region " << randomRegion->getName() );
}

// ----------------------------------------------------------------------------

void EventTest::destroyRegion(RegionPtr &randomRegion, bool bIsLocalDestroy)
{
  // destroy the region
  FWKINFO( "In destroyRegion, enter destroy region " << randomRegion->getName() );
  if (bIsLocalDestroy) {
    randomRegion->localDestroyRegion();
  } else {
    randomRegion->destroyRegion();
  }
  //FWKINFO( "In destroyRegion, exit destroy region " << randomRegion->getName() );
}

// ----------------------------------------------------------------------------

void EventTest::createRootRegion( const char * regionName )
{
  FWKINFO( "In createRootRegion region" );
  RegionPtr rootRegion;
  try {
    RegionHelper regionHelper( g_test );
    if ( regionName == NULL ) {
      rootRegion = regionHelper.createRootRegion( m_cache );
    }
    else {
      rootRegion = regionHelper.createRootRegion( m_cache, regionName );
    }

    bbIncrement( m_bb, rootRegion->getFullPath() );
    std::string key( "ROOT_REGION_COUNT" );
    bbIncrement( m_bb, key );

    FWKINFO( "In createRootRegion, Created root region: " << rootRegion->getFullPath() );
  } catch ( Exception &ex ) {
    FWKSEVERE( "Caught unexpected exception during region creation: " <<
      ex.getMessage() );
  }
}

// ----------------------------------------------------------------------------


CacheableKeyPtr EventTest::findKeyNotInCache( RegionPtr& regionPtr ) {
  CacheableKeyPtr keyPtr;
  if ( m_keysVec.size() == 0 ) {
    ACE_Guard<ACE_Thread_Mutex> guard( *keyLock);
    m_numKeys = getIntValue( "distinctKeys" );
    for ( int32_t i = 0; i < m_numKeys; i++ ) {
      std::string skey = FwkStrCvt( i ).toString();
      keyPtr = CacheableKey::create( skey.c_str() );
      m_keysVec.push_back( keyPtr );
    }
  }
  keyPtr = NULLPTR;
  // Find one that doesn't already exist
  int32_t start = GsRandom::random( m_keysVec.size() );
  bool wrapped = false;
  int32_t cur = start;
  while ( ( cur != start ) || !wrapped ) {
    if ( cur >= m_keysVec.size() ) {
      cur = 0;
      wrapped = true;
    }
    else {
      if ( !regionPtr->containsKey( m_keysVec.at( cur ) ) ) {
        keyPtr = m_keysVec.at( cur );
        cur = start;
        wrapped = true;
      }
      else {
        cur++;
      }
    }
  }
  return keyPtr;
}

// ----------------------------------------------------------------------------

void EventTest::addObject( RegionPtr& regionPtr, const char * key, const char * value )
{
  std::string sValue;
  CacheableKeyPtr keyPtr;
  if ( key == NULL ) {
    keyPtr = findKeyNotInCache( regionPtr );
  }
  else {
//    FWKINFO( "AddObject(): will use key: " << key );
    keyPtr = CacheableKey::create( key );
  }

  if (keyPtr == NULLPTR) {
//    FWKINFO( "EventTest::addObject null keyPtr generated for " << key );
    return;
  }

  if ( value == NULL ) {
    // get value size
    int32_t vsize = getIntValue( "valueSizes" );
    if ( vsize < 0 ) {
      vsize = 1000;
    }
    sValue = GsRandom::getAlphanumericString( vsize );
    value = sValue.c_str();
    //FWKINFO( "AddObject(): will use value size: " << vsize );
  }

  CacheableBytesPtr valuePtr = CacheableBytes::create( ( const unsigned char * )value, static_cast<int32_t>(strlen( value )) );
  if (valuePtr == NULLPTR) {
    FWKINFO( "EventTest::addObject null valuePtr generated." );
    return;
  }
  regionPtr->create( keyPtr, valuePtr );
  std::string bbkey( "CREATE_COUNT" );
  bbIncrement( m_bb, bbkey );
}

// ----------------------------------------------------------------------------

void EventTest::netsearchObject( RegionPtr& randomRegion )
{
  CacheableKeyPtr keyPtr = findKeyNotInCache( randomRegion );
  CacheablePtr anObj = randomRegion->get( keyPtr );

  std::string key( "NETSEARCH_COUNT" );
  bbIncrement( m_bb, key );
}

// ----------------------------------------------------------------------------

int32_t EventTest::percentDifferent()
{
  const std::string testRegionName =  getStringValue( "regionName" );
  if ( testRegionName.empty() ) {
    FWKEXCEPTION( "Data not provided for 'regionName', failing." );
  }

  int32_t expected = getIntValue( "expectedKeyCount" );
  if ( expected < 0 ) {
    std::string key( "CREATE_COUNT" );
    int64_t bbExpected = bbGet( m_bb, key );
    if ( bbExpected <= 0 ) {
      FWKEXCEPTION( "Data not provided for 'expectedKeyCount', failing." );
    }
    else {
      expected = ( int32_t )bbExpected;
    }
  }

  RegionPtr regionPtr = m_cache->getRegion( testRegionName.c_str() );
  VectorOfCacheableKey keys;
  regionPtr->keys( keys );
  int32_t keyCount = keys.size();
  double diff = 0;
  if ( keyCount > expected ) {
    diff = keyCount - expected;
  }
  else {
    diff = expected - keyCount;
  }
  int32_t retVal = ( int32_t )( ( diff / ( ( double )expected + 1.0 ) ) * 100.0 ); // + 1.0 so no divide by zero
  FWKINFO( "Expected to have " << expected << " keys, found " << keys.size()
    << " keys, percent difference: " << retVal );
  return retVal;
}

// ----------------------------------------------------------------------------

int32_t EventTest::verifyKeyCount()
{
  int32_t fwkResult = FWK_SEVERE;

  try {
    int32_t percentDiff = percentDifferent();
    if ( percentDiff > 10 ) {
      FWKSEVERE( "Actual number of keys does not match expected number." );
    }
    else {
      fwkResult = FWK_SUCCESS;
    }
  } catch( Exception& e ) {
    FWKSEVERE( "Caught unexpected exception during validation: " << e.getMessage() );
  } catch( FwkException& e ) {
    FWKSEVERE( "Caught unexpected FwkException during validation: " << e.getMessage() );
  } catch( std::exception e ) {
    FWKSEVERE( "verifyKeyCount caught std::exception: " << e.what() );
  }

  return fwkResult;
}


int32_t EventTest::doBasicTest()
{
  int32_t result = FWK_SUCCESS;
  try {
    RegionPtr regionPtr = getRandomRegion(true);
    int32_t numKeys = getIntValue( "distinctKeys" );
    numKeys = numKeys > 0 ? numKeys : 1000;
    VectorOfCacheableKey keys;
    VectorOfCacheable values;
    for (int i = 0; i < numKeys; ++i) {
      int32_t ksize = getIntValue( "valueSizes" );
      ksize = ksize > 0 ? ksize : 12;
      int32_t vsize = getIntValue( "valueSizes" );
      vsize = vsize > 0 ? vsize : 100;

      std::ostringstream id;
      id << "key_" << i;
      const std::string kStr = id.str();
      const std::string vStr = GsRandom::getAlphanumericString( vsize );
      CacheableKeyPtr keyPtr = CacheableKey::create( kStr.c_str() );
      CacheableBytesPtr valuePtr = CacheableBytes::create( ( const unsigned char * )vStr.c_str(), static_cast<int32_t>(vStr.length()) );
      keys.push_back(keyPtr);
      values.push_back(valuePtr);
      regionPtr->create( keyPtr, valuePtr );      // create
    }
    VectorOfCacheableKey expectKeys;
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != numKeys) {
      FWKSEVERE( "Expect " <<  numKeys << " keys after create, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }
    for (int i = 0; i < numKeys; ++i) {
      regionPtr->localInvalidate( keys[i]);          // localInvalidate
    }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != numKeys) {
      FWKSEVERE( "Expect " <<  numKeys << " keys after localInvalidate, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }
    for (int i = 0; i < numKeys; ++i) {
      CacheablePtr val = regionPtr->get( keys[i]);           // get
      if (strcmp( val->toString()->asChar(), values[i]->toString()->asChar() ) != 0) {
      FWKSEVERE( "Expect " <<  values[i]->toString()->asChar() << ", got " <<  val->toString()->asChar());
        return FWK_SEVERE;
      }
    }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != numKeys) {
      FWKSEVERE( "Expect " <<  numKeys << " keys after first get, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }
    for (int i = 0; i < numKeys; ++i) {
      regionPtr->localDestroy( keys[i]);          //localDestroy
    }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != 0) {
      FWKSEVERE( "Expect 0 keys after localDestroy, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }
    for (int i = 0; i < numKeys; ++i) {
      CacheablePtr val = regionPtr->get( keys[i]);           // get
      if (strcmp( val->toString()->asChar(), values[i]->toString()->asChar() ) != 0) {
      FWKSEVERE( "Expect " <<  values[i]->toString()->asChar() << ", got " <<  val->toString()->asChar());
        return FWK_SEVERE;
      }
    }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != numKeys) {
     FWKSEVERE( "Expect " <<  numKeys << " keys after second get, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }
    for (int i = 0; i < numKeys; ++i) {
      regionPtr->invalidate( keys[i]);          // invalidate
    }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != numKeys) {
      FWKSEVERE( "Expect " <<  numKeys << " keys after invalidate, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }
    for (int i = 0; i < numKeys; ++i) {
      regionPtr->get( keys[i]);         // get
     }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != numKeys) {
      FWKSEVERE( "Expect " <<  numKeys << " keys after invalidate all entries in server, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }
    for (int i = 0; i < numKeys; ++i) {
      regionPtr->put( keys[i], values[i]);          //put
    }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != numKeys) {
     FWKSEVERE( "Expect " <<  numKeys << " keys after put, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }
    for (int i = 0; i < numKeys; ++i) {
      regionPtr->destroy( keys[i]);          // destroy
    }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != 0) {
      FWKSEVERE( "Expect 0 keys after destroy, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }
    int excepCount = 0;
    for (int i = 0; i < numKeys; ++i) {
      try {
        regionPtr->get( keys[i]);         // get
      } catch( EntryNotFoundException & ) {
        ++excepCount;
      }
    }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != 0) {
      FWKSEVERE( "Expect 0 keys because all entries are destoyed in server, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }
    if (excepCount != numKeys) {
      FWKSEVERE( "Expect " << numKeys << " exceptions because all entries are destoyed in server, got " << excepCount << " exceptions" );
      return FWK_SEVERE;
    }
     for (int i = 0; i < numKeys; ++i) {
      regionPtr->create( keys[i], values[i]);          //create
    }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != numKeys) {
     FWKSEVERE( "Expect " <<  numKeys << " keys after second create, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }

    for (int i = 0; i < numKeys; ++i) {
      regionPtr->get( keys[i]);         // get
     }
    regionPtr->keys(expectKeys);
    if ((int32_t)(expectKeys.size()) != numKeys) {
      FWKSEVERE( "Expect " <<  numKeys << " keys after invalidate all entries in server, got " << expectKeys.size() << " keys" );
      return FWK_SEVERE;
    }

  } catch( Exception& e ) {
    FWKSEVERE( "Caught unexpected exception during basic test: " << e.getMessage() );
  } catch( FwkException& e ) {
    FWKSEVERE( "Caught unexpected FwkException during basic test: " << e.getMessage() );
  } catch( std::exception e ) {
    FWKSEVERE( "doBasicTest caught std::exception: " << e.what() );
  }

  return result;
}

// ----------------------------------------------------------------------------

int32_t EventTest::feedEntries()
{
  int32_t result = FWK_SUCCESS;

  const std::string testRegionName =  getStringValue( "regionName" );
  if ( testRegionName.empty() ) {
    FWKEXCEPTION( "Data not provided for 'regionName', failing." );
  }
  RegionPtr regionPtr = m_cache->getRegion( testRegionName.c_str() );

  int32_t opsSecond = getIntValue( "opsSecond" );
  if ( opsSecond < 0 ) {
    opsSecond = 0; // No throttle
  }
  PaceMeter pm( opsSecond );

  int32_t secondsToRun = getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 100 : secondsToRun;
  FWKINFO( "feedEntries: Will add entries for " << secondsToRun << " seconds." );
  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  ACE_Time_Value now = ACE_OS::gettimeofday();

  int32_t count = 0;
  while ( now < end ) {
    try {
      std::string key = FwkStrCvt( ++count ).toString();

      // get value size
      int32_t vsize = getIntValue( "valueSizes" );
      if ( vsize < 0 ) {
        vsize = 1000;
      }
      std::string value = GsRandom::getAlphanumericString( vsize );

      CacheableKeyPtr keyPtr = CacheableKey::create( key.c_str() );
      CacheableBytesPtr valuePtr = CacheableBytes::create( ( const unsigned char * )value.c_str(), static_cast<int32_t>(value.length()) );

      if (keyPtr == NULLPTR){
        FWKSEVERE("EventTest::feedEntries null keyPtr generated.");
        result = FWK_SEVERE;
        now = end;
        continue;
      }
      if (keyPtr == NULLPTR){
        FWKSEVERE("EventTest::feedEntries null valuePtr generated.");
        result = FWK_SEVERE;
        now = end;
        continue;
      }
      // FWKINFO("created entry with key: " << keyPtr->toString());
      regionPtr->put( keyPtr, valuePtr );
      std::string bbkey( "CREATE_COUNT" );
      bbIncrement( m_bb, bbkey );
      pm.checkPace();
      now = ACE_OS::gettimeofday();
    } catch( Exception &e ) {
      FWKSEVERE( "Caught unexpected exception during entry operation: " << e.getMessage() << " continuing with test." );
      result = FWK_SEVERE;
    }
    catch( std::exception e ) {
      FWKSEVERE( "doIterate caught std::exception: " << e.what() );
      result = FWK_SEVERE;
    }
  }
  return result;
}

// ----------------------------------------------------------------------------

int32_t EventTest::addEntry()
{
  int32_t fwkResult = FWK_SUCCESS;
  const std::string testRegionName =  getStringValue( "regionName" );
  if ( testRegionName.empty() ) {
    FWKEXCEPTION( "Data not provided for 'regionName', failing." );
  }
  RegionPtr regionPtr = m_cache->getRegion( testRegionName.c_str() );

  int32_t usePid = getIntValue( "usePID" );
  int32_t pid = ACE_OS::getpid();

  int32_t opsSecond = getIntValue( "opsSecond" );
  if ( opsSecond < 0 ) {
    opsSecond = 0; // No throttle
  }
  PaceMeter pm( opsSecond );

  int32_t entryCount = getIntValue( "EntryCount" );
  if ( entryCount <= 0 ) {
    entryCount = 100;
  }
  FWKINFO( "addEntry: Adding " << entryCount << " entries to the cache." );
  for ( int32_t count = 0; count < entryCount; count++ ) {
    try {
      std::string sKey, sValue;

      if ( usePid == 1 ) {
        sKey = FwkStrCvt( pid ).toString();
      }
      else {
        sKey = "";
      }

      sKey.append( FwkStrCvt( count ).toString() );
      // get value size
      int32_t vsize = getIntValue( "valueSizes" );
      if ( vsize < 0 ) {
        vsize = 1000;
      }
      sValue = GsRandom::getAlphanumericString( vsize );
      CacheableKeyPtr keyPtr = CacheableKey::create( sKey.c_str() );
      CacheableBytesPtr valuePtr = CacheableBytes::create( ( const unsigned char * )sValue.c_str(), static_cast<int32_t>(sValue.length()) );

      if (keyPtr == NULLPTR) {
        FWKSEVERE("EventTest::addObject null keyPtr generated.");
        fwkResult = FWK_SEVERE;
      }
      // FWKINFO("created entry with key: " << keyPtr->toString()->asChar());
      regionPtr->put( keyPtr, valuePtr );
     std::string key( "CREATE_COUNT" );
      bbIncrement( m_bb, key );
      pm.checkPace();
    }catch (Exception &e) {
      FWKSEVERE("Caught unexpected exception during entry operation: " << e.getMessage() << " continuing with test.");
    }
    catch (std::exception e) {
      FWKSEVERE("doIterate caught std::exception: " <<
        e.what());
      fwkResult = FWK_SEVERE;
    }
  }
  FWKINFO( "addEntry: Complete." );
  return fwkResult;
}

int32_t EventTest::addOrDestroyEntry()
{
  int32_t fwkResult = FWK_SUCCESS;
  const std::string testRegionName =  getStringValue( "regionName" );
  if ( testRegionName.empty() ) {
    FWKEXCEPTION( "Data not provided for 'regionName', failing." );
  }
  RegionPtr regionPtr = m_cache->getRegion( testRegionName.c_str() );

  int32_t usePid = getIntValue( "usePID" );
  int32_t pid = ACE_OS::getpid();

  int32_t entryCount = getIntValue( "EntryCount" );
  if ( entryCount <= 0 ) {
    entryCount = 100;
  }
  FWKINFO( "addOrDestroyEntry: Adding or Destroying ( if present )" << entryCount << " entries to the cache." );
  for ( int32_t count = 0; count < entryCount; count++ ) {
    try {
      std::string sKey, sValue;

      if ( usePid == 1 ) {
        sKey = FwkStrCvt( pid ).toString();
      }
      else {
        sKey = "";
      }

      sKey.append( FwkStrCvt( count ).toString() );
      // get value size
      int32_t vsize = getIntValue( "valueSizes" );
      if ( vsize < 0 ) {
        vsize = 1000;
      }
      sValue = GsRandom::getAlphanumericString( vsize );

      //FWKINFO( " key = " << key );
      CacheableKeyPtr keyPtr = CacheableKey::create( sKey.c_str() );
      CacheableBytesPtr valuePtr = CacheableBytes::create( ( const unsigned char * )sValue.c_str(), static_cast<int32_t>(sValue.length()) );

      if (keyPtr == NULLPTR) {
        FWKSEVERE("EventTest::addObject null keyPtr generated.");
        fwkResult = FWK_SEVERE;
        return fwkResult;
      }

      const std::string op = getStringValue( "popOp" );
      if ( op == "put" ) {
        regionPtr->put( keyPtr, valuePtr );
      }
      else
      {
        regionPtr->destroy( keyPtr );
      }

      std::string key( "CREATE_COUNT" );
      bbIncrement( m_bb, key );
    }catch (EntryNotFoundException &e) {
      e.getMessage();
      // Not printing this line as it is very much expected in tis test.
      // FWKSEVERE("Caught an expected exception during entry destroy: " << e.getMessage() << " continuing with test.");
    }catch (Exception &e) {
      FWKSEVERE("Caught unexpected exception during entry operation: " << e.getMessage() << " continuing with test.");
    }
    catch (std::exception e) {
      FWKSEVERE("doIterate caught std::exception: " <<
        e.what());
      fwkResult = FWK_SEVERE;
    }
  }
  FWKINFO( "addOrDestroyEntry: Complete." );
  return fwkResult;
}

// ----------------------------------------------------------------------

void EventTest::invalidateObject(RegionPtr &randomRegion, bool bIsLocalInvalidate )
{
  CacheableKeyPtr keyPtr = getKey( randomRegion, false );
  if (keyPtr == NULLPTR) {
    std::string key( "OPS_SKIPPED_COUNT" );
    bbIncrement( m_bb, key );
    return;
  }
//  FWKINFO( "EventTest::invalidateObject "  << keyPtr->toString());

  if (bIsLocalInvalidate){
    randomRegion->localInvalidate(keyPtr);
    std::string key( "LOCAL_INVALIDATE_COUNT" );
    bbIncrement( m_bb, key );
  }else {
    randomRegion->invalidate(keyPtr);
    std::string key( "INVALIDATE_COUNT" );
    bbIncrement( m_bb, key );
  }
}

// ----------------------------------------------------------------------------

void EventTest::destroyObject(RegionPtr &randomRegion, bool bIsLocalDestroy )
{
  //  FWKINFO( "EventTest::destroyObject" );
  CacheableKeyPtr keyPtr = getKey( randomRegion, true );
  if (keyPtr == NULLPTR) {
    std::string key( "OPS_SKIPPED_COUNT" );
    bbIncrement( m_bb, key );
    return;
  }
//  FWKINFO( "EventTest::destroyObject key = " << keyPtr->toString() );
  if (bIsLocalDestroy) {
    randomRegion->localDestroy(keyPtr);
    std::string key( "LOCAL_DESTROY_COUNT" );
    bbIncrement( m_bb, key );
  } else {
    randomRegion->destroy(keyPtr);
    std::string key( "DESTROY_COUNT" );
    bbIncrement( m_bb, key );
  }
}

// ----------------------------------------------------------------------------

void EventTest::updateObject(RegionPtr &randomRegion)
{
  CacheableKeyPtr keyPtr = getKey( randomRegion, true );
  if (keyPtr == NULLPTR) {
    FWKINFO("EventTest::updateObject key is null" );
    std::string key( "OPS_SKIPPED_COUNT" );
    bbIncrement( m_bb, key );
    return;
  }
  CacheablePtr anObj;
  anObj = randomRegion->get( keyPtr );

  // get random value size
  int32_t vsize = getIntValue( "valueSizes" );
  if ( vsize < 0 ) {
    vsize = 1000;
  }
  std::string str = GsRandom::getAlphanumericString( vsize );
  CacheableBytesPtr newObj =
    CacheableBytes::create( ( const unsigned char * )str.c_str(), static_cast<int32_t>(str.length()) );
  randomRegion->put(keyPtr, newObj);
  std::string key( "UPDATE_COUNT" );
  bbIncrement( m_bb, key );
}

// ----------------------------------------------------------------------------

void EventTest::readObject(RegionPtr &randomRegion)
{
  CacheableKeyPtr keyPtr = getKey( randomRegion, true );

  if (keyPtr == NULLPTR) {
    std::string key( "OPS_SKIPPED_COUNT" );
    bbIncrement( m_bb, key );
    return;
  }
//  FWKINFO( "EventTest::readObject "  << keyPtr->toString());
  CacheablePtr anObj;
  anObj = randomRegion->get(keyPtr);

  std::string key( "READ_COUNT" );
  bbIncrement( m_bb, key );
}

// ----------------------------------------------------------------------------

CacheableKeyPtr EventTest::getKey(RegionPtr& regionPtr, bool bInvalidOK)
{
  int32_t randomKey = getIntValue( "randomKey" );
  CacheableKeyPtr keyPtr;
  if (randomKey > 0) {
    std::string sKey;
    const char* key = NULL;
    sKey = FwkStrCvt( randomKey ).toString();
    key = sKey.c_str();
    keyPtr = CacheableKey::create( key );
    return keyPtr;
  }

  VectorOfCacheableKey keys;
  regionPtr->keys(keys);
  int iKeySize = keys.size();
  if (iKeySize == 0){
    return keyPtr;
  }
  int iStartAt = GsRandom::random( iKeySize );
  if (bInvalidOK)
    return keys.at(iStartAt);
  int iKeyIndex = iStartAt;
  do {
    bool hasValue = regionPtr->containsValueForKey( keys.at( iKeyIndex ) );
    if ( hasValue )
      return keys.at(iKeyIndex);
    iKeyIndex++;
    if (iKeyIndex >= iKeySize)
      iKeyIndex = 0;
  } while (iKeyIndex != iStartAt);

  FWKINFO("getKey: All values invalid in region");
  return keyPtr;
}

// ----------------------------------------------------------------------------

RegionPtr EventTest::getRandomRegion(bool bAllowRootRegion)
{
  VectorOfRegion rootRegionVector;
  VectorOfRegion subRegionVector;

  m_cache->rootRegions(rootRegionVector);
  int iRootSize = rootRegionVector.size();

  if (iRootSize == 0)
    return RegionPtr();

  VectorOfRegion choseRegionVector;

  // if roots can be chosen, add them to candidates
  if (bAllowRootRegion){
    for (int iRootIndex = 0; iRootIndex < iRootSize; iRootIndex++)
      choseRegionVector.push_back(rootRegionVector.at(iRootIndex));
  }

  // add all subregions
  for (int iRootIndex = 0; iRootIndex < iRootSize; iRootIndex++){
    rootRegionVector.at(iRootIndex)->subregions(true, subRegionVector);
    int iSubSize = subRegionVector.size();
    for (int iSubIndex = 0; iSubIndex < iSubSize; iSubIndex++)
      choseRegionVector.push_back(subRegionVector.at(iSubIndex));
  }

  int iChoseRegionSize = choseRegionVector.size();
  if (iChoseRegionSize == 0) {
    return RegionPtr();
  }

  int idx = GsRandom::random( iChoseRegionSize );
  std::string regionName = choseRegionVector.at(idx)->getFullPath();
  return choseRegionVector.at(idx);
}

// ----------------------------------------------------------------------------

void EventTest::handleExpectedException( Exception & e )
{
  FWKDEBUG( "Caught and ignored: " << e.getMessage() );
}

// ----------------------------------------------------------------------------

void EventTest::verifyObjectInvalidated( RegionPtr& regionPtr, CacheableKeyPtr& keyPtr)
{
  if ((regionPtr == NULLPTR) && (keyPtr == NULLPTR)) {
    return;
  }

  std::ostringstream osError;

  bool bContainsKey = regionPtr->containsKey(keyPtr);
  if (!bContainsKey){
    osError << "Unexpected containsKey " << bContainsKey <<
      " for key " << dynCast<CacheableStringPtr>( keyPtr )->asChar() <<
      " in region " << regionPtr->getFullPath() <<
      "\n";
  }

  bool bContainsValueForKey = regionPtr->containsValueForKey(keyPtr);
  if (bContainsValueForKey){
    osError << "Unexpected containsValueForKey " << bContainsValueForKey <<
      " for key " << dynCast<CacheableStringPtr>( keyPtr )->asChar() <<
      " in region " << regionPtr->getFullPath() << "\n";
  }

  RegionEntryPtr entryPtr = regionPtr->getEntry(keyPtr);

  if (entryPtr == NULLPTR){
    osError << "getEntry for key " << dynCast<CacheableStringPtr>( keyPtr )->asChar() <<
      " in region " << regionPtr->getFullPath() <<
      " returned NULL\n";
  }else{
    CacheableStringPtr entryKeyPtr =
      dynCast<CacheableStringPtr>( entryPtr->getKey() );

    if (entryKeyPtr != keyPtr){
      osError << "getEntry.getKey() " << entryKeyPtr->asChar() <<
        " does not equal key " << dynCast<CacheableStringPtr>( keyPtr )->asChar() <<
        " in region " << regionPtr->getFullPath() <<
        "\n";

      CacheableBytesPtr entryValuePtr =
        dynCast<CacheableBytesPtr>( entryPtr->getValue() );

      if (entryValuePtr != NULLPTR) {
        osError << "Expected getEntry.getValue() " <<
          entryValuePtr->value() << " to be null.\n";
      }
    }
  }

  std::string sError(osError.str());

  if (sError.size() > 0)
    FWKEXCEPTION(sError);

}

// ----------------------------------------------------------------------------

void EventTest::verifyObjectDestroyed( RegionPtr& regionPtr, CacheableKeyPtr& keyPtr)
{
  if ((regionPtr == NULLPTR) && (keyPtr == NULLPTR)) {
    return;
  }

  std::ostringstream osError;

  bool bContainsKey = regionPtr->containsKey(keyPtr);
  if (bContainsKey){
    osError << "Unexpected containsKey " << bContainsKey <<
      " for key " << dynCast<CacheableStringPtr>( keyPtr )->asChar() <<
      " in region " << regionPtr->getFullPath() <<
      "\n";
  }

  bool bContainsValueForKey = regionPtr->containsValueForKey(keyPtr);
  if (bContainsValueForKey){
    osError << "Unexpected containsValueForKey " << bContainsValueForKey <<
      " for key " << dynCast<CacheableStringPtr>( keyPtr )->asChar() <<
      " in region " << regionPtr->getFullPath() <<
      "\n";
  }

  RegionEntryPtr entryPtr = regionPtr->getEntry(keyPtr);

  if (entryPtr != NULLPTR) {
    CacheableStringPtr entryKeyPtr =
      dynCast<CacheableStringPtr>( entryPtr->getKey() );

    CacheableBytesPtr entryValuePtr =
      dynCast<CacheableBytesPtr>( entryPtr->getValue() );

    osError << "getEntry for key " << dynCast<CacheableStringPtr>( keyPtr )->asChar() <<
      " in region " << regionPtr->getFullPath() <<
      " returned was non-null; getKey is " << entryKeyPtr->asChar() <<
      ", value is " << entryValuePtr->value() << "\n";
  }

  std::string sError(osError.str());

  if (sError.size() > 0)
    FWKEXCEPTION(sError);
}

// ----------------------------------------------------------------------------

int EventTest::getSubRegionCount(const RegionPtr& regionPtr)
{
  VectorOfRegion subRegionVector;
  regionPtr->subregions(true, subRegionVector);
  return subRegionVector.size();
}

// ----------------------------------------------------------------------------

int EventTest::getAllRegionCount()
{
  VectorOfRegion rootRegionVector;
  try {
    if (m_cache == NULLPTR) {
      FWKSEVERE("Null cache pointer, no connection established.");
      return 0;
    }
    m_cache->rootRegions(rootRegionVector);
  } catch( std::exception e ) {
    FWKSEVERE( "EventTest::getAllRegionCount exception occurred "
      "during call to Cache::rootRegions: " << e.what() );
  }
  int iRootSize = rootRegionVector.size();
  int iTotalRegions = iRootSize;

  for (int iIndex = 0; iIndex < iRootSize; iIndex++) {
    iTotalRegions += getSubRegionCount(rootRegionVector.at(iIndex));
  }
  return iTotalRegions;
}

void EventTest::measureMemory(std::string location, double & vs, double & rs)
{
  //sleep 2 seconds to let things settle down
  perf::sleepSeconds( 10 );

#ifndef WIN32
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

  FWKINFO(location << "VmSize: " << vs << " Mb   RSS: " << rs << " Mb" );
#else
  FWKEXCEPTION("Memory measurement is not implemented on WIN32" );
#endif
}


// ----------------------------------------------------------------------------

void EventTest::iterateRegion(RegionPtr regionPtr, bool bAllowZeroKeys, bool bAllowZeroNonNullValues,
                              uint32_t& ulKeysInRegion, uint32_t& ulNoneNullValuesInRegion, std::string& sError)
{
  if (regionPtr == NULLPTR) {
    return;
  }

  std::ostringstream osError;

  ulKeysInRegion = 0;
  ulNoneNullValuesInRegion = 0;
  sError.clear();

  VectorOfCacheableKey keyVector;

  regionPtr->keys(keyVector);
  ulKeysInRegion = keyVector.size();
  if (ulKeysInRegion == 0) {
    if (!bAllowZeroKeys){
      osError << "Region " << regionPtr->getFullPath() <<
        " has " << ulKeysInRegion << " keys\n";
    }
  }

  CacheableKeyPtr keyPtr;
  CacheablePtr    valuePtr;

  for(uint32_t ulIndex = 0; ulIndex < ulKeysInRegion; ulIndex++){
    keyPtr = keyVector.at(ulIndex);

    try {
      valuePtr = regionPtr->get(keyPtr);
    } catch (CacheLoaderException e) {
      FWKEXCEPTION("CacheLoaderException " << e.getMessage());
    } catch (TimeoutException e) {
      FWKEXCEPTION("TimeoutException " << e.getMessage());
    }

    if (valuePtr != NULLPTR) {
      ulNoneNullValuesInRegion++;
    }
  }

  if (ulNoneNullValuesInRegion == 0) {
    if (!bAllowZeroNonNullValues){
      osError << "Region " << regionPtr->getFullPath() <<
        " has " << ulNoneNullValuesInRegion << " non-null values\n";
    }
  }

  sError = osError.str();
}

//----------------------------------------------------------------------------
int32_t EventTest::doGets()
{
  int32_t fwkResult = FWK_SUCCESS;
  int32_t usePid = getIntValue( "usePID" );
  int32_t myPid = ACE_OS::getpid();
  int32_t entryCount = getIntValue( "EntryCount" );
  int32_t vsize = getIntValue( "valueSizes" );
  vsize = (vsize < 0) ? 1000 : vsize;
  const std::string testRegionName =  getStringValue( "regionName" );
  if ( testRegionName.empty() ) {
    FWKEXCEPTION( "Data not provided for 'regionName', failing." );
  }
  RegionPtr regionPtr = m_cache->getRegion( testRegionName.c_str() );

  try {
        for ( int32_t count = 0; count < entryCount; count++ ) {
          std::string sKey;
          const char* key = NULL;
          if ( usePid == 1 ) {
            sKey = FwkStrCvt( myPid ).toString();
          }
          else {
           sKey = "";
          }
          sKey.append( FwkStrCvt( count ).toString() );
          key = sKey.c_str();
          CacheableKeyPtr keyPtr = CacheableKey::create( key );
          if (keyPtr == NULLPTR) {
            FWKSEVERE("EventTest::doGets null keyPtr generated.");
            fwkResult = FWK_SEVERE;
          }
          CacheableBytesPtr valuePtr = dynCast<CacheableBytesPtr>( regionPtr->get(keyPtr) );
          if (valuePtr == NULLPTR) {
            incFailCount();
            FWKSEVERE("EventTest::doGets get an unexpected null value!" );
            fwkResult = FWK_SEVERE;
          } else {
            int32_t vlength = valuePtr->length();
            if (vlength != vsize) {
              incFailCount();
              FWKSEVERE("EventTest::doGets value size " << vlength << "does not match expected size " << vsize << ".");
              fwkResult = FWK_SEVERE;
            }
          }
        }
  } catch( const Exception& e ) {
    FWKSEVERE( "EventTest::doGets Caught unexpected exception: " << e.getMessage() );
    fwkResult = FWK_SEVERE;
  } catch( const std::exception e ) {
    FWKSEVERE( "EventTest::doGets caught std::exception: " << e.what() );
    fwkResult = FWK_SEVERE;
  }
  return fwkResult;
}

// ============================================================================

void ETCacheListener::afterCreate( const EntryEvent& event )
{
  std::string key( "AFTER_CREATE_COUNT" );
  m_test->bbIncrement( m_bb, key );
}

// ----------------------------------------------------------------------------

void ETCacheListener::afterUpdate( const EntryEvent& event )
{
   std::string key( "AFTER_UPDATE_COUNT" );
   m_test->bbIncrement( m_bb, key );

}

// ----------------------------------------------------------------------------

void ETCacheListener::afterInvalidate( const EntryEvent& event )
{
  std::string key( "AFTER_INVALIDATE_COUNT" );
  m_test->bbIncrement( m_bb, key );

}

// ----------------------------------------------------------------------------

void ETCacheListener::afterDestroy( const EntryEvent& event )
{
  std::string key( "AFTER_DESTROY_COUNT" );
  m_test->bbIncrement( m_bb, key );

}

// ----------------------------------------------------------------------------

void ETCacheListener::afterRegionInvalidate( const RegionEvent& event )
{
  std::string key( "AFTER_REGION_INVALIDATE_COUNT" );
  m_test->bbIncrement( m_bb, key );

}

// ----------------------------------------------------------------------------

void ETCacheListener::afterRegionDestroy( const RegionEvent& event ) {}

// ----------------------------------------------------------------------------

void ETCacheListener::close( const RegionPtr& region )
{
  std::string key( "CLOSE_COUNT" );
  m_test->bbIncrement( m_bb, key );
}

// ============================================================================

bool ETCacheWriter::beforeUpdate( const EntryEvent& event )
{
  std::string key( "BEFORE_UPDATE_COUNT" );
  m_test->bbIncrement( m_bb, key );
  return true;
}

// ----------------------------------------------------------------------------

bool ETCacheWriter::beforeCreate( const EntryEvent& event )
{
  std::string key( "BEFORE_CREATE_COUNT" );
  m_test->bbIncrement( m_bb, key );
  return true;
}

// ----------------------------------------------------------------------------

bool ETCacheWriter::beforeDestroy( const EntryEvent& event )
{
  std::string key( "BEFORE_DESTROY_COUNT" );
  m_test->bbIncrement( m_bb, key );
  return true;
}

// ----------------------------------------------------------------------------

bool ETCacheWriter::beforeRegionDestroy( const RegionEvent& event )
{
  std::string key( "BEFORE_REGION_DESTROY_COUNT" );
  m_test->bbIncrement( m_bb, key );
  return true;
}

// ----------------------------------------------------------------------------

void ETCacheWriter::close( const RegionPtr& region )
{
  std::string key( "CLOSE_COUNT" );
  m_test->bbIncrement( m_bb, key );
}

// ============================================================================

CacheablePtr ETCacheLoader::load(const RegionPtr& rp,
                                 const CacheableKeyPtr& key,
                                 const UserDataPtr& aCallbackArgument)
{
  std::string bbkey( "LOAD_CACHEABLE_STRING_COUNT" );
  m_test->bbIncrement( m_bb, bbkey );

  std::string sValue = GsRandom::getAlphanumericString( 2000 );
 // FWKINFO("ETCacheLoader::load creates new value for key: " << key->toString() );
  return CacheableBytes::create( ( const unsigned char * )sValue.c_str(), static_cast<int32_t>(sValue.length()) );
}

// ----------------------------------------------------------------------------

void ETCacheLoader::close( const RegionPtr& region )
{
  std::string key( "CLOSE_COUNT" );
  m_test->bbIncrement( m_bb, key );

}
